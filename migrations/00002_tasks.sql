-- SQL code is mostly taken or adapted from pgflow (https://github.com/pgflow-dev/pgflow)

-- +goose up

-- TODO acquire locks where necessary

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_flow_runs (
  id uuid primary key default gen_random_uuid(),
  flow_name text not null references cb_flows (name),
  status text not null default 'started',
  input jsonb not null,
  output jsonb,
  remaining_steps int not null default 0 check (remaining_steps >= 0),
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  failed_at timestamptz,
  constraint completed_at_or_failed_at check (not (completed_at is not null and failed_at is not null)),
  constraint completed_at_is_after_started_at check (completed_at is null or completed_at >= started_at),
  constraint failed_at_is_after_started_at check (failed_at is null or failed_at >= started_at),
  constraint status_is_valid check (status in ('started', 'completed', 'failed'))
);


CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    task_name text NOT NULL,
    idx int not null,
    dependency_count int not null default 0 check (dependency_count >= 0),
    PRIMARY KEY (flow_name, name)
);

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_step_dependencies (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    step_name text not null,
    dependency_name text not null,
    primary key (flow_name, step_name, dependency_name),
    foreign key (flow_name, step_name) references cb_steps (flow_name, name),
    foreign key (flow_name, dependency_name) references cb_steps (flow_name, name),
    check (step_name != dependency_name)
);

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_step_runs (
  id uuid primary key default gen_random_uuid(),
  flow_run_id uuid not null references cb_flow_runs (id),
  flow_name text not null references cb_flows (name),
  step_name text not null,
  task_name text not null references cb_tasks (name),
  status text not null default 'created',
  error_message text,
  remaining_dependencies int not null default 0 check (remaining_dependencies >= 0),
  remaining_tasks int not null default 0 check (remaining_tasks >= 0),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  failed_at timestamptz,
  foreign key (flow_name, step_name) references cb_steps (flow_name, name),
  constraint status_is_valid check (status in ('created', 'started', 'completed', 'failed')),
--   constraint status_and_remaining_tasks_match check (status != 'completed' or remaining_tasks = 0),
  -- Add constraint to ensure remaining_tasks is only set when step has started
--   constraint remaining_tasks_state_consistency check (
    -- remaining_tasks is null or status != 'created'
--   ),
--   constraint initial_tasks_known_when_started check (
    -- status != 'started' or initial_tasks is not null
--   ),
  constraint completed_at_or_failed_at check (not (completed_at is not null and failed_at is not null)),
  constraint started_at_is_after_created_at check (started_at is null or started_at >= created_at),
  constraint completed_at_is_after_started_at check (completed_at is null or completed_at >= started_at),
  constraint failed_at_is_after_started_at check (failed_at is null or failed_at >= started_at)
);

CREATE TABLE IF NOT EXISTS cb_tasks (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_task_runs (
  id uuid primary key default gen_random_uuid(),
  step_run_id uuid references cb_step_runs (id), -- if task is part of a flow
  task_name text not null references cb_tasks (name),
  message_id bigint, -- TODO constraint not null when started
  status text not null default 'started',
  output jsonb,
  error_message text,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  failed_at timestamptz,
  constraint status_is_valid check (status in ('started', 'completed', 'failed')),
  constraint completed_at_or_failed_at check (not (completed_at is not null and failed_at is not null)),
  constraint completed_at_is_after_started_at check (completed_at is null or completed_at >= started_at),
  constraint failed_at_is_after_started_at check (failed_at is null or failed_at >= started_at)
);

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_task(
    name text
)
RETURNS void AS $$
DECLARE
BEGIN
    INSERT INTO cb_tasks (name)
    VALUES (
        cb_create_task.name
    )
    ON CONFLICT DO NOTHING;

    PERFORM cb_create_queue('t_' || cb_create_task.name);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_run_task(
    name text,
    input jsonb
)
RETURNS uuid AS $$
DECLARE
    _id uuid = gen_random_uuid();
BEGIN
    INSERT INTO cb_task_runs (id, task_name, status, started_at, message_id)
    VALUES (
      _id,
      cb_run_task.name,
      'started',
      now(),
      cb_send(
      	queue => 't_' || cb_run_task.name,
      	payload => json_build_object(
          'run_id', _id,
      		'input', cb_run_task.input
        )
      )
    );
    RETURN _id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_complete_task(
    run_id uuid,
    output json
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _step_run_id uuid;
  _flow_run_id uuid;
BEGIN
  -- TODO all in one query 
	-- fail task run
	UPDATE cb_task_runs
  	SET status = 'completed',
      completed_at = now(),
      output = cb_complete_task.output
  	WHERE id = cb_complete_task.run_id
      AND status = 'started'
  RETURNING step_run_id
  INTO _step_run_id;

	-- delete queued task message
	PERFORM cb_delete('t_' || task_name, message_id)
	FROM cb_task_runs
	WHERE id = cb_complete_task.run_id AND message_id IS NOT NULL;

  -- maybe complete step and flow run if part of a flow
  IF _step_run_id IS NOT NULL THEN

    -- TODO GUARD: No mutations on failed runs

    WITH
    -- complete step run
    step AS (
      UPDATE cb_step_runs s_r
        SET status = 'completed',
          completed_at = now(),
          remaining_tasks = s_r.remaining_tasks - 1
        WHERE s_r.id = _step_run_id
          AND s_r.status = 'started'
        RETURNING s_r.*
    ),
    child_steps AS (
      SELECT deps.step_name AS child_step_name
      FROM cb_step_dependencies deps
      JOIN step parent_step ON parent_step.status = 'completed' AND deps.flow_name = parent_step.flow_name
      WHERE deps.dependency_name = parent_step.step_name  -- dep_slug is the parent, step_slug is the child
      ORDER BY deps.step_name  -- Ensure consistent ordering
    ),
    -- Acquire locks on all child steps before updating them
    child_steps_lock AS (
      SELECT * FROM cb_step_runs s_r, step
      WHERE s_r.flow_run_id = step.flow_run_id
        AND s_r.step_name IN (SELECT child_step_name FROM child_steps)
      FOR UPDATE
    ),
    -- Decrement remaining_dependencies
    child_steps_update AS (
      UPDATE cb_step_runs s_r
      SET remaining_dependencies = s_r.remaining_dependencies - 1
        FROM child_steps children, step
      WHERE s_r.flow_run_id = step.flow_run_id
      AND s_r.step_name = children.child_step_name
    )
    UPDATE cb_flow_runs f_r
    SET remaining_steps = remaining_steps - 1
    FROM step
    WHERE f_r.id = step.flow_run_id
    RETURNING f_r.id
    INTO _flow_run_id;

    -- maybe complete flow run
      UPDATE cb_flow_runs
      SET status = 'completed',
          completed_at = now()
    WHERE id = _flow_run_id
      AND remaining_steps = 0
        AND status != 'completed';

    -- TODO only if needed
    PERFORM _cb_start_steps(_flow_run_id);

  END IF;

END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_fail_task(
    run_id uuid,
    error_message text
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _step_run_id uuid;
  _flow_run_id uuid;
BEGIN
  -- TODO all in one query 
	-- fail task run
	UPDATE cb_task_runs
  	SET status = 'failed',
      failed_at = now(),
      error_message = cb_fail_task.error_message
  	WHERE id = cb_fail_task.run_id
      AND status = 'started'
  RETURNING step_run_id
  INTO _step_run_id;

  -- delete queued task message
    PERFORM cb_delete('t_' || task_name, message_id)
    FROM cb_task_runs
    WHERE id = cb_fail_task.run_id AND message_id IS NOT NULL;

  -- fail step and flow run if part of a flow
  IF _step_run_id IS NOT NULL THEN
    WITH step_run AS (
    	UPDATE cb_step_runs s_r
  	  SET status = 'failed',
        failed_at = now(),
        error_message = cb_fail_task.error_message
      WHERE s_r.id = _step_run_id
        AND s_r.status = 'started'
      RETURNING s_r.flow_run_id
    )
    UPDATE cb_flow_runs
    SET status = 'failed',
        failed_at = now()
    FROM step_run
    WHERE cb_flow_runs.id = step_run.flow_run_id
      AND cb_flow_runs.status = 'started'
      RETURNING step_run.flow_run_id
      INTO _flow_run_id;

    -- DELETE ALL ACTIVE MESSAGES IN QUEUE
      PERFORM cb_delete_many('t_' || t_r.task_name, ARRAY_AGG(t_r.message_id))
      FROM cb_step_runs s_r
      JOIN cb_task_runs t_r ON t_r.step_run_id = s_r.id
      WHERE s_r.flow_run_id = _flow_run_id
        AND s_r.status IN ('queued', 'started')
        AND t_r.message_id IS NOT NULL
      GROUP BY t_r.task_name
      HAVING COUNT(t_r.message_id) > 0;
  END IF;

END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_flow(
    name text,
    steps jsonb
)
RETURNS void AS $$
DECLARE
    _idx int = 0;
    _step jsonb;
BEGIN
    -- TODO should check attributes are same
    INSERT INTO cb_flows (name)
    VALUES (
        cb_create_flow.name
    )
    ON CONFLICT DO NOTHING;

    FOR _step IN SELECT * FROM jsonb_array_elements(steps)
    loop
        INSERT INTO cb_steps (flow_name, name, task_name, idx, dependency_count)
        VALUES (
            cb_create_flow.name,
            _step->>'name',
            COALESCE(_step->>'task_name', _step->>'name'),
            _idx,
            COALESCE(jsonb_array_length(_step->'depends_on'), 0)
        )
        ON CONFLICT DO NOTHING;

		INSERT INTO cb_step_dependencies (flow_name, step_name, dependency_name)
		SELECT cb_create_flow.name, _step->>'name', dep
		FROM jsonb_array_elements_text(COALESCE(_step->'depends_on', '[]'::jsonb)) AS dep
		ON CONFLICT DO NOTHING;
		
        PERFORM cb_create_queue('t_' || COALESCE(_step->>'task_name', _step->>'name'));

        _idx = _idx + 1;
    end loop;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_run_flow(
    name text,
    input jsonb
)
RETURNS uuid AS $$
DECLARE
    _id uuid;
BEGIN
    WITH
  -- ---------- Gather flow metadata ----------
  flow_steps AS (
    SELECT *
    FROM cb_steps s
    WHERE s.flow_name = cb_run_flow.name
  ),
  flow_run AS (
    INSERT INTO cb_flow_runs (flow_name, input, remaining_steps)
    VALUES (
      cb_run_flow.name,
      cb_run_flow.input,
      (SELECT count(*) FROM flow_steps)
    )
    RETURNING *
  ),
  step_runs AS (
    INSERT INTO cb_step_runs (flow_name, flow_run_id, step_name, task_name, remaining_dependencies, remaining_tasks)
    SELECT
      s.flow_name,
      (SELECT flow_run.id FROM flow_run),
      s.name,
      s.task_name,
      s.dependency_count,
      1
    FROM flow_steps s
    RETURNING *
  )
  SELECT id FROM flow_run
  INTO _id;

  PERFORM _cb_start_steps(_id);

  RETURN _id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_start_steps(
    flow_run_id uuid
)
RETURNS void AS $$
DECLARE
BEGIN

-- ==========================================
-- GUARD: No mutations on failed runs
-- ==========================================
IF EXISTS (SELECT 1 FROM cb_flow_runs f_r WHERE f_r.id = _cb_start_steps.flow_run_id AND f_r.status = 'failed') THEN
  RETURN;
END IF;

WITH
flow_run as (
    select
      r.input
    from cb_flow_runs r
    where r.id = _cb_start_steps.flow_run_id
  ),

ready_steps AS (
  SELECT *
  FROM cb_step_runs AS s_r
  WHERE s_r.flow_run_id = _cb_start_steps.flow_run_id
    AND s_r.status = 'created'
    AND s_r.remaining_dependencies = 0
  ORDER BY s_r.step_name
  FOR UPDATE
), step_runs AS (
-- ---------- Mark steps as started ----------
  UPDATE cb_step_runs
  SET status = 'started',
      started_at = now()
  FROM ready_steps, flow_run
  WHERE cb_step_runs.flow_run_id = _cb_start_steps.flow_run_id
    AND cb_step_runs.step_name = ready_steps.step_name
  RETURNING cb_step_runs.id, cb_step_runs.task_name
), task_runs AS ( --TODO combine this and thew following into one statement
    INSERT INTO cb_task_runs (step_run_id, task_name)
    SELECT
      s_r.id,
      s_r.task_name
    FROM step_runs s_r
    RETURNING id
  )
  UPDATE cb_task_runs t_r
  SET message_id = cb_send(
      	queue => 't_' || t_r.task_name,
      	payload => json_build_object(
      		'run_id', t_r.id,
          'input', flow_run.input
      	)
      )
  FROM task_runs, flow_run
  WHERE t_r.id = task_runs.id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_run_flow;
DROP FUNCTION _cb_start_steps;
DROP FUNCTION cb_create_flow;
DROP FUNCTION cb_complete_task;
DROP FUNCTION cb_fail_task;
DROP FUNCTION cb_run_task;
DROP FUNCTION cb_create_task;
DROP TABLE cb_task_runs;
DROP TABLE cb_step_runs;
DROP TABLE cb_flow_runs;
DROP TABLE cb_tasks;
DROP TABLE cb_step_dependencies;
DROP TABLE cb_steps;
DROP TABLE cb_flows;
