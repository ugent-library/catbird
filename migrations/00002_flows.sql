-- +goose up

-- TODO acquire locks where necessary

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
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

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_step_runs (
  flow_run_id uuid not null references cb_flow_runs (id),
  flow_name text not null references cb_flows (name),
  step_name text not null,
  task_name text NOT NULL,
  message_id bigint,  -- TODO constraint not null when started
  status text not null default 'created',
  output jsonb,
  error_message text,
  remaining_dependencies int not null default 0 check (remaining_dependencies >= 0),
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  failed_at timestamptz,
  primary key (flow_run_id, step_name),
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
    SELECT s.flow_name, s.name
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
    INSERT INTO cb_step_runs (flow_name, flow_run_id, step_name, task_name, remaining_dependencies)
    SELECT
      s.flow_name,
      (SELECT flow_run.id FROM flow_run),
      s.name,
      s.task_name,
      s.dependency_count
    FROM cb_steps s
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
)
-- ---------- Mark steps as started ----------
  UPDATE cb_step_runs
  SET status = 'started',
      started_at = now(),
      message_id = cb_send(
      	queue => 't_' || cb_step_runs.task_name,
      	payload => json_build_object(
      		'flow_name', cb_step_runs.flow_name,
      		'flow_run_id', cb_step_runs.flow_run_id,
      		'step_name', cb_step_runs.step_name,
            'input', flow_run.input
      	)
      )
  FROM ready_steps, flow_run
  WHERE cb_step_runs.flow_run_id = _cb_start_steps.flow_run_id
    AND cb_step_runs.step_name = ready_steps.step_name
    ;

END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_fail_step(
    flow_run_id uuid,
    step_name text,
    error_message text
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
BEGIN
	-- fail step run
	UPDATE cb_step_runs s_r
  	SET status = 'failed',
      failed_at = now(),
      error_message = cb_fail_step.error_message
  	WHERE s_r.flow_run_id = cb_fail_step.flow_run_id
      AND s_r.step_name = cb_fail_step.step_name
      AND s_r.status = 'started';

	-- fail flow run
    UPDATE cb_flow_runs
    SET status = 'failed',
        failed_at = now()
	WHERE id = cb_fail_step.flow_run_id
      AND status = 'started';

	-- delete all queued task messages
	PERFORM cb_delete('t_' || s_r.task_name, message_id)
	FROM cb_step_runs s_r
	WHERE s_r.flow_run_id = cb_fail_step.flow_run_id
      AND s_r.message_id IS NOT NULL;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_complete_step(
    flow_run_id uuid,
    step_name text,
    output json
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
BEGIN

	-- ==========================================
	-- GUARD: No mutations on failed runs
	-- ==========================================
	IF EXISTS (SELECT 1 FROM cb_flow_runs WHERE id = cb_complete_step.flow_run_id AND status = 'failed') THEN
	  RETURN;
	END IF;


-- TODO
  -- Acquire locks first to prevent race conditions
-- SELECT * INTO v_run_record FROM pgflow.runs
-- WHERE pgflow.runs.run_id = complete_task.run_id
-- FOR UPDATE;

-- SELECT * INTO v_step_record FROM pgflow.step_states
-- WHERE pgflow.step_states.run_id = complete_task.run_id
--   AND pgflow.step_states.step_slug = complete_task.step_slug
-- FOR UPDATE;

	WITH
	-- complete step run
	step AS (
		UPDATE cb_step_runs s_r
	  	SET status = 'completed',
	      completed_at = now(),
          output = cb_complete_step.output
	  	WHERE s_r.flow_run_id = cb_complete_step.flow_run_id
	      AND s_r.step_name = cb_complete_step.step_name
	      AND s_r.status = 'started'
		RETURNING *
	),
  child_steps AS (
    SELECT deps.step_name AS child_step_name
    FROM cb_step_dependencies deps
    JOIN step parent_step ON parent_step.status = 'completed' AND deps.flow_name = parent_step.flow_name
    WHERE deps.dependency_name = cb_complete_step.step_name  -- dep_slug is the parent, step_slug is the child
    ORDER BY deps.step_name  -- Ensure consistent ordering
  ),
  -- Acquire locks on all child steps before updating them
  child_steps_lock AS (
    SELECT * FROM cb_step_runs s_r
    WHERE s_r.flow_run_id = cb_complete_step.flow_run_id
      AND s_r.step_name IN (SELECT child_step_name FROM child_steps)
    FOR UPDATE
  ),
  -- Decrement remaining_dependencies
  child_steps_update AS (
    UPDATE cb_step_runs s_r
    SET remaining_dependencies = s_r.remaining_dependencies - 1
      FROM child_steps children
    WHERE s_r.flow_run_id = cb_complete_step.flow_run_id
    AND s_r.step_name = children.child_step_name
  )
	UPDATE cb_flow_runs
	SET remaining_steps = remaining_steps - 1
	FROM step
	WHERE id = step.flow_run_id;

	-- delete queued task message
	PERFORM cb_delete('t_' || s_r.task_name, message_id)
	FROM cb_step_runs s_r
	WHERE s_r.flow_run_id = cb_complete_step.flow_run_id
      AND s_r.step_name = cb_complete_step.step_name;

	-- maybe complete flow run
    UPDATE cb_flow_runs
    SET status = 'completed',
        completed_at = now()
	WHERE id = cb_complete_step.flow_run_id
	  AND remaining_steps = 0
      AND status != 'completed';

  -- TODO only if needed
  PERFORM _cb_start_steps(cb_complete_step.flow_run_id);
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_complete_step;
DROP FUNCTION cb_fail_step;
DROP FUNCTION cb_run_flow;
DROP FUNCTION _cb_start_steps;
DROP FUNCTION cb_create_flow;
DROP TABLE cb_step_runs;
DROP TABLE cb_flow_runs;
DROP TABLE cb_step_dependencies;
DROP TABLE cb_steps;
DROP TABLE cb_flows;
