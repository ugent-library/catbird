-- SQL code is mostly taken or adapted from pgflow (https://github.com/pgflow-dev/pgflow)

-- +goose up

CREATE TABLE IF NOT EXISTS cb_tasks (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT name_not_empty CHECK (name <> '' )
);

CREATE TABLE IF NOT EXISTS cb_task_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deduplication_id text,
  task_name text NOT NULL REFERENCES cb_tasks (name),
  message_id bigint NOT NULL,
  status text NOT NULL DEFAULT 'started',
  output jsonb,
  error_message text,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  failed_at timestamptz,
  CONSTRAINT status_valid CHECK (status IN ('started', 'completed', 'failed')),
  CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
  CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
  CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
  CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
  CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
);

-- TODO what other indexes are needed? 
CREATE UNIQUE INDEX IF NOT EXISTS cb_task_runs_task_name_deduplication_id_idx ON cb_task_runs (task_name, deduplication_id) WHERE deduplication_id IS NOT NULL AND status = 'started';

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT name_not_empty CHECK (name <> '' )
);

CREATE TABLE IF NOT EXISTS cb_flow_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deduplication_id text,
  flow_name text NOT NULL REFERENCES cb_flows (name),
  status text NOT NULL DEFAULT 'started',
  input jsonb NOT NULL,
  error_message text,
  output jsonb,
  remaining_steps int NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  failed_at timestamptz,
  CONSTRAINT status_valid CHECK (status IN ('started', 'completed', 'failed')),
  CONSTRAINT remaining_steps_valid CHECK (remaining_steps >= 0),
  CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
  CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
  CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
  CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
  CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
);

CREATE INDEX IF NOT EXISTS cb_flow_runs_flow_name_idx ON cb_flow_runs (flow_name);
CREATE INDEX IF NOT EXISTS cb_flow_runs_status_idx ON cb_flow_runs (status);
CREATE UNIQUE INDEX IF NOT EXISTS cb_flow_runs_flow_name_deduplication_id_idx ON cb_flow_runs (flow_name, deduplication_id) WHERE deduplication_id IS NOT NULL AND status = 'started';

CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    idx int NOT NULL DEFAULT 0,
    dependency_count int NOT NULL DEFAULT 0,
    PRIMARY KEY (flow_name, name),
    UNIQUE (flow_name, idx),
    CONSTRAINT name_valid CHECK (name <> ''),
    CONSTRAINT idx_valid CHECK (idx >= 0),
    CONSTRAINT dependency_count_valid CHECK (dependency_count >= 0)
);

CREATE TABLE IF NOT EXISTS cb_step_dependencies (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    step_name text NOT NULL,
    dependency_name text NOT NULL,
    idx int NOT NULL DEFAULT 0,
    PRIMARY KEY (flow_name, step_name, idx),
    UNIQUE (flow_name, step_name, dependency_name),
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name),
    FOREIGN KEY (flow_name, dependency_name) REFERENCES cb_steps (flow_name, name),
    CONSTRAINT dependency_name_is_different CHECK (dependency_name != step_name),
    CONSTRAINT idx_valid CHECK (idx >= 0)
);

CREATE INDEX IF NOT EXISTS cb_step_dependencies_step_fk ON cb_step_dependencies (flow_name, step_name);
CREATE INDEX IF NOT EXISTS cb_step_dependencies_dependency_name_fk ON cb_step_dependencies (flow_name, dependency_name);

-- -- TODO indexes
CREATE TABLE IF NOT EXISTS cb_step_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  flow_run_id uuid NOT NULL REFERENCES cb_flow_runs (id),
  flow_name text NOT NULL REFERENCES cb_flows (name),
  step_name text NOT NULL,
  status text NOT NULL DEFAULT 'created',
  message_id bigint,
  output jsonb,
  error_message text,
  remaining_dependencies int NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  failed_at timestamptz,
  FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name),
  CONSTRAINT status_valid CHECK (status IN ('created', 'started', 'completed', 'failed')),
  CONSTRAINT remaining_dependencies_valid CHECK (remaining_dependencies >= 0),
  CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
  CONSTRAINT started_at_is_after_created_at CHECK (started_at IS NULL OR started_at >= created_at),
  CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
  CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
  CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
  CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
);

CREATE INDEX IF NOT EXISTS cb_step_flow_run_fk on cb_step_runs (flow_run_id);
CREATE INDEX IF NOT EXISTS cb_step_runs_ready_idx on cb_step_runs (id, status, remaining_dependencies) WHERE status = 'created' AND remaining_dependencies = 0;

CREATE TABLE IF NOT EXISTS cb_workers (
  id uuid PRIMARY KEY,
  started_at timestamptz NOT NULL DEFAULT now(),
  last_heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cb_workers_last_heartbeat_at_idx on cb_workers (last_heartbeat_at);

CREATE TABLE IF NOT EXISTS cb_task_handlers (
  worker_id uuid NOT NULL REFERENCES cb_workers (id) ON DELETE CASCADE,
  task_name text NOT NULL REFERENCES cb_tasks (name),
  PRIMARY KEY (worker_id, task_name)
);

CREATE TABLE IF NOT EXISTS cb_step_handlers (
  worker_id uuid NOT NULL REFERENCES cb_workers (id) ON DELETE CASCADE,
  flow_name text NOT NULL,
  step_name text NOT NULL,
  FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name),
  PRIMARY KEY (worker_id, flow_name, step_name)
);

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_task(name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_t_' || lower(cb_create_task.name)));

    -- Return early if task already exists
    IF EXISTS (SELECT 1 FROM cb_tasks WHERE cb_tasks.name = cb_create_task.name) THEN
        RETURN;
    END IF;

    INSERT INTO cb_tasks (name) VALUES (cb_create_task.name);

    PERFORM cb_create_queue('t_' || cb_create_task.name);
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_run_task(name text, input jsonb, deduplication_id text = NULL)
RETURNS uuid
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
  _id uuid = gen_random_uuid();
BEGIN
  WITH task_run AS (
    INSERT INTO cb_task_runs (id, deduplication_id, task_name, message_id)
    VALUES (
      _id,
      cb_run_task.deduplication_id,
      cb_run_task.name,
      cb_send(
        queue => 't_' || cb_run_task.name,
        deduplication_id => cb_run_task.deduplication_id,
        payload => jsonb_build_object(
          'id', _id,
          'input', cb_run_task.input
        )
      )
    )
    ON CONFLICT (task_name, deduplication_id) WHERE deduplication_id IS NOT NULL AND status = 'started' DO NOTHING
    RETURNING id
  )

  SELECT id FROM task_run
  UNION ALL
  SELECT id FROM cb_task_runs t_r
  WHERE t_r.deduplication_id = cb_run_task.deduplication_id
    AND t_r.status = 'started'
  INTO _id;

  RETURN _id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_complete_task(id uuid, output jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  -- delete queued task message
  PERFORM cb_delete('t_' || task_name, message_id)
  FROM cb_task_runs t_r
  WHERE t_r.id = cb_complete_task.id
    AND t_r.status = 'started';

	-- complete task run
	UPDATE cb_task_runs t_r
  SET status = 'completed',
      completed_at = now(),
      output = cb_complete_task.output
  WHERE t_r.id = cb_complete_task.id
    AND t_r.status = 'started';
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_fail_task(id uuid, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  -- delete queued task message
  PERFORM cb_delete('t_' || task_name, message_id)
  FROM cb_task_runs t_r
  WHERE t_r.id = cb_fail_task.id
    AND t_r.status = 'started';

	-- fail task run
	UPDATE cb_task_runs t_r
  SET status = 'failed',
      failed_at = now(),
      error_message = cb_fail_task.error_message
  WHERE t_r.id = cb_fail_task.id
    AND t_r.status = 'started';
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_flow(name text, steps jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _idx int = 0;
  _step jsonb;
  _existing_steps jsonb;
  _dep_idx int;
  _dep_name text;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext('cb_f_' || lower(cb_create_flow.name)));

  -- Check if flow already exists
  IF EXISTS (SELECT 1 FROM cb_flows WHERE cb_flows.name = cb_create_flow.name) THEN
    -- Reconstruct existing steps to compare
    _existing_steps := (
      SELECT jsonb_agg(
        jsonb_build_object(
          'name', s.name,
          'depends_on', COALESCE(
            (SELECT jsonb_agg(sd.dependency_name ORDER BY sd.idx)
             FROM cb_step_dependencies sd
             WHERE sd.flow_name = s.flow_name AND sd.step_name = s.name),
            '[]'::jsonb
          )
        )
        ORDER BY s.idx
      )
      FROM cb_steps s
      WHERE s.flow_name = cb_create_flow.name
    );

    -- Compare steps - raise exception if they differ
    IF _existing_steps IS DISTINCT FROM steps THEN
      RAISE EXCEPTION 'Flow "%" already exists with different steps', cb_create_flow.name;
    END IF;

    RETURN;
  END IF;

  INSERT INTO cb_flows (name) VALUES (cb_create_flow.name);

  FOR _step IN SELECT * FROM jsonb_array_elements(steps)
  LOOP
    INSERT INTO cb_steps (flow_name, name, idx, dependency_count)
    VALUES (
      cb_create_flow.name,
      _step->>'name',
      _idx,
      jsonb_array_length(coalesce(_step->'depends_on', '[]'::jsonb))
    );

		_dep_idx := 0;
		FOR _dep_name IN SELECT jsonb_array_elements_text(coalesce(_step->'depends_on', '[]'::jsonb))
		LOOP
			INSERT INTO cb_step_dependencies (flow_name, step_name, dependency_name, idx)
			VALUES (cb_create_flow.name, _step->>'name', _dep_name, _dep_idx);
			_dep_idx := _dep_idx + 1;
		END LOOP;

    PERFORM cb_create_queue('f_' || cb_create_flow.name || '_' || (_step->>'name'));

    _idx = _idx + 1;
  END LOOP;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_run_flow(name text, input jsonb, deduplication_id text = NULL)
RETURNS uuid
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
  _id uuid = gen_random_uuid();
BEGIN
  WITH
  -- gather flow steps
  flow_steps AS (
    SELECT *
    FROM cb_steps s
    WHERE s.flow_name = cb_run_flow.name
  ),
  -- create flow run
  flow_run AS (
    INSERT INTO cb_flow_runs (id, deduplication_id, flow_name, input, remaining_steps)
    VALUES (
      _id,
      cb_run_flow.deduplication_id,
      cb_run_flow.name,
      cb_run_flow.input,
      (SELECT count(*) FROM flow_steps)
    )
    ON CONFLICT (flow_name, deduplication_id) WHERE deduplication_id IS NOT NULL AND status = 'started' DO NOTHING
    RETURNING id
  ),
  -- create step runs
  step_runs AS (
    INSERT INTO cb_step_runs (flow_name, flow_run_id, step_name, remaining_dependencies)
    SELECT
      s.flow_name,
      (SELECT flow_run.id FROM flow_run),
      s.name,
      s.dependency_count
    FROM flow_steps s
  )
  -- get flow run id
  SELECT id FROM flow_run
  UNION ALL
  SELECT id FROM cb_flow_runs f_r
  WHERE f_r.deduplication_id = cb_run_flow.deduplication_id
    AND f_r.status = 'started'
  INTO _id;

  -- start steps with no dependencies
  PERFORM _cb_start_steps(_id);

  RETURN _id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_start_steps(flow_run_id uuid)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  WITH flow_run AS (
    SELECT f_r.id, f_r.input
    FROM cb_flow_runs f_r
    WHERE f_r.id = _cb_start_steps.flow_run_id
    AND f_r.status = 'started'
  )

  UPDATE cb_step_runs s_r
  SET status = 'started',
      started_at = now(),
      message_id = cb_send(
        queue   => 'f_' || s_r.flow_name || '_' || s_r.step_name,
        payload => jsonb_build_object(
          'id', s_r.id,
          'flow_input', flow_run.input,
          'step_outputs', coalesce((
            SELECT jsonb_object_agg(o.step_name, o.output)
            FROM (
              SELECT dep_runs.step_name, dep_runs.output
              FROM cb_step_dependencies deps
              JOIN cb_step_runs dep_runs ON dep_runs.flow_run_id = flow_run.id AND dep_runs.step_name = deps.dependency_name
              WHERE deps.flow_name = dep_runs.flow_name
                AND deps.step_name = s_r.step_name
            ) o
          ), '{}'::jsonb)
        )
      )
  FROM flow_run
  WHERE s_r.flow_run_id = flow_run.id
    AND s_r.status = 'created'
    AND s_r.remaining_dependencies = 0;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_complete_step(id uuid, output jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _flow_run_id uuid;
  _flow_run_record cb_flow_runs%ROWTYPE;
BEGIN
  -- delete queued task message
  PERFORM cb_delete('f_' || s_r.flow_name || '_' || s_r.step_name, s_r.message_id)
  FROM cb_step_runs s_r
  WHERE s_r.id = cb_complete_step.id
    AND s_r.status = 'started';

	-- complete step run
	UPDATE cb_step_runs s_r
  SET status = 'completed',
      completed_at = now(),
      output = cb_complete_step.output
  WHERE s_r.id = cb_complete_step.id
    AND s_r.status = 'started'
  RETURNING flow_run_id
  INTO _flow_run_id;

  -- lock flow run
  SELECT * INTO _flow_run_record
  FROM cb_flow_runs f_r
  WHERE f_r.id = _flow_run_id
  FOR UPDATE;

  IF _flow_run_record.status <> 'started' THEN
    RETURN;
  END IF;

  -- decrement flow_run remaining_steps
  UPDATE cb_flow_runs f_r
  SET remaining_steps = remaining_steps - 1
  WHERE f_r.id = _flow_run_id
    AND f_r.status = 'started';

  -- decrement dependent step run remaining_dependencies
  WITH dependent_steps AS (
    SELECT s_d.step_name
    FROM cb_step_dependencies s_d
    JOIN cb_step_runs s_r ON s_r.flow_run_id = _flow_run_id AND s_r.step_name = s_d.dependency_name
    WHERE s_r.id = cb_complete_step.id
      AND s_r.status = 'completed'
  ),
  dependent_step_runs_lock AS (
    SELECT * FROM cb_step_runs s_r
    WHERE s_r.flow_run_id = _flow_run_id
      AND s_r.step_name IN (SELECT step_name FROM dependent_steps)
    FOR UPDATE
  )
  UPDATE cb_step_runs s_r
  SET remaining_dependencies = s_r.remaining_dependencies - 1
  FROM dependent_steps
  WHERE s_r.flow_run_id = _flow_run_id
  AND s_r.step_name = dependent_steps.step_name;

  -- maybe complete flow run
  UPDATE cb_flow_runs f_r
  SET status = 'completed',
      completed_at = now(),
      output = (
        SELECT jsonb_object_agg(o.step_name, o.output)
        FROM (
          SELECT s_r.step_name, s_r.output
          FROM cb_step_runs s_r
          WHERE s_r.flow_run_id = _flow_run_id
        ) o
      )
  WHERE f_r.id = _flow_run_id
    AND f_r.remaining_steps = 0
    AND f_r.status = 'started';

  -- start steps with no dependencies
  PERFORM _cb_start_steps(_flow_run_id);
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_fail_step(id uuid, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _flow_run_id uuid;
BEGIN
  -- delete queued task message
  PERFORM cb_delete('f_' || s_r.flow_name || '_' || s_r.step_name, s_r.message_id)
  FROM cb_step_runs s_r
  WHERE s_r.id = cb_fail_step.id
    AND s_r.status = 'started';

	-- fail step run
	UPDATE cb_step_runs s_r
  SET status = 'failed',
      failed_at = now(),
      error_message = cb_fail_step.error_message
  WHERE s_r.id = cb_fail_step.id
    AND s_r.status = 'started'
  RETURNING s_r.flow_run_id
  INTO _flow_run_id;

  -- fail flow run
  UPDATE cb_flow_runs f_r
  SET status = 'failed',
      failed_at = now(),
      error_message = cb_fail_step.error_message
  WHERE f_r.id = _flow_run_id
    AND f_r.status = 'started';

  -- delete task messages for remaining steps in the flow run
  PERFORM cb_delete_many('f_' || s_r.flow_name || '_' || s_r.step_name, array_agg(s_r.message_id))
  FROM cb_step_runs s_r
  WHERE s_r.flow_run_id = _flow_run_id
    AND s_r.status IN ('created', 'started')
  GROUP BY s_r.flow_name, s_r.step_name
  HAVING COUNT(s_r.message_id) > 0;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_worker_started(id uuid, task_handlers jsonb, step_handlers jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _handler jsonb;
  _flow_name text;
  _step_name text;
  _task_name text;
BEGIN
    INSERT INTO cb_workers (id) VALUES (cb_worker_started.id);

    -- Insert task handlers
    FOR _handler IN SELECT jsonb_array_elements(cb_worker_started.task_handlers)
    LOOP
        -- Validate task handler has required key
        IF NOT (_handler ? 'task_name') THEN
            RAISE EXCEPTION 'Task handler missing required key: task_name';
        END IF;

        _task_name := _handler->>'task_name';

        -- Validate value is non-empty string
        IF _task_name IS NULL OR _task_name = '' THEN
            RAISE EXCEPTION 'Task handler task_name must be a non-empty string';
        END IF;

        INSERT INTO cb_task_handlers (worker_id, task_name)
        VALUES (cb_worker_started.id, _task_name);
    END LOOP;

    -- Insert flow handlers
    FOR _handler IN SELECT jsonb_array_elements(cb_worker_started.step_handlers)
    LOOP
        -- Validate flow handler has required keys
        IF NOT (_handler ? 'flow_name' AND _handler ? 'step_name') THEN
            RAISE EXCEPTION 'Flow handler missing required keys: flow_name, step_name';
        END IF;

        _flow_name := _handler->>'flow_name';
        _step_name := _handler->>'step_name';

        -- Validate values are non-empty strings
        IF _flow_name IS NULL OR _flow_name = '' THEN
            RAISE EXCEPTION 'Flow handler flow_name must be a non-empty string';
        END IF;
        IF _step_name IS NULL OR _step_name = '' THEN
            RAISE EXCEPTION 'Flow handler step_name must be a non-empty string';
        END IF;

        INSERT INTO cb_step_handlers (worker_id, flow_name, step_name)
        VALUES (cb_worker_started.id, _flow_name, _step_name);
    END LOOP;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_worker_heartbeat(id uuid)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
BEGIN
    UPDATE cb_workers
    SET last_heartbeat_at = now()
    WHERE cb_workers.id = cb_worker_heartbeat.id;
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_worker_started;
DROP FUNCTION cb_worker_heartbeat;
DROP FUNCTION cb_create_flow;
DROP FUNCTION cb_run_flow;
DROP FUNCTION cb_complete_step;
DROP FUNCTION cb_fail_step;
DROP FUNCTION cb_create_task;
DROP FUNCTION cb_run_task;
DROP FUNCTION cb_complete_task;
DROP FUNCTION cb_fail_task;
DROP FUNCTION _cb_start_steps;

DROP TABLE cb_task_handlers;
DROP TABLE cb_step_handlers;
DROP TABLE cb_workers;
DROP TABLE cb_task_runs;
DROP TABLE cb_step_runs;
DROP TABLE cb_flow_runs;
DROP TABLE cb_step_dependencies;
DROP TABLE cb_steps;
DROP TABLE cb_flows;
DROP TABLE cb_tasks;
