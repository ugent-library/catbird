-- SQL code is mostly taken or adapted from pgflow (https://github.com/pgflow-dev/pgflow)

-- +goose up

CREATE TABLE IF NOT EXISTS cb_tasks (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT name_not_empty CHECK (name <> '' )
);

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT name_not_empty CHECK (name <> '' )
);

CREATE TABLE IF NOT EXISTS cb_flow_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  flow_name text NOT NULL REFERENCES cb_flows (name),
  status text NOT NULL DEFAULT 'started',
  input json NOT NULL,
  output json,
  remaining_steps int NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  failed_at timestamptz,
  CONSTRAINT remaining_steps_valid CHECK (remaining_steps >= 0),
  CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
  CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
  CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
  CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
  CONSTRAINT status_valid CHECK (status IN ('started', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS cb_flow_runs_flow_name_idx ON cb_flow_runs (flow_name);
CREATE INDEX IF NOT EXISTS cb_flow_runs_status_idx ON cb_flow_runs (status);

CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    task_name text NOT NULL REFERENCES cb_tasks (name),
    idx int NOT NULL DEFAULT 0,
    map text,
    dependency_name_count int NOT NULL DEFAULT 0,
    PRIMARY KEY (flow_name, name),
    UNIQUE (flow_name, idx),
    FOREIGN KEY (flow_name, map) REFERENCES cb_steps (flow_name, name),
    CONSTRAINT name_not_empty CHECK (name <> '' ),
    CONSTRAINT idx_valid CHECK (idx >= 0),
    CONSTRAINT map_is_different CHECK (map IS NULL OR map != name),
    CONSTRAINT dependency_name_count_valid CHECK (dependency_name_count >= 0)
);

CREATE TABLE IF NOT EXISTS cb_step_dependencies (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    step_name text NOT NULL,
    dependency_name text NOT NULL,
    PRIMARY KEY (flow_name, step_name, dependency_name),
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name),
    FOREIGN KEY (flow_name, dependency_name) REFERENCES cb_steps (flow_name, name),
    CONSTRAINT dependency_name_is_different CHECK (dependency_name != step_name)
);

CREATE INDEX IF NOT EXISTS cb_step_dependencies_step_fk ON cb_step_dependencies (flow_name, step_name);
CREATE INDEX IF NOT EXISTS cb_step_dependencies_dependency_name_fk ON cb_step_dependencies (flow_name, dependency_name);

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_step_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  flow_run_id uuid NOT NULL REFERENCES cb_flow_runs (id),
  flow_name text NOT NULL REFERENCES cb_flows (name),
  step_name text NOT NULL,
  task_name text NOT NULL REFERENCES cb_tasks (name),
  map text,
  status text NOT NULL DEFAULT 'created',
  error_message text,
  remaining_dependencies int NOT NULL DEFAULT 0,
  remaining_tasks int,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  failed_at timestamptz,
  FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name),
  CONSTRAINT map_is_different CHECK (map IS NULL OR map != step_name),
  CONSTRAINT status_is_valid cHECK (status IN ('created', 'started', 'completed', 'failed')),
  CONSTRAINT remaining_dependencies_valid CHECK (remaining_dependencies >= 0),
  CONSTRAINT remaining_tasks_valid CHECK (remaining_tasks >= 0),
  CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
  CONSTRAINT started_at_is_after_created_at CHECK (started_at IS NULL OR started_at >= created_at),
  CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
  CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
  CONSTRAINT created_or_remaining_tasks CHECK (NOT (status = 'created' AND remaining_tasks IS NOT NULL)),
  CONSTRAINT completed_or_remaining_tasks CHECK (NOT (status = 'completed' AND remaining_tasks > 0)),
  CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
);

CREATE INDEX IF NOT EXISTS cb_step_flow_run_fk on cb_step_runs (flow_run_id);
CREATE INDEX IF NOT EXISTS cb_step_runs_ready_idx on cb_step_runs (id, status, remaining_dependencies) WHERE status = 'created' AND remaining_dependencies = 0;

CREATE TABLE IF NOT EXISTS cb_task_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deduplication_id text, -- always null if task is part of a flow
  task_name text NOT NULL REFERENCES cb_tasks (name),
  step_run_id uuid REFERENCES cb_step_runs (id), -- not null if task is part of a flow
  idx int,  -- not null if task is part of a flow
  message_id bigint NOT NULL,
  status text NOT NULL DEFAULT 'started',
  output json,
  error_message text,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  failed_at timestamptz,
  CONSTRAINT step_run_and_idx CHECK ((step_run_id IS NULL AND idx IS NULL) OR (step_run_id IS NOT NULL AND idx IS NOT NULL AND idx >= 0)),
  CONSTRAINT status_is_valid CHECK (status IN ('started', 'completed', 'failed')),
  CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
  CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
  CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
  CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
  CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
);

-- TODO what other indexes are needed? 
CREATE UNIQUE INDEX IF NOT EXISTS cb_task_runs_task_name_deduplication_id_idx ON cb_task_runs (task_name, deduplication_id) WHERE deduplication_id IS NOT NULL AND status = 'started';
CREATE INDEX IF NOT EXISTS cb_task_runs_step_run_fk on cb_task_runs (step_run_id);

CREATE TABLE IF NOT EXISTS cb_workers (
  id uuid PRIMARY KEY,
  started_at timestamptz NOT NULL DEFAULT now(),
  last_heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cb_workers_last_heartbeat_at_idx on cb_workers (last_heartbeat_at);

CREATE TABLE IF NOT EXISTS cb_worker_tasks (
  worker_id uuid NOT NULL REFERENCES cb_workers (id) ON DELETE CASCADE,
  task_name text NOT NULL REFERENCES cb_tasks (name),
  PRIMARY KEY (worker_id, task_name)
);

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_task(name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO cb_tasks (name)
    VALUES (
        cb_create_task.name
    )
    ON CONFLICT DO NOTHING;

    PERFORM cb_create_queue('t_' || cb_create_task.name);
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_run_task(
  name text,
  input json,
  deduplication_id text = NULL
)
RETURNS uuid
LANGUAGE plpgsql AS $$
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
        payload => json_build_object(
          'run_id', _id,
          'input', cb_run_task.input
        )
      )
    )
    ON CONFLICT DO NOTHING
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
CREATE OR REPLACE FUNCTION cb_complete_task(run_id uuid, output json)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _step_run_id uuid;
  _flow_run_id uuid;
  _flow_run_record cb_flow_runs%ROWTYPE;
BEGIN
  -- delete queued task message
  PERFORM cb_delete('t_' || task_name, message_id)
  FROM cb_task_runs
  WHERE id = cb_complete_task.run_id
    AND status = 'started';

	-- complete task run
	UPDATE cb_task_runs
  SET status = 'completed',
      completed_at = now(),
      output = cb_complete_task.output
  WHERE id = cb_complete_task.run_id
    AND status = 'started'
  RETURNING step_run_id
  INTO _step_run_id;

  -- maybe complete step and flow run if part of a flow
  IF _step_run_id IS NOT NULL THEN
    SELECT flow_run_id
    FROM cb_step_runs
    WHERE id = _step_run_id
    INTO _flow_run_id;

    -- lock flow run
    SELECT * INTO _flow_run_record
    FROM cb_flow_runs
    WHERE id = _flow_run_id
    FOR UPDATE;

    -- return if flow run has already failed
    IF _flow_run_record.status = 'failed' THEN
      RETURN;
    END IF;

    WITH
    step_run_lock AS (
      SELECT *
      FROM cb_step_runs
      WHERE id = _step_run_id
      FOR UPDATE
    ),
    -- maybe complete step run
    step_run AS (
      UPDATE cb_step_runs s_r
        SET status = CASE WHEN s_r.remaining_tasks = 1 THEN 'completed' ELSE 'started' END,
            completed_at = CASE WHEN s_r.remaining_tasks = 1 THEN now() ELSE NULL END,
            remaining_tasks = s_r.remaining_tasks - 1
        WHERE s_r.id = _step_run_id
          AND s_r.status = 'started'
        RETURNING s_r.*
    ),
    dependent_step_runs AS (
      SELECT s_d.step_name AS dependent_step_name
      FROM cb_step_dependencies s_d
      JOIN step_run ON step_run.status = 'completed' AND s_d.flow_name = step_run.flow_name
      WHERE s_d.dependency_name = step_run.step_name
      ORDER BY s_d.step_name  -- ensure consistent ordering
    ),
    dependent_step_runs_lock AS (
      SELECT * FROM cb_step_runs s_r, step_run
      WHERE s_r.flow_run_id = step_run.flow_run_id
        AND s_r.step_name IN (SELECT dependent_step_name FROM dependent_step_runs)
      FOR UPDATE
    ),
    -- decrement step run remaining_dependencies
    dependent_step_runs_update AS (
      UPDATE cb_step_runs s_r
      SET remaining_dependencies = s_r.remaining_dependencies - 1
      FROM dependent_step_runs, step_run
      WHERE s_r.flow_run_id = step_run.flow_run_id
      AND s_r.step_name = dependent_step_runs.dependent_step_name
    )
    -- decrement flow_run remaining_steps
    UPDATE cb_flow_runs f_r
    SET remaining_steps = remaining_steps - 1
    FROM step_run
    WHERE f_r.id = step_run.flow_run_id
      AND step_run.status = 'completed';

    -- maybe complete flow run
    UPDATE cb_flow_runs
    SET status = 'completed',
        completed_at = now(),
        output = (
          SELECT json_object_agg(o.step_name, o.output)
          FROM (
            SELECT s_r.step_name, (CASE WHEN s_r.map IS NOT NULL THEN json_agg(t_r.output ORDER BY t_r.idx) ELSE any_value(t_r.output) END) as output
            FROM cb_task_runs t_r
            JOIN cb_step_runs s_r ON s_r.id = t_r.step_run_id
            WHERE s_r.flow_run_id = _flow_run_id
            GROUP BY s_r.step_name, s_r.map
          ) o
        )
    WHERE id = _flow_run_id
      AND remaining_steps = 0
      AND status != 'completed';

    -- start steps with no dependencies
    PERFORM _cb_start_steps(_flow_run_id);
  END IF;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_fail_task(run_id uuid, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _step_run_id uuid;
  _flow_run_id uuid;
BEGIN
  -- delete queued task message
  PERFORM cb_delete('t_' || task_name, message_id)
  FROM cb_task_runs
  WHERE id = cb_fail_task.run_id
    AND status = 'started';

	-- fail task run
	UPDATE cb_task_runs
  SET status = 'failed',
      failed_at = now(),
      error_message = cb_fail_task.error_message
  WHERE id = cb_fail_task.run_id
    AND status = 'started'
  RETURNING step_run_id
  INTO _step_run_id;

  -- fail step and flow run if part of a flow
  IF _step_run_id IS NOT NULL THEN
    SELECT flow_run_id
    FROM cb_step_runs
    WHERE id = _step_run_id
    INTO _flow_run_id;

    WITH
    flow_run_lock AS (
      SELECT *
      FROM cb_flow_runs
      WHERE id = _flow_run_id
      FOR UPDATE
    ),
    step_run_lock AS (
      SELECT *
      FROM cb_step_runs
      WHERE id = _step_run_id
      FOR UPDATE
    ),
    -- fail step run
    step_run AS (
    	UPDATE cb_step_runs s_r
  	  SET status = 'failed',
          failed_at = now(),
          error_message = cb_fail_task.error_message
      WHERE s_r.id = _step_run_id
        AND s_r.status = 'started'
    )
    -- fail flow run
    UPDATE cb_flow_runs f_r
    SET status = 'failed',
        failed_at = now()
    WHERE f_r.id = _flow_run_id
      AND f_r.status = 'started';

    -- delete all queued task messages for remaining tasks in the flow run
    PERFORM cb_delete_many('t_' || t_r.task_name, array_agg(t_r.message_id))
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
CREATE OR REPLACE FUNCTION cb_create_flow(name text, steps json)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _idx int = 0;
  _step json;
BEGIN
  INSERT INTO cb_flows (name)
  VALUES (cb_create_flow.name)
  ON CONFLICT DO NOTHING;

  FOR _step IN SELECT * FROM json_array_elements(steps)
  LOOP
    IF _step->>'name' = cb_create_flow.name THEN
      RAISE EXCEPTION 'cb: step name %s cannot be the same as flow name', _step->>'name';
    END IF;

    INSERT INTO cb_steps (flow_name, name, task_name, idx, map, dependency_name_count)
    VALUES (
      cb_create_flow.name,
      _step->>'name',
      coalesce(_step->>'task_name', _step->>'name'),
      _idx,
      _step->>'map',
      coalesce(json_array_length(_step->'depends_on'), 0)
    )
    ON CONFLICT DO NOTHING;

		INSERT INTO cb_step_dependencies (flow_name, step_name, dependency_name)
		SELECT cb_create_flow.name, _step->>'name', depends_on
		FROM json_array_elements_text(coalesce(_step->'depends_on', '[]'::json)) AS depends_on
		ON CONFLICT DO NOTHING;

    PERFORM cb_create_queue('t_' || coalesce(_step->>'task_name', _step->>'name'));

    _idx = _idx + 1;
  END LOOP;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_run_flow(name text, input json)
RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
    _id uuid;
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
    INSERT INTO cb_flow_runs (flow_name, input, remaining_steps)
    VALUES (
      cb_run_flow.name,
      cb_run_flow.input,
      (SELECT count(*) FROM flow_steps)
    )
    RETURNING *
  ),
  -- create step runs
  step_runs AS (
    INSERT INTO cb_step_runs (flow_name, flow_run_id, step_name, task_name, map, remaining_dependencies)
    SELECT
      s.flow_name,
      (SELECT flow_run.id FROM flow_run),
      s.name,
      s.task_name,
      s.map,
      s.dependency_name_count
    FROM flow_steps s
  )
  -- get flow run id
  SELECT id FROM flow_run
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
  -- return if flow run has already completed or failed
  IF EXISTS (SELECT 1 FROM cb_flow_runs f_r WHERE f_r.id = _cb_start_steps.flow_run_id AND f_r.status IN ('completed', 'failed')) THEN
    RETURN;
  END IF;

  WITH
  flow_run AS (
    SELECT f_r.id, f_r.input
    FROM cb_flow_runs f_r
    WHERE f_r.id = _cb_start_steps.flow_run_id
  ),
  step_run_outputs AS (
    SELECT json_object_agg(o.step_name, o.output) as outputs
    FROM (
      SELECT s_r.step_name, (CASE WHEN s_r.map IS NOT NULL THEN json_agg(t_r.output ORDER BY t_r.idx) ELSE any_value(t_r.output) END) AS output
      FROM cb_task_runs t_r
      JOIN cb_step_runs s_r ON s_r.id = t_r.step_run_id
      WHERE s_r.flow_run_id = _cb_start_steps.flow_run_id
        AND s_r.status = 'completed'
      GROUP BY s_r.step_name, s_r.map
    ) o
  ),
  ready_step_runs AS (
    SELECT *
    FROM cb_step_runs AS s_r
    WHERE s_r.flow_run_id = _cb_start_steps.flow_run_id
      AND s_r.status = 'created'
      AND s_r.remaining_dependencies = 0
    ORDER BY s_r.step_name
    FOR UPDATE
  ),
  start_step_runs AS (
    UPDATE cb_step_runs
    SET status = 'started',
        started_at = now(),
        remaining_tasks = (
          -- TODO use json_array_length?
          CASE WHEN ready_step_runs.map IS NOT NULL
          THEN
            (
              SELECT COUNT(*)
              FROM json_array_elements(
                coalesce(
                  (SELECT step_run_outputs.outputs->ready_step_runs.map),
                  '[]'::json
                )
              ) AS elem
            )
          ELSE
            1
          END
        )
    FROM ready_step_runs, flow_run, step_run_outputs
    WHERE cb_step_runs.flow_run_id = _cb_start_steps.flow_run_id
    AND cb_step_runs.step_name = ready_step_runs.step_name
  )

  INSERT INTO cb_task_runs (id, step_run_id, task_name, idx, message_id)
  SELECT t.task_id,
         t.step_run_id,
         t.task_name,
         t.idx,
         cb_send(
           queue => 't_' || t.task_name,
           payload => json_build_object(
             'run_id', t.task_id,
             'input', json_build_object(
               'idx', t.idx,
               'flow', flow_run.input,
               'steps', coalesce(step_run_outputs.outputs, '{}'::json)
             )
           )
         )
  FROM flow_run, 
  	   step_run_outputs,
  	   (SELECT id AS step_run_id, task_name, task_indexes.task_index AS idx, gen_random_uuid() AS task_id
  	    FROM ready_step_runs, step_run_outputs
  	    CROSS JOIN LATERAL generate_series(
          0,
          (CASE
           WHEN ready_step_runs.map IS NOT NULL THEN json_array_length(step_run_outputs.outputs->ready_step_runs.map) - 1
           ELSE 0
           END)
         ) AS task_indexes(task_index)
  	   ) t;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_worker_started(id uuid, tasks text[])
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO cb_workers (id)
    VALUES (cb_worker_started.id)
    ON CONFLICT DO NOTHING;

    INSERT INTO cb_worker_tasks(worker_id, task_name)
    SELECT cb_worker_started.id, task
    FROM unnest(cb_worker_started.tasks) AS task
    ON CONFLICT DO NOTHING;
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
DROP FUNCTION cb_run_flow;
DROP FUNCTION cb_create_flow;
DROP FUNCTION cb_complete_task;
DROP FUNCTION cb_fail_task;
DROP FUNCTION cb_run_task;
DROP FUNCTION cb_create_task;
DROP FUNCTION _cb_start_steps;

DROP TABLE cb_worker_tasks;
DROP TABLE cb_workers;
DROP TABLE cb_task_runs;
DROP TABLE cb_step_runs;
DROP TABLE cb_flow_runs;
DROP TABLE cb_step_dependencies;
DROP TABLE cb_steps;
DROP TABLE cb_flows;
DROP TABLE cb_tasks;
