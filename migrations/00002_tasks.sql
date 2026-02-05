-- SQL code is taken from or inspired by pgflow (https://github.com/pgflow-dev/pgflow)

-- +goose up

-- +goose statementbegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cb_task_message') THEN
        CREATE TYPE cb_task_message AS (
            id bigint,
            deduplication_id text,
            input jsonb,
            deliveries int
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cb_step_message') THEN
        CREATE TYPE cb_step_message AS (
            id bigint,
            flow_run_id bigint,
            step_name text,
            deliveries int,
            flow_input jsonb,
            step_outputs jsonb
        );
    END IF;
END$$;
-- +goose statementend

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

CREATE TABLE IF NOT EXISTS cb_workers (
  id uuid PRIMARY KEY,
  started_at timestamptz NOT NULL DEFAULT now(),
  last_heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cb_workers_last_heartbeat_at_idx ON cb_workers (last_heartbeat_at);

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
-- cb_create_task: Create a task definition
-- Creates the task metadata and associated queue table for task runs
-- Parameters:
--   name: Task name (must be unique)
-- Returns: void
CREATE OR REPLACE FUNCTION cb_create_task(name text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := cb_table_name(cb_create_task.name, 't');
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_t_table));

    -- Return early if task already exists
    IF EXISTS (SELECT 1 FROM cb_tasks WHERE cb_tasks.name = cb_create_task.name) THEN
        RETURN;
    END IF;

    INSERT INTO cb_tasks (name)
    VALUES (
        cb_create_task.name
    );

    EXECUTE format(
      $QUERY$
      CREATE TABLE IF NOT EXISTS %I (
        id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        deduplication_id text,
        status text NOT NULL DEFAULT 'queued',
        deliveries int NOT NULL DEFAULT 0,
        input jsonb NOT NULL,
        output jsonb,
        error_message text,
        deliver_at timestamptz NOT NULL DEFAULT now(),
        started_at timestamptz NOT NULL DEFAULT now(),
        completed_at timestamptz,
        failed_at timestamptz,
        CONSTRAINT status_valid CHECK (status IN ('queued', 'started', 'completed', 'failed')),
        CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
        CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
        CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
        CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
        CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
      )
      $QUERY$,
      _t_table
    );

    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (deduplication_id) WHERE deduplication_id IS NOT NULL AND status IN (''queued'', ''started'')', _t_table || '_deduplication_id_idx', _t_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (deliver_at);', _t_table || '_deliver_at_idx', _t_table);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_run_task: Create a task run (enqueue a task execution)
-- Parameters:
--   name: Task name
--   input: JSON input data for the task
--   deduplication_id: Optional unique ID for deduplication (prevents duplicate executions)
-- Returns: bigint - the task run ID
CREATE OR REPLACE FUNCTION cb_run_task(name text, input jsonb, deduplication_id text = NULL)
RETURNS bigint
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    _t_table text := cb_table_name(cb_run_task.name, 't');
    _id bigint;
BEGIN
  -- See https://dba.stackexchange.com/questions/212580/concurrent-transactions-result-in-race-condition-with-unique-constraint-on-inser/213625#213625
  EXECUTE format(
    $QUERY$
    INSERT INTO %I (input, deduplication_id)
    VALUES ($1, $2)
    ON CONFLICT (deduplication_id) WHERE deduplication_id IS NOT NULL AND status IN ('queued', 'started') DO UPDATE
    SET deduplication_id = EXCLUDED.deduplication_id WHERE FALSE -- no-op to force a returning value
    RETURNING id
    $QUERY$,
    _t_table,
    _t_table
  )
  USING cb_run_task.input,
        cb_run_task.deduplication_id
  INTO _id;

  RETURN _id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_read_tasks: Read task runs from the queue
-- Parameters:
--   name: Task name
--   quantity: Number of task runs to read (must be > 0)
--   hide_for: Duration in milliseconds to hide task runs from other workers (must be > 0)
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: Set of cb_task_message records
CREATE OR REPLACE FUNCTION cb_read_tasks(
    name text,
    quantity int,
    hide_for int,
    poll_for int,
    poll_interval int
)
RETURNS SETOF cb_task_message
LANGUAGE plpgsql AS $$
DECLARE
    _m cb_task_message;
    _sleep_for double precision;
    _stop_at timestamp;
    _q text;
    _t_table text := cb_table_name(cb_read_tasks.name, 't');
BEGIN
    IF cb_read_tasks.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_read_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;
    IF cb_read_tasks.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;
    IF cb_read_tasks.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_read_tasks.poll_interval / 1000.0;

    IF _sleep_for >= cb_read_tasks.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_read_tasks.poll_for / 1000.0);

    _q := FORMAT(
        $QUERY$
        WITH runs AS (
          SELECT id
          FROM %I
          WHERE deliver_at <= clock_timestamp()
            AND status IN ('queued', 'started')
          ORDER BY id ASC
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE %I m
        SET status = 'started',
            started_at = clock_timestamp(),
            deliveries = deliveries + 1,
            deliver_at = clock_timestamp() + $2
        FROM runs
        WHERE m.id = runs.id
        RETURNING m.id,
                  m.deduplication_id,
                  m.input,
                  m.deliveries;
        $QUERY$,
        _t_table, _t_table
      );

    LOOP
      IF (SELECT clock_timestamp() >= _stop_at) THEN
        RETURN;
      END IF;

      FOR _m IN
        EXECUTE _q USING cb_read_tasks.quantity, make_interval(secs => cb_read_tasks.hide_for / 1000.0)
      LOOP
        RETURN NEXT _m;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_sleep(_sleep_for);
      END IF;
    END LOOP;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_hide_tasks: Hide task runs from being read by workers
-- Parameters:
--   name: Task name
--   ids: Array of task run IDs to hide
--   hide_for: Duration in milliseconds to hide the task runs (must be > 0)
-- Returns: void
CREATE OR REPLACE FUNCTION cb_hide_tasks(name text, ids bigint[], hide_for integer)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := cb_table_name(cb_hide_tasks.name, 't');
    _jitter_factor numeric := 0.1;
    _jittered_delay numeric;
BEGIN
    IF cb_hide_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    _jittered_delay := cb_hide_tasks.hide_for * (1 + (random() * 2 - 1) * _jitter_factor);

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET deliver_at = (clock_timestamp() + $2)
      WHERE id = any($1);
      $QUERY$,
      _t_table
    )
    USING cb_hide_tasks.ids,
          make_interval(secs => _jittered_delay / 1000.0);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_complete_task: Mark a task run as completed
-- Sets the task run status to 'completed' and stores the output
-- Parameters:
--   name: Task name
--   id: Task run ID
--   output: JSON output data from the task execution
-- Returns: void
CREATE OR REPLACE FUNCTION cb_complete_task(name text, id bigint, output jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := cb_table_name(cb_complete_task.name, 't');
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I t_r
      SET status = 'completed',
          completed_at = now(),
          output = $2
      WHERE t_r.id = $1
        AND t_r.status = 'started';
      $QUERY$,
      _t_table
    )
    USING cb_complete_task.id,
          cb_complete_task.output;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_fail_task: Mark a task run as failed
-- Sets the task run status to 'failed' and stores the error message
-- Parameters:
--   name: Task name
--   id: Task run ID
--   error_message: Description of the error that occurred
-- Returns: void
CREATE OR REPLACE FUNCTION cb_fail_task(name text, id bigint, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := cb_table_name(cb_fail_task.name, 't');
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I t_r
      SET status = 'failed',
          failed_at = now(),
          error_message = $2
      WHERE t_r.id = $1
        AND t_r.status = 'started';
      $QUERY$,
      _t_table
    )
    USING cb_fail_task.id,
          cb_fail_task.error_message;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_delete_task: Delete a task definition and all its runs
-- Removes the task metadata, handlers, and drops the associated queue table
-- Parameters:
--   name: Task name
-- Returns: boolean - true if task was deleted, false if not found
CREATE OR REPLACE FUNCTION cb_delete_task(name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  _t_table text := cb_table_name(cb_delete_task.name, 't');
  _res boolean;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext(_t_table));

  EXECUTE format('DROP TABLE IF EXISTS %I;', _t_table);

  DELETE FROM cb_task_handlers t
  WHERE t.task_name = cb_delete_task.name;

  DELETE FROM cb_tasks t
  WHERE t.name = cb_delete_task.name
  RETURNING true
  INTO _res;

  RETURN coalesce(_res, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_create_flow: Create a flow definition
-- Creates the flow metadata and associated tables for flow runs and steps
-- Parameters:
--   name: Flow name (must be unique)
--   steps: JSON array describing flow steps and their dependencies
-- Returns: void
CREATE OR REPLACE FUNCTION cb_create_flow(name text, steps jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _idx int := 0;
  _step jsonb;
  _step_name text;
  _existing_steps jsonb;
  _dep_idx int;
  _dep jsonb;
  _f_table text := cb_table_name(cb_create_flow.name, 'f');
  _s_table text := cb_table_name(cb_create_flow.name, 's');
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext(_f_table));

  -- Check if flow already exists
  IF EXISTS (SELECT 1 FROM cb_flows WHERE cb_flows.name = cb_create_flow.name) THEN
    -- Reconstruct existing steps to compare
    _existing_steps := (
      SELECT jsonb_agg(
        jsonb_build_object(
          'name', s.name,
          'depends_on', COALESCE(
            (SELECT jsonb_agg(jsonb_build_object('name', sd.dependency_name) ORDER BY sd.idx)
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
    IF _existing_steps IS DISTINCT FROM (
    SELECT jsonb_agg(
      CASE
        WHEN step_elem ? 'depends_on' THEN step_elem
        ELSE step_elem || jsonb_build_object('depends_on', '[]'::jsonb)
      END
    )
    FROM jsonb_array_elements(steps) AS step_elem
  ) THEN
      RAISE EXCEPTION 'Flow "%" already exists with different steps', cb_create_flow.name;
    END IF;

    RETURN;
  END IF;

  INSERT INTO cb_flows (name) VALUES (cb_create_flow.name);

  FOR _step IN SELECT jsonb_array_elements(steps)
  LOOP
    INSERT INTO cb_steps (flow_name, name, idx, dependency_count)
    VALUES (
      cb_create_flow.name,
      _step->>'name',
      _idx,
      jsonb_array_length(coalesce(_step->'depends_on', '[]'::jsonb))
    );

    -- TODO validate steps (e.g., dependencies refer to existing steps, no circular dependencies, etc.)

    _dep_idx := 0;
    FOR _dep IN SELECT jsonb_array_elements(coalesce(_step->'depends_on', '[]'::jsonb))
    LOOP
      INSERT INTO cb_step_dependencies (flow_name, step_name, dependency_name, idx)
      VALUES (cb_create_flow.name, _step->>'name', _dep->>'name', _dep_idx);
      _dep_idx := _dep_idx + 1;
    END LOOP;

    _idx := _idx + 1;
  END LOOP;

  EXECUTE format(
    $QUERY$
    CREATE TABLE IF NOT EXISTS %I (
      id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
      deduplication_id text,
      status text NOT NULL DEFAULT 'started',
      remaining_steps int NOT NULL DEFAULT 0,
      input jsonb NOT NULL,
      output jsonb,
      error_message text,
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
    )
    $QUERY$,
    _f_table
  );

  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (status);', _f_table || '_status_idx', _f_table);
  EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (deduplication_id) WHERE deduplication_id IS NOT NULL AND status = ''started''', _f_table || '_deduplication_id_idx', _f_table);

  -- Create single step runs table for this flow
  EXECUTE format(
    $QUERY$
    CREATE TABLE IF NOT EXISTS %I (
      id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
      flow_run_id bigint NOT NULL,
      step_name text NOT NULL,
      status text NOT NULL DEFAULT 'created',
      deliveries int NOT NULL DEFAULT 0,
      output jsonb,
      error_message text,
      remaining_dependencies int NOT NULL DEFAULT 0,
      deliver_at timestamptz NOT NULL DEFAULT now(),
      created_at timestamptz NOT NULL DEFAULT now(),
      started_at timestamptz,
      completed_at timestamptz,
      failed_at timestamptz,
      UNIQUE (flow_run_id, step_name),
      FOREIGN KEY (flow_run_id) REFERENCES %I (id),
      CONSTRAINT status_valid CHECK (status IN ('created', 'started', 'completed', 'failed')),
      CONSTRAINT remaining_dependencies_valid CHECK (remaining_dependencies >= 0),
      CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
      CONSTRAINT started_at_is_after_created_at CHECK (started_at IS NULL OR started_at >= created_at),
      CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
      CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
      CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
      CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
    )
    $QUERY$,
    _s_table,
    _f_table
  );

  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (flow_run_id);', _s_table || '_flow_run_fk', _s_table);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (step_name);', _s_table || '_step_name_idx', _s_table);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (deliver_at);', _s_table || '_deliver_at_idx', _s_table);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (status, remaining_dependencies) WHERE status = ''created'' AND remaining_dependencies = 0;', _s_table || '_ready_idx', _s_table);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (status);', _s_table || '_status_idx', _s_table);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_run_flow: Create a flow run (enqueue a flow execution)
-- Creates a flow run and initializes all step runs with dependency tracking
-- Parameters:
--   name: Flow name
--   input: JSON input data for the flow
--   deduplication_id: Optional unique ID for deduplication (prevents duplicate executions)
-- Returns: bigint - the flow run ID
CREATE OR REPLACE FUNCTION cb_run_flow(name text, input jsonb, deduplication_id text = NULL)
RETURNS bigint
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
  _f_table text := cb_table_name(cb_run_flow.name, 'f');
  _s_table text := cb_table_name(cb_run_flow.name, 's');
  _id bigint;
  _remaining_steps int;
BEGIN
  -- Count total steps
  SELECT count(*) INTO _remaining_steps
  FROM cb_steps s
  WHERE s.flow_name = cb_run_flow.name;

  -- Create flow run or get existing 'started' run with same deduplication_id
  -- Partial unique index ensures only one 'started' run per deduplication_id
  -- Completed/failed runs don't block new runs with same deduplication_id
  EXECUTE format(
    $QUERY$
    INSERT INTO %I (deduplication_id, input, remaining_steps, status)
    VALUES ($1, $2, $3, 'started')
    ON CONFLICT (deduplication_id) WHERE deduplication_id IS NOT NULL AND status = 'started' DO UPDATE
    SET deduplication_id = EXCLUDED.deduplication_id WHERE FALSE
    RETURNING id
    $QUERY$,
    _f_table
  )
  USING cb_run_flow.deduplication_id,
        cb_run_flow.input,
        _remaining_steps
  INTO _id;

  -- Create step runs for all steps in a single INSERT
  EXECUTE format(
    $QUERY$
    INSERT INTO %I (flow_run_id, step_name, status, remaining_dependencies)
    SELECT %L, s.name, 'created', s.dependency_count
    FROM cb_steps s
    WHERE s.flow_name = %L
    ON CONFLICT (flow_run_id, step_name) DO NOTHING
    $QUERY$,
    _s_table,
    _id,
    cb_run_flow.name
  );

  -- Start steps with no dependencies
  PERFORM cb_start_steps(cb_run_flow.name, _id);

  RETURN _id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_start_steps: Start steps in a flow that have no pending dependencies
-- Called automatically after step completion to start dependent steps
-- Parameters:
--   flow_name: Flow name
--   flow_run_id: Flow run ID
-- Returns: void
CREATE OR REPLACE FUNCTION cb_start_steps(flow_name text, flow_run_id bigint)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _f_table text := cb_table_name(cb_start_steps.flow_name, 'f');
  _s_table text := cb_table_name(cb_start_steps.flow_name, 's');
  _flow_input jsonb;
BEGIN
  -- Get flow input
  EXECUTE format(
    $QUERY$
    SELECT input FROM %I WHERE id = $1 AND status = 'started'
    $QUERY$,
    _f_table
  )
  USING cb_start_steps.flow_run_id
  INTO _flow_input;

  -- If flow not found or not started, return
  IF _flow_input IS NULL THEN
    RETURN;
  END IF;

  -- Start all steps with no dependencies
  EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'started',
        started_at = now(),
        deliver_at = now()
    WHERE flow_run_id = $1
      AND status = 'created'
      AND remaining_dependencies = 0
    $QUERY$,
    _s_table
  )
  USING cb_start_steps.flow_run_id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_read_steps: Read step runs from a flow
-- Parameters:
--   flow_name: Flow name
--   step_name: Step name within the flow
--   quantity: Number of step runs to read (must be > 0)
--   hide_for: Duration in milliseconds to hide step runs from other workers (must be > 0)
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: Set of cb_step_message records
CREATE OR REPLACE FUNCTION cb_read_steps(
    flow_name text,
    step_name text,
    quantity int,
    hide_for int,
    poll_for int,
    poll_interval int
)
RETURNS SETOF cb_step_message
LANGUAGE plpgsql AS $$
DECLARE
    _m cb_step_message;
    _sleep_for double precision;
    _stop_at timestamp;
    _q text;
    _f_table text := cb_table_name(cb_read_steps.flow_name, 'f');
    _s_table text := cb_table_name(cb_read_steps.flow_name, 's');
BEGIN
    IF cb_read_steps.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_read_steps.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;
    IF cb_read_steps.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;
    IF cb_read_steps.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_read_steps.poll_interval / 1000.0;

    IF _sleep_for >= cb_read_steps.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_read_steps.poll_for / 1000.0);

    _q := FORMAT(
        $QUERY$
        WITH runs AS (
          SELECT m.id, m.flow_run_id
          FROM %I m
          INNER JOIN %I f ON m.flow_run_id = f.id
          WHERE m.step_name = $1
            AND m.deliver_at <= clock_timestamp()
            AND m.status = 'started'
            AND f.status = 'started'
          ORDER BY m.id ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE %I m
        SET deliveries = deliveries + 1,
            deliver_at = clock_timestamp() + $3
        FROM runs
        WHERE m.id = runs.id
          AND EXISTS (SELECT 1 FROM %I f WHERE f.id = runs.flow_run_id AND f.status = 'started')
        RETURNING m.id,
                  m.flow_run_id,
                  m.step_name,
                  m.deliveries,
                  (SELECT input FROM %I f WHERE f.id = m.flow_run_id) AS flow_input,
                  (SELECT jsonb_object_agg(deps.step_name, deps.output)
                   FROM %I deps
                   WHERE deps.flow_run_id = m.flow_run_id
                     AND deps.step_name IN (
                       SELECT dependency_name
                       FROM cb_step_dependencies
                       WHERE flow_name = $4 AND step_name = m.step_name
                     )
                  ) AS step_outputs;
        $QUERY$,
        _s_table, _f_table, _s_table, _f_table, _f_table, _s_table
      );

    LOOP
      IF (SELECT clock_timestamp() >= _stop_at) THEN
        RETURN;
      END IF;

      FOR _m IN
        EXECUTE _q USING cb_read_steps.step_name, cb_read_steps.quantity, make_interval(secs => cb_read_steps.hide_for / 1000.0), cb_read_steps.flow_name
      LOOP
        RETURN NEXT _m;
      END LOOP;
      IF FOUND THEN
        RETURN;
      ELSE
        PERFORM pg_sleep(_sleep_for);
      END IF;
    END LOOP;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_hide_steps: Hide step runs from being read by workers
-- Parameters:
--   flow_name: Flow name
--   step_name: Step name within the flow
--   ids: Array of step run IDs to hide
--   hide_for: Duration in milliseconds to hide the step runs (must be > 0)
-- Returns: void
CREATE OR REPLACE FUNCTION cb_hide_steps(flow_name text, step_name text, ids bigint[], hide_for integer)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := cb_table_name(cb_hide_steps.flow_name, 's');
    _jitter_factor numeric := 0.1;
    _jittered_delay numeric;
BEGIN
    IF cb_hide_steps.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    _jittered_delay := cb_hide_steps.hide_for * (1 + (random() * 2 - 1) * _jitter_factor);

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET deliver_at = (clock_timestamp() + $2)
      WHERE id = any($1)
        AND step_name = $3;
      $QUERY$,
      _s_table
    )
    USING cb_hide_steps.ids,
          make_interval(secs => _jittered_delay / 1000.0),
          cb_hide_steps.step_name;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_complete_step: Mark a step run as completed
-- Sets the step run status to 'completed', decrements flow dependencies, and starts dependent steps
-- Parameters:
--   flow_name: Flow name
--   step_name: Step name within the flow
--   step_id: Step run ID
--   output: JSON output data from the step execution
-- Returns: void
CREATE OR REPLACE FUNCTION cb_complete_step(flow_name text, step_name text, step_id bigint, output jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _f_table text := cb_table_name(cb_complete_step.flow_name, 'f');
  _s_table text := cb_table_name(cb_complete_step.flow_name, 's');
  _flow_run_id bigint;
  _remaining int;
BEGIN
  -- Complete step run atomically - only succeeds if status='started'
  EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'completed',
        completed_at = now(),
        output = $2
    WHERE id = $1
      AND status = 'started'
    RETURNING flow_run_id
    $QUERY$,
    _s_table
  )
  USING cb_complete_step.step_id, cb_complete_step.output
  INTO _flow_run_id;

  -- If step wasn't in 'created' status, return early (already completed or failed)
  IF _flow_run_id IS NULL THEN
    RETURN;
  END IF;

  -- Atomically decrement remaining_steps and get new value
  EXECUTE format(
    $QUERY$
    UPDATE %I
    SET remaining_steps = remaining_steps - 1
    WHERE id = $1 AND status = 'started'
    RETURNING remaining_steps
    $QUERY$,
    _f_table
  )
  USING _flow_run_id
  INTO _remaining;

  -- If flow already completed/failed, return early
  IF _remaining IS NULL THEN
    RETURN;
  END IF;

  -- Decrement dependent step run remaining_dependencies atomically
  EXECUTE format(
    $QUERY$
    UPDATE %I
    SET remaining_dependencies = remaining_dependencies - 1
    WHERE flow_run_id = $1
      AND step_name IN (SELECT step_name FROM cb_step_dependencies WHERE flow_name = $2 AND dependency_name = $3)
      AND status = 'created'
    $QUERY$,
    _s_table
  )
  USING _flow_run_id, cb_complete_step.flow_name, cb_complete_step.step_name;

  -- Maybe complete flow run - only if remaining_steps reached 0
  IF _remaining = 0 THEN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'completed',
          completed_at = now(),
          output = (
            SELECT jsonb_object_agg(step_name, output)
            FROM %I
            WHERE flow_run_id = $1 AND status = 'completed'
          )
      WHERE id = $1
        AND status = 'started'
      $QUERY$,
      _f_table, _s_table
    )
    USING _flow_run_id;
  END IF;

  -- Start steps with no dependencies
  PERFORM cb_start_steps(cb_complete_step.flow_name, _flow_run_id);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_fail_step: Mark a step run as failed
-- Sets the step run and the parent flow run to 'failed' status, stores the error message
-- Parameters:
--   flow_name: Flow name
--   step_name: Step name within the flow
--   step_id: Step run ID
--   error_message: Description of the error that occurred
-- Returns: void
CREATE OR REPLACE FUNCTION cb_fail_step(flow_name text, step_name text, step_id bigint, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _f_table text := cb_table_name(cb_fail_step.flow_name, 'f');
  _s_table text := cb_table_name(cb_fail_step.flow_name, 's');
  _flow_run_id bigint;
BEGIN
  -- Fail step run atomically - only succeeds if status is 'created' or 'started'
  EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'failed',
        failed_at = now(),
        error_message = $2
    WHERE id = $1
      AND status IN ('created', 'started')
    RETURNING flow_run_id
    $QUERY$,
    _s_table
  )
  USING cb_fail_step.step_id, cb_fail_step.error_message
  INTO _flow_run_id;

  -- If step wasn't in 'created' or 'started' status, return early (already completed/failed)
  IF _flow_run_id IS NULL THEN
    RETURN;
  END IF;

  -- Fail flow run atomically - only if status is 'started'
  EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'failed',
        failed_at = now(),
        error_message = $2
    WHERE id = $1
      AND status = 'started'
    $QUERY$,
    _f_table
  )
  USING _flow_run_id, cb_fail_step.error_message;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_delete_flow: Delete a flow definition and all its runs
-- Removes the flow metadata, handlers, steps, and drops the associated tables
-- Parameters:
--   name: Flow name
-- Returns: boolean - true if flow was deleted, false if not found
CREATE OR REPLACE FUNCTION cb_delete_flow(name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  _f_table text := cb_table_name(cb_delete_flow.name, 'f');
  _s_table text := cb_table_name(cb_delete_flow.name, 's');
  _res boolean;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext(_f_table));

  EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', _s_table);
  EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', _f_table);

  DELETE FROM cb_step_handlers h
  WHERE h.flow_name = cb_delete_flow.name;

  DELETE FROM cb_step_dependencies d
  WHERE d.flow_name = cb_delete_flow.name;

  DELETE FROM cb_steps s
  WHERE s.flow_name = cb_delete_flow.name;

  DELETE FROM cb_flows f
  WHERE f.name = cb_delete_flow.name
  RETURNING true
  INTO _res;

  RETURN coalesce(_res, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_worker_started: Register a worker with the system
-- Creates worker record and registers task and step handlers
-- Parameters:
--   id: Worker UUID (unique identifier)
--   task_handlers: JSON array of {task_name: string} objects
--   step_handlers: JSON array of {flow_name: string, step_name: string} objects
-- Returns: void
CREATE OR REPLACE FUNCTION cb_worker_started(id uuid, task_handlers jsonb, step_handlers jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  _handler jsonb;
  _task_name text;
  _flow_name text;
  _step_name text;
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
-- cb_worker_heartbeat: Update worker's last heartbeat timestamp
-- Called periodically by workers to indicate they are still alive
-- Parameters:
--   id: Worker UUID
-- Returns: void
CREATE OR REPLACE FUNCTION cb_worker_heartbeat(id uuid)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cb_workers
    SET last_heartbeat_at = now()
    WHERE cb_workers.id = cb_worker_heartbeat.id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_flow_info: Query information about all flow definitions
-- Returns all registered flows with their step definitions and metadata
-- Columns:
--   - name: Flow name
--   - steps: JSON array of step definitions with dependencies
--   - created_at: Flow creation timestamp
CREATE OR REPLACE VIEW cb_flow_info AS
  SELECT
    f.name,
    s.steps,
    f.created_at
  FROM cb_flows f
  LEFT JOIN LATERAL (
    SELECT
      s.flow_name,
      jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'name', s.name,
        'depends_on', (
          SELECT jsonb_agg(jsonb_build_object('name', s_d.dependency_name))
          FROM cb_step_dependencies AS s_d
          WHERE s_d.flow_name = s.flow_name
          AND s_d.step_name = s.name
        )
      )) ORDER BY s.idx) FILTER (WHERE s.idx IS NOT NULL) AS steps
    FROM cb_steps s
    WHERE s.flow_name = f.name
    GROUP BY flow_name
  ) s ON s.flow_name = f.name;
-- +goose statementend

-- +goose statementbegin
-- cb_worker_info: Query information about all registered workers
-- Returns worker registration details and their assigned handlers
-- Columns:
--   - id: Worker UUID
--   - started_at: When the worker registered
--   - last_heartbeat_at: Timestamp of last heartbeat
--   - task_handlers: JSON array of assigned tasks
--   - step_handlers: JSON array of assigned flow steps
CREATE OR REPLACE VIEW cb_worker_info AS
  SELECT
    w.id,
    w.started_at,
    w.last_heartbeat_at,
    t.task_handlers AS task_handlers,
    s.step_handlers AS step_handlers
  FROM cb_workers w
  LEFT JOIN LATERAL (
    SELECT t.worker_id,
         json_agg(json_build_object('task_name', t.task_name) ORDER BY t.task_name) FILTER (WHERE t.worker_id IS NOT NULL) AS task_handlers
    FROM cb_task_handlers t
    WHERE t.worker_id = w.id
    GROUP BY worker_id
  ) t ON t.worker_id = w.id
  LEFT JOIN LATERAL (
    SELECT s.worker_id,
         json_agg(json_build_object('flow_name', s.flow_name, 'step_name', s.step_name) ORDER BY s.flow_name, s.step_name) FILTER (WHERE s.worker_id IS NOT NULL) AS step_handlers
    FROM cb_step_handlers s
    WHERE s.worker_id = w.id
    GROUP BY worker_id
  ) s ON s.worker_id = w.id
  ORDER BY w.started_at DESC;
-- +goose statementend

-- +goose down

-- Drop dynamically created tables
SELECT cb_delete_task(name) FROM cb_tasks;
SELECT cb_delete_flow(name) FROM cb_flows;

DROP VIEW IF EXISTS cb_worker_info;
DROP VIEW IF EXISTS cb_flow_info;
DROP FUNCTION IF EXISTS cb_worker_heartbeat(uuid);
DROP FUNCTION IF EXISTS cb_worker_started(uuid, jsonb, jsonb);
DROP FUNCTION IF EXISTS cb_fail_step(text, text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_step(text, text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_hide_steps(text, text, bigint[], integer);
DROP FUNCTION IF EXISTS cb_read_steps(text, text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_start_steps(text, bigint);
DROP FUNCTION IF EXISTS cb_run_flow(text, jsonb, text);
DROP FUNCTION IF EXISTS cb_delete_flow(text);
DROP FUNCTION IF EXISTS cb_create_flow(text, jsonb);
DROP FUNCTION IF EXISTS cb_fail_task(text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_task(text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_hide_tasks(text, bigint[], integer);
DROP FUNCTION IF EXISTS cb_read_tasks(text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_run_task(text, jsonb, text);
DROP FUNCTION IF EXISTS cb_delete_task(text);
DROP FUNCTION IF EXISTS cb_create_task(text);

DROP TABLE IF EXISTS cb_step_handlers;
DROP TABLE IF EXISTS cb_task_handlers;
DROP TABLE IF EXISTS cb_workers;
DROP TABLE IF EXISTS cb_step_dependencies;
DROP TABLE IF EXISTS cb_steps;
DROP TABLE IF EXISTS cb_flows;
DROP TABLE IF EXISTS cb_tasks;

DROP TYPE IF EXISTS cb_step_message;
DROP TYPE IF EXISTS cb_task_message;
