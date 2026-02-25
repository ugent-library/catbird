-- Task execution functions

-- CONCURRENCY AUDIT:
-- Setup functions (initialization, not hot path):
--   cb_create_task(): Uses pg_advisory_xact_lock, safe for concurrent creation (first-wins)
-- Hot path functions (millions of executions, no advisory locks):
--   cb_run_task(): Uses atomic CTE + UNION ALL pattern with ON CONFLICT DO UPDATE WHERE FALSE - SAFE
--     Critical: MUST use UNION ALL fallback, not bare RETURNING (returns NULL on conflict without it)
--   cb_poll_tasks(): Uses FOR UPDATE SKIP LOCKED for lock-free row polling - SAFE
--   cb_hide_tasks(): Direct UPDATE indexed by visible_at, no locks - SAFE
-- All operations maintain task state invariants
-- Deduplication pattern reference: https://stackoverflow.com/a/35953488

-- +goose up

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

    -- Validate task name
    IF cb_create_task.name = 'input' THEN
        RAISE EXCEPTION 'cb: task name "input" is reserved';
    END IF;

    INSERT INTO cb_tasks (name)
    VALUES (
        cb_create_task.name
    );

    EXECUTE format(
      $QUERY$
      CREATE TABLE IF NOT EXISTS %I (
        id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        concurrency_key text,
        idempotency_key text,
        status text NOT NULL DEFAULT 'queued',
        attempts int NOT NULL DEFAULT 0,
        input jsonb NOT NULL,
        output jsonb,
        error_message text,
        visible_at timestamptz NOT NULL DEFAULT now(),
        started_at timestamptz NOT NULL DEFAULT now(),
        completed_at timestamptz,
        failed_at timestamptz,
        skipped_at timestamptz,
        CONSTRAINT status_valid CHECK (status IN ('queued', 'started', 'completed', 'failed', 'skipped')),
        CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
        CONSTRAINT skipped_and_completed_failed CHECK (NOT (skipped_at IS NOT NULL AND (completed_at IS NOT NULL OR failed_at IS NOT NULL))),
        CONSTRAINT completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
        CONSTRAINT failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
        CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
        CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
      )
      $QUERY$,
      _t_table
    );

    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (concurrency_key) WHERE concurrency_key IS NOT NULL AND status IN (''queued'', ''started'')', _t_table || '_concurrency_key_idx', _t_table);
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN (''queued'', ''started'', ''completed'')', _t_table || '_idempotency_key_idx', _t_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (visible_at);', _t_table || '_visible_at_idx', _t_table);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_run_task: Create a task run (enqueue a task execution)
-- Parameters:
--   name: Task name
--   input: JSON input data for the task
--   concurrency_key: Optional key for concurrency control (prevents overlapping runs)
--   idempotency_key: Optional key for idempotency (prevents duplicate runs including completed)
-- Returns: bigint - the task run ID
CREATE OR REPLACE FUNCTION cb_run_task(
    name text,
    input jsonb,
    concurrency_key text = NULL,
    idempotency_key text = NULL,
    visible_at timestamptz = NULL
)
RETURNS bigint
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    _t_table text := cb_table_name(cb_run_task.name, 't');
    _id bigint;
BEGIN
    -- Validate: both keys cannot be set simultaneously
    IF cb_run_task.concurrency_key IS NOT NULL AND cb_run_task.idempotency_key IS NOT NULL THEN
        RAISE EXCEPTION 'cb: cannot specify both concurrency_key and idempotency_key';
    END IF;

    -- ON CONFLICT DO UPDATE with WHERE FALSE: atomic insert + return conflicting row ID.
    -- WHERE FALSE prevents the update from executing, but RETURNING still returns the conflict row.
    -- Use UNION ALL to handle both INSERT success and conflict cases atomically.
    -- Pattern from: https://stackoverflow.com/a/35953488
    IF cb_run_task.concurrency_key IS NOT NULL THEN
        -- Concurrency control: dedupe only queued/started
        EXECUTE format(
            $QUERY$
            WITH ins AS (
                INSERT INTO %I (input, concurrency_key, visible_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (concurrency_key) WHERE concurrency_key IS NOT NULL AND status IN ('queued', 'started')
                DO UPDATE SET status = EXCLUDED.status WHERE FALSE
                RETURNING id
            )
            SELECT id FROM ins
            UNION ALL
            SELECT id FROM %I
            WHERE concurrency_key = $2 AND concurrency_key IS NOT NULL AND status IN ('queued', 'started')
            LIMIT 1
            $QUERY$,
            _t_table, _t_table
        )
        USING cb_run_task.input, cb_run_task.concurrency_key, coalesce(cb_run_task.visible_at, now())
        INTO _id;
    ELSIF cb_run_task.idempotency_key IS NOT NULL THEN
        -- Idempotency: dedupe queued/started/completed
        EXECUTE format(
            $QUERY$
            WITH ins AS (
                INSERT INTO %I (input, idempotency_key, visible_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued', 'started', 'completed')
                DO UPDATE SET status = EXCLUDED.status WHERE FALSE
                RETURNING id
            )
            SELECT id FROM ins
            UNION ALL
            SELECT id FROM %I
            WHERE idempotency_key = $2 AND idempotency_key IS NOT NULL AND status IN ('queued', 'started', 'completed')
            LIMIT 1
            $QUERY$,
            _t_table, _t_table
        )
        USING cb_run_task.input, cb_run_task.idempotency_key, coalesce(cb_run_task.visible_at, now())
        INTO _id;
    ELSE
        -- No deduplication
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (input, visible_at)
            VALUES ($1, $2)
            RETURNING id
            $QUERY$,
            _t_table
        )
        USING cb_run_task.input, coalesce(cb_run_task.visible_at, now())
        INTO _id;
    END IF;

    RETURN _id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_poll_tasks: Poll for task runs from the queue
-- Parameters:
--   name: Task name
--   quantity: Number of task runs to read (must be > 0)
--   hide_for: Duration in milliseconds to hide task runs from other workers (must be > 0)
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: Set of cb_task_claim records
CREATE OR REPLACE FUNCTION cb_poll_tasks(
    name text,
    quantity int,
    hide_for int,
    poll_for int,
    poll_interval int
)
RETURNS SETOF cb_task_claim
LANGUAGE plpgsql AS $$
DECLARE
    _m cb_task_claim;
    _sleep_for double precision;
    _stop_at timestamp;
    _q text;
    _t_table text := cb_table_name(cb_poll_tasks.name, 't');
BEGIN
    IF cb_poll_tasks.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_poll_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;
    IF cb_poll_tasks.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;
    IF cb_poll_tasks.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_poll_tasks.poll_interval / 1000.0;

    IF _sleep_for >= cb_poll_tasks.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_poll_tasks.poll_for / 1000.0);

    _q := FORMAT(
        $QUERY$
        WITH runs AS (
          SELECT id
          FROM %I
          WHERE visible_at <= clock_timestamp()
            AND status IN ('queued', 'started')
          ORDER BY id ASC
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE %I m
        SET status = 'started',
            started_at = clock_timestamp(),
            attempts = attempts + 1,
            visible_at = clock_timestamp() + $2
        FROM runs
        WHERE m.id = runs.id
        RETURNING m.id,
                  m.attempts,
                  m.input;
        $QUERY$,
        _t_table, _t_table
      );

    LOOP
      IF (SELECT clock_timestamp() >= _stop_at) THEN
        RETURN;
      END IF;

      FOR _m IN
        EXECUTE _q USING cb_poll_tasks.quantity, make_interval(secs => cb_poll_tasks.hide_for / 1000.0)
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
BEGIN
    IF cb_hide_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
    SET visible_at = (clock_timestamp() + $2)
      WHERE id = any($1);
      $QUERY$,
      _t_table
    )
    USING cb_hide_tasks.ids,
          make_interval(secs => cb_hide_tasks.hide_for / 1000.0);
END;
$$;
-- +goose statementend

-- +goose statementbegin
--cb_complete_task: Mark a task run as completed
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
-- cb_wait_task_output: Long-poll for task completion without client-side polling loops
-- Parameters:
--   name: Task name
--   run_id: Task run ID
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: status/output/error_message once run reaches terminal state, or no rows on timeout
DROP FUNCTION IF EXISTS cb_wait_task_output(text, bigint, int, int);
DROP FUNCTION IF EXISTS cb_wait_task_output(text, bigint, int);
CREATE OR REPLACE FUNCTION cb_wait_task_output(
    task_name text,
    run_id bigint,
    poll_for int DEFAULT 5000,
    poll_interval int DEFAULT 200
)
RETURNS TABLE(status text, output jsonb, error_message text)
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := cb_table_name(cb_wait_task_output.task_name, 't');
    _status text;
    _output jsonb;
    _error_message text;
    _sleep_for double precision;
    _stop_at timestamp;
BEGIN
    IF cb_wait_task_output.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;

    IF cb_wait_task_output.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_wait_task_output.poll_interval / 1000.0;

    IF _sleep_for >= cb_wait_task_output.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_wait_task_output.poll_for / 1000.0);

    LOOP
        IF clock_timestamp() >= _stop_at THEN
            RETURN;
        END IF;

        _status := NULL;
        _output := NULL;
        _error_message := NULL;

        EXECUTE format(
          $QUERY$
          SELECT t.status, t.output, t.error_message
          FROM %I t
          WHERE t.id = $1
          $QUERY$,
          _t_table
        )
        USING cb_wait_task_output.run_id
        INTO _status, _output, _error_message;

        IF _status IS NULL THEN
            RAISE EXCEPTION 'cb: task run % not found for task %', cb_wait_task_output.run_id, cb_wait_task_output.task_name;
        END IF;

        IF _status IN ('completed', 'failed', 'skipped') THEN
            status := _status;
            output := _output;
            error_message := _error_message;
            RETURN NEXT;
            RETURN;
        END IF;

        PERFORM pg_sleep(_sleep_for);
    END LOOP;
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

-- +goose down

DROP FUNCTION IF EXISTS cb_delete_task(text);
DROP FUNCTION IF EXISTS cb_fail_task(text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_task(text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_wait_task_output(text, bigint, int, int);
DROP FUNCTION IF EXISTS cb_wait_task_output(text, bigint, int);
DROP FUNCTION IF EXISTS cb_hide_tasks(text, bigint[], integer);
DROP FUNCTION IF EXISTS cb_poll_tasks(text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_run_task(text, jsonb, text, text, timestamptz);
DROP FUNCTION IF EXISTS cb_create_task(text);
