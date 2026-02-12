-- SQL code is taken from or inspired by pgflow (https://github.com/pgflow-dev/pgflow)

-- CONCURRENCY AUDIT:
-- Setup functions (initialization, not hot path):
--   cb_create_task(): Uses pg_advisory_xact_lock, safe for concurrent creation (first-wins)
--   cb_create_flow(): Uses pg_advisory_xact_lock, safe for concurrent creation (first-wins)
-- Hot path functions (millions of executions, no advisory locks):
--   cb_run_task(): Uses ON CONFLICT with dedup-id for idempotent task enqueueing - SAFE
--   cb_read_tasks(): Uses FOR UPDATE SKIP LOCKED for lock-free row polling - SAFE
--   cb_read_poll_tasks(): Uses FOR UPDATE SKIP LOCKED in polling loop - SAFE
--   cb_hide_tasks(): Direct UPDATE indexed by deliver_at, no locks - SAFE
--   cb_delete_task_run(): Direct DELETE, no locks - SAFE
--   cb_activate_steps(): Lock-free condition evaluation + FOR UPDATE SKIP LOCKED - SAFE
--   cb_update_step_output(): Atomic UPDATE with WHERE constraints - SAFE
-- All operations maintain task/flow state invariants and step ordering

-- +goose up

-- +goose statementbegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cb_task_message') THEN
        CREATE TYPE cb_task_message AS (
            id bigint,
            deliveries int,
            input jsonb
       );
    END IF;
    -- Drop and recreate cb_step_message to ensure it has the latest structure (including signal_input)
    DROP TYPE IF EXISTS cb_step_message CASCADE;
    CREATE TYPE cb_step_message AS (
        id bigint,
        deliveries int,
        input jsonb,
        step_outputs jsonb,
        signal_input jsonb
    );
END$$;
-- +goose statementend

CREATE TABLE IF NOT EXISTS cb_tasks (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    condition jsonb,
    CONSTRAINT name_not_empty CHECK (name <> ''),
    CONSTRAINT name_not_reserved CHECK (name <> 'input')
);

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT name_not_empty CHECK (name <> '')
);

CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    idx int NOT NULL DEFAULT 0,
    dependency_count int NOT NULL DEFAULT 0,
    has_signal boolean NOT NULL DEFAULT false,
    condition jsonb,
    PRIMARY KEY (flow_name, name),
    UNIQUE (flow_name, idx),
    CONSTRAINT name_valid CHECK (name <> ''),
    CONSTRAINT name_not_reserved CHECK (name NOT IN ('input', 'signal')),
    CONSTRAINT idx_valid CHECK (idx >= 0),
    CONSTRAINT dependency_count_valid CHECK (dependency_count >= 0)
);

CREATE TABLE IF NOT EXISTS cb_step_dependencies (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    step_name text NOT NULL,
    dependency_name text NOT NULL,
    idx int NOT NULL DEFAULT 0,
    optional boolean NOT NULL DEFAULT false,
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
-- cb_parse_condition: Canonical condition parser for all clients (go, python, js, ruby, java, etc.)
-- Takes a condition expression and returns jsonb with parsed components: {field, operator, value}
-- Raises an exception if the expression is invalid
CREATE OR REPLACE FUNCTION cb_parse_condition(expr text)
RETURNS jsonb AS $$
DECLARE
    _expr text := trim(expr);
    _negated boolean := false;
    _op text;
    _op_idx int;
    _field text;
    _value_str text;
    _value jsonb;
    _operators text[] := ARRAY['exists', 'contains', 'lte', 'gte', 'eq', 'ne', 'gt', 'lt', 'in'];
BEGIN
    IF _expr = '' THEN
        RAISE EXCEPTION 'cb: empty condition expression';
    END IF;

    IF _expr LIKE 'not %' THEN
        _negated := true;
        _expr := trim(substring(_expr FROM 5));

        IF _expr = '' THEN
            RAISE EXCEPTION 'cb: empty condition expression after not';
        END IF;
    END IF;

    -- Find operator (check longer operators first to avoid "gte" matching "gt")
    FOR _op IN SELECT unnest(_operators)
    LOOP
        _op_idx := position(' ' || _op || ' ' IN ' ' || _expr || ' ');
        IF _op_idx > 0 THEN
            _op := _op;
            _op_idx := _op_idx - 1; -- Adjust for leading space we added
            EXIT;
        END IF;
        
        -- Check at beginning: "operator "
        IF _expr LIKE _op || ' %' THEN
            _op_idx := 0;
            EXIT;
        END IF;

        -- For "exists" operator at end: "field exists"
        IF _op = 'exists' AND _expr LIKE '% ' || _op THEN
            _op_idx := length(_expr) - length(_op) - 1;
            _field := trim(substring(_expr FROM 1 FOR _op_idx));
            RETURN jsonb_build_object(
                'field', _field,
                'operator', _op,
                'value', NULL
            );
        END IF;
    END LOOP;

    IF _op IS NULL THEN
        RAISE EXCEPTION 'cb: no valid operator found in condition: %', _expr;
    END IF;

    -- Extract field and value
    IF _op_idx = 0 THEN
        -- Operator at beginning
        _field := '';
        _value_str := trim(substring(_expr FROM length(_op) + 1));
    ELSE
        -- Operator in middle
        _field := trim(substring(_expr FROM 1 FOR _op_idx - 1));
        _value_str := trim(substring(_expr FROM _op_idx + length(_op) + 2));
    END IF;

    IF _field = '' AND _op != 'exists' THEN
        RAISE EXCEPTION 'cb: missing field in condition: %', _expr;
    END IF;

    -- Validate field (alphanumeric, dots, underscores, brackets)
    IF _field ~ '[^a-zA-Z0-9_.\[\]]' THEN
        RAISE EXCEPTION 'cb: invalid field name: %', _field;
    END IF;

    -- Parse value
    _value := cb_parse_condition_value(_value_str, _op);

    RETURN jsonb_build_object(
        'field', _field,
        'operator', _op,
        'value', _value,
        'negated', _negated
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- cb_parse_condition_value: Parse a condition value string into appropriate JSON type
-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_parse_condition_value(val_str text, op text)
RETURNS jsonb AS $$
DECLARE
    _val_str text := trim(val_str);
    _num numeric;
BEGIN
    IF op = 'exists' THEN
        RETURN NULL;
    END IF;

    IF _val_str = '' THEN
        RAISE EXCEPTION 'cb: empty value in condition';
    END IF;

    -- Try JSON array first (for "in" operator or literal arrays)
    IF _val_str LIKE '[%' THEN
        BEGIN
            RETURN _val_str::jsonb;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'cb: invalid JSON array value: %', _val_str;
        END;
    END IF;

    -- Try as boolean
    IF _val_str = 'true' THEN
        RETURN 'true'::jsonb;
    END IF;
    IF _val_str = 'false' THEN
        RETURN 'false'::jsonb;
    END IF;

    -- Try as number
    BEGIN
        _num := _val_str::numeric;
        RETURN to_jsonb(_num);
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    -- Try as JSON string (quoted)
    IF _val_str LIKE '"%' THEN
        BEGIN
            RETURN _val_str::jsonb;
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    END IF;

    -- Accept as unquoted string identifier (letters, digits, underscore, hyphen)
    IF _val_str ~ '^[a-zA-Z_][a-zA-Z0-9_-]*$' THEN
        RETURN to_jsonb(_val_str);
    END IF;

    RAISE EXCEPTION 'cb: unable to parse condition value: %', _val_str;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- cb_evaluate_condition: Evaluate a parsed condition against step output
-- Condition jsonb format: {field, operator, value, negated}
-- Returns true if condition is satisfied, false otherwise
-- Used to determine if conditions allow step execution
-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_evaluate_condition(condition jsonb, output jsonb)
RETURNS boolean AS $$
DECLARE
    _field text := condition->>'field';
    _operator text := condition->>'operator';
    _value jsonb := condition->'value';
    _field_val jsonb;
    _negated boolean := coalesce((condition->>'negated')::boolean, false);
    _result boolean;
BEGIN
    IF condition IS NULL OR output IS NULL THEN
        RETURN false;
    END IF;

    IF _field IS NULL OR _operator IS NULL THEN
        RETURN false;
    END IF;

    -- Extract the field value from output using jsonb path
    _field_val := cb_get_jsonb_field(output, _field);

    -- Evaluate based on operator
    CASE _operator
        WHEN 'exists' THEN
            _result := _field_val IS NOT NULL;
        
        WHEN 'eq' THEN
            -- Handle NULL: if field doesn't exist, return false
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := _field_val = _value;
            END IF;
        
        WHEN 'ne' THEN
            IF _field_val IS NULL THEN
                _result := true; -- NULL != value
            ELSE
                _result := _field_val <> _value;
            END IF;
        
        WHEN 'gt' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := (_field_val::text::numeric) > (_value::text::numeric);
            END IF;
        
        WHEN 'gte' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := (_field_val::text::numeric) >= (_value::text::numeric);
            END IF;
        
        WHEN 'lt' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := (_field_val::text::numeric) < (_value::text::numeric);
            END IF;
        
        WHEN 'lte' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                _result := (_field_val::text::numeric) <= (_value::text::numeric);
            END IF;
        
        WHEN 'in' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                -- Check if _field_val is in the array _value
                _result := _field_val = ANY(jsonb_array_elements(_value));
            END IF;
        
        WHEN 'contains' THEN
            IF _field_val IS NULL THEN
                _result := false;
            ELSE
                -- For strings, check substring
                -- For arrays/objects, check containment
                IF jsonb_typeof(_field_val) = 'string' THEN
                    _result := (_field_val#>>'{}') LIKE '%' || (_value#>>'{}') || '%';
                ELSE
                    _result := _field_val @> _value;
                END IF;
            END IF;
        
        ELSE
            _result := false;
    END CASE;

    IF _negated THEN
        RETURN NOT _result;
    END IF;

    RETURN _result;
EXCEPTION WHEN OTHERS THEN
    -- If evaluation fails, return false (condition not met)
    RETURN false;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- cb_get_jsonb_field: Extract a value from jsonb using a field path
-- Supports nested paths: "user.age" or array access: "items[0]"
-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_get_jsonb_field(obj jsonb, field_path text)
RETURNS jsonb AS $$
DECLARE
    _parts text[];
    _part text;
    _current jsonb := obj;
    _i int;
BEGIN
    IF obj IS NULL OR field_path IS NULL OR field_path = '' THEN
        RETURN NULL;
    END IF;

    -- Split path by dots (simple split, doesn't handle complex escaping)
    _parts := string_to_array(field_path, '.');
    
    FOREACH _part IN ARRAY _parts
    LOOP
        IF _current IS NULL THEN
            RETURN NULL;
        END IF;

        -- Check for array index notation like "items[0]"
        IF _part LIKE '%[%]%' THEN
            _current := _current -> split_part(_part, '[', 1) -> split_part(split_part(_part, '[', 2), ']', 1)::int;
        ELSE
            -- Regular object key access
            _current := _current -> _part;
        END IF;
    END LOOP;

    RETURN _current;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- +goose statementbegin
-- cb_evaluate_condition_expr: Wrapper that takes text condition
-- Parses the condition first, then evaluates it
-- This is useful for testing and when conditions are stored as text
CREATE OR REPLACE FUNCTION cb_evaluate_condition_expr(
    condition_expr text,
    output jsonb
)
RETURNS boolean AS $$
DECLARE
    _parsed_condition jsonb;
BEGIN
    IF condition_expr IS NULL OR output IS NULL THEN
        RETURN false;
    END IF;

    -- Parse the condition
    _parsed_condition := cb_parse_condition(condition_expr);
    
    IF _parsed_condition IS NULL THEN
        RETURN false;
    END IF;

    -- Evaluate using the JSONB version
    RETURN cb_evaluate_condition(_parsed_condition, output);
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

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
        deliveries int NOT NULL DEFAULT 0,
        input jsonb NOT NULL,
        output jsonb,
        error_message text,
        deliver_at timestamptz NOT NULL DEFAULT now(),
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
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (deliver_at);', _t_table || '_deliver_at_idx', _t_table);
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
    idempotency_key text = NULL
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

    -- See https://dba.stackexchange.com/questions/212580/concurrent-transactions-result-in-race-condition-with-unique-constraint-on-inser/213625#213625
    IF cb_run_task.concurrency_key IS NOT NULL THEN
        -- Concurrency control: dedupe only queued/started
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (input, concurrency_key)
            VALUES ($1, $2)
            ON CONFLICT (concurrency_key) WHERE concurrency_key IS NOT NULL AND status IN ('queued', 'started') DO NOTHING
            RETURNING id
            $QUERY$,
            _t_table
        )
        USING cb_run_task.input, cb_run_task.concurrency_key
        INTO _id;
        
        -- If duplicate (no row inserted), get the existing row's ID
        IF _id IS NULL THEN
            EXECUTE format(
                $QUERY$
                SELECT id FROM %I
                WHERE concurrency_key = $1 AND status IN ('queued', 'started')
                LIMIT 1
                $QUERY$,
                _t_table
            )
            USING cb_run_task.concurrency_key
            INTO _id;
        END IF;
    ELSIF cb_run_task.idempotency_key IS NOT NULL THEN
        -- Idempotency: dedupe queued/started/completed
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (input, idempotency_key)
            VALUES ($1, $2)
            ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued', 'started', 'completed') DO NOTHING
            RETURNING id
            $QUERY$,
            _t_table
        )
        USING cb_run_task.input, cb_run_task.idempotency_key
        INTO _id;
        
        -- If duplicate (no row inserted), get the existing row's ID
        IF _id IS NULL THEN
            EXECUTE format(
                $QUERY$
                SELECT id FROM %I
                WHERE idempotency_key = $1 AND status IN ('queued', 'started', 'completed')
                LIMIT 1
                $QUERY$,
                _t_table
            )
            USING cb_run_task.idempotency_key
            INTO _id;
        END IF;
    ELSE
        -- No deduplication
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (input)
            VALUES ($1)
            RETURNING id
            $QUERY$,
            _t_table
        )
        USING cb_run_task.input
        INTO _id;
    END IF;

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
                  m.deliveries,
                  m.input;
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
BEGIN
    IF cb_hide_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET deliver_at = (clock_timestamp() + $2)
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
RETURNS void AS $$
#variable_conflict use_column
DECLARE
    _step jsonb;
    _dep jsonb;
    _step_name text;
    _dep_name text;
    _idx int := 0;
    _dep_idx int;
    _f_table text;
    _s_table text;
    _condition jsonb;
    _optional boolean;
BEGIN
    IF cb_create_flow.name IS NULL OR cb_create_flow.name = '' THEN
    RAISE EXCEPTION 'cb: flow name must not be empty';
    END IF;

    IF NOT cb_create_flow.name ~ '^[a-z0-9_]+$' THEN
    RAISE EXCEPTION 'cb: flow name "%" contains invalid characters, allowed: a-z, 0-9, _', cb_create_flow.name;
    END IF;

    IF length(cb_create_flow.name) >= 58 THEN
    RAISE EXCEPTION 'cb: flow name "%" is too long, maximum length is 58', cb_create_flow.name;
    END IF;

    _f_table := cb_table_name(cb_create_flow.name, 'f');
    _s_table := cb_table_name(cb_create_flow.name, 's');

    PERFORM pg_advisory_xact_lock(hashtext(_f_table));

    INSERT INTO cb_flows (name) VALUES (cb_create_flow.name)
    ON CONFLICT (name) DO NOTHING;

    DELETE FROM cb_step_handlers WHERE flow_name = cb_create_flow.name;
    DELETE FROM cb_step_dependencies WHERE flow_name = cb_create_flow.name;
    DELETE FROM cb_steps WHERE flow_name = cb_create_flow.name;

    FOR _step IN SELECT jsonb_array_elements(cb_create_flow.steps)
    LOOP
    _step_name := _step->>'name';

    IF _step_name IS NULL OR _step_name = '' THEN
      RAISE EXCEPTION 'cb: step name must not be empty';
    END IF;

    IF NOT _step_name ~ '^[a-z0-9_]+$' THEN
      RAISE EXCEPTION 'cb: step name "%" contains invalid characters, allowed: a-z, 0-9, _', _step_name;
    END IF;

    IF _step_name = 'input' THEN
      RAISE EXCEPTION 'cb: step name "input" is reserved';
    END IF;

    IF _step_name = 'signal' THEN
      RAISE EXCEPTION 'cb: step name "signal" is reserved';
    END IF;

    IF length(_step_name) >= 58 THEN
      RAISE EXCEPTION 'cb: step name "%" is too long, maximum length is 58', _step_name;
    END IF;

    -- Extract condition from step if present
    _condition := NULL;
    IF _step ? 'condition' AND _step->>'condition' IS NOT NULL AND _step->>'condition' != '' THEN
      _condition := cb_parse_condition(_step->>'condition');
    END IF;

    INSERT INTO cb_steps (flow_name, name, idx, dependency_count, has_signal, condition)
    VALUES (
      cb_create_flow.name,
      _step_name,
      _idx,
      jsonb_array_length(coalesce(_step->'depends_on', '[]'::jsonb)),
      coalesce((_step->>'has_signal')::boolean, false),
      _condition
    );

    _dep_idx := 0;
    FOR _dep IN SELECT jsonb_array_elements(coalesce(_step->'depends_on', '[]'::jsonb))
    LOOP
      _dep_name := _dep->>'name';
      _optional := coalesce((_dep->>'optional')::boolean, false);
      
      INSERT INTO cb_step_dependencies (flow_name, step_name, dependency_name, idx, optional)
      VALUES (cb_create_flow.name, _step_name, _dep_name, _dep_idx, _optional);
      _dep_idx := _dep_idx + 1;
    END LOOP;

    _idx := _idx + 1;
    END LOOP;

    EXECUTE format(
    $QUERY$
    CREATE TABLE IF NOT EXISTS %I (
      id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
      concurrency_key text,
      idempotency_key text,
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
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (concurrency_key) WHERE concurrency_key IS NOT NULL AND status = ''started''', _f_table || '_concurrency_key_idx', _f_table);
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN (''started'', ''completed'')', _f_table || '_idempotency_key_idx', _f_table);

    -- Create step runs table - includes 'skipped' in status constraint
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
      signal_input jsonb,
      remaining_dependencies int NOT NULL DEFAULT 0,
      deliver_at timestamptz NOT NULL DEFAULT now(),
      created_at timestamptz NOT NULL DEFAULT now(),
      started_at timestamptz,
      completed_at timestamptz,
      failed_at timestamptz,
      skipped_at timestamptz,
      UNIQUE (flow_run_id, step_name),
      FOREIGN KEY (flow_run_id) REFERENCES %I (id),
      CONSTRAINT status_valid CHECK (status IN ('created', 'started', 'completed', 'failed', 'skipped')),
      CONSTRAINT remaining_dependencies_valid CHECK (remaining_dependencies >= 0),
      CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
      CONSTRAINT skipped_and_completed_failed CHECK (NOT (skipped_at IS NOT NULL AND (completed_at IS NOT NULL OR failed_at IS NOT NULL))),
      CONSTRAINT deliveries_valid CHECK (deliveries >= 0)
    )
    $QUERY$,
    _s_table, _f_table
    );

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (flow_run_id, status);', _s_table || '_flow_run_id_status_idx', _s_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (deliver_at);', _s_table || '_deliver_at_idx', _s_table);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
-- cb_run_flow: Create a flow run (enqueue a flow execution)
-- Creates a flow run and initializes all step runs with dependency tracking
-- Parameters:
--   name: Flow name
--   input: JSON input data for the flow
--   concurrency_key: Optional key for concurrency control (prevents overlapping runs)
--   idempotency_key: Optional key for idempotency (prevents duplicate runs including completed)
-- Returns: bigint - the flow run ID
CREATE OR REPLACE FUNCTION cb_run_flow(
    name text,
    input jsonb,
    concurrency_key text = NULL,
    idempotency_key text = NULL
)
RETURNS bigint
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    _f_table text := cb_table_name(cb_run_flow.name, 'f');
    _s_table text := cb_table_name(cb_run_flow.name, 's');
    _id bigint;
    _remaining_steps int;
BEGIN
    -- Validate: both keys cannot be set simultaneously
    IF cb_run_flow.concurrency_key IS NOT NULL AND cb_run_flow.idempotency_key IS NOT NULL THEN
        RAISE EXCEPTION 'cb: cannot specify both concurrency_key and idempotency_key';
    END IF;

    -- Count total steps
    SELECT count(*) INTO _remaining_steps
    FROM cb_steps s
    WHERE s.flow_name = cb_run_flow.name;

    -- Create flow run or get existing run with same key
    IF cb_run_flow.concurrency_key IS NOT NULL THEN
        -- Concurrency control: dedupe only started
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (concurrency_key, input, remaining_steps, status)
            VALUES ($1, $2, $3, 'started')
            ON CONFLICT (concurrency_key) WHERE concurrency_key IS NOT NULL AND status = 'started' DO NOTHING
            RETURNING id
            $QUERY$,
            _f_table
        )
        USING cb_run_flow.concurrency_key, cb_run_flow.input, _remaining_steps
        INTO _id;
        
        -- If duplicate (no row inserted), get the existing row's ID
        IF _id IS NULL THEN
            EXECUTE format(
                $QUERY$
                SELECT id FROM %I
                WHERE concurrency_key = $1 AND status = 'started'
                LIMIT 1
                $QUERY$,
                _f_table
            )
            USING cb_run_flow.concurrency_key
            INTO _id;
        END IF;
    ELSIF cb_run_flow.idempotency_key IS NOT NULL THEN
        -- Idempotency: dedupe started/completed
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (idempotency_key, input, remaining_steps, status)
            VALUES ($1, $2, $3, 'started')
            ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('started', 'completed') DO NOTHING
            RETURNING id
            $QUERY$,
            _f_table
        )
        USING cb_run_flow.idempotency_key, cb_run_flow.input, _remaining_steps
        INTO _id;
        
        -- If duplicate (no row inserted), get the existing row's ID
        IF _id IS NULL THEN
            EXECUTE format(
                $QUERY$
                SELECT id FROM %I
                WHERE idempotency_key = $1 AND status IN ('started', 'completed')
                LIMIT 1
                $QUERY$,
                _f_table
            )
            USING cb_run_flow.idempotency_key
            INTO _id;
        END IF;
    ELSE
        -- No deduplication
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (input, remaining_steps, status)
            VALUES ($1, $2, 'started')
            RETURNING id
            $QUERY$,
            _f_table
        )
        USING cb_run_flow.input, _remaining_steps
        INTO _id;
    END IF;

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
-- Evaluates conditions and can skip steps based on condition evaluation
-- Parameters:
--   flow_name: Flow name
--   flow_run_id: Flow run ID
-- Returns: void
CREATE OR REPLACE FUNCTION cb_start_steps(flow_name text, flow_run_id bigint)
RETURNS void AS $$
DECLARE
    _f_table text := cb_table_name(cb_start_steps.flow_name, 'f');
    _s_table text := cb_table_name(cb_start_steps.flow_name, 's');
    _flow_input jsonb;
    _step_to_process record;
    _step_condition jsonb;
    _step_inputs jsonb;
    _is_condition_true boolean;
    _remaining int;
    _steps_processed_this_iteration int;
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

    -- Loop until no more steps can be started (handles cascading optional dependencies)
    LOOP
    _steps_processed_this_iteration := 0;

    -- Start all steps with no dependencies and no signal requirement (or signal already received)
    -- Evaluate step conditions to decide if step should be skipped
    FOR _step_to_process IN
    EXECUTE format(
      $QUERY$
      SELECT sr.id, sr.step_name, sr.remaining_dependencies,
             (SELECT jsonb_object_agg(deps.step_name, deps.output)
              FROM %I deps
              WHERE deps.flow_run_id = $1
                AND deps.step_name IN (
                  SELECT dependency_name
                  FROM cb_step_dependencies
                  WHERE flow_name = $3 AND step_name = sr.step_name
                )
              AND deps.status = 'completed'
             ) AS step_outputs,
             sr.signal_input,
             (SELECT condition FROM cb_steps WHERE flow_name = $3 AND name = sr.step_name) AS condition
      FROM %I sr
      WHERE sr.flow_run_id = $1
        AND sr.status = 'created'
        AND sr.remaining_dependencies = 0
        AND (NOT (SELECT has_signal FROM cb_steps WHERE flow_name = $3 AND name = sr.step_name) 
             OR sr.signal_input IS NOT NULL)
      FOR UPDATE SKIP LOCKED
      $QUERY$,
      _s_table, _s_table
    )
    USING cb_start_steps.flow_run_id, cb_start_steps.flow_run_id, cb_start_steps.flow_name
    LOOP
    -- Check if step has a condition
    _step_condition := _step_to_process.condition;

    IF _step_condition IS NOT NULL THEN
      -- Build step_inputs: combine flow input, dependency outputs, and signal input
      -- Flow input accessible as input.*, dependency outputs as dependency_name.*, signal as signal.*
      _step_inputs := jsonb_build_object('input', _flow_input);
      
      IF _step_to_process.step_outputs IS NOT NULL THEN
        _step_inputs := _step_inputs || _step_to_process.step_outputs;
      END IF;

      IF _step_to_process.signal_input IS NOT NULL THEN
        _step_inputs := _step_inputs || jsonb_build_object('signal', _step_to_process.signal_input);
      END IF;

      -- Evaluate the condition
      _is_condition_true := cb_evaluate_condition(_step_condition, _step_inputs);

      IF NOT _is_condition_true THEN
        -- Condition is false - mark step as skipped
        EXECUTE format(
          $QUERY$
          UPDATE %I
          SET status = 'skipped',
              skipped_at = now(),
              output = '{}'::jsonb
          WHERE id = $1
          $QUERY$,
          _s_table
        )
        USING _step_to_process.id;
        
        -- Decrement dependent steps' remaining_dependencies
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
        USING cb_start_steps.flow_run_id, cb_start_steps.flow_name, _step_to_process.step_name;

        -- Decrement remaining_steps in flow run for skipped step
        -- And check if flow should be completed
        EXECUTE format(
          $QUERY$
          UPDATE %I
          SET remaining_steps = remaining_steps - 1
          WHERE id = $1 AND status = 'started'
          RETURNING remaining_steps
          $QUERY$,
          _f_table
        )
        USING cb_start_steps.flow_run_id
        INTO _remaining;
        
        -- If all steps done (remaining_steps = 0), mark flow as completed
        IF _remaining IS NOT NULL AND _remaining = 0 THEN
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
          USING cb_start_steps.flow_run_id;
        END IF;
        
        _steps_processed_this_iteration := _steps_processed_this_iteration + 1;
        
        -- Continue to next step
        CONTINUE;
      END IF;
    END IF;

    -- Normal activation (no condition or condition is true)
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'started',
          started_at = now(),
          deliver_at = now()
      WHERE id = $1
      $QUERY$,
      _s_table
    )
    USING _step_to_process.id;
    
    _steps_processed_this_iteration := _steps_processed_this_iteration + 1;
    END LOOP;
    
    -- Exit outer loop if no steps were processed in this iteration
    EXIT WHEN _steps_processed_this_iteration = 0;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
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
                  m.deliveries,
                  (SELECT input FROM %I f WHERE f.id = m.flow_run_id) AS input,
                  (SELECT jsonb_object_agg(deps.step_name, deps.output)
                   FROM %I deps
                   WHERE deps.flow_run_id = m.flow_run_id
                     AND deps.step_name IN (
                       SELECT dependency_name
                       FROM cb_step_dependencies
                       WHERE flow_name = $4 AND step_name = m.step_name
                     )
                     AND deps.status = 'completed'
                  ) AS step_outputs,
                  m.signal_input;
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
BEGIN
    IF cb_hide_steps.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

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
          make_interval(secs => cb_hide_steps.hide_for / 1000.0),
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
-- cb_signal_flow: Deliver a signal to a waiting step run
-- Atomically delivers signal input to a step that requires it, then starts the step if all conditions are met
-- Parameters:
--   flow_name: Flow name
--   flow_run_id: Flow run ID
--   step_name: Step name within the flow
--   input: JSON signal input data
-- Returns: boolean - true if signal was delivered, false if already signaled or step doesn't require signal
CREATE OR REPLACE FUNCTION cb_signal_flow(flow_name text, flow_run_id bigint, step_name text, input jsonb)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := cb_table_name(cb_signal_flow.flow_name, 's');
    _updated boolean;
BEGIN
    -- Atomically update signal_input - only succeeds if step is created, requires signal, and not already signaled
    EXECUTE format(
    $QUERY$
    UPDATE %I sr
    SET signal_input = $3
    FROM cb_steps s
    WHERE sr.flow_run_id = $1
      AND sr.step_name = $2
      AND sr.status = 'created'
      AND sr.signal_input IS NULL
      AND s.flow_name = $4
      AND s.name = sr.step_name
      AND s.has_signal = true
    RETURNING true
    $QUERY$,
    _s_table
    )
    USING cb_signal_flow.flow_run_id, cb_signal_flow.step_name, cb_signal_flow.input, cb_signal_flow.flow_name
    INTO _updated;

    IF _updated IS NULL THEN
    RETURN false;
    END IF;

    -- Try to start steps now that signal has been delivered
    PERFORM cb_start_steps(cb_signal_flow.flow_name, cb_signal_flow.flow_run_id);

    RETURN true;
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

    DELETE FROM cb_step_handlers h
    WHERE h.flow_name = cb_delete_flow.name;

    DELETE FROM cb_step_dependencies d
    WHERE d.flow_name = cb_delete_flow.name;

    DELETE FROM cb_steps s
    WHERE s.flow_name = cb_delete_flow.name;

    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', _s_table);
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', _f_table);

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
        'has_signal', s.has_signal,
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
DROP FUNCTION IF EXISTS cb_signal_flow(text, bigint, text, jsonb);
DROP FUNCTION IF EXISTS cb_fail_step(text, text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_step(text, text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_hide_steps(text, text, bigint[], integer);
DROP FUNCTION IF EXISTS cb_read_steps(text, text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_start_steps(text, bigint);
DROP FUNCTION IF EXISTS cb_run_flow(text, jsonb, text);
DROP FUNCTION IF EXISTS cb_run_flow(text, jsonb, text, text);
DROP FUNCTION IF EXISTS cb_delete_flow(text);
DROP FUNCTION IF EXISTS cb_create_flow(text, jsonb);
DROP FUNCTION IF EXISTS cb_fail_task(text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_task(text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_hide_tasks(text, bigint[], integer);
DROP FUNCTION IF EXISTS cb_read_tasks(text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_run_task(text, jsonb, text);
DROP FUNCTION IF EXISTS cb_run_task(text, jsonb, text, text);
DROP FUNCTION IF EXISTS cb_delete_task(text);
DROP FUNCTION IF EXISTS cb_create_task(text);
DROP FUNCTION IF EXISTS cb_check_reconvergence(text, jsonb);
DROP FUNCTION IF EXISTS cb_evaluate_condition_expr(text, jsonb);
DROP FUNCTION IF EXISTS cb_evaluate_condition(jsonb, jsonb);
DROP FUNCTION IF EXISTS cb_get_jsonb_field(jsonb, text);
DROP FUNCTION IF EXISTS cb_parse_condition_value(text, text);
DROP FUNCTION IF EXISTS cb_parse_condition(text);
