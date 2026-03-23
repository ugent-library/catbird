-- IgnoreOutput: allow steps to declare ordering-only dependencies
-- whose output should not be fetched at claim time.

-- +goose up

-- Add ignore_output flag to step dependencies definition table
ALTER TABLE cb_step_dependencies ADD COLUMN IF NOT EXISTS ignore_output boolean NOT NULL DEFAULT false;

-- +goose statementbegin
-- Add ignore_output_step_names column to existing dynamic step run tables
DO $$
DECLARE
    _flow record;
    _s_table text;
BEGIN
    FOR _flow IN SELECT name FROM cb_flows LOOP
        _s_table := 'cb_s_' || _flow.name;
        IF to_regclass(_s_table) IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE %I ADD COLUMN IF NOT EXISTS ignore_output_step_names text[] NOT NULL DEFAULT ''{}''::text[]',
                _s_table
            );
        END IF;
    END LOOP;
END$$;
-- +goose statementend

-- +goose statementbegin

-- Rewrite cb_create_flow to:
-- 1. Parse "ignore_output" from step JSON and set ignore_output=true in cb_step_dependencies
-- 2. Add ignore_output_step_names column to dynamic cb_s_{name} table
CREATE OR REPLACE FUNCTION cb_create_flow(name text, description text DEFAULT NULL, steps jsonb DEFAULT '[]'::jsonb, output_priority text[] DEFAULT NULL, retention_period interval DEFAULT NULL)
RETURNS void AS $$
DECLARE
    _step jsonb;
    _dep jsonb;
    _step_name text;
    _dep_name text;
    _step_type text;
    _map_source_step_name text;
    _reduce_source_step_name text;
    _has_map_source_dependency boolean;
    _has_reduce_source_dependency boolean;
    _idx int := 0;
    _dep_idx int;
    _f_table text;
    _s_table text;
    _m_table text;
    _condition jsonb;
    _output_priority text[];
    _output_step text;
    _terminal_step text;
    _terminal_count int;
    _priority_count int;
    _priority_distinct_count int;
    _create_table_stmt text;
    _ignored_output jsonb;
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

    IF cb_create_flow.output_priority IS NOT NULL THEN
        IF cardinality(cb_create_flow.output_priority) = 0 THEN
            RAISE EXCEPTION 'cb: output priority must not be empty';
        END IF;

        _output_priority := cb_create_flow.output_priority;
    ELSE
        _output_priority := NULL;
    END IF;

    _f_table := cb_table_name(cb_create_flow.name, 'f');
    _s_table := cb_table_name(cb_create_flow.name, 's');
    _m_table := cb_table_name(cb_create_flow.name, 'm');

    PERFORM pg_advisory_xact_lock(hashtext(_f_table));

    _create_table_stmt := 'CREATE TABLE';

    INSERT INTO cb_flows (name, description, output_priority, step_count, retention_period)
    VALUES (cb_create_flow.name, cb_create_flow.description, coalesce(_output_priority, ARRAY['__pending_output_priority__']), 0, cb_create_flow.retention_period)
    ON CONFLICT ON CONSTRAINT cb_flows_pkey DO UPDATE
    SET description = EXCLUDED.description,
        output_priority = coalesce(EXCLUDED.output_priority, ARRAY['__pending_output_priority__']),
        step_count = 0,
        retention_period = EXCLUDED.retention_period;

    IF to_regclass('cb_step_handlers') IS NOT NULL THEN
      EXECUTE 'DELETE FROM cb_step_handlers WHERE flow_name = $1'
      USING cb_create_flow.name;
    END IF;

    BEGIN
      DELETE FROM cb_step_dependencies WHERE flow_name = cb_create_flow.name;
      DELETE FROM cb_steps WHERE flow_name = cb_create_flow.name;
    EXCEPTION WHEN foreign_key_violation THEN
      IF to_regclass('cb_step_handlers') IS NOT NULL THEN
        EXECUTE 'DELETE FROM cb_step_handlers WHERE flow_name = $1'
        USING cb_create_flow.name;
      END IF;
      DELETE FROM cb_step_dependencies WHERE flow_name = cb_create_flow.name;
      DELETE FROM cb_steps WHERE flow_name = cb_create_flow.name;
    END;

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

        _step_type := coalesce(nullif(_step->>'step_type', ''), 'normal');

        IF _step_type NOT IN ('normal', 'mapper', 'generator', 'reducer') THEN
            RAISE EXCEPTION 'cb: step "%" has invalid step_type "%"', _step_name, _step_type;
        END IF;

        _map_source_step_name := nullif(_step->>'map_source_step_name', '');
        _reduce_source_step_name := nullif(_step->>'reduce_source_step_name', '');

        IF _step_type <> 'mapper' AND _map_source_step_name IS NOT NULL THEN
          RAISE EXCEPTION 'cb: step "%" has map_source_step_name but step_type is not mapper', _step_name;
        END IF;

        IF _step_type <> 'reducer' AND _reduce_source_step_name IS NOT NULL THEN
          RAISE EXCEPTION 'cb: step "%" has reduce_source_step_name but step_type is not reducer', _step_name;
        END IF;

        -- Map input mode: step_type=mapper and map_source_step_name omitted/null.
        -- Map dependency mode: step_type=mapper and map_source_step_name set to a dependency step name.
        IF _step_type = 'mapper' AND _map_source_step_name IS NULL THEN
            -- map-input mode, valid
            NULL;
        END IF;

        IF _step_type = 'mapper' AND _map_source_step_name = _step_name THEN
            RAISE EXCEPTION 'cb: step "%" cannot map its own output', _step_name;
        END IF;

        IF _step_type = 'reducer' AND _reduce_source_step_name IS NULL THEN
          RAISE EXCEPTION 'cb: reducer step "%" must specify reduce_source_step_name', _step_name;
        END IF;

        IF _step_type = 'reducer' AND _reduce_source_step_name = _step_name THEN
            RAISE EXCEPTION 'cb: step "%" cannot reduce its own output', _step_name;
        END IF;

        IF _map_source_step_name IS NOT NULL THEN
          IF NOT _map_source_step_name ~ '^[a-z0-9_]+$' THEN
            RAISE EXCEPTION 'cb: map source "%" contains invalid characters, allowed: a-z, 0-9, _', _map_source_step_name;
            END IF;

            SELECT EXISTS(
                SELECT 1
                FROM jsonb_array_elements(coalesce(_step->'depends_on', '[]'::jsonb)) d
                WHERE d->>'name' = _map_source_step_name
            ) INTO _has_map_source_dependency;

            IF NOT _has_map_source_dependency THEN
                RAISE EXCEPTION 'cb: step "%" maps "%" but does not depend on it', _step_name, _map_source_step_name;
            END IF;
        END IF;

            IF _reduce_source_step_name IS NOT NULL THEN
              IF NOT _reduce_source_step_name ~ '^[a-z0-9_]+$' THEN
                RAISE EXCEPTION 'cb: reduce source step "%" contains invalid characters, allowed: a-z, 0-9, _', _reduce_source_step_name;
            END IF;

            SELECT EXISTS(
                SELECT 1
                FROM jsonb_array_elements(coalesce(_step->'depends_on', '[]'::jsonb)) d
                WHERE d->>'name' = _reduce_source_step_name
            ) INTO _has_reduce_source_dependency;

            IF NOT _has_reduce_source_dependency THEN
                RAISE EXCEPTION 'cb: step "%" reduces "%" but does not depend on it', _step_name, _reduce_source_step_name;
            END IF;
        END IF;

        -- Extract condition from step if present
        _condition := NULL;
        IF _step ? 'condition' AND _step->>'condition' IS NOT NULL AND _step->>'condition' != '' THEN
            _condition := cb_parse_condition(_step->>'condition');
        END IF;

        INSERT INTO cb_steps (flow_name, name, description, idx, dependency_count, step_type, map_source_step_name, reduce_source_step_name, signal, condition)
        VALUES (
            cb_create_flow.name,
            _step_name,
            nullif(_step->>'description', ''),
            _idx,
            jsonb_array_length(coalesce(_step->'depends_on', '[]'::jsonb)),
            _step_type,
            _map_source_step_name,
            _reduce_source_step_name,
            coalesce((_step->>'signal')::boolean, false),
            _condition
        );

        -- Build set of ignored output dependency names for this step
        _ignored_output := coalesce(_step->'ignore_output', '[]'::jsonb);

        _dep_idx := 0;
        FOR _dep IN SELECT jsonb_array_elements(coalesce(_step->'depends_on', '[]'::jsonb))
        LOOP
            _dep_name := _dep->>'name';

            INSERT INTO cb_step_dependencies (flow_name, step_name, dependency_step_name, idx, ignore_output)
            VALUES (
                cb_create_flow.name,
                _step_name,
                _dep_name,
                _dep_idx,
                EXISTS (SELECT 1 FROM jsonb_array_elements_text(_ignored_output) io WHERE io = _dep_name)
            );
            _dep_idx := _dep_idx + 1;
        END LOOP;

        _idx := _idx + 1;
    END LOOP;

    -- SQL-level map-step validation (independent from Go-side builder checks)
    IF EXISTS (
        SELECT 1
        FROM cb_steps s
        WHERE s.flow_name = cb_create_flow.name
        AND s.step_type = 'mapper'
            AND s.map_source_step_name IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM cb_step_dependencies d
                WHERE d.flow_name = s.flow_name
                    AND d.step_name = s.name
                AND d.dependency_step_name = s.map_source_step_name
            )
    ) THEN
          RAISE EXCEPTION 'cb: map step must depend on its map_source_step_name (flow=%)', cb_create_flow.name;
    END IF;

    -- SQL-level reducer validation
    IF EXISTS (
      SELECT 1
      FROM cb_steps s
      WHERE s.flow_name = cb_create_flow.name
        AND s.step_type = 'reducer'
        AND (
          s.reduce_source_step_name IS NULL
          OR NOT EXISTS (
            SELECT 1
            FROM cb_step_dependencies d
            WHERE d.flow_name = s.flow_name
              AND d.step_name = s.name
              AND d.dependency_step_name = s.reduce_source_step_name
          )
        )
    ) THEN
      RAISE EXCEPTION 'cb: reducer step must depend on its reduce_source_step_name (flow=%)', cb_create_flow.name;
    END IF;

    -- Output ownership validation
    SELECT count(*)
    INTO _terminal_count
    FROM cb_steps s
    WHERE s.flow_name = cb_create_flow.name
      AND NOT EXISTS (
        SELECT 1
        FROM cb_step_dependencies d
        WHERE d.flow_name = s.flow_name
            AND d.dependency_step_name = s.name
      );

    IF _terminal_count = 0 THEN
        RAISE EXCEPTION 'cb: flow % has no final step (circular dependency?)', cb_create_flow.name;
    END IF;

    IF _output_priority IS NULL THEN
        SELECT array_agg(s.name ORDER BY s.idx)
        INTO _output_priority
        FROM cb_steps s
        WHERE s.flow_name = cb_create_flow.name
            AND NOT EXISTS (
                SELECT 1
                FROM cb_step_dependencies d
                WHERE d.flow_name = s.flow_name
                        AND d.dependency_step_name = s.name
            );
    END IF;

    SELECT value
    INTO _output_step
    FROM unnest(_output_priority) AS p(value)
    WHERE value = ''
    LIMIT 1;

    IF _output_step IS NOT NULL THEN
        RAISE EXCEPTION 'cb: output priority contains empty step name';
    END IF;

    SELECT value
    INTO _output_step
    FROM unnest(_output_priority) AS p(value)
    WHERE NOT EXISTS (
        SELECT 1
        FROM cb_steps s
        WHERE s.flow_name = cb_create_flow.name
            AND s.name = p.value
    )
    LIMIT 1;

    IF _output_step IS NOT NULL THEN
        RAISE EXCEPTION 'cb: output priority references unknown step "%"', _output_step;
    END IF;

    SELECT count(*), count(DISTINCT value)
    INTO _priority_count, _priority_distinct_count
    FROM unnest(_output_priority) AS p(value);

    IF _priority_count <> _priority_distinct_count THEN
        RAISE EXCEPTION 'cb: output priority contains duplicate step names';
    END IF;

    SELECT s.name
    INTO _terminal_step
    FROM cb_steps s
    WHERE s.flow_name = cb_create_flow.name
        AND NOT EXISTS (
            SELECT 1
            FROM cb_step_dependencies d
            WHERE d.flow_name = s.flow_name
                AND d.dependency_step_name = s.name
        )
        AND NOT EXISTS (
            SELECT 1
            FROM unnest(_output_priority) AS p(value)
            WHERE p.value = s.name
        )
    LIMIT 1;

    IF _terminal_step IS NOT NULL THEN
        RAISE EXCEPTION 'cb: output priority must include structural terminal step "%"', _terminal_step;
    END IF;

    UPDATE cb_flows f
    SET output_priority = _output_priority,
        step_count = _idx
    WHERE f.name = cb_create_flow.name;

    EXECUTE format(
    $QUERY$
    %s IF NOT EXISTS %I (
      id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
      concurrency_key text,
      idempotency_key text,
      status text NOT NULL DEFAULT 'started',
      remaining_steps int NOT NULL DEFAULT 0,
      input jsonb NOT NULL,
      headers jsonb,
      output_priority text[] NOT NULL,
      output jsonb,
      error_message text,
      failed_step_name text,
      failed_step_id bigint,
      failed_step_input jsonb,
      failed_step_signal_input jsonb,
      failed_step_attempts int,
      on_fail_status text,
      on_fail_attempts int NOT NULL DEFAULT 0,
      on_fail_visible_at timestamptz,
      on_fail_error_message text,
      on_fail_started_at timestamptz,
      on_fail_completed_at timestamptz,
      started_at timestamptz NOT NULL DEFAULT now(),
      cancel_requested_at timestamptz,
      completed_at timestamptz,
      failed_at timestamptz,
      canceled_at timestamptz,
      priority int NOT NULL DEFAULT 0,
      cancel_reason text,
      CONSTRAINT cb_status_valid CHECK (status IN ('started', 'canceling', 'completed', 'failed', 'canceled')),
      CONSTRAINT cb_remaining_steps_valid CHECK (remaining_steps >= 0),
      CONSTRAINT cb_completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
      CONSTRAINT cb_canceled_terminal_exclusive CHECK (NOT (canceled_at IS NOT NULL AND (completed_at IS NOT NULL OR failed_at IS NOT NULL))),
      CONSTRAINT cb_completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
      CONSTRAINT cb_failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
      CONSTRAINT cb_canceled_at_is_after_started_at CHECK (canceled_at IS NULL OR canceled_at >= started_at),
      CONSTRAINT cb_completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
      CONSTRAINT cb_output_priority_not_empty CHECK (cardinality(output_priority) > 0),
      CONSTRAINT cb_failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = ''))),
      CONSTRAINT cb_on_fail_status_valid CHECK (on_fail_status IS NULL OR on_fail_status IN ('queued', 'started', 'completed', 'failed')),
      CONSTRAINT cb_headers_is_object CHECK (headers IS NULL OR jsonb_typeof(headers) = 'object')
    )
    $QUERY$,
    _create_table_stmt,
    _f_table
    );

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (status);', _f_table || '_status_idx', _f_table);
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (concurrency_key) WHERE concurrency_key IS NOT NULL AND status = ''started''', _f_table || '_concurrency_key_idx', _f_table);
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN (''started'', ''completed'')', _f_table || '_idempotency_key_idx', _f_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (on_fail_visible_at, id) WHERE status = ''failed'' AND on_fail_status IN (''queued'', ''failed'');', _f_table || '_on_fail_poll_idx', _f_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (GREATEST(completed_at, failed_at, canceled_at)) WHERE status IN (''completed'', ''failed'', ''canceled'');', _f_table || '_retention_idx', _f_table);

    -- Create step runs table - includes 'skipped' in status constraint
    EXECUTE format(
    $QUERY$
    %s IF NOT EXISTS %I (
      id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
      flow_run_id bigint NOT NULL,
      step_name text NOT NULL,
      flow_input jsonb NOT NULL DEFAULT '{}'::jsonb,
      status text NOT NULL DEFAULT 'waiting_for_dependencies',
      condition jsonb,
      step_type text NOT NULL DEFAULT 'normal',
      map_source_step_name text,
      reduce_source_step_name text,
      signal boolean NOT NULL DEFAULT false,
      dependency_step_names text[] NOT NULL DEFAULT '{}'::text[],
      dependent_step_names text[] NOT NULL DEFAULT '{}'::text[],
      ignore_output_step_names text[] NOT NULL DEFAULT '{}'::text[],
      generator_status text,
      map_tasks_spawned int NOT NULL DEFAULT 0,
      map_tasks_completed int NOT NULL DEFAULT 0,
      generator_error text,
      attempts int NOT NULL DEFAULT 0,
      output jsonb,
      error_message text,
      signal_input jsonb,
      remaining_dependencies int NOT NULL DEFAULT 0,
      priority int NOT NULL DEFAULT 0,
      visible_at timestamptz NOT NULL DEFAULT now(),
      created_at timestamptz NOT NULL DEFAULT now(),
      started_at timestamptz,
      completed_at timestamptz,
      failed_at timestamptz,
      skipped_at timestamptz,
      canceled_at timestamptz,
      UNIQUE (flow_run_id, step_name),
      FOREIGN KEY (flow_run_id) REFERENCES %I (id) ON DELETE CASCADE,
      CONSTRAINT cb_status_valid CHECK (status IN ('waiting_for_dependencies', 'waiting_for_signal', 'queued', 'started', 'waiting_for_map_tasks', 'completed', 'failed', 'skipped', 'canceled')),
      CONSTRAINT cb_step_type_valid CHECK (step_type IN ('normal', 'mapper', 'generator', 'reducer')),
      CONSTRAINT cb_generator_status_valid CHECK (generator_status IS NULL OR generator_status IN ('started', 'complete', 'failed')),
      CONSTRAINT cb_map_tasks_spawned_valid CHECK (map_tasks_spawned >= 0),
      CONSTRAINT cb_map_tasks_completed_valid CHECK (map_tasks_completed >= 0 AND map_tasks_completed <= map_tasks_spawned),
      CONSTRAINT cb_remaining_dependencies_valid CHECK (remaining_dependencies >= 0),
      CONSTRAINT cb_completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
      CONSTRAINT cb_skipped_and_completed_failed CHECK (NOT (skipped_at IS NOT NULL AND (completed_at IS NOT NULL OR failed_at IS NOT NULL))),
      CONSTRAINT cb_attempts_valid CHECK (attempts >= 0)
    )
    $QUERY$,
    _create_table_stmt, _s_table, _f_table
    );

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (flow_run_id, status);', _s_table || '_flow_run_id_status_idx', _s_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (step_name, visible_at, priority DESC, id) WHERE status IN (''queued'', ''started'');', _s_table || '_poll_step_visible_id_idx', _s_table);
    EXECUTE format('DROP INDEX IF EXISTS %I;', _s_table || '_visible_at_idx');

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (status, generator_status) WHERE status IN (''waiting_for_dependencies'', ''waiting_for_signal'', ''started'', ''waiting_for_map_tasks'');', _s_table || '_generator_status_idx', _s_table);

    -- Create map task table used by map steps for per-item coordination
    EXECUTE format(
    $QUERY$
    %s IF NOT EXISTS %I (
      id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
      flow_run_id bigint NOT NULL,
      step_name text NOT NULL,
      item_idx int NOT NULL,
      flow_input jsonb NOT NULL DEFAULT '{}'::jsonb,
      status text NOT NULL DEFAULT 'queued',
      dependency_step_names text[] NOT NULL DEFAULT '{}'::text[],
      signal_input jsonb,
      attempts int NOT NULL DEFAULT 0,
      item jsonb NOT NULL,
      output jsonb,
      error_message text,
      priority int NOT NULL DEFAULT 0,
      visible_at timestamptz NOT NULL DEFAULT now(),
      created_at timestamptz NOT NULL DEFAULT now(),
      started_at timestamptz,
      completed_at timestamptz,
      failed_at timestamptz,
      canceled_at timestamptz,
      UNIQUE (flow_run_id, step_name, item_idx),
      FOREIGN KEY (flow_run_id, step_name) REFERENCES %I (flow_run_id, step_name) ON DELETE CASCADE,
      CONSTRAINT cb_status_valid CHECK (status IN ('queued', 'started', 'completed', 'failed', 'canceled')),
      CONSTRAINT cb_item_idx_valid CHECK (item_idx >= 0),
      CONSTRAINT cb_attempts_valid CHECK (attempts >= 0)
    )
    $QUERY$,
    _create_table_stmt, _m_table, _s_table
    );

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (flow_run_id, step_name, status);', _m_table || '_flow_run_id_step_name_status_idx', _m_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (step_name, visible_at, priority DESC, id) WHERE status IN (''queued'', ''started'');', _m_table || '_poll_step_visible_id_idx', _m_table);
    EXECUTE format('DROP INDEX IF EXISTS %I;', _m_table || '_visible_at_idx');
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin

-- Rewrite cb_run_flow to:
-- 1. Filter dependency_step_names to exclude ignore_output deps
-- 2. Populate ignore_output_step_names from cb_step_dependencies where ignore_output = true
CREATE OR REPLACE FUNCTION cb_run_flow(
    name text,
    input jsonb,
    concurrency_key text DEFAULT NULL,
    idempotency_key text DEFAULT NULL,
    headers jsonb DEFAULT NULL,
    visible_at timestamptz DEFAULT NULL,
    priority int DEFAULT 0
)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := cb_table_name(cb_run_flow.name, 'f');
    _s_table text := cb_table_name(cb_run_flow.name, 's');
    _id bigint;
    _remaining_steps int;
    _output_priority text[];
BEGIN
    -- Validate: both keys cannot be set simultaneously
    IF cb_run_flow.concurrency_key IS NOT NULL AND cb_run_flow.idempotency_key IS NOT NULL THEN
        RAISE EXCEPTION 'cb: cannot specify both concurrency_key and idempotency_key';
    END IF;

    IF cb_run_flow.headers IS NOT NULL AND jsonb_typeof(cb_run_flow.headers) <> 'object' THEN
      RAISE EXCEPTION 'cb: headers must be a JSON object';
    END IF;

    -- Read run initialization metadata once.
    SELECT f.step_count, f.output_priority
    INTO _remaining_steps, _output_priority
    FROM cb_flows f
    WHERE f.name = cb_run_flow.name;

    -- Create flow run or get existing run with same key
    -- ON CONFLICT DO UPDATE with WHERE FALSE: atomic insert + return conflicting row ID.
    -- Use UNION ALL to handle both INSERT success and conflict cases atomically.
    -- Pattern from: https://stackoverflow.com/a/35953488
    IF cb_run_flow.concurrency_key IS NOT NULL THEN
        -- Concurrency control: dedupe only started
        EXECUTE format(
            $QUERY$
            WITH ins AS (
              INSERT INTO %I (concurrency_key, input, headers, remaining_steps, output_priority, status, priority)
              VALUES ($1, $2, $3, $4, $5, 'started', $6)
                ON CONFLICT (concurrency_key) WHERE concurrency_key IS NOT NULL AND status = 'started'
                DO UPDATE SET status = EXCLUDED.status WHERE FALSE
                RETURNING id
            )
            SELECT id FROM ins
            UNION ALL
            SELECT id FROM %I
            WHERE concurrency_key = $1 AND concurrency_key IS NOT NULL AND status = 'started'
            LIMIT 1
            $QUERY$,
            _f_table, _f_table
        )
        USING cb_run_flow.concurrency_key, cb_run_flow.input, cb_run_flow.headers, _remaining_steps, _output_priority, cb_run_flow.priority
        INTO _id;
    ELSIF cb_run_flow.idempotency_key IS NOT NULL THEN
        -- Idempotency: dedupe started/completed
        EXECUTE format(
            $QUERY$
            WITH ins AS (
              INSERT INTO %I (idempotency_key, input, headers, remaining_steps, output_priority, status, priority)
              VALUES ($1, $2, $3, $4, $5, 'started', $6)
                ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('started', 'completed')
                DO UPDATE SET status = EXCLUDED.status WHERE FALSE
                RETURNING id
            )
            SELECT id FROM ins
            UNION ALL
            SELECT id FROM %I
            WHERE idempotency_key = $1 AND idempotency_key IS NOT NULL AND status IN ('started', 'completed')
            LIMIT 1
            $QUERY$,
            _f_table, _f_table
        )
        USING cb_run_flow.idempotency_key, cb_run_flow.input, cb_run_flow.headers, _remaining_steps, _output_priority, cb_run_flow.priority
        INTO _id;
    ELSE
        -- No deduplication
        EXECUTE format(
            $QUERY$
          INSERT INTO %I (input, headers, remaining_steps, output_priority, status, priority)
          VALUES ($1, $2, $3, $4, 'started', $5)
            RETURNING id
            $QUERY$,
            _f_table
        )
        USING cb_run_flow.input, cb_run_flow.headers, _remaining_steps, _output_priority, cb_run_flow.priority
        INTO _id;
    END IF;

    -- Create step runs for all steps in a single INSERT
    EXECUTE format(
    $QUERY$
    INSERT INTO %I (
      flow_run_id,
      step_name,
      flow_input,
      status,
      remaining_dependencies,
      condition,
      step_type,
      map_source_step_name,
      reduce_source_step_name,
      signal,
      dependency_step_names,
      dependent_step_names,
      ignore_output_step_names,
      priority
    )
    SELECT
      %L,
      s.name,
      %L,
      CASE
        WHEN s.dependency_count = 0 AND s.signal THEN 'waiting_for_signal'
        ELSE 'waiting_for_dependencies'
      END,
      s.dependency_count,
      s.condition,
      s.step_type,
      s.map_source_step_name,
      s.reduce_source_step_name,
      s.signal,
      coalesce(array(
        SELECT d.dependency_step_name
        FROM cb_step_dependencies d
        WHERE d.flow_name = %L
          AND d.step_name = s.name
          AND d.ignore_output = false
        ORDER BY d.idx
      ), '{}'::text[]),
      coalesce(array(
        SELECT d.step_name
        FROM cb_step_dependencies d
        WHERE d.flow_name = %L
          AND d.dependency_step_name = s.name
        ORDER BY d.idx
      ), '{}'::text[]),
      coalesce(array(
        SELECT d.dependency_step_name
        FROM cb_step_dependencies d
        WHERE d.flow_name = %L
          AND d.step_name = s.name
          AND d.ignore_output = true
        ORDER BY d.idx
      ), '{}'::text[]),
      %L
    FROM cb_steps s
    WHERE s.flow_name = %L
    ON CONFLICT (flow_run_id, step_name) DO NOTHING
    $QUERY$,
    _s_table,
    _id,
    cb_run_flow.input,
    cb_run_flow.name,
    cb_run_flow.name,
    cb_run_flow.name,
    cb_run_flow.priority,
    cb_run_flow.name
    );

    -- Start steps with no dependencies
    PERFORM cb_start_steps(cb_run_flow.name, _id, cb_run_flow.visible_at);

    PERFORM pg_notify(current_schema || '.cb_s_' || cb_run_flow.name, to_char(coalesce(cb_run_flow.visible_at, now()) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));

    RETURN _id;
END;
$$;
-- +goose statementend

-- +goose down

-- +goose statementbegin
-- Remove ignore_output_step_names from existing dynamic step run tables
DO $$
DECLARE
    _flow record;
    _s_table text;
BEGIN
    FOR _flow IN SELECT name FROM cb_flows LOOP
        _s_table := 'cb_s_' || _flow.name;
        IF to_regclass(_s_table) IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE %I DROP COLUMN IF EXISTS ignore_output_step_names',
                _s_table
            );
        END IF;
    END LOOP;
END$$;
-- +goose statementend

-- Remove ignore_output column from cb_step_dependencies
ALTER TABLE cb_step_dependencies DROP COLUMN IF EXISTS ignore_output;
