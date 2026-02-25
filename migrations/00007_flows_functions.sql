-- Flow execution functions

-- CONCURRENCY AUDIT:
-- Setup functions (initialization, not hot path):
--   cb_create_flow(): Uses pg_advisory_xact_lock, safe for concurrent creation (first-wins)
-- Hot path functions (millions of executions, no advisory locks):
--   cb_run_flow(): Uses atomic CTE + UNION ALL pattern with ON CONFLICT DO UPDATE WHERE FALSE - SAFE
--     Critical: MUST use UNION ALL fallback, not bare RETURNING (returns NULL on conflict without it)
--   cb_poll_steps(): Uses FOR UPDATE SKIP LOCKED for lock-free row polling - SAFE
--   cb_activate_steps(): Lock-free condition evaluation + FOR UPDATE SKIP LOCKED - SAFE
--   cb_hide_steps(): Direct UPDATE indexed by visible_at, no locks - SAFE
-- All operations maintain flow state invariants and step ordering
-- Deduplication pattern reference: https://stackoverflow.com/a/35953488

-- +goose up

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
    _is_map_step boolean;
    _map_source text;
    _has_map_source_dependency boolean;
    _idx int := 0;
    _dep_idx int;
    _f_table text;
    _s_table text;
    _m_table text;
    _condition jsonb;
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
    _m_table := cb_table_name(cb_create_flow.name, 'm');

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

    _is_map_step := coalesce((_step->>'is_map_step')::boolean, false);
    _map_source := nullif(_step->>'map_source', '');

    IF NOT _is_map_step AND _map_source IS NOT NULL THEN
      RAISE EXCEPTION 'cb: step "%" has map_source but is_map_step is false', _step_name;
    END IF;

    -- Map input mode: is_map_step=true and map_source omitted/null.
    -- Map dependency mode: is_map_step=true and map_source set to a dependency step name.
    IF _is_map_step AND _map_source IS NULL THEN
      -- map-input mode, valid
      NULL;
    END IF;

    IF _is_map_step AND _map_source = _step_name THEN
      RAISE EXCEPTION 'cb: step "%" cannot map its own output', _step_name;
    END IF;

    IF _map_source IS NOT NULL THEN
      IF NOT _map_source ~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'cb: map source "%" contains invalid characters, allowed: a-z, 0-9, _', _map_source;
      END IF;

      SELECT EXISTS(
        SELECT 1
        FROM jsonb_array_elements(coalesce(_step->'depends_on', '[]'::jsonb)) d
        WHERE d->>'name' = _map_source
      ) INTO _has_map_source_dependency;

      IF NOT _has_map_source_dependency THEN
        RAISE EXCEPTION 'cb: step "%" maps "%" but does not depend on it', _step_name, _map_source;
      END IF;
    END IF;

    -- Extract condition from step if present
    _condition := NULL;
    IF _step ? 'condition' AND _step->>'condition' IS NOT NULL AND _step->>'condition' != '' THEN
      _condition := cb_parse_condition(_step->>'condition');
    END IF;

    INSERT INTO cb_steps (flow_name, name, idx, dependency_count, is_map_step, map_source, has_signal, condition)
    VALUES (
      cb_create_flow.name,
      _step_name,
      _idx,
      jsonb_array_length(coalesce(_step->'depends_on', '[]'::jsonb)),
      _is_map_step,
      _map_source,
      coalesce((_step->>'has_signal')::boolean, false),
      _condition
    );

    _dep_idx := 0;
    FOR _dep IN SELECT jsonb_array_elements(coalesce(_step->'depends_on', '[]'::jsonb))
    LOOP
      _dep_name := _dep->>'name';
      
      INSERT INTO cb_step_dependencies (flow_name, step_name, dependency_name, idx)
      VALUES (cb_create_flow.name, _step_name, _dep_name, _dep_idx);
      _dep_idx := _dep_idx + 1;
    END LOOP;

    _idx := _idx + 1;
    END LOOP;

    -- SQL-level map-step validation (independent from Go-side builder checks)
    IF EXISTS (
      SELECT 1
      FROM cb_steps s
      WHERE s.flow_name = cb_create_flow.name
        AND s.is_map_step = true
        AND s.map_source IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM cb_step_dependencies d
          WHERE d.flow_name = s.flow_name
            AND d.step_name = s.name
            AND d.dependency_name = s.map_source
        )
    ) THEN
      RAISE EXCEPTION 'cb: map step must depend on its map_source (flow=%)', cb_create_flow.name;
    END IF;

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
      attempts int NOT NULL DEFAULT 0,
      output jsonb,
      error_message text,
      signal_input jsonb,
      remaining_dependencies int NOT NULL DEFAULT 0,
      visible_at timestamptz NOT NULL DEFAULT now(),
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
      CONSTRAINT attempts_valid CHECK (attempts >= 0)
    )
    $QUERY$,
    _s_table, _f_table
    );

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (flow_run_id, status);', _s_table || '_flow_run_id_status_idx', _s_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (visible_at);', _s_table || '_visible_at_idx', _s_table);

    -- Create map task table used by map steps for per-item coordination
    EXECUTE format(
    $QUERY$
    CREATE TABLE IF NOT EXISTS %I (
      id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
      flow_run_id bigint NOT NULL,
      step_name text NOT NULL,
      item_idx int NOT NULL,
      status text NOT NULL DEFAULT 'created',
      attempts int NOT NULL DEFAULT 0,
      item jsonb NOT NULL,
      output jsonb,
      error_message text,
      visible_at timestamptz NOT NULL DEFAULT now(),
      created_at timestamptz NOT NULL DEFAULT now(),
      started_at timestamptz,
      completed_at timestamptz,
      failed_at timestamptz,
      UNIQUE (flow_run_id, step_name, item_idx),
      FOREIGN KEY (flow_run_id, step_name) REFERENCES %I (flow_run_id, step_name),
      CONSTRAINT status_valid CHECK (status IN ('created', 'started', 'completed', 'failed')),
      CONSTRAINT item_idx_valid CHECK (item_idx >= 0),
      CONSTRAINT attempts_valid CHECK (attempts >= 0)
    )
    $QUERY$,
    _m_table, _s_table
    );

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (step_name, status, visible_at);', _m_table || '_step_status_visible_idx', _m_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (flow_run_id, step_name, status);', _m_table || '_flow_step_status_idx', _m_table);
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
    idempotency_key text = NULL,
    visible_at timestamptz = NULL
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
    -- ON CONFLICT DO UPDATE with WHERE FALSE: atomic insert + return conflicting row ID.
    -- Use UNION ALL to handle both INSERT success and conflict cases atomically.
    -- Pattern from: https://stackoverflow.com/a/35953488
    IF cb_run_flow.concurrency_key IS NOT NULL THEN
        -- Concurrency control: dedupe only started
        EXECUTE format(
            $QUERY$
            WITH ins AS (
                INSERT INTO %I (concurrency_key, input, remaining_steps, status)
                VALUES ($1, $2, $3, 'started')
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
        USING cb_run_flow.concurrency_key, cb_run_flow.input, _remaining_steps
        INTO _id;
    ELSIF cb_run_flow.idempotency_key IS NOT NULL THEN
        -- Idempotency: dedupe started/completed
        EXECUTE format(
            $QUERY$
            WITH ins AS (
                INSERT INTO %I (idempotency_key, input, remaining_steps, status)
                VALUES ($1, $2, $3, 'started')
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
        USING cb_run_flow.idempotency_key, cb_run_flow.input, _remaining_steps
        INTO _id;
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
    PERFORM cb_start_steps(cb_run_flow.name, _id, cb_run_flow.visible_at);

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
CREATE OR REPLACE FUNCTION cb_start_steps(flow_name text, flow_run_id bigint, initial_visible_at timestamptz DEFAULT NULL)
RETURNS void AS $$
DECLARE
    _f_table text := cb_table_name(cb_start_steps.flow_name, 'f');
    _s_table text := cb_table_name(cb_start_steps.flow_name, 's');
    _m_table text := cb_table_name(cb_start_steps.flow_name, 'm');
    _flow_input jsonb;
    _step_to_process record;
    _step_condition jsonb;
    _step_inputs jsonb;
    _is_condition_true boolean;
    _map_items jsonb;
    _map_item_count int;
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
             (SELECT condition FROM cb_steps WHERE flow_name = $3 AND name = sr.step_name) AS condition,
             (SELECT is_map_step FROM cb_steps WHERE flow_name = $3 AND name = sr.step_name) AS is_map_step,
             (SELECT map_source FROM cb_steps WHERE flow_name = $3 AND name = sr.step_name) AS map_source
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
      -- Flow input accessible as input.*, dependency outputs as dependency_name.*, signal input as signal.*
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
        EXECUTE format(
          $QUERY$
          UPDATE %I
          SET remaining_steps = remaining_steps - 1
          WHERE id = $1 AND status = 'started'
          $QUERY$,
          _f_table
        )
        USING cb_start_steps.flow_run_id;
        
        _steps_processed_this_iteration := _steps_processed_this_iteration + 1;
        
        -- Continue to next step
        CONTINUE;
      END IF;
    END IF;

    -- Map-step activation: spawn item-level map tasks coordinated in SQL
    IF _step_to_process.is_map_step THEN
      IF _step_to_process.map_source IS NULL THEN
        _map_items := _flow_input;
      ELSE
        _map_items := _step_to_process.step_outputs -> _step_to_process.map_source;
      END IF;

      IF _map_items IS NULL OR jsonb_typeof(_map_items) <> 'array' THEN
        PERFORM cb_fail_step(
          cb_start_steps.flow_name,
          _step_to_process.step_name,
          _step_to_process.id,
          format('map source for step %s must be a JSON array', _step_to_process.step_name)
        );
        _steps_processed_this_iteration := _steps_processed_this_iteration + 1;
        CONTINUE;
      END IF;

      _map_item_count := jsonb_array_length(_map_items);

      -- Mark parent step started
      EXECUTE format(
        $QUERY$
        UPDATE %I
        SET status = 'started',
            started_at = now(),
            visible_at = coalesce($2, now())
        WHERE id = $1
        $QUERY$,
        _s_table
      )
      USING _step_to_process.id, cb_start_steps.initial_visible_at;

      -- Spawn map tasks in deterministic order
      EXECUTE format(
        $QUERY$
        INSERT INTO %I (flow_run_id, step_name, item_idx, status, item, visible_at)
        SELECT $1,
               $2,
               ordinality - 1,
               'created',
               value,
               coalesce($3, now())
        FROM jsonb_array_elements($4) WITH ORDINALITY
        ON CONFLICT (flow_run_id, step_name, item_idx) DO NOTHING
        $QUERY$,
        _m_table
      )
      USING cb_start_steps.flow_run_id, _step_to_process.step_name, cb_start_steps.initial_visible_at, _map_items;

      -- Empty map completes immediately with []
      IF _map_item_count = 0 THEN
        PERFORM cb_complete_step(cb_start_steps.flow_name, _step_to_process.step_name, _step_to_process.id, '[]'::jsonb);
      END IF;

      _steps_processed_this_iteration := _steps_processed_this_iteration + 1;
      CONTINUE;
    END IF;

    -- Normal activation (no condition or condition is true)
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'started',
          started_at = now(),
          visible_at = coalesce($2, now())
      WHERE id = $1
      $QUERY$,
      _s_table
    )
    USING _step_to_process.id, cb_start_steps.initial_visible_at;

    _steps_processed_this_iteration := _steps_processed_this_iteration + 1;
    END LOOP;
    
    -- Exit outer loop if no steps were processed in this iteration
    EXIT WHEN _steps_processed_this_iteration = 0;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
-- cb_poll_steps: Poll for step runs from a flow
-- Parameters:
--   flow_name: Flow name
--   step_name: Step name within the flow
--   quantity: Number of step runs to read (must be > 0)
--   hide_for: Duration in milliseconds to hide step runs from other workers (must be > 0)
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: Set of cb_step_claim records
CREATE OR REPLACE FUNCTION cb_poll_steps(
    flow_name text,
    step_name text,
    quantity int,
    hide_for int,
    poll_for int,
    poll_interval int
)
RETURNS SETOF cb_step_claim
LANGUAGE plpgsql AS $$
DECLARE
    _m cb_step_claim;
    _sleep_for double precision;
    _stop_at timestamp;
    _q text;
    _f_table text := cb_table_name(cb_poll_steps.flow_name, 'f');
    _s_table text := cb_table_name(cb_poll_steps.flow_name, 's');
BEGIN
    IF cb_poll_steps.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_poll_steps.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;
    IF cb_poll_steps.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;
    IF cb_poll_steps.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_poll_steps.poll_interval / 1000.0;

    IF _sleep_for >= cb_poll_steps.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_poll_steps.poll_for / 1000.0);

    _q := FORMAT(
        $QUERY$
        WITH runs AS (
          SELECT m.id, m.flow_run_id
          FROM %I m
          INNER JOIN %I f ON m.flow_run_id = f.id
          WHERE m.step_name = $1
            AND m.visible_at <= clock_timestamp()
            AND m.status = 'started'
            AND f.status = 'started'
          ORDER BY m.id ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE %I m
        SET attempts = attempts + 1,
            visible_at = clock_timestamp() + $3
        FROM runs
        WHERE m.id = runs.id
          AND EXISTS (SELECT 1 FROM %I f WHERE f.id = runs.flow_run_id AND f.status = 'started')
        RETURNING m.id,
                  m.attempts,
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
        EXECUTE _q USING cb_poll_steps.step_name, cb_poll_steps.quantity, make_interval(secs => cb_poll_steps.hide_for / 1000.0), cb_poll_steps.flow_name
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
      SET visible_at = (clock_timestamp() + $2)
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
-- cb_poll_map_tasks: Poll map-item tasks for a specific map step
CREATE OR REPLACE FUNCTION cb_poll_map_tasks(
    flow_name text,
    step_name text,
    quantity int,
    hide_for int,
    poll_for int,
    poll_interval int
)
RETURNS TABLE(id bigint, attempts int, input jsonb, step_outputs jsonb, signal_input jsonb, item jsonb)
LANGUAGE plpgsql AS $$
DECLARE
    _sleep_for double precision;
    _stop_at timestamp;
    _q text;
    _f_table text := cb_table_name(cb_poll_map_tasks.flow_name, 'f');
    _s_table text := cb_table_name(cb_poll_map_tasks.flow_name, 's');
    _m_table text := cb_table_name(cb_poll_map_tasks.flow_name, 'm');
BEGIN
    IF cb_poll_map_tasks.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_poll_map_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;
    IF cb_poll_map_tasks.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;
    IF cb_poll_map_tasks.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_poll_map_tasks.poll_interval / 1000.0;
    IF _sleep_for >= cb_poll_map_tasks.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_poll_map_tasks.poll_for / 1000.0);

    _q := format(
      $QUERY$
      WITH runs AS (
        SELECT m.id, m.flow_run_id, m.item
        FROM %I m
        INNER JOIN %I s ON s.flow_run_id = m.flow_run_id AND s.step_name = m.step_name
        INNER JOIN %I f ON f.id = m.flow_run_id
        WHERE m.step_name = $1
          AND m.status = 'created'
          AND m.visible_at <= clock_timestamp()
          AND s.status = 'started'
          AND f.status = 'started'
        ORDER BY m.id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE %I m
      SET status = 'started',
          attempts = attempts + 1,
          started_at = coalesce(m.started_at, now()),
          visible_at = clock_timestamp() + $3
      FROM runs
      WHERE m.id = runs.id
      RETURNING m.id,
                m.attempts,
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
                (SELECT signal_input FROM %I s WHERE s.flow_run_id = m.flow_run_id AND s.step_name = m.step_name) AS signal_input,
                m.item;
      $QUERY$,
      _m_table, _s_table, _f_table, _m_table, _f_table, _s_table, _s_table
    );

    LOOP
      IF clock_timestamp() >= _stop_at THEN
        RETURN;
      END IF;

      RETURN QUERY EXECUTE _q
        USING cb_poll_map_tasks.step_name,
              cb_poll_map_tasks.quantity,
              make_interval(secs => cb_poll_map_tasks.hide_for / 1000.0),
              cb_poll_map_tasks.flow_name;

      IF FOUND THEN
        RETURN;
      END IF;

      PERFORM pg_sleep(_sleep_for);
    END LOOP;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_hide_map_tasks: Hide map tasks from workers for a duration
CREATE OR REPLACE FUNCTION cb_hide_map_tasks(flow_name text, step_name text, ids bigint[], hide_for int)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _m_table text := cb_table_name(cb_hide_map_tasks.flow_name, 'm');
BEGIN
    IF cb_hide_map_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET visible_at = (clock_timestamp() + $2)
      WHERE id = any($1)
        AND step_name = $3
        AND status IN ('created', 'started')
      $QUERY$,
      _m_table
    )
    USING cb_hide_map_tasks.ids,
          make_interval(secs => cb_hide_map_tasks.hide_for / 1000.0),
          cb_hide_map_tasks.step_name;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_complete_map_task: Complete one map task and complete parent step when all items finish
CREATE OR REPLACE FUNCTION cb_complete_map_task(flow_name text, step_name text, map_task_id bigint, output jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := cb_table_name(cb_complete_map_task.flow_name, 's');
    _m_table text := cb_table_name(cb_complete_map_task.flow_name, 'm');
    _flow_run_id bigint;
    _step_id bigint;
    _agg_output jsonb;
    _has_pending boolean;
BEGIN
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
      _m_table
    )
    USING cb_complete_map_task.map_task_id, cb_complete_map_task.output
    INTO _flow_run_id;

    IF _flow_run_id IS NULL THEN
      RETURN;
    END IF;

    EXECUTE format(
      'SELECT EXISTS (SELECT 1 FROM %I WHERE flow_run_id = $1 AND step_name = $2 AND status IN (''created'', ''started''))',
      _m_table
    )
    USING _flow_run_id, cb_complete_map_task.step_name
    INTO _has_pending;

    IF _has_pending THEN
      RETURN;
    END IF;

    EXECUTE format(
      'SELECT id FROM %I WHERE flow_run_id = $1 AND step_name = $2',
      _s_table
    )
    USING _flow_run_id, cb_complete_map_task.step_name
    INTO _step_id;

    EXECUTE format(
      'SELECT coalesce(jsonb_agg(output ORDER BY item_idx), ''[]''::jsonb) FROM %I WHERE flow_run_id = $1 AND step_name = $2 AND status = ''completed''',
      _m_table
    )
    USING _flow_run_id, cb_complete_map_task.step_name
    INTO _agg_output;

    PERFORM cb_complete_step(cb_complete_map_task.flow_name, cb_complete_map_task.step_name, _step_id, _agg_output);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_fail_map_task: Fail one map task and fail parent step/flow
CREATE OR REPLACE FUNCTION cb_fail_map_task(flow_name text, step_name text, map_task_id bigint, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := cb_table_name(cb_fail_map_task.flow_name, 's');
    _m_table text := cb_table_name(cb_fail_map_task.flow_name, 'm');
    _flow_run_id bigint;
    _step_id bigint;
BEGIN
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
      _m_table
    )
    USING cb_fail_map_task.map_task_id, cb_fail_map_task.error_message
    INTO _flow_run_id;

    IF _flow_run_id IS NULL THEN
      RETURN;
    END IF;

    EXECUTE format(
      'SELECT id FROM %I WHERE flow_run_id = $1 AND step_name = $2',
      _s_table
    )
    USING _flow_run_id, cb_fail_map_task.step_name
    INTO _step_id;

    PERFORM cb_fail_step(cb_fail_map_task.flow_name, cb_fail_map_task.step_name, _step_id, cb_fail_map_task.error_message);
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
    -- Get the final step output and set it directly
    EXECUTE format(
      $QUERY$
      UPDATE %I flow_table
      SET status = 'completed',
          completed_at = now(),
          output = (
            SELECT step_table.output
            FROM %I step_table
            WHERE step_table.flow_run_id = %L AND step_table.status IN ('completed', 'skipped')
            ORDER BY step_table.id DESC
            LIMIT 1
          )
      WHERE flow_table.id = %L AND flow_table.status = 'started'
      $QUERY$,
      _f_table, _s_table, _flow_run_id, _flow_run_id
    );
    END IF;

    -- Start steps with no dependencies
    PERFORM cb_start_steps(cb_complete_step.flow_name, _flow_run_id, NULL);
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
    PERFORM cb_start_steps(cb_signal_flow.flow_name, cb_signal_flow.flow_run_id, NULL);

    RETURN true;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_wait_flow_output: Long-poll for flow completion without client-side polling loops
-- Parameters:
--   name: Flow name
--   run_id: Flow run ID
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: status/output/error_message once run reaches terminal state, or no rows on timeout
DROP FUNCTION IF EXISTS cb_wait_flow_output(text, bigint, int, int);
DROP FUNCTION IF EXISTS cb_wait_flow_output(text, bigint, int);
CREATE OR REPLACE FUNCTION cb_wait_flow_output(
    flow_name text,
    run_id bigint,
    poll_for int DEFAULT 5000,
    poll_interval int DEFAULT 200
)
RETURNS TABLE(status text, output jsonb, error_message text)
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := cb_table_name(cb_wait_flow_output.flow_name, 'f');
    _status text;
    _output jsonb;
    _error_message text;
    _sleep_for double precision;
    _stop_at timestamp;
BEGIN
    IF cb_wait_flow_output.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;

    IF cb_wait_flow_output.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_wait_flow_output.poll_interval / 1000.0;

    IF _sleep_for >= cb_wait_flow_output.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_wait_flow_output.poll_for / 1000.0);

    LOOP
        IF clock_timestamp() >= _stop_at THEN
            RETURN;
        END IF;

      _status := NULL;
      _output := NULL;
      _error_message := NULL;

        EXECUTE format(
          $QUERY$
          SELECT f.status, f.output, f.error_message
          FROM %I f
          WHERE f.id = $1
          $QUERY$,
          _f_table
        )
        USING cb_wait_flow_output.run_id
        INTO _status, _output, _error_message;

        IF _status IS NULL THEN
          RAISE EXCEPTION 'cb: flow run % not found for flow %', cb_wait_flow_output.run_id, cb_wait_flow_output.flow_name;
        END IF;

        IF _status IN ('completed', 'failed') THEN
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
    _m_table text := cb_table_name(cb_delete_flow.name, 'm');
    _res boolean;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_f_table));

    DELETE FROM cb_step_handlers h
    WHERE h.flow_name = cb_delete_flow.name;

    DELETE FROM cb_step_dependencies d
    WHERE d.flow_name = cb_delete_flow.name;

    DELETE FROM cb_steps s
    WHERE s.flow_name = cb_delete_flow.name;

    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', _m_table);
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


-- +goose down

DROP FUNCTION IF EXISTS cb_delete_flow(text);
DROP FUNCTION IF EXISTS cb_wait_flow_output(text, bigint, int, int);
DROP FUNCTION IF EXISTS cb_wait_flow_output(text, bigint, int);
DROP FUNCTION IF EXISTS cb_signal_flow(text, bigint, text, jsonb);
DROP FUNCTION IF EXISTS cb_fail_step(text, text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_step(text, text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_fail_map_task(text, text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_map_task(text, text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_hide_map_tasks(text, text, bigint[], int);
DROP FUNCTION IF EXISTS cb_poll_map_tasks(text, text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_hide_steps(text, text, bigint[], integer);
DROP FUNCTION IF EXISTS cb_poll_steps(text, text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_start_steps(text, bigint, timestamptz);
DROP FUNCTION IF EXISTS cb_run_flow(text, jsonb, text, text, timestamptz);
DROP FUNCTION IF EXISTS cb_create_flow(text, jsonb);
