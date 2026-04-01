-- +goose up

CREATE TYPE cb_message AS (
    id bigint,
    concurrency_key text,
    topic text,
    body jsonb,
    headers jsonb,
    priority int,
    deliveries int,
    created_at timestamptz,
    visible_at timestamptz,
    expires_at timestamptz
);

CREATE TYPE cb_task_claim AS (
    id bigint,
    attempts int,
    input jsonb
);

CREATE TYPE cb_step_claim AS (
    id bigint,
    flow_run_id bigint,
    attempts int,
    input jsonb,
    step_outputs jsonb,
    signal_input jsonb
);

CREATE TABLE IF NOT EXISTS cb_queues (
    name text PRIMARY KEY,
    description text,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    CONSTRAINT cb_expires_at_is_valid CHECK (expires_at IS NULL OR expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS cb_queues_expires_at_idx ON cb_queues (expires_at);

CREATE TABLE IF NOT EXISTS cb_bindings (
    target_name text NOT NULL,
    target_type text NOT NULL DEFAULT 'queue',
    pattern text NOT NULL,
    pattern_type text NOT NULL,
    prefix text,
    regex text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (target_name, target_type, pattern),
    CONSTRAINT cb_bindings_pattern_type_valid CHECK (pattern_type IN ('exact', 'wildcard')),
    CONSTRAINT cb_bindings_target_type_valid CHECK (target_type IN ('queue', 'task', 'flow'))
);

CREATE INDEX IF NOT EXISTS cb_bindings_exact_idx ON cb_bindings(pattern)
    WHERE pattern_type = 'exact';
CREATE INDEX IF NOT EXISTS cb_bindings_prefix_idx ON cb_bindings(prefix text_pattern_ops)
    WHERE prefix IS NOT NULL;

CREATE TABLE IF NOT EXISTS cb_tasks (
    name text PRIMARY KEY,
    description text,
    created_at timestamptz NOT NULL DEFAULT now(),
    condition jsonb,
    retention_period interval,
    CONSTRAINT cb_name_not_empty CHECK (name <> ''),
    CONSTRAINT cb_name_not_reserved CHECK (name <> 'input')
);

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    description text,
    output_priority text[] NOT NULL,
    step_count int NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    retention_period interval,
    CONSTRAINT cb_name_not_empty CHECK (name <> ''),
    CONSTRAINT cb_step_count_valid CHECK (step_count >= 0),
    CONSTRAINT cb_output_priority_valid CHECK (
        cardinality(output_priority) > 0
    )
);

CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    description text,
    idx int NOT NULL DEFAULT 0,
    dependency_count int NOT NULL DEFAULT 0,
    step_type text NOT NULL DEFAULT 'normal',
    map_source_step_name text,
    reduce_source_step_name text,
    signal boolean NOT NULL DEFAULT false,
    condition jsonb,
    PRIMARY KEY (flow_name, name),
    UNIQUE (flow_name, idx),
    CONSTRAINT cb_name_valid CHECK (name <> ''),
    CONSTRAINT cb_name_not_reserved CHECK (name NOT IN ('input', 'signal')),
    CONSTRAINT cb_idx_valid CHECK (idx >= 0),
    CONSTRAINT cb_dependency_count_valid CHECK (dependency_count >= 0),
    CONSTRAINT cb_step_type_valid CHECK (step_type IN ('normal', 'mapper', 'generator', 'reducer')),
    CONSTRAINT cb_map_source_not_self CHECK (map_source_step_name IS NULL OR map_source_step_name <> name),
    CONSTRAINT cb_map_source_requires_mapper CHECK ((step_type <> 'mapper' AND map_source_step_name IS NULL) OR step_type = 'mapper'),
    CONSTRAINT cb_reduce_source_not_self CHECK (reduce_source_step_name IS NULL OR reduce_source_step_name <> name),
    CONSTRAINT cb_reduce_source_requires_reducer CHECK (
        (step_type = 'reducer' AND reduce_source_step_name IS NOT NULL)
        OR (step_type <> 'reducer' AND reduce_source_step_name IS NULL)
    ),
    CONSTRAINT cb_map_source_fk FOREIGN KEY (flow_name, map_source_step_name) REFERENCES cb_steps (flow_name, name),
    CONSTRAINT cb_reduce_source_fk FOREIGN KEY (flow_name, reduce_source_step_name) REFERENCES cb_steps (flow_name, name)
);

CREATE TABLE IF NOT EXISTS cb_step_dependencies (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    step_name text NOT NULL,
    dependency_step_name text NOT NULL,
    idx int NOT NULL DEFAULT 0,
    ignore_output boolean NOT NULL DEFAULT false,
    PRIMARY KEY (flow_name, step_name, idx),
    UNIQUE (flow_name, step_name, dependency_step_name),
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name),
    FOREIGN KEY (flow_name, dependency_step_name) REFERENCES cb_steps (flow_name, name),
    CONSTRAINT cb_dependency_name_is_different CHECK (dependency_step_name != step_name),
    CONSTRAINT cb_idx_valid CHECK (idx >= 0)
);

CREATE INDEX IF NOT EXISTS cb_step_dependencies_step_fk ON cb_step_dependencies (flow_name, step_name);
CREATE INDEX IF NOT EXISTS cb_step_dependencies_dependency_name_fk ON cb_step_dependencies (flow_name, dependency_step_name);

-- +goose statementbegin
-- cb_flow_info: Query information about all flow definitions
-- Returns all registered flows with their step definitions and metadata
-- Single SELECT with recursive derivation for compatibility with analyzers
CREATE OR REPLACE VIEW cb_flow_info AS
    SELECT
        f.name,
        nullif((CASE WHEN jsonb_typeof(to_jsonb(f)) = 'object' THEN to_jsonb(f)->>'description' ELSE NULL END), '') AS description,
        (CASE WHEN step_data.steps IS NOT NULL THEN step_data.steps ELSE '[]'::jsonb END) AS steps,
        (CASE WHEN jsonb_typeof(to_jsonb(f)->'output_priority') = 'array' THEN
            ARRAY(SELECT jsonb_array_elements_text(to_jsonb(f)->'output_priority'))
         ELSE ARRAY[]::text[] END) AS output_priority,
        f.retention_period,
        f.created_at
    FROM cb_flows f
    LEFT JOIN LATERAL (
        SELECT
            st.flow_name,
            jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
                'name', st.name,
                'description', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'description' ELSE NULL END),
                'step_type', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'step_type' ELSE NULL END),
                'map_source_step_name', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'map_source_step_name' ELSE NULL END),
                'reduce_source_step_name', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'reduce_source_step_name' ELSE NULL END),
                'signal', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'signal' ELSE NULL END),
                'depends_on', (
                    SELECT jsonb_agg(jsonb_build_object('name', s_d.dependency_step_name))
                    FROM cb_step_dependencies AS s_d
                    WHERE s_d.flow_name = st.flow_name
                    AND s_d.step_name = st.name
                )
            )) ORDER BY st.idx) FILTER (WHERE st.idx IS NOT NULL) AS steps
        FROM cb_steps st
        WHERE st.flow_name = f.name
        GROUP BY flow_name
    ) step_data ON step_data.flow_name = f.name;
-- +goose statementend

CREATE TABLE IF NOT EXISTS cb_workers (
    id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cb_workers_last_heartbeat_at_idx ON cb_workers (last_heartbeat_at);

CREATE TABLE IF NOT EXISTS cb_task_handlers (
    worker_id uuid NOT NULL REFERENCES cb_workers (id) ON DELETE CASCADE,
    task_name text NOT NULL REFERENCES cb_tasks (name) ON DELETE CASCADE,
    PRIMARY KEY (worker_id, task_name)
);

CREATE TABLE IF NOT EXISTS cb_step_handlers (
    worker_id uuid NOT NULL REFERENCES cb_workers (id) ON DELETE CASCADE,
    flow_name text NOT NULL,
    step_name text NOT NULL,
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name) ON DELETE CASCADE,
    PRIMARY KEY (worker_id, flow_name, step_name)
);

CREATE TABLE IF NOT EXISTS cb_task_schedules (
    id               BIGSERIAL PRIMARY KEY,
    task_name        TEXT NOT NULL UNIQUE REFERENCES cb_tasks(name) ON DELETE CASCADE,
    cron_spec        TEXT NOT NULL,
    next_run_at      TIMESTAMPTZ NOT NULL,
    last_run_at      TIMESTAMPTZ,
    last_enqueued_at TIMESTAMPTZ,
    enabled          BOOLEAN NOT NULL DEFAULT true,
    input            JSONB NOT NULL DEFAULT '{}'::jsonb,
    catch_up         TEXT NOT NULL DEFAULT 'one',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cb_cron_spec_not_empty CHECK (cron_spec <> ''),
    CONSTRAINT cb_input_is_object CHECK (jsonb_typeof(input) = 'object'),
    CONSTRAINT cb_updated_at_gte_created_at CHECK (updated_at >= created_at),
    CONSTRAINT cb_catch_up_valid CHECK (catch_up IN ('skip', 'one', 'all'))
);

CREATE INDEX IF NOT EXISTS cb_task_schedules_next_run_at_enabled_idx
    ON cb_task_schedules (next_run_at, enabled)
    WHERE enabled = true;

CREATE TABLE IF NOT EXISTS cb_flow_schedules (
    id               BIGSERIAL PRIMARY KEY,
    flow_name        TEXT NOT NULL UNIQUE REFERENCES cb_flows(name) ON DELETE CASCADE,
    cron_spec        TEXT NOT NULL,
    next_run_at      TIMESTAMPTZ NOT NULL,
    last_run_at      TIMESTAMPTZ,
    last_enqueued_at TIMESTAMPTZ,
    enabled          BOOLEAN NOT NULL DEFAULT true,
    input            JSONB NOT NULL DEFAULT '{}'::jsonb,
    catch_up         TEXT NOT NULL DEFAULT 'one',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cb_cron_spec_not_empty CHECK (cron_spec <> ''),
    CONSTRAINT cb_input_is_object CHECK (jsonb_typeof(input) = 'object'),
    CONSTRAINT cb_updated_at_gte_created_at CHECK (updated_at >= created_at),
    CONSTRAINT cb_catch_up_valid CHECK (catch_up IN ('skip', 'one', 'all'))
);

CREATE INDEX IF NOT EXISTS cb_flow_schedules_next_run_at_enabled_idx
    ON cb_flow_schedules (next_run_at, enabled)
    WHERE enabled = true;

CREATE UNLOGGED TABLE IF NOT EXISTS cb_wire_nodes (
    id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cb_wire_nodes_last_heartbeat_at_idx ON cb_wire_nodes (last_heartbeat_at);

CREATE UNLOGGED TABLE IF NOT EXISTS cb_wire_presence (
    subscriber_id uuid NOT NULL,
    identity text NOT NULL,
    topic text NOT NULL,
    node_id uuid NOT NULL REFERENCES cb_wire_nodes (id) ON DELETE CASCADE,
    connected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subscriber_id, topic)
);

CREATE INDEX IF NOT EXISTS cb_wire_presence_topic_idx ON cb_wire_presence (topic);
CREATE INDEX IF NOT EXISTS cb_wire_presence_node_id_idx ON cb_wire_presence (node_id);

-- +goose statementbegin
-- _cb_table_name: Generate a PostgreSQL table name for queue/task/flow storage
-- Validates the name and constructs the internal table name with prefix
-- Parameters:
--   name: Queue/task/flow name (must contain only a-z, 0-9, _; max 58 chars)
--   prefix: Type prefix ('q' for queue, 't' for task, etc.)
-- Returns: text - the full PostgreSQL table name
CREATE OR REPLACE FUNCTION _cb_table_name(name text, prefix text)
RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
    IF _cb_table_name.name !~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'cb: queue name can only contain characters: a-z, 0-9 or _';
    END IF;
    IF length(_cb_table_name.name) >= 58 THEN
        RAISE EXCEPTION 'cb: queue name is too long, maximum length is 58';
    END IF;
    RETURN 'cb_' || _cb_table_name.prefix || '_' || lower(_cb_table_name.name);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_create_queue: Create a queue definition
-- Creates the queue metadata and associated message table for enqueued messages
-- Parameters:
--   name: Queue name (must be unique)
--   expires_at: Optional timestamp when the queue can be garbage collected
--   description: Optional queue description metadata
-- Returns: void
CREATE OR REPLACE FUNCTION cb_create_queue(
    name text,
    expires_at timestamptz DEFAULT NULL,
    description text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := _cb_table_name(cb_create_queue.name, 'q');
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_q_table));

    EXECUTE format(
        $QUERY$
        CREATE TABLE IF NOT EXISTS %I (
            id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            concurrency_key text,
            topic text,
            body jsonb NOT NULL,
            headers jsonb,
            priority int NOT NULL DEFAULT 0,
            deliveries int NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT now(),
            visible_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz,
            CONSTRAINT cb_headers_is_object CHECK (headers IS NULL OR jsonb_typeof(headers) = 'object')
        )
        $QUERY$,
        _q_table
    );

    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (concurrency_key);', _q_table || '_concurrency_key_idx', _q_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (visible_at, priority DESC, id);', _q_table || '_poll_visible_id_idx', _q_table);
    EXECUTE format('DROP INDEX IF EXISTS %I;', _q_table || '_visible_at_idx');

    -- Insert queue metadata
    INSERT INTO cb_queues (name, description, expires_at)
    VALUES (
        cb_create_queue.name,
        cb_create_queue.description,
        cb_create_queue.expires_at
    )
    ON CONFLICT DO NOTHING;

    PERFORM _cb_notify(topic => 'catbird.queue.created', message => jsonb_build_object('queue', cb_create_queue.name)::text);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_delete_queue: Delete a queue and clean up its bindings
CREATE OR REPLACE FUNCTION cb_delete_queue(name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := _cb_table_name(cb_delete_queue.name, 'q');
    _res boolean;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_q_table));

    EXECUTE FORMAT('DROP TABLE IF EXISTS %I;', _q_table);

    DELETE FROM cb_bindings
    WHERE target_name = cb_delete_queue.name
      AND target_type = 'queue';

    DELETE FROM cb_queues q
    WHERE q.name = cb_delete_queue.name
    RETURNING true
    INTO _res;

    IF coalesce(_res, false) THEN
        PERFORM _cb_notify(topic => 'catbird.queue.deleted', message => jsonb_build_object('queue', cb_delete_queue.name)::text);
    END IF;

    RETURN coalesce(_res, false);
end
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_bind: Subscribe a queue to a topic pattern (updated for new schema)
CREATE OR REPLACE FUNCTION cb_bind(queue text, pattern text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    p_type text;
    p_prefix text;
    p_regex text;
    base_pattern text;
    test_result boolean;
    token text;
    tokens text[];
    token_count integer;
    i integer;
BEGIN
    -- Validate queue exists
    IF NOT EXISTS (SELECT 1 FROM cb_queues q WHERE q.name = cb_bind.queue) THEN
        RAISE EXCEPTION 'catbird: queue "%" is not defined', cb_bind.queue;
    END IF;

    -- Validate pattern contains only allowed characters
    IF cb_bind.pattern !~ '^[a-zA-Z0-9._#*-]+$' THEN
        RAISE EXCEPTION 'cb: pattern can only contain: a-z, A-Z, 0-9, ., _, -, *, #';
    END IF;

    -- Validate no double dots, leading or trailing dots
    IF cb_bind.pattern ~ '\.\.' OR cb_bind.pattern ~ '(^\.|\.$)' THEN
        RAISE EXCEPTION 'cb: pattern cannot contain double dots (..), or start/end with a dot';
    END IF;

    -- Validate wildcard placement by token
    tokens := string_to_array(cb_bind.pattern, '.');
    token_count := array_length(tokens, 1);

    FOR i IN 1..token_count LOOP
        token := tokens[i];

        IF token = '*' THEN
            CONTINUE;
        END IF;

        IF token = '#' THEN
            IF i <> token_count THEN
                RAISE EXCEPTION 'cb: # wildcard must be the final token';
            END IF;
            CONTINUE;
        END IF;

        IF token LIKE '%*%' THEN
            RAISE EXCEPTION 'cb: * wildcard must occupy an entire token';
        END IF;

        IF token LIKE '%#%' THEN
            RAISE EXCEPTION 'cb: # wildcard must occupy an entire token';
        END IF;
    END LOOP;

    -- Check if pattern has wildcards
    IF cb_bind.pattern !~ '[*#]' THEN
        -- Exact match
        p_type := 'exact';
        p_prefix := NULL;
        p_regex := NULL;
    ELSE
        -- Wildcard match
        p_type := 'wildcard';

        -- Extract literal prefix before first wildcard
        p_prefix := substring(cb_bind.pattern FROM '^([^*#]+)');

        -- If no prefix (pattern starts with wildcard), set to empty for matching
        IF p_prefix IS NULL OR p_prefix = '' THEN
            p_prefix := '';
        END IF;

        -- If pattern ends in .#, trim trailing dot so prefix filter keeps
        -- matching the zero-token tail case (e.g., events.# matches events)
        IF cb_bind.pattern ~ E'\\.\\#$' AND p_prefix <> '' AND right(p_prefix, 1) = '.' THEN
            p_prefix := left(p_prefix, length(p_prefix) - 1);
        END IF;

        -- Precompile regex pattern
        -- *  -> [a-zA-Z0-9_-]+ (single token)
        -- .# -> (\.[a-zA-Z0-9_-]+)* (zero or more trailing tokens)
        base_pattern := cb_bind.pattern;
        IF cb_bind.pattern ~ E'\\.\\#$' THEN
            base_pattern := regexp_replace(base_pattern, E'\\.\\#$', '');
        END IF;

        p_regex := regexp_replace(base_pattern, E'\\.', E'\\\\.', 'g');
        p_regex := regexp_replace(p_regex, E'\\*', '[a-zA-Z0-9_-]+', 'g');

        IF cb_bind.pattern = '#' THEN
            p_regex := E'^[a-zA-Z0-9_-]+(?:\\.[a-zA-Z0-9_-]+)*$';
        ELSIF cb_bind.pattern ~ E'\\.\\#$' THEN
            p_regex := '^' || p_regex || E'(?:\\.[a-zA-Z0-9_-]+)*$';
        ELSE
            p_regex := '^' || p_regex || '$';
        END IF;

        -- Validate regex compiles correctly by testing it
        BEGIN
            test_result := 'test.topic' ~ p_regex;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'cb: invalid pattern "%" generated invalid regex: %', cb_bind.pattern, SQLERRM;
        END;
    END IF;

    -- Insert or update binding
    INSERT INTO cb_bindings(target_name, target_type, pattern, pattern_type, prefix, regex)
    VALUES (cb_bind.queue, 'queue', cb_bind.pattern, p_type, p_prefix, p_regex)
    ON CONFLICT ON CONSTRAINT cb_bindings_pkey DO NOTHING;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_unbind: Unsubscribe a queue from a topic pattern (updated for new schema)
CREATE OR REPLACE FUNCTION cb_unbind(queue text, pattern text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
        _deleted boolean := false;
BEGIN
    DELETE FROM cb_bindings
    WHERE cb_bindings.target_name = cb_unbind.queue
            AND cb_bindings.target_type = 'queue'
            AND cb_bindings.pattern = cb_unbind.pattern
        RETURNING true INTO _deleted;

        RETURN coalesce(_deleted, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_send: Send a message to a specific queue
-- Enqueues a message in the queue, with optional topic and concurrency control
-- Parameters:
--   queue: Queue name
--   body: JSON message body
--   topic: Optional topic string for categorization
--   concurrency_key: Optional unique ID for concurrency control (prevents duplicate messages)
--   visible_at: Optional timestamp when message should become visible (default: now)
-- Returns: bigint - the message ID
CREATE OR REPLACE FUNCTION cb_send(
    queue text,
    body jsonb,
    topic text DEFAULT NULL,
    concurrency_key text DEFAULT NULL,
    headers jsonb DEFAULT NULL,
    visible_at timestamptz DEFAULT NULL,
    priority int DEFAULT 0,
    expires_at timestamptz DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz := coalesce(cb_send.visible_at, now());
    _q_table text := _cb_table_name(cb_send.queue, 'q');
    _id bigint;
BEGIN
    IF cb_send.headers IS NOT NULL AND jsonb_typeof(cb_send.headers) <> 'object' THEN
        RAISE EXCEPTION 'cb: headers must be a JSON object';
    END IF;

    -- ON CONFLICT DO UPDATE with WHERE FALSE: atomic insert + return row ID (new or conflicting).
    -- Use UNION ALL to handle both INSERT success and conflict cases atomically.
    -- Pattern from: https://stackoverflow.com/a/35953488
    EXECUTE format(
        $QUERY$
        WITH ins AS (
            INSERT INTO %I (topic, body, concurrency_key, headers, visible_at, priority, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (concurrency_key) WHERE concurrency_key IS NOT NULL
            DO UPDATE SET visible_at = EXCLUDED.visible_at WHERE FALSE
            RETURNING id
        )
        SELECT id FROM ins
        UNION ALL
        SELECT id FROM %I
        WHERE concurrency_key = $3 AND concurrency_key IS NOT NULL
        LIMIT 1
        $QUERY$,
        _q_table, _q_table
    )
    USING cb_send.topic,
          cb_send.body,
          cb_send.concurrency_key,
          cb_send.headers,
          _visible_at,
          cb_send.priority,
          cb_send.expires_at
    INTO _id;

    PERFORM pg_notify(current_schema || '.cb_q_' || cb_send.queue, to_char(coalesce(cb_send.visible_at, now()) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));

    RETURN _id;
END
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_read: Read messages from a queue
-- Parameters:
--   queue: Queue name
--   quantity: Number of messages to read (must be > 0)
--   hide_for: Duration in milliseconds to hide messages from other readers (must be > 0)
-- Returns: Set of cb_message records
CREATE OR REPLACE FUNCTION cb_read(
    queue text,
    quantity int,
    hide_for int
)
RETURNS SETOF cb_message
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := _cb_table_name(cb_read.queue, 'q');
    _q text;
BEGIN
    IF cb_read.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_read.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    _q := format(
        $QUERY$
        WITH msgs AS (
          SELECT id
          FROM %I
          WHERE visible_at <= clock_timestamp()
            AND (expires_at IS NULL OR expires_at > clock_timestamp())
          ORDER BY priority DESC, id ASC
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE %I m
        SET deliveries = deliveries + 1,
            visible_at = clock_timestamp() + $2
        FROM msgs
        WHERE m.id = msgs.id
        RETURNING m.id,
                  m.concurrency_key,
                  m.topic,
                  m.body,
                  m.headers,
                  m.priority,
                  m.deliveries,
                  m.created_at,
                  m.visible_at,
                  m.expires_at;
        $QUERY$,
        _q_table, _q_table
    );
    RETURN QUERY EXECUTE _q USING cb_read.quantity, make_interval(secs => cb_read.hide_for / 1000.0);
end
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_read_poll: Read messages from a queue with polling
-- Polls the queue repeatedly until messages are available or timeout is reached
-- Parameters:
--   queue: Queue name
--   quantity: Number of messages to read (must be > 0)
--   hide_for: Duration in milliseconds to hide messages from other readers (must be > 0)
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: Set of cb_message records
CREATE OR REPLACE FUNCTION cb_read_poll(
    queue text,
    quantity int,
    hide_for int,
    poll_for int,
    poll_interval int
)
RETURNS SETOF cb_message
LANGUAGE plpgsql AS $$
DECLARE
    _m cb_message;
    _sleep_for double precision;
    _stop_at timestamp;
    _q text;
    _q_table text := _cb_table_name(cb_read_poll.queue, 'q');
BEGIN
    IF cb_read_poll.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_read_poll.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;
    IF cb_read_poll.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;
    IF cb_read_poll.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_read_poll.poll_interval / 1000.0;

    IF _sleep_for >= cb_read_poll.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_read_poll.poll_for / 1000.0);

    LOOP
        IF (SELECT clock_timestamp() >= _stop_at) THEN
            RETURN;
        END IF;

        _q := FORMAT(
            $QUERY$
            WITH msgs AS (
                SELECT id
                FROM %I
                WHERE visible_at <= clock_timestamp()
                  AND (expires_at IS NULL OR expires_at > clock_timestamp())
                ORDER BY priority DESC, id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE %I m
            SET deliveries = deliveries + 1,
                visible_at = clock_timestamp() + $2
            FROM msgs
            WHERE m.id = msgs.id
            RETURNING m.id,
                      m.concurrency_key,
                      m.topic,
                      m.body,
                      m.headers,
                      m.priority,
                      m.deliveries,
                      m.created_at,
                      m.visible_at,
                      m.expires_at;
            $QUERY$,
            _q_table, _q_table
      );

      FOR _m IN
        EXECUTE _q USING cb_read_poll.quantity, make_interval(secs => cb_read_poll.hide_for / 1000.0)
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
-- cb_hide: Hide a message from being read
-- Parameters:
--   queue: Queue name
--   id: Message ID to hide
--   hide_for: Duration in milliseconds to hide the message (must be > 0)
-- Returns: true if message was hidden, false if not found
CREATE OR REPLACE FUNCTION cb_hide(
    queue text,
    id bigint,
    hide_for int
)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := _cb_table_name(cb_hide.queue, 'q');
    _res boolean;
BEGIN
    IF cb_hide.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    EXECUTE format(
        $QUERY$
        UPDATE %I
        SET visible_at = (clock_timestamp() + $2)
        WHERE id = $1
        RETURNING TRUE;
        $QUERY$,
        _q_table
    )
    USING cb_hide.id, make_interval(secs => cb_hide.hide_for / 1000.0)
    INTO _res;
    RETURN coalesce(_res, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_hide: Hide multiple messages from being read
-- Parameters:
--   queue: Queue name
--   ids: Array of message IDs to hide
--   hide_for: Duration in milliseconds to hide the messages (must be > 0)
-- Returns: bigint[] - IDs actually hidden
CREATE OR REPLACE FUNCTION cb_hide(
    queue text,
    ids bigint[],
    hide_for int
)
RETURNS bigint[]
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := _cb_table_name(cb_hide.queue, 'q');
    _ids bigint[];
BEGIN
    IF cb_hide.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    EXECUTE format(
        $QUERY$
        WITH updated AS (
            UPDATE %I
            SET visible_at = (clock_timestamp() + $2)
            WHERE id = any($1)
            RETURNING id
        )
        SELECT coalesce(array_agg(id ORDER BY id), '{}'::bigint[])
        FROM updated;
        $QUERY$,
        _q_table
    )
    USING cb_hide.ids, make_interval(secs => cb_hide.hide_for / 1000.0)
    INTO _ids;

    RETURN _ids;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_delete: Delete a message from a queue
-- Parameters:
--   queue: Queue name
--   id: Message ID to delete
-- Returns: boolean - true if message was deleted, false if not found
CREATE OR REPLACE FUNCTION cb_delete(queue text, id bigint)
RETURNS boolean AS $$
DECLARE
    _q_table text := _cb_table_name(cb_delete.queue, 'q');
    _res boolean;
BEGIN
    EXECUTE format(
        $QUERY$
        DELETE FROM %I WHERE id = $1 RETURNING TRUE;
        $QUERY$,
        _q_table
    )
    USING cb_delete.id
    INTO _res;
    RETURN coalesce(_res, false);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
-- cb_delete: Delete multiple messages from a queue
-- Parameters:
--   queue: Queue name
--   ids: Array of message IDs to delete
-- Returns: bigint[] - IDs actually deleted
CREATE OR REPLACE FUNCTION cb_delete(queue text, ids bigint[])
RETURNS bigint[] AS $$
DECLARE
    _q_table text := _cb_table_name(cb_delete.queue, 'q');
    _ids bigint[];
BEGIN
    EXECUTE format(
        $QUERY$
        WITH deleted AS (
            DELETE FROM %I
            WHERE id = any($1)
            RETURNING id
        )
        SELECT coalesce(array_agg(id ORDER BY id), '{}'::bigint[])
        FROM deleted;
        $QUERY$,
        _q_table
    )
    USING cb_delete.ids
    INTO _ids;

    RETURN _ids;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
-- cb_send (bulk): Send multiple messages to a specific queue
-- Enqueues messages in a queue from a jsonb[] body array.
-- Parameters:
--   queue: Queue name
--   bodies: JSONB array of message bodies
--   topic: Optional topic string for categorization
--   concurrency_keys: Optional array of concurrency keys (must match body count when provided)
--   visible_at: Optional timestamp when messages should become visible (default: now)
-- Returns: bigint[] - inserted message IDs
CREATE OR REPLACE FUNCTION cb_send(
    queue text,
    bodies jsonb[],
    topic text DEFAULT NULL,
    concurrency_keys text[] DEFAULT NULL,
    headers jsonb[] DEFAULT NULL,
    visible_at timestamptz DEFAULT NULL,
    priority int DEFAULT 0,
    expires_at timestamptz DEFAULT NULL
)
RETURNS bigint[]
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz := coalesce(cb_send.visible_at, now());
    _q_table text := _cb_table_name(cb_send.queue, 'q');
    _ids bigint[];
BEGIN
    IF cb_send.concurrency_keys IS NOT NULL AND cardinality(cb_send.concurrency_keys) <> cardinality(cb_send.bodies) THEN
        RAISE EXCEPTION 'cb: concurrency_keys length must match bodies length';
    END IF;

    IF cb_send.headers IS NOT NULL AND cardinality(cb_send.headers) <> cardinality(cb_send.bodies) THEN
        RAISE EXCEPTION 'cb: headers length must match bodies length';
    END IF;

    IF cb_send.headers IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM unnest(cb_send.headers) AS h(header)
            WHERE h.header IS NOT NULL AND jsonb_typeof(h.header) <> 'object'
        ) THEN
            RAISE EXCEPTION 'cb: headers must be JSON objects';
        END IF;
    END IF;

    EXECUTE format(
        $QUERY$
        WITH body_list AS (
            SELECT
                body,
                ordinality,
                CASE
                    WHEN $5 IS NULL THEN NULL
                    ELSE $5[ordinality]
                END AS concurrency_key,
                CASE
                    WHEN $3 IS NULL THEN NULL
                    ELSE $3[ordinality]
                END AS headers
            FROM unnest($1) WITH ORDINALITY AS t(body, ordinality)
        ),
        ins AS (
            INSERT INTO %I (topic, body, concurrency_key, headers, visible_at, priority, expires_at)
            SELECT $2, body, concurrency_key, headers, $4, $6, $7
            FROM body_list
            ORDER BY ordinality
            ON CONFLICT (concurrency_key) DO NOTHING
            RETURNING id
        )
        SELECT coalesce(array_agg(id ORDER BY id), '{}'::bigint[])
        FROM ins
        $QUERY$,
        _q_table
    )
    USING cb_send.bodies,
          cb_send.topic,
          cb_send.headers,
          _visible_at,
          cb_send.concurrency_keys,
          cb_send.priority,
          cb_send.expires_at
    INTO _ids;

    PERFORM pg_notify(current_schema || '.cb_q_' || cb_send.queue, to_char(coalesce(cb_send.visible_at, now()) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));

    RETURN _ids;
END
$$;
-- +goose statementend

-- +goose statementbegin
-- _cb_parse_condition: Canonical condition parser for all clients (go, python, js, ruby, java, etc.)
-- Takes a condition expression and returns jsonb with parsed components: {field, operator, value}
-- Raises an exception if the expression is invalid
CREATE OR REPLACE FUNCTION _cb_parse_condition(expr text)
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
    _value := __cb_parse_condition_value(_value_str, _op);

    RETURN jsonb_build_object(
        'field', _field,
        'operator', _op,
        'value', _value,
        'negated', _negated
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION __cb_parse_condition_value(val_str text, op text)
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

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_evaluate_condition(condition jsonb, output jsonb)
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
    _field_val := _cb_get_jsonb_field(output, _field);

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

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_get_jsonb_field(obj jsonb, field_path text)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE AS $$
    SELECT
        CASE
            WHEN obj IS NULL OR field_path IS NULL OR field_path = '' THEN NULL
            ELSE obj #> regexp_split_to_array(
                regexp_replace(field_path, '\[([0-9]+)\]', '.\1', 'g'),
                '\.'
            )
        END
$$;
-- +goose statementend

-- +goose statementbegin
-- __cb_evaluate_condition_expr: Wrapper that takes text condition
-- Parses the condition first, then evaluates it
-- This is useful for testing and when conditions are stored as text
CREATE OR REPLACE FUNCTION __cb_evaluate_condition_expr(
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
    _parsed_condition := _cb_parse_condition(condition_expr);

    IF _parsed_condition IS NULL THEN
        RETURN false;
    END IF;

    -- Evaluate using the JSONB version
    RETURN _cb_evaluate_condition(_parsed_condition, output);
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
--   description: Optional task description metadata
--   condition: Optional condition expression for task execution
--   retention_period: Optional retention period; completed/failed/skipped/canceled runs older than this are deleted by cb_gc()
-- Returns: void
CREATE OR REPLACE FUNCTION cb_create_task(name text, description text DEFAULT NULL, condition text DEFAULT NULL, retention_period interval DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_create_task.name, 't');
    _condition jsonb;
    _existing_name text;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_t_table));

    SELECT t.name
    INTO _existing_name
    FROM cb_tasks t
    WHERE t.name = cb_create_task.name;

    IF _existing_name IS NOT NULL THEN
        UPDATE cb_tasks
        SET retention_period = cb_create_task.retention_period
        WHERE cb_tasks.name = cb_create_task.name;
        RETURN;
    END IF;

    -- Validate task name
    IF cb_create_task.name = 'input' THEN
        RAISE EXCEPTION 'cb: task name "input" is reserved';
    END IF;

    _condition := NULL;
    IF cb_create_task.condition IS NOT NULL AND cb_create_task.condition <> '' THEN
        _condition := _cb_parse_condition(cb_create_task.condition);
    END IF;

    INSERT INTO cb_tasks (name, description, condition, retention_period)
    VALUES (
        cb_create_task.name,
        cb_create_task.description,
        _condition,
        cb_create_task.retention_period
    );

    EXECUTE format(
        $QUERY$
        CREATE TABLE IF NOT EXISTS %I (
            id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            concurrency_key text,
            status text NOT NULL DEFAULT 'queued',
            attempts int NOT NULL DEFAULT 0,
            input jsonb NOT NULL,
            headers jsonb,
            output jsonb,
            error_message text,
            on_fail_status text,
            on_fail_attempts int NOT NULL DEFAULT 0,
            on_fail_visible_at timestamptz,
            on_fail_error_message text,
            on_fail_started_at timestamptz,
            on_fail_completed_at timestamptz,
            priority int NOT NULL DEFAULT 0,
            visible_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz,
            started_at timestamptz NOT NULL DEFAULT now(),
            cancel_requested_at timestamptz,
            completed_at timestamptz,
            failed_at timestamptz,
            skipped_at timestamptz,
            canceled_at timestamptz,
            expired_at timestamptz,
            cancel_reason text,
            CONSTRAINT cb_status_valid CHECK (status IN ('queued', 'started', 'canceling', 'completed', 'failed', 'skipped', 'canceled', 'expired')),
            CONSTRAINT cb_completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
            CONSTRAINT cb_skipped_and_completed_failed CHECK (NOT (skipped_at IS NOT NULL AND (completed_at IS NOT NULL OR failed_at IS NOT NULL OR canceled_at IS NOT NULL))),
            CONSTRAINT cb_canceled_terminal_exclusive CHECK (NOT (canceled_at IS NOT NULL AND (completed_at IS NOT NULL OR failed_at IS NOT NULL OR skipped_at IS NOT NULL))),
            CONSTRAINT cb_completed_at_is_after_started_at CHECK (completed_at IS NULL OR completed_at >= started_at),
            CONSTRAINT cb_failed_at_is_after_started_at CHECK (failed_at IS NULL OR failed_at >= started_at),
            CONSTRAINT cb_canceled_at_is_after_started_at CHECK (canceled_at IS NULL OR canceled_at >= started_at),
            CONSTRAINT cb_completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
            CONSTRAINT cb_failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = ''))),
            CONSTRAINT cb_on_fail_status_valid CHECK (on_fail_status IS NULL OR on_fail_status IN ('queued', 'started', 'completed', 'failed')),
            CONSTRAINT cb_headers_is_object CHECK (headers IS NULL OR jsonb_typeof(headers) = 'object')
        )
        $QUERY$,
        _t_table
    );

    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (concurrency_key) WHERE concurrency_key IS NOT NULL AND status IN (''queued'', ''started'')', _t_table || '_concurrency_key_idx', _t_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (visible_at, priority DESC, id) WHERE status IN (''queued'', ''started'');', _t_table || '_poll_visible_id_idx', _t_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (on_fail_visible_at, id) WHERE status = ''failed'' AND on_fail_status IN (''queued'', ''failed'');', _t_table || '_on_fail_poll_idx', _t_table);
    EXECUTE format('DROP INDEX IF EXISTS %I;', _t_table || '_visible_at_idx');
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (GREATEST(completed_at, failed_at, skipped_at, canceled_at, expired_at)) WHERE status IN (''completed'', ''failed'', ''skipped'', ''canceled'', ''expired'');', _t_table || '_retention_idx', _t_table);

    PERFORM _cb_notify(topic => 'catbird.task.created', message => jsonb_build_object('task', cb_create_task.name)::text);
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_cancel_task(
        name text,
        run_id bigint,
        reason text DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_cancel_task.name, 't');
    _status text;
BEGIN
        EXECUTE format(
            $QUERY$
            UPDATE %I
            SET status = CASE
                        WHEN status = 'queued' THEN 'canceled'
                        WHEN status = 'started' THEN 'canceled'
                        ELSE status
                    END,
                    cancel_requested_at = CASE
                        WHEN status IN ('queued', 'started') THEN now()
                        ELSE cancel_requested_at
                    END,
                    canceled_at = CASE
                        WHEN status IN ('queued', 'started') THEN now()
                        ELSE canceled_at
                    END,
                    cancel_reason = coalesce($2, cancel_reason)
            WHERE id = $1
            RETURNING status
            $QUERY$,
            _t_table
        )
        USING cb_cancel_task.run_id, cb_cancel_task.reason
        INTO _status;

        IF _status IS NULL THEN
            EXECUTE format('SELECT status FROM %I WHERE id = $1', _t_table)
            USING cb_cancel_task.run_id
            INTO _status;
            IF _status IS NULL THEN
                RETURN false;
            END IF;
        END IF;

        IF _status = 'canceled' THEN
            PERFORM pg_notify(current_schema || '.cb_task_stop_' || cb_cancel_task.name, '');
            PERFORM _cb_notify(topic => 'catbird.task.canceled', message => jsonb_build_object('task', cb_cancel_task.name, 'run_id', cb_cancel_task.run_id)::text);
        END IF;

        RETURN true;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_run_task: Create a task run (enqueue a task execution)
-- Parameters:
--   name: Task name
--   input: JSON input data for the task
--   concurrency_key: Optional key for concurrency control (prevents overlapping runs)
-- Returns: bigint - the task run ID
CREATE OR REPLACE FUNCTION cb_run_task(
    name text,
    input jsonb,
    concurrency_key text DEFAULT NULL,
    headers jsonb DEFAULT NULL,
    visible_at timestamptz DEFAULT NULL,
    priority int DEFAULT 0,
    expires_at timestamptz DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_run_task.name, 't');
    _id bigint;
BEGIN
    IF cb_run_task.headers IS NOT NULL AND jsonb_typeof(cb_run_task.headers) <> 'object' THEN
        RAISE EXCEPTION 'cb: headers must be a JSON object';
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
                INSERT INTO %I (input, concurrency_key, headers, visible_at, priority, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6)
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
        USING cb_run_task.input, cb_run_task.concurrency_key, cb_run_task.headers, coalesce(cb_run_task.visible_at, now()), cb_run_task.priority, cb_run_task.expires_at
        INTO _id;
    ELSE
        -- No deduplication
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (input, headers, visible_at, priority, expires_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            $QUERY$,
            _t_table
        )
        USING cb_run_task.input, cb_run_task.headers, coalesce(cb_run_task.visible_at, now()), cb_run_task.priority, cb_run_task.expires_at
        INTO _id;
    END IF;

    PERFORM pg_notify(current_schema || '.cb_t_' || cb_run_task.name, to_char(coalesce(cb_run_task.visible_at, now()) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));

    PERFORM _cb_notify(topic => 'catbird.task.started', message => jsonb_build_object('task', cb_run_task.name, 'run_id', _id)::text);

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
CREATE OR REPLACE FUNCTION cb_claim_tasks(
    name text,
    quantity int,
    hide_for int
)
RETURNS SETOF cb_task_claim
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_claim_tasks.name, 't');
BEGIN
    IF cb_claim_tasks.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_claim_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    RETURN QUERY EXECUTE format(
        $QUERY$
        WITH runs AS (
          SELECT id
          FROM %I
          WHERE visible_at <= clock_timestamp()
            AND status IN ('queued', 'started')
            AND (expires_at IS NULL OR expires_at > clock_timestamp())
          ORDER BY priority DESC, id ASC
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
    )
    USING cb_claim_tasks.quantity, make_interval(secs => cb_claim_tasks.hide_for / 1000.0);
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
CREATE OR REPLACE FUNCTION cb_hide_tasks(name text, ids bigint[], hide_for int)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_hide_tasks.name, 't');
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

    PERFORM pg_notify(current_schema || '.cb_t_' || cb_hide_tasks.name, to_char((clock_timestamp() + make_interval(secs => cb_hide_tasks.hide_for / 1000.0)) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
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
CREATE OR REPLACE FUNCTION cb_complete_task(name text, run_id bigint, output jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_complete_task.name, 't');
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
    USING cb_complete_task.run_id,
          cb_complete_task.output;

    PERFORM _cb_notify(topic => 'catbird.task.completed', message => jsonb_build_object('task', cb_complete_task.name, 'run_id', cb_complete_task.run_id)::text);
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
CREATE OR REPLACE FUNCTION cb_fail_task(name text, run_id bigint, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_fail_task.name, 't');
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I t_r
      SET status = 'failed',
          failed_at = now(),
          error_message = $2,
          on_fail_status = 'queued',
          on_fail_visible_at = now(),
          on_fail_error_message = NULL,
          on_fail_started_at = NULL,
          on_fail_completed_at = NULL
      WHERE t_r.id = $1
        AND t_r.status = 'started';
      $QUERY$,
      _t_table
    )
    USING cb_fail_task.run_id,
          cb_fail_task.error_message;

    PERFORM pg_notify(current_schema || '.cb_t_onfail_' || cb_fail_task.name, '');

    PERFORM _cb_notify(topic => 'catbird.task.failed', message => jsonb_build_object('task', cb_fail_task.name, 'run_id', cb_fail_task.run_id)::text);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_claim_task_on_fail: Claim failed task runs for on-fail handling
-- Parameters:
--   name: Task name
--   quantity: Number of failed task runs to claim (must be > 0)
-- Returns: Task runs claimed for on-fail processing
CREATE OR REPLACE FUNCTION cb_claim_task_on_fail(name text, quantity int)
RETURNS TABLE(
        id bigint,
        input jsonb,
        error_message text,
        attempts int,
        on_fail_attempts int,
        started_at timestamptz,
        failed_at timestamptz,
        concurrency_key text
)
LANGUAGE plpgsql AS $$
DECLARE
        _t_table text := _cb_table_name(cb_claim_task_on_fail.name, 't');
BEGIN
        IF cb_claim_task_on_fail.quantity <= 0 THEN
                RAISE EXCEPTION 'cb: quantity must be greater than 0';
        END IF;

        RETURN QUERY EXECUTE format(
            $QUERY$
            WITH runs AS (
                SELECT t.id
                FROM %I t
                WHERE t.status = 'failed'
                    AND t.on_fail_status IN ('queued', 'failed')
                    AND coalesce(t.on_fail_visible_at, clock_timestamp()) <= clock_timestamp()
                ORDER BY t.id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE %I t
            SET on_fail_status = 'started',
                    on_fail_started_at = clock_timestamp(),
                    on_fail_attempts = t.on_fail_attempts + 1
            FROM runs
            WHERE t.id = runs.id
            RETURNING t.id,
                                t.input,
                                t.error_message,
                                t.attempts,
                                t.on_fail_attempts,
                                t.started_at,
                                t.failed_at,
                                t.concurrency_key
            $QUERY$,
            _t_table,
            _t_table
        )
        USING cb_claim_task_on_fail.quantity;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_complete_task_on_fail: Mark on-fail handling as completed for a task run
CREATE OR REPLACE FUNCTION cb_complete_task_on_fail(name text, run_id bigint)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
        _t_table text := _cb_table_name(cb_complete_task_on_fail.name, 't');
BEGIN
        EXECUTE format(
            $QUERY$
            UPDATE %I
            SET on_fail_status = 'completed',
                    on_fail_completed_at = clock_timestamp(),
                    on_fail_error_message = NULL
            WHERE id = $1
                AND on_fail_status = 'started'
            $QUERY$,
            _t_table
        )
        USING cb_complete_task_on_fail.run_id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_fail_task_on_fail(
        name text,
        run_id bigint,
        error_message text,
        retry_exhausted boolean,
        retry_delay bigint
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
        _t_table text := _cb_table_name(cb_fail_task_on_fail.name, 't');
BEGIN
        EXECUTE format(
            $QUERY$
            UPDATE %I
            SET on_fail_status = 'failed',
                    on_fail_error_message = $2,
                    on_fail_visible_at = CASE
                        WHEN $3 THEN 'infinity'::timestamptz
                        ELSE clock_timestamp() + make_interval(secs => $4 / 1000.0)
                    END
            WHERE id = $1
                AND on_fail_status = 'started'
            $QUERY$,
            _t_table
        )
        USING cb_fail_task_on_fail.run_id,
                    cb_fail_task_on_fail.error_message,
                    cb_fail_task_on_fail.retry_exhausted,
                    cb_fail_task_on_fail.retry_delay;
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
CREATE OR REPLACE FUNCTION cb_wait_task_output(
    name text,
    run_id bigint,
    poll_for int DEFAULT 5000,
    poll_interval int DEFAULT 200
)
RETURNS TABLE(status text, output jsonb, error_message text)
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_wait_task_output.name, 't');
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
                    SELECT t.status, t.output, coalesce(t.error_message, t.cancel_reason)
                    FROM %I t
                    WHERE t.id = $1
                    $QUERY$,
                    _t_table
                )
                USING cb_wait_task_output.run_id
                INTO _status, _output, _error_message;

        IF _status IS NULL THEN
            RAISE EXCEPTION 'cb: task run % not found for task %', cb_wait_task_output.run_id, cb_wait_task_output.name;
        END IF;

        IF _status IN ('completed', 'failed', 'skipped', 'canceled') THEN
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
-- cb_delete_task: Delete a task and clean up its bindings
CREATE OR REPLACE FUNCTION cb_delete_task(name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _t_table text := _cb_table_name(cb_delete_task.name, 't');
    _res boolean;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_t_table));

    EXECUTE format('DROP TABLE IF EXISTS %I;', _t_table);

    DELETE FROM cb_bindings
    WHERE target_name = cb_delete_task.name
      AND target_type = 'task';

    DELETE FROM cb_tasks t
    WHERE t.name = cb_delete_task.name
    RETURNING true
    INTO _res;

    IF coalesce(_res, false) THEN
        PERFORM _cb_notify(topic => 'catbird.task.deleted', message => jsonb_build_object('task', cb_delete_task.name)::text);
    END IF;

    RETURN coalesce(_res, false);
END;
$$;
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

    _f_table := _cb_table_name(cb_create_flow.name, 'f');
    _s_table := _cb_table_name(cb_create_flow.name, 's');
    _m_table := _cb_table_name(cb_create_flow.name, 'm');

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
            _condition := _cb_parse_condition(_step->>'condition');
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
      expired_at timestamptz,
      expires_at timestamptz,
      priority int NOT NULL DEFAULT 0,
      cancel_reason text,
      CONSTRAINT cb_status_valid CHECK (status IN ('started', 'canceling', 'completed', 'failed', 'canceled', 'expired')),
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
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (on_fail_visible_at, id) WHERE status = ''failed'' AND on_fail_status IN (''queued'', ''failed'');', _f_table || '_on_fail_poll_idx', _f_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (GREATEST(completed_at, failed_at, canceled_at, expired_at)) WHERE status IN (''completed'', ''failed'', ''canceled'', ''expired'');', _f_table || '_retention_idx', _f_table);

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

    PERFORM _cb_notify(topic => 'catbird.flow.created', message => jsonb_build_object('flow', cb_create_flow.name)::text);
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
    headers jsonb DEFAULT NULL,
    visible_at timestamptz DEFAULT NULL,
    priority int DEFAULT 0,
    expires_at timestamptz DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(cb_run_flow.name, 'f');
    _s_table text := _cb_table_name(cb_run_flow.name, 's');
    _id bigint;
    _remaining_steps int;
    _output_priority text[];
BEGIN
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
              INSERT INTO %I (concurrency_key, input, headers, remaining_steps, output_priority, status, priority, expires_at)
              VALUES ($1, $2, $3, $4, $5, 'started', $6, $7)
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
        USING cb_run_flow.concurrency_key, cb_run_flow.input, cb_run_flow.headers, _remaining_steps, _output_priority, cb_run_flow.priority, cb_run_flow.expires_at
        INTO _id;
    ELSE
        -- No deduplication
        EXECUTE format(
            $QUERY$
          INSERT INTO %I (input, headers, remaining_steps, output_priority, status, priority, expires_at)
          VALUES ($1, $2, $3, $4, 'started', $5, $6)
            RETURNING id
            $QUERY$,
            _f_table
        )
        USING cb_run_flow.input, cb_run_flow.headers, _remaining_steps, _output_priority, cb_run_flow.priority, cb_run_flow.expires_at
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

    PERFORM _cb_notify(topic => 'catbird.flow.started', message => jsonb_build_object('flow', cb_run_flow.name, 'run_id', _id)::text);

    RETURN _id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_complete_flow_early: complete a flow run early and cancel remaining work
CREATE OR REPLACE FUNCTION cb_complete_flow_early(
    flow_name text,
    run_id bigint,
    step_name text,
    output jsonb,
    reason text DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  _f_table text := _cb_table_name(cb_complete_flow_early.flow_name, 'f');
  _s_table text := _cb_table_name(cb_complete_flow_early.flow_name, 's');
  _m_table text := _cb_table_name(cb_complete_flow_early.flow_name, 'm');
    _status text;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'completed',
          completed_at = coalesce(completed_at, now()),
          output = $2
      WHERE id = $1
        AND status = 'started'
      RETURNING status
      $QUERY$,
      _f_table
    )
    USING cb_complete_flow_early.run_id, cb_complete_flow_early.output
    INTO _status;

    IF _status IS NULL THEN
      EXECUTE format('SELECT status FROM %I WHERE id = $1', _f_table)
      USING cb_complete_flow_early.run_id
      INTO _status;

      IF _status IS NULL THEN
        RETURN false;
      END IF;

      RETURN true;
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = coalesce(canceled_at, now())
      WHERE flow_run_id = $1
        AND status IN ('waiting_for_dependencies', 'waiting_for_signal', 'queued', 'started', 'waiting_for_map_tasks')
      $QUERY$,
      _s_table
    )
    USING cb_complete_flow_early.run_id;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = coalesce(canceled_at, now())
      WHERE flow_run_id = $1
        AND status IN ('queued', 'started')
      $QUERY$,
      _m_table
    )
    USING cb_complete_flow_early.run_id;

    PERFORM pg_notify(current_schema || '.cb_flow_stop_' || cb_complete_flow_early.flow_name, '');

    PERFORM _cb_notify(topic => 'catbird.flow.completed', message => jsonb_build_object('flow', cb_complete_flow_early.flow_name, 'run_id', cb_complete_flow_early.run_id)::text);

    RETURN true;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_maybe_finalize_flow_cancellation(name text, run_id bigint)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(_cb_maybe_finalize_flow_cancellation.name, 'f');
    _s_table text := _cb_table_name(_cb_maybe_finalize_flow_cancellation.name, 's');
    _m_table text := _cb_table_name(_cb_maybe_finalize_flow_cancellation.name, 'm');
    _active_steps int;
    _active_maps int;
    _updated boolean;
BEGIN
    EXECUTE format(
      'SELECT count(*) FROM %I WHERE flow_run_id = $1 AND status = ''started''',
      _s_table
    )
    USING _cb_maybe_finalize_flow_cancellation.run_id
    INTO _active_steps;

    EXECUTE format(
      'SELECT count(*) FROM %I WHERE flow_run_id = $1 AND status = ''started''',
      _m_table
    )
    USING _cb_maybe_finalize_flow_cancellation.run_id
    INTO _active_maps;

    IF _active_steps > 0 OR _active_maps > 0 THEN
      RETURN false;
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = coalesce(canceled_at, now())
      WHERE id = $1
        AND status = 'canceling'
      RETURNING true
      $QUERY$,
      _f_table
    )
    USING _cb_maybe_finalize_flow_cancellation.run_id
    INTO _updated;

    IF coalesce(_updated, false) THEN
        PERFORM _cb_notify(topic => 'catbird.flow.canceled', message => jsonb_build_object('flow', _cb_maybe_finalize_flow_cancellation.name, 'run_id', _cb_maybe_finalize_flow_cancellation.run_id)::text);
    END IF;

    RETURN coalesce(_updated, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_cancel_flow(
    name text,
    run_id bigint,
    reason text DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  _f_table text := _cb_table_name(cb_cancel_flow.name, 'f');
  _s_table text := _cb_table_name(cb_cancel_flow.name, 's');
  _m_table text := _cb_table_name(cb_cancel_flow.name, 'm');
    _status text;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = CASE
            WHEN status = 'started' THEN 'canceling'
            ELSE status
          END,
          cancel_requested_at = CASE
            WHEN status = 'started' THEN now()
            ELSE cancel_requested_at
          END,
          cancel_reason = coalesce($2, cancel_reason)
      WHERE id = $1
      RETURNING status
      $QUERY$,
      _f_table
    )
    USING cb_cancel_flow.run_id, cb_cancel_flow.reason
    INTO _status;

    IF _status IS NULL THEN
      EXECUTE format('SELECT status FROM %I WHERE id = $1', _f_table)
      USING cb_cancel_flow.run_id
      INTO _status;
      IF _status IS NULL THEN
        RETURN false;
      END IF;
      RETURN true;
    END IF;

    IF _status = 'canceling' THEN
        PERFORM pg_notify(current_schema || '.cb_flow_stop_' || cb_cancel_flow.name, '');
    END IF;

    -- Cancel non-started step runs
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = now()
      WHERE flow_run_id = $1
        AND status IN ('waiting_for_dependencies', 'waiting_for_signal', 'waiting_for_map_tasks', 'queued')
      $QUERY$,
      _s_table
    )
    USING cb_cancel_flow.run_id;

    -- Cancel non-started map tasks
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = now()
      WHERE flow_run_id = $1
        AND status = 'queued'
      $QUERY$,
      _m_table
    )
    USING cb_cancel_flow.run_id;

    PERFORM _cb_maybe_finalize_flow_cancellation(cb_cancel_flow.name, cb_cancel_flow.run_id);

    EXECUTE format('SELECT status FROM %I WHERE id = $1', _f_table)
    USING cb_cancel_flow.run_id
    INTO _status;

    RETURN true;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_finalize_flow_cancellation(name text, run_id bigint)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    RETURN _cb_maybe_finalize_flow_cancellation(cb_finalize_flow_cancellation.name, cb_finalize_flow_cancellation.run_id);
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_cancel_step_run(flow_name text, id bigint)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_cancel_step_run.flow_name, 's');
    _flow_run_id bigint;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = now()
      WHERE id = $1
        AND status IN ('waiting_for_dependencies', 'waiting_for_signal', 'queued', 'started', 'waiting_for_map_tasks')
      RETURNING flow_run_id
      $QUERY$,
      _s_table
    )
    USING cb_cancel_step_run.id
    INTO _flow_run_id;

    IF _flow_run_id IS NOT NULL THEN
      PERFORM _cb_maybe_finalize_flow_cancellation(cb_cancel_step_run.flow_name, _flow_run_id);
    END IF;

    RETURN _flow_run_id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_cancel_map_task_run(flow_name text, id bigint)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _m_table text := _cb_table_name(cb_cancel_map_task_run.flow_name, 'm');
    _flow_run_id bigint;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = now()
      WHERE id = $1
        AND status IN ('queued', 'started')
      RETURNING flow_run_id
      $QUERY$,
      _m_table
    )
    USING cb_cancel_map_task_run.id
    INTO _flow_run_id;

    IF _flow_run_id IS NOT NULL THEN
      PERFORM _cb_maybe_finalize_flow_cancellation(cb_cancel_map_task_run.flow_name, _flow_run_id);
    END IF;

    RETURN _flow_run_id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_start_steps: Start steps in a flow that are ready to run
-- Called automatically after step completion to start dependent steps
-- Evaluates conditions and can skip steps based on condition evaluation
-- Parameters:
--   flow_name: Flow name
--   flow_run_id: Flow run ID
-- Returns: void
CREATE OR REPLACE FUNCTION cb_start_steps(flow_name text, run_id bigint, initial_visible_at timestamptz DEFAULT NULL)
RETURNS void AS $$
DECLARE
    _f_table text := _cb_table_name(cb_start_steps.flow_name, 'f');
    _s_table text := _cb_table_name(cb_start_steps.flow_name, 's');
    _m_table text := _cb_table_name(cb_start_steps.flow_name, 'm');
    _flow_input jsonb;
    _step_to_process record;
    _step_condition jsonb;
    _step_inputs jsonb;
    _is_condition_true boolean;
    _map_items jsonb;
    _map_item_count int;
    _remaining int;
    _output_priority text[];
    _selected_output jsonb;
    _steps_processed_this_iteration int;
BEGIN
    -- Get flow input
    EXECUTE format(
    $QUERY$
    SELECT input, output_priority
    FROM %I
    WHERE id = $1
      AND status = 'started'
    $QUERY$,
    _f_table
    )
    USING cb_start_steps.run_id
    INTO _flow_input, _output_priority;

    -- If flow not found or not started, return
    IF _flow_input IS NULL THEN
    RETURN;
    END IF;

    -- Loop until no more steps can be started (handles cascading optional dependencies)
    LOOP
    _steps_processed_this_iteration := 0;

    -- Promote dependency waiters to signal waiters when deps are fully resolved but signal is still missing.
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'waiting_for_signal'
      WHERE flow_run_id = $1
        AND status = 'waiting_for_dependencies'
        AND remaining_dependencies = 0
        AND signal = true
        AND signal_input IS NULL
      $QUERY$,
      _s_table
    )
    USING cb_start_steps.run_id;

    -- Start all steps that are now ready to activate.
    -- Evaluate step conditions to decide if step should be skipped.
    FOR _step_to_process IN
    EXECUTE format(
      $QUERY$
                  SELECT sr.id, sr.step_name, sr.remaining_dependencies,
              sr.dependency_step_names,
                    sr.dependent_step_names,
             CASE
               WHEN cardinality(sr.dependency_step_names) = 0 THEN NULL
               WHEN sr.step_type = 'reducer' AND sr.condition IS NULL THEN NULL
               ELSE (
                 SELECT jsonb_object_agg(deps.step_name, CASE
                   WHEN deps.step_type IN ('mapper', 'generator') THEN (
                     SELECT coalesce(jsonb_agg(mt.output ORDER BY mt.item_idx), '[]'::jsonb)
                     FROM %I mt
                     WHERE mt.flow_run_id = deps.flow_run_id
                       AND mt.step_name = deps.step_name
                       AND mt.status = 'completed'
                   )
                   ELSE deps.output
                 END)
                 FROM %I deps
                 WHERE deps.flow_run_id = $1
                   AND deps.step_name = any(sr.dependency_step_names)
                   AND deps.status = 'completed'
               )
             END AS step_outputs,
             sr.signal_input,
             sr.condition,
                  sr.step_type,
             sr.map_source_step_name,
             sr.priority
      FROM %I sr
      WHERE sr.flow_run_id = $1
        AND (
          (sr.status = 'waiting_for_dependencies' AND sr.remaining_dependencies = 0 AND (NOT sr.signal OR sr.signal_input IS NOT NULL))
          OR
          (sr.status = 'waiting_for_signal' AND sr.remaining_dependencies = 0 AND sr.signal_input IS NOT NULL)
        )
      FOR UPDATE SKIP LOCKED
      $QUERY$,
      _m_table, _s_table, _s_table
    )
    USING cb_start_steps.run_id
    LOOP
    -- Check if step has a condition
    _step_condition := _step_to_process.condition;

    IF _step_condition IS NOT NULL THEN
      -- Build step_inputs: combine flow input, dependency outputs, and signal input
      -- Flow input accessible as input.*, dependency outputs as dependency step names, signal input as signal.*
      _step_inputs := jsonb_build_object('input', _flow_input);

      IF _step_to_process.step_outputs IS NOT NULL THEN
        _step_inputs := _step_inputs || _step_to_process.step_outputs;
      END IF;

      IF _step_to_process.signal_input IS NOT NULL THEN
        _step_inputs := _step_inputs || jsonb_build_object('signal', _step_to_process.signal_input);
      END IF;

      -- Evaluate the condition
      _is_condition_true := _cb_evaluate_condition(_step_condition, _step_inputs);

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

        -- Decrement dependent steps' remaining_dependencies and promote newly-unblocked steps.
        EXECUTE format(
          $QUERY$
          UPDATE %I
          SET remaining_dependencies = remaining_dependencies - 1,
              status = CASE
                WHEN remaining_dependencies - 1 = 0 THEN
                  CASE
                    WHEN signal = true AND signal_input IS NULL THEN 'waiting_for_signal'
                    ELSE 'waiting_for_dependencies'
                  END
                ELSE status
              END
          WHERE flow_run_id = $1
            AND step_name = any($2)
            AND status = 'waiting_for_dependencies'
          $QUERY$,
          _s_table
        )
        USING cb_start_steps.run_id, _step_to_process.dependent_step_names;

        -- Decrement remaining_steps in flow run for skipped step
        EXECUTE format(
          $QUERY$
          UPDATE %I
          SET remaining_steps = remaining_steps - 1
          WHERE id = $1 AND status = 'started'
          RETURNING remaining_steps
          $QUERY$,
          _f_table
        )
        USING cb_start_steps.run_id
        INTO _remaining;

        IF _remaining = 0 THEN
          EXECUTE format(
            $QUERY$
            SELECT CASE
              WHEN s.step_type IN ('mapper', 'generator') THEN (
                SELECT coalesce(jsonb_agg(m.output ORDER BY m.item_idx), '[]'::jsonb)
                FROM %I m
                WHERE m.flow_run_id = s.flow_run_id AND m.step_name = s.step_name AND m.status = 'completed'
              )
              ELSE s.output
            END
            FROM %I s
            WHERE s.flow_run_id = $1
              AND s.step_name = any($2)
              AND s.status = 'completed'
            ORDER BY array_position($2, s.step_name)
            LIMIT 1
            $QUERY$,
            _m_table, _s_table
          )
          USING cb_start_steps.run_id, _output_priority
          INTO _selected_output;

          IF _selected_output IS NULL THEN
            EXECUTE format(
              $QUERY$
              UPDATE %I
              SET status = 'failed',
                  failed_at = now(),
                  error_message = $2,
                  on_fail_status = 'queued',
                  on_fail_visible_at = now(),
                  on_fail_error_message = NULL,
                  on_fail_started_at = NULL,
                  on_fail_completed_at = NULL
              WHERE id = $1
                AND status = 'started'
              $QUERY$,
              _f_table
            )
            USING cb_start_steps.run_id, 'no output candidate produced output';

            PERFORM _cb_notify(topic => 'catbird.flow.failed', message => jsonb_build_object('flow', cb_start_steps.flow_name, 'run_id', cb_start_steps.run_id)::text);
          ELSE
            EXECUTE format(
              $QUERY$
              UPDATE %I
              SET status = 'completed',
                  completed_at = now(),
                  output = $2
              WHERE id = $1
                AND status = 'started'
              $QUERY$,
              _f_table
            )
            USING cb_start_steps.run_id, _selected_output;

            PERFORM _cb_notify(topic => 'catbird.flow.completed', message => jsonb_build_object('flow', cb_start_steps.flow_name, 'run_id', cb_start_steps.run_id)::text);
          END IF;
        END IF;

        _steps_processed_this_iteration := _steps_processed_this_iteration + 1;

        -- Continue to next step
        CONTINUE;
      END IF;
    END IF;

    IF _step_to_process.step_type = 'generator' THEN
      EXECUTE format(
        $QUERY$
        UPDATE %I
        SET status = 'queued',
            started_at = NULL,
            generator_status = 'started',
            map_tasks_spawned = 0,
            map_tasks_completed = 0,
            generator_error = NULL,
            visible_at = coalesce($2, now())
        WHERE id = $1
        $QUERY$,
        _s_table
      )
      USING _step_to_process.id, cb_start_steps.initial_visible_at;

      _steps_processed_this_iteration := _steps_processed_this_iteration + 1;
      CONTINUE;
    END IF;

    -- Map-step activation: spawn item-level map tasks coordinated in SQL
    IF _step_to_process.step_type = 'mapper' THEN
      IF _step_to_process.map_source_step_name IS NULL THEN
        _map_items := _flow_input;
      ELSE
        _map_items := _step_to_process.step_outputs -> _step_to_process.map_source_step_name;
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
        SET status = 'waiting_for_map_tasks',
            started_at = coalesce(started_at, now()),
            visible_at = coalesce($2, now())
        WHERE id = $1
        $QUERY$,
        _s_table
      )
      USING _step_to_process.id, cb_start_steps.initial_visible_at;

      -- Spawn map tasks in deterministic order
      EXECUTE format(
        $QUERY$
        INSERT INTO %I (flow_run_id, step_name, item_idx, flow_input, status, dependency_step_names, signal_input, item, visible_at, priority)
        SELECT $1,
               $2,
               ordinality - 1,
               $4,
               'queued',
               $5,
               $6,
               value,
               coalesce($3, now()),
               $8
        FROM jsonb_array_elements($7) WITH ORDINALITY
        ON CONFLICT (flow_run_id, step_name, item_idx) DO NOTHING
        $QUERY$,
        _m_table
      )
      USING cb_start_steps.run_id,
            _step_to_process.step_name,
            cb_start_steps.initial_visible_at,
            _flow_input,
            _step_to_process.dependency_step_names,
            _step_to_process.signal_input,
            _map_items,
            _step_to_process.priority;

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
      SET status = 'queued',
          started_at = NULL,
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

    PERFORM pg_notify(current_schema || '.cb_s_' || cb_start_steps.flow_name, '');
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
CREATE OR REPLACE FUNCTION cb_claim_steps(
    flow_name text,
    step_name text,
    quantity int,
    hide_for int
)
RETURNS SETOF cb_step_claim
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_claim_steps.flow_name, 's');
    _m_table text := _cb_table_name(cb_claim_steps.flow_name, 'm');
    _f_table text := _cb_table_name(cb_claim_steps.flow_name, 'f');
BEGIN
    IF cb_claim_steps.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_claim_steps.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    RETURN QUERY EXECUTE format(
        $QUERY$
        WITH runs AS (
          SELECT m.id
          FROM %I m
          WHERE m.step_name = $1
            AND m.visible_at <= clock_timestamp()
            AND m.status IN ('queued', 'started')
            AND EXISTS (
                SELECT 1
                FROM %I f
                WHERE f.id = m.flow_run_id
                  AND f.status = 'started'
                  AND (f.expires_at IS NULL OR f.expires_at > clock_timestamp())
            )
          ORDER BY m.priority DESC, m.id ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE %I m
        SET status = 'started',
            attempts = attempts + 1,
            started_at = clock_timestamp(),
            visible_at = clock_timestamp() + $3
        FROM runs
        WHERE m.id = runs.id
        RETURNING m.id,
                  m.flow_run_id,
                  m.attempts,
                  m.flow_input AS input,
                  CASE
                    WHEN cardinality(m.dependency_step_names) = 0 THEN NULL
                    WHEN m.step_type = 'reducer' THEN NULL
                    ELSE (
                      SELECT jsonb_object_agg(deps.step_name, CASE
                        WHEN deps.step_type IN ('mapper', 'generator') THEN (
                          SELECT coalesce(jsonb_agg(mt.output ORDER BY mt.item_idx), '[]'::jsonb)
                          FROM %I mt
                          WHERE mt.flow_run_id = deps.flow_run_id
                            AND mt.step_name = deps.step_name
                            AND mt.status = 'completed'
                        )
                        ELSE deps.output
                      END)
                      FROM %I deps
                      WHERE deps.flow_run_id = m.flow_run_id
                        AND deps.step_name = any(m.dependency_step_names)
                        AND deps.status = 'completed'
                    )
                  END AS step_outputs,
                  m.signal_input;
        $QUERY$,
        _s_table, _f_table, _s_table, _m_table, _s_table
    )
    USING cb_claim_steps.step_name, cb_claim_steps.quantity, make_interval(secs => cb_claim_steps.hide_for / 1000.0);
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
CREATE OR REPLACE FUNCTION cb_hide_steps(flow_name text, step_name text, ids bigint[], hide_for int)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_hide_steps.flow_name, 's');
BEGIN
    IF cb_hide_steps.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET visible_at = (clock_timestamp() + $2)
      WHERE id = any($1)
        AND step_name = $3
        AND status IN ('queued', 'started');
      $QUERY$,
      _s_table
    )
    USING cb_hide_steps.ids,
          make_interval(secs => cb_hide_steps.hide_for / 1000.0),
          cb_hide_steps.step_name;

    PERFORM pg_notify(current_schema || '.cb_s_' || cb_hide_steps.flow_name, to_char((clock_timestamp() + make_interval(secs => cb_hide_steps.hide_for / 1000.0)) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_poll_map_tasks: Poll map-item tasks for a specific map step
CREATE OR REPLACE FUNCTION cb_claim_map_tasks(
    flow_name text,
    step_name text,
    quantity int,
    hide_for int
)
RETURNS TABLE(id bigint, flow_run_id bigint, attempts int, input jsonb, step_outputs jsonb, signal_input jsonb, item jsonb)
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_claim_map_tasks.flow_name, 's');
    _m_table text := _cb_table_name(cb_claim_map_tasks.flow_name, 'm');
    _f_table text := _cb_table_name(cb_claim_map_tasks.flow_name, 'f');
BEGIN
    IF cb_claim_map_tasks.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;
    IF cb_claim_map_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    RETURN QUERY EXECUTE format(
      $QUERY$
      WITH runs AS (
        SELECT m.id, m.flow_run_id, m.item
        FROM %I m
        WHERE m.step_name = $1
          AND m.status IN ('queued', 'started')
          AND m.visible_at <= clock_timestamp()
          AND EXISTS (
              SELECT 1
              FROM %I f
              WHERE f.id = m.flow_run_id
                AND f.status = 'started'
          )
        ORDER BY m.priority DESC, m.id ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      )
      UPDATE %I m
      SET status = 'started',
          attempts = attempts + 1,
          started_at = clock_timestamp(),
          visible_at = clock_timestamp() + $3
      FROM runs
      WHERE m.id = runs.id
      RETURNING m.id,
                m.flow_run_id,
                m.attempts,
                m.flow_input AS input,
                CASE
                  WHEN cardinality(m.dependency_step_names) = 0 THEN NULL
                  ELSE (
                    SELECT jsonb_object_agg(deps.step_name, CASE
                      WHEN deps.step_type IN ('mapper', 'generator') THEN (
                        SELECT coalesce(jsonb_agg(mt.output ORDER BY mt.item_idx), '[]'::jsonb)
                        FROM %I mt
                        WHERE mt.flow_run_id = deps.flow_run_id
                          AND mt.step_name = deps.step_name
                          AND mt.status = 'completed'
                      )
                      ELSE deps.output
                    END)
                    FROM %I deps
                    WHERE deps.flow_run_id = m.flow_run_id
                      AND deps.step_name = any(m.dependency_step_names)
                      AND deps.status = 'completed'
                  )
                END AS step_outputs,
                m.signal_input,
                m.item;
      $QUERY$,
      _m_table, _f_table, _m_table, _m_table, _s_table
    )
    USING cb_claim_map_tasks.step_name,
          cb_claim_map_tasks.quantity,
          make_interval(secs => cb_claim_map_tasks.hide_for / 1000.0);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_hide_map_tasks: Hide map tasks from workers for a duration
CREATE OR REPLACE FUNCTION cb_hide_map_tasks(flow_name text, step_name text, ids bigint[], hide_for int)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _m_table text := _cb_table_name(cb_hide_map_tasks.flow_name, 'm');
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
        AND status IN ('queued', 'started')
      $QUERY$,
      _m_table
    )
    USING cb_hide_map_tasks.ids,
          make_interval(secs => cb_hide_map_tasks.hide_for / 1000.0),
          cb_hide_map_tasks.step_name;

    PERFORM pg_notify(current_schema || '.cb_s_' || cb_hide_map_tasks.flow_name, to_char((clock_timestamp() + make_interval(secs => cb_hide_map_tasks.hide_for / 1000.0)) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_spawn_generator_map_tasks: Spawn map tasks for a generator step batch
CREATE OR REPLACE FUNCTION cb_spawn_generator_map_tasks(
    flow_name text,
    step_name text,
    id bigint,
    items jsonb,
    visible_at timestamptz DEFAULT NULL
)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_spawn_generator_map_tasks.flow_name, 's');
    _m_table text := _cb_table_name(cb_spawn_generator_map_tasks.flow_name, 'm');
    _flow_run_id bigint;
    _spawn_offset int;
    _flow_input jsonb;
    _dependency_step_names text[];
    _signal_input jsonb;
    _priority int;
    _spawned int := 0;
BEGIN
    IF cb_spawn_generator_map_tasks.items IS NULL OR jsonb_typeof(cb_spawn_generator_map_tasks.items) <> 'array' THEN
        RAISE EXCEPTION 'cb: generator items must be a JSON array';
    END IF;

    IF jsonb_array_length(cb_spawn_generator_map_tasks.items) = 0 THEN
        RETURN 0;
    END IF;

    EXECUTE format(
      $QUERY$
      SELECT flow_run_id, map_tasks_spawned, flow_input, dependency_step_names, signal_input, priority
      FROM %I
      WHERE id = $1
        AND step_name = $2
        AND status = 'started'
        AND generator_status = 'started'
      FOR UPDATE
      $QUERY$,
      _s_table
    )
    USING cb_spawn_generator_map_tasks.id, cb_spawn_generator_map_tasks.step_name
    INTO _flow_run_id, _spawn_offset, _flow_input, _dependency_step_names, _signal_input, _priority;

    IF _flow_run_id IS NULL THEN
        RETURN 0;
    END IF;

    EXECUTE format(
      $QUERY$
      WITH ins AS (
        INSERT INTO %I (flow_run_id, step_name, item_idx, flow_input, status, dependency_step_names, signal_input, item, visible_at, priority)
        SELECT $1,
               $2,
               $3 + ordinality - 1,
               $4,
               'queued',
               $5,
               $6,
               value,
               coalesce($7, now()),
               $9
        FROM jsonb_array_elements($8) WITH ORDINALITY
        ON CONFLICT (flow_run_id, step_name, item_idx) DO NOTHING
        RETURNING 1
      )
      SELECT count(*)::int
      FROM ins
      $QUERY$,
      _m_table
    )
    USING _flow_run_id,
          cb_spawn_generator_map_tasks.step_name,
          _spawn_offset,
          _flow_input,
          _dependency_step_names,
          _signal_input,
          cb_spawn_generator_map_tasks.visible_at,
          cb_spawn_generator_map_tasks.items,
          _priority
    INTO _spawned;

    IF _spawned > 0 THEN
      EXECUTE format(
        $QUERY$
        UPDATE %I
        SET map_tasks_spawned = map_tasks_spawned + $2
        WHERE id = $1
          AND status = 'started'
          AND generator_status = 'started'
        $QUERY$,
        _s_table
      )
      USING cb_spawn_generator_map_tasks.id, _spawned;
    END IF;

    IF _spawned > 0 THEN
        PERFORM pg_notify(current_schema || '.cb_s_' || cb_spawn_generator_map_tasks.flow_name, '');
    END IF;

    RETURN coalesce(_spawned, 0);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_complete_generator_step: Mark generator as complete and finalize parent step if all map tasks are complete
CREATE OR REPLACE FUNCTION cb_complete_generator_step(flow_name text, step_name text, id bigint)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_complete_generator_step.flow_name, 's');
    _m_table text := _cb_table_name(cb_complete_generator_step.flow_name, 'm');
    _flow_run_id bigint;
    _spawned int;
    _completed int;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET generator_status = 'complete',
          status = CASE
              WHEN map_tasks_spawned = map_tasks_completed THEN status
              ELSE 'waiting_for_map_tasks'
          END
      WHERE id = $1
        AND step_name = $2
        AND status = 'started'
        AND generator_status = 'started'
      RETURNING flow_run_id, map_tasks_spawned, map_tasks_completed
      $QUERY$,
      _s_table
    )
    USING cb_complete_generator_step.id, cb_complete_generator_step.step_name
    INTO _flow_run_id, _spawned, _completed;

    IF _flow_run_id IS NULL THEN
      RETURN;
    END IF;

    IF _spawned <> _completed THEN
      RETURN;
    END IF;

    -- Item outputs are stored in cb_m_* and read live from there; pass NULL here.
    PERFORM cb_complete_step(cb_complete_generator_step.flow_name, cb_complete_generator_step.step_name, cb_complete_generator_step.id, NULL);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_fail_generator_step: Mark generator as failed and fail parent step/flow
CREATE OR REPLACE FUNCTION cb_fail_generator_step(flow_name text, step_name text, id bigint, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_fail_generator_step.flow_name, 's');
    _updated boolean;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET generator_status = 'failed',
          generator_error = $2
      WHERE id = $1
        AND step_name = $3
        AND status IN ('queued', 'started')
      RETURNING true
      $QUERY$,
      _s_table
    )
        USING cb_fail_generator_step.id,
          cb_fail_generator_step.error_message,
          cb_fail_generator_step.step_name
    INTO _updated;

    IF _updated IS NULL THEN
      RETURN;
    END IF;

    PERFORM cb_fail_step(
      cb_fail_generator_step.flow_name,
      cb_fail_generator_step.step_name,
      cb_fail_generator_step.id,
      cb_fail_generator_step.error_message
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_complete_map_tasks: Complete one or more map tasks and complete parent step when all items finish.
CREATE OR REPLACE FUNCTION cb_complete_map_tasks(flow_name text, step_name text, ids bigint[], outputs jsonb[])
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_complete_map_tasks.flow_name, 's');
    _m_table text := _cb_table_name(cb_complete_map_tasks.flow_name, 'm');
    _flow_run_id bigint;
    _updated_count int;
    _step_id bigint;
    _generator_status text;
    _spawned int;
    _completed int;
    _has_pending boolean;
BEGIN
    -- Complete all map tasks in a single UPDATE, count how many actually changed.
    -- unnest pairs ids with outputs positionally.
    EXECUTE format(
      $QUERY$
      WITH pairs AS (
        SELECT unnest($1::bigint[]) AS id, unnest($2::jsonb[]) AS output
      ),
      updated AS (
        UPDATE %I m
        SET status = 'completed',
            completed_at = now(),
            output = pairs.output
        FROM pairs
        WHERE m.id = pairs.id
          AND m.status = 'started'
        RETURNING m.flow_run_id
      )
      SELECT flow_run_id, count(*)::int
      FROM updated
      GROUP BY flow_run_id
      $QUERY$,
      _m_table
    )
    USING cb_complete_map_tasks.ids, cb_complete_map_tasks.outputs
    INTO _flow_run_id, _updated_count;

    IF _flow_run_id IS NULL OR _updated_count = 0 THEN
      RETURN;
    END IF;

    -- Atomically increment completed count by the batch size.
    -- Only one concurrent caller will see _completed = _spawned.
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET map_tasks_completed = CASE
          WHEN generator_status IS NULL THEN map_tasks_completed
          ELSE map_tasks_completed + $3
      END
      WHERE flow_run_id = $1
        AND step_name = $2
        AND status IN ('started', 'waiting_for_map_tasks')
      RETURNING id, generator_status, map_tasks_spawned, map_tasks_completed
      $QUERY$,
      _s_table
    )
    USING _flow_run_id, cb_complete_map_tasks.step_name, _updated_count
    INTO _step_id, _generator_status, _spawned, _completed;

    IF _step_id IS NULL THEN
      RETURN;
    END IF;

    -- Generator parent steps complete only after generator_status='complete' and all spawned map tasks are completed.
    IF _generator_status IS NOT NULL THEN
      IF _generator_status <> 'complete' OR _completed <> _spawned THEN
        RETURN;
      END IF;

      PERFORM cb_complete_step(cb_complete_map_tasks.flow_name, cb_complete_map_tasks.step_name, _step_id, NULL);
      RETURN;
    END IF;

    EXECUTE format(
      'SELECT EXISTS (SELECT 1 FROM %I WHERE flow_run_id = $1 AND step_name = $2 AND status IN (''queued'', ''started''))',
      _m_table
    )
    USING _flow_run_id, cb_complete_map_tasks.step_name
    INTO _has_pending;

    IF _has_pending THEN
      RETURN;
    END IF;

    PERFORM cb_complete_step(cb_complete_map_tasks.flow_name, cb_complete_map_tasks.step_name, _step_id, NULL);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_fail_map_task: Fail one map task and fail parent step/flow
CREATE OR REPLACE FUNCTION cb_fail_map_task(flow_name text, step_name text, id bigint, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_fail_map_task.flow_name, 's');
    _m_table text := _cb_table_name(cb_fail_map_task.flow_name, 'm');
    _f_table text := _cb_table_name(cb_fail_map_task.flow_name, 'f');
    _flow_run_id bigint;
    _step_id bigint;
    _failed_item jsonb;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'failed',
          failed_at = now(),
          error_message = $2
      WHERE id = $1
        AND status IN ('queued', 'started')
      RETURNING flow_run_id, item
      $QUERY$,
      _m_table
    )
    USING cb_fail_map_task.id, cb_fail_map_task.error_message
    INTO _flow_run_id, _failed_item;

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

    EXECUTE format(
      $QUERY$
      UPDATE %I f
      SET failed_step_input = $2
      WHERE f.id = $1
        AND f.status = 'failed'
        AND f.failed_step_name = $3
        AND f.failed_step_input = f.input
      $QUERY$,
      _f_table
    )
    USING _flow_run_id, _failed_item, cb_fail_map_task.step_name;
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
CREATE OR REPLACE FUNCTION cb_complete_step(flow_name text, step_name text, id bigint, output jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(cb_complete_step.flow_name, 'f');
    _m_table text := _cb_table_name(cb_complete_step.flow_name, 'm');
    _s_table text := _cb_table_name(cb_complete_step.flow_name, 's');
    _flow_run_id bigint;
    _dependent_step_names text[];
    _remaining int;
    _output_priority text[];
    _selected_output jsonb;
BEGIN
    -- Complete step run atomically - only succeeds if status='started'
    EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'completed',
        completed_at = now(),
        output = $2
    WHERE id = $1
      AND status IN ('started', 'waiting_for_map_tasks')
    RETURNING flow_run_id, dependent_step_names
    $QUERY$,
    _s_table
    )
    USING cb_complete_step.id, cb_complete_step.output
    INTO _flow_run_id, _dependent_step_names;

    -- If step wasn't in 'started' status, return early (already completed or failed)
    IF _flow_run_id IS NULL THEN
    RETURN;
    END IF;

    -- Atomically decrement remaining_steps and get new value
    EXECUTE format(
    $QUERY$
    UPDATE %I
    SET remaining_steps = remaining_steps - 1
    WHERE id = $1 AND status = 'started'
    RETURNING remaining_steps, output_priority
    $QUERY$,
    _f_table
    )
    USING _flow_run_id
    INTO _remaining, _output_priority;

    -- If flow already completed/failed, return early
    IF _remaining IS NULL THEN
    RETURN;
    END IF;

    -- Decrement dependent step run remaining_dependencies atomically
    EXECUTE format(
    $QUERY$
    UPDATE %I
    SET remaining_dependencies = remaining_dependencies - 1,
        status = CASE
          WHEN remaining_dependencies - 1 = 0 THEN
            CASE
              WHEN signal = true AND signal_input IS NULL THEN 'waiting_for_signal'
              ELSE 'waiting_for_dependencies'
            END
          ELSE status
        END
    WHERE flow_run_id = $1
      AND step_name = any($2)
      AND status = 'waiting_for_dependencies'
    $QUERY$,
    _s_table
    )
    USING _flow_run_id, _dependent_step_names;

    -- Maybe complete flow run - only if remaining_steps reached 0
    IF _remaining = 0 THEN
    EXECUTE format(
      $QUERY$
      SELECT CASE
        WHEN s.step_type IN ('mapper', 'generator') THEN (
          SELECT coalesce(jsonb_agg(m.output ORDER BY m.item_idx), '[]'::jsonb)
          FROM %I m
          WHERE m.flow_run_id = s.flow_run_id AND m.step_name = s.step_name AND m.status = 'completed'
        )
        ELSE s.output
      END
      FROM %I s
      WHERE s.flow_run_id = $1
        AND s.step_name = any($2)
        AND s.status = 'completed'
      ORDER BY array_position($2, s.step_name)
      LIMIT 1
      $QUERY$,
      _m_table, _s_table
    )
    USING _flow_run_id, _output_priority
    INTO _selected_output;

    IF _selected_output IS NULL THEN
      EXECUTE format(
        $QUERY$
        UPDATE %I
        SET status = 'failed',
            failed_at = now(),
            error_message = $2,
            on_fail_status = 'queued',
            on_fail_visible_at = now(),
            on_fail_error_message = NULL,
            on_fail_started_at = NULL,
            on_fail_completed_at = NULL
        WHERE id = $1
          AND status = 'started'
        $QUERY$,
        _f_table
      )
      USING _flow_run_id, 'no output candidate produced output';

      PERFORM _cb_notify(topic => 'catbird.flow.failed', message => jsonb_build_object('flow', cb_complete_step.flow_name, 'run_id', _flow_run_id)::text);
    ELSE
      EXECUTE format(
        $QUERY$
        UPDATE %I
        SET status = 'completed',
            completed_at = now(),
            output = $2
        WHERE id = $1
          AND status = 'started'
        $QUERY$,
        _f_table
      )
      USING _flow_run_id, _selected_output;

      PERFORM _cb_notify(topic => 'catbird.flow.completed', message => jsonb_build_object('flow', cb_complete_step.flow_name, 'run_id', _flow_run_id)::text);
    END IF;
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
CREATE OR REPLACE FUNCTION cb_fail_step(flow_name text, step_name text, id bigint, error_message text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(cb_fail_step.flow_name, 'f');
    _s_table text := _cb_table_name(cb_fail_step.flow_name, 's');
    _m_table text := _cb_table_name(cb_fail_step.flow_name, 'm');
    _flow_run_id bigint;
BEGIN
    -- Fail step run atomically - only succeeds if status is non-terminal.
    EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'failed',
        failed_at = now(),
        error_message = $2
    WHERE id = $1
      AND status IN ('waiting_for_dependencies', 'waiting_for_signal', 'queued', 'started', 'waiting_for_map_tasks')
    RETURNING flow_run_id
    $QUERY$,
    _s_table
    )
    USING cb_fail_step.id, cb_fail_step.error_message
    INTO _flow_run_id;

    -- If step wasn't in a non-terminal status, return early (already terminal)
    IF _flow_run_id IS NULL THEN
    RETURN;
    END IF;

    -- Fail flow run atomically - only if status is 'started'
    EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'failed',
        failed_at = now(),
        error_message = $2,
        failed_step_name = $3,
        failed_step_id = $4,
        failed_step_input = (SELECT input FROM %I flow_input WHERE flow_input.id = $1),
        failed_step_signal_input = (SELECT signal_input FROM %I step_run WHERE step_run.id = $4),
        failed_step_attempts = (SELECT attempts FROM %I step_run WHERE step_run.id = $4),
        on_fail_status = 'queued',
        on_fail_visible_at = now(),
        on_fail_error_message = NULL,
        on_fail_started_at = NULL,
        on_fail_completed_at = NULL
    WHERE id = $1
      AND status = 'started'
    $QUERY$,
    _f_table,
    _f_table,
    _s_table,
    _s_table
    )
    USING _flow_run_id, cb_fail_step.error_message, cb_fail_step.step_name, cb_fail_step.id;

    PERFORM pg_notify(current_schema || '.cb_flow_stop_' || cb_fail_step.flow_name, '');
    PERFORM pg_notify(current_schema || '.cb_f_onfail_' || cb_fail_step.flow_name, '');

    PERFORM _cb_notify(topic => 'catbird.flow.failed', message => jsonb_build_object('flow', cb_fail_step.flow_name, 'run_id', _flow_run_id)::text);

    -- Propagate terminal failure to all remaining step runs for this flow run.
    EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'failed',
        failed_at = coalesce(failed_at, now()),
        error_message = coalesce(error_message, $2)
    WHERE flow_run_id = $1
      AND status IN ('waiting_for_dependencies', 'waiting_for_signal', 'queued', 'started', 'waiting_for_map_tasks')
    $QUERY$,
    _s_table
    )
    USING _flow_run_id, cb_fail_step.error_message;

    -- Propagate terminal failure to all remaining map task runs for this flow run.
    EXECUTE format(
    $QUERY$
    UPDATE %I
    SET status = 'failed',
        failed_at = coalesce(failed_at, now()),
        error_message = coalesce(error_message, $2)
    WHERE flow_run_id = $1
      AND status IN ('queued', 'started')
    $QUERY$,
    _m_table
    )
    USING _flow_run_id, cb_fail_step.error_message;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_claim_flow_on_fail: Claim failed flow runs for on-fail handling
-- Parameters:
--   name: Flow name
--   quantity: Number of failed flow runs to claim (must be > 0)
-- Returns: Flow runs claimed for on-fail processing with failure context
CREATE OR REPLACE FUNCTION cb_claim_flow_on_fail(name text, quantity int)
RETURNS TABLE(
    id bigint,
    input jsonb,
    error_message text,
    on_fail_attempts int,
    started_at timestamptz,
    failed_at timestamptz,
    concurrency_key text,
    failed_step_name text,
    failed_step_input jsonb,
    failed_step_signal_input jsonb,
    failed_step_attempts int
)
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(cb_claim_flow_on_fail.name, 'f');
BEGIN
    IF cb_claim_flow_on_fail.quantity <= 0 THEN
        RAISE EXCEPTION 'cb: quantity must be greater than 0';
    END IF;

    RETURN QUERY EXECUTE format(
      $QUERY$
      WITH runs AS (
        SELECT f.id
        FROM %I f
        WHERE f.status = 'failed'
          AND f.on_fail_status IN ('queued', 'failed')
          AND coalesce(f.on_fail_visible_at, clock_timestamp()) <= clock_timestamp()
        ORDER BY f.id ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE %I f
      SET on_fail_status = 'started',
          on_fail_started_at = clock_timestamp(),
          on_fail_attempts = f.on_fail_attempts + 1
      FROM runs
      WHERE f.id = runs.id
      RETURNING f.id,
                f.input,
                f.error_message,
                f.on_fail_attempts,
                f.started_at,
                f.failed_at,
                f.concurrency_key,
                f.failed_step_name,
                f.failed_step_input,
                f.failed_step_signal_input,
                f.failed_step_attempts
      $QUERY$,
      _f_table,
      _f_table
    )
    USING cb_claim_flow_on_fail.quantity;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_get_flow_step_output: Fetch the completed output of a single step on demand.
-- Handles mapper/generator steps by aggregating from cb_m_* in item order.
-- Returns NULL if the step is not found or not completed.
CREATE OR REPLACE FUNCTION cb_get_flow_step_output(
    flow_name text,
    run_id bigint,
    step_name text
)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_get_flow_step_output.flow_name, 's');
    _m_table text := _cb_table_name(cb_get_flow_step_output.flow_name, 'm');
    _step_type text;
    _output jsonb;
BEGIN
    EXECUTE format(
        'SELECT step_type FROM %I WHERE flow_run_id = $1 AND step_name = $2 AND status = ''completed''',
        _s_table
    )
    USING cb_get_flow_step_output.run_id, cb_get_flow_step_output.step_name
    INTO _step_type;

    IF _step_type IS NULL THEN
        RETURN NULL;
    END IF;

    IF _step_type IN ('mapper', 'generator') THEN
        EXECUTE format(
            'SELECT coalesce(jsonb_agg(output ORDER BY item_idx), ''[]''::jsonb) FROM %I WHERE flow_run_id = $1 AND step_name = $2 AND status = ''completed''',
            _m_table
        )
        USING cb_get_flow_step_output.run_id, cb_get_flow_step_output.step_name
        INTO _output;
    ELSE
        EXECUTE format(
            'SELECT output FROM %I WHERE flow_run_id = $1 AND step_name = $2 AND status = ''completed''',
            _s_table
        )
        USING cb_get_flow_step_output.run_id, cb_get_flow_step_output.step_name
        INTO _output;
    END IF;

    RETURN _output;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_complete_flow_on_fail: Mark on-fail handling as completed for a flow run
CREATE OR REPLACE FUNCTION cb_complete_flow_on_fail(name text, run_id bigint)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(cb_complete_flow_on_fail.name, 'f');
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET on_fail_status = 'completed',
          on_fail_completed_at = clock_timestamp(),
          on_fail_error_message = NULL
      WHERE id = $1
        AND on_fail_status = 'started'
      $QUERY$,
      _f_table
    )
    USING cb_complete_flow_on_fail.run_id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_fail_flow_on_fail: Mark on-fail handling as failed and schedule retry
CREATE OR REPLACE FUNCTION cb_fail_flow_on_fail(
    name text,
    run_id bigint,
    error_message text,
    retry_exhausted boolean,
    retry_delay bigint
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(cb_fail_flow_on_fail.name, 'f');
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET on_fail_status = 'failed',
          on_fail_error_message = $2,
          on_fail_visible_at = CASE
            WHEN $3 THEN 'infinity'::timestamptz
            ELSE clock_timestamp() + make_interval(secs => $4 / 1000.0)
          END
      WHERE id = $1
        AND on_fail_status = 'started'
      $QUERY$,
      _f_table
    )
    USING cb_fail_flow_on_fail.run_id,
          cb_fail_flow_on_fail.error_message,
          cb_fail_flow_on_fail.retry_exhausted,
          cb_fail_flow_on_fail.retry_delay;
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
CREATE OR REPLACE FUNCTION cb_signal_flow(flow_name text, run_id bigint, step_name text, input jsonb)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_signal_flow.flow_name, 's');
    _updated boolean;
BEGIN
    -- Atomically update signal_input - only succeeds for a step that is waiting for dependencies or signal,
    -- requires signal, and has not already received one.
    EXECUTE format(
    $QUERY$
    UPDATE %I sr
    SET signal_input = $3
    WHERE sr.flow_run_id = $1
      AND sr.step_name = $2
      AND sr.status IN ('waiting_for_dependencies', 'waiting_for_signal')
      AND sr.signal_input IS NULL
      AND sr.signal = true
    RETURNING true
    $QUERY$,
    _s_table
    )
    USING cb_signal_flow.run_id, cb_signal_flow.step_name, cb_signal_flow.input
    INTO _updated;

    IF _updated IS NULL THEN
    RETURN false;
    END IF;

    -- Try to start steps now that signal has been delivered
    PERFORM cb_start_steps(cb_signal_flow.flow_name, cb_signal_flow.run_id, NULL);

    RETURN true;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_get_flow_step_status: Get status details for a single step run
-- Parameters:
--   flow_name: Flow name
--   run_id: Flow run ID
--   step_name: Step name within the flow
-- Returns: step status details including attempts, timestamps, and output/error
CREATE OR REPLACE FUNCTION cb_get_flow_step_status(
    flow_name text,
    run_id bigint,
    step_name text
)
RETURNS TABLE(
    id bigint,
    status text,
    attempts int,
    output jsonb,
    error_message text,
    created_at timestamptz,
    visible_at timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    skipped_at timestamptz,
    canceled_at timestamptz
)
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_get_flow_step_status.flow_name, 's');
BEGIN
    EXECUTE format(
        $QUERY$
        SELECT s.id,
            s.status,
            s.attempts,
            s.output,
            s.error_message,
            s.created_at,
            s.visible_at,
            s.started_at,
            s.completed_at,
            s.failed_at,
            s.skipped_at,
            s.canceled_at
    FROM %I s
    WHERE s.flow_run_id = $1
    AND s.step_name = $2
        $QUERY$,
        _s_table
    )
    USING cb_get_flow_step_status.run_id, cb_get_flow_step_status.step_name
    INTO id, status, attempts, output, error_message, created_at, visible_at, started_at, completed_at, failed_at, skipped_at, canceled_at;

    IF id IS NULL THEN
        RAISE EXCEPTION 'cb: step run not found for flow %, run %, step %', cb_get_flow_step_status.flow_name, cb_get_flow_step_status.run_id, cb_get_flow_step_status.step_name;
    END IF;

    RETURN NEXT;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_wait_flow_step_output: Long-poll for a step reaching terminal state
-- Parameters:
--   flow_name: Flow name
--   run_id: Flow run ID
--   step_name: Step name within the flow
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: status/output/error_message once step reaches terminal state, or no rows on timeout
CREATE OR REPLACE FUNCTION cb_wait_flow_step_output(
    flow_name text,
    run_id bigint,
    step_name text,
    poll_for int DEFAULT 5000,
    poll_interval int DEFAULT 200
)
RETURNS TABLE(status text, output jsonb, error_message text)
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_wait_flow_step_output.flow_name, 's');
    _status text;
    _output jsonb;
    _error_message text;
    _sleep_for double precision;
    _stop_at timestamp;
BEGIN
    IF cb_wait_flow_step_output.poll_for <= 0 THEN
        RAISE EXCEPTION 'cb: poll_for must be greater than 0';
    END IF;

    IF cb_wait_flow_step_output.poll_interval <= 0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be greater than 0';
    END IF;

    _sleep_for := cb_wait_flow_step_output.poll_interval / 1000.0;

    IF _sleep_for >= cb_wait_flow_step_output.poll_for / 1000.0 THEN
        RAISE EXCEPTION 'cb: poll_interval must be smaller than poll_for';
    END IF;

    _stop_at := clock_timestamp() + make_interval(secs => cb_wait_flow_step_output.poll_for / 1000.0);

    LOOP
        IF clock_timestamp() >= _stop_at THEN
            RETURN;
        END IF;

        _status := NULL;
        _output := NULL;
        _error_message := NULL;

        EXECUTE format(
            $QUERY$
            SELECT s.status, s.output, s.error_message
            FROM %I s
            WHERE s.flow_run_id = $1
            AND s.step_name = $2
            $QUERY$,
            _s_table
        )
        USING cb_wait_flow_step_output.run_id, cb_wait_flow_step_output.step_name
        INTO _status, _output, _error_message;

        IF _status IS NULL THEN
            RAISE EXCEPTION 'cb: step run not found for flow %, run %, step %', cb_wait_flow_step_output.flow_name, cb_wait_flow_step_output.run_id, cb_wait_flow_step_output.step_name;
        END IF;

        IF _status IN ('completed', 'failed', 'skipped', 'canceled') THEN
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
-- cb_wait_flow_output: Long-poll for flow completion without client-side polling loops
-- Parameters:
--   name: Flow name
--   run_id: Flow run ID
--   poll_for: Total duration in milliseconds to poll before timing out (must be > 0)
--   poll_interval: Duration in milliseconds between poll attempts (must be > 0 and < poll_for)
-- Returns: status/output/error_message once run reaches terminal state, or no rows on timeout
CREATE OR REPLACE FUNCTION cb_wait_flow_output(
    flow_name text,
    run_id bigint,
    poll_for int DEFAULT 5000,
    poll_interval int DEFAULT 200
)
RETURNS TABLE(status text, output jsonb, error_message text)
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(cb_wait_flow_output.flow_name, 'f');
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
          SELECT f.status, f.output, coalesce(f.error_message, f.cancel_reason)
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

        IF _status IN ('completed', 'failed', 'canceled') THEN
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
-- cb_delete_flow: Delete a flow and clean up its bindings
CREATE OR REPLACE FUNCTION cb_delete_flow(name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := _cb_table_name(cb_delete_flow.name, 'f');
    _s_table text := _cb_table_name(cb_delete_flow.name, 's');
    _m_table text := _cb_table_name(cb_delete_flow.name, 'm');
    _res boolean;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_f_table));

    DELETE FROM cb_step_dependencies d
    WHERE d.flow_name = cb_delete_flow.name;

    DELETE FROM cb_steps s
    WHERE s.flow_name = cb_delete_flow.name;

    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', _m_table);
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', _s_table);
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', _f_table);

    DELETE FROM cb_bindings
    WHERE target_name = cb_delete_flow.name
      AND target_type = 'flow';

    DELETE FROM cb_flows f
    WHERE f.name = cb_delete_flow.name
    RETURNING true
    INTO _res;

    IF coalesce(_res, false) THEN
        PERFORM _cb_notify(topic => 'catbird.flow.deleted', message => jsonb_build_object('flow', cb_delete_flow.name)::text);
    END IF;

    RETURN coalesce(_res, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_purge_task_runs: Delete terminal task runs older than a given duration.
-- Terminal statuses: completed, failed, skipped, canceled.
-- Parameters:
--   name: Task name
--   older_than: Delete runs finished more than this duration ago
-- Returns: int - deleted row count
CREATE OR REPLACE FUNCTION cb_purge_task_runs(name text, older_than interval)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _table text := _cb_table_name(cb_purge_task_runs.name, 't');
    _deleted int := 0;
BEGIN
    IF to_regclass(_table) IS NULL THEN
        RETURN 0;
    END IF;
    EXECUTE format(
        'DELETE FROM %I
        WHERE status IN (''completed'', ''failed'', ''skipped'', ''canceled'', ''expired'')
          AND GREATEST(completed_at, failed_at, skipped_at, canceled_at, expired_at) < $1',
        _table
    ) USING (now() - cb_purge_task_runs.older_than);

    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_purge_flow_runs: Delete terminal flow runs older than a given duration.
-- Terminal statuses: completed, failed, canceled.
-- Step runs and map tasks are deleted automatically via ON DELETE CASCADE.
-- Parameters:
--   name: Flow name
--   older_than: Delete runs finished more than this duration ago
-- Returns: int - deleted row count
CREATE OR REPLACE FUNCTION cb_purge_flow_runs(name text, older_than interval)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _table text := _cb_table_name(cb_purge_flow_runs.name, 'f');
    _deleted int := 0;
BEGIN
    IF to_regclass(_table) IS NULL THEN
        RETURN 0;
    END IF;
    EXECUTE format(
        'DELETE FROM %I
        WHERE status IN (''completed'', ''failed'', ''canceled'', ''expired'')
          AND GREATEST(completed_at, failed_at, canceled_at, expired_at) < $1',
        _table
    ) USING (now() - cb_purge_flow_runs.older_than);

    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- Update cb_gc to also clean up stale Wire nodes.
-- Wire nodes that haven't heartbeated in 15 seconds are considered stale.
-- CASCADE deletes presence rows automatically.
CREATE OR REPLACE FUNCTION cb_gc()
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    _rec record;
    _expired_queues_deleted int := 0;
    _expired_messages_deleted int := 0;
    _expired_task_runs int := 0;
    _expired_flow_runs int := 0;
    _stale_workers_deleted int := 0;
    _stale_wire_nodes_deleted int := 0;
    _task_runs_purged int := 0;
    _flow_runs_purged int := 0;
    _purged int;
    _purged_id bigint;
    _purged_worker_id uuid;
BEGIN
    -- Delete queues whose expiry has passed
    FOR _rec IN
        SELECT name
        FROM cb_queues
        WHERE expires_at IS NOT NULL AND expires_at <= now()
    LOOP
        IF cb_delete_queue(_rec.name) THEN
            _expired_queues_deleted := _expired_queues_deleted + 1;
        END IF;
    END LOOP;

    -- Delete expired messages from all queues
    FOR _rec IN
        SELECT name FROM cb_queues
    LOOP
        EXECUTE format('DELETE FROM %I WHERE expires_at IS NOT NULL AND expires_at <= now()', _cb_table_name(_rec.name, 'q'));
        GET DIAGNOSTICS _purged = ROW_COUNT;
        _expired_messages_deleted := _expired_messages_deleted + _purged;
    END LOOP;

    -- Expire task runs past their expires_at
    FOR _rec IN
        SELECT name FROM cb_tasks
    LOOP
        FOR _purged_id IN
            EXECUTE format(
                'UPDATE %I SET status = ''expired'', expired_at = now()
                WHERE status IN (''queued'', ''started'')
                  AND expires_at IS NOT NULL AND expires_at <= now()
                RETURNING id',
                _cb_table_name(_rec.name, 't')
            )
        LOOP
            PERFORM _cb_notify(topic => 'catbird.task.expired', message => jsonb_build_object('task', _rec.name, 'run_id', _purged_id)::text);
            _expired_task_runs := _expired_task_runs + 1;
        END LOOP;
    END LOOP;

    -- Expire flow runs past their expires_at
    FOR _rec IN
        SELECT name FROM cb_flows
    LOOP
        FOR _purged_id IN
            EXECUTE format(
                'UPDATE %I SET status = ''expired'', expired_at = now()
                WHERE status IN (''started'', ''canceling'')
                  AND expires_at IS NOT NULL AND expires_at <= now()
                RETURNING id',
                _cb_table_name(_rec.name, 'f')
            )
        LOOP
            PERFORM _cb_notify(topic => 'catbird.flow.expired', message => jsonb_build_object('flow', _rec.name, 'run_id', _purged_id)::text);
            _expired_flow_runs := _expired_flow_runs + 1;
        END LOOP;
    END LOOP;

    -- Delete workers with no heartbeat for more than 5 minutes
    FOR _purged_worker_id IN
        DELETE FROM cb_workers
        WHERE last_heartbeat_at < now() - INTERVAL '5 minutes'
        RETURNING id
    LOOP
        PERFORM _cb_notify(topic => 'catbird.worker.stopped', message => jsonb_build_object('worker_id', _purged_worker_id)::text);
        _stale_workers_deleted := _stale_workers_deleted + 1;
    END LOOP;

    -- Delete Wire nodes with no heartbeat for more than 15 seconds
    DELETE FROM cb_wire_nodes
    WHERE last_heartbeat_at < now() - INTERVAL '15 seconds';
    GET DIAGNOSTICS _stale_wire_nodes_deleted = ROW_COUNT;

    FOR _rec IN
        SELECT name, retention_period
        FROM cb_tasks
        WHERE retention_period IS NOT NULL
    LOOP
        _purged := cb_purge_task_runs(_rec.name, _rec.retention_period);
        _task_runs_purged := _task_runs_purged + _purged;
    END LOOP;

    FOR _rec IN
        SELECT name, retention_period
        FROM cb_flows
        WHERE retention_period IS NOT NULL
    LOOP
        _purged := cb_purge_flow_runs(_rec.name, _rec.retention_period);
        _flow_runs_purged := _flow_runs_purged + _purged;
    END LOOP;

    RETURN jsonb_build_object(
        'expired_queues_deleted', _expired_queues_deleted,
        'expired_messages_deleted', _expired_messages_deleted,
        'expired_task_runs', _expired_task_runs,
        'expired_flow_runs', _expired_flow_runs,
        'stale_workers_deleted', _stale_workers_deleted,
        'stale_wire_nodes_deleted', _stale_wire_nodes_deleted,
        'task_runs_purged', _task_runs_purged,
        'flow_runs_purged', _flow_runs_purged
    );
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

    PERFORM _cb_notify(topic => 'catbird.worker.started', message => jsonb_build_object('worker_id', cb_worker_started.id)::text);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_worker_heartbeat: Update worker's last heartbeat timestamp and perform cleanup
-- Called periodically by workers to indicate they are still alive
-- Also opportunistically runs garbage collection via cb_gc()
-- Parameters:
--   id: Worker UUID
-- Returns: jsonb report with `gc_info` key
CREATE OR REPLACE FUNCTION cb_worker_heartbeat(id uuid)
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    _updated int := 0;
BEGIN
    UPDATE cb_workers
    SET last_heartbeat_at = now()
    WHERE cb_workers.id = cb_worker_heartbeat.id;
    GET DIAGNOSTICS _updated = ROW_COUNT;

    IF _updated = 0 THEN
        RAISE EXCEPTION 'cb: worker not found for heartbeat: %', cb_worker_heartbeat.id;
    END IF;

    -- Opportunistically run garbage collection
    RETURN jsonb_build_object(
        'gc_info', cb_gc()
    );
END;
$$;
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

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_cron_expand_spec(cron_spec text)
RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    _spec text := lower(trim(cron_spec));
BEGIN
    IF _spec IS NULL OR _spec = '' THEN
        RAISE EXCEPTION 'cb: cron spec must not be empty';
    END IF;

    CASE _spec
        WHEN '@yearly', '@annually' THEN RETURN '0 0 1 1 *';
        WHEN '@monthly' THEN RETURN '0 0 1 * *';
        WHEN '@weekly' THEN RETURN '0 0 * * 0';
        WHEN '@daily', '@midnight' THEN RETURN '0 0 * * *';
        WHEN '@hourly' THEN RETURN '0 * * * *';
        ELSE
            RETURN regexp_replace(_spec, '\s+', ' ', 'g');
    END CASE;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_cron_parse_field(field text, min_value int, max_value int, allow_seven_as_sunday boolean DEFAULT false)
RETURNS int[]
LANGUAGE plpgsql AS $$
DECLARE
    _field text := lower(trim(field));
    _token text;
    _result int[] := ARRAY[]::int[];
    _vals int[];
    _step int;
    _start int;
    _end int;
    _value int;
BEGIN
    IF _field IS NULL OR _field = '' THEN
        RAISE EXCEPTION 'cb: cron field must not be empty';
    END IF;

    IF _field = '*' THEN
        SELECT array_agg(i ORDER BY i)
        INTO _result
        FROM generate_series(min_value, max_value) AS g(i);
        RETURN _result;
    END IF;

    FOREACH _token IN ARRAY regexp_split_to_array(_field, ',')
    LOOP
        _token := trim(_token);

        IF _token = '*' THEN
            SELECT array_agg(i ORDER BY i)
            INTO _vals
            FROM generate_series(min_value, max_value) AS g(i);
            _result := _result || coalesce(_vals, ARRAY[]::int[]);
        ELSIF _token ~ '^\*/\d+$' THEN
            _step := split_part(_token, '/', 2)::int;
            IF _step <= 0 THEN
                RAISE EXCEPTION 'cb: invalid cron step value in token "%"', _token;
            END IF;

            SELECT array_agg(i ORDER BY i)
            INTO _vals
            FROM generate_series(min_value, max_value, _step) AS g(i);
            _result := _result || coalesce(_vals, ARRAY[]::int[]);
        ELSIF _token ~ '^\d+$' THEN
            _value := _token::int;
            IF allow_seven_as_sunday AND _value = 7 THEN
                _value := 0;
            END IF;
            IF _value < min_value OR _value > max_value THEN
                RAISE EXCEPTION 'cb: cron value % out of range [% - %] in token "%"', _value, min_value, max_value, _token;
            END IF;
            _result := _result || ARRAY[_value];
        ELSIF _token ~ '^\d+-\d+(\/\d+)?$' THEN
            _start := split_part(split_part(_token, '/', 1), '-', 1)::int;
            _end := split_part(split_part(_token, '/', 1), '-', 2)::int;

            IF position('/' in _token) > 0 THEN
                _step := split_part(_token, '/', 2)::int;
            ELSE
                _step := 1;
            END IF;

            IF _step <= 0 THEN
                RAISE EXCEPTION 'cb: invalid cron step value in token "%"', _token;
            END IF;
            IF _start > _end THEN
                RAISE EXCEPTION 'cb: invalid cron range in token "%" (start > end)', _token;
            END IF;

            -- Keep day-of-week handling explicit: 7 is only accepted as a single value.
            IF allow_seven_as_sunday AND (_start = 7 OR _end = 7) THEN
                RAISE EXCEPTION 'cb: use day-of-week 0 or single value 7 for Sunday in token "%"', _token;
            END IF;

            IF _start < min_value OR _end > max_value THEN
                RAISE EXCEPTION 'cb: cron range %-% out of bounds [% - %] in token "%"', _start, _end, min_value, max_value, _token;
            END IF;

            SELECT array_agg(i ORDER BY i)
            INTO _vals
            FROM generate_series(_start, _end, _step) AS g(i);
            _result := _result || coalesce(_vals, ARRAY[]::int[]);
        ELSE
            RAISE EXCEPTION 'cb: unsupported cron token "%"', _token;
        END IF;
    END LOOP;

    IF cardinality(_result) = 0 THEN
        RAISE EXCEPTION 'cb: cron field "%" produced no values', _field;
    END IF;

    SELECT array_agg(v ORDER BY v)
    INTO _result
    FROM (
        SELECT DISTINCT unnest(_result) AS v
    ) x;

    RETURN _result;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_cron_matches(cron_spec text, ts timestamptz)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _expanded text;
    _parts text[];
    _minute_values int[];
    _hour_values int[];
    _dom_values int[];
    _month_values int[];
    _dow_values int[];
    _ts_utc timestamp;
    _minute int;
    _hour int;
    _dom int;
    _month int;
    _dow int;
    _dom_field text;
    _dow_field text;
    _dom_is_star boolean;
    _dow_is_star boolean;
    _day_matches boolean;
BEGIN
    _expanded := _cb_cron_expand_spec(cron_spec);
    _parts := regexp_split_to_array(_expanded, ' ');

    IF array_length(_parts, 1) <> 5 THEN
        RAISE EXCEPTION 'cb: cron spec must contain 5 fields, got "%"', _expanded;
    END IF;

    _minute_values := _cb_cron_parse_field(_parts[1], 0, 59, false);
    _hour_values := _cb_cron_parse_field(_parts[2], 0, 23, false);
    _dom_values := _cb_cron_parse_field(_parts[3], 1, 31, false);
    _month_values := _cb_cron_parse_field(_parts[4], 1, 12, false);
    _dow_values := _cb_cron_parse_field(_parts[5], 0, 6, true);

    _ts_utc := ts AT TIME ZONE 'UTC';

    _minute := extract(minute FROM _ts_utc)::int;
    _hour := extract(hour FROM _ts_utc)::int;
    _dom := extract(day FROM _ts_utc)::int;
    _month := extract(month FROM _ts_utc)::int;
    _dow := extract(dow FROM _ts_utc)::int;

    _dom_field := _parts[3];
    _dow_field := _parts[5];
    _dom_is_star := (_dom_field = '*');
    _dow_is_star := (_dow_field = '*');

    IF _dom_is_star AND _dow_is_star THEN
        _day_matches := true;
    ELSIF _dom_is_star THEN
        _day_matches := (_dow = ANY(_dow_values));
    ELSIF _dow_is_star THEN
        _day_matches := (_dom = ANY(_dom_values));
    ELSE
        -- Standard cron semantics: when both DOM and DOW are restricted,
        -- the date matches if either field matches.
        _day_matches := (_dom = ANY(_dom_values)) OR (_dow = ANY(_dow_values));
    END IF;

    RETURN
        _minute = ANY(_minute_values)
        AND _hour = ANY(_hour_values)
        AND _month = ANY(_month_values)
        AND _day_matches;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_next_cron_tick(cron_spec text, from_time timestamptz)
RETURNS timestamptz
LANGUAGE plpgsql AS $$
DECLARE
    _candidate timestamp;
    _candidate_tz timestamptz;
    _limit int := 5 * 366 * 24 * 60; -- search up to 5 years minute-by-minute
    _i int;
BEGIN
    _candidate := date_trunc('minute', from_time AT TIME ZONE 'UTC');

    IF _candidate <= (from_time AT TIME ZONE 'UTC') THEN
        _candidate := _candidate + interval '1 minute';
    END IF;

    FOR _i IN 1.._limit LOOP
        _candidate_tz := _candidate AT TIME ZONE 'UTC';

        IF _cb_cron_matches(cron_spec, _candidate_tz) THEN
            RETURN _candidate_tz;
        END IF;

        _candidate := _candidate + interval '1 minute';
    END LOOP;

    RAISE EXCEPTION 'cb: no cron tick found within % minutes for spec "%"', _limit, cron_spec;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_task_schedule(task_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb, catch_up text DEFAULT 'one')
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_task_schedules:' || cb_create_task_schedule.task_name));

    -- Return early if schedule already exists
    IF EXISTS (
        SELECT 1
        FROM cb_task_schedules s
        WHERE s.task_name = cb_create_task_schedule.task_name
    ) THEN
        RETURN;
    END IF;

    INSERT INTO cb_task_schedules (
        task_name,
        cron_spec,
        next_run_at,
        input,
        catch_up,
        enabled,
        updated_at
    )
    VALUES (
        cb_create_task_schedule.task_name,
        cb_create_task_schedule.cron_spec,
        cb_next_cron_tick(cb_create_task_schedule.cron_spec, now()),
        cb_create_task_schedule.input,
        cb_create_task_schedule.catch_up,
        true,
        now()
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_flow_schedule(flow_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb, catch_up text DEFAULT 'one')
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_flow_schedules:' || cb_create_flow_schedule.flow_name));

    -- Return early if schedule already exists
    IF EXISTS (
        SELECT 1
        FROM cb_flow_schedules s
        WHERE s.flow_name = cb_create_flow_schedule.flow_name
    ) THEN
        RETURN;
    END IF;

    INSERT INTO cb_flow_schedules (
        flow_name,
        cron_spec,
        next_run_at,
        input,
        catch_up,
        enabled,
        updated_at
    )
    VALUES (
        cb_create_flow_schedule.flow_name,
        cb_create_flow_schedule.cron_spec,
        cb_next_cron_tick(cb_create_flow_schedule.cron_spec, now()),
        cb_create_flow_schedule.input,
        cb_create_flow_schedule.catch_up,
        true,
        now()
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_advance_task_schedule(id bigint, catch_up text DEFAULT 'one')
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF cb_advance_task_schedule.catch_up = 'all' THEN
        -- Advance one tick at a time; stays in the past until caught up
        UPDATE cb_task_schedules s
        SET
            next_run_at = cb_next_cron_tick(s.cron_spec, s.next_run_at),
            last_run_at = s.next_run_at,
            last_enqueued_at = now(),
            updated_at = now()
        WHERE s.id = cb_advance_task_schedule.id;
    ELSE
        -- skip / one: jump to the future
        UPDATE cb_task_schedules s
        SET
            next_run_at = cb_next_cron_tick(s.cron_spec, GREATEST(s.next_run_at, now())),
            last_run_at = s.next_run_at,
            last_enqueued_at = now(),
            updated_at = now()
        WHERE s.id = cb_advance_task_schedule.id;
    END IF;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_advance_flow_schedule(id bigint, catch_up text DEFAULT 'one')
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF cb_advance_flow_schedule.catch_up = 'all' THEN
        -- Advance one tick at a time; stays in the past until caught up
        UPDATE cb_flow_schedules s
        SET
            next_run_at = cb_next_cron_tick(s.cron_spec, s.next_run_at),
            last_run_at = s.next_run_at,
            last_enqueued_at = now(),
            updated_at = now()
        WHERE s.id = cb_advance_flow_schedule.id;
    ELSE
        -- skip / one: jump to the future
        UPDATE cb_flow_schedules s
        SET
            next_run_at = cb_next_cron_tick(s.cron_spec, GREATEST(s.next_run_at, now())),
            last_run_at = s.next_run_at,
            last_enqueued_at = now(),
            updated_at = now()
        WHERE s.id = cb_advance_flow_schedule.id;
    END IF;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_execute_due_task_schedules(task_names text[], batch_size int DEFAULT 32)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_executed int := 0;
    v_id bigint;
    v_task_name text;
    v_input jsonb;
    v_scheduled_at timestamptz;
    v_key text;
    v_policy text;
BEGIN
    FOR i IN 1..cb_execute_due_task_schedules.batch_size LOOP
        -- Claim one due schedule with FOR UPDATE SKIP LOCKED
        SELECT s.id, s.task_name, s.input, s.next_run_at, s.catch_up
        INTO v_id, v_task_name, v_input, v_scheduled_at, v_policy
        FROM cb_task_schedules s
        WHERE
            s.enabled = true
            AND s.next_run_at <= now()
            AND s.task_name = ANY(cb_execute_due_task_schedules.task_names)
        ORDER BY s.next_run_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        -- No more due schedules in this batch
        EXIT WHEN v_id IS NULL;

        IF v_policy = 'skip' THEN
            -- Skip: advance without enqueuing
            PERFORM cb_advance_task_schedule(v_id, v_policy);
        ELSE
            -- one / all: enqueue + advance (difference is in advance function)
            v_key := 'schedule:' || EXTRACT(EPOCH FROM v_scheduled_at)::text;
            v_input := COALESCE(v_input, '{}'::jsonb);

            PERFORM cb_run_task(v_task_name, v_input, v_key);
            PERFORM cb_advance_task_schedule(v_id, v_policy);

            v_executed := v_executed + 1;
        END IF;
    END LOOP;

    RETURN v_executed;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_execute_due_flow_schedules(flow_names text[], batch_size int DEFAULT 32)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_executed int := 0;
    v_id bigint;
    v_flow_name text;
    v_input jsonb;
    v_scheduled_at timestamptz;
    v_key text;
    v_policy text;
BEGIN
    FOR i IN 1..cb_execute_due_flow_schedules.batch_size LOOP
        -- Claim one due schedule with FOR UPDATE SKIP LOCKED
        SELECT s.id, s.flow_name, s.input, s.next_run_at, s.catch_up
        INTO v_id, v_flow_name, v_input, v_scheduled_at, v_policy
        FROM cb_flow_schedules s
        WHERE
            s.enabled = true
            AND s.next_run_at <= now()
            AND s.flow_name = ANY(cb_execute_due_flow_schedules.flow_names)
        ORDER BY s.next_run_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        -- No more due schedules in this batch
        EXIT WHEN v_id IS NULL;

        IF v_policy = 'skip' THEN
            -- Skip: advance without enqueuing
            PERFORM cb_advance_flow_schedule(v_id, v_policy);
        ELSE
            -- one / all: enqueue + advance (difference is in advance function)
            v_key := 'schedule:' || EXTRACT(EPOCH FROM v_scheduled_at)::text;
            v_input := COALESCE(v_input, '{}'::jsonb);

            PERFORM cb_run_flow(v_flow_name, v_input, v_key);
            PERFORM cb_advance_flow_schedule(v_id, v_policy);

            v_executed := v_executed + 1;
        END IF;
    END LOOP;

    RETURN v_executed;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_clear_task_runs: Delete all task runs regardless of status.
-- Use with caution — in-flight work will be lost.
-- Parameters:
--   name: Task name
-- Returns: int - deleted row count
CREATE OR REPLACE FUNCTION cb_clear_task_runs(name text)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _table text := _cb_table_name(cb_clear_task_runs.name, 't');
    _deleted int := 0;
BEGIN
    IF to_regclass(_table) IS NULL THEN
        RETURN 0;
    END IF;
    EXECUTE format('DELETE FROM %I', _table);
    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_clear_flow_runs: Delete all flow runs regardless of status.
-- Step runs and map tasks are deleted automatically via ON DELETE CASCADE.
-- Use with caution — in-flight work will be lost.
-- Parameters:
--   name: Flow name
-- Returns: int - deleted row count
CREATE OR REPLACE FUNCTION cb_clear_flow_runs(name text)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _table text := _cb_table_name(cb_clear_flow_runs.name, 'f');
    _deleted int := 0;
BEGIN
    IF to_regclass(_table) IS NULL THEN
        RETURN 0;
    END IF;
    EXECUTE format('DELETE FROM %I', _table);
    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_notify: Send a notification via pg NOTIFY on the schema-qualified cb_wire channel.
-- Parameters:
--   topic: Notification topic (also used as SSE event name)
--   message: Optional message payload (text)
--   sent_by: Optional sender ID (NULL from Client, set from Wire/Listener to skip self-delivery)
CREATE OR REPLACE FUNCTION cb_notify(
    topic text,
    message text DEFAULT NULL,
    sent_by text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF cb_notify.topic LIKE 'catbird.%' THEN
        RAISE EXCEPTION 'cb: topic "catbird.*" is reserved for internal use';
    END IF;

    PERFORM pg_notify(
        current_schema || '.cb_wire',
        json_build_object(
            'sent_by', cb_notify.sent_by,
            'topic', cb_notify.topic,
            'message', cb_notify.message
        )::text
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- _cb_notify: Internal function for emitting catbird system events.
-- Same as cb_notify but bypasses the reserved topic check.
CREATE OR REPLACE FUNCTION _cb_notify(
    topic text,
    message text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(
        current_schema || '.cb_wire',
        json_build_object(
            'topic', _cb_notify.topic,
            'message', _cb_notify.message
        )::text
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_bind_task: Subscribe a task to a topic pattern
-- When a message is published to a matching topic, a task run is created
CREATE OR REPLACE FUNCTION cb_bind_task(name text, pattern text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    p_type text;
    p_prefix text;
    p_regex text;
    base_pattern text;
    test_result boolean;
    token text;
    tokens text[];
    token_count integer;
    i integer;
BEGIN
    -- Validate task exists
    IF NOT EXISTS (SELECT 1 FROM cb_tasks t WHERE t.name = cb_bind_task.name) THEN
        RAISE EXCEPTION 'catbird: task "%" is not defined', cb_bind_task.name;
    END IF;

    -- Validate pattern contains only allowed characters
    IF cb_bind_task.pattern !~ '^[a-zA-Z0-9._#*-]+$' THEN
        RAISE EXCEPTION 'cb: pattern can only contain: a-z, A-Z, 0-9, ., _, -, *, #';
    END IF;

    IF cb_bind_task.pattern ~ '\.\.' OR cb_bind_task.pattern ~ '(^\.|\.$)' THEN
        RAISE EXCEPTION 'cb: pattern cannot contain double dots (..), or start/end with a dot';
    END IF;

    tokens := string_to_array(cb_bind_task.pattern, '.');
    token_count := array_length(tokens, 1);

    FOR i IN 1..token_count LOOP
        token := tokens[i];
        IF token = '*' THEN CONTINUE; END IF;
        IF token = '#' THEN
            IF i <> token_count THEN
                RAISE EXCEPTION 'cb: # wildcard must be the final token';
            END IF;
            CONTINUE;
        END IF;
        IF token LIKE '%*%' THEN
            RAISE EXCEPTION 'cb: * wildcard must occupy an entire token';
        END IF;
        IF token LIKE '%#%' THEN
            RAISE EXCEPTION 'cb: # wildcard must occupy an entire token';
        END IF;
    END LOOP;

    IF cb_bind_task.pattern !~ '[*#]' THEN
        p_type := 'exact';
        p_prefix := NULL;
        p_regex := NULL;
    ELSE
        p_type := 'wildcard';
        p_prefix := substring(cb_bind_task.pattern FROM '^([^*#]+)');
        IF p_prefix IS NULL OR p_prefix = '' THEN
            p_prefix := '';
        END IF;
        IF cb_bind_task.pattern ~ E'\\.\\#$' AND p_prefix <> '' AND right(p_prefix, 1) = '.' THEN
            p_prefix := left(p_prefix, length(p_prefix) - 1);
        END IF;

        base_pattern := cb_bind_task.pattern;
        IF cb_bind_task.pattern ~ E'\\.\\#$' THEN
            base_pattern := regexp_replace(base_pattern, E'\\.\\#$', '');
        END IF;
        p_regex := regexp_replace(base_pattern, E'\\.', E'\\\\.', 'g');
        p_regex := regexp_replace(p_regex, E'\\*', '[a-zA-Z0-9_-]+', 'g');
        IF cb_bind_task.pattern = '#' THEN
            p_regex := E'^[a-zA-Z0-9_-]+(?:\\.[a-zA-Z0-9_-]+)*$';
        ELSIF cb_bind_task.pattern ~ E'\\.\\#$' THEN
            p_regex := '^' || p_regex || E'(?:\\.[a-zA-Z0-9_-]+)*$';
        ELSE
            p_regex := '^' || p_regex || '$';
        END IF;
        BEGIN
            test_result := 'test.topic' ~ p_regex;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'cb: invalid pattern "%" generated invalid regex: %', cb_bind_task.pattern, SQLERRM;
        END;
    END IF;

    INSERT INTO cb_bindings(target_name, target_type, pattern, pattern_type, prefix, regex)
    VALUES (cb_bind_task.name, 'task', cb_bind_task.pattern, p_type, p_prefix, p_regex)
    ON CONFLICT ON CONSTRAINT cb_bindings_pkey DO NOTHING;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_unbind_task: Unsubscribe a task from a topic pattern
CREATE OR REPLACE FUNCTION cb_unbind_task(name text, pattern text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _deleted boolean := false;
BEGIN
    DELETE FROM cb_bindings
    WHERE cb_bindings.target_name = cb_unbind_task.name
        AND cb_bindings.target_type = 'task'
        AND cb_bindings.pattern = cb_unbind_task.pattern
    RETURNING true INTO _deleted;

    RETURN coalesce(_deleted, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_bind_flow: Subscribe a flow to a topic pattern
-- When a message is published to a matching topic, a flow run is created
CREATE OR REPLACE FUNCTION cb_bind_flow(name text, pattern text)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    p_type text;
    p_prefix text;
    p_regex text;
    base_pattern text;
    test_result boolean;
    token text;
    tokens text[];
    token_count integer;
    i integer;
BEGIN
    -- Validate flow exists
    IF NOT EXISTS (SELECT 1 FROM cb_flows f WHERE f.name = cb_bind_flow.name) THEN
        RAISE EXCEPTION 'catbird: flow "%" is not defined', cb_bind_flow.name;
    END IF;

    -- Validate pattern contains only allowed characters
    IF cb_bind_flow.pattern !~ '^[a-zA-Z0-9._#*-]+$' THEN
        RAISE EXCEPTION 'cb: pattern can only contain: a-z, A-Z, 0-9, ., _, -, *, #';
    END IF;

    IF cb_bind_flow.pattern ~ '\.\.' OR cb_bind_flow.pattern ~ '(^\.|\.$)' THEN
        RAISE EXCEPTION 'cb: pattern cannot contain double dots (..), or start/end with a dot';
    END IF;

    tokens := string_to_array(cb_bind_flow.pattern, '.');
    token_count := array_length(tokens, 1);

    FOR i IN 1..token_count LOOP
        token := tokens[i];
        IF token = '*' THEN CONTINUE; END IF;
        IF token = '#' THEN
            IF i <> token_count THEN
                RAISE EXCEPTION 'cb: # wildcard must be the final token';
            END IF;
            CONTINUE;
        END IF;
        IF token LIKE '%*%' THEN
            RAISE EXCEPTION 'cb: * wildcard must occupy an entire token';
        END IF;
        IF token LIKE '%#%' THEN
            RAISE EXCEPTION 'cb: # wildcard must occupy an entire token';
        END IF;
    END LOOP;

    IF cb_bind_flow.pattern !~ '[*#]' THEN
        p_type := 'exact';
        p_prefix := NULL;
        p_regex := NULL;
    ELSE
        p_type := 'wildcard';
        p_prefix := substring(cb_bind_flow.pattern FROM '^([^*#]+)');
        IF p_prefix IS NULL OR p_prefix = '' THEN
            p_prefix := '';
        END IF;
        IF cb_bind_flow.pattern ~ E'\\.\\#$' AND p_prefix <> '' AND right(p_prefix, 1) = '.' THEN
            p_prefix := left(p_prefix, length(p_prefix) - 1);
        END IF;

        base_pattern := cb_bind_flow.pattern;
        IF cb_bind_flow.pattern ~ E'\\.\\#$' THEN
            base_pattern := regexp_replace(base_pattern, E'\\.\\#$', '');
        END IF;
        p_regex := regexp_replace(base_pattern, E'\\.', E'\\\\.', 'g');
        p_regex := regexp_replace(p_regex, E'\\*', '[a-zA-Z0-9_-]+', 'g');
        IF cb_bind_flow.pattern = '#' THEN
            p_regex := E'^[a-zA-Z0-9_-]+(?:\\.[a-zA-Z0-9_-]+)*$';
        ELSIF cb_bind_flow.pattern ~ E'\\.\\#$' THEN
            p_regex := '^' || p_regex || E'(?:\\.[a-zA-Z0-9_-]+)*$';
        ELSE
            p_regex := '^' || p_regex || '$';
        END IF;
        BEGIN
            test_result := 'test.topic' ~ p_regex;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'cb: invalid pattern "%" generated invalid regex: %', cb_bind_flow.pattern, SQLERRM;
        END;
    END IF;

    INSERT INTO cb_bindings(target_name, target_type, pattern, pattern_type, prefix, regex)
    VALUES (cb_bind_flow.name, 'flow', cb_bind_flow.pattern, p_type, p_prefix, p_regex)
    ON CONFLICT ON CONSTRAINT cb_bindings_pkey DO NOTHING;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_unbind_flow: Unsubscribe a flow from a topic pattern
CREATE OR REPLACE FUNCTION cb_unbind_flow(name text, pattern text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _deleted boolean := false;
BEGIN
    DELETE FROM cb_bindings
    WHERE cb_bindings.target_name = cb_unbind_flow.name
        AND cb_bindings.target_type = 'flow'
        AND cb_bindings.pattern = cb_unbind_flow.pattern
    RETURNING true INTO _deleted;

    RETURN coalesce(_deleted, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_publish (single): Send a message to all queues/tasks/flows subscribed to a topic
CREATE OR REPLACE FUNCTION cb_publish(
    topic text,
    body jsonb,
    concurrency_key text DEFAULT NULL,
    headers jsonb DEFAULT NULL,
    visible_at timestamptz DEFAULT NULL,
    priority int DEFAULT 0,
    expires_at timestamptz DEFAULT NULL
)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz = coalesce(cb_publish.visible_at, now());
    _rec record;
    _q_table text;
    _matched int := 0;
BEGIN
    IF cb_publish.headers IS NOT NULL AND jsonb_typeof(cb_publish.headers) <> 'object' THEN
        RAISE EXCEPTION 'cb: headers must be a JSON object';
    END IF;

    FOR _rec IN
        SELECT DISTINCT target_name, target_type
        FROM cb_bindings
        WHERE (
            (pattern_type = 'exact' AND pattern = cb_publish.topic)
            OR
            (pattern_type = 'wildcard'
             AND (prefix = '' OR cb_publish.topic LIKE prefix || '%')
             AND cb_publish.topic ~ regex)
        )
    LOOP
        IF _rec.target_type = 'queue' THEN
            _q_table := _cb_table_name(_rec.target_name, 'q');
            BEGIN
                EXECUTE format(
                    $QUERY$
                        INSERT INTO %I (topic, body, concurrency_key, headers, visible_at, priority, expires_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (concurrency_key) DO NOTHING;
                    $QUERY$,
                    _q_table
                )
                USING cb_publish.topic,
                    cb_publish.body,
                    cb_publish.concurrency_key,
                    cb_publish.headers,
                    _visible_at,
                    cb_publish.priority,
                    cb_publish.expires_at;

                _matched := _matched + 1;
            EXCEPTION WHEN undefined_table THEN
                CONTINUE;
            END;
        ELSIF _rec.target_type = 'task' THEN
            BEGIN
                PERFORM cb_run_task(
                    _rec.target_name,
                    cb_publish.body,
                    cb_publish.concurrency_key,
                    cb_publish.headers,
                    cb_publish.visible_at,
                    cb_publish.priority,
                    cb_publish.expires_at
                );
                _matched := _matched + 1;
            EXCEPTION WHEN undefined_table THEN
                CONTINUE;
            END;
        ELSIF _rec.target_type = 'flow' THEN
            BEGIN
                PERFORM cb_run_flow(
                    _rec.target_name,
                    cb_publish.body,
                    cb_publish.concurrency_key,
                    cb_publish.headers,
                    cb_publish.visible_at,
                    cb_publish.priority,
                    cb_publish.expires_at
                );
                _matched := _matched + 1;
            EXCEPTION WHEN undefined_table THEN
                CONTINUE;
            END;
        END IF;
    END LOOP;

    RETURN _matched;
END
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_publish (bulk): Send multiple messages to all queues/tasks/flows subscribed to a topic
CREATE OR REPLACE FUNCTION cb_publish(
    topic text,
    bodies jsonb[],
    concurrency_keys text[] DEFAULT NULL,
    headers jsonb[] DEFAULT NULL,
    visible_at timestamptz DEFAULT NULL,
    priority int DEFAULT 0,
    expires_at timestamptz DEFAULT NULL
)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz = coalesce(cb_publish.visible_at, now());
    _rec record;
    _q_table text;
    _matched int := 0;
    _i int;
    _body jsonb;
    _concurrency_key text;
    _header jsonb;
BEGIN
    IF cb_publish.concurrency_keys IS NOT NULL AND cardinality(cb_publish.concurrency_keys) <> cardinality(cb_publish.bodies) THEN
        RAISE EXCEPTION 'cb: concurrency_keys length must match bodies length';
    END IF;

    IF cb_publish.headers IS NOT NULL AND cardinality(cb_publish.headers) <> cardinality(cb_publish.bodies) THEN
        RAISE EXCEPTION 'cb: headers length must match bodies length';
    END IF;

    IF cb_publish.headers IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM unnest(cb_publish.headers) AS h(header)
            WHERE h.header IS NOT NULL AND jsonb_typeof(h.header) <> 'object'
        ) THEN
            RAISE EXCEPTION 'cb: headers must be JSON objects';
        END IF;
    END IF;

    FOR _rec IN
        SELECT DISTINCT target_name, target_type
        FROM cb_bindings
        WHERE (
            (pattern_type = 'exact' AND pattern = cb_publish.topic)
            OR
            (pattern_type = 'wildcard'
             AND (prefix = '' OR cb_publish.topic LIKE prefix || '%')
             AND cb_publish.topic ~ regex)
        )
    LOOP
        IF _rec.target_type = 'queue' THEN
            _q_table := _cb_table_name(_rec.target_name, 'q');
            BEGIN
                EXECUTE format(
                    $QUERY$
                    INSERT INTO %I (topic, body, concurrency_key, headers, visible_at, priority, expires_at)
                    SELECT
                        $1,
                        body,
                        CASE
                            WHEN $4 IS NULL THEN NULL
                            ELSE $4[ordinality]
                        END,
                        CASE
                            WHEN $5 IS NULL THEN NULL
                            ELSE $5[ordinality]
                        END,
                        $2,
                        $6,
                        $7
                    FROM unnest($3) WITH ORDINALITY AS t(body, ordinality)
                    ON CONFLICT (concurrency_key) DO NOTHING;
                    $QUERY$,
                    _q_table
                )
                USING cb_publish.topic,
                      _visible_at,
                      cb_publish.bodies,
                      cb_publish.concurrency_keys,
                      cb_publish.headers,
                      cb_publish.priority,
                      cb_publish.expires_at;

                _matched := _matched + 1;
            EXCEPTION WHEN undefined_table THEN
                CONTINUE;
            END;
        ELSIF _rec.target_type IN ('task', 'flow') THEN
            BEGIN
                FOR _i IN 1..cardinality(cb_publish.bodies) LOOP
                    _body := cb_publish.bodies[_i];
                    _concurrency_key := NULL;
                    _header := NULL;

                    IF cb_publish.concurrency_keys IS NOT NULL THEN
                        _concurrency_key := cb_publish.concurrency_keys[_i];
                    END IF;
                    IF cb_publish.headers IS NOT NULL THEN
                        _header := cb_publish.headers[_i];
                    END IF;

                    IF _rec.target_type = 'task' THEN
                        PERFORM cb_run_task(
                            _rec.target_name,
                            _body,
                            _concurrency_key,
                            _header,
                            cb_publish.visible_at,
                            cb_publish.priority,
                            cb_publish.expires_at
                        );
                    ELSE
                        PERFORM cb_run_flow(
                            _rec.target_name,
                            _body,
                            _concurrency_key,
                            _header,
                            cb_publish.visible_at,
                            cb_publish.priority,
                            cb_publish.expires_at
                        );
                    END IF;
                END LOOP;
                _matched := _matched + 1;
            EXCEPTION WHEN undefined_table THEN
                CONTINUE;
            END;
        END IF;
    END LOOP;

    RETURN _matched;
END
$$;
-- +goose statementend

-- +goose down

-- +goose statementbegin
DO $$
BEGIN
    IF to_regclass('cb_flows') IS NOT NULL THEN
        PERFORM cb_delete_flow(name) FROM cb_flows;
    END IF;
    IF to_regclass('cb_tasks') IS NOT NULL THEN
        PERFORM cb_delete_task(name) FROM cb_tasks;
    END IF;
    IF to_regclass('cb_queues') IS NOT NULL THEN
        PERFORM cb_delete_queue(name) FROM cb_queues;
    END IF;
END$$;
-- +goose statementend

DROP FUNCTION IF EXISTS cb_unbind_flow(text, text);
DROP FUNCTION IF EXISTS cb_bind_flow(text, text);
DROP FUNCTION IF EXISTS cb_unbind_task(text, text);
DROP FUNCTION IF EXISTS cb_bind_task(text, text);
DROP FUNCTION IF EXISTS cb_publish(text, jsonb, text, jsonb, timestamptz, int, timestamptz);
DROP FUNCTION IF EXISTS cb_publish(text, jsonb[], text[], jsonb[], timestamptz, int, timestamptz);
DROP FUNCTION IF EXISTS cb_execute_due_flow_schedules(text[], int);
DROP FUNCTION IF EXISTS cb_execute_due_task_schedules(text[], int);
DROP FUNCTION IF EXISTS cb_advance_flow_schedule(bigint, text);
DROP FUNCTION IF EXISTS cb_advance_task_schedule(bigint, text);
DROP FUNCTION IF EXISTS cb_create_flow_schedule(text, text, jsonb, text);
DROP FUNCTION IF EXISTS cb_create_task_schedule(text, text, jsonb, text);
DROP FUNCTION IF EXISTS cb_next_cron_tick(text, timestamptz);
DROP FUNCTION IF EXISTS _cb_cron_matches(text, timestamptz);
DROP FUNCTION IF EXISTS _cb_cron_parse_field(text, int, int, boolean);
DROP FUNCTION IF EXISTS _cb_cron_expand_spec(text);
DROP VIEW IF EXISTS cb_worker_info;
DROP FUNCTION IF EXISTS cb_worker_heartbeat(uuid);
DROP FUNCTION IF EXISTS cb_worker_started(uuid, jsonb, jsonb);
DROP FUNCTION IF EXISTS cb_gc();
DROP FUNCTION IF EXISTS cb_purge_flow_runs(text, interval);
DROP FUNCTION IF EXISTS cb_purge_task_runs(text, interval);
DROP FUNCTION IF EXISTS cb_notify(text, text, text);
DROP FUNCTION IF EXISTS cb_clear_flow_runs(text);
DROP FUNCTION IF EXISTS cb_clear_task_runs(text);
DROP FUNCTION IF EXISTS cb_delete_flow(text);
DROP FUNCTION IF EXISTS cb_cancel_map_task_run(text, bigint);
DROP FUNCTION IF EXISTS cb_cancel_step_run(text, bigint);
DROP FUNCTION IF EXISTS cb_finalize_flow_cancellation(text, bigint);
DROP FUNCTION IF EXISTS _cb_maybe_finalize_flow_cancellation(text, bigint);
DROP FUNCTION IF EXISTS cb_cancel_flow(text, bigint, text);
DROP FUNCTION IF EXISTS cb_fail_flow_on_fail(text, bigint, text, boolean, bigint);
DROP FUNCTION IF EXISTS cb_complete_flow_on_fail(text, bigint);
DROP FUNCTION IF EXISTS cb_get_flow_step_output(text, bigint, text);
DROP FUNCTION IF EXISTS cb_claim_flow_on_fail(text, int);
DROP FUNCTION IF EXISTS cb_complete_flow_early(text, bigint, text, jsonb, text);
DROP FUNCTION IF EXISTS cb_wait_flow_step_output(text, bigint, text, int, int);
DROP FUNCTION IF EXISTS cb_get_flow_step_status(text, bigint, text);
DROP FUNCTION IF EXISTS cb_wait_flow_output(text, bigint, int, int);
DROP FUNCTION IF EXISTS cb_signal_flow(text, bigint, text, jsonb);
DROP FUNCTION IF EXISTS cb_fail_step(text, text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_step(text, text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_fail_map_task(text, text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_map_tasks(text, text, bigint[], jsonb[]);
DROP FUNCTION IF EXISTS cb_fail_generator_step(text, text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_generator_step(text, text, bigint);
DROP FUNCTION IF EXISTS cb_spawn_generator_map_tasks(text, text, bigint, jsonb, timestamptz);
DROP FUNCTION IF EXISTS cb_hide_map_tasks(text, text, bigint[], int);
DROP FUNCTION IF EXISTS cb_claim_map_tasks(text, text, int, int);
DROP FUNCTION IF EXISTS cb_hide_steps(text, text, bigint[], int);
DROP FUNCTION IF EXISTS cb_claim_steps(text, text, int, int);
DROP FUNCTION IF EXISTS cb_start_steps(text, bigint, timestamptz);
DROP FUNCTION IF EXISTS cb_run_flow(text, jsonb, text, jsonb, timestamptz, int, timestamptz);
DROP FUNCTION IF EXISTS cb_create_flow(text, text, jsonb, text[], interval);
DROP FUNCTION IF EXISTS cb_delete_task(text);
DROP FUNCTION IF EXISTS cb_cancel_task(text, bigint, text);
DROP FUNCTION IF EXISTS cb_fail_task_on_fail(text, bigint, text, boolean, bigint);
DROP FUNCTION IF EXISTS cb_complete_task_on_fail(text, bigint);
DROP FUNCTION IF EXISTS cb_claim_task_on_fail(text, int);
DROP FUNCTION IF EXISTS cb_fail_task(text, bigint, text);
DROP FUNCTION IF EXISTS cb_complete_task(text, bigint, jsonb);
DROP FUNCTION IF EXISTS cb_wait_task_output(text, bigint, int, int);
DROP FUNCTION IF EXISTS cb_hide_tasks(text, bigint[], int);
DROP FUNCTION IF EXISTS cb_claim_tasks(text, int, int);
DROP FUNCTION IF EXISTS cb_run_task(text, jsonb, text, jsonb, timestamptz, int, timestamptz);
DROP FUNCTION IF EXISTS cb_create_task(text, text, text, interval);
DROP FUNCTION IF EXISTS __cb_evaluate_condition_expr(text, jsonb);
DROP FUNCTION IF EXISTS _cb_get_jsonb_field(jsonb, text);
DROP FUNCTION IF EXISTS _cb_evaluate_condition(jsonb, jsonb);
DROP FUNCTION IF EXISTS __cb_parse_condition_value(text, text);
DROP FUNCTION IF EXISTS _cb_parse_condition(text);
DROP FUNCTION IF EXISTS cb_unbind(text, text);
DROP FUNCTION IF EXISTS cb_bind(text, text);
DROP FUNCTION IF EXISTS cb_create_queue(text, timestamptz, text);
DROP FUNCTION IF EXISTS cb_delete_queue(text);
DROP FUNCTION IF EXISTS cb_send(text, jsonb, text, text, jsonb, timestamptz, int, timestamptz);
DROP FUNCTION IF EXISTS cb_send(text, jsonb[], text, text[], jsonb[], timestamptz, int, timestamptz);
DROP FUNCTION IF EXISTS cb_read(text, int, int);
DROP FUNCTION IF EXISTS cb_read_poll(text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_hide(text, bigint, int);
DROP FUNCTION IF EXISTS cb_hide(text, bigint[], int);
DROP FUNCTION IF EXISTS cb_delete(text, bigint);
DROP FUNCTION IF EXISTS cb_delete(text, bigint[]);
DROP VIEW IF EXISTS cb_flow_info;
DROP TABLE IF EXISTS cb_wire_presence CASCADE;
DROP TABLE IF EXISTS cb_wire_nodes CASCADE;
DROP INDEX IF EXISTS cb_flow_schedules_next_run_at_enabled_idx;
DROP TABLE IF EXISTS cb_flow_schedules CASCADE;
DROP INDEX IF EXISTS cb_task_schedules_next_run_at_enabled_idx;
DROP TABLE IF EXISTS cb_task_schedules CASCADE;
DROP TABLE IF EXISTS cb_step_handlers CASCADE;
DROP TABLE IF EXISTS cb_task_handlers CASCADE;
DROP TABLE IF EXISTS cb_workers CASCADE;
DROP TABLE IF EXISTS cb_step_dependencies CASCADE;
DROP TABLE IF EXISTS cb_steps CASCADE;
DROP TABLE IF EXISTS cb_flows CASCADE;
DROP TABLE IF EXISTS cb_tasks CASCADE;
DROP TABLE IF EXISTS cb_bindings CASCADE;
DROP TABLE IF EXISTS cb_queues CASCADE;
DROP FUNCTION IF EXISTS _cb_table_name(text, text);
DROP TYPE IF EXISTS cb_step_claim CASCADE;
DROP TYPE IF EXISTS cb_task_claim CASCADE;
DROP TYPE IF EXISTS cb_message CASCADE;
