-- SQL code is taken from or inspired by pgmq (https://github.com/pgmq/pgmq)

-- CONCURRENCY AUDIT:
-- Setup functions (initialization, not hot path):
--   cb_create_queue():Uses pg_advisory_xact_lock, safe for concurrent creation (first-wins)
--   cb_delete_queue(): Uses pg_advisory_xact_lock, safe for concurrent deletion
--   cb_bind(): Uses ON CONFLICT DO NOTHING, idempotent and safe
-- Hot path functions (millions of executions, no advisory locks):
--   cb_send(): Uses atomic CTE + UNION ALL pattern with ON CONFLICT DO UPDATE WHERE FALSE - SAFE
--     Critical: MUST use UNION ALL fallback, not bare RETURNING (returns NULL on conflict without it)
--   cb_read(): Uses FOR UPDATE SKIP LOCKED for row-level concurrency - SAFE
--   cb_read_poll(): Uses FOR UPDATE SKIP LOCKED in polling loop - SAFE
--   cb_publish(): Iterates bindings, uses ON CONFLICT per queue - SAFE
--   cb_hide(): Uses UPDATE with indexed visible_at, no locks - SAFE
--   cb_delete(): Direct DELETE operation, no locks - SAFE
-- All operations maintain message ordering and deduplication invariants
-- Deduplication pattern reference: https://stackoverflow.com/a/35953488

-- +goose up

DROP FUNCTION IF EXISTS cb_send(text, jsonb[], text, text[], jsonb, timestamptz);
DROP FUNCTION IF EXISTS cb_publish(text, jsonb[], text[], jsonb, timestamptz);

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
    expires_at timestamptz = null,
    description text = null
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := cb_table_name(cb_create_queue.name, 'q');
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_q_table));

    EXECUTE format(
        $QUERY$
        CREATE TABLE IF NOT EXISTS %I (
            id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            idempotency_key text,
            topic text,
            payload jsonb NOT NULL,
            headers jsonb,
            deliveries int NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT now(),
            visible_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT headers_is_object CHECK (headers IS NULL OR jsonb_typeof(headers) = 'object')
        )
        $QUERY$,
        _q_table
    );

    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (idempotency_key);', _q_table || '_idempotency_key_idx', _q_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (visible_at, id);', _q_table || '_poll_visible_id_idx', _q_table);
    EXECUTE format('DROP INDEX IF EXISTS %I;', _q_table || '_visible_at_idx');

    -- Insert queue metadata
    INSERT INTO cb_queues (name, description, expires_at)
    VALUES (
        cb_create_queue.name,
        cb_create_queue.description,
        cb_create_queue.expires_at
    )
    ON CONFLICT DO NOTHING;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_delete_queue: Delete a queue definition and all its messages
-- Removes the queue metadata and drops the associated message table
-- Parameters:
--   name: Queue name
-- Returns: boolean - true if queue was deleted, false if not found
CREATE OR REPLACE FUNCTION cb_delete_queue(name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := cb_table_name(cb_delete_queue.name, 'q');
    _res boolean;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_q_table));

    EXECUTE FORMAT('DROP TABLE IF EXISTS %I;', _q_table);

    DELETE FROM cb_queues q
    WHERE q.name = cb_delete_queue.name
    RETURNING true
    INTO _res;

    RETURN coalesce(_res, false);
end
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_publish: Send a message to all queues subscribed to a topic
-- Routes messages based on bindings with support for exact and wildcard patterns
-- Parameters:
--   topic: Topic string to route to subscribed queues
--   payload: JSON message payload
--   idempotency_key: Optional unique ID for idempotency (prevents duplicate messages)
--   visible_at: Optional timestamp when message should become visible (default: now)
-- Returns: void
CREATE OR REPLACE FUNCTION cb_publish(
    topic text,
    payload jsonb,
    idempotency_key text = null,
    headers jsonb = null,
    visible_at timestamptz = null
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz = coalesce(cb_publish.visible_at, now());
    _rec record;
    _q_table text;
BEGIN
    IF cb_publish.headers IS NOT NULL AND jsonb_typeof(cb_publish.headers) <> 'object' THEN
        RAISE EXCEPTION 'cb: headers must be a JSON object';
    END IF;

    -- Find all matching queues via bindings (exact + wildcard patterns)
    FOR _rec IN
        SELECT DISTINCT queue_name AS name
        FROM cb_bindings
        WHERE (
            -- Exact match (fastest path)
            (pattern_type = 'exact' AND pattern = cb_publish.topic)
            OR
            -- Wildcard match with prefix filter + regex
            (pattern_type = 'wildcard'
             AND (prefix = '' OR cb_publish.topic LIKE prefix || '%')
             AND cb_publish.topic ~ regex)
        )
    LOOP
        _q_table := cb_table_name(_rec.name, 'q');
        BEGIN
            EXECUTE format(
                $QUERY$
                                INSERT INTO %I (topic, payload, idempotency_key, headers, visible_at)
                                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (idempotency_key) DO NOTHING;
                $QUERY$,
                _q_table
            )
            USING cb_publish.topic,
                cb_publish.payload,
                cb_publish.idempotency_key,
                                cb_publish.headers,
                  _visible_at;
        EXCEPTION WHEN undefined_table THEN
            -- Queue was deleted between SELECT and INSERT, skip it
            CONTINUE;
        END;
    END LOOP;
END
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_bind: Subscribe a queue to a topic pattern
-- Supports exact topics and wildcards (* for single token, # for multi-token tail)
-- Parameters:
--   queue_name: Name of the queue to bind
--   pattern: Topic pattern (e.g., 'foo.bar', 'foo.*.bar', 'foo.bar.#')
-- Returns: void
CREATE OR REPLACE FUNCTION cb_bind(queue_name text, pattern text)
RETURNS void
LANGUAGE plpgsql AS $$
#variable_conflict use_column
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
    INSERT INTO cb_bindings(queue_name, pattern, pattern_type, prefix, regex)
    VALUES (cb_bind.queue_name, cb_bind.pattern, p_type, p_prefix, p_regex)
    ON CONFLICT (queue_name, pattern) DO NOTHING;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_unbind: Unsubscribe a queue from a topic pattern
-- Parameters:
--   queue_name: Name of the queue
--   pattern: Topic pattern to remove
-- Returns: void
CREATE OR REPLACE FUNCTION cb_unbind(queue_name text, pattern text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM cb_bindings
    WHERE cb_bindings.queue_name = cb_unbind.queue_name
      AND cb_bindings.pattern = cb_unbind.pattern;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'cb: binding not found for queue "%" and pattern "%"', cb_unbind.queue_name, cb_unbind.pattern;
    END IF;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_send: Send a message to a specific queue
-- Enqueues a message in the queue, with optional topic and idempotency
-- Parameters:
--   queue: Queue name
--   payload: JSON message payload
--   topic: Optional topic string for categorization
--   idempotency_key: Optional unique ID for idempotency (prevents duplicate messages)
--   visible_at: Optional timestamp when message should become visible (default: now)
-- Returns: bigint - the message ID
CREATE OR REPLACE FUNCTION cb_send(
    queue text,
    payload jsonb,
    topic text = null,
    idempotency_key text = null,
    headers jsonb = null,
    visible_at timestamptz = null
)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz := coalesce(cb_send.visible_at, now());
    _q_table text := cb_table_name(cb_send.queue, 'q');
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
            INSERT INTO %I (topic, payload, idempotency_key, headers, visible_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
            DO UPDATE SET visible_at = EXCLUDED.visible_at WHERE FALSE
            RETURNING id
        )
        SELECT id FROM ins
        UNION ALL
        SELECT id FROM %I
        WHERE idempotency_key = $3 AND idempotency_key IS NOT NULL
        LIMIT 1
        $QUERY$,
        _q_table, _q_table
    )
    USING cb_send.topic,
          cb_send.payload,
          cb_send.idempotency_key,
            cb_send.headers,
          _visible_at
    INTO _id;

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
    _q_table text := cb_table_name(cb_read.queue, 'q');
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
          ORDER BY id ASC
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE %I m
        SET deliveries = deliveries + 1,
            visible_at = clock_timestamp() + $2
        FROM msgs
        WHERE m.id = msgs.id
        RETURNING m.id,
                  m.idempotency_key,
                  m.topic,
                  m.payload,
                  m.headers,
                  m.deliveries,
                  m.created_at,
                  m.visible_at;
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
    _q_table text := cb_table_name(cb_read_poll.queue, 'q');
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
                ORDER BY id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE %I m
            SET deliveries = deliveries + 1,
                visible_at = clock_timestamp() + $2
            FROM msgs
            WHERE m.id = msgs.id
            RETURNING m.id,
                      m.idempotency_key,
                      m.topic,
                      m.payload,
                      m.headers,
                      m.deliveries,
                      m.created_at,
                      m.visible_at;
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
    _q_table text := cb_table_name(cb_hide.queue, 'q');
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
-- Returns: void
CREATE OR REPLACE FUNCTION cb_hide(
    queue text,
    ids bigint[],
    hide_for int
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := cb_table_name(cb_hide.queue, 'q');
BEGIN
    IF cb_hide.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    EXECUTE format(
        $QUERY$
        UPDATE %I
        SET visible_at = (clock_timestamp() + $2)
        WHERE id = any($1);
        $QUERY$,
        _q_table
    )
    USING cb_hide.ids, make_interval(secs => cb_hide.hide_for / 1000.0);
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
    _q_table text := cb_table_name(cb_delete.queue, 'q');
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
-- Returns: void
CREATE OR REPLACE FUNCTION cb_delete(queue text, ids bigint[])
RETURNS void AS $$
DECLARE
    _q_table text := cb_table_name(cb_delete.queue, 'q');
BEGIN
    EXECUTE format(
        $QUERY$
        DELETE FROM %I WHERE id = any($1);
        $QUERY$,
        _q_table
    )
    USING cb_delete.ids;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
-- cb_send (bulk): Send multiple messages to a specific queue
-- Enqueues messages in a queue from a jsonb[] payload array.
-- Parameters:
--   queue: Queue name
--   payloads: JSONB array of message payloads
--   topic: Optional topic string for categorization
--   idempotency_keys: Optional array of idempotency keys (must match payload count when provided)
--   visible_at: Optional timestamp when messages should become visible (default: now)
-- Returns: bigint[] - inserted message IDs
CREATE OR REPLACE FUNCTION cb_send(
    queue text,
    payloads jsonb[],
    topic text = null,
    idempotency_keys text[] = null,
    headers jsonb[] = null,
    visible_at timestamptz = null
)
RETURNS bigint[]
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz := coalesce(cb_send.visible_at, now());
    _q_table text := cb_table_name(cb_send.queue, 'q');
    _ids bigint[];
BEGIN
    IF cb_send.idempotency_keys IS NOT NULL AND cardinality(cb_send.idempotency_keys) <> cardinality(cb_send.payloads) THEN
        RAISE EXCEPTION 'cb: idempotency_keys length must match payloads length';
    END IF;

    IF cb_send.headers IS NOT NULL AND cardinality(cb_send.headers) <> cardinality(cb_send.payloads) THEN
        RAISE EXCEPTION 'cb: headers length must match payloads length';
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
        WITH payload_list AS (
            SELECT
                payload,
                ordinality,
                CASE
                    WHEN $5 IS NULL THEN NULL
                    ELSE $5[ordinality]
                END AS idempotency_key,
                CASE
                    WHEN $3 IS NULL THEN NULL
                    ELSE $3[ordinality]
                END AS headers
            FROM unnest($1) WITH ORDINALITY AS t(payload, ordinality)
        ),
        ins AS (
            INSERT INTO %I (topic, payload, idempotency_key, headers, visible_at)
            SELECT $2, payload, idempotency_key, headers, $4
            FROM payload_list
            ORDER BY ordinality
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
        )
        SELECT coalesce(array_agg(id ORDER BY id), '{}'::bigint[])
        FROM ins
        $QUERY$,
        _q_table
    )
    USING cb_send.payloads,
          cb_send.topic,
            cb_send.headers,
          _visible_at,
          cb_send.idempotency_keys
    INTO _ids;

    RETURN _ids;
END
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_publish (bulk): Send multiple messages to all queues subscribed to a topic
-- Routes each payload from a jsonb[] array to all matching queues.
-- Parameters:
--   topic: Topic string to route to subscribed queues
--   payloads: JSONB array of message payloads
--   idempotency_keys: Optional array of idempotency keys (must match payload count when provided)
--   visible_at: Optional timestamp when messages should become visible (default: now)
-- Returns: void
CREATE OR REPLACE FUNCTION cb_publish(
    topic text,
    payloads jsonb[],
    idempotency_keys text[] = null,
    headers jsonb[] = null,
    visible_at timestamptz = null
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz = coalesce(cb_publish.visible_at, now());
    _rec record;
    _q_table text;
BEGIN
    IF cb_publish.idempotency_keys IS NOT NULL AND cardinality(cb_publish.idempotency_keys) <> cardinality(cb_publish.payloads) THEN
        RAISE EXCEPTION 'cb: idempotency_keys length must match payloads length';
    END IF;

    IF cb_publish.headers IS NOT NULL AND cardinality(cb_publish.headers) <> cardinality(cb_publish.payloads) THEN
        RAISE EXCEPTION 'cb: headers length must match payloads length';
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
        SELECT DISTINCT queue_name AS name
        FROM cb_bindings
        WHERE (
            (pattern_type = 'exact' AND pattern = cb_publish.topic)
            OR
            (pattern_type = 'wildcard'
             AND (prefix = '' OR cb_publish.topic LIKE prefix || '%')
             AND cb_publish.topic ~ regex)
        )
    LOOP
        _q_table := cb_table_name(_rec.name, 'q');
        BEGIN
            EXECUTE format(
                $QUERY$
                INSERT INTO %I (topic, payload, idempotency_key, headers, visible_at)
                SELECT
                    $1,
                    payload,
                    CASE
                        WHEN $4 IS NULL THEN NULL
                        ELSE $4[ordinality]
                    END,
                    CASE
                        WHEN $5 IS NULL THEN NULL
                        ELSE $5[ordinality]
                    END,
                    $2
                FROM unnest($3) WITH ORDINALITY AS t(payload, ordinality)
                ON CONFLICT (idempotency_key) DO NOTHING;
                $QUERY$,
                _q_table
            )
            USING cb_publish.topic,
                  _visible_at,
                  cb_publish.payloads,
                  cb_publish.idempotency_keys,
                  cb_publish.headers;
        EXCEPTION WHEN undefined_table THEN
            CONTINUE;
        END;
    END LOOP;
END
$$;
-- +goose statementend

-- +goose down

-- +goose statementbegin
DO $$
BEGIN
    IF to_regclass('public.cb_queues') IS NOT NULL THEN
        PERFORM cb_delete_queue(name)
        FROM cb_queues;
    END IF;
END
$$;
-- +goose statementend

DROP FUNCTION IF EXISTS cb_unbind(text, text);
DROP FUNCTION IF EXISTS cb_bind(text, text);
DROP FUNCTION IF EXISTS cb_create_queue(text, timestamptz, text);
DROP FUNCTION IF EXISTS cb_delete_queue(text);
DROP FUNCTION IF EXISTS cb_publish(text, jsonb, text, jsonb, timestamptz);
DROP FUNCTION IF EXISTS cb_publish(text, jsonb[], text[], jsonb[], timestamptz);
DROP FUNCTION IF EXISTS cb_send(text, jsonb, text, text, jsonb, timestamptz);
DROP FUNCTION IF EXISTS cb_send(text, jsonb[], text, text[], jsonb[], timestamptz);
DROP FUNCTION IF EXISTS cb_send(text, jsonb[], text, text[], jsonb, timestamptz);
DROP FUNCTION IF EXISTS cb_publish(text, jsonb[], text[], jsonb, timestamptz);
DROP FUNCTION IF EXISTS cb_publish(text, jsonb, text, text, jsonb, timestamptz);
DROP FUNCTION IF EXISTS cb_read(text, int, int);
DROP FUNCTION IF EXISTS cb_read_poll(text, int, int, int, int);
DROP FUNCTION IF EXISTS cb_hide(text, bigint, integer);
DROP FUNCTION IF EXISTS cb_hide(text, bigint[], integer);
DROP FUNCTION IF EXISTS cb_delete(text, bigint);
DROP FUNCTION IF EXISTS cb_delete(text, bigint[]);
