-- SQL code is mostly taken or adapted from pgmq (https://github.com/pgmq/pgmq) 
-- TODO better return values

-- +goose up

-- +goose statementbegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cb_message') THEN
        CREATE TYPE cb_message AS (
            id bigint,
            deduplication_id text,
            topic text,
            payload jsonb,
            deliveries int,
            created_at timestamptz,
            deliver_at timestamptz
        );
    END IF;
END$$;
-- +goose statementend

CREATE TABLE IF NOT EXISTS cb_queues (
    name text PRIMARY KEY,
    topics text[],
    unlogged boolean NOT null,
    created_at timestamptz NOT NULL DEFAULT now(),
    delete_at timestamptz,
    CONSTRAINT delete_at_is_valid CHECK (delete_at IS NULL OR delete_at > created_at)
);

CREATE INDEX IF NOT EXISTS cb_queues_topics_idx ON cb_queues USING gin (topics);
CREATE INDEX IF NOT EXISTS cb_queues_delete_at_idx ON cb_queues (delete_at);

-- +goose statementbegin
-- cb_table_name: Generate a PostgreSQL table name for queue/task/flow storage
-- Validates the name and constructs the internal table name with prefix
-- Parameters:
--   name: Queue/task/flow name (must contain only a-z, 0-9, _; max 58 chars)
--   prefix: Type prefix ('q' for queue, 't' for task, etc.)
-- Returns: text - the full PostgreSQL table name
CREATE OR REPLACE FUNCTION cb_table_name(name text, prefix text)
RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
    IF cb_table_name.name !~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'cb: queue name can only contain characters: a-z, 0-9 or _';
    END IF;
    IF length(cb_table_name.name) >= 58 THEN
        RAISE EXCEPTION 'cb: queue name is too long, maximum length is 58';
    END IF;
    RETURN 'cb_' || cb_table_name.prefix || '_' || lower(cb_table_name.name);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_create_queue: Create a queue definition
-- Creates the queue metadata and associated message table for enqueued messages
-- Parameters:
--   name: Queue name (must be unique)
--   topics: Optional array of topic strings for message routing
--   delete_at: Optional timestamp when the queue should be automatically deleted
--   unlogged: Whether to use an unlogged table for better performance (loses durability)
-- Returns: void
CREATE OR REPLACE FUNCTION cb_create_queue(
    name text,
    topics text[] = null,
    delete_at timestamptz = null,
    unlogged boolean = false
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _q_table text := cb_table_name(cb_create_queue.name, 'q');
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(_q_table));

    IF cb_create_queue.unlogged THEN
        EXECUTE format(
            $QUERY$
            CREATE UNLOGGED TABLE IF NOT EXISTS %I (
                id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                deduplication_id text,
                topic text,
                payload jsonb NOT NULL,
                deliveries int NOT NULL DEFAULT 0,
                created_at timestamptz NOT NULL DEFAULT now(),
                deliver_at timestamptz NOT NULL DEFAULT now()
            )
            $QUERY$,
            _q_table
        );
    ELSE
        EXECUTE format(
            $QUERY$
            CREATE TABLE IF NOT EXISTS %I (
                id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                deduplication_id text,
                topic text,
                payload jsonb NOT NULL,
                deliveries int NOT NULL DEFAULT 0,
                created_at timestamptz NOT NULL DEFAULT now(),
                deliver_at timestamptz NOT NULL DEFAULT now()
            )
            $QUERY$,
            _q_table
        );
    END IF;

    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (deduplication_id);', _q_table || '_deduplication_id_idx', _q_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (deliver_at);', _q_table || '_deliver_at_idx', _q_table);

    -- TODO should check attributes are same
    INSERT INTO cb_queues (name, topics, unlogged, delete_at)
    VALUES (
        cb_create_queue.name,
        cb_create_queue.topics,
        cb_create_queue.unlogged,
        cb_create_queue.delete_at
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
-- cb_dispatch: Send a message to all queues subscribed to a topic
-- Routes the message to queues based on topic subscription
-- Parameters:
--   topic: Topic string to route to subscribed queues
--   payload: JSON message payload
--   deduplication_id: Optional unique ID for deduplication (prevents duplicate messages)
--   deliver_at: Optional timestamp when message should become deliverable (default: now)
-- Returns: void
CREATE OR REPLACE FUNCTION cb_dispatch(
    topic text,
    payload jsonb,
    deduplication_id text = null,
    deliver_at timestamptz = null
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _deliver_at timestamptz = coalesce(cb_dispatch.deliver_at, now());
    _rec record;
    _q_table text;
BEGIN
    FOR _rec IN 
        SELECT q.name
        FROM cb_queues q
        WHERE cb_dispatch.topic = any(q.topics) AND (q.delete_at IS NULL OR q.delete_at > now())
    LOOP
        _q_table := cb_table_name(_rec.name, 'q');
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (topic, payload, deduplication_id, deliver_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (deduplication_id) DO NOTHING;
            $QUERY$,
            _q_table
        )
        USING cb_dispatch.topic,
              cb_dispatch.payload,
              cb_dispatch.deduplication_id,
              _deliver_at;
    END LOOP;
END
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_send: Send a message to a specific queue
-- Enqueues a message in the queue, with optional topic and deduplication
-- Parameters:
--   queue: Queue name
--   payload: JSON message payload
--   topic: Optional topic string for categorization
--   deduplication_id: Optional unique ID for deduplication (prevents duplicate messages)
--   deliver_at: Optional timestamp when message should become deliverable (default: now)
-- Returns: bigint - the message ID
CREATE OR REPLACE FUNCTION cb_send(
    queue text,
    payload jsonb,
    topic text = null,
    deduplication_id text = null,
    deliver_at timestamptz = null
)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _deliver_at timestamptz := coalesce(cb_send.deliver_at, now());
    _q_table text := cb_table_name(cb_send.queue, 'q');
    _id bigint;
BEGIN
    EXECUTE format(
        $QUERY$
        INSERT INTO %I (topic, payload, deduplication_id, deliver_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (deduplication_id) DO NOTHING
        RETURNING id;
        $QUERY$,
        _q_table
    )
    USING cb_send.topic,
          cb_send.payload,
          cb_send.deduplication_id,
          _deliver_at
    INTO _id;

    RETURN _id;
END
$$;
-- +goose statementend

-- +goose statementbegin
-- TODO return or error if deleted-- cb_read: Read messages from a queue
-- Parameters:
--   queue: Queue name
--   quantity: Number of messages to read (must be > 0)
--   hide_for: Duration in milliseconds to hide messages from other readers (must be > 0)
-- Returns: Set of cb_message recordsCREATE OR REPLACE FUNCTION cb_read(
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
          WHERE deliver_at <= clock_timestamp()
          ORDER BY id ASC
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE %I m
        SET deliveries = deliveries + 1,
            deliver_at = clock_timestamp() + $2
        FROM msgs
        WHERE m.id = msgs.id
        RETURNING m.id,
                  m.deduplication_id,
                  m.topic,
                  m.payload,
                  m.deliveries,
                  m.created_at,
                  m.deliver_at;
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
                WHERE deliver_at <= clock_timestamp()
                ORDER BY id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE %I m
            SET deliveries = deliveries + 1,
                deliver_at = clock_timestamp() + $2
            FROM msgs
            WHERE m.id = msgs.id
            RETURNING m.id,
                      m.deduplication_id,
                      m.topic,
                      m.payload,
                      m.deliveries,
                      m.created_at,
                      m.deliver_at;
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
        SET deliver_at = (clock_timestamp() + $2)
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
        SET deliver_at = (clock_timestamp() + $2)
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

-- +goose down

SELECT cb_delete_queue(name) FROM cb_queues;

DROP FUNCTION cb_create_queue(text, text[], timestamptz, boolean);
DROP FUNCTION cb_delete_queue(text);
DROP FUNCTION cb_dispatch(text, jsonb, text, timestamptz);
DROP FUNCTION cb_send(text, jsonb, text, text, timestamptz);
DROP FUNCTION cb_read(text, int, int);
DROP FUNCTION cb_read_poll(text, int, int, int, int);
DROP FUNCTION cb_hide(text, bigint, integer);
DROP FUNCTION cb_hide(text, bigint[], integer);
DROP FUNCTION cb_delete(text, bigint);
DROP FUNCTION cb_delete(text, bigint[]);
DROP FUNCTION cb_table_name(text, text);

DROP TABLE cb_queues;

DROP TYPE cb_message;
