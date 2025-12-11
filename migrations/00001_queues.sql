-- SQL code is mostly taken or adapted from the excellent PGMQ https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql 

-- +goose up

-- +goose statementbegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cb_message') THEN
        CREATE TYPE cb_message AS (
            id bigint,
            deduplication_id text,
            topic text,
            payload json,
            priority int,
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
    delete_at timestamptz
);

CREATE INDEX IF NOT EXISTS cb_queues_topics_idx ON cb_queues USING gin (topics);
CREATE INDEX IF NOT EXISTS cb_queues_delete_at_idx ON cb_queues (delete_at);

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_acquire_queue_lock(name text) 
RETURNS void AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext(_cb_table_name(_cb_acquire_queue_lock.name, 'q')));
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION _cb_table_name(name text, prefix text)
RETURNS text AS $$
BEGIN
    IF _cb_table_name.name !~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'cb: queue name can only contain characters: a-z, 0-9 or _';
    END IF;
    IF length(_cb_table_name.name) >= 58 THEN
        RAISE EXCEPTION 'cb: queue name is too long, maximum length is 58';
    END IF;
    RETURN 'cb_' || _cb_table_name.prefix || '_' || lower(_cb_table_name.name);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_queue(
    name text,
    topics text[] = null,
    delete_at timestamptz = null,
    unlogged boolean = false
)
RETURNS void AS $$
DECLARE
    _q_table text = _cb_table_name(cb_create_queue.name, 'q');
    _a_table text = _cb_table_name(cb_create_queue.name, 'a');
    _f_table text = _cb_table_name(cb_create_queue.name, 'f');
BEGIN
    PERFORM _cb_acquire_queue_lock(cb_create_queue.name);

    IF cb_create_queue.unlogged THEN
        EXECUTE format(
            $QUERY$
            CREATE UNLOGGED TABLE IF NOT EXISTS %I (
                id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                deduplication_id text,
                topic text,
                payload json NOT NULL,
                priority int NOT NULL DEFAULT 0,
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
                payload json NOT NULL,
                priority int NOT NULL DEFAULT 0,
                deliveries int NOT NULL DEFAULT 0,
                created_at timestamptz NOT NULL DEFAULT now(),
                deliver_at timestamptz NOT NULL DEFAULT now()
            )
            $QUERY$,
            _q_table
        );

        EXECUTE format(
            $QUERY$
            CREATE TABLE IF NOT EXISTS %I (
                id bigint PRIMARY KEY,
                deduplication_id text,
                topic text,
                payload json NOT NULL,
                priority int NOT NULL DEFAULT 0,
                deliveries int NOT NULL,
                created_at timestamptz NOT NULL,
                deliver_at timestamptz NOT NULL,
                archived_at timestamptz NOT NULL DEFAULT now()
            )
            $QUERY$,
            _a_table
        );

        EXECUTE format(
            $QUERY$
            CREATE TABLE IF NOT EXISTS %I (
                id bigint PRIMARY KEY,
                deduplication_id text,
                topic text,
                payload json NOT NULL,
                priority int NOT NULL DEFAULT 0,
                deliveries int NOT NULL,
                created_at timestamptz NOT NULL,
                deliver_at timestamptz NOT NULL,
                failed_at timestamptz NOT NULL DEFAULT now()
            )
            $QUERY$,
            _f_table
        );
    END IF;

    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (deduplication_id);', _q_table || '_deduplication_id_idx', _q_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (priority);', _q_table || '_priority_idx', _q_table);
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
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_delete_queue(name text)
RETURNS boolean AS $$
DECLARE
    _q_table text = _cb_table_name(cb_delete_queue.name, 'q');
    _a_table text = _cb_table_name(cb_delete_queue.name, 'a');
    _f_table text = _cb_table_name(cb_delete_queue.name, 'f');
    _res boolean;
BEGIN
    PERFORM _cb_acquire_queue_lock(cb_delete_queue.name);

    EXECUTE FORMAT('DROP TABLE IF EXISTS %I;', _q_table);
    EXECUTE FORMAT('DROP TABLE IF EXISTS %I;', _a_table);
    EXECUTE FORMAT('DROP TABLE IF EXISTS %I;', _f_table);

    DELETE FROM cb_queues q
    WHERE q.name = cb_delete_queue.name
    RETURNING true
    INTO _res;
 
    RETURN coalesce(_res, false);
end
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_dispatch(
    topic text,
    payload json,
    deduplication_id text = null,
    priority int = 0,
    deliver_at timestamptz = null
)
RETURNS void AS $$
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
        _q_table = _cb_table_name(_rec.name, 'q');
        EXECUTE format(
            $QUERY$
            INSERT INTO %I (topic, payload, deduplication_id, priority, deliver_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (deduplication_id) DO NOTHING;
            $QUERY$,
            _q_table
        )
        USING cb_dispatch.topic,
              cb_dispatch.payload,
              cb_dispatch.deduplication_id,
              cb_dispatch.priority,
              _deliver_at;
    END LOOP;
END
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_send(
    queue text,
    payload json,
    topic text = null,
    deduplication_id text = null,
    priority int = 0,
    deliver_at timestamptz = null
)
RETURNS bigint AS $$
DECLARE
    _deliver_at timestamptz = coalesce(cb_send.deliver_at, now());
    _q_table text = _cb_table_name(cb_send.queue, 'q');
    _id bigint;
BEGIN
    EXECUTE format(
        $QUERY$
        INSERT INTO %I (topic, payload, deduplication_id, priority, deliver_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (deduplication_id) DO NOTHING
        RETURNING id;
        $QUERY$,
        _q_table
    )
    USING cb_send.topic,
          cb_send.payload,
          cb_send.deduplication_id,
          cb_send.priority,
          _deliver_at
    INTO _id;

    RETURN _id;
END
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
-- TODO return or error if deleted
CREATE OR REPLACE FUNCTION cb_read(
    queue text,
    quantity int,
    hide_for int
)
RETURNS SETOF cb_message AS $$
DECLARE
    _q_table text = _cb_table_name(cb_read.queue, 'q');
    _q text;
BEGIN
    _q = format(
        $QUERY$
        WITH msgs AS (
			SELECT id
			FROM %I
			WHERE deliver_at <= clock_timestamp()
			ORDER BY priority DESC, id ASC
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
                  m.priority,
                  m.deliveries,
                  m.created_at,
                  m.deliver_at;
        $QUERY$,
        _q_table, _q_table
    );
    RETURN QUERY EXECUTE _q USING cb_read.quantity, make_interval(secs => cb_read.hide_for);
end
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_read_poll(
    queue text,
    quantity int,
    hide_for int,
    poll_for int = 5,
    poll_interval int = 100
)
RETURNS SETOF cb_message AS $$
DECLARE
    _m cb_message;
    _stop_at timestamp;
    _sleep_for double precision = cb_read_poll.poll_interval::numeric / 1000;
    _q text;
    _q_table text = _cb_table_name(cb_read_poll.queue, 'q');
BEGIN
    _stop_at := clock_timestamp() + make_interval(secs => cb_read_poll.poll_for);
    LOOP
        IF (SELECT clock_timestamp() >= _stop_at) THEN
            RETURN;
        END IF;

        _q = FORMAT(
            $QUERY$
            WITH msgs AS (
                SELECT id
                FROM %I
                WHERE deliver_at <= clock_timestamp()
    			ORDER BY priority DESC, id ASC
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
                      m.priority,
                      m.deliveries,
                      m.created_at,
                      m.deliver_at;
            $QUERY$,
            _q_table, _q_table
      );

      FOR _m IN
        EXECUTE _q USING cb_read_poll.quantity, make_interval(secs => cb_read_poll.hide_for)
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
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_hide(
    queue text,
    id bigint,
    hide_for integer
)
RETURNS boolean AS $$
DECLARE
    _q_table text = _cb_table_name(cb_hide.queue, 'q');
    _res boolean;
BEGIN
    EXECUTE format(
        $QUERY$
        UPDATE %I
        SET deliver_at = (clock_timestamp() + $2)
        WHERE id = $1
        RETURNING TRUE;
        $QUERY$,
        _q_table
    )
    USING cb_hide.id, make_interval(secs => cb_hide.hide_for)
    INTO _res;
    RETURN coalesce(_res, false);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_delete(queue text, id bigint)
RETURNS boolean AS $$
DECLARE
    _q_table text = _cb_table_name(cb_delete.queue, 'q');
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
CREATE OR REPLACE FUNCTION cb_archive(queue text, id bigint)
RETURNS boolean AS $$
DECLARE
    _q_table text = _cb_table_name(cb_archive.queue, 'q');
    _a_table text = _cb_table_name(cb_archive.queue, 'a');
    _res boolean;
BEGIN
    EXECUTE format(
        $QUERY$
        WITH msgs AS (
            DELETE FROM %I
            WHERE id = $1
            RETURNING id,
                      deduplication_id,
                      topic,
                      payload,
                      deliveries,
                      created_at,
                      deliver_at
        )
        INSERT INTO %I (id, deduplication_id, topic, payload, deliveries, created_at, deliver_at)
        SELECT id, deduplication_id, topic, payload, deliveries, created_at, deliver_at
        FROM msgs
        RETURNING TRUE;
        $QUERY$,
        _q_table,
        _a_table
    )
    USING cb_archive.id
    INTO _res;
    RETURN coalesce(_res, false);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_fail(queue text, id bigint)
RETURNS boolean AS $$
DECLARE
    _q_table text = _cb_table_name(cb_fail.queue, 'q');
    _f_table text = _cb_table_name(cb_fail.queue, 'f');
    _res boolean;
BEGIN
    EXECUTE format(
        $QUERY$
        WITH msgs AS (
            DELETE FROM %I
            WHERE id = $1
            RETURNING id,
                      deduplication_id,
                      topic,
                      payload,
                      deliveries,
                      created_at,
                      deliver_at
        )
        INSERT INTO %I (id, deduplication_id, topic, payload, deliveries, created_at, deliver_at)
        SELECT id, deduplication_id, topic, payload, deliveries, created_at, deliver_at
        FROM msgs
        RETURNING TRUE;
        $QUERY$,
        _q_table,
        _f_table
    )
    USING cb_fail.id
    INTO _res;
    RETURN coalesce(_res, false);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_gc()
RETURNS void AS $$
DECLARE
BEGIN
    SELECT cb_delete_queue(name)
    FROM cb_queues
    WHERE delete_at IS NOT NULL AND delete_at <= now();
END
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose down

SELECT cb_delete_queue(name) FROM cb_queues;

DROP FUNCTION cb_create_queue;
DROP FUNCTION cb_delete_queue;
DROP FUNCTION cb_dispatch;
DROP FUNCTION cb_send;
DROP FUNCTION cb_read;
DROP FUNCTION cb_read_poll;
DROP FUNCTION cb_hide;
DROP FUNCTION cb_delete;
DROP FUNCTION cb_archive;
DROP FUNCTION cb_fail;
DROP FUNCTION cb_gc;
DROP FUNCTION _cb_table_name;
DROP FUNCTION _cb_acquire_queue_lock;
DROP TABLE cb_queues CASCADE;
DROP TYPE cb_message;
