-- SQL code is mostly taken or adapted from the excellent PGMQ https://github.com/pgmq/pgmq/blob/main/pgmq-extension/sql/pgmq.sql 

-- +goose up

CREATE TYPE cb_message AS (
    id bigint,
    topic text,
    payload jsonb,
    created_at timestamptz,
    deliver_at timestamptz
);

CREATE TABLE cb_queues (
    name text UNIQUE NOT NULL,
    topics text[] NOT NULL,
    unlogged boolean NOT null,
    delete_at timestamptz
);

CREATE INDEX cb_queues_topics_idx ON cb_queues USING gin (topics);
CREATE INDEX cb_queues_delete_at_idx ON cb_queues (delete_at);

-- +goose statementbegin
CREATE FUNCTION _cb_acquire_queue_lock(name text) 
RETURNS void AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtext(_cb_queue_table(_cb_acquire_queue_lock.name)));
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION _cb_queue_table(name text)
RETURNS text AS $$
BEGIN
    IF _cb_queue_table.name !~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'cb: queue name can only contain characters: a-z, 0-9 or _';
    END IF;
    IF length(_cb_queue_table.name) >= 58 THEN
        raise exception 'cb: queue name is too long, maximum length is 58';
    END IF;
    RETURN 'cb_q_' || lower(_cb_queue_table.name);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_create_queue(
    name text,
    topics text[],
    delete_at timestamptz = null,
    unlogged boolean = false
)
RETURNS void AS $$
DECLARE
    _q_table text = _cb_queue_table(cb_create_queue.name);
BEGIN
    PERFORM _cb_acquire_queue_lock(cb_create_queue.name);

    IF cb_create_queue.unlogged THEN
        EXECUTE format(
            $QUERY$
            CREATE UNLOGGED TABLE IF NOT EXISTS %I (
                id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                topic text NOT NULL,
                payload jsonb NOT NULL,
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
                topic text NOT NULL,
                payload jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(),
                deliver_at timestamptz NOT NULL DEFAULT now()
            )
            $QUERY$,
            _q_table
        );
    END IF;    

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (deliver_at);', _q_table || '_deliver_at_idx', _q_table);

    -- TODO should check topic and delete_at is same
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
CREATE FUNCTION cb_delete_queue(name text)
RETURNS boolean AS $$
DECLARE
    _q_table text = _cb_queue_table(cb_delete_queue.name);
    _res boolean;
BEGIN
    PERFORM _cb_acquire_queue_lock(cb_delete_queue.name);

    EXECUTE FORMAT('DROP TABLE IF EXISTS %I;', _q_table);

    DELETE FROM cb_queues q
    WHERE q.name = cb_delete_queue.name
    RETURNING true
    INTO _res;
 
    RETURN coalesce(_res, false);
end
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_send(
    topic text,
    payload jsonb,
    deliver_at timestamptz = null
)
RETURNS void AS $$
DECLARE
    _deliver_at timestamptz = coalesce(cb_send.deliver_at, now());
    _rec record;
    _q_table text;
BEGIN
    FOR _rec IN 
        SELECT q.name
        FROM cb_queues q
        WHERE cb_send.topic = any(q.topics) AND (q.delete_at IS NULL OR q.delete_at > now())
    LOOP
        _q_table = _cb_queue_table(_rec.name);
        EXECUTE format('INSERT INTO %I (topic, payload, deliver_at) VALUES ($1, $2, $3);', _q_table)
        USING cb_send.topic, cb_send.payload, _deliver_at;
    END LOOP;
END
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
-- TODO return or error if deleted
CREATE FUNCTION cb_read(
    queue text,
    quantity int = 1,
    hide_for int = 10
)
RETURNS SETOF cb_message AS $$
DECLARE
    _q_table text = _cb_queue_table(cb_read.queue);
    _q text;
BEGIN
    _q = format(
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
		SET deliver_at = clock_timestamp() + $2
		FROM msgs
		WHERE m.id = msgs.id
		RETURNING m.id, m.topic, m.payload, m.created_at, m.deliver_at;
        $QUERY$,
        _q_table, _q_table
    );
    RETURN QUERY EXECUTE _q USING cb_read.quantity, make_interval(secs => cb_read.hide_for);
end
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_read_poll(
    queue text,
    quantity int = 1,
    hide_for int = 10,
    poll_for int = 5,
    poll_interval double precision = 0.1
)
RETURNS SETOF cb_message AS $$
DECLARE
    _m cb_message;
    _stop_at timestamp;
    _q text;
    _q_table text = _cb_queue_table(cb_read_poll.queue);
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
                ORDER BY id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE %I m
            SET deliver_at = clock_timestamp() + $2
            FROM msgs
            WHERE m.id = msgs.id
            RETURNING m.id, m.topic, m.payload, m.created_at, m.deliver_at;
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
        PERFORM pg_sleep(cb_read_poll.poll_interval);
      END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_hide(
    queue text,
    id bigint,
    hide_for integer
)
RETURNS boolean AS $$
DECLARE
    _q_table text = _cb_queue_table(cb_hide.queue);
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
CREATE FUNCTION cb_delete(queue text, id bigint)
RETURNS boolean AS $$
DECLARE
    _q_table text = _cb_queue_table(cb_delete.queue);
    _res boolean;
BEGIN
    EXECUTE format('DELETE FROM %I WHERE id = $1 RETURNING TRUE;', _q_table)
    USING cb_delete.id
    INTO _res;
    RETURN coalesce(_res, false);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_gc()
RETURNS void AS $$
DECLARE
    _rec record;
BEGIN
    FOR _rec IN 
        DELETE FROM cb_queues
		WHERE delete_at IS NOT NULL AND delete_at <= now()
        RETURNING name
    LOOP
        PERFORM _cb_acquire_queue_lock(_rec.name);
        EXECUTE format('DROP TABLE %I;', _cb_queue_table(_rec.name));
    END LOOP;
END
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose down

SELECT cb_delete_queue(name) FROM cb_queues;

DROP FUNCTION cb_create_queue;
DROP FUNCTION cb_delete_queue;
DROP FUNCTION cb_send;
DROP FUNCTION cb_read;
DROP FUNCTION cb_read_poll;
DROP FUNCTION cb_hide;
DROP FUNCTION cb_delete;
DROP FUNCTION cb_gc;
DROP FUNCTION _cb_queue_table;
DROP FUNCTION _cb_acquire_queue_lock;
DROP TABLE cb_queues CASCADE;
DROP TYPE cb_message;
