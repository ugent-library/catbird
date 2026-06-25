-- +goose up

-- Durable, per-identity notification inbox. A mailbox keyed by identity with a
-- monotonic cursor (id) and a seen marker. The store is the cursor authority;
-- transports (poll, SSE push) are dumb readers. "Replay since I last looked" is
-- just WHERE id > cursor. Logged (durability is the point), deliberately separate
-- from Wire's UNLOGGED tables so a future Wire extraction never touches it.

CREATE TABLE IF NOT EXISTS cb_notifications (
    id                bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,  -- the cursor
    identity          text        NOT NULL,
    topic             text        NOT NULL,
    collapse_key      text,                  -- optional; a newer row collapses prior unseen rows with the same (identity, collapse_key)
    message           text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    seen_at           timestamptz,
    expires_at        timestamptz,           -- relevance window; also the GC drop trigger
    CONSTRAINT cb_expires_at_is_valid CHECK (expires_at IS NULL OR expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS cb_notifications_unseen_idx
    ON cb_notifications (identity, id) WHERE seen_at IS NULL;                        -- hot poll path
CREATE INDEX IF NOT EXISTS cb_notifications_expires_at_idx
    ON cb_notifications (expires_at) WHERE expires_at IS NOT NULL;                   -- gc sweep
CREATE INDEX IF NOT EXISTS cb_notifications_collapse_key_idx
    ON cb_notifications (identity, collapse_key)
    WHERE collapse_key IS NOT NULL AND seen_at IS NULL;                              -- collapse lookup

-- +goose statementbegin
-- cb_notify_durable: append a durable notification to an identity's inbox.
-- Returns the new row id (the cursor value).
-- When collapse_key is set, prior unseen rows with the same (identity, collapse_key)
-- are first marked seen (write-time collapse, the FCM collapse_key semantics: a newer
-- message with the same key replaces the older undelivered one), so the new row is the
-- only live one for that subject. Note this is keep-newest, the deliberate opposite of
-- the queue's keep-oldest concurrency_key.
CREATE OR REPLACE FUNCTION cb_notify_durable(
    identity text,
    topic text,
    message text DEFAULT NULL,
    collapse_key text DEFAULT NULL,
    expires_at timestamptz DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _id bigint;
BEGIN
    IF cb_notify_durable.collapse_key IS NOT NULL THEN
        UPDATE cb_notifications
        SET seen_at = now()
        WHERE cb_notifications.identity = cb_notify_durable.identity
          AND cb_notifications.collapse_key = cb_notify_durable.collapse_key
          AND cb_notifications.seen_at IS NULL;
    END IF;

    INSERT INTO cb_notifications (identity, topic, collapse_key, message, expires_at)
    VALUES (
        cb_notify_durable.identity,
        cb_notify_durable.topic,
        cb_notify_durable.collapse_key,
        cb_notify_durable.message,
        cb_notify_durable.expires_at
    )
    RETURNING id INTO _id;

    RETURN _id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_mark_seen: cursor ack. Marks unseen rows with id <= up_to_id as seen for this
-- identity. Returns the number of rows marked. Seen-tracking always flows through this
-- cursor regardless of transport.
CREATE OR REPLACE FUNCTION cb_mark_seen(identity text, up_to_id bigint)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _marked bigint;
BEGIN
    UPDATE cb_notifications
    SET seen_at = now()
    WHERE cb_notifications.identity = cb_mark_seen.identity
      AND cb_notifications.id <= cb_mark_seen.up_to_id
      AND cb_notifications.seen_at IS NULL;

    GET DIAGNOSTICS _marked = ROW_COUNT;

    RETURN _marked;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- Fold durable-notification retention into the core cb_gc sweep (no new background
-- loop): drop notifications whose relevance window has passed. For notifications
-- relevance and retention coincide, so a single expires_at does double duty.
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
    _expired_notifications_deleted int := 0;
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

    -- Delete durable notifications whose relevance window has passed
    DELETE FROM cb_notifications
    WHERE expires_at IS NOT NULL AND expires_at <= now();
    GET DIAGNOSTICS _expired_notifications_deleted = ROW_COUNT;

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
        'expired_notifications_deleted', _expired_notifications_deleted,
        'task_runs_purged', _task_runs_purged,
        'flow_runs_purged', _flow_runs_purged
    );
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_mark_seen(text, bigint);
DROP FUNCTION IF EXISTS cb_notify_durable(text, text, text, text, timestamptz);

-- Restore cb_gc without the durable-notifications retention sweep.

-- +goose statementbegin
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

DROP INDEX IF EXISTS cb_notifications_collapse_key_idx;
DROP INDEX IF EXISTS cb_notifications_expires_at_idx;
DROP INDEX IF EXISTS cb_notifications_unseen_idx;
DROP TABLE IF EXISTS cb_notifications;
