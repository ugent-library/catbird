-- Wire schema for SSE toolkit
-- Node heartbeat, presence tracking, and notification function

-- +goose up

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
-- cb_notify: Send a notification to Wire SSE subscribers via pg NOTIFY.
-- Uses the schema-qualified channel name and JSON payload.
-- Parameters:
--   topic: SSE subscription topic
--   event: SSE event name
--   data: Optional event data (text)
--   node_id: Optional origin Wire node ID (NULL from Client, set from Wire to prevent double delivery)
CREATE OR REPLACE FUNCTION cb_notify(
    topic text,
    event text,
    data text DEFAULT NULL,
    node_id uuid DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(
        current_schema || '.cb_wire',
        json_build_object(
            'node_id', cb_notify.node_id,
            'topic', cb_notify.topic,
            'event', cb_notify.event,
            'data', cb_notify.data
        )::text
    );
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
    _stale_workers_deleted int := 0;
    _stale_wire_nodes_deleted int := 0;
    _task_runs_purged int := 0;
    _flow_runs_purged int := 0;
    _purged int;
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

    -- Delete workers with no heartbeat for more than 5 minutes
    DELETE FROM cb_workers
    WHERE last_heartbeat_at < now() - INTERVAL '5 minutes';
    GET DIAGNOSTICS _stale_workers_deleted = ROW_COUNT;

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
        'stale_workers_deleted', _stale_workers_deleted,
        'stale_wire_nodes_deleted', _stale_wire_nodes_deleted,
        'task_runs_purged', _task_runs_purged,
        'flow_runs_purged', _flow_runs_purged
    );
END;
$$;
-- +goose statementend

-- +goose down

-- Restore cb_gc without Wire node cleanup
-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_gc()
RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    _rec record;
    _expired_queues_deleted int := 0;
    _stale_workers_deleted int := 0;
    _task_runs_purged int := 0;
    _flow_runs_purged int := 0;
    _purged int;
BEGIN
    FOR _rec IN
        SELECT name
        FROM cb_queues
        WHERE expires_at IS NOT NULL AND expires_at <= now()
    LOOP
        IF cb_delete_queue(_rec.name) THEN
            _expired_queues_deleted := _expired_queues_deleted + 1;
        END IF;
    END LOOP;

    DELETE FROM cb_workers
    WHERE last_heartbeat_at < now() - INTERVAL '5 minutes';
    GET DIAGNOSTICS _stale_workers_deleted = ROW_COUNT;

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
        'stale_workers_deleted', _stale_workers_deleted,
        'task_runs_purged', _task_runs_purged,
        'flow_runs_purged', _flow_runs_purged
    );
END;
$$;
-- +goose statementend

DROP FUNCTION IF EXISTS cb_notify(text, text, text, uuid);
DROP TABLE IF EXISTS cb_wire_presence CASCADE;
DROP TABLE IF EXISTS cb_wire_nodes CASCADE;
