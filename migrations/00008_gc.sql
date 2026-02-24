-- Garbage collection function

-- +goose up

-- +goose statementbegin
-- cb_gc: Garbage collection for stale workers and expired queues
-- Cleans up workers with no heartbeat for > 5 minutes and expired queues
-- Called automatically by worker heartbeats, but can also be invoked manually
-- for deployments without workers or for manual control
-- Returns: void
CREATE OR REPLACE FUNCTION cb_gc()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM cb_delete_queue(name)
    FROM cb_queues
    WHERE expires_at IS NOT NULL AND expires_at <= now();

    DELETE FROM cb_workers
    WHERE last_heartbeat_at < now() - INTERVAL '5 minutes';
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_gc();
