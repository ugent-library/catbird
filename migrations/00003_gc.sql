-- Garbage collection routines for expired queues and stale worker heartbeats

-- CONCURRENCY AUDIT:
-- cb_gc():
--   - Called by workers every 5 minutes for maintenance (not hot path)
--   - cb_delete_queue(): Uses advisory lock internally via callback - SAFE
--   - DELETE FROM cb_workers: Simple DELETE by timestamp, no locks needed - SAFE
--   - Safe for concurrent GC runs (idempotent, scans by expires_at and timestamp) - SAFE
-- GC operations are idempotent and cause no race conditions

-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_gc()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
BEGIN
    PERFORM cb_delete_queue(name)
    FROM cb_queues
    WHERE expires_at IS NOT NULL AND expires_at <= now();

    DELETE FROM cb_workers
    WHERE last_heartbeat_at < now() - INTERVAL '5 minutes';
END
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_gc();
