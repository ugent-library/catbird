-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_gc()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
BEGIN
    PERFORM cb_delete_queue(name)
    FROM cb_queues
    WHERE delete_at IS NOT NULL AND delete_at <= now();

    DELETE FROM cb_workers
    WHERE last_heartbeat_at < now() - INTERVAL '5 minutes';
END
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_gc();
