-- Garbage collection function

-- +goose up

-- +goose statementbegin
-- cb_purge_task_runs: Delete terminal task runs older than a given duration.
-- Terminal statuses: completed, failed, skipped, canceled.
-- Parameters:
--   name: Task name
--   older_than: Delete runs finished more than this duration ago
-- Returns: void
CREATE OR REPLACE FUNCTION cb_purge_task_runs(name text, older_than interval)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _table text := cb_table_name(cb_purge_task_runs.name, 't');
BEGIN
    IF to_regclass('public.' || _table) IS NULL THEN
        RETURN;
    END IF;
    EXECUTE format(
        'DELETE FROM %I
        WHERE status IN (''completed'', ''failed'', ''skipped'', ''canceled'')
          AND GREATEST(completed_at, failed_at, skipped_at, canceled_at) < $1',
        _table
    ) USING (now() - cb_purge_task_runs.older_than);
END;
$$;

-- cb_purge_flow_runs: Delete terminal flow runs older than a given duration.
-- Terminal statuses: completed, failed, canceled.
-- Step runs and map tasks are deleted automatically via ON DELETE CASCADE.
-- Parameters:
--   name: Flow name
--   older_than: Delete runs finished more than this duration ago
-- Returns: void
CREATE OR REPLACE FUNCTION cb_purge_flow_runs(name text, older_than interval)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _table text := cb_table_name(cb_purge_flow_runs.name, 'f');
BEGIN
    IF to_regclass('public.' || _table) IS NULL THEN
        RETURN;
    END IF;
    EXECUTE format(
        'DELETE FROM %I
        WHERE status IN (''completed'', ''failed'', ''canceled'')
          AND GREATEST(completed_at, failed_at, canceled_at) < $1',
        _table
    ) USING (now() - cb_purge_flow_runs.older_than);
END;
$$;

-- cb_gc: Garbage collection for stale workers, expired queues, and old task/flow runs
-- Cleans up workers with no heartbeat for > 5 minutes, expired queues, and terminal
-- task/flow runs that have exceeded their configured retention period
-- Called automatically by worker heartbeats, but can also be invoked manually
-- for deployments without workers or for manual control
-- Returns: void
CREATE OR REPLACE FUNCTION cb_gc()
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _rec record;
BEGIN
    -- Delete queues whose expiry has passed
    PERFORM cb_delete_queue(name)
    FROM cb_queues
    WHERE expires_at IS NOT NULL AND expires_at <= now();

    -- Delete workers with no heartbeat for more than 5 minutes
    DELETE FROM cb_workers
    WHERE last_heartbeat_at < now() - INTERVAL '5 minutes';

    FOR _rec IN
        SELECT name, retention_period
        FROM cb_tasks
        WHERE retention_period IS NOT NULL
    LOOP
        PERFORM cb_purge_task_runs(_rec.name, _rec.retention_period);
    END LOOP;

    FOR _rec IN
        SELECT name, retention_period
        FROM cb_flows
        WHERE retention_period IS NOT NULL
    LOOP
        PERFORM cb_purge_flow_runs(_rec.name, _rec.retention_period);
    END LOOP;
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_gc();
DROP FUNCTION IF EXISTS cb_purge_flow_runs(text, interval);
DROP FUNCTION IF EXISTS cb_purge_task_runs(text, interval);
