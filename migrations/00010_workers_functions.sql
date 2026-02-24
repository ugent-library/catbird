-- Worker management functions

-- +goose up

-- +goose statementbegin
-- cb_worker_started: Register a worker with the system
-- Creates worker record and registers task and step handlers
-- Parameters:
--   id: Worker UUID (unique identifier)
--   task_handlers: JSON array of {task_name: string} objects
--   step_handlers: JSON array of {flow_name: string, step_name: string} objects
-- Returns: void
CREATE OR REPLACE FUNCTION cb_worker_started(id uuid, task_handlers jsonb, step_handlers jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _handler jsonb;
    _task_name text;
    _flow_name text;
    _step_name text;
BEGIN
    INSERT INTO cb_workers (id) VALUES (cb_worker_started.id);

    -- Insert task handlers
    FOR _handler IN SELECT jsonb_array_elements(cb_worker_started.task_handlers)
    LOOP
        -- Validate task handler has required key
        IF NOT (_handler ? 'task_name') THEN
            RAISE EXCEPTION 'Task handler missing required key: task_name';
        END IF;

        _task_name := _handler->>'task_name';

        -- Validate value is non-empty string
        IF _task_name IS NULL OR _task_name = '' THEN
            RAISE EXCEPTION 'Task handler task_name must be a non-empty string';
        END IF;

        INSERT INTO cb_task_handlers (worker_id, task_name)
        VALUES (cb_worker_started.id, _task_name);
    END LOOP;

    -- Insert flow handlers
    FOR _handler IN SELECT jsonb_array_elements(cb_worker_started.step_handlers)
    LOOP
        -- Validate flow handler has required keys
        IF NOT (_handler ? 'flow_name' AND _handler ? 'step_name') THEN
            RAISE EXCEPTION 'Flow handler missing required keys: flow_name, step_name';
        END IF;

        _flow_name := _handler->>'flow_name';
        _step_name := _handler->>'step_name';

        -- Validate values are non-empty strings
        IF _flow_name IS NULL OR _flow_name = '' THEN
            RAISE EXCEPTION 'Flow handler flow_name must be a non-empty string';
        END IF;
        IF _step_name IS NULL OR _step_name = '' THEN
            RAISE EXCEPTION 'Flow handler step_name must be a non-empty string';
        END IF;

        INSERT INTO cb_step_handlers (worker_id, flow_name, step_name)
        VALUES (cb_worker_started.id, _flow_name, _step_name);
    END LOOP;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_worker_heartbeat: Update worker's last heartbeat timestamp and perform cleanup
-- Called periodically by workers to indicate they are still alive
-- Also opportunistically runs garbage collection via cb_gc()
-- Parameters:
--   id: Worker UUID
-- Returns: void
CREATE OR REPLACE FUNCTION cb_worker_heartbeat(id uuid)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cb_workers
    SET last_heartbeat_at = now()
    WHERE cb_workers.id = cb_worker_heartbeat.id;

    -- Opportunistically run garbage collection
    PERFORM cb_gc();
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_worker_info: Query information about all registered workers
-- Returns worker registration details and their assigned handlers
-- Columns:
--   - id: Worker UUID
--   - started_at: When the worker registered
--   - last_heartbeat_at: Timestamp of last heartbeat
--   - task_handlers: JSON array of assigned tasks
--   - step_handlers: JSON array of assigned flow steps
CREATE OR REPLACE VIEW cb_worker_info AS
    SELECT
    w.id,
    w.started_at,
    w.last_heartbeat_at,
    t.task_handlers AS task_handlers,
    s.step_handlers AS step_handlers
    FROM cb_workers w
    LEFT JOIN LATERAL (
    SELECT t.worker_id,
         json_agg(json_build_object('task_name', t.task_name) ORDER BY t.task_name) FILTER (WHERE t.worker_id IS NOT NULL) AS task_handlers
    FROM cb_task_handlers t
    WHERE t.worker_id = w.id
    GROUP BY worker_id
    ) t ON t.worker_id = w.id
    LEFT JOIN LATERAL (
    SELECT s.worker_id,
         json_agg(json_build_object('flow_name', s.flow_name, 'step_name', s.step_name) ORDER BY s.flow_name, s.step_name) FILTER (WHERE s.worker_id IS NOT NULL) AS step_handlers
    FROM cb_step_handlers s
    WHERE s.worker_id = w.id
    GROUP BY worker_id
    ) s ON s.worker_id = w.id
    ORDER BY w.started_at DESC;
-- +goose statementend

-- +goose down

DROP VIEW IF EXISTS cb_worker_info;
DROP FUNCTION IF EXISTS cb_worker_heartbeat(uuid);
DROP FUNCTION IF EXISTS cb_worker_started(uuid, jsonb, jsonb);
