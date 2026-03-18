-- Clear all runs for a task or flow regardless of status.

-- +goose up

-- +goose statementbegin
-- cb_clear_task_runs: Delete all task runs regardless of status.
-- Use with caution — in-flight work will be lost.
-- Parameters:
--   name: Task name
-- Returns: int - deleted row count
CREATE OR REPLACE FUNCTION cb_clear_task_runs(name text)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _table text := cb_table_name(cb_clear_task_runs.name, 't');
    _deleted int := 0;
BEGIN
    IF to_regclass(_table) IS NULL THEN
        RETURN 0;
    END IF;
    EXECUTE format('DELETE FROM %I', _table);
    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- cb_clear_flow_runs: Delete all flow runs regardless of status.
-- Step runs and map tasks are deleted automatically via ON DELETE CASCADE.
-- Use with caution — in-flight work will be lost.
-- Parameters:
--   name: Flow name
-- Returns: int - deleted row count
CREATE OR REPLACE FUNCTION cb_clear_flow_runs(name text)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _table text := cb_table_name(cb_clear_flow_runs.name, 'f');
    _deleted int := 0;
BEGIN
    IF to_regclass(_table) IS NULL THEN
        RETURN 0;
    END IF;
    EXECUTE format('DELETE FROM %I', _table);
    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_clear_flow_runs(text);
DROP FUNCTION IF EXISTS cb_clear_task_runs(text);
