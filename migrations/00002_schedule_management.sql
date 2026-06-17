-- +goose up

-- cb_create_task_schedule / cb_create_flow_schedule previously returned early
-- when a schedule already existed, silently dropping cron_spec/input/catch_up
-- changes on subsequent calls. Bring them in line with cb_create_flow by
-- upserting on conflict. next_run_at is only recomputed when the cron spec
-- actually changes, so repeat calls with the same spec don't discard a
-- catch-up backlog (which deliberately keeps next_run_at in the past).
--
-- Also add cb_delete_task_schedule / cb_delete_flow_schedule so callers can
-- remove a schedule explicitly. Schedules are poll-based (no NOTIFY), so unlike
-- cb_delete_queue/task/flow these emit no event: the next poll simply skips the
-- removed row.

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_task_schedule(task_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb, catch_up text DEFAULT 'one')
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_task_schedules:' || cb_create_task_schedule.task_name));

    INSERT INTO cb_task_schedules (
        task_name,
        cron_spec,
        next_run_at,
        input,
        catch_up,
        enabled,
        updated_at
    )
    VALUES (
        cb_create_task_schedule.task_name,
        cb_create_task_schedule.cron_spec,
        cb_next_cron_tick(cb_create_task_schedule.cron_spec, now()),
        cb_create_task_schedule.input,
        cb_create_task_schedule.catch_up,
        true,
        now()
    )
    ON CONFLICT ON CONSTRAINT cb_task_schedules_task_name_key DO UPDATE
    SET
        cron_spec = EXCLUDED.cron_spec,
        input = EXCLUDED.input,
        catch_up = EXCLUDED.catch_up,
        next_run_at = CASE
            WHEN cb_task_schedules.cron_spec IS DISTINCT FROM EXCLUDED.cron_spec
            THEN cb_next_cron_tick(EXCLUDED.cron_spec, now())
            ELSE cb_task_schedules.next_run_at
        END,
        updated_at = now();
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_flow_schedule(flow_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb, catch_up text DEFAULT 'one')
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_flow_schedules:' || cb_create_flow_schedule.flow_name));

    INSERT INTO cb_flow_schedules (
        flow_name,
        cron_spec,
        next_run_at,
        input,
        catch_up,
        enabled,
        updated_at
    )
    VALUES (
        cb_create_flow_schedule.flow_name,
        cb_create_flow_schedule.cron_spec,
        cb_next_cron_tick(cb_create_flow_schedule.cron_spec, now()),
        cb_create_flow_schedule.input,
        cb_create_flow_schedule.catch_up,
        true,
        now()
    )
    ON CONFLICT ON CONSTRAINT cb_flow_schedules_flow_name_key DO UPDATE
    SET
        cron_spec = EXCLUDED.cron_spec,
        input = EXCLUDED.input,
        catch_up = EXCLUDED.catch_up,
        next_run_at = CASE
            WHEN cb_flow_schedules.cron_spec IS DISTINCT FROM EXCLUDED.cron_spec
            THEN cb_next_cron_tick(EXCLUDED.cron_spec, now())
            ELSE cb_flow_schedules.next_run_at
        END,
        updated_at = now();
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_delete_task_schedule(task_name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _res boolean;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_task_schedules:' || cb_delete_task_schedule.task_name));

    DELETE FROM cb_task_schedules s
    WHERE s.task_name = cb_delete_task_schedule.task_name
    RETURNING true
    INTO _res;

    RETURN coalesce(_res, false);
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_delete_flow_schedule(flow_name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _res boolean;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_flow_schedules:' || cb_delete_flow_schedule.flow_name));

    DELETE FROM cb_flow_schedules s
    WHERE s.flow_name = cb_delete_flow_schedule.flow_name
    RETURNING true
    INTO _res;

    RETURN coalesce(_res, false);
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_delete_flow_schedule(text);
DROP FUNCTION IF EXISTS cb_delete_task_schedule(text);

-- Restore the original early-return behaviour for the create functions.

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_task_schedule(task_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb, catch_up text DEFAULT 'one')
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_task_schedules:' || cb_create_task_schedule.task_name));

    -- Return early if schedule already exists
    IF EXISTS (
        SELECT 1
        FROM cb_task_schedules s
        WHERE s.task_name = cb_create_task_schedule.task_name
    ) THEN
        RETURN;
    END IF;

    INSERT INTO cb_task_schedules (
        task_name,
        cron_spec,
        next_run_at,
        input,
        catch_up,
        enabled,
        updated_at
    )
    VALUES (
        cb_create_task_schedule.task_name,
        cb_create_task_schedule.cron_spec,
        cb_next_cron_tick(cb_create_task_schedule.cron_spec, now()),
        cb_create_task_schedule.input,
        cb_create_task_schedule.catch_up,
        true,
        now()
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_flow_schedule(flow_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb, catch_up text DEFAULT 'one')
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_flow_schedules:' || cb_create_flow_schedule.flow_name));

    -- Return early if schedule already exists
    IF EXISTS (
        SELECT 1
        FROM cb_flow_schedules s
        WHERE s.flow_name = cb_create_flow_schedule.flow_name
    ) THEN
        RETURN;
    END IF;

    INSERT INTO cb_flow_schedules (
        flow_name,
        cron_spec,
        next_run_at,
        input,
        catch_up,
        enabled,
        updated_at
    )
    VALUES (
        cb_create_flow_schedule.flow_name,
        cb_create_flow_schedule.cron_spec,
        cb_next_cron_tick(cb_create_flow_schedule.cron_spec, now()),
        cb_create_flow_schedule.input,
        cb_create_flow_schedule.catch_up,
        true,
        now()
    );
END;
$$;
-- +goose statementend
