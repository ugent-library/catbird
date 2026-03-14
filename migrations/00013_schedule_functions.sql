-- Schedule management functions

-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_task_schedule(task_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb)
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
        enabled,
        updated_at
    )
    VALUES (
        cb_create_task_schedule.task_name,
        cb_create_task_schedule.cron_spec,
        cb_next_cron_tick(cb_create_task_schedule.cron_spec, now()),
        cb_create_task_schedule.input,
        true,
        now()
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_flow_schedule(flow_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb)
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
        enabled,
        updated_at
    )
    VALUES (
        cb_create_flow_schedule.flow_name,
        cb_create_flow_schedule.cron_spec,
        cb_next_cron_tick(cb_create_flow_schedule.cron_spec, now()),
        cb_create_flow_schedule.input,
        true,
        now()
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_advance_task_schedule(id bigint)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cb_task_schedules s
    SET
        next_run_at = cb_next_cron_tick(s.cron_spec, GREATEST(s.next_run_at, now())),
        last_run_at = s.next_run_at,
        last_enqueued_at = now(),
        updated_at = now()
    WHERE s.id = cb_advance_task_schedule.id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_advance_flow_schedule(id bigint)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cb_flow_schedules s
    SET
        next_run_at = cb_next_cron_tick(s.cron_spec, GREATEST(s.next_run_at, now())),
        last_run_at = s.next_run_at,
        last_enqueued_at = now(),
        updated_at = now()
    WHERE s.id = cb_advance_flow_schedule.id;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_execute_due_task_schedules(task_names text[], batch_size int DEFAULT 32)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_executed int := 0;
    v_id bigint;
    v_task_name text;
    v_input jsonb;
    v_scheduled_at timestamptz;
    v_key text;
BEGIN
    FOR i IN 1..cb_execute_due_task_schedules.batch_size LOOP
        -- Claim one due schedule with FOR UPDATE SKIP LOCKED
        SELECT s.id, s.task_name, s.input, s.next_run_at
        INTO v_id, v_task_name, v_input, v_scheduled_at
        FROM cb_task_schedules s
        WHERE
            s.enabled = true
            AND s.next_run_at <= now()
            AND s.task_name = ANY(cb_execute_due_task_schedules.task_names)
        ORDER BY s.next_run_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        -- No more due schedules in this batch
        EXIT WHEN v_id IS NULL;

        v_key := 'schedule:' || EXTRACT(EPOCH FROM v_scheduled_at)::text;
        v_input := COALESCE(v_input, '{}'::jsonb);

        -- Enqueue task
        PERFORM cb_run_task(v_task_name, v_input, v_key);

        -- Advance schedule to next tick
        PERFORM cb_advance_task_schedule(v_id);

        v_executed := v_executed + 1;
    END LOOP;

    RETURN v_executed;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_execute_due_flow_schedules(flow_names text[], batch_size int DEFAULT 32)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_executed int := 0;
    v_id bigint;
    v_flow_name text;
    v_input jsonb;
    v_scheduled_at timestamptz;
    v_key text;
BEGIN
    FOR i IN 1..cb_execute_due_flow_schedules.batch_size LOOP
        -- Claim one due schedule with FOR UPDATE SKIP LOCKED
        SELECT s.id, s.flow_name, s.input, s.next_run_at
        INTO v_id, v_flow_name, v_input, v_scheduled_at
        FROM cb_flow_schedules s
        WHERE
            s.enabled = true
            AND s.next_run_at <= now()
            AND s.flow_name = ANY(cb_execute_due_flow_schedules.flow_names)
        ORDER BY s.next_run_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        -- No more due schedules in this batch
        EXIT WHEN v_id IS NULL;

        v_key := 'schedule:' || EXTRACT(EPOCH FROM v_scheduled_at)::text;
        v_input := COALESCE(v_input, '{}'::jsonb);

        -- Enqueue flow
        PERFORM cb_run_flow(v_flow_name, v_input, v_key);

        -- Advance schedule to next tick
        PERFORM cb_advance_flow_schedule(v_id);

        v_executed := v_executed + 1;
    END LOOP;

    RETURN v_executed;
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_execute_due_flow_schedules(text[], int);
DROP FUNCTION IF EXISTS cb_execute_due_task_schedules(text[], int);
DROP FUNCTION IF EXISTS cb_advance_flow_schedule(bigint);
DROP FUNCTION IF EXISTS cb_advance_task_schedule(bigint);
DROP FUNCTION IF EXISTS cb_create_flow_schedule(text, text);
DROP FUNCTION IF EXISTS cb_create_task_schedule(text, text);
