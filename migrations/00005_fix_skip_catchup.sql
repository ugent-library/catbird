-- +goose up

-- cb_execute_due_task_schedules / cb_execute_due_flow_schedules previously
-- routed *every* due tick under the 'skip' policy through the no-enqueue
-- branch, so a schedule created with WithSkipCatchUp() never produced a run at
-- all -- not just on downtime recovery, but during normal operation too.
--
-- The catch-up policy is only meant to govern how *missed* ticks are handled on
-- recovery; an on-time tick must still enqueue under every policy. A tick is
-- genuinely stale (missed) only when a later tick has already come due, i.e.
-- cb_next_cron_tick(cron_spec, next_run_at) <= now(). Under 'skip' we suppress
-- the run only in that case; on-time ticks fall through to the enqueue branch.
--
-- Because the skip advance jumps next_run_at straight past now() in a single
-- step (GREATEST(next_run_at, now())), a multi-tick backlog only ever evaluates
-- the oldest tick, so recovery from N missed ticks still enqueues 0 runs -- the
-- documented behaviour. The stale-skip branch also no longer records a phantom
-- run: last_run_at / last_enqueued_at are left untouched when nothing is
-- enqueued (cb_advance_*_schedule set them unconditionally, masking the bug).

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
    v_cron_spec text;
    v_key text;
    v_policy text;
BEGIN
    FOR i IN 1..cb_execute_due_task_schedules.batch_size LOOP
        -- Claim one due schedule with FOR UPDATE SKIP LOCKED
        SELECT s.id, s.task_name, s.input, s.next_run_at, s.cron_spec, s.catch_up
        INTO v_id, v_task_name, v_input, v_scheduled_at, v_cron_spec, v_policy
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

        IF v_policy = 'skip' AND cb_next_cron_tick(v_cron_spec, v_scheduled_at) <= now() THEN
            -- Stale tick (a later tick is already due): skip the backlog and
            -- jump to the future without enqueuing or recording a run.
            UPDATE cb_task_schedules s
            SET
                next_run_at = cb_next_cron_tick(s.cron_spec, GREATEST(s.next_run_at, now())),
                updated_at = now()
            WHERE s.id = v_id;
        ELSE
            -- one / all / on-time skip: enqueue + advance (policy governs advance)
            v_key := 'schedule:' || EXTRACT(EPOCH FROM v_scheduled_at)::text;
            v_input := COALESCE(v_input, '{}'::jsonb);

            PERFORM cb_run_task(v_task_name, v_input, v_key);
            PERFORM cb_advance_task_schedule(v_id, v_policy);

            v_executed := v_executed + 1;
        END IF;
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
    v_cron_spec text;
    v_key text;
    v_policy text;
BEGIN
    FOR i IN 1..cb_execute_due_flow_schedules.batch_size LOOP
        -- Claim one due schedule with FOR UPDATE SKIP LOCKED
        SELECT s.id, s.flow_name, s.input, s.next_run_at, s.cron_spec, s.catch_up
        INTO v_id, v_flow_name, v_input, v_scheduled_at, v_cron_spec, v_policy
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

        IF v_policy = 'skip' AND cb_next_cron_tick(v_cron_spec, v_scheduled_at) <= now() THEN
            -- Stale tick (a later tick is already due): skip the backlog and
            -- jump to the future without enqueuing or recording a run.
            UPDATE cb_flow_schedules s
            SET
                next_run_at = cb_next_cron_tick(s.cron_spec, GREATEST(s.next_run_at, now())),
                updated_at = now()
            WHERE s.id = v_id;
        ELSE
            -- one / all / on-time skip: enqueue + advance (policy governs advance)
            v_key := 'schedule:' || EXTRACT(EPOCH FROM v_scheduled_at)::text;
            v_input := COALESCE(v_input, '{}'::jsonb);

            PERFORM cb_run_flow(v_flow_name, v_input, v_key);
            PERFORM cb_advance_flow_schedule(v_id, v_policy);

            v_executed := v_executed + 1;
        END IF;
    END LOOP;

    RETURN v_executed;
END;
$$;
-- +goose statementend

-- +goose down

-- Restore the pre-fix behaviour: skip always suppresses, advance always stamps.

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
    v_policy text;
BEGIN
    FOR i IN 1..cb_execute_due_task_schedules.batch_size LOOP
        -- Claim one due schedule with FOR UPDATE SKIP LOCKED
        SELECT s.id, s.task_name, s.input, s.next_run_at, s.catch_up
        INTO v_id, v_task_name, v_input, v_scheduled_at, v_policy
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

        IF v_policy = 'skip' THEN
            -- Skip: advance without enqueuing
            PERFORM cb_advance_task_schedule(v_id, v_policy);
        ELSE
            -- one / all: enqueue + advance (difference is in advance function)
            v_key := 'schedule:' || EXTRACT(EPOCH FROM v_scheduled_at)::text;
            v_input := COALESCE(v_input, '{}'::jsonb);

            PERFORM cb_run_task(v_task_name, v_input, v_key);
            PERFORM cb_advance_task_schedule(v_id, v_policy);

            v_executed := v_executed + 1;
        END IF;
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
    v_policy text;
BEGIN
    FOR i IN 1..cb_execute_due_flow_schedules.batch_size LOOP
        -- Claim one due schedule with FOR UPDATE SKIP LOCKED
        SELECT s.id, s.flow_name, s.input, s.next_run_at, s.catch_up
        INTO v_id, v_flow_name, v_input, v_scheduled_at, v_policy
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

        IF v_policy = 'skip' THEN
            -- Skip: advance without enqueuing
            PERFORM cb_advance_flow_schedule(v_id, v_policy);
        ELSE
            -- one / all: enqueue + advance (difference is in advance function)
            v_key := 'schedule:' || EXTRACT(EPOCH FROM v_scheduled_at)::text;
            v_input := COALESCE(v_input, '{}'::jsonb);

            PERFORM cb_run_flow(v_flow_name, v_input, v_key);
            PERFORM cb_advance_flow_schedule(v_id, v_policy);

            v_executed := v_executed + 1;
        END IF;
    END LOOP;

    RETURN v_executed;
END;
$$;
-- +goose statementend
