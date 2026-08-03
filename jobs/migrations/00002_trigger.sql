-- +goose up

-- A trigger turns events into work: a filter over a stream, a job to run
-- for every matching message — declared as a row, delivered by the
-- module's tick, no consumer code deployed. The trigger owns the cursor
-- named after it on its stream; the cursor is the filter's home — source
-- text and compiled form together — and remembers how far delivery got.
--
-- This file is the module's one deliberate use of the stream schema: job
-- SQL calls stream public functions (cb_stream_define_cursor,
-- cb_stream_delete_cursor, cb_stream_read), never the reverse. PL/pgSQL
-- bodies are late-bound, so this migration installs cleanly without the
-- stream schema; cb_job_define_trigger and the tick check for it and raise
-- 'catbird: stream schema required' at use (SQLSTATE IRD03: a required
-- module is not installed).
CREATE TABLE cb_job_triggers (
    name text PRIMARY KEY CHECK (cb_valid_name(name)),
    -- No FK on stream: it lives in the other module's schema. No FK on
    -- job: same choice as cb_job_schedules — cb_job_define_trigger checks it,
    -- and a job deleted out from under a live trigger stalls delivery
    -- loudly instead of making the delete impossible.
    stream text NOT NULL,
    job text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose statementbegin
-- Declares a trigger whole: creating and updating are the same call, an
-- identical declaration writes nothing. Checks that the job is declared
-- and — through cb_stream_define_cursor — that the stream exists and the
-- filter compiles, so a broken trigger is refused here, not discovered by
-- the tick. The filter is stored on the cursor and nowhere else; this
-- function passes it through. The cursor's position is delivery state and
-- stays put on redeclare; start_pos, when given, sets it deliberately: 0
-- delivers the stream from the beginning, N from after N. When creating,
-- NULL starts at the tail — only messages published from now on deliver.
CREATE FUNCTION cb_job_define_trigger(
    name      text,
    stream    text,
    job       text,
    topic     text   DEFAULT NULL,
    condition text   DEFAULT NULL,
    start_pos bigint DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _old cb_job_triggers;
BEGIN
    IF to_regclass('cb_streams') IS NULL THEN
        RAISE EXCEPTION 'catbird: stream schema required (a trigger reads a stream; install the stream module first)'
            USING ERRCODE = 'IRD03';
    END IF;

    IF NOT cb_valid_name(cb_job_define_trigger.name) THEN
        RAISE EXCEPTION 'catbird: invalid trigger name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_job_define_trigger.name USING ERRCODE = 'IRD01';
    END IF;

    PERFORM 1 FROM cb_jobs j WHERE j.name = cb_job_define_trigger.job;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: job % not defined',
            cb_job_define_trigger.job USING ERRCODE = 'IRD02';
    END IF;

    -- The row lock serializes this declaration against the delivery tick
    -- and reads the old stream name for the move below.
    SELECT t.* INTO _old FROM cb_job_triggers t
    WHERE t.name = cb_job_define_trigger.name
    FOR UPDATE;

    -- A trigger moved to another stream leaves no cursor behind.
    IF FOUND AND _old.stream <> cb_job_define_trigger.stream THEN
        PERFORM cb_stream_delete_cursor(_old.stream, _old.name);
    END IF;

    INSERT INTO cb_job_triggers AS t (name, stream, job)
    VALUES (cb_job_define_trigger.name, cb_job_define_trigger.stream, cb_job_define_trigger.job)
    ON CONFLICT ON CONSTRAINT cb_job_triggers_pkey DO UPDATE
    SET stream = excluded.stream,
        job    = excluded.job
    -- an identical declaration writes nothing
    WHERE (t.stream, t.job) IS DISTINCT FROM (excluded.stream, excluded.job);

    -- The cursor's own change-guard writes nothing when the filter is
    -- unchanged and start_pos is not given, so calling it every time
    -- keeps the no-op property.
    PERFORM cb_stream_define_cursor(
        cb_job_define_trigger.stream,
        cb_job_define_trigger.name,
        cb_job_define_trigger.start_pos,
        cb_job_define_trigger.topic,
        cb_job_define_trigger.condition);
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Removes a trigger and its cursor. Reports whether one existed; deleting
-- a missing trigger is a no-op. Matches between the cursor and the
-- stream's head are gone with it — the trigger is its cursor's only reader.
CREATE FUNCTION cb_job_delete_trigger(name text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _old cb_job_triggers;
BEGIN
    DELETE FROM cb_job_triggers t
    WHERE t.name = cb_job_delete_trigger.name
    RETURNING t.* INTO _old;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    -- The guard covers a jobs-only install where trigger rows were
    -- restored or hand-written: delete what exists, never raise.
    IF to_regclass('cb_streams') IS NOT NULL THEN
        PERFORM cb_stream_delete_cursor(_old.stream, _old.name);
    END IF;
    RETURN true;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Delivers one trigger's next batch: read the matching messages after the
-- cursor, create one run per message, advance the cursor — one
-- transaction (cb_stream_read advances in the same call). A raise rolls
-- the whole batch back, cursor included, so creation is exactly-once: the
-- trigger stalls at its cursor, the tick logs the error every interval,
-- and delivery resumes when a define or a deploy fixes the cause. The run
-- key makes even a replayed batch idempotent. Returns how many messages
-- delivered.
CREATE FUNCTION cb_job_run_triggered(trigger text, batch_size int DEFAULT 100)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _trigger cb_job_triggers;
    _message record; -- a cb_stream_messages row; declared loosely so this
                     -- function never names the other module's row type
    _n int := 0;
BEGIN
    IF to_regclass('cb_streams') IS NULL THEN
        RAISE EXCEPTION 'catbird: stream schema required (a trigger reads a stream; install the stream module first)'
            USING ERRCODE = 'IRD03';
    END IF;

    -- One deliverer per trigger: a concurrent tick skips instead of
    -- queueing, and a redeclare waits for the in-flight batch to commit.
    SELECT t.* INTO _trigger FROM cb_job_triggers t
    WHERE t.name = cb_job_run_triggered.trigger
    FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    FOR _message IN
        SELECT * FROM cb_stream_read(_trigger.stream, _trigger.name,
                                     cb_job_run_triggered.batch_size)
    LOOP
        -- The payload is the run's input, exactly as published: a job has
        -- one input shape no matter who creates the run. The key — the
        -- trigger's name and the message's position — makes creation
        -- idempotent even across a cursor reset.
        PERFORM cb_job_run(
            _trigger.job,
            _message.payload,
            _trigger.name || ':' || _message.pos);
        _n := _n + 1;
    END LOOP;

    RETURN _n;
END; $$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_job_run_triggered(text, int);
DROP FUNCTION cb_job_delete_trigger(text);
DROP FUNCTION cb_job_define_trigger(text, text, text, text, text, bigint);
DROP TABLE cb_job_triggers;
