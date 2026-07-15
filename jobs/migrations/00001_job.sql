-- +goose up

-- The job engine's schema (docs/plan/03-job.md). Seven tables, no edges:
-- a job is the declared, named thing; a run is one instance of a job — the
-- group and the record; a step is a unit of owed work inside a run, running
-- a declared job; an attempt is one execution of a step. The step row that
-- records the work is the unit a worker claims. Uses the kernel's SQL unit
-- (cb_valid_name, cb_forever, cb_backoff), applied before this file.

-- Dispatch is two independent gates combined into one word: all_done waits
-- for the run's current phase to drain, signal waits for a payload. The
-- words are the same in the Go options, the spawns JSON and the column.
CREATE TYPE cb_job_dispatch AS ENUM ('immediate', 'all_done', 'signal', 'all_done_signal');
CREATE TYPE cb_job_catch_up_policy AS ENUM ('skip', 'all');

-- One row per declared job: the authority spawns are validated against and
-- the routing map in one. on_fail and retention matter when the job is a
-- run's birth job; on a job only ever spawned mid-run they are inert config.
CREATE TABLE cb_jobs (
    name text PRIMARY KEY CHECK (cb_valid_name(name)),
    queue text,   -- pool the job routes to; NULL means 'default'
    on_fail text, -- job spawned at give-up; cb_job_define checks it names a declared job
    retention interval NOT NULL DEFAULT interval '30 days'
        CHECK (retention = cb_forever() OR retention > interval '0')
);

-- The retry and claim terms per pool, written whole by jobs.Define. The
-- 'default' row is seeded below so a bare install works; every other pool
-- is declared, and 'default' itself is redeclarable like any pool.
CREATE TABLE cb_job_queues (
    name text PRIMARY KEY CHECK (cb_valid_name(name)),
    claim_ttl interval NOT NULL CHECK (claim_ttl > interval '0'),
    claim_batch_size int NOT NULL CHECK (claim_batch_size > 0),
    max_attempts int NOT NULL CHECK (max_attempts > 0),
    backoff_kind cb_backoff_kind NOT NULL,
    backoff_base interval NOT NULL,
    backoff_max interval NOT NULL
);

INSERT INTO cb_job_queues
VALUES ('default', interval '30 seconds', 10, 3, 'full_jitter', interval '1 second', interval '1 minute');

-- Interval schedules for scheduled runs. The module's tick calls
-- cb_job_run(job, input) for a due row and re-arms next_at in one
-- transaction: exactly-once per slot by construction.
CREATE TABLE cb_job_schedules (
    name text PRIMARY KEY CHECK (cb_valid_name(name)),
    job text NOT NULL,
    every interval NOT NULL CHECK (
        every > interval '0'
        AND extract(day   FROM every) = 0   -- fixed durations only: no day/month/year
        AND extract(month FROM every) = 0   -- component, so the re-arm epoch math
        AND extract(year  FROM every) = 0   -- is exact
    ),
    catch_up cb_job_catch_up_policy NOT NULL DEFAULT 'skip',
    input jsonb,
    next_at timestamptz NOT NULL -- when this schedule fires next
);

CREATE INDEX ON cb_job_schedules (next_at);

-- A run is one instance of a job: the group of steps and the record.
-- 'failing' means the outcome is already decided (failed) and only the
-- on_fail chain may still execute. steps_remaining counts the steps the
-- run still owes: queued, started, or waiting on a signal — a step with an
-- unsatisfied all_done gate stays outside the count until it dispatches.
-- next_step_id hands out per-run step ids; a plain counter is enough
-- because every engine call that creates steps already holds the run lock.
CREATE TABLE cb_job_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job text NOT NULL, -- the birth job; the run's on_fail and retention are read from its definition
    key text,          -- dedup point and app-key lookup in one, scoped per birth job
    status text NOT NULL
        CHECK (status IN ('running', 'failing', 'completed', 'failed', 'canceled')),
    input jsonb,
    output jsonb,
    error text,
    steps_remaining int NOT NULL,
    next_step_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    CONSTRAINT cb_job_runs_job_key_key UNIQUE (job, key)
);

-- The work table: one timestamp column, claimable_at, carries visibility,
-- lease and backoff. Replay identity is the plain tuple (run_id,
-- parent_step_id, name, ordinal), where ordinal is the spawn's zero-based
-- index in its parent's Plan buffer — deterministic across replays because
-- the buffer is replayed whole. parent_step_id is 0 for the run's first
-- step, the one running the birth job. attempt counts starts: the whole
-- retry budget, one number.
CREATE TABLE cb_job_steps (
    run_id bigint NOT NULL REFERENCES cb_job_runs (id),
    id bigint NOT NULL,
    -- queue and name are denormalized from the job's definition at spawn so
    -- the claim query joins nothing. Deliberately no FK on either: an FK to
    -- a shared hot config row takes a KEY SHARE lock per spawn (multixact
    -- churn at volume), and cb_job_complete already validates both with
    -- better errors.
    queue text NOT NULL,
    name text NOT NULL, -- the declared job this step runs
    parent_step_id bigint NOT NULL,
    ordinal int NOT NULL,
    status text NOT NULL
        CHECK (status IN ('waiting', 'queued', 'started', 'completed', 'failed', 'canceled')),
    dispatch cb_job_dispatch NOT NULL,
    input jsonb,
    signal jsonb, -- the satisfied payload of a signal-gated step, NULL until then
    output jsonb,
    error text,
    attempt int NOT NULL DEFAULT 0,
    claimable_at timestamptz, -- when the step may next be handed out; a claim's lease deadline
    worker text,
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    PRIMARY KEY (run_id, id),
    CONSTRAINT cb_job_steps_identity_key UNIQUE (run_id, parent_step_id, name, ordinal)
);

-- The hot claim index holds only the working set: terminal rows leave it.
CREATE INDEX cb_job_steps_claim_idx ON cb_job_steps (queue, claimable_at)
    WHERE status IN ('queued', 'started');

-- A signal-gated step's name must be unique among the run's unresolved
-- steps, so cb_job_signal can address it by name.
CREATE UNIQUE INDEX cb_job_steps_signal_name_idx ON cb_job_steps (run_id, name)
    WHERE dispatch IN ('signal', 'all_done_signal')
      AND status NOT IN ('completed', 'failed', 'canceled');

-- One row per start of a step, kept when the run finishes. A NULL status
-- means the attempt never reported a result: the worker crashed, or the
-- step was handed to another worker and the outcome of this start no
-- longer counts.
CREATE TABLE cb_job_attempts (
    run_id bigint NOT NULL,
    step_id bigint NOT NULL,
    attempt int NOT NULL,
    worker text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text CHECK (status IN ('completed', 'failed')),
    error text,
    PRIMARY KEY (run_id, step_id, attempt),
    FOREIGN KEY (run_id, step_id) REFERENCES cb_job_steps (run_id, id)
);

-- The signal buffer, one slot per name: a second signal for a name nobody
-- consumed yet overwrites the slot; matching consumes it — deletes the row.
CREATE TABLE cb_job_signals (
    run_id bigint NOT NULL REFERENCES cb_job_runs (id),
    name text NOT NULL,
    payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, name)
);

-- The three FKs above tie children to their run. They are cheap: each is
-- checked only at insert, inside a transaction that already holds the run
-- lock, and the key columns never change afterward — the hot
-- claim/start/complete UPDATEs pay nothing. They also make the janitor's
-- children-first delete order a constraint instead of a convention.

-- The engine functions. Rules shared by all of them:
--
-- Checks. Every function that changes a run locks the run row first (FOR
-- UPDATE) and checks its status: only 'running' and 'failing' runs accept
-- changes. cb_job_complete and cb_job_fail also check that the step is
-- 'started' and that the caller's attempt equals the step's. When a check
-- fails, the function returns false (or nothing) and changes nothing: the
-- caller is acting on outdated information and its late call must not
-- disturb what already happened. Only cb_job_start increments attempt.
--
-- Locks. The order is: run row first, then step rows. Functions that
-- update several step rows lock them in (run_id, id) order through an
-- ordered subselect. cb_job_claim and cb_job_release lock step rows only
-- and never the run row; claim also skips rows that are already locked.
-- One shared order plus skip-locked means engine calls cannot deadlock
-- each other.
--
-- Notifications. cbq_<queue> fires when a step becomes claimable; the
-- payload is the step's claimable_at as an RFC 3339 UTC timestamp.
-- cbj_<job> fires when a run reaches a terminal status; the payload is
-- '<run_id>:<status>'. Channel names are prefixed with the current
-- schema. Channels and payload format are what worker_notifier.go
-- already parses.
--
-- Terminal steps. worker and claimable_at are set to NULL when a step
-- reaches 'completed', 'failed' or 'canceled'. The claim index covers
-- only 'queued' and 'started' rows, so finished steps drop out of it.
--
-- Errors. SQLSTATE IRD01: the call is invalid. IRD02: a named object
-- does not exist.

-- +goose statementbegin
-- Declares a pool and all its terms in one call. An argument that is not
-- given gets the stock value (the same values the migration seeds for
-- 'default'); it never means "keep the current value". Declaring the same
-- terms again writes nothing.
CREATE FUNCTION cb_job_define_queue(
    queue            text,
    claim_ttl        interval        DEFAULT NULL,
    claim_batch_size int             DEFAULT NULL,
    max_attempts     int             DEFAULT NULL,
    backoff_kind     cb_backoff_kind DEFAULT NULL,
    backoff_base     interval        DEFAULT NULL,
    backoff_max      interval        DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF NOT cb_valid_name(cb_job_define_queue.queue) THEN
        RAISE EXCEPTION 'catbird: invalid queue name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_job_define_queue.queue USING ERRCODE = 'IRD01';
    END IF;

    INSERT INTO cb_job_queues AS q
        (name, claim_ttl, claim_batch_size, max_attempts, backoff_kind, backoff_base, backoff_max)
    VALUES (
        cb_job_define_queue.queue,
        coalesce(cb_job_define_queue.claim_ttl, interval '30 seconds'),
        coalesce(cb_job_define_queue.claim_batch_size, 10),
        coalesce(cb_job_define_queue.max_attempts, 3),
        coalesce(cb_job_define_queue.backoff_kind, 'full_jitter'),
        coalesce(cb_job_define_queue.backoff_base, interval '1 second'),
        coalesce(cb_job_define_queue.backoff_max, interval '1 minute')
    )
    ON CONFLICT ON CONSTRAINT cb_job_queues_pkey DO UPDATE
    SET claim_ttl        = excluded.claim_ttl,
        claim_batch_size = excluded.claim_batch_size,
        max_attempts     = excluded.max_attempts,
        backoff_kind     = excluded.backoff_kind,
        backoff_base     = excluded.backoff_base,
        backoff_max      = excluded.backoff_max
    WHERE (q.claim_ttl, q.claim_batch_size, q.max_attempts,
           q.backoff_kind, q.backoff_base, q.backoff_max)
          IS DISTINCT FROM
          (excluded.claim_ttl, excluded.claim_batch_size, excluded.max_attempts,
           excluded.backoff_kind, excluded.backoff_base, excluded.backoff_max);
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Declares a job and all its config in one call: queue NULL routes the
-- job to 'default', retention NULL means the stock 30 days. queue must
-- name a declared pool and on_fail a declared job; checking this at
-- declaration time turns a typo into a deploy error instead of a runtime
-- failure. Both checks run after the insert (an error rolls the insert
-- back anyway), so a job may name itself as its own on_fail.
CREATE FUNCTION cb_job_define(
    job       text,
    queue     text     DEFAULT NULL,
    on_fail   text     DEFAULT NULL,
    retention interval DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF NOT cb_valid_name(cb_job_define.job) THEN
        RAISE EXCEPTION 'catbird: invalid job name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_job_define.job USING ERRCODE = 'IRD01';
    END IF;

    IF cb_job_define.retention IS NOT NULL
       AND cb_job_define.retention <> cb_forever()
       AND cb_job_define.retention <= interval '0' THEN
        RAISE EXCEPTION 'catbird: retention must be positive, or cb_forever()'
            USING ERRCODE = 'IRD01';
    END IF;

    INSERT INTO cb_jobs AS j (name, queue, on_fail, retention)
    VALUES (
        cb_job_define.job,
        cb_job_define.queue,
        cb_job_define.on_fail,
        coalesce(cb_job_define.retention, interval '30 days')
    )
    ON CONFLICT ON CONSTRAINT cb_jobs_pkey DO UPDATE
    SET queue     = excluded.queue,
        on_fail   = excluded.on_fail,
        retention = excluded.retention
    WHERE (j.queue, j.on_fail, j.retention)
          IS DISTINCT FROM (excluded.queue, excluded.on_fail, excluded.retention);

    IF cb_job_define.queue IS NOT NULL AND NOT EXISTS
       (SELECT 1 FROM cb_job_queues q WHERE q.name = cb_job_define.queue) THEN
        RAISE EXCEPTION 'catbird: queue % not defined', cb_job_define.queue USING ERRCODE = 'IRD02';
    END IF;

    IF cb_job_define.on_fail IS NOT NULL AND NOT EXISTS
       (SELECT 1 FROM cb_jobs j WHERE j.name = cb_job_define.on_fail) THEN
        RAISE EXCEPTION 'catbird: on_fail job % not defined', cb_job_define.on_fail USING ERRCODE = 'IRD02';
    END IF;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Declares a schedule in one call. next_at is not declared config, the
-- engine manages it: a redeclaration keeps the firing phase, a changed
-- cadence re-anchors it to now() + every, and start_at sets it directly.
-- input is stored exactly as given; NULL is a valid job input.
CREATE FUNCTION cb_job_define_schedule(
    name     text,
    job      text,
    every    interval,
    catch_up cb_job_catch_up_policy DEFAULT NULL, -- NULL means 'skip'
    input    jsonb       DEFAULT NULL,
    start_at timestamptz DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF NOT cb_valid_name(cb_job_define_schedule.name) THEN
        RAISE EXCEPTION 'catbird: invalid schedule name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_job_define_schedule.name USING ERRCODE = 'IRD01';
    END IF;

    IF cb_job_define_schedule.every IS NULL THEN
        RAISE EXCEPTION 'catbird: schedule % needs an interval',
            cb_job_define_schedule.name USING ERRCODE = 'IRD01';
    END IF;

    IF extract(day   FROM cb_job_define_schedule.every) <> 0
    OR extract(month FROM cb_job_define_schedule.every) <> 0
    OR extract(year  FROM cb_job_define_schedule.every) <> 0 THEN
        RAISE EXCEPTION 'catbird: schedule interval must be hours or less (got %); days, months and years need cron',
            cb_job_define_schedule.every USING ERRCODE = 'IRD01';
    END IF;

    PERFORM 1 FROM cb_jobs j WHERE j.name = cb_job_define_schedule.job;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: job % not defined', cb_job_define_schedule.job USING ERRCODE = 'IRD02';
    END IF;

    INSERT INTO cb_job_schedules AS sc (name, job, every, catch_up, input, next_at)
    VALUES (
        cb_job_define_schedule.name,
        cb_job_define_schedule.job,
        cb_job_define_schedule.every,
        coalesce(cb_job_define_schedule.catch_up, 'skip'),
        cb_job_define_schedule.input,
        coalesce(cb_job_define_schedule.start_at,
                 clock_timestamp() + cb_job_define_schedule.every)
    )
    ON CONFLICT ON CONSTRAINT cb_job_schedules_pkey DO UPDATE
    SET job      = excluded.job,
        every    = excluded.every,
        catch_up = excluded.catch_up,
        input    = excluded.input,
        next_at  = CASE
            WHEN cb_job_define_schedule.start_at IS NOT NULL
                THEN cb_job_define_schedule.start_at
            WHEN sc.every IS DISTINCT FROM excluded.every
                THEN clock_timestamp() + excluded.every
            ELSE sc.next_at
        END
    -- an identical declaration writes nothing
    WHERE (sc.job, sc.every, sc.catch_up, sc.input)
          IS DISTINCT FROM (excluded.job, excluded.every, excluded.catch_up, excluded.input)
       OR cb_job_define_schedule.start_at IS NOT NULL;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_job_delete_schedule(name text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _found boolean;
BEGIN
    DELETE FROM cb_job_schedules sc
    WHERE sc.name = cb_job_delete_schedule.name
    RETURNING true INTO _found;

    RETURN coalesce(_found, false);
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Creates a run: the run row plus its first step, queued on the job's
-- pool, in one statement. Works on any connection, so an application can
-- create a run inside its own transaction. key deduplicates: when a run
-- of this job with this key already exists, live or finished, its id is
-- returned with existing = true and nothing is inserted. The ON CONFLICT
-- DO UPDATE WHERE false + UNION ALL construction returns the existing
-- row's id from the same statement; do not simplify it.
CREATE FUNCTION cb_job_run(
    job   text,
    input jsonb    DEFAULT NULL,
    key   text     DEFAULT NULL,
    delay interval DEFAULT NULL,

    OUT run_id   bigint,
    OUT existing boolean
)
LANGUAGE plpgsql AS $$
DECLARE
    _job cb_jobs;
    _queue text;
    _claimable_at timestamptz;
BEGIN
    SELECT j.* INTO _job FROM cb_jobs j WHERE j.name = cb_job_run.job;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: job % not defined', cb_job_run.job USING ERRCODE = 'IRD02';
    END IF;
    _queue := coalesce(_job.queue, 'default');
    _claimable_at := clock_timestamp() + coalesce(cb_job_run.delay, interval '0');

    WITH _run AS (
        INSERT INTO cb_job_runs (job, key, status, input, steps_remaining, next_step_id)
        VALUES (cb_job_run.job, cb_job_run.key, 'running', cb_job_run.input, 1, 2)
        ON CONFLICT ON CONSTRAINT cb_job_runs_job_key_key
            DO UPDATE SET job = excluded.job WHERE false
        RETURNING id
    ),
    _step AS (
        INSERT INTO cb_job_steps
            (run_id, id, queue, name, parent_step_id, ordinal, status, dispatch, input, claimable_at)
        SELECT r.id, 1, _queue, cb_job_run.job, 0, 0, 'queued', 'immediate',
               cb_job_run.input, _claimable_at
        FROM _run r
    )
    SELECT result.id, result.existing INTO run_id, existing
    FROM (
        SELECT r.id, false AS existing FROM _run r
        UNION ALL
        SELECT r.id, true FROM cb_job_runs r
        WHERE r.job = cb_job_run.job AND r.key = cb_job_run.key
        LIMIT 1
    ) result;

    IF NOT existing THEN
        PERFORM pg_notify(current_schema || '.cbq_' || _queue,
            to_char(_claimable_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
    END IF;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks a step permanently failed and decides how the run ends. Called by
-- cb_job_start and cb_job_fail when a step's attempts are used up; the
-- caller holds the run lock and passes the run and step rows it read
-- under that lock. All other unfinished steps of the run are canceled.
-- Then:
--   * If the run is 'running' and its birth job declares an on_fail job,
--     the run turns 'failing' and one new step is created to run that
--     job, with input {job, error, input} describing the failed step. The
--     run ends 'failed' when that step, and whatever it spawns, is done.
--   * Otherwise the run ends 'failed' now. A 'failing' run never gets a
--     second on_fail step, and it keeps its first error; the on_fail
--     step's own failure is recorded on its step and attempt rows.
CREATE FUNCTION _cb_job_give_up(run cb_job_runs, step cb_job_steps, error text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _run_id bigint := _cb_job_give_up.run.id;
    _step_id bigint := _cb_job_give_up.step.id;
    _on_fail cb_jobs;
    _spawn_id bigint := _cb_job_give_up.run.next_step_id;
    _queue text;
    _claimable_at timestamptz;
BEGIN
    UPDATE cb_job_steps s
    SET status = 'failed',
        error = _cb_job_give_up.error,
        worker = NULL,
        claimable_at = NULL,
        finished_at = clock_timestamp()
    WHERE s.run_id = _run_id AND s.id = _step_id;

    -- "step" would be ambiguous here (it is also this function's parameter)
    UPDATE cb_job_steps other
    SET status = 'canceled',
        worker = NULL,
        claimable_at = NULL,
        finished_at = clock_timestamp()
    FROM (
        -- locked in (run_id, id) order; see the lock rules above
        SELECT s.run_id, s.id FROM cb_job_steps s
        WHERE s.run_id = _run_id
          AND s.id <> _step_id
          AND s.status IN ('waiting', 'queued', 'started')
        ORDER BY s.run_id, s.id
        FOR UPDATE
    ) locked
    WHERE (other.run_id, other.id) = (locked.run_id, locked.id);

    ---- A 'running' run whose birth job declares on_fail gets a cleanup step. ----
    IF _cb_job_give_up.run.status = 'running' THEN
        SELECT o.* INTO _on_fail
        FROM cb_jobs b
        JOIN cb_jobs o ON o.name = b.on_fail
        WHERE b.name = _cb_job_give_up.run.job;

        IF FOUND THEN
            _queue := coalesce(_on_fail.queue, 'default');
            _claimable_at := clock_timestamp();

            UPDATE cb_job_runs r
            SET status = 'failing',
                error = _cb_job_give_up.error,
                steps_remaining = 1,
                next_step_id = _spawn_id + 1
            WHERE r.id = _run_id;

            INSERT INTO cb_job_steps
                (run_id, id, queue, name, parent_step_id, ordinal, status, dispatch, input, claimable_at)
            VALUES (
                _run_id, _spawn_id, _queue, _on_fail.name, _step_id, 0, 'queued', 'immediate',
                jsonb_build_object(
                    'job',   _cb_job_give_up.step.name,
                    'error', _cb_job_give_up.error,
                    'input', _cb_job_give_up.step.input),
                _claimable_at
            );

            PERFORM pg_notify(current_schema || '.cbq_' || _queue,
                to_char(_claimable_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));

            RETURN;
        END IF;
    END IF;

    ---- No on_fail step to wait for: the run ends 'failed' now. ----
    UPDATE cb_job_runs r
    SET status = 'failed',
        error = coalesce(r.error, _cb_job_give_up.error),
        steps_remaining = 0,
        finished_at = clock_timestamp()
    WHERE r.id = _run_id;

    PERFORM pg_notify(current_schema || '.cbj_' || _cb_job_give_up.run.job, _run_id || ':failed');
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Hands out steps that are ready to run. Per pool in queues: up to
-- claim_batch_size step rows whose claimable_at has passed, oldest first,
-- each stamped with the caller's worker name and leased until now() +
-- claim_ttl (the lease lives in claimable_at). Returns the handed-out
-- steps; lease_at is when the lease runs out, and cb_job_extend moves it
-- forward while the handler runs.
--
-- What happens to a ready row depends on its state:
--   * 'queued': handed out. A worker stamp still on the row means a
--     worker claimed it earlier and died before calling start; no attempt
--     was spent, so the row is simply handed out again and the stamp
--     overwritten.
--   * 'started' with a worker stamp: that worker started the step and has
--     not reported for a whole lease. It crashed, or it is stuck. Not
--     handed out; the row gets what cb_job_fail would have done had the
--     worker been able to report: stamp cleared, claimable_at moved to
--     now() + backoff(attempt), or to plain now() when no attempts are
--     left (delaying the give-up would gain nothing). The status stays
--     'started' so a worker that was merely stuck can still deliver its
--     result: complete and fail accept a 'started' step at the same
--     attempt.
--   * 'started' without a worker stamp: a crashed row (previous bullet)
--     whose backoff has passed. Handed out. cb_job_start then spends the
--     next attempt, or marks the step failed when none are left. That
--     give-up cannot happen here, because it needs the run lock and claim
--     works without it.
--
-- A crashed row is cleared in one call and handed out in a later one,
-- never both at once. Reason: the worker most likely to call claim right
-- after a lease runs out is the stuck worker itself, alive again with the
-- old handler still running. Given its own step back, it would run the
-- step twice at the same time. With the stamp cleared first, its next
-- cb_job_extend no longer returns the step, so it cancels the old handler
-- before the step reaches any worker.
--
-- Clearing crashed rows uses batch slots, so a call can return fewer
-- steps than are ready; the next call picks up the rest.
CREATE FUNCTION cb_job_claim(queues text[], worker text)
RETURNS TABLE (run_id bigint, step_id bigint, name text, lease_at timestamptz)
LANGUAGE plpgsql AS $$
DECLARE
    _missing text;
BEGIN
    SELECT queue_name INTO _missing
    FROM unnest(cb_job_claim.queues) AS queue_name
    WHERE NOT EXISTS (SELECT 1 FROM cb_job_queues pool WHERE pool.name = queue_name)
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'catbird: queue % not defined', _missing USING ERRCODE = 'IRD02';
    END IF;

    RETURN QUERY
    WITH _updated AS (
        UPDATE cb_job_steps step
        SET worker = CASE WHEN due.crashed THEN NULL -- freed; handed out on a later call
                          ELSE cb_job_claim.worker END,
            claimable_at = clock_timestamp() + CASE
                WHEN NOT due.crashed THEN due.claim_ttl -- the lease
                -- no attempts left: ready at once, cb_job_start does the give-up
                WHEN due.attempt >= due.max_attempts THEN interval '0'
                -- the same backoff a reported failure gets
                ELSE cb_backoff(due.backoff_kind, due.backoff_base, due.backoff_max, due.attempt)
            END
        FROM (
            SELECT s.run_id, s.id, s.attempt,
                   -- 'started' but past its lease with the worker still
                   -- stamped: the worker stopped reporting
                   s.status = 'started' AND s.worker IS NOT NULL AS crashed,
                   pool.claim_ttl, pool.max_attempts,
                   pool.backoff_kind, pool.backoff_base, pool.backoff_max
            FROM cb_job_queues pool
            CROSS JOIN LATERAL (
                -- the pool's next batch of ready rows, oldest first
                SELECT s.run_id, s.id, s.status, s.worker, s.attempt
                FROM cb_job_steps s
                WHERE s.queue = pool.name
                  AND s.status IN ('queued', 'started')
                  AND s.claimable_at <= clock_timestamp()
                ORDER BY s.claimable_at
                LIMIT pool.claim_batch_size
                FOR UPDATE SKIP LOCKED
            ) s
            WHERE pool.name = ANY (cb_job_claim.queues)
        ) due
        WHERE (step.run_id, step.id) = (due.run_id, due.id)
        RETURNING step.run_id, step.id, step.name, step.claimable_at, due.crashed
    )
    SELECT u.run_id, u.id, u.name, u.claimable_at
    FROM _updated u
    WHERE NOT u.crashed; -- crashed rows were rescheduled, not handed out
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Starts a claimed step: increments attempt, inserts the attempt row and
-- returns what the handler needs. The checks, in order: the run accepts
-- work; the step is 'queued' or 'started' ('started' happens when an
-- earlier attempt crashed); the step still carries this worker's stamp.
-- When a check fails the step was finished or taken over by another
-- worker in the meantime: nothing is returned (name IS NULL) and the
-- caller moves on to its next claimed step.
--
-- When the step's attempts are already used up, starting it would exceed
-- the budget: _cb_job_give_up marks it failed instead, no attempt row is
-- written, and nothing is returned. This check sits here and not in
-- cb_job_claim because the give-up needs the run lock, which claim never
-- takes.
CREATE FUNCTION cb_job_start(
    run_id  bigint,
    step_id bigint,
    worker  text,

    OUT name    text,
    OUT input   jsonb,
    OUT signal  jsonb,
    OUT attempt int
)
LANGUAGE plpgsql AS $$
DECLARE
    _run cb_job_runs;
    _step cb_job_steps;
    _terms cb_job_queues;
BEGIN
    SELECT r.* INTO _run FROM cb_job_runs r
    WHERE r.id = cb_job_start.run_id
    FOR UPDATE;
    IF NOT FOUND OR _run.status NOT IN ('running', 'failing') THEN
        RETURN;
    END IF;

    SELECT s.* INTO _step FROM cb_job_steps s
    WHERE s.run_id = cb_job_start.run_id AND s.id = cb_job_start.step_id
    FOR UPDATE;
    IF NOT FOUND
       OR _step.status NOT IN ('queued', 'started')
       OR _step.worker IS DISTINCT FROM cb_job_start.worker THEN
        RETURN;
    END IF;

    SELECT q.* INTO _terms FROM cb_job_queues q WHERE q.name = _step.queue;

    IF _step.attempt >= _terms.max_attempts THEN
        PERFORM _cb_job_give_up(_run, _step, 'attempts exhausted; last attempt ended in silence');
        RETURN;
    END IF;

    UPDATE cb_job_steps s
    SET status = 'started',
        attempt = s.attempt + 1
    WHERE s.run_id = cb_job_start.run_id AND s.id = cb_job_start.step_id
    RETURNING s.attempt INTO attempt;

    INSERT INTO cb_job_attempts (run_id, step_id, attempt, worker)
    VALUES (cb_job_start.run_id, cb_job_start.step_id, cb_job_start.attempt, cb_job_start.worker);

    name := _step.name;
    input := _step.input;
    signal := _step.signal;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Moves the lease end (claimable_at) forward on every step this worker
-- holds in the given pools, and returns those steps. A step the worker
-- thinks it holds but that is missing from the result was taken over
-- after its lease ran out: the worker must cancel that handler. A lease
-- that ran out but was not yet taken over can still be extended; until
-- someone else claims the step, it is still this worker's.
CREATE FUNCTION cb_job_extend(queues text[], worker text)
RETURNS TABLE (run_id bigint, step_id bigint, lease_at timestamptz)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH _held AS (
        -- locked in (run_id, id) order; see the lock rules above
        SELECT s.run_id, s.id, pool.claim_ttl
        FROM cb_job_steps s
        JOIN cb_job_queues pool ON pool.name = s.queue
        WHERE s.queue = ANY (cb_job_extend.queues)
          AND s.worker = cb_job_extend.worker
          AND s.status IN ('queued', 'started')
        ORDER BY s.run_id, s.id
        FOR UPDATE OF s
    )
    UPDATE cb_job_steps step
    SET claimable_at = clock_timestamp() + held.claim_ttl
    FROM _held held
    WHERE (step.run_id, step.id) = (held.run_id, held.id)
    RETURNING step.run_id, step.id, step.claimable_at;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Gives back a claimed step this worker has not started. The stamp is
-- cleared and the step becomes claimable again after pause (default 0:
-- immediately). No attempt is spent. A worker that claims a step it has
-- no handler for — possible during a rolling deploy — releases it with a
-- short pause, so that two not-yet-updated workers do not pass it back
-- and forth as fast as they can. Like claim, no run lock. Returns false
-- when this worker no longer holds the step.
CREATE FUNCTION cb_job_release(
    run_id  bigint,
    step_id bigint,
    worker  text,
    pause   interval DEFAULT interval '0'
)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _queue text;
    _claimable_at timestamptz;
BEGIN
    UPDATE cb_job_steps s
    SET worker = NULL,
        claimable_at = clock_timestamp() + coalesce(cb_job_release.pause, interval '0')
    WHERE s.run_id = cb_job_release.run_id
      AND s.id = cb_job_release.step_id
      AND s.worker = cb_job_release.worker
      AND s.status = 'queued'
    RETURNING s.queue, s.claimable_at INTO _queue, _claimable_at;

    IF NOT FOUND THEN
        RETURN false;
    END IF;

    PERFORM pg_notify(current_schema || '.cbq_' || _queue,
        to_char(_claimable_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
    RETURN true;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Records a successful attempt: the attempt row and the step store the
-- result, and the run's steps_remaining drops by one. run_output, when
-- given, becomes the run's output no matter which step this is; when
-- several steps set it, the last one wins. SQL NULL means "not given",
-- which is why an explicit run output can never be the JSON value null.
--
-- When steps_remaining reaches zero the run is done: a 'running' run
-- becomes 'completed', a 'failing' run becomes 'failed'. The run's output
-- is then the run_output if any call set one, otherwise the output of
-- this last step. Returns false, having changed nothing, when the checks
-- at the top fail.
CREATE FUNCTION cb_job_complete(
    run_id     bigint,
    step_id    bigint,
    attempt    int,
    output     jsonb DEFAULT NULL,
    spawns     jsonb DEFAULT NULL,
    run_output jsonb DEFAULT NULL
)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _run cb_job_runs;
    _step cb_job_steps;
    _remaining int;
    _status text;
BEGIN
    SELECT r.* INTO _run FROM cb_job_runs r
    WHERE r.id = cb_job_complete.run_id
    FOR UPDATE;
    IF NOT FOUND OR _run.status NOT IN ('running', 'failing') THEN
        RETURN false;
    END IF;

    SELECT s.* INTO _step FROM cb_job_steps s
    WHERE s.run_id = cb_job_complete.run_id AND s.id = cb_job_complete.step_id
    FOR UPDATE;
    IF NOT FOUND
       OR _step.status <> 'started'
       OR _step.attempt <> cb_job_complete.attempt THEN
        RETURN false;
    END IF;

    -- the spawns branch opens with M4b
    IF cb_job_complete.spawns IS NOT NULL AND jsonb_array_length(cb_job_complete.spawns) > 0 THEN
        RAISE EXCEPTION 'catbird: spawns are not supported yet' USING ERRCODE = 'IRD01';
    END IF;

    UPDATE cb_job_attempts a
    SET status = 'completed',
        finished_at = clock_timestamp()
    WHERE (a.run_id, a.step_id, a.attempt)
        = (cb_job_complete.run_id, cb_job_complete.step_id, cb_job_complete.attempt);

    UPDATE cb_job_steps s
    SET status = 'completed',
        output = cb_job_complete.output,
        worker = NULL,
        claimable_at = NULL,
        finished_at = clock_timestamp()
    WHERE s.run_id = cb_job_complete.run_id AND s.id = cb_job_complete.step_id;

    UPDATE cb_job_runs r
    SET steps_remaining = r.steps_remaining - 1,
        output = coalesce(cb_job_complete.run_output, r.output)
    WHERE r.id = cb_job_complete.run_id
    RETURNING r.steps_remaining INTO _remaining;

    IF _remaining > 0 THEN
        RETURN true;
    END IF;

    ---- All steps done: finish the run. ---- (M4b: dispatch waiting all_done steps here first)
    _status := CASE _run.status WHEN 'running' THEN 'completed' ELSE 'failed' END;

    UPDATE cb_job_runs r
    SET status = _status,
        output = coalesce(r.output, cb_job_complete.output),
        finished_at = clock_timestamp()
    WHERE r.id = cb_job_complete.run_id;

    PERFORM pg_notify(current_schema || '.cbj_' || _run.job,
        cb_job_complete.run_id || ':' || _status);
    RETURN true;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Records a failed attempt on the attempt row, then one comparison
-- decides. Attempts left: the step goes back to 'queued' and becomes
-- claimable again after backoff(attempt); the retry is just this row
-- becoming claimable later, nothing is copied or moved. Attempts used up:
-- _cb_job_give_up, in the same transaction. Returns false, having changed
-- nothing, when the checks at the top fail.
CREATE FUNCTION cb_job_fail(
    run_id  bigint,
    step_id bigint,
    attempt int,
    error   text
)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _run cb_job_runs;
    _step cb_job_steps;
    _terms cb_job_queues;
    _claimable_at timestamptz;
BEGIN
    SELECT r.* INTO _run FROM cb_job_runs r
    WHERE r.id = cb_job_fail.run_id
    FOR UPDATE;
    IF NOT FOUND OR _run.status NOT IN ('running', 'failing') THEN
        RETURN false;
    END IF;

    SELECT s.* INTO _step FROM cb_job_steps s
    WHERE s.run_id = cb_job_fail.run_id AND s.id = cb_job_fail.step_id
    FOR UPDATE;
    IF NOT FOUND
       OR _step.status <> 'started'
       OR _step.attempt <> cb_job_fail.attempt THEN
        RETURN false;
    END IF;

    UPDATE cb_job_attempts a
    SET status = 'failed',
        error = cb_job_fail.error,
        finished_at = clock_timestamp()
    WHERE (a.run_id, a.step_id, a.attempt)
        = (cb_job_fail.run_id, cb_job_fail.step_id, cb_job_fail.attempt);

    SELECT q.* INTO _terms FROM cb_job_queues q WHERE q.name = _step.queue;

    IF _step.attempt >= _terms.max_attempts THEN
        PERFORM _cb_job_give_up(_run, _step, cb_job_fail.error);
        RETURN true;
    END IF;

    UPDATE cb_job_steps s
    SET status = 'queued',
        worker = NULL,
        claimable_at = clock_timestamp()
            + cb_backoff(_terms.backoff_kind, _terms.backoff_base, _terms.backoff_max, _step.attempt)
    WHERE s.run_id = cb_job_fail.run_id AND s.id = cb_job_fail.step_id
    RETURNING s.claimable_at INTO _claimable_at;

    PERFORM pg_notify(current_schema || '.cbq_' || _step.queue,
        to_char(_claimable_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
    RETURN true;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Cancels a run. A 'running' run ends 'canceled'. A 'failing' run already
-- has its verdict and ends 'failed'; cancel only stops the on_fail steps
-- (refusing to cancel a 'failing' run would assume its cleanup always
-- finishes). All unfinished steps become 'canceled'. A handler that is
-- already running is not interrupted here: its worker sees the canceled
-- step on the next extend and cancels it, and a late complete or fail
-- fails the checks and changes nothing. Returns false when the run does
-- not exist or is already finished.
CREATE FUNCTION cb_job_cancel(run_id bigint, reason text DEFAULT NULL)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _run cb_job_runs;
    _status text;
BEGIN
    SELECT r.* INTO _run FROM cb_job_runs r
    WHERE r.id = cb_job_cancel.run_id
    FOR UPDATE;
    IF NOT FOUND OR _run.status NOT IN ('running', 'failing') THEN
        RETURN false;
    END IF;

    UPDATE cb_job_steps step
    SET status = 'canceled',
        worker = NULL,
        claimable_at = NULL,
        finished_at = clock_timestamp()
    FROM (
        -- locked in (run_id, id) order; see the lock rules above
        SELECT s.run_id, s.id FROM cb_job_steps s
        WHERE s.run_id = cb_job_cancel.run_id
          AND s.status IN ('waiting', 'queued', 'started')
        ORDER BY s.run_id, s.id
        FOR UPDATE
    ) locked
    WHERE (step.run_id, step.id) = (locked.run_id, locked.id);

    _status := CASE _run.status WHEN 'running' THEN 'canceled' ELSE 'failed' END;

    UPDATE cb_job_runs r
    SET status = _status,
        error = CASE WHEN _run.status = 'running' THEN cb_job_cancel.reason ELSE r.error END,
        steps_remaining = 0,
        finished_at = clock_timestamp()
    WHERE r.id = cb_job_cancel.run_id;

    PERFORM pg_notify(current_schema || '.cbj_' || _run.job,
        cb_job_cancel.run_id || ':' || _status);
    RETURN true;
END; $$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_job_cancel(bigint, text);
DROP FUNCTION cb_job_fail(bigint, bigint, int, text);
DROP FUNCTION cb_job_complete(bigint, bigint, int, jsonb, jsonb, jsonb);
DROP FUNCTION cb_job_release(bigint, bigint, text, interval);
DROP FUNCTION cb_job_extend(text[], text);
DROP FUNCTION cb_job_start(bigint, bigint, text);
DROP FUNCTION cb_job_claim(text[], text);
DROP FUNCTION _cb_job_give_up(cb_job_runs, cb_job_steps, text);
DROP FUNCTION cb_job_run(text, jsonb, text, interval);
DROP FUNCTION cb_job_delete_schedule(text);
DROP FUNCTION cb_job_define_schedule(text, text, interval, cb_job_catch_up_policy, jsonb, timestamptz);
DROP FUNCTION cb_job_define(text, text, text, interval);
DROP FUNCTION cb_job_define_queue(text, interval, int, int, cb_backoff_kind, interval, interval);

DROP TABLE cb_job_signals;
DROP TABLE cb_job_attempts;
DROP TABLE cb_job_steps;
DROP TABLE cb_job_runs;
DROP TABLE cb_job_schedules;
DROP TABLE cb_job_queues;
DROP TABLE cb_jobs;
DROP TYPE cb_job_catch_up_policy;
DROP TYPE cb_job_dispatch;
