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
-- next_step_id mints per-run step ids under the run lock every engine call
-- already takes.
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

-- Per-attempt history and the fence record, kept when the run turns
-- terminal. A NULL outcome is recorded silence: a start that never
-- reported — a crash, or a restart that superseded it.
CREATE TABLE cb_job_attempts (
    run_id bigint NOT NULL,
    step_id bigint NOT NULL,
    attempt int NOT NULL,
    worker text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome text CHECK (outcome IN ('completed', 'failed')),
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

-- +goose down

DROP TABLE cb_job_signals;
DROP TABLE cb_job_attempts;
DROP TABLE cb_job_steps;
DROP TABLE cb_job_runs;
DROP TABLE cb_job_schedules;
DROP TABLE cb_job_queues;
DROP TABLE cb_jobs;
DROP TYPE cb_job_catch_up_policy;
DROP TYPE cb_job_dispatch;
