-- +goose up
-- Catbird Lite schema. Plain SQL, no PL/pgSQL.

-- Every job's payload and every published message is one row here.
CREATE SEQUENCE cb_position_seq;

CREATE TABLE cb_messages (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    payload JSONB,
    dedup_key TEXT, -- a second insert with the same key does nothing
    -- true for Publish, false for Enqueue. Only published messages get a position.
    stream BOOLEAN NOT NULL DEFAULT false,
    -- Place in the stream, set once by the assigner (see runtime.go) after the
    -- message committed, so positions follow commit order. A message from a
    -- long transaction gets its position when it commits. Readers go by
    -- position, never by id, so no message is skipped. This is the one update
    -- cb_messages rows receive.
    position BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- for GC retention
);

-- Both unique indexes are partial, over the rows that have a value. A job's
-- message has neither a position nor usually a dedup key, and a full unique
-- index stores an entry for every NULL: 1272 kB per index per 200k job
-- messages, probed by nothing. The deduplicating inserts name the predicate:
-- ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING.
CREATE UNIQUE INDEX cb_messages_position_idx ON cb_messages (position) WHERE position IS NOT NULL;
CREATE UNIQUE INDEX cb_messages_dedup_key_idx ON cb_messages (dedup_key) WHERE dedup_key IS NOT NULL;

-- Stream reads: topic prefix (LIKE 'a.%') in position order. A job's message
-- has no position and is not in this index.
CREATE INDEX cb_messages_topic_position_idx ON cb_messages (topic text_pattern_ops, position) WHERE position IS NOT NULL;
-- The assigner's work list: published messages that have no position yet.
CREATE INDEX cb_messages_unassigned_idx ON cb_messages (id) WHERE stream AND position IS NULL;

-- One row per stream consumer: the highest position it has processed.
CREATE TABLE cb_cursors (
    name TEXT PRIMARY KEY,
    last_position BIGINT NOT NULL
) WITH (fillfactor = 90);

-- One row per job that still has to run. Deleted when the job completes.
-- visible_at is both the earliest start time and the lease deadline: a claimed
-- job has visible_at in the future; when it passes, any worker may claim it again.
CREATE TABLE cb_claims (
    message_id BIGINT PRIMARY KEY REFERENCES cb_messages (id) ON DELETE CASCADE,
    queue TEXT NOT NULL, -- the claim key: which workers may take this job
    job_type TEXT NOT NULL, -- which handler runs it
    -- The workflow this job belongs to: the id of the job that started it.
    -- NULL for a job that started one or stands alone, so the volume of
    -- single-shot jobs stays out of the index below and pays nothing for it
    -- on every claim and retry. A job's own group is coalesce(group_id, message_id).
    group_id BIGINT,
    visible_at TIMESTAMPTZ NOT NULL,
    status SMALLINT NOT NULL DEFAULT 0, -- 0 live, 1 dead (failed permanently or canceled)
    attempts SMALLINT NOT NULL DEFAULT 0, -- incremented on claim; doubles as the lease token
    -- How many jobs this one is still waiting for; it runs when this reaches 0.
    dependencies SMALLINT NOT NULL DEFAULT 0,
    -- The other direction: the jobs waiting for this one. Completing it takes one
    -- off each of their dependencies. Both are set by the completion that created
    -- them, so nothing outside catbird holds a count.
    dependent_job_ids BIGINT[],
    -- Whether this job waits for a signal, and the payload once one arrived.
    -- A waiting job has visible_at = 'infinity'; Signal writes the payload and
    -- sets visible_at to now(), so the wait is a delay and needs no place in
    -- the ready index. The payload lives here rather than in a table of its
    -- own because it is created, read and deleted with the claim.
    awaits_signal BOOLEAN NOT NULL DEFAULT false,
    signal JSONB
) WITH (
    -- This table is small and rewritten constantly; vacuum it when 1% of it changed.
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01
);
-- Only claimable rows are in the index; dead and waiting rows leave it on their
-- own, and a job waiting for a signal sits at the far end on 'infinity' where
-- the claim's LIMIT never reaches it.
CREATE INDEX cb_claims_ready_idx ON cb_claims (queue, visible_at) WHERE status = 0 AND dependencies = 0;
-- What Cancel and Signal probe. A worker cancels the group of every job that
-- dies in one, so without this a downstream outage runs one full scan of
-- cb_claims per dead job: 834 buffers and 4.9 ms at 100k live claims, against 6
-- buffers here. Jobs outside a workflow stay out of a structure that can never
-- match them: 112 kB against 2128 kB with 1% of jobs grouped.
CREATE INDEX cb_claims_group_idx ON cb_claims (group_id) WHERE status = 0 AND group_id IS NOT NULL;

-- Optional result of a job. The handler records it with SetOutput and the
-- completion writes it here, so a result cannot outlive an attempt that never
-- finished. group_id and job_type are copied from the claim, which is deleted
-- by the same statement: a later job of the same workflow reads an earlier
-- one's result by what it was, not by an id it cannot know.
CREATE TABLE cb_outputs (
    message_id BIGINT PRIMARY KEY REFERENCES cb_messages (id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL,
    job_type TEXT NOT NULL,
    output JSONB
);
CREATE INDEX cb_outputs_group_idx ON cb_outputs (group_id, job_type);

-- +goose down
DROP TABLE cb_outputs;
DROP TABLE cb_claims;
DROP TABLE cb_cursors;
DROP TABLE cb_messages;
DROP SEQUENCE cb_position_seq;
