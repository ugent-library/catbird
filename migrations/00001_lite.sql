-- +goose up
-- Catbird Lite schema. Plain SQL, no PL/pgSQL.

-- Every job input and every published message is one row here.
CREATE SEQUENCE cb_position_seq;

CREATE TABLE cb_messages (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    payload JSONB,
    dedup_key TEXT UNIQUE, -- a second insert with the same key does nothing
    -- true for Publish, false for Enqueue. Only published messages get a position.
    stream BOOLEAN NOT NULL DEFAULT false,
    -- Place in the stream, set once by the assigner (see stream.go) after the
    -- message committed, so positions follow commit order. A message from a
    -- long transaction gets its position when it commits. Readers go by
    -- position, never by id, so no message is skipped. This is the one update
    -- cb_messages rows receive.
    position BIGINT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- for GC retention
);

-- Stream reads: topic prefix (LIKE 'a.%') in position order. Job inputs have
-- no position and are not in this index.
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
    queue TEXT NOT NULL,
    correlation_id TEXT, -- lets Cancel stop a group of jobs together
    visible_at TIMESTAMPTZ NOT NULL,
    status SMALLINT NOT NULL DEFAULT 0, -- 0 live, 1 dead (failed permanently or canceled)
    attempts SMALLINT NOT NULL DEFAULT 0, -- incremented on claim; doubles as the lease token
    dependencies SMALLINT NOT NULL DEFAULT 0 -- the job runs when this reaches 0
) WITH (
    -- This table is small and rewritten constantly; vacuum it when 1% of it changed.
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01
);
-- Only claimable rows are in the index; dead and waiting rows leave it on their own.
CREATE INDEX cb_claims_ready_idx ON cb_claims (queue, visible_at) WHERE status = 0 AND dependencies = 0;

-- Payloads delivered to a job that is waiting on them. Read once, at claim time.
CREATE TABLE cb_signals (
    message_id BIGINT NOT NULL REFERENCES cb_messages (id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    payload JSONB,
    PRIMARY KEY (message_id, name)
);

-- Optional result of a job, written by its handler with SetOutput in the job's
-- transaction and read with Output. Deleted with the message.
CREATE TABLE cb_outputs (
    message_id BIGINT PRIMARY KEY REFERENCES cb_messages (id) ON DELETE CASCADE,
    output JSONB
);

-- +goose down
DROP TABLE cb_outputs;
DROP TABLE cb_signals;
DROP TABLE cb_claims;
DROP TABLE cb_cursors;
DROP TABLE cb_messages;
DROP SEQUENCE cb_position_seq;
