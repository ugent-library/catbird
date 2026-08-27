-- +goose up
-- The unified, pure SQL schema for Catbird Lite. No PL/pgSQL required.

-- 1. Immutable Facts & Payloads
CREATE TABLE cb_messages (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    payload JSONB,
    dedup_key TEXT UNIQUE, -- Enforces exactly-once step generation & Leaderless Cron
    created_at TIMESTAMPTZ DEFAULT now() -- Enables cheap bulk GC retention
);
-- Makes reading sparse/rare events via Fan-Out-On-Read subscriptions incredibly fast (O(log N))
CREATE INDEX idx_cb_messages_topic_id ON cb_messages (topic text_pattern_ops, id);

-- 2. Stream Pointers (For pure log consumption)
CREATE TABLE cb_cursors (
    name TEXT PRIMARY KEY,
    last_message_id BIGINT NOT NULL
) WITH (fillfactor = 90); -- Leaves space for HOT updates on cursor advancement

-- 3. Job Claims (Narrow, HOT-updatable)
CREATE TABLE cb_claims (
    message_id BIGINT PRIMARY KEY,
    correlation_id TEXT, -- Enables cascading cancellation of DAG workflows
    queue TEXT NOT NULL,
    visible_at TIMESTAMPTZ NOT NULL,
    status SMALLINT DEFAULT 0, -- 0=ready, 1=leased, 3=dead
    attempts SMALLINT DEFAULT 0,
    dependencies SMALLINT DEFAULT 0, -- For DAG orchestration
    max_attempts SMALLINT DEFAULT 5, -- Policy injected on write
    claim_ttl_seconds INT DEFAULT 300
) WITH (
    -- PGMQ Strategy: Hyper-aggressive autovacuum exclusively for the volatile claims table.
    -- Forces Postgres to clean dead tuples when just 1% of the table changes (instead of default 20%).
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01
);
-- Ensures SKIP LOCKED is extremely fast, and automatically pauses jobs waiting on dependencies
CREATE INDEX idx_cb_claims_ready ON cb_claims(queue, visible_at) WHERE status = 0 AND dependencies = 0;

-- 4. Ephemeral Signal Storage (Append-Only Sidecar)
-- Stores payloads for external signals arriving for paused jobs
CREATE TABLE cb_signals (
    message_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    payload JSONB,
    PRIMARY KEY (message_id, name)
);

-- +goose down
DROP TABLE cb_signals;
DROP TABLE cb_claims;
DROP TABLE cb_cursors;
DROP TABLE cb_messages;
