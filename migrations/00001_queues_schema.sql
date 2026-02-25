-- SQL code is taken from or inspired by pgmq (https://github.com/pgmq/pgmq)

-- +goose up

-- +goose statementbegin
DO $$
BEGIN
    -- Drop and recreate cb_message type to ensure it has the latest structure
    DROP TYPE IF EXISTS cb_message CASCADE;
    CREATE TYPE cb_message AS (
        id bigint,
        idempotency_key text,
        topic text,
        payload jsonb,
        deliveries int,
        created_at timestamptz,
        visible_at timestamptz
    );
END$$;
-- +goose statementend

CREATE TABLE IF NOT EXISTS cb_queues (
    name text PRIMARY KEY,
    unlogged boolean NOT null,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    CONSTRAINT expires_at_is_valid CHECK (expires_at IS NULL OR expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS cb_queues_expires_at_idx ON cb_queues (expires_at);

-- Bindings table for topic routing with wildcard support
CREATE TABLE IF NOT EXISTS cb_bindings (
    queue_name text NOT NULL REFERENCES cb_queues(name) ON DELETE CASCADE,
    pattern text NOT NULL,
    pattern_type text NOT NULL CHECK (pattern_type IN ('exact', 'wildcard')),
    prefix text,
    regex text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_name, pattern)
);

CREATE INDEX IF NOT EXISTS cb_bindings_exact_idx ON cb_bindings(pattern)
    WHERE pattern_type = 'exact';
CREATE INDEX IF NOT EXISTS cb_bindings_prefix_idx ON cb_bindings(prefix text_pattern_ops)
    WHERE prefix IS NOT NULL;

-- +goose statementbegin
-- cb_table_name: Generate a PostgreSQL table name for queue/task/flow storage
-- Validates the name and constructs the internal table name with prefix
-- Parameters:
--   name: Queue/task/flow name (must contain only a-z, 0-9, _; max 58 chars)
--   prefix: Type prefix ('q' for queue, 't' for task, etc.)
-- Returns: text - the full PostgreSQL table name
CREATE OR REPLACE FUNCTION cb_table_name(name text, prefix text)
RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
    IF cb_table_name.name !~ '^[a-z0-9_]+$' THEN
        RAISE EXCEPTION 'cb: queue name can only contain characters: a-z, 0-9 or _';
    END IF;
    IF length(cb_table_name.name) >= 58 THEN
        RAISE EXCEPTION 'cb: queue name is too long, maximum length is 58';
    END IF;
    RETURN 'cb_' || cb_table_name.prefix || '_' || lower(cb_table_name.name);
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_table_name(text, text);

DROP TABLE IF EXISTS cb_bindings;
DROP TABLE IF EXISTS cb_queues;

DROP TYPE IF EXISTS cb_message;
