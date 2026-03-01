-- Task execution schema

-- +goose up

-- +goose statementbegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'cb_task_claim') THEN
        CREATE TYPE cb_task_claim AS (
            id bigint,
            attempts int,
            input jsonb
       );
    END IF;
END$$;
-- +goose statementend

CREATE TABLE IF NOT EXISTS cb_tasks (
    name text PRIMARY KEY,
    description text,
    created_at timestamptz NOT NULL DEFAULT now(),
    condition jsonb,
    CONSTRAINT name_not_empty CHECK (name <> ''),
    CONSTRAINT name_not_reserved CHECK (name <> 'input')
);

-- +goose down

DROP TABLE IF EXISTS cb_tasks CASCADE;
DROP TYPE IF EXISTS cb_task_claim CASCADE;
