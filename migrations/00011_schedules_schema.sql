-- +goose up

CREATE TABLE IF NOT EXISTS cb_task_schedules (
    id               BIGSERIAL PRIMARY KEY,
    task_name        TEXT NOT NULL UNIQUE REFERENCES cb_tasks(name) ON DELETE CASCADE,
    cron_spec        TEXT NOT NULL,
    next_run_at      TIMESTAMPTZ NOT NULL,
    last_run_at      TIMESTAMPTZ,
    last_enqueued_at TIMESTAMPTZ,
    enabled          BOOLEAN NOT NULL DEFAULT true,
    input            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cb_task_schedules_next_run_at_enabled_idx
    ON cb_task_schedules (next_run_at, enabled)
    WHERE enabled = true;

CREATE TABLE IF NOT EXISTS cb_flow_schedules (
    id               BIGSERIAL PRIMARY KEY,
    flow_name        TEXT NOT NULL UNIQUE REFERENCES cb_flows(name) ON DELETE CASCADE,
    cron_spec        TEXT NOT NULL,
    next_run_at      TIMESTAMPTZ NOT NULL,
    last_run_at      TIMESTAMPTZ,
    last_enqueued_at TIMESTAMPTZ,
    enabled          BOOLEAN NOT NULL DEFAULT true,
    input            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cb_flow_schedules_next_run_at_enabled_idx
    ON cb_flow_schedules (next_run_at, enabled)
    WHERE enabled = true;

-- +goose down

DROP INDEX IF EXISTS cb_flow_schedules_next_run_at_enabled_idx;
DROP TABLE IF EXISTS cb_flow_schedules;
DROP INDEX IF EXISTS cb_task_schedules_next_run_at_enabled_idx;
DROP TABLE IF EXISTS cb_task_schedules;
