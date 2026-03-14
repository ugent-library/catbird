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
    catch_up         TEXT NOT NULL DEFAULT 'one',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cb_cron_spec_not_empty CHECK (cron_spec <> ''),
    CONSTRAINT cb_input_is_object CHECK (jsonb_typeof(input) = 'object'),
    CONSTRAINT cb_updated_at_gte_created_at CHECK (updated_at >= created_at),
    CONSTRAINT cb_catch_up_valid CHECK (catch_up IN ('skip', 'one', 'all'))
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
    catch_up         TEXT NOT NULL DEFAULT 'one',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT cb_cron_spec_not_empty CHECK (cron_spec <> ''),
    CONSTRAINT cb_input_is_object CHECK (jsonb_typeof(input) = 'object'),
    CONSTRAINT cb_updated_at_gte_created_at CHECK (updated_at >= created_at),
    CONSTRAINT cb_catch_up_valid CHECK (catch_up IN ('skip', 'one', 'all'))
);

CREATE INDEX IF NOT EXISTS cb_flow_schedules_next_run_at_enabled_idx
    ON cb_flow_schedules (next_run_at, enabled)
    WHERE enabled = true;

-- +goose down

DROP INDEX IF EXISTS cb_flow_schedules_next_run_at_enabled_idx;
DROP TABLE IF EXISTS cb_flow_schedules;
DROP INDEX IF EXISTS cb_task_schedules_next_run_at_enabled_idx;
DROP TABLE IF EXISTS cb_task_schedules;
