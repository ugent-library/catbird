-- Worker management schema
-- Includes worker registration table and handler mappings

-- +goose up

CREATE TABLE IF NOT EXISTS cb_workers (
    id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cb_workers_last_heartbeat_at_idx ON cb_workers (last_heartbeat_at);

-- Worker handler tables (worker-to-task and worker-to-step mappings)
-- These tables depend on cb_workers, cb_tasks (v3), and cb_steps (v4)

CREATE TABLE IF NOT EXISTS cb_task_handlers (
    worker_id uuid NOT NULL REFERENCES cb_workers (id) ON DELETE CASCADE,
    task_name text NOT NULL REFERENCES cb_tasks (name) ON DELETE CASCADE,
    PRIMARY KEY (worker_id, task_name)
);

CREATE TABLE IF NOT EXISTS cb_step_handlers (
    worker_id uuid NOT NULL REFERENCES cb_workers (id) ON DELETE CASCADE,
    flow_name text NOT NULL,
    step_name text NOT NULL,
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name) ON DELETE CASCADE,
    PRIMARY KEY (worker_id, flow_name, step_name)
);

-- +goose down

DROP TABLE IF EXISTS cb_step_handlers CASCADE;
DROP TABLE IF EXISTS cb_task_handlers CASCADE;
DROP TABLE IF EXISTS cb_workers CASCADE;
