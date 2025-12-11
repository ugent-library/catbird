-- +goose up

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    idx int not null,
    PRIMARY KEY (flow_name, name)
);

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_step_dependencies (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    step_name text not null,
    dependency_name text not null,
    primary key (flow_name, step_name, dependency_name),
    foreign key (flow_name, step_name) references cb_steps (flow_name, name),
    foreign key (flow_name, dependency_name) references cb_steps (flow_name, name),
    check (step_name != dependency_name)
);

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_flow_runs (
  id uuid primary key default gen_random_uuid(),
  flow_name text not null references cb_flows (name),
  status text not null default 'started',
  input jsonb not null,
  output jsonb,
  remaining_steps int not null default 0 check (remaining_steps >= 0),
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  failed_at timestamptz,
  constraint completed_at_or_failed_at check (not (completed_at is not null and failed_at is not null)),
  constraint completed_at_is_after_started_at check (completed_at is null or completed_at >= started_at),
  constraint failed_at_is_after_started_at check (failed_at is null or failed_at >= started_at),
  constraint status_is_valid check (status in ('started', 'completed', 'failed'))
);

-- TODO indexes
CREATE TABLE IF NOT EXISTS cb_step_runs (
  flow_run_id uuid not null references cb_flow_runs (id),
  flow_name text not null references cb_flows (name),
  step_name text not null,
  message_id bigint,  -- TODO constraint not null when started
  status text not null default 'created',
  error_message text,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  failed_at timestamptz,
  primary key (flow_run_id, step_name),
  foreign key (flow_name, step_name) references cb_steps (flow_name, name),
  constraint status_is_valid check (status in ('created', 'started', 'completed', 'failed')),
--   constraint status_and_remaining_tasks_match check (status != 'completed' or remaining_tasks = 0),
  -- Add constraint to ensure remaining_tasks is only set when step has started
--   constraint remaining_tasks_state_consistency check (
    -- remaining_tasks is null or status != 'created'
--   ),
--   constraint initial_tasks_known_when_started check (
    -- status != 'started' or initial_tasks is not null
--   ),
  constraint completed_at_or_failed_at check (not (completed_at is not null and failed_at is not null)),
  constraint started_at_is_after_created_at check (started_at is null or started_at >= created_at),
  constraint completed_at_is_after_started_at check (completed_at is null or completed_at >= started_at),
  constraint failed_at_is_after_started_at check (failed_at is null or failed_at >= started_at)
);

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_create_flow(
    name text,
    steps jsonb
)
RETURNS void AS $$
DECLARE
    _idx int = 0;
    _step jsonb;
BEGIN
    -- TODO should check attributes are same
    INSERT INTO cb_flows (name)
    VALUES (
        cb_create_flow.name
    )
    ON CONFLICT DO NOTHING;
    
    FOR _step IN SELECT * FROM jsonb_array_elements(steps)
    loop
        INSERT INTO cb_steps (flow_name, name, idx)
        VALUES (
            cb_create_flow.name,
            _step->>'name',
            _idx
        )
        ON CONFLICT DO NOTHING;

        _idx = _idx + 1;
    end loop;

    PERFORM cb_create_queue(cb_create_flow.name);
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_run_flow(
    name text,
    input jsonb
)
RETURNS text AS $$
DECLARE
    _run_id text;
BEGIN
    WITH
  -- ---------- Gather flow metadata ----------
  flow_steps AS (
    SELECT s.flow_name, s.name
    FROM cb_steps s
    WHERE s.flow_name = cb_run_flow.name
  ),
  flow_run AS (
    INSERT INTO cb_flow_runs (flow_name, input, remaining_steps)
    VALUES (
      cb_run_flow.name,
      cb_run_flow.input,
      (SELECT count(*) FROM flow_steps)
    )
    RETURNING *
  ),
  step_runs AS (
    INSERT INTO cb_step_runs (flow_name, flow_run_id, step_name)
    SELECT
      s.flow_name,
      (SELECT flow_run.id FROM flow_run),
      s.name
    FROM cb_steps s
  )
  SELECT run_id FROM flow_run
  INTO _run_id;

  RETURN _run_id;
END;
$$ LANGUAGE plpgsql;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_run_flow;
DROP FUNCTION cb_create_flow;
DROP TABLE cb_step_runs;
DROP TABLE cb_flow_runs;
DROP TABLE cb_step_dependencies;
DROP TABLE cb_steps;
DROP TABLE cb_flows;
