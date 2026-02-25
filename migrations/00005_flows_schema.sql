-- Flow execution schema

-- +goose up

-- +goose statementbegin
DO $$
BEGIN
    -- Drop and recreate cb_step_claim to ensure it has the latest structure (including signal_input)
    DROP TYPE IF EXISTS cb_step_claim CASCADE;
    CREATE TYPE cb_step_claim AS (
        id bigint,
        attempts int,
        input jsonb,
        step_outputs jsonb,
        signal_input jsonb
    );
END$$;
-- +goose statementend

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT name_not_empty CHECK (name <> '')
);

CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    idx int NOT NULL DEFAULT 0,
    dependency_count int NOT NULL DEFAULT 0,
    is_map_step boolean NOT NULL DEFAULT false,
    map_source text,
    signal boolean NOT NULL DEFAULT false,
    condition jsonb,
    PRIMARY KEY (flow_name, name),
    UNIQUE (flow_name, idx),
    CONSTRAINT name_valid CHECK (name <> ''),
    CONSTRAINT name_not_reserved CHECK (name NOT IN ('input', 'signal')),
    CONSTRAINT idx_valid CHECK (idx >= 0),
    CONSTRAINT dependency_count_valid CHECK (dependency_count >= 0),
    CONSTRAINT map_source_not_self CHECK (map_source IS NULL OR map_source <> name),
    CONSTRAINT map_source_requires_map_step CHECK ((NOT is_map_step AND map_source IS NULL) OR is_map_step)
);

CREATE TABLE IF NOT EXISTS cb_step_dependencies (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    step_name text NOT NULL,
    dependency_name text NOT NULL,
    idx int NOT NULL DEFAULT 0,
    PRIMARY KEY (flow_name, step_name, idx),
    UNIQUE (flow_name, step_name, dependency_name),
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name),
    FOREIGN KEY (flow_name, dependency_name) REFERENCES cb_steps (flow_name, name),
    CONSTRAINT dependency_name_is_different CHECK (dependency_name != step_name),
    CONSTRAINT idx_valid CHECK (idx >= 0)
);

CREATE INDEX IF NOT EXISTS cb_step_dependencies_step_fk ON cb_step_dependencies (flow_name, step_name);
CREATE INDEX IF NOT EXISTS cb_step_dependencies_dependency_name_fk ON cb_step_dependencies (flow_name, dependency_name);

-- cb_flow_info: Query information about all flow definitions
-- Returns all registered flows with their step definitions and metadata
-- Columns:
--   - name: Flow name
--   - steps: JSON array of step definitions with dependencies
--   - created_at: Flow creation timestamp
CREATE OR REPLACE VIEW cb_flow_info AS
    SELECT
    f.name,
    s.steps,
    f.created_at
    FROM cb_flows f
    LEFT JOIN LATERAL (
    SELECT
      s.flow_name,
      jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'name', s.name,
        'is_map_step', s.is_map_step,
        'map_source', s.map_source,
        'signal', s.signal,
        'depends_on', (
          SELECT jsonb_agg(jsonb_build_object('name', s_d.dependency_name))
          FROM cb_step_dependencies AS s_d
          WHERE s_d.flow_name = s.flow_name
          AND s_d.step_name = s.name
        )
      )) ORDER BY s.idx) FILTER (WHERE s.idx IS NOT NULL) AS steps
    FROM cb_steps s
    WHERE s.flow_name = f.name
    GROUP BY flow_name
    ) s ON s.flow_name = f.name;

-- +goose down

DROP VIEW IF EXISTS cb_flow_info;

DROP TABLE IF EXISTS cb_step_dependencies;
DROP TABLE IF EXISTS cb_steps;
DROP TABLE IF EXISTS cb_flows;
DROP TYPE IF EXISTS cb_step_claim;
