-- Flow execution schema

-- +goose up

-- +goose statementbegin
DO $$
BEGIN
    -- Drop and recreate cb_step_claim to ensure it has the latest structure (including signal_input)
    DROP TYPE IF EXISTS cb_step_claim CASCADE;
    CREATE TYPE cb_step_claim AS (
        id bigint,
        flow_run_id bigint,
        attempts int,
        input jsonb,
        step_outputs jsonb,
        signal_input jsonb
    );
END$$;
-- +goose statementend

CREATE TABLE IF NOT EXISTS cb_flows (
    name text PRIMARY KEY,
    description text,
    output_priority text[] NOT NULL,
    step_count int NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT name_not_empty CHECK (name <> ''),
    CONSTRAINT step_count_valid CHECK (step_count >= 0),
    CONSTRAINT output_priority_valid CHECK (
        cardinality(output_priority) > 0
    )
);

CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    description text,
    idx int NOT NULL DEFAULT 0,
    dependency_count int NOT NULL DEFAULT 0,
    is_generator boolean NOT NULL DEFAULT false,
    is_map_step boolean NOT NULL DEFAULT false,
    map_source text,
    has_signal boolean NOT NULL DEFAULT false,
    condition jsonb,
    PRIMARY KEY (flow_name, name),
    UNIQUE (flow_name, idx),
    CONSTRAINT name_valid CHECK (name <> ''),
    CONSTRAINT name_not_reserved CHECK (name NOT IN ('input', 'signal')),
    CONSTRAINT idx_valid CHECK (idx >= 0),
    CONSTRAINT dependency_count_valid CHECK (dependency_count >= 0),
    CONSTRAINT map_source_not_self CHECK (map_source IS NULL OR map_source <> name),
    CONSTRAINT map_source_requires_map_step CHECK ((NOT is_map_step AND map_source IS NULL) OR is_map_step),
    CONSTRAINT map_source_fk FOREIGN KEY (flow_name, map_source) REFERENCES cb_steps (flow_name, name)
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
        f.description,
        step_data.steps,
        f.output_priority,
        f.created_at
    FROM cb_flows f
    LEFT JOIN LATERAL (
        SELECT
            st.flow_name,
            jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
                'name', st.name,
                'description', st.description,
                'is_generator', st.is_generator,
                'is_map_step', st.is_map_step,
                'map_source', st.map_source,
                'has_signal', st.has_signal,
                'depends_on', (
                    SELECT jsonb_agg(jsonb_build_object('name', s_d.dependency_name))
                    FROM cb_step_dependencies AS s_d
                    WHERE s_d.flow_name = st.flow_name
                    AND s_d.step_name = st.name
                )
            )) ORDER BY st.idx) FILTER (WHERE st.idx IS NOT NULL) AS steps
        FROM cb_steps st
        WHERE st.flow_name = f.name
        GROUP BY flow_name
    ) step_data ON step_data.flow_name = f.name;

-- +goose down

DROP VIEW IF EXISTS cb_flow_info;

DROP TABLE IF EXISTS cb_step_dependencies CASCADE;
DROP TABLE IF EXISTS cb_steps CASCADE;
DROP TABLE IF EXISTS cb_flows CASCADE;
DROP TYPE IF EXISTS cb_step_claim CASCADE;
