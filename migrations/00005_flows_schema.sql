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
    retention_period interval,
    CONSTRAINT cb_name_not_empty CHECK (name <> ''),
    CONSTRAINT cb_step_count_valid CHECK (step_count >= 0),
    CONSTRAINT cb_output_priority_valid CHECK (
        cardinality(output_priority) > 0
    )
);

CREATE TABLE IF NOT EXISTS cb_steps (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    name text NOT NULL,
    description text,
    idx int NOT NULL DEFAULT 0,
    dependency_count int NOT NULL DEFAULT 0,
    step_type text NOT NULL DEFAULT 'normal',
    map_source_step_name text,
    reduce_source_step_name text,
    signal boolean NOT NULL DEFAULT false,
    condition jsonb,
    PRIMARY KEY (flow_name, name),
    UNIQUE (flow_name, idx),
    CONSTRAINT cb_name_valid CHECK (name <> ''),
    CONSTRAINT cb_name_not_reserved CHECK (name NOT IN ('input', 'signal')),
    CONSTRAINT cb_idx_valid CHECK (idx >= 0),
    CONSTRAINT cb_dependency_count_valid CHECK (dependency_count >= 0),
    CONSTRAINT cb_step_type_valid CHECK (step_type IN ('normal', 'mapper', 'generator', 'reducer')),
    CONSTRAINT cb_map_source_not_self CHECK (map_source_step_name IS NULL OR map_source_step_name <> name),
    CONSTRAINT cb_map_source_requires_mapper CHECK ((step_type <> 'mapper' AND map_source_step_name IS NULL) OR step_type = 'mapper'),
    CONSTRAINT cb_reduce_source_not_self CHECK (reduce_source_step_name IS NULL OR reduce_source_step_name <> name),
    CONSTRAINT cb_reduce_source_requires_reducer CHECK (
        (step_type = 'reducer' AND reduce_source_step_name IS NOT NULL)
        OR (step_type <> 'reducer' AND reduce_source_step_name IS NULL)
    ),
    CONSTRAINT cb_map_source_fk FOREIGN KEY (flow_name, map_source_step_name) REFERENCES cb_steps (flow_name, name),
    CONSTRAINT cb_reduce_source_fk FOREIGN KEY (flow_name, reduce_source_step_name) REFERENCES cb_steps (flow_name, name)
);

CREATE TABLE IF NOT EXISTS cb_step_dependencies (
    flow_name text NOT NULL REFERENCES cb_flows (name),
    step_name text NOT NULL,
    dependency_step_name text NOT NULL,
    idx int NOT NULL DEFAULT 0,
    PRIMARY KEY (flow_name, step_name, idx),
    UNIQUE (flow_name, step_name, dependency_step_name),
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps (flow_name, name),
    FOREIGN KEY (flow_name, dependency_step_name) REFERENCES cb_steps (flow_name, name),
    CONSTRAINT cb_dependency_name_is_different CHECK (dependency_step_name != step_name),
    CONSTRAINT cb_idx_valid CHECK (idx >= 0)
);

CREATE INDEX IF NOT EXISTS cb_step_dependencies_step_fk ON cb_step_dependencies (flow_name, step_name);
CREATE INDEX IF NOT EXISTS cb_step_dependencies_dependency_name_fk ON cb_step_dependencies (flow_name, dependency_step_name);

-- cb_flow_info: Query information about all flow definitions
-- Returns all registered flows with their step definitions and metadata
-- Single SELECT with recursive derivation for compatibility with analyzers
CREATE OR REPLACE VIEW cb_flow_info AS
    SELECT
        f.name,
        nullif((CASE WHEN jsonb_typeof(to_jsonb(f)) = 'object' THEN to_jsonb(f)->>'description' ELSE NULL END), '') AS description,
        (CASE WHEN step_data.steps IS NOT NULL THEN step_data.steps ELSE '[]'::jsonb END) AS steps,
        (CASE WHEN jsonb_typeof(to_jsonb(f)->'output_priority') = 'array' THEN
            ARRAY(SELECT jsonb_array_elements_text(to_jsonb(f)->'output_priority'))
         ELSE ARRAY[]::text[] END) AS output_priority,
        f.retention_period,
        f.created_at
    FROM cb_flows f
    LEFT JOIN LATERAL (
        SELECT
            st.flow_name,
            jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
                'name', st.name,
                'description', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'description' ELSE NULL END),
                'step_type', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'step_type' ELSE NULL END),
                'map_source_step_name', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'map_source_step_name' ELSE NULL END),
                'reduce_source_step_name', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'reduce_source_step_name' ELSE NULL END),
                'signal', (CASE WHEN jsonb_typeof(to_jsonb(st)) = 'object' THEN to_jsonb(st)->'signal' ELSE NULL END),
                'depends_on', (
                    SELECT jsonb_agg(jsonb_build_object('name', s_d.dependency_step_name))
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
