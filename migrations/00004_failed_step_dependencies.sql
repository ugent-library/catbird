-- +goose up

-- cb_get_flow_step_dependency_outputs: Fetch the outputs a step consumed from its
-- dependencies, in dependency (handler-argument) order. Backs
-- FlowFailure.FailedStepDependencies so an on-fail handler can inspect the
-- inputs a failed step received from upstream steps without re-encoding the DAG.
--
-- The step's dependency_step_names array is already ordered by declaration and
-- excludes IgnoreOutput dependencies, so it mirrors the handler's parameter
-- order. Each dependency's output is resolved via cb_get_flow_step_output, which
-- aggregates mapper/generator outputs in item order.

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_get_flow_step_dependency_outputs(
    flow_name text,
    run_id bigint,
    step_name text
)
RETURNS TABLE(dependency_step_name text, output jsonb)
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := _cb_table_name(cb_get_flow_step_dependency_outputs.flow_name, 's');
    _dep_names text[];
BEGIN
    EXECUTE format(
        'SELECT dependency_step_names FROM %I WHERE flow_run_id = $1 AND step_name = $2',
        _s_table
    )
    USING cb_get_flow_step_dependency_outputs.run_id, cb_get_flow_step_dependency_outputs.step_name
    INTO _dep_names;

    IF _dep_names IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT dep.name,
           cb_get_flow_step_output(
               cb_get_flow_step_dependency_outputs.flow_name,
               cb_get_flow_step_dependency_outputs.run_id,
               dep.name
           )
    FROM unnest(_dep_names) WITH ORDINALITY AS dep(name, ord)
    ORDER BY dep.ord;
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_get_flow_step_dependency_outputs(text, bigint, text);
