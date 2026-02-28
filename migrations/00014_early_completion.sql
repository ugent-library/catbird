-- Early completion support for flow runs

-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_early_exit_flow(
    flow_name text,
    flow_run_id bigint,
    step_name text,
    output jsonb,
    reason text DEFAULT NULL
)
RETURNS TABLE(changed boolean, status text)
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := cb_table_name(cb_early_exit_flow.flow_name, 'f');
    _s_table text := cb_table_name(cb_early_exit_flow.flow_name, 's');
    _m_table text := cb_table_name(cb_early_exit_flow.flow_name, 'm');
    _status text;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'completed',
          completed_at = coalesce(completed_at, now()),
          output = $2
      WHERE id = $1
        AND status = 'started'
      RETURNING status
      $QUERY$,
      _f_table
    )
    USING cb_early_exit_flow.flow_run_id, cb_early_exit_flow.output
    INTO _status;

    IF _status IS NULL THEN
      EXECUTE format('SELECT status FROM %I WHERE id = $1', _f_table)
      USING cb_early_exit_flow.flow_run_id
      INTO _status;

      IF _status IS NULL THEN
        RETURN;
      END IF;

      RETURN QUERY SELECT false, _status;
      RETURN;
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = coalesce(canceled_at, now())
      WHERE flow_run_id = $1
        AND status IN ('pending', 'queued', 'started')
      $QUERY$,
      _s_table
    )
    USING cb_early_exit_flow.flow_run_id;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'canceled',
          canceled_at = coalesce(canceled_at, now())
      WHERE flow_run_id = $1
        AND status IN ('queued', 'started')
      $QUERY$,
      _m_table
    )
    USING cb_early_exit_flow.flow_run_id;

    RETURN QUERY SELECT true, 'completed';
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_early_exit_flow(text, bigint, text, jsonb, text);
