-- Generator reducer helper functions

-- +goose up

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_mark_generator_complete_pending(flow_name text, step_name text, step_id bigint)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := cb_table_name(cb_mark_generator_complete_pending.flow_name, 's');
    _spawned int;
    _completed int;
  _ready boolean;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET generator_status = 'complete'
      WHERE id = $1
        AND step_name = $2
        AND status = 'started'
        AND generator_status = 'started'
      RETURNING map_tasks_spawned, map_tasks_completed
      $QUERY$,
      _s_table
    )
    USING cb_mark_generator_complete_pending.step_id, cb_mark_generator_complete_pending.step_name
    INTO _spawned, _completed;

    IF _spawned IS NULL THEN
        RETURN false;
    END IF;

    _ready := _spawned = _completed;

    IF _ready THEN
        EXECUTE format(
          $QUERY$
          UPDATE %I
          SET status = 'queued',
              visible_at = now()
          WHERE id = $1
            AND step_name = $2
            AND status = 'started'
            AND generator_status = 'complete'
          $QUERY$,
          _s_table
        )
        USING cb_mark_generator_complete_pending.step_id, cb_mark_generator_complete_pending.step_name;
    END IF;

    RETURN _ready;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE OR REPLACE FUNCTION cb_complete_map_task_pending(flow_name text, step_name text, map_task_id bigint, output jsonb)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    _s_table text := cb_table_name(cb_complete_map_task_pending.flow_name, 's');
    _m_table text := cb_table_name(cb_complete_map_task_pending.flow_name, 'm');
    _flow_run_id bigint;
    _step_id bigint;
    _generator_status text;
    _spawned int;
    _completed int;
  _has_pending boolean;
BEGIN
    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'completed',
          completed_at = now(),
          output = $2
      WHERE id = $1
        AND status = 'started'
      RETURNING flow_run_id
      $QUERY$,
      _m_table
    )
    USING cb_complete_map_task_pending.map_task_id, cb_complete_map_task_pending.output
    INTO _flow_run_id;

    IF _flow_run_id IS NULL THEN
        RETURN NULL;
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET map_tasks_completed = CASE
          WHEN generator_status IS NULL THEN map_tasks_completed
          ELSE map_tasks_completed + 1
      END
      WHERE flow_run_id = $1
        AND step_name = $2
        AND status = 'started'
      RETURNING id, generator_status, map_tasks_spawned, map_tasks_completed
      $QUERY$,
      _s_table
    )
    USING _flow_run_id, cb_complete_map_task_pending.step_name
    INTO _step_id, _generator_status, _spawned, _completed;

    IF _step_id IS NULL THEN
        RETURN NULL;
    END IF;

    IF _generator_status IS NOT NULL THEN
      IF _generator_status = 'complete' AND _completed = _spawned THEN
        EXECUTE format(
          $QUERY$
          UPDATE %I
          SET status = 'queued',
            visible_at = now()
          WHERE id = $1
          AND status = 'started'
          AND generator_status = 'complete'
          $QUERY$,
          _s_table
        )
        USING _step_id;
        RETURN _step_id;
        END IF;
        RETURN NULL;
    END IF;

    EXECUTE format(
      'SELECT EXISTS (SELECT 1 FROM %I WHERE flow_run_id = $1 AND step_name = $2 AND status IN (''queued'', ''started''))',
      _m_table
    )
    USING _flow_run_id, cb_complete_map_task_pending.step_name
    INTO _has_pending;

    IF _has_pending THEN
        RETURN NULL;
    END IF;

    EXECUTE format(
      $QUERY$
      UPDATE %I
      SET status = 'queued',
          visible_at = now()
      WHERE id = $1
        AND status = 'started'
        AND generator_status IS NULL
      $QUERY$,
      _s_table
    )
    USING _step_id;

    RETURN _step_id;
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_complete_map_task_pending(flow_name text, step_name text, map_task_id bigint, output jsonb);
DROP FUNCTION IF EXISTS cb_mark_generator_complete_pending(flow_name text, step_name text, step_id bigint);
