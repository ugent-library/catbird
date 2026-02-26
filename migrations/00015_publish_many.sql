-- +goose up

-- +goose statementbegin
-- cb_publish (bulk): Send multiple messages to all queues subscribed to a topic
-- Routes each payload from a jsonb[] array to all matching queues.
-- Parameters:
--   topic: Topic string to route to subscribed queues
--   payloads: JSONB array of message payloads
--   idempotency_keys: Optional array of idempotency keys (must match payload count when provided)
--   visible_at: Optional timestamp when messages should become visible (default: now)
-- Returns: void
CREATE OR REPLACE FUNCTION cb_publish(
    topic text,
    payloads jsonb[],
    idempotency_keys text[] = null,
    visible_at timestamptz = null
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz = coalesce(cb_publish.visible_at, now());
    _rec record;
    _q_table text;
BEGIN
    IF cb_publish.idempotency_keys IS NOT NULL AND cardinality(cb_publish.idempotency_keys) <> cardinality(cb_publish.payloads) THEN
        RAISE EXCEPTION 'cb: idempotency_keys length must match payloads length';
    END IF;

    FOR _rec IN
        SELECT DISTINCT queue_name AS name
        FROM cb_bindings
        WHERE (
            (pattern_type = 'exact' AND pattern = cb_publish.topic)
            OR
            (pattern_type = 'wildcard'
             AND (prefix = '' OR cb_publish.topic LIKE prefix || '%')
             AND cb_publish.topic ~ regex)
        )
    LOOP
        _q_table := cb_table_name(_rec.name, 'q');
        BEGIN
            EXECUTE format(
                $QUERY$
                INSERT INTO %I (topic, payload, idempotency_key, visible_at)
                SELECT
                    $1,
                    payload,
                    CASE
                        WHEN $4 IS NULL THEN NULL
                        ELSE $4[ordinality]
                    END,
                    $2
                FROM unnest($3) WITH ORDINALITY AS t(payload, ordinality)
                ON CONFLICT (idempotency_key) DO NOTHING;
                $QUERY$,
                _q_table
            )
            USING cb_publish.topic,
                  _visible_at,
                  cb_publish.payloads,
                  cb_publish.idempotency_keys;
        EXCEPTION WHEN undefined_table THEN
            CONTINUE;
        END;
    END LOOP;
END
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_publish(text, jsonb[], text[], timestamptz);
