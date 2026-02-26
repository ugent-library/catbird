-- +goose up

-- +goose statementbegin
-- cb_send (bulk): Send multiple messages to a specific queue
-- Enqueues messages in a queue from a jsonb[] payload array.
-- Parameters:
--   queue: Queue name
--   payloads: JSONB array of message payloads
--   topic: Optional topic string for categorization
--   idempotency_keys: Optional array of idempotency keys (must match payload count when provided)
--   visible_at: Optional timestamp when messages should become visible (default: now)
-- Returns: bigint[] - inserted message IDs
CREATE OR REPLACE FUNCTION cb_send(
    queue text,
    payloads jsonb[],
    topic text = null,
    idempotency_keys text[] = null,
    visible_at timestamptz = null
)
RETURNS bigint[]
LANGUAGE plpgsql AS $$
DECLARE
    _visible_at timestamptz := coalesce(cb_send.visible_at, now());
    _q_table text := cb_table_name(cb_send.queue, 'q');
    _ids bigint[];
BEGIN
    IF cb_send.idempotency_keys IS NOT NULL AND cardinality(cb_send.idempotency_keys) <> cardinality(cb_send.payloads) THEN
        RAISE EXCEPTION 'cb: idempotency_keys length must match payloads length';
    END IF;

    EXECUTE format(
        $QUERY$
        WITH payload_list AS (
            SELECT
                payload,
                ordinality,
                CASE
                    WHEN $4 IS NULL THEN NULL
                    ELSE $4[ordinality]
                END AS idempotency_key
            FROM unnest($1) WITH ORDINALITY AS t(payload, ordinality)
        ),
        ins AS (
            INSERT INTO %I (topic, payload, idempotency_key, visible_at)
            SELECT $2, payload, idempotency_key, $3
            FROM payload_list
            ORDER BY ordinality
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
        )
        SELECT coalesce(array_agg(id ORDER BY id), '{}'::bigint[])
        FROM ins
        $QUERY$,
        _q_table
    )
    USING cb_send.payloads,
          cb_send.topic,
            _visible_at,
            cb_send.idempotency_keys
    INTO _ids;

    RETURN _ids;
END
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION IF EXISTS cb_send(text, jsonb[], text, text[], timestamptz);
