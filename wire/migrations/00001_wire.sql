-- +goose up

-- The wire module: ephemeral browser push and the durable per-identity
-- inbox. Two NOTIFY channels, both fixed (channels scale with the
-- declared catalog, payloads carry the runtime coordinates):
--   <schema>.cbw        carries a whole event as JSON {sent_by, topic, message}
--   <schema>.cbw_inbox  carries an identity; the receiver re-reads its inbox
-- The old root-schema names (cb_notify, cb_notifications, cb_wire
-- channel) stay live beside these until the old schema is dropped;
-- nothing here reuses them.

-- One row per durable notification, keyed by identity. The row is a
-- perishable pointer to a durable fact: the result it points at lives
-- elsewhere (a run row, a record); the row is only the prompt to look.
-- id orders one identity's inbox and is the poll cursor. message is
-- opaque text; renderers and apps decode it.
--
-- Three timestamps, set once each and never cleared:
--   created_at  the row exists (delivered)
--   seen_at     rendered in the identity's list; unseen count drives badges
--   read_at     the identity opened or acted on it; setting read_at also
--               sets seen_at, an opened row must leave the badge count
CREATE TABLE cb_wire_inbox (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    identity text NOT NULL,
    topic text NOT NULL,
    message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    seen_at timestamptz,
    read_at timestamptz,
    expires_at timestamptz, -- relevance window; past it the row is not returned and the prune deletes it
    CONSTRAINT cb_wire_inbox_expires_at_valid CHECK (expires_at IS NULL OR expires_at > created_at)
);

-- The unseen poll path: one identity's unseen rows in cursor order.
CREATE INDEX cb_wire_inbox_unseen_idx ON cb_wire_inbox (identity, id)
    WHERE seen_at IS NULL;

-- The expiry sweep.
CREATE INDEX cb_wire_inbox_expires_at_idx ON cb_wire_inbox (expires_at)
    WHERE expires_at IS NOT NULL;

-- +goose statementbegin
-- Sends an ephemeral event to every wire in every process: pg_notify on
-- the module's bus channel, nothing stored. Delivery is at-most-once —
-- a process that is down or disconnected misses the event; clients
-- catch up by polling the inbox or re-reading their own state.
-- The payload must fit NOTIFY's 8000-byte limit: send a pointer to
-- state, not the state. An oversized payload raises in the caller's
-- transaction.
-- sent_by names the sending wire so it can skip the echo of its own
-- send; NULL means no sender to skip.
CREATE FUNCTION cb_wire_notify(
    topic text,
    message text DEFAULT NULL,
    sent_by text DEFAULT NULL
)
RETURNS void LANGUAGE sql AS $$
    SELECT pg_notify(
        current_schema || '.cbw',
        json_build_object(
            'sent_by', cb_wire_notify.sent_by,
            'topic', cb_wire_notify.topic,
            'message', cb_wire_notify.message
        )::text
    );
$$;
-- +goose statementend

-- +goose statementbegin
-- Appends a durable notification to an identity's inbox and nudges that
-- identity's connected clients to re-read it: the insert and the
-- pg_notify live in one body so a caller in any language gets both.
-- Returns the new row's id, the caller's cursor value.
-- Callable inside the caller's transaction: the row commits atomically
-- with the app's writes, and NOTIFY fires only on commit — a rollback
-- delivers neither row nor nudge. Exactly-once in the store,
-- at-most-once on the nudge; a client that misses the nudge finds the
-- row on its next poll.
-- The nudge carries the identity, not the row: the receiving wire tells
-- that identity's connections to poll, and the poll is where rendering
-- and seen-tracking happen.
-- An empty identity raises IRD01: the inbox is identity-keyed, and a
-- row no identity can address is meaningless.
CREATE FUNCTION cb_wire_notify_durable(
    identity text,
    topic text,
    message text DEFAULT NULL,
    expires_at timestamptz DEFAULT NULL
)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _id bigint; -- the new row's id, returned as the cursor value
BEGIN
    IF cb_wire_notify_durable.identity IS NULL OR cb_wire_notify_durable.identity = '' THEN
        RAISE EXCEPTION 'catbird: identity cannot be empty' USING ERRCODE = 'IRD01';
    END IF;

    INSERT INTO cb_wire_inbox (identity, topic, message, expires_at)
    VALUES (
        cb_wire_notify_durable.identity,
        cb_wire_notify_durable.topic,
        cb_wire_notify_durable.message,
        cb_wire_notify_durable.expires_at
    )
    RETURNING id INTO _id;

    PERFORM pg_notify(current_schema || '.cbw_inbox', cb_wire_notify_durable.identity);

    RETURN _id;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks an identity's unseen rows with id at or below the given id as
-- seen and returns the number of rows marked. The id bound is
-- load-bearing: it must not mark rows that arrived between a reader's
-- fetch and its ack. Whole-inbox scope only — one inbox can hold
-- several topic subsets whose ids interleave, and a range ack would
-- clobber a sibling subset's unseen rows; subset-scoped acks use
-- cb_wire_mark_seen(ids).
CREATE FUNCTION cb_wire_mark_seen_until(identity text, id bigint)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _marked bigint;
BEGIN
    UPDATE cb_wire_inbox i
    SET seen_at = now()
    WHERE i.identity = cb_wire_mark_seen_until.identity
      AND i.id <= cb_wire_mark_seen_until.id
      AND i.seen_at IS NULL;

    GET DIAGNOSTICS _marked = ROW_COUNT;
    RETURN _marked;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks exactly the identity's unseen rows named in ids as seen and
-- returns the number of rows marked. The precise sibling of
-- cb_wire_mark_seen_until, for acking one topic subset of an inbox.
CREATE FUNCTION cb_wire_mark_seen(identity text, ids bigint[])
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _marked bigint;
BEGIN
    UPDATE cb_wire_inbox i
    SET seen_at = now()
    WHERE i.identity = cb_wire_mark_seen.identity
      AND i.id = ANY (cb_wire_mark_seen.ids)
      AND i.seen_at IS NULL;

    GET DIAGNOSTICS _marked = ROW_COUNT;
    RETURN _marked;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks one row as read — the identity opened or acted on it. Reading
-- implies seeing, so an unseen row gets its seen_at stamped too; each
-- timestamp keeps its first value. Returns whether the row exists:
-- marking an already-read row changes nothing and still returns true.
CREATE FUNCTION cb_wire_mark_read(identity text, id bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cb_wire_inbox i
    SET read_at = coalesce(i.read_at, now()),
        seen_at = coalesce(i.seen_at, now())
    WHERE i.identity = cb_wire_mark_read.identity
      AND i.id = cb_wire_mark_read.id;

    RETURN FOUND;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks the identity's unread rows with id at or below the given id as
-- read, stamping seen_at on the way (reading implies seeing), and
-- returns the number of rows marked. The watermark sibling of
-- cb_wire_mark_read, for "mark all as read".
CREATE FUNCTION cb_wire_mark_read_until(identity text, id bigint)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _marked bigint;
BEGIN
    UPDATE cb_wire_inbox i
    SET read_at = now(),
        seen_at = coalesce(i.seen_at, now())
    WHERE i.identity = cb_wire_mark_read_until.identity
      AND i.id <= cb_wire_mark_read_until.id
      AND i.read_at IS NULL;

    GET DIAGNOSTICS _marked = ROW_COUNT;
    RETURN _marked;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Deletes the rows the inbox is done with, one DELETE on the module's
-- tick. A row leaves when its explicit expires_at has passed — seen or
-- not, a stale prompt is not worth keeping — or when the identity is
-- done with it: read longer ago than read_older_than, seen longer ago
-- than seen_older_than, or older than max_age outright. A NULL
-- timestamp fails its age comparison, so unread rows pass the read
-- tier and unseen rows pass the seen tier by construction: a row that
-- was never seen and has no expires_at lives the full max_age.
-- Returns the number of rows deleted.
CREATE FUNCTION _cb_wire_prune_inbox(
    read_older_than interval,
    seen_older_than interval,
    max_age interval
)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _deleted bigint;
BEGIN
    DELETE FROM cb_wire_inbox i
    WHERE (i.expires_at IS NOT NULL AND i.expires_at <= now())
       OR i.read_at < now() - _cb_wire_prune_inbox.read_older_than
       OR i.seen_at < now() - _cb_wire_prune_inbox.seen_older_than
       OR i.created_at < now() - _cb_wire_prune_inbox.max_age;

    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END; $$;
-- +goose statementend

-- +goose down

DROP FUNCTION _cb_wire_prune_inbox(interval, interval, interval);
DROP FUNCTION cb_wire_mark_read_until(text, bigint);
DROP FUNCTION cb_wire_mark_read(text, bigint);
DROP FUNCTION cb_wire_mark_seen(text, bigint[]);
DROP FUNCTION cb_wire_mark_seen_until(text, bigint);
DROP FUNCTION cb_wire_notify_durable(text, text, text, timestamptz);
DROP FUNCTION cb_wire_notify(text, text, text);

DROP INDEX cb_wire_inbox_expires_at_idx;
DROP INDEX cb_wire_inbox_unseen_idx;
DROP TABLE cb_wire_inbox;
