-- +goose up

-- The wire module: delivering events to people. Every message is a row —
-- an inbox row (kept until read or expired), a relayed stream message
-- (the log keeps it), or a presence row (newest wins, evaporates on
-- silence). The NOTIFY channels carry addresses, never payloads, and both
-- are fixed (channels scale with the declared catalog, payloads carry the
-- runtime coordinates):
--   <schema>.cbw        a live frame: {stream, pos, topic} for a relayed
--                       message (the receiver fetches the row), or
--                       {topic} for a presence change (nothing to fetch)
--   <schema>.cbw_inbox  a recipient; the receiver re-reads its inbox
-- The old root-schema names (cb_notify, cb_notifications, cb_wire
-- channel) stay live beside these until the old schema is dropped;
-- nothing here reuses them.

-- One row per durable notification, keyed by recipient. The row carries
-- the event itself — topic and payload exactly as published — and is
-- rendered at read time. id orders one recipient's inbox and is the poll
-- cursor.
--
-- Three timestamps, set once each and never cleared:
--   created_at  the row exists (delivered)
--   seen_at     rendered in the recipient's list; unseen count drives badges
--   read_at     the recipient opened or acted on it; setting read_at also
--               sets seen_at, an opened row must leave the badge count
CREATE TABLE cb_wire_inbox (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    recipient text NOT NULL,
    topic text NOT NULL,
    payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    seen_at timestamptz,
    read_at timestamptz,
    expires_at timestamptz, -- relevance window; past it the row is not returned and the prune deletes it
    CONSTRAINT cb_wire_inbox_expires_at_valid CHECK (expires_at IS NULL OR expires_at > created_at)
);

-- The unseen poll path: one recipient's unseen rows in cursor order.
CREATE INDEX cb_wire_inbox_unseen_idx ON cb_wire_inbox (recipient, id)
    WHERE seen_at IS NULL;

-- The expiry sweep.
CREATE INDEX cb_wire_inbox_expires_at_idx ON cb_wire_inbox (expires_at)
    WHERE expires_at IS NOT NULL;

-- A relay forwards a stream's matching messages to the web, declared as a
-- row and delivered by the module's tick — the same shape as
-- cb_job_triggers, pointed at people instead of work. The relay owns the
-- cursor named after it on its stream; the cursor is the filter's home
-- and remembers how far delivery got. Per message the relay sends one
-- live frame (the address; connected clients whose token topics match
-- receive it) and writes one inbox row per addressed recipient — the
-- recipients the publisher named plus everyone subscribed to a matching
-- pattern.
CREATE TABLE cb_wire_relays (
    name text PRIMARY KEY CHECK (cb_valid_name(name)),
    -- No FK on stream: it lives in the other module's schema.
    stream text NOT NULL,
    -- The inbox relevance window, anchored at each message's created_at:
    -- an inbox row from this relay expires that long after the event
    -- happened, and a message already past its window is skipped quietly
    -- — a stalled relay catching up must not flood inboxes with stale
    -- rows granted fresh windows. NULL means no window; the retention
    -- tiers still apply.
    expires_after interval CHECK (expires_after IS NULL OR expires_after > interval '0'),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Who watches what: matching relayed messages land in the recipient's
-- inbox. Patterns are prefix-only — an exact topic, or a prefix followed
-- by '.#' ('order.#' covers order and everything under it); there is no
-- '*'. Matching is a B-tree probe: the deliverer expands a message's
-- topic into its handful of covering patterns and looks those up, so cost
-- follows the topic's length, not the table's size.
CREATE TABLE cb_wire_subscriptions (
    recipient text NOT NULL,
    pattern text NOT NULL,
    expires_at timestamptz, -- how long the watch lasts; NULL watches until unsubscribed
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT cb_wire_subscriptions_pkey PRIMARY KEY (recipient, pattern)
);

-- Where people are right now: one row per (topic, recipient), newest
-- payload wins, gone when the heartbeat stops. Not messages — nothing is
-- kept, nothing is addressed to anyone — so none of the inbox machinery
-- applies: a change nudges the topic and readers refetch the current
-- rows.
CREATE TABLE cb_wire_presence (
    topic text NOT NULL,
    recipient text NOT NULL,
    payload jsonb,
    expires_at timestamptz NOT NULL,
    CONSTRAINT cb_wire_presence_pkey PRIMARY KEY (topic, recipient)
);

-- The expiry sweep.
CREATE INDEX cb_wire_presence_expires_at_idx ON cb_wire_presence (expires_at);

-- +goose statementbegin
-- Appends a durable notification to a recipient's inbox and nudges that
-- recipient's connected clients to re-read it: the insert and the
-- pg_notify live in one body so a caller in any language gets both.
-- Returns the new row's id, the caller's cursor value.
-- Callable inside the caller's transaction: the row commits atomically
-- with the app's writes, and NOTIFY fires only on commit — a rollback
-- delivers neither row nor nudge. Exactly-once in the store,
-- at-most-once on the nudge; a client that misses the nudge finds the
-- row on its next poll.
-- The nudge carries the recipient, not the row: the receiving wire tells
-- that recipient's connections to poll, and the poll is where rendering
-- and seen-tracking happen.
-- An empty recipient or topic raises IRD01: the inbox is recipient-keyed
-- and rendered by topic; a row missing either cannot be delivered.
CREATE FUNCTION cb_wire_send(
    recipient text,
    topic text,
    payload jsonb DEFAULT NULL,
    expires_at timestamptz DEFAULT NULL
)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _id bigint; -- the new row's id, returned as the cursor value
BEGIN
    IF cb_wire_send.recipient IS NULL OR cb_wire_send.recipient = '' THEN
        RAISE EXCEPTION 'catbird: recipient cannot be empty' USING ERRCODE = 'IRD01';
    END IF;
    IF cb_wire_send.topic IS NULL OR cb_wire_send.topic = '' THEN
        RAISE EXCEPTION 'catbird: topic cannot be empty' USING ERRCODE = 'IRD01';
    END IF;

    INSERT INTO cb_wire_inbox (recipient, topic, payload, expires_at)
    VALUES (
        cb_wire_send.recipient,
        cb_wire_send.topic,
        cb_wire_send.payload,
        cb_wire_send.expires_at
    )
    RETURNING id INTO _id;

    PERFORM pg_notify(current_schema || '.cbw_inbox', cb_wire_send.recipient);

    RETURN _id;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks a recipient's unseen rows with id at or below the given id as
-- seen and returns the number of rows marked. The id bound is
-- load-bearing: it must not mark rows that arrived between a reader's
-- fetch and its ack. Whole-inbox scope only — one inbox can hold
-- several topic subsets whose ids interleave, and a range ack would
-- clobber a sibling subset's unseen rows; subset-scoped acks use
-- cb_wire_mark_seen(ids).
CREATE FUNCTION cb_wire_mark_seen_until(recipient text, id bigint)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _marked bigint;
BEGIN
    UPDATE cb_wire_inbox i
    SET seen_at = now()
    WHERE i.recipient = cb_wire_mark_seen_until.recipient
      AND i.id <= cb_wire_mark_seen_until.id
      AND i.seen_at IS NULL;

    GET DIAGNOSTICS _marked = ROW_COUNT;
    RETURN _marked;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks exactly the recipient's unseen rows named in ids as seen and
-- returns the number of rows marked. The precise sibling of
-- cb_wire_mark_seen_until, for acking one topic subset of an inbox.
CREATE FUNCTION cb_wire_mark_seen(recipient text, ids bigint[])
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _marked bigint;
BEGIN
    UPDATE cb_wire_inbox i
    SET seen_at = now()
    WHERE i.recipient = cb_wire_mark_seen.recipient
      AND i.id = ANY (cb_wire_mark_seen.ids)
      AND i.seen_at IS NULL;

    GET DIAGNOSTICS _marked = ROW_COUNT;
    RETURN _marked;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks one row as read — the recipient opened or acted on it. Reading
-- implies seeing, so an unseen row gets its seen_at stamped too; each
-- timestamp keeps its first value. Returns whether the row exists:
-- marking an already-read row changes nothing and still returns true.
CREATE FUNCTION cb_wire_mark_read(recipient text, id bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cb_wire_inbox i
    SET read_at = coalesce(i.read_at, now()),
        seen_at = coalesce(i.seen_at, now())
    WHERE i.recipient = cb_wire_mark_read.recipient
      AND i.id = cb_wire_mark_read.id;

    RETURN FOUND;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Marks the recipient's unread rows with id at or below the given id as
-- read, stamping seen_at on the way (reading implies seeing), and
-- returns the number of rows marked. The watermark sibling of
-- cb_wire_mark_read, for "mark all as read".
CREATE FUNCTION cb_wire_mark_read_until(recipient text, id bigint)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _marked bigint;
BEGIN
    UPDATE cb_wire_inbox i
    SET read_at = now(),
        seen_at = coalesce(i.seen_at, now())
    WHERE i.recipient = cb_wire_mark_read_until.recipient
      AND i.id <= cb_wire_mark_read_until.id
      AND i.read_at IS NULL;

    GET DIAGNOSTICS _marked = ROW_COUNT;
    RETURN _marked;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Declares a relay whole: creating and updating are the same call, an
-- identical declaration writes nothing. The filter — topic pattern and
-- condition — is stored on the cursor the relay owns and nowhere else;
-- this function passes it through cb_stream_define_cursor, which also
-- checks that the stream exists and the filter compiles, so a broken
-- relay is refused here, not discovered by the tick. The cursor's
-- position is delivery state and stays put on redeclare; start_pos, when
-- given, sets it deliberately: 0 delivers the stream from the beginning,
-- N from after N. When creating, NULL starts at the tail — only messages
-- published from now on deliver.
CREATE FUNCTION cb_wire_define_relay(
    name          text,
    stream        text,
    topic         text     DEFAULT NULL,
    condition     text     DEFAULT NULL,
    start_pos     bigint   DEFAULT NULL,
    expires_after interval DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _old cb_wire_relays;
BEGIN
    IF to_regclass('cb_streams') IS NULL THEN
        RAISE EXCEPTION 'catbird: stream schema required (a relay reads a stream; install the stream module first)'
            USING ERRCODE = 'IRD03';
    END IF;

    IF NOT cb_valid_name(cb_wire_define_relay.name) THEN
        RAISE EXCEPTION 'catbird: invalid relay name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_wire_define_relay.name USING ERRCODE = 'IRD01';
    END IF;

    -- The row lock serializes this declaration against the delivery tick
    -- and reads the old stream name for the move below.
    SELECT r.* INTO _old FROM cb_wire_relays r
    WHERE r.name = cb_wire_define_relay.name
    FOR UPDATE;

    -- A relay moved to another stream leaves no cursor behind.
    IF FOUND AND _old.stream <> cb_wire_define_relay.stream THEN
        PERFORM cb_stream_delete_cursor(_old.stream, _old.name);
    END IF;

    INSERT INTO cb_wire_relays AS r (name, stream, expires_after)
    VALUES (cb_wire_define_relay.name, cb_wire_define_relay.stream, cb_wire_define_relay.expires_after)
    ON CONFLICT ON CONSTRAINT cb_wire_relays_pkey DO UPDATE
    SET stream        = excluded.stream,
        expires_after = excluded.expires_after
    -- an identical declaration writes nothing
    WHERE (r.stream, r.expires_after) IS DISTINCT FROM (excluded.stream, excluded.expires_after);

    -- The cursor's own change-guard writes nothing when the filter is
    -- unchanged and start_pos is not given, so calling it every time
    -- keeps the no-op property.
    PERFORM cb_stream_define_cursor(
        cb_wire_define_relay.stream,
        cb_wire_define_relay.name,
        cb_wire_define_relay.start_pos,
        cb_wire_define_relay.topic,
        cb_wire_define_relay.condition);
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Removes a relay and its cursor. Reports whether one existed; deleting
-- a missing relay is a no-op. Matches between the cursor and the
-- stream's head are gone with it — the relay is its cursor's only reader.
CREATE FUNCTION cb_wire_delete_relay(name text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _old cb_wire_relays;
BEGIN
    DELETE FROM cb_wire_relays r
    WHERE r.name = cb_wire_delete_relay.name
    RETURNING r.* INTO _old;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    -- The guard covers a wire-only install where relay rows were
    -- restored or hand-written: delete what exists, never raise.
    IF to_regclass('cb_streams') IS NOT NULL THEN
        PERFORM cb_stream_delete_cursor(_old.stream, _old.name);
    END IF;
    RETURN true;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Declares a watch: matching relayed messages land in the recipient's
-- inbox until expires_at (NULL watches until unsubscribed). Creating and
-- updating are the same call; an identical declaration writes nothing.
-- The pattern is prefix-only — an exact topic or a prefix followed by
-- '.#'; '*' is refused. 'p.#' also covers p itself, matching the topic
-- languages elsewhere.
CREATE FUNCTION cb_wire_subscribe(
    recipient  text,
    pattern    text,
    expires_at timestamptz DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF cb_wire_subscribe.recipient IS NULL OR cb_wire_subscribe.recipient = '' THEN
        RAISE EXCEPTION 'catbird: recipient cannot be empty' USING ERRCODE = 'IRD01';
    END IF;
    IF cb_wire_subscribe.pattern IS NULL
       OR cb_wire_subscribe.pattern !~ '^(#|[^.*#\s]+(\.[^.*#\s]+)*(\.#)?)$' THEN
        RAISE EXCEPTION 'catbird: invalid subscription pattern %; use an exact topic or a prefix ending in .# (no *)',
            cb_wire_subscribe.pattern USING ERRCODE = 'IRD01';
    END IF;

    INSERT INTO cb_wire_subscriptions AS s (recipient, pattern, expires_at)
    VALUES (cb_wire_subscribe.recipient, cb_wire_subscribe.pattern, cb_wire_subscribe.expires_at)
    ON CONFLICT ON CONSTRAINT cb_wire_subscriptions_pkey DO UPDATE
    SET expires_at = excluded.expires_at
    -- an identical declaration writes nothing
    WHERE s.expires_at IS DISTINCT FROM excluded.expires_at;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Removes a watch. Returns false when there was none.
CREATE FUNCTION cb_wire_unsubscribe(recipient text, pattern text)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM cb_wire_subscriptions s
    WHERE s.recipient = cb_wire_unsubscribe.recipient
      AND s.pattern   = cb_wire_unsubscribe.pattern;
    RETURN FOUND;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- The patterns that cover a topic: the topic itself, '#', and every
-- prefix with '.#' appended — for 'a.b.c' that is {a.b.c, #, a.#, a.b.#,
-- a.b.c.#}. The deliverer probes the subscription table with this array,
-- so matching costs the topic's length, never the table's size.
CREATE FUNCTION _cb_wire_topic_patterns(topic text)
RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
    SELECT ARRAY[_cb_wire_topic_patterns.topic, '#']
        || coalesce(
            (SELECT array_agg(array_to_string(t.tokens[1:i], '.') || '.#')
             FROM (SELECT string_to_array(_cb_wire_topic_patterns.topic, '.') AS tokens) t,
                  generate_series(1, array_length(t.tokens, 1)) AS i),
            '{}');
$$;
-- +goose statementend

-- +goose statementbegin
-- Delivers one relay's next batch: read the matching messages after the
-- cursor, send one live frame and write the addressed inbox rows per
-- message, advance the cursor — one transaction (cb_stream_read advances
-- in the same call). A raise rolls the whole batch back, cursor
-- included, so inbox rows are exactly-once: insert and advance share one
-- commit. The live frame is the message's address — the receiving wires
-- fetch the row, render it and push it; NOTIFY fires only on commit, so
-- the push is once per message. Returns how many messages delivered.
--
-- A message with no topic is skipped: wire routes and renders by topic,
-- so there is nothing to deliver it as. The cursor advances over it like
-- over any non-match.
CREATE FUNCTION cb_wire_relay_deliver(relay text, batch_size int DEFAULT 100)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _relay cb_wire_relays;
    _message record; -- a cb_stream_messages row; declared loosely so this
                     -- function never names the other module's row type
    _expires_at timestamptz;
    _recipient text;
    _n int := 0;
BEGIN
    IF to_regclass('cb_streams') IS NULL THEN
        RAISE EXCEPTION 'catbird: stream schema required (a relay reads a stream; install the stream module first)'
            USING ERRCODE = 'IRD03';
    END IF;

    -- One deliverer per relay: a concurrent tick skips instead of
    -- queueing, and a redeclare waits for the in-flight batch to commit.
    SELECT r.* INTO _relay FROM cb_wire_relays r
    WHERE r.name = cb_wire_relay_deliver.relay
    FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    FOR _message IN
        SELECT * FROM cb_stream_read(_relay.stream, _relay.name,
                                     cb_wire_relay_deliver.batch_size)
    LOOP
        IF _message.topic IS NULL THEN
            CONTINUE;
        END IF;

        -- The live frame: the address, never the payload. Receivers fetch
        -- the row from the log, so frame size is constant and the log
        -- stays the single copy.
        PERFORM pg_notify(current_schema || '.cbw', json_build_object(
            'stream', _relay.stream,
            'pos',    _message.pos,
            'topic',  _message.topic)::text);

        -- The inbox rows: the recipients the publisher named, plus
        -- everyone subscribed to a covering pattern. The relevance window
        -- anchors at the message, not at delivery: a message already past
        -- its window is skipped quietly — stale events must not arrive
        -- with fresh windows when a stalled relay catches up.
        _expires_at := CASE WHEN _relay.expires_after IS NOT NULL
                            THEN _message.created_at + _relay.expires_after END;
        IF _expires_at IS NULL OR _expires_at > now() THEN
            FOR _recipient IN
                SELECT unnest(coalesce(_message.recipients, ARRAY[]::text[]))
                UNION
                SELECT s.recipient FROM cb_wire_subscriptions s
                WHERE s.pattern = ANY (_cb_wire_topic_patterns(_message.topic))
                  AND (s.expires_at IS NULL OR s.expires_at > now())
            LOOP
                INSERT INTO cb_wire_inbox (recipient, topic, payload, expires_at)
                VALUES (_recipient, _message.topic, _message.payload, _expires_at);
                -- one nudge per recipient; NOTIFY dedups identical
                -- payloads within the transaction, so a batch of matches
                -- nudges each recipient once
                PERFORM pg_notify(current_schema || '.cbw_inbox', _recipient);
            END LOOP;
        END IF;

        _n := _n + 1;
    END LOOP;

    RETURN _n;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Records that a recipient is at a topic — editing a record, viewing a
-- page — with a payload for the detail (which field, what state) and a
-- heartbeat: calling again re-arms expires_at, and silence lets the row
-- evaporate. Nudges the topic's connected watchers only when something
-- visible changed — the row is new, came back from expired, or changed
-- payload; a bare heartbeat re-arm is silent, so heartbeats never spam
-- refetches.
CREATE FUNCTION cb_wire_appear(
    topic     text,
    recipient text,
    payload   jsonb    DEFAULT NULL,
    ttl       interval DEFAULT interval '30 seconds'
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _old cb_wire_presence;
    _changed boolean := true;
BEGIN
    IF cb_wire_appear.topic IS NULL OR cb_wire_appear.topic = '' THEN
        RAISE EXCEPTION 'catbird: topic cannot be empty' USING ERRCODE = 'IRD01';
    END IF;
    IF cb_wire_appear.recipient IS NULL OR cb_wire_appear.recipient = '' THEN
        RAISE EXCEPTION 'catbird: recipient cannot be empty' USING ERRCODE = 'IRD01';
    END IF;
    IF cb_wire_appear.ttl IS NULL OR cb_wire_appear.ttl <= interval '0' THEN
        RAISE EXCEPTION 'catbird: ttl must be positive' USING ERRCODE = 'IRD01';
    END IF;

    SELECT p.* INTO _old FROM cb_wire_presence p
    WHERE p.topic = cb_wire_appear.topic AND p.recipient = cb_wire_appear.recipient
    FOR UPDATE;

    IF FOUND THEN
        _changed := _old.expires_at <= now()
                 OR _old.payload IS DISTINCT FROM cb_wire_appear.payload;
        UPDATE cb_wire_presence p
        SET payload = cb_wire_appear.payload,
            expires_at = now() + cb_wire_appear.ttl
        WHERE p.topic = cb_wire_appear.topic AND p.recipient = cb_wire_appear.recipient;
    ELSE
        INSERT INTO cb_wire_presence (topic, recipient, payload, expires_at)
        VALUES (cb_wire_appear.topic, cb_wire_appear.recipient,
                cb_wire_appear.payload, now() + cb_wire_appear.ttl);
    END IF;

    IF _changed THEN
        PERFORM pg_notify(current_schema || '.cbw',
            json_build_object('topic', cb_wire_appear.topic)::text);
    END IF;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- The polite leave: removes the row at once and nudges the topic's
-- watchers. Silence works too — the row expires on its own — this just
-- spares the room the ttl of a ghost. Returns whether a row existed.
CREATE FUNCTION cb_wire_disappear(topic text, recipient text)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM cb_wire_presence p
    WHERE p.topic = cb_wire_disappear.topic
      AND p.recipient = cb_wire_disappear.recipient;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    PERFORM pg_notify(current_schema || '.cbw',
        json_build_object('topic', cb_wire_disappear.topic)::text);
    RETURN true;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Deletes the inbox rows the recipients are done with, one DELETE on the
-- module's tick. A row leaves when its explicit expires_at has passed —
-- seen or not, a stale prompt is not worth keeping — or when the
-- recipient is done with it: read longer ago than read_older_than, seen
-- longer ago than seen_older_than, or older than max_age outright. A
-- NULL timestamp fails its age comparison, so unread rows pass the read
-- tier and unseen rows pass the seen tier by construction: a row that
-- was never seen and has no expires_at lives the full max_age.
-- Returns the number of rows deleted.
CREATE FUNCTION cb_wire_prune_inbox(
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
       OR i.read_at < now() - cb_wire_prune_inbox.read_older_than
       OR i.seen_at < now() - cb_wire_prune_inbox.seen_older_than
       OR i.created_at < now() - cb_wire_prune_inbox.max_age;

    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Deletes lapsed watches. Returns the number of rows deleted.
CREATE FUNCTION cb_wire_prune_subscriptions()
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _deleted bigint;
BEGIN
    DELETE FROM cb_wire_subscriptions s
    WHERE s.expires_at IS NOT NULL AND s.expires_at <= now();

    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Deletes expired presence rows and nudges each topic that lost one, so
-- watching pages drop people whose heartbeat stopped. Expired rows never
-- render anyway — reads filter on expires_at — the nudge is what turns
-- the silent expiry into a visible refetch. Returns the number of rows
-- deleted.
CREATE FUNCTION cb_wire_prune_presence()
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _topics text[];
    _topic text;
    _deleted bigint;
BEGIN
    WITH gone AS (
        DELETE FROM cb_wire_presence p
        WHERE p.expires_at <= now()
        RETURNING p.topic
    )
    SELECT count(*), array_agg(DISTINCT gone.topic)
    INTO _deleted, _topics
    FROM gone;

    FOREACH _topic IN ARRAY coalesce(_topics, '{}') LOOP
        PERFORM pg_notify(current_schema || '.cbw',
            json_build_object('topic', _topic)::text);
    END LOOP;

    RETURN _deleted;
END; $$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_wire_prune_presence();
DROP FUNCTION cb_wire_prune_subscriptions();
DROP FUNCTION cb_wire_prune_inbox(interval, interval, interval);
DROP FUNCTION cb_wire_disappear(text, text);
DROP FUNCTION cb_wire_appear(text, text, jsonb, interval);
DROP FUNCTION cb_wire_relay_deliver(text, int);
DROP FUNCTION _cb_wire_topic_patterns(text);
DROP FUNCTION cb_wire_unsubscribe(text, text);
DROP FUNCTION cb_wire_subscribe(text, text, timestamptz);
DROP FUNCTION cb_wire_delete_relay(text);
DROP FUNCTION cb_wire_define_relay(text, text, text, text, bigint, interval);
DROP FUNCTION cb_wire_mark_read_until(text, bigint);
DROP FUNCTION cb_wire_mark_read(text, bigint);
DROP FUNCTION cb_wire_mark_seen(text, bigint[]);
DROP FUNCTION cb_wire_mark_seen_until(text, bigint);
DROP FUNCTION cb_wire_send(text, text, jsonb, timestamptz);

DROP INDEX cb_wire_presence_expires_at_idx;
DROP TABLE cb_wire_presence;
DROP TABLE cb_wire_subscriptions;
DROP TABLE cb_wire_relays;
DROP INDEX cb_wire_inbox_expires_at_idx;
DROP INDEX cb_wire_inbox_unseen_idx;
DROP TABLE cb_wire_inbox;
