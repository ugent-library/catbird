-- +goose up

-- Every error this API raises carries one of these SQLSTATE codes, so
-- clients classify by code instead of parsing message text:
--   IRD01  invalid argument  the call itself is malformed
--   IRD02  not defined       the stream, subscription, cursor or schedule does not exist
--   IRD03  not found         the message does not exist
-- Why IRD: the natural prefix CB sits in the SQLSTATE class range the
-- standard reserves for itself (0-4, A-H).

-- cb_valid_name, cb_forever, cb_backoff and cb_backoff_kind live in the
-- kernel's SQL unit (internal/migrate/migrations), applied before this file.

CREATE TYPE cb_ref_kind AS ENUM ('message', 'pending');
CREATE TYPE cb_fail_policy AS ENUM ('keep', 'delete');
CREATE TYPE cb_catch_up_policy AS ENUM ('skip', 'all');

-- +goose statementbegin
CREATE FUNCTION _cb_stream_notify(stream text, payload text)
RETURNS void LANGUAGE sql AS $$
    SELECT pg_notify(current_schema || '.cbs_' || stream, payload);
$$;
-- +goose statementend

-- +goose statementbegin
-- Validate a topic pattern and compile it to a regex. '*' matches one
-- segment, '#' matches zero or more trailing segments and must be the
-- final segment. Raises for anything else.
CREATE FUNCTION _cb_stream_compile_topic(pattern text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    _tokens text[];
    _token text;
    _i int;
    _n int;
    _regex text;
BEGIN
    IF _cb_stream_compile_topic.pattern IS NULL OR _cb_stream_compile_topic.pattern = '' THEN
        RAISE EXCEPTION 'catbird: topic pattern cannot be empty' USING ERRCODE = 'IRD01';
    END IF;

    IF _cb_stream_compile_topic.pattern !~ '^[a-zA-Z0-9._#*-]+$' THEN
        RAISE EXCEPTION 'catbird: topic pattern % may only contain a-z, A-Z, 0-9, ., _, -, * and #',
            _cb_stream_compile_topic.pattern USING ERRCODE = 'IRD01';
    END IF;

    IF _cb_stream_compile_topic.pattern ~ '\.\.'
    OR _cb_stream_compile_topic.pattern ~ '(^\.|\.$)' THEN
        RAISE EXCEPTION 'catbird: topic pattern % cannot contain empty segments',
            _cb_stream_compile_topic.pattern USING ERRCODE = 'IRD01';
    END IF;

    _tokens := string_to_array(_cb_stream_compile_topic.pattern, '.');
    _n := array_length(_tokens, 1);
    FOR _i IN 1.._n LOOP
        _token := _tokens[_i];
        IF _token = '#' AND _i <> _n THEN
            RAISE EXCEPTION 'catbird: # must be the final segment of topic pattern %',
                _cb_stream_compile_topic.pattern USING ERRCODE = 'IRD01';
        END IF;
        IF _token <> '*' AND _token <> '#' AND _token ~ '[*#]' THEN
            RAISE EXCEPTION 'catbird: * and # must be whole segments in topic pattern %',
                _cb_stream_compile_topic.pattern USING ERRCODE = 'IRD01';
        END IF;
    END LOOP;

    IF _cb_stream_compile_topic.pattern = '#' THEN
        RETURN '^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)*$';
    END IF;

    -- Strip a trailing '.#' (its zero-or-more tail is appended below),
    -- escape literal dots, then widen '*' to one-segment matches.
    _regex := regexp_replace(_cb_stream_compile_topic.pattern, '\.#$', '');
    _regex := regexp_replace(_regex, '\.', '\\.', 'g');
    _regex := regexp_replace(_regex, '\*', '[a-zA-Z0-9_-]+', 'g');

    IF _cb_stream_compile_topic.pattern ~ '\.#$' THEN
        -- 'a.#' also matches the bare 'a': the tail may be zero segments
        RETURN '^' || _regex || '(\.[a-zA-Z0-9_-]+)*$';
    END IF;
    RETURN '^' || _regex || '$';
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Parse a condition into per-column predicates, once, at registration.
-- Conjuncts are joined by '&&'; each is either
-- exists($.headers.a.b) / exists($.payload.a.b),
-- $.headers.a.b == <scalar> with <scalar> a "string", a number, true or
-- false, or $.recipients == "name" — true when the recipients the
-- publisher named contain the name. Header and payload conjuncts compile
-- to jsonpath; recipients conjuncts collect into the array of required
-- names, probed with @> against the recipients column. Header keys
-- starting with cb_ are refused: that namespace is the engine's.
-- Anything else raises.
CREATE FUNCTION _cb_stream_compile_condition(
    condition text,
    OUT headers_condition jsonpath,
    OUT payload_condition jsonpath,
    OUT recipients_condition text[]
)
LANGUAGE plpgsql AS $$
DECLARE
    _conjunct text;
    _m text[];
    _pred text;
    _headers text[] := '{}';
    _payload text[] := '{}';
    _recipients text[] := '{}';
BEGIN
    IF _cb_stream_compile_condition.condition IS NULL
    OR btrim(_cb_stream_compile_condition.condition) = '' THEN
        RAISE EXCEPTION 'catbird: condition cannot be empty' USING ERRCODE = 'IRD01';
    END IF;

    FOREACH _conjunct IN ARRAY regexp_split_to_array(_cb_stream_compile_condition.condition, '\s*&&\s*') LOOP
        -- recipient membership: $.recipients == "name". Every named name
        -- must be present; a message with no recipients never matches.
        _m := regexp_match(_conjunct, '^\s*\$\.recipients\s*==\s*"([^"\\]*)"\s*$');
        IF _m IS NOT NULL THEN
            _recipients := _recipients || _m[1];
            CONTINUE;
        END IF;

        -- nested-key existence: exists($.headers.a.b)
        _m := regexp_match(_conjunct,
            '^\s*exists\(\$\.(headers|payload)((?:\.[a-zA-Z_][a-zA-Z0-9_]*)+)\)\s*$');
        IF _m IS NOT NULL THEN
            _pred := 'exists($' || _m[2] || ')';
        ELSE
            -- nested-key scalar equality: $.payload.a.b == <scalar>
            _m := regexp_match(_conjunct,
                '^\s*\$\.(headers|payload)((?:\.[a-zA-Z_][a-zA-Z0-9_]*)+)\s*==\s*("[^"\\]*"|-?[0-9]+(?:\.[0-9]+)?|true|false)\s*$');
            IF _m IS NULL THEN
                RAISE EXCEPTION 'catbird: unsupported condition near "%"; use exists($.headers.a.b), $.payload.a.b == <scalar> or $.recipients == "name", joined with &&',
                    _conjunct USING ERRCODE = 'IRD01';
            END IF;
            _pred := '$' || _m[2] || ' == ' || _m[3];
        END IF;

        -- cb_ header keys are the engine's own storage; conditions read
        -- them through their first-class forms.
        IF _m[1] = 'headers' AND left(_m[2], 4) = '.cb_' THEN
            RAISE EXCEPTION 'catbird: header keys starting with cb_ are reserved; recipients are matched with $.recipients == "name"'
                USING ERRCODE = 'IRD01';
        END IF;

        IF _m[1] = 'headers' THEN
            _headers := _headers || _pred;
        ELSE
            _payload := _payload || _pred;
        END IF;
    END LOOP;

    IF array_length(_headers, 1) > 0 THEN
        headers_condition := array_to_string(_headers, ' && ')::jsonpath;
    END IF;
    IF array_length(_payload, 1) > 0 THEN
        payload_condition := array_to_string(_payload, ' && ')::jsonpath;
    END IF;
    IF array_length(_recipients, 1) > 0 THEN
        recipients_condition := _recipients;
    END IF;
END; $$;
-- +goose statementend

CREATE TABLE cb_streams (
    name text PRIMARY KEY CHECK (cb_valid_name(name)),
    last_pos bigint NOT NULL DEFAULT 0,
    retention interval NOT NULL DEFAULT cb_forever()
        CHECK (retention = cb_forever() OR retention > interval '0')
);

CREATE TABLE cb_stream_cursors (
    stream text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    name text NOT NULL CHECK (cb_valid_name(name)),
    pos bigint NOT NULL DEFAULT 0, -- how far this cursor has read: everything at or below this position is acked
    topic text,                 -- topic pattern; NULL reads every topic
    topic_regex text,           -- compiled by _cb_stream_compile_topic at ensure
    condition text,             -- headers/payload/recipients expression; NULL reads everything
    headers_condition jsonpath, -- disassembled by _cb_stream_compile_condition at ensure
    payload_condition jsonpath,
    recipients_condition text[],
    PRIMARY KEY (stream, name)
);

-- Messages that are delayed. cb_stream_deliver_pending moves them
-- into cb_stream_messages when they are due.
CREATE TABLE cb_stream_pending (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    stream text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    topic text,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(headers) = 'object'),
    recipients text[],
    deliver_at timestamptz NOT NULL,
    key text -- set when delayed message has a deduplication key
);

CREATE INDEX ON cb_stream_pending (deliver_at);

CREATE TABLE cb_stream_keys (
    stream text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    key            text NOT NULL,
    ref_kind       cb_ref_kind NOT NULL,
    ref_id         bigint NOT NULL,
    ref_created_at timestamptz NOT NULL DEFAULT now(), -- bumped when ref_kind changes from 'pending' to 'message'
    PRIMARY KEY (stream, key)
);

CREATE INDEX ON cb_stream_keys (stream, ref_created_at);

-- No identity column: identity on partitioned tables needs Postgres 17, and
-- the floor is 14 (plan D16). Explicit sequence instead; OWNED BY keeps
-- pg_get_serial_sequence() working in cb_stream_publish.
CREATE SEQUENCE cb_stream_messages_id_seq;

CREATE TABLE cb_stream_messages (
    id bigint NOT NULL DEFAULT nextval('cb_stream_messages_id_seq'),
    stream text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    pos bigint,
    topic text,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(headers) = 'object'),
    recipients text[], -- who the publisher named; matched by $.recipients, delivered to inboxes by relays
    created_at timestamptz NOT NULL DEFAULT now()
)
PARTITION BY LIST (stream);

ALTER SEQUENCE cb_stream_messages_id_seq OWNED BY cb_stream_messages.id;

CREATE INDEX ON cb_stream_messages (stream, pos);

CREATE TABLE cb_stream_subscriptions (
    stream text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    name text NOT NULL CHECK (cb_valid_name(name)),
    claimed_pos bigint NOT NULL DEFAULT 0, -- everything at or below this position is claimed
    closed_pos bigint NOT NULL DEFAULT 0, -- everything at or below this position is claimed and closed
    claim_ttl interval NOT NULL,
    claim_batch_size int NOT NULL DEFAULT 100 CHECK (claim_batch_size > 0),
    max_attempts int NOT NULL CHECK (max_attempts > 0),
    backoff_kind cb_backoff_kind NOT NULL,
    backoff_base interval NOT NULL,
    backoff_max interval NOT NULL,
    on_fail cb_fail_policy NOT NULL,
    topic text,                 -- topic pattern; NULL reads every topic
    topic_regex text,           -- compiled by _cb_stream_compile_topic at ensure
    condition text,             -- headers/payload/recipients expression; NULL reads everything
    headers_condition jsonpath, -- disassembled by _cb_stream_compile_condition at ensure
    payload_condition jsonpath,
    recipients_condition text[],
    PRIMARY KEY (stream, name),
    CHECK (closed_pos <= claimed_pos)
);

CREATE TABLE cb_stream_claims (
    stream text NOT NULL,
    subscription text NOT NULL,
    from_pos bigint NOT NULL,
    to_pos bigint NOT NULL,
    consumer text NOT NULL,
    closed boolean NOT NULL DEFAULT false,
    released boolean NOT NULL DEFAULT false, -- a claim handed back on purpose, never a crash
    ttl interval NOT NULL,
    expires_at timestamptz NOT NULL, -- past this moment any consumer may claim again
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, subscription, from_pos),
    FOREIGN KEY (stream, subscription)
        REFERENCES cb_stream_subscriptions(stream, name) ON DELETE CASCADE,
    CHECK (from_pos <= to_pos)
);

-- One row per base-stream position that a subscription could not process.
-- Replaces the old sr.* retry and sd.* streams: a failed or
-- crashed message becomes a row here, retried on its own with a backoff,
-- and marked failed when its attempts run out. origin_pos is the position on
-- the base stream that failed, and the row's identity. claimable_at carries
-- three jobs at once: a live lease holds it in the future, expiry brings it
-- back, and a backoff pushes it out. attempt counts delivery tries consumed;
-- one budget, max_attempts, covers both verdicts and crashes.
-- The headers and recipients are the original message's, untouched: a
-- handler retried from here sees exactly what it saw the first time.
CREATE TABLE cb_stream_retries (
    stream       text NOT NULL,
    subscription text NOT NULL,
    origin_pos   bigint NOT NULL,
    topic        text,
    payload      jsonb NOT NULL,
    headers      jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(headers) = 'object'),
    recipients   text[],
    attempt      int NOT NULL,
    last_error   text,                 -- 'silence' for a crash, the handler's text for a verdict
    failed       boolean NOT NULL DEFAULT false,
    claimable_at timestamptz NOT NULL, -- visibility, backoff and lease in one: due when <= now
    consumer     text,                 -- the consumer holding the lease; NULL when free
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, subscription, origin_pos),
    FOREIGN KEY (stream, subscription)
        REFERENCES cb_stream_subscriptions(stream, name) ON DELETE CASCADE
);

CREATE INDEX ON cb_stream_retries (stream, subscription, claimable_at);

CREATE TABLE cb_stream_schedules (
    stream   text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    name     text NOT NULL CHECK (cb_valid_name(name)),
    every    interval NOT NULL CHECK (
        every > interval '0'
        AND extract(day   FROM every) = 0   -- fixed durations only: no day/month/year
        AND extract(month FROM every) = 0   -- component, so the epoch math in
        AND extract(year  FROM every) = 0   -- cb_stream_deliver_schedules is exact
    ),
    catch_up cb_catch_up_policy NOT NULL DEFAULT 'skip',
    topic    text,
    payload  jsonb NOT NULL DEFAULT '{}',
    headers  jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(headers) = 'object'),
    recipients text[], -- copied onto each fired message
    next_at  timestamptz NOT NULL, -- when this schedule fires next
    PRIMARY KEY (stream, name)
);

CREATE INDEX ON cb_stream_schedules (next_at);

-- +goose statementbegin
-- Create the stream's physical partition.
CREATE FUNCTION _cb_stream_ensure_partition(stream text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _partition text := 'cbm_' || _cb_stream_ensure_partition.stream;
BEGIN
    -- cb_stream_ensure already holds this lock; taking it again in the
    -- same transaction is free, and it keeps this function safe for any
    -- caller of its own.
    PERFORM pg_advisory_xact_lock(hashtext('cb_stream_ensure'));
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF cb_stream_messages FOR VALUES IN (%L)',
        _partition, _cb_stream_ensure_partition.stream);
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_ensure(stream text, retention interval DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF NOT cb_valid_name(cb_stream_ensure.stream) THEN
        RAISE EXCEPTION 'catbird: invalid stream name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_ensure.stream USING ERRCODE = 'IRD01';
    END IF;

    IF cb_stream_ensure.retention IS NOT NULL
       AND cb_stream_ensure.retention <= interval '0'
       AND cb_stream_ensure.retention <> cb_forever() THEN
        RAISE EXCEPTION 'catbird: retention must be positive, or cb_forever() for no limit (got %)',
            cb_stream_ensure.retention USING ERRCODE = 'IRD01';
    END IF;

    -- The whole ensure serializes on this lock, taken before the insert.
    -- The partition DDL below needs a share-row-exclusive lock on
    -- cb_streams (the partition clones the stream foreign key), so with
    -- the insert first two ensures deadlock: one holds its cb_streams
    -- row lock and waits here, the other holds this lock and waits on
    -- that row.
    PERFORM pg_advisory_xact_lock(hashtext('cb_stream_ensure'));

    INSERT INTO cb_streams (name, retention)
    VALUES (cb_stream_ensure.stream, coalesce(cb_stream_ensure.retention, cb_forever()))
    ON CONFLICT ON CONSTRAINT cb_streams_pkey DO NOTHING;

    PERFORM _cb_stream_ensure_partition(cb_stream_ensure.stream);
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_ensure_cursor(
    stream text,
    cursor text,
    start_pos bigint DEFAULT NULL,
    topic text DEFAULT NULL,
    condition text DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _start bigint;
    _topic_re text;
    _headers_cond jsonpath;
    _payload_cond jsonpath;
    _recipients_cond text[];
BEGIN
    IF NOT cb_valid_name(cb_stream_ensure_cursor.cursor) THEN
        RAISE EXCEPTION 'catbird: invalid cursor name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_ensure_cursor.cursor USING ERRCODE = 'IRD01';
    END IF;
    IF NOT cb_valid_name(cb_stream_ensure_cursor.stream) THEN
        RAISE EXCEPTION 'catbird: invalid stream name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_ensure_cursor.stream USING ERRCODE = 'IRD01';
    END IF;

    -- NULL start = tail: skip everything already in the stream.
    SELECT coalesce(cb_stream_ensure_cursor.start_pos, s.last_pos)
    INTO _start FROM cb_streams s WHERE s.name = cb_stream_ensure_cursor.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', cb_stream_ensure_cursor.stream USING ERRCODE = 'IRD02';
    END IF;

    IF cb_stream_ensure_cursor.topic IS NOT NULL THEN
        _topic_re := _cb_stream_compile_topic(cb_stream_ensure_cursor.topic);
    END IF;
    IF cb_stream_ensure_cursor.condition IS NOT NULL THEN
        SELECT c.headers_condition, c.payload_condition, c.recipients_condition
        INTO _headers_cond, _payload_cond, _recipients_cond
        FROM _cb_stream_compile_condition(cb_stream_ensure_cursor.condition) c;
    END IF;

    INSERT INTO cb_stream_cursors
        (stream, name, pos, topic, topic_regex, condition,
         headers_condition, payload_condition, recipients_condition)
    VALUES (cb_stream_ensure_cursor.stream, cb_stream_ensure_cursor.cursor, _start,
            cb_stream_ensure_cursor.topic, _topic_re,
            cb_stream_ensure_cursor.condition, _headers_cond, _payload_cond, _recipients_cond)
    ON CONFLICT ON CONSTRAINT cb_stream_cursors_pkey DO NOTHING;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Declares a cursor whole: creating and updating are the same call, an
-- identical declaration writes nothing. The filter (topic, condition) is
-- the cursor's config and is recompiled when it changes; pos is reading
-- state and stays put — start_pos, when given, sets it deliberately.
-- When creating, NULL start_pos means the stream's tail. Unlike
-- cb_stream_ensure_cursor, whose start_pos applies at birth only, this
-- start_pos repositions an existing cursor every time it is given.
CREATE FUNCTION cb_stream_define_cursor(
    stream text,
    cursor text,
    start_pos bigint DEFAULT NULL,
    topic text DEFAULT NULL,
    condition text DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _start bigint;
    _topic_re text;
    _headers_cond jsonpath;
    _payload_cond jsonpath;
    _recipients_cond text[];
BEGIN
    IF NOT cb_valid_name(cb_stream_define_cursor.cursor) THEN
        RAISE EXCEPTION 'catbird: invalid cursor name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_define_cursor.cursor USING ERRCODE = 'IRD01';
    END IF;
    IF NOT cb_valid_name(cb_stream_define_cursor.stream) THEN
        RAISE EXCEPTION 'catbird: invalid stream name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_define_cursor.stream USING ERRCODE = 'IRD01';
    END IF;

    -- NULL start = tail: skip everything already in the stream.
    SELECT coalesce(cb_stream_define_cursor.start_pos, s.last_pos)
    INTO _start FROM cb_streams s WHERE s.name = cb_stream_define_cursor.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined',
            cb_stream_define_cursor.stream USING ERRCODE = 'IRD02';
    END IF;

    IF cb_stream_define_cursor.topic IS NOT NULL THEN
        _topic_re := _cb_stream_compile_topic(cb_stream_define_cursor.topic);
    END IF;
    IF cb_stream_define_cursor.condition IS NOT NULL THEN
        SELECT c.headers_condition, c.payload_condition, c.recipients_condition
        INTO _headers_cond, _payload_cond, _recipients_cond
        FROM _cb_stream_compile_condition(cb_stream_define_cursor.condition) c;
    END IF;

    INSERT INTO cb_stream_cursors AS c
        (stream, name, pos, topic, topic_regex, condition,
         headers_condition, payload_condition, recipients_condition)
    VALUES (cb_stream_define_cursor.stream, cb_stream_define_cursor.cursor, _start,
            cb_stream_define_cursor.topic, _topic_re,
            cb_stream_define_cursor.condition, _headers_cond, _payload_cond, _recipients_cond)
    ON CONFLICT ON CONSTRAINT cb_stream_cursors_pkey DO UPDATE
    SET topic                = excluded.topic,
        topic_regex          = excluded.topic_regex,
        condition            = excluded.condition,
        headers_condition    = excluded.headers_condition,
        payload_condition    = excluded.payload_condition,
        recipients_condition = excluded.recipients_condition,
        pos = CASE WHEN cb_stream_define_cursor.start_pos IS NOT NULL
                   THEN cb_stream_define_cursor.start_pos
                   ELSE c.pos END
    -- an identical declaration writes nothing; comparing the source text
    -- covers the compiled columns, which are functions of it
    WHERE (c.topic, c.condition) IS DISTINCT FROM (excluded.topic, excluded.condition)
       OR cb_stream_define_cursor.start_pos IS NOT NULL;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Removes a cursor. Reports whether one existed; deleting a missing
-- cursor is a no-op.
CREATE FUNCTION cb_stream_delete_cursor(stream text, cursor text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _found boolean;
BEGIN
    DELETE FROM cb_stream_cursors c
    WHERE c.stream = cb_stream_delete_cursor.stream
      AND c.name = cb_stream_delete_cursor.cursor
    RETURNING true INTO _found;
    RETURN coalesce(_found, false);
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_ensure_subscription(
    stream         text,
    subscription          text,
    start_pos bigint DEFAULT NULL,
    claim_ttl      interval DEFAULT NULL,
    max_attempts   int      DEFAULT NULL,
    backoff_kind   cb_backoff_kind DEFAULT NULL,
    backoff_base   interval DEFAULT NULL,
    backoff_max    interval DEFAULT NULL,
    on_fail        cb_fail_policy DEFAULT NULL,
    claim_batch_size int    DEFAULT NULL,
    topic text DEFAULT NULL,
    condition text DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _start bigint;
    _topic_re text;
    _headers_cond jsonpath;
    _payload_cond jsonpath;
    _recipients_cond text[];
BEGIN
    IF NOT cb_valid_name(cb_stream_ensure_subscription.subscription) THEN
        RAISE EXCEPTION 'catbird: invalid subscription name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_ensure_subscription.subscription USING ERRCODE = 'IRD01';
    END IF;

    IF NOT cb_valid_name(cb_stream_ensure_subscription.stream) THEN
        RAISE EXCEPTION 'catbird: invalid stream name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_ensure_subscription.stream USING ERRCODE = 'IRD01';
    END IF;

    SELECT coalesce(cb_stream_ensure_subscription.start_pos, s.last_pos)
    INTO _start
    FROM cb_streams s
    WHERE s.name = cb_stream_ensure_subscription.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', cb_stream_ensure_subscription.stream USING ERRCODE = 'IRD02';
    END IF;

    IF cb_stream_ensure_subscription.topic IS NOT NULL THEN
        _topic_re := _cb_stream_compile_topic(cb_stream_ensure_subscription.topic);
    END IF;
    IF cb_stream_ensure_subscription.condition IS NOT NULL THEN
        SELECT c.headers_condition, c.payload_condition, c.recipients_condition
        INTO _headers_cond, _payload_cond, _recipients_cond
        FROM _cb_stream_compile_condition(cb_stream_ensure_subscription.condition) c;
    END IF;

    INSERT INTO cb_stream_subscriptions
        (stream, name, claimed_pos, closed_pos,
         claim_ttl, claim_batch_size, max_attempts,
         backoff_kind, backoff_base, backoff_max, on_fail,
         topic, topic_regex, condition,
         headers_condition, payload_condition, recipients_condition)
    VALUES (
        cb_stream_ensure_subscription.stream,
        cb_stream_ensure_subscription.subscription,
        _start, _start,
        coalesce(cb_stream_ensure_subscription.claim_ttl,        interval '30 seconds'),
        coalesce(cb_stream_ensure_subscription.claim_batch_size, 100),
        coalesce(cb_stream_ensure_subscription.max_attempts, 3),
        coalesce(cb_stream_ensure_subscription.backoff_kind, 'full_jitter'),
        coalesce(cb_stream_ensure_subscription.backoff_base, interval '5 seconds'),
        coalesce(cb_stream_ensure_subscription.backoff_max,  interval '5 minutes'),
        coalesce(cb_stream_ensure_subscription.on_fail,      'keep'),
        cb_stream_ensure_subscription.topic,
        _topic_re,
        cb_stream_ensure_subscription.condition,
        _headers_cond,
        _payload_cond,
        _recipients_cond
    )
    ON CONFLICT ON CONSTRAINT cb_stream_subscriptions_pkey DO NOTHING;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_define_schedule(
    stream     text,
    name       text,
    every      interval,
    topic      text        DEFAULT NULL,
    payload    jsonb       DEFAULT NULL,
    headers    jsonb       DEFAULT NULL,
    recipients text[]      DEFAULT NULL,
    catch_up   cb_catch_up_policy DEFAULT NULL,
    start_at   timestamptz DEFAULT NULL
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _next_at timestamptz;
BEGIN
    IF NOT cb_valid_name(cb_stream_define_schedule.name) THEN
        RAISE EXCEPTION 'catbird: invalid schedule name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_define_schedule.name USING ERRCODE = 'IRD01';
    END IF;

    IF cb_stream_define_schedule.every IS NULL THEN
        RAISE EXCEPTION 'catbird: schedule %.% needs an interval',
            cb_stream_define_schedule.stream, cb_stream_define_schedule.name USING ERRCODE = 'IRD01';
    END IF;

    IF extract(day   FROM cb_stream_define_schedule.every) <> 0
    OR extract(month FROM cb_stream_define_schedule.every) <> 0
    OR extract(year  FROM cb_stream_define_schedule.every) <> 0 THEN
        RAISE EXCEPTION 'catbird: schedule interval must be hours or less (got %); days, months and years need cron',
            cb_stream_define_schedule.every USING ERRCODE = 'IRD01';
    END IF;

    -- cb_ header keys are catbird's own.
    IF cb_stream_define_schedule.headers IS NOT NULL
       AND EXISTS (SELECT 1 FROM jsonb_object_keys(cb_stream_define_schedule.headers) AS k
                   WHERE left(k, 3) = 'cb_') THEN
        RAISE EXCEPTION 'catbird: header keys starting with cb_ are reserved' USING ERRCODE = 'IRD01';
    END IF;

    IF cb_stream_define_schedule.recipients IS NOT NULL
       AND EXISTS (SELECT 1 FROM unnest(cb_stream_define_schedule.recipients) r
                   WHERE r IS NULL OR r = '') THEN
        RAISE EXCEPTION 'catbird: recipients must be non-empty strings' USING ERRCODE = 'IRD01';
    END IF;

    PERFORM 1 FROM cb_streams s WHERE s.name = cb_stream_define_schedule.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', cb_stream_define_schedule.stream USING ERRCODE = 'IRD02';
    END IF;

    -- Topics are never empty; empty means none.
    INSERT INTO cb_stream_schedules AS sc
        (stream, name, every, topic, payload, headers, recipients, catch_up, next_at)
    VALUES (
        cb_stream_define_schedule.stream,
        cb_stream_define_schedule.name,
        cb_stream_define_schedule.every,
        nullif(cb_stream_define_schedule.topic, ''),
        coalesce(cb_stream_define_schedule.payload, '{}'),
        coalesce(cb_stream_define_schedule.headers, '{}'),
        nullif(cb_stream_define_schedule.recipients, '{}'),
        coalesce(cb_stream_define_schedule.catch_up, 'skip'),
        coalesce(cb_stream_define_schedule.start_at,
                 clock_timestamp() + cb_stream_define_schedule.every)
    )
    ON CONFLICT ON CONSTRAINT cb_stream_schedules_pkey DO UPDATE
    SET every      = excluded.every,
        topic      = excluded.topic,
        payload    = excluded.payload,
        headers    = excluded.headers,
        recipients = excluded.recipients,
        catch_up   = excluded.catch_up,
        next_at    = CASE
            WHEN cb_stream_define_schedule.start_at IS NOT NULL
                THEN cb_stream_define_schedule.start_at
            WHEN sc.every IS DISTINCT FROM excluded.every
                THEN clock_timestamp() + excluded.every
            ELSE sc.next_at
        END
    -- an identical declaration writes nothing and notifies nothing
    WHERE (sc.every, sc.topic, sc.payload, sc.headers, sc.recipients, sc.catch_up)
          IS DISTINCT FROM
          (excluded.every, excluded.topic, excluded.payload, excluded.headers, excluded.recipients, excluded.catch_up)
       OR cb_stream_define_schedule.start_at IS NOT NULL
    RETURNING sc.next_at INTO _next_at;

    IF _next_at IS NOT NULL THEN
        PERFORM pg_notify(current_schema || '.cb_tick',
            to_char(_next_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
    END IF;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_delete_schedule(stream text, name text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    _found boolean;
BEGIN
    DELETE FROM cb_stream_schedules sc
    WHERE sc.stream = cb_stream_delete_schedule.stream
      AND sc.name   = cb_stream_delete_schedule.name
    RETURNING true INTO _found;

    RETURN coalesce(_found, false);
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_publish(
    stream     text,
    topic      text,
    payload    jsonb,
    headers    jsonb       DEFAULT '{}',
    recipients text[]      DEFAULT NULL, -- who the message is for; matched by $.recipients
    key        text        DEFAULT NULL, -- deduplication key (keep oldest)
    delay      interval    DEFAULT NULL, -- relative delayed delivery
    deliver_at timestamptz DEFAULT NULL, -- absolute delayed delivery

    OUT ref_kind cb_ref_kind,
    OUT ref_id   bigint,
    OUT existing boolean -- true: key already taken, nothing stored
)
LANGUAGE plpgsql AS $$
DECLARE
    _id bigint;
    _deliver_at timestamptz;
    _future boolean;
BEGIN
    -- Header keys starting with cb_ are for catbird's own use.
    IF EXISTS (SELECT 1 FROM jsonb_object_keys(cb_stream_publish.headers) AS k
               WHERE left(k, 3) = 'cb_') THEN
        RAISE EXCEPTION 'catbird: header keys starting with cb_ are reserved' USING ERRCODE = 'IRD01';
    END IF;

    IF cb_stream_publish.recipients IS NOT NULL
       AND EXISTS (SELECT 1 FROM unnest(cb_stream_publish.recipients) r
                   WHERE r IS NULL OR r = '') THEN
        RAISE EXCEPTION 'catbird: recipients must be non-empty strings' USING ERRCODE = 'IRD01';
    END IF;

    existing := false;

    -- Validate arguments.
    IF delay IS NOT NULL AND cb_stream_publish.deliver_at IS NOT NULL THEN
        RAISE EXCEPTION 'catbird: cannot specify both delay and deliver_at' USING ERRCODE = 'IRD01';
    END IF;

    _deliver_at := coalesce(cb_stream_publish.deliver_at, clock_timestamp() + cb_stream_publish.delay);
    _future := _deliver_at IS NOT NULL AND _deliver_at > clock_timestamp();

    -- Check the stream exists.
    PERFORM 1 FROM cb_streams s WHERE s.name = cb_stream_publish.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', cb_stream_publish.stream USING ERRCODE = 'IRD02';
    END IF;

    ---- Deduplication by key. ----
    IF cb_stream_publish.key IS NOT NULL THEN
        _id := CASE WHEN _future
            THEN nextval(pg_get_serial_sequence('cb_stream_pending', 'id'))
            ELSE nextval(pg_get_serial_sequence('cb_stream_messages', 'id')) END;

        -- Try to claim this key. If the key is already claimed, learn who owns it.
        -- Do not simplify:
        --   * ON CONFLICT ... DO UPDATE ... WHERE FALSE: on a duplicate key we
        --     must NOT modify the existing row, but plain DO NOTHING would
        --     neither lock it nor let us RETURN it. DO UPDATE locks the
        --     existing row; WHERE FALSE then cancels the update itself.
        --   * RETURNING only produces a row if OUR insert won, so the "won"
        --     CTE is either our new claim or empty.
        --   * The UNION ALL branch reads the existing owner when we lost.
        -- If the competing claim commits while we wait on the conflict, this
        -- statement's snapshot may not see its row yet and both branches come
        -- back empty. The retry SELECT runs as a new statement and handles that race.
        WITH won AS (
            INSERT INTO cb_stream_keys AS k (stream, key, ref_kind, ref_id)
            VALUES (cb_stream_publish.stream,
                    cb_stream_publish.key,
                    CASE WHEN _future THEN 'pending' ELSE 'message' END::cb_ref_kind, _id)
            ON CONFLICT ON CONSTRAINT cb_stream_keys_pkey
            DO UPDATE SET ref_id = k.ref_id WHERE FALSE
            RETURNING k.ref_kind, k.ref_id
        )
        SELECT ref.ref_kind, ref.ref_id INTO ref_kind, ref_id FROM (
            SELECT w.ref_kind, w.ref_id FROM won w
            UNION ALL
            SELECT k.ref_kind, k.ref_id FROM cb_stream_keys k
            WHERE k.stream = cb_stream_publish.stream
              AND k.key         = cb_stream_publish.key
            LIMIT 1
        ) ref;
        -- Handle race edge case.
        IF ref_id IS NULL THEN
            SELECT k.ref_kind, k.ref_id INTO ref_kind, ref_id
            FROM cb_stream_keys k
            WHERE k.stream = cb_stream_publish.stream
              AND k.key         = cb_stream_publish.key;
        END IF;

        -- It's a duplicate. Return the existing id.
        IF ref_id <> _id THEN
            existing := true;
            RETURN;
        END IF;
    END IF;

    ---- Delayed delivery: store as pending, notify the sweeper and return early. ----
    IF _future THEN
        ref_kind := 'pending';
        ref_id   := coalesce(_id, nextval(pg_get_serial_sequence('cb_stream_pending', 'id')));

        INSERT INTO cb_stream_pending
            (id, stream, topic, payload, headers, recipients, deliver_at, key)
        VALUES (
            ref_id,
            cb_stream_publish.stream,
            cb_stream_publish.topic,
            cb_stream_publish.payload,
            cb_stream_publish.headers,
            nullif(cb_stream_publish.recipients, '{}'),
            _deliver_at,
            cb_stream_publish.key
        );

        PERFORM pg_notify(current_schema || '.cb_tick',
            to_char(_deliver_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));

        RETURN;
    END IF;

    ---- Immediate delivery: store as message, notify the assigner. ----
    ref_kind := 'message';
    ref_id   := coalesce(_id, nextval(pg_get_serial_sequence('cb_stream_messages', 'id')));

    -- id is GENERATED BY DEFAULT, so explicit inserts are allowed.
    INSERT INTO cb_stream_messages (id, stream, topic, payload, headers, recipients)
    VALUES (
        ref_id,
        cb_stream_publish.stream,
        cb_stream_publish.topic,
        cb_stream_publish.payload,
        cb_stream_publish.headers,
        nullif(cb_stream_publish.recipients, '{}')
    );

    -- Notify the position assigner.
    PERFORM _cb_stream_notify(cb_stream_publish.stream, cb_stream_publish.topic);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- Batch publish: the equivalent of one cb_stream_publish per message, in
-- one call. messages is a jsonb array of {payload, topic?, headers?,
-- recipients?, key?, delay?, deliver_at?} envelopes; delay is in seconds,
-- recipients an array of strings. Returns one row per element, in input
-- order.
CREATE FUNCTION cb_stream_publish_messages(stream text, messages jsonb)
RETURNS TABLE (ref_kind cb_ref_kind, ref_id bigint, existing boolean)
LANGUAGE plpgsql AS $$
DECLARE
    _m jsonb;
BEGIN
    IF cb_stream_publish_messages.messages IS NULL
    OR jsonb_typeof(cb_stream_publish_messages.messages) <> 'array' THEN
        RAISE EXCEPTION 'catbird: messages must be a JSON array' USING ERRCODE = 'IRD01';
    END IF;

    FOR _m IN SELECT e.* FROM jsonb_array_elements(cb_stream_publish_messages.messages) e LOOP
        IF jsonb_typeof(_m) <> 'object' THEN
            RAISE EXCEPTION 'catbird: message must be a JSON object' USING ERRCODE = 'IRD01';
        END IF;
        IF _m->'payload' IS NULL THEN
            RAISE EXCEPTION 'catbird: message without payload' USING ERRCODE = 'IRD01';
        END IF;
        IF _m ? 'recipients' AND jsonb_typeof(_m->'recipients') <> 'array' THEN
            RAISE EXCEPTION 'catbird: recipients must be a JSON array of strings' USING ERRCODE = 'IRD01';
        END IF;

        RETURN QUERY
        SELECT p.ref_kind, p.ref_id, p.existing
        FROM cb_stream_publish(
            cb_stream_publish_messages.stream,
            _m->>'topic',
            _m->'payload',
            coalesce(_m->'headers', '{}'),
            CASE WHEN _m ? 'recipients'
                THEN (SELECT array_agg(r) FROM jsonb_array_elements_text(_m->'recipients') r) END,
            _m->>'key',
            CASE WHEN _m ? 'delay'
                THEN make_interval(secs => (_m->>'delay')::double precision) END,
            (_m->>'deliver_at')::timestamptz) p;
    END LOOP;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_assign_positions(stream text, batch_size int DEFAULT 5000)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _n int;
BEGIN
    -- Only one process may number a stream at a time.
    IF NOT pg_try_advisory_xact_lock(hashtext('cb_assign:' || cb_stream_assign_positions.stream)) THEN
        RETURN 0;
    END IF;

    WITH unassigned AS (
        SELECT m.id, row_number() OVER (ORDER BY m.id) AS pos
        FROM cb_stream_messages m
        WHERE m.stream = cb_stream_assign_positions.stream AND m.pos IS NULL
        ORDER BY m.id
        LIMIT cb_stream_assign_positions.batch_size
    ), bump AS (
        UPDATE cb_streams s
        SET last_pos = s.last_pos + (SELECT count(*) FROM unassigned)
        WHERE s.name = cb_stream_assign_positions.stream
        RETURNING s.last_pos - (SELECT count(*) FROM unassigned) AS last_pos
    ), assigned AS (
        UPDATE cb_stream_messages m
        SET pos = bump.last_pos + unassigned.pos
        FROM unassigned, bump
        WHERE m.stream = cb_stream_assign_positions.stream
          AND m.id = unassigned.id
           -- Not really needed with the advisory lock, just extra safety.
           -- Without the lock, gaps would appear in the position sequence,
           -- but no existing message would be updated.
          AND m.pos IS NULL
        RETURNING 1
    )
    SELECT count(*) INTO _n FROM assigned;

    IF _n > 0 THEN
        -- Inform consumers that new messages are available.
        PERFORM _cb_stream_notify(cb_stream_assign_positions.stream, '');
    END IF;
    RETURN _n;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_deliver_pending(batch_size int DEFAULT 500)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _p cb_stream_pending;
    _msg_id bigint;
    _n int := 0;
BEGIN
    FOR _p IN
        SELECT * FROM cb_stream_pending
        WHERE deliver_at <= clock_timestamp()
        ORDER BY deliver_at LIMIT cb_stream_deliver_pending.batch_size
        FOR UPDATE SKIP LOCKED
    LOOP
        DELETE FROM cb_stream_pending WHERE id = _p.id;

        -- Publish the message
        INSERT INTO cb_stream_messages (stream, topic, payload, headers, recipients)
        VALUES (_p.stream, _p.topic, _p.payload, _p.headers, _p.recipients)
        RETURNING id INTO _msg_id;
        -- If the message has a key, update it.
        UPDATE cb_stream_keys k
        SET ref_kind = 'message', ref_id = _msg_id, ref_created_at = now()
        WHERE k.ref_kind = 'pending' AND k.ref_id = _p.id AND k.stream = _p.stream;

        PERFORM _cb_stream_notify(_p.stream, _p.topic);

        _n := _n + 1;
    END LOOP;
    RETURN _n;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_deliver_schedules(batch_size int DEFAULT 500)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _schedule cb_stream_schedules;
    _due_ticks int;  -- ticks due from next_at through now, inclusive (always >= 1)
    _fire_ticks int; -- how many of those ticks this policy actually emits
    _n int := 0;     -- messages published, the return value (matches cb_stream_deliver_pending)
BEGIN
    FOR _schedule IN
        SELECT * FROM cb_stream_schedules
        WHERE next_at <= clock_timestamp()
        ORDER BY next_at LIMIT cb_stream_deliver_schedules.batch_size
        FOR UPDATE SKIP LOCKED
    LOOP
        -- Ticks due from next_at through now -- at least one, since the row is due.
        _due_ticks := floor(extract(epoch FROM clock_timestamp() - _schedule.next_at)
            / extract(epoch FROM _schedule.every))::int + 1;

        -- 'all' replays every missed tick. 'skip' fires only an on-time tick
        -- (_due_ticks = 1) and skips the backlog (_due_ticks > 1).
        _fire_ticks := CASE WHEN _schedule.catch_up = 'all' THEN _due_ticks
                            WHEN _due_ticks = 1             THEN 1
                            ELSE 0 END;

        IF _fire_ticks > 0 THEN
            INSERT INTO cb_stream_messages (stream, topic, payload, headers, recipients)
            SELECT _schedule.stream, _schedule.topic, _schedule.payload,
                   _schedule.headers, _schedule.recipients
            FROM generate_series(1, _fire_ticks);

            PERFORM _cb_stream_notify(_schedule.stream, _schedule.topic);

            _n := _n + _fire_ticks;
        END IF;

        -- Re-arm to the first tick after now, in one step. Same expression for
        -- both policies: 'all' has just caught up to it, 'skip' jumps its
        -- backlog past it. Anchored on the old next_at, so tick phase holds.
        UPDATE cb_stream_schedules sc
        SET next_at = _schedule.next_at + _schedule.every * _due_ticks
        WHERE sc.stream = _schedule.stream
          AND sc.name = _schedule.name;
    END LOOP;

    RETURN _n;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_prune_messages(stream text, batch_size int DEFAULT 10000)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _retention interval;
    _n bigint;
BEGIN
    SELECT s.retention INTO _retention
    FROM cb_streams s WHERE s.name = cb_stream_prune_messages.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', cb_stream_prune_messages.stream USING ERRCODE = 'IRD02';
    END IF;
    IF _retention < interval '0' THEN  -- cb_forever() sentinel: nothing to prune
        RETURN 0;
    END IF;

    WITH doomed AS (
        SELECT m.ctid FROM cb_stream_messages m
        WHERE m.stream = cb_stream_prune_messages.stream
          AND m.created_at < clock_timestamp() - _retention
        ORDER BY m.pos
        LIMIT cb_stream_prune_messages.batch_size
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM cb_stream_messages m USING doomed d
    WHERE m.stream = cb_stream_prune_messages.stream
      AND m.ctid = d.ctid;

    GET DIAGNOSTICS _n = ROW_COUNT;
    RETURN _n;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_prune_keys(stream text, batch_size int DEFAULT 10000)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _retention interval;
    _n bigint;
BEGIN
    SELECT s.retention INTO _retention
    FROM cb_streams s WHERE s.name = cb_stream_prune_keys.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', cb_stream_prune_keys.stream USING ERRCODE = 'IRD02';
    END IF;
    IF _retention < interval '0' THEN  -- cb_forever() sentinel: nothing to prune
        RETURN 0;
    END IF;

    WITH doomed AS (
        SELECT k.ctid FROM cb_stream_keys k
        WHERE k.stream = cb_stream_prune_keys.stream
          AND k.ref_kind = 'message'  -- keep undelivered 'pending' keys
          AND k.ref_created_at < clock_timestamp() - _retention
        ORDER BY k.ref_created_at
        LIMIT cb_stream_prune_keys.batch_size
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM cb_stream_keys k USING doomed d
    WHERE k.ctid = d.ctid;

    GET DIAGNOSTICS _n = ROW_COUNT;
    RETURN _n;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_read(stream text, cursor text, batch_size int DEFAULT 100)
RETURNS SETOF cb_stream_messages
LANGUAGE plpgsql AS $$
DECLARE
    _pos bigint;
    _regex text;
    _headers jsonpath;
    _payload jsonpath;
    _recipients text[];
    _new_pos bigint;
BEGIN
    -- Get current cursor position and conditions.
    SELECT c.pos, c.topic_regex, c.headers_condition, c.payload_condition, c.recipients_condition
    INTO _pos, _regex, _headers, _payload, _recipients
    FROM cb_stream_cursors c
    WHERE c.stream = cb_stream_read.stream AND c.name = cb_stream_read.cursor
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: cursor %.% not defined', cb_stream_read.stream, cb_stream_read.cursor USING ERRCODE = 'IRD02';
    END IF;

    SELECT max(b.pos) INTO _new_pos FROM (
        SELECT m.pos FROM cb_stream_messages m
        WHERE m.stream = cb_stream_read.stream
          AND m.pos > _pos
        ORDER BY m.pos
        LIMIT cb_stream_read.batch_size) b;

    IF _new_pos IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT m.* FROM cb_stream_messages m
    WHERE m.stream = cb_stream_read.stream
      AND m.pos > _pos AND m.pos <= _new_pos
      AND (_regex IS NULL OR m.topic ~ _regex)          -- a NULL topic never matches
      AND (_headers IS NULL OR m.headers @@ _headers)   -- lax: an error means no match
      AND (_payload IS NULL OR m.payload @@ _payload)
      AND (_recipients IS NULL OR m.recipients @> _recipients) -- no recipients never matches
    ORDER BY m.pos;

    UPDATE cb_stream_cursors c SET pos = _new_pos
    WHERE c.stream = cb_stream_read.stream AND c.name = cb_stream_read.cursor;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- One message by address. The log is position-addressable: a live wire
-- frame carries {stream, pos}, and the receiving node fetches the row it
-- was told about, renders it and pushes it on. Returns nothing when the
-- position holds nothing — pruned by retention, or never assigned — gone
-- is gone, not an error.
CREATE FUNCTION cb_stream_fetch(stream text, pos bigint)
RETURNS SETOF cb_stream_messages
LANGUAGE sql STABLE AS $$
    SELECT * FROM cb_stream_messages m
    WHERE m.stream = cb_stream_fetch.stream
      AND m.pos = cb_stream_fetch.pos;
$$;
-- +goose statementend

-- +goose statementbegin
-- Move the closed position forward over adjacent closed claims until an
-- open claim stops it.
CREATE FUNCTION _cb_stream_advance_closed_position(stream text, subscription text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _closed_pos bigint;
    _to_pos bigint;
BEGIN
    SELECT q.closed_pos INTO _closed_pos FROM cb_stream_subscriptions q
    WHERE q.stream = _cb_stream_advance_closed_position.stream
      AND q.name = _cb_stream_advance_closed_position.subscription
    FOR UPDATE;

    LOOP
        DELETE FROM cb_stream_claims c
        WHERE c.stream   = _cb_stream_advance_closed_position.stream
          AND c.subscription    = _cb_stream_advance_closed_position.subscription
          AND c.from_pos = _closed_pos + 1
          AND c.closed
        RETURNING c.to_pos INTO _to_pos;
        EXIT WHEN NOT FOUND;
        _closed_pos := _to_pos;
    END LOOP;

    UPDATE cb_stream_subscriptions q SET closed_pos = _closed_pos
    WHERE q.stream = _cb_stream_advance_closed_position.stream
      AND q.name = _cb_stream_advance_closed_position.subscription
      AND q.closed_pos < _closed_pos;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Turns a crashed base range into retry rows, one per message the
-- subscription would have delivered (topic and condition). A crash is
-- silence, so each row starts at attempt 0 with no verdict, due now. A
-- position that already has a retry row keeps it: the primary key plus
-- ON CONFLICT DO NOTHING is the keep-existing-copy rule, so a verdict the
-- consumer reported before it died is not overwritten by a later silence.
CREATE FUNCTION _cb_stream_retry(claim cb_stream_claims)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _stream text := _cb_stream_retry.claim.stream;
    _subscription text := _cb_stream_retry.claim.subscription;
    _from_pos bigint := _cb_stream_retry.claim.from_pos;
    _to_pos bigint := _cb_stream_retry.claim.to_pos;
    _q cb_stream_subscriptions;
BEGIN
    SELECT q.* INTO _q FROM cb_stream_subscriptions q
    WHERE q.stream = _stream AND q.name = _subscription;

    INSERT INTO cb_stream_retries
        (stream, subscription, origin_pos, topic, payload, headers, recipients, attempt, last_error, claimable_at)
    SELECT m.stream, _subscription, m.pos, m.topic, m.payload, m.headers, m.recipients, 0, 'silence', clock_timestamp()
    FROM cb_stream_messages m
    WHERE m.stream = _stream AND m.pos BETWEEN _from_pos AND _to_pos
      AND (_q.topic_regex IS NULL OR m.topic ~ _q.topic_regex)
      AND (_q.headers_condition IS NULL OR m.headers @@ _q.headers_condition)
      AND (_q.payload_condition IS NULL OR m.payload @@ _q.payload_condition)
      AND (_q.recipients_condition IS NULL OR m.recipients @> _q.recipients_condition)
    ON CONFLICT ON CONSTRAINT cb_stream_retries_pkey DO NOTHING;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- Rules every branch of this function must preserve (checkClaims in the Go
-- test suite checks them after every step):
--   1. Open and closed claims exactly cover the region between closed_pos
--      and claimed_pos. No gaps, no overlaps. Claim boundaries never change
--      after insert, so whoever closes or removes a claim must leave the
--      chain intact, or the closed position stalls and messages are lost
--      to retention.
--   2. claimed_pos and closed_pos only ever grow.
--
-- Due retry rows are served first, each as a solo pseudo-claim
-- (from_pos = to_pos = origin_pos), so a failed message is retried on its
-- own before fresh work. A retry row still marked claimed but past due lost
-- its holder to a crash: that try was already counted at hand-out, so it is
-- recorded as silence and either backed off or given up on. Then expired base
-- claims are adopted: a released one is handed back out whole and uncharged,
-- any other expiry is a crash and is recorded as retry rows and closed.
CREATE FUNCTION cb_stream_claim(
    stream     text,
    subscription      text,
    consumer   text,
    ttl        interval DEFAULT NULL, -- the subscription's claim_ttl is used if NULL

    OUT from_pos   bigint, -- NULL when there is nothing to claim
    OUT to_pos     bigint,
    OUT expires_at timestamptz
)
LANGUAGE plpgsql AS $$
DECLARE
    _q cb_stream_subscriptions;
    _ttl interval;
    _r cb_stream_retries;
    _c cb_stream_claims;
    _claimed_pos bigint;
    _last_pos bigint;
BEGIN
    SELECT q.* INTO _q FROM cb_stream_subscriptions q
    WHERE q.stream = cb_stream_claim.stream AND q.name = cb_stream_claim.subscription;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: subscription %.% not defined', cb_stream_claim.stream, cb_stream_claim.subscription USING ERRCODE = 'IRD02';
    END IF;
    _ttl := coalesce(cb_stream_claim.ttl, _q.claim_ttl);

    ---- Serve a due retry row. ----
    LOOP
        SELECT r.* INTO _r
        FROM cb_stream_retries r
        WHERE r.stream = cb_stream_claim.stream
          AND r.subscription = cb_stream_claim.subscription
          AND NOT r.failed
          AND r.claimable_at <= clock_timestamp()
        ORDER BY r.claimable_at, r.origin_pos
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
        EXIT WHEN NOT FOUND;

        -- A due row still claimed lost its holder to a crash.
        IF _r.consumer IS NOT NULL THEN
            IF _r.attempt >= _q.max_attempts THEN
                PERFORM _cb_stream_give_up(_r, _q.on_fail, 'silence');
            ELSE
                UPDATE cb_stream_retries t
                SET last_error   = 'silence',
                    consumer     = NULL,
                    claimable_at = clock_timestamp()
                        + cb_backoff(_q.backoff_kind, _q.backoff_base, _q.backoff_max, _r.attempt)
                WHERE (t.stream, t.subscription, t.origin_pos)
                    = (_r.stream, _r.subscription, _r.origin_pos);
            END IF;
            CONTINUE;
        END IF;

        -- Hand it out as a solo pseudo-claim, minting one try.
        UPDATE cb_stream_retries t
        SET attempt      = t.attempt + 1,
            consumer     = cb_stream_claim.consumer,
            claimable_at = clock_timestamp() + _ttl
        WHERE (t.stream, t.subscription, t.origin_pos)
            = (_r.stream, _r.subscription, _r.origin_pos)
        RETURNING t.origin_pos, t.origin_pos, t.claimable_at
        INTO from_pos, to_pos, expires_at;
        RETURN;
    END LOOP;

    ---- Adopt an expired base claim. ----
    LOOP
        SELECT r.* INTO _c
        FROM cb_stream_claims r
        WHERE r.stream = cb_stream_claim.stream
          AND r.subscription  = cb_stream_claim.subscription
          AND NOT r.closed
          AND r.expires_at <= clock_timestamp()
        ORDER BY r.from_pos
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
        EXIT WHEN NOT FOUND; -- nothing expired, claim a fresh range

        IF _c.released THEN
            -- handed back on purpose: hand the whole range out again, uncharged
            UPDATE cb_stream_claims c
            SET consumer   = cb_stream_claim.consumer,
                ttl        = coalesce(cb_stream_claim.ttl, c.ttl),
                expires_at = clock_timestamp() + coalesce(cb_stream_claim.ttl, c.ttl),
                released   = false
            WHERE (c.stream, c.subscription, c.from_pos)
                = (_c.stream, _c.subscription, _c.from_pos)
            RETURNING c.from_pos, c.to_pos, c.expires_at
            INTO from_pos, to_pos, expires_at;
            RETURN;
        END IF;

        -- a crash: turn the range into retry rows and close it
        PERFORM _cb_stream_retry(_c);
        UPDATE cb_stream_claims c
        SET closed = true
        WHERE (c.stream, c.subscription, c.from_pos)
            = (_c.stream, _c.subscription, _c.from_pos);
        PERFORM _cb_stream_advance_closed_position(cb_stream_claim.stream, cb_stream_claim.subscription);
        CONTINUE;
    END LOOP;

    ---- Claim a fresh range. ----
    SELECT q.claimed_pos INTO _claimed_pos
    FROM cb_stream_subscriptions q
    WHERE q.stream = cb_stream_claim.stream AND q.name = cb_stream_claim.subscription
    FOR UPDATE;

    -- No lock needed on the streams row. last_position only grows,
    -- so a stale read just means a slightly smaller range.
    SELECT s.last_pos INTO _last_pos FROM cb_streams s
    WHERE s.name = cb_stream_claim.stream;

    IF _claimed_pos >= _last_pos THEN
        RETURN; -- caught up, return NULLs
    END IF;

    from_pos := _claimed_pos + 1;
    to_pos := least(_claimed_pos + _q.claim_batch_size, _last_pos);
    expires_at := clock_timestamp() + _ttl;

    UPDATE cb_stream_subscriptions q
    SET claimed_pos = cb_stream_claim.to_pos
    WHERE q.stream = cb_stream_claim.stream
      AND q.name = cb_stream_claim.subscription;

    INSERT INTO cb_stream_claims
        (stream, subscription, from_pos, to_pos, consumer, ttl, expires_at)
    VALUES (
        cb_stream_claim.stream,
        cb_stream_claim.subscription,
        cb_stream_claim.from_pos,
        cb_stream_claim.to_pos,
        cb_stream_claim.consumer,
        _ttl,
        cb_stream_claim.expires_at
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- The messages of a claim, in order. A base range returns the log's messages
-- honoring the subscription's topic and condition. A solo retry claim returns
-- the retry row's own stored copy as one message (id 0, pos = origin_pos); it
-- must never read the log, because the original message may already be pruned.
-- A base claim is told apart by an existing claim row at from_pos: a served
-- retry row never shares a from_pos with a live base claim, since its
-- origin_pos was claimed and closed well before this claim was handed out.
CREATE FUNCTION cb_stream_read_claim(stream text, subscription text, from_pos bigint, to_pos bigint)
RETURNS SETOF cb_stream_messages
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM cb_stream_claims c
               WHERE c.stream = cb_stream_read_claim.stream
                 AND c.subscription = cb_stream_read_claim.subscription
                 AND c.from_pos = cb_stream_read_claim.from_pos) THEN
        RETURN QUERY
        SELECT m.*
        FROM cb_stream_messages m
        JOIN cb_stream_subscriptions q
          ON q.stream = cb_stream_read_claim.stream
         AND q.name   = cb_stream_read_claim.subscription
        WHERE m.stream = cb_stream_read_claim.stream
          AND m.pos BETWEEN cb_stream_read_claim.from_pos AND cb_stream_read_claim.to_pos
          AND (q.topic_regex IS NULL OR m.topic ~ q.topic_regex)
          AND (q.headers_condition IS NULL OR m.headers @@ q.headers_condition)
          AND (q.payload_condition IS NULL OR m.payload @@ q.payload_condition)
          AND (q.recipients_condition IS NULL OR m.recipients @> q.recipients_condition)
        ORDER BY m.pos;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT 0::bigint, r.stream, r.origin_pos, r.topic, r.payload, r.headers, r.recipients, r.created_at
    FROM cb_stream_retries r
    WHERE r.stream = cb_stream_read_claim.stream
      AND r.subscription = cb_stream_read_claim.subscription
      AND r.origin_pos = cb_stream_read_claim.from_pos;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- If ttl is NULL, the claim's existing ttl is used. Returns the new expires_at timestamp.
CREATE FUNCTION cb_stream_extend_claim(
    stream text, subscription text,
    consumer text,
    from_pos bigint,
    ttl interval DEFAULT NULL
)
RETURNS timestamptz LANGUAGE plpgsql AS $$
DECLARE
    _expires_at timestamptz;
BEGIN
    IF cb_stream_extend_claim.ttl <= '0' THEN
        RAISE EXCEPTION 'catbird: invalid ttl %', cb_stream_extend_claim.ttl USING ERRCODE = 'IRD01';
    END IF;

    UPDATE cb_stream_claims c
    SET expires_at = clock_timestamp() + coalesce(cb_stream_extend_claim.ttl, c.ttl)
    WHERE c.stream   = cb_stream_extend_claim.stream
      AND c.subscription    = cb_stream_extend_claim.subscription
      AND c.consumer      = cb_stream_extend_claim.consumer
      AND c.from_pos = cb_stream_extend_claim.from_pos
      AND NOT c.closed
    RETURNING c.expires_at INTO _expires_at;
    IF _expires_at IS NOT NULL THEN
        RETURN _expires_at;
    END IF;

    -- A solo retry claim: push its lease out. The row carries no ttl of its
    -- own, so fall back to the subscription's claim_ttl when none is given.
    UPDATE cb_stream_retries r
    SET claimable_at = clock_timestamp()
        + coalesce(cb_stream_extend_claim.ttl,
                   (SELECT q.claim_ttl FROM cb_stream_subscriptions q
                    WHERE q.stream = cb_stream_extend_claim.stream
                      AND q.name   = cb_stream_extend_claim.subscription))
    WHERE r.stream   = cb_stream_extend_claim.stream
      AND r.subscription = cb_stream_extend_claim.subscription
      AND r.origin_pos = cb_stream_extend_claim.from_pos
      AND r.consumer = cb_stream_extend_claim.consumer
      AND NOT r.failed
    RETURNING r.claimable_at INTO _expires_at;
    RETURN _expires_at;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Hands a claim back so the next cb_stream_claim may take it. A base claim
-- expires now and is marked released, so its adoption counts nothing. A solo
-- retry claim becomes due again and refunds the try minted at hand-out: a
-- clean handback costs the message nothing.
CREATE FUNCTION cb_stream_release_claim(
    stream text,
    subscription text,
    consumer text,
    from_pos bigint
)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cb_stream_claims c
    SET expires_at = clock_timestamp(),
        released   = true
    WHERE c.stream   = cb_stream_release_claim.stream
      AND c.subscription    = cb_stream_release_claim.subscription
      AND c.consumer      = cb_stream_release_claim.consumer
      AND c.from_pos = cb_stream_release_claim.from_pos
      AND NOT c.closed;
    IF FOUND THEN
        RETURN;
    END IF;

    UPDATE cb_stream_retries r
    SET consumer     = NULL,
        claimable_at = clock_timestamp(),
        attempt      = r.attempt - 1
    WHERE r.stream   = cb_stream_release_claim.stream
      AND r.subscription = cb_stream_release_claim.subscription
      AND r.origin_pos = cb_stream_release_claim.from_pos
      AND r.consumer = cb_stream_release_claim.consumer
      AND NOT r.failed;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_close_claim(
    stream text,
    subscription text,
    consumer text,
    from_pos bigint
)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    -- Only the current owner may close. A base claim closes and lets the
    -- closed position advance; a solo retry claim is resolved by deleting
    -- its row, the message finally handled.
    UPDATE cb_stream_claims c SET closed = true
    WHERE c.stream   = cb_stream_close_claim.stream
      AND c.subscription    = cb_stream_close_claim.subscription
      AND c.consumer = cb_stream_close_claim.consumer
      AND c.from_pos = cb_stream_close_claim.from_pos
      AND NOT c.closed;
    IF FOUND THEN
        PERFORM _cb_stream_advance_closed_position(cb_stream_close_claim.stream, cb_stream_close_claim.subscription);
        RETURN;
    END IF;

    DELETE FROM cb_stream_retries r
    WHERE r.stream   = cb_stream_close_claim.stream
      AND r.subscription = cb_stream_close_claim.subscription
      AND r.origin_pos = cb_stream_close_claim.from_pos
      AND r.consumer = cb_stream_close_claim.consumer
      AND NOT r.failed;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Gives up on a retry row whose attempts are spent. Under the 'keep' policy
-- the row is kept as a failed row and the cb_failed channel is rung; under 'delete'
-- the row is deleted and nothing is kept.
CREATE FUNCTION _cb_stream_give_up(r cb_stream_retries, on_fail cb_fail_policy, last_error text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _stream text := _cb_stream_give_up.r.stream;
    _subscription text := _cb_stream_give_up.r.subscription;
    _origin_pos bigint := _cb_stream_give_up.r.origin_pos;
BEGIN
    IF _cb_stream_give_up.on_fail = 'delete' THEN
        DELETE FROM cb_stream_retries t
        WHERE t.stream = _stream AND t.subscription = _subscription AND t.origin_pos = _origin_pos;
        RETURN;
    END IF;

    UPDATE cb_stream_retries t
    SET failed       = true,
        last_error = _cb_stream_give_up.last_error,
        consumer   = NULL
    WHERE t.stream = _stream AND t.subscription = _subscription AND t.origin_pos = _origin_pos;

    PERFORM pg_notify(current_schema || '.cb_failed', _stream || '.' || _subscription);
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Reports that a handler could not process a message. Called once per failed
-- message. The failure becomes, or updates, a retry row: retried with a
-- backoff while attempts remain, marked failed (or deleted) once they run out.
-- Three shapes:
--   * a retry row this consumer holds -> record the verdict, back off or give up
--   * no retry row -> a base message's first failure: seed a row from it
--   * a retry row this consumer does not hold -> a zombie's late report, no-op
-- A base failure whose message is already gone (pruned mid-claim) is a silent
-- no-op, as is a call from a consumer that no longer holds the covering claim.
-- Failing does not change the claim: it still closes through cb_stream_close_claim.
CREATE FUNCTION cb_stream_fail(stream text, subscription text, consumer text, pos bigint, error text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _q cb_stream_subscriptions;
    _r cb_stream_retries;
    _m cb_stream_messages;
    _failed boolean;
    _inserted boolean;
BEGIN
    SELECT q.* INTO _q FROM cb_stream_subscriptions q
    WHERE q.stream = cb_stream_fail.stream AND q.name = cb_stream_fail.subscription;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: subscription %.% not defined',
            cb_stream_fail.stream, cb_stream_fail.subscription USING ERRCODE = 'IRD02';
    END IF;

    ---- A retry row already at this position? ----
    SELECT r.* INTO _r FROM cb_stream_retries r
    WHERE r.stream = cb_stream_fail.stream
      AND r.subscription = cb_stream_fail.subscription
      AND r.origin_pos = cb_stream_fail.pos
    FOR UPDATE;

    IF FOUND THEN
        -- not ours, or already failed: leave it for its real holder
        IF _r.consumer IS DISTINCT FROM cb_stream_fail.consumer OR _r.failed THEN
            RETURN;
        END IF;

        IF _r.attempt >= _q.max_attempts THEN
            PERFORM _cb_stream_give_up(_r, _q.on_fail, cb_stream_fail.error);
        ELSE
            UPDATE cb_stream_retries t
            SET last_error   = cb_stream_fail.error,
                consumer     = NULL,
                claimable_at = clock_timestamp()
                    + cb_backoff(_q.backoff_kind, _q.backoff_base, _q.backoff_max, _r.attempt)
            WHERE (t.stream, t.subscription, t.origin_pos)
                = (_r.stream, _r.subscription, _r.origin_pos);
        END IF;
        RETURN;
    END IF;

    ---- No retry row: a base message's first failure. ----
    -- The caller must still hold the covering claim; once that claim has been
    -- adopted and closed, a late report finds it closed and no-ops.
    -- Accepted residue: a consumer whose claim has expired but is not yet
    -- adopted can still record its verdict here, so a range already recorded as
    -- retries and resolved elsewhere may gain one late row and be delivered once more.
    PERFORM 1 FROM cb_stream_claims c
    WHERE c.stream   = cb_stream_fail.stream
      AND c.subscription = cb_stream_fail.subscription
      AND c.consumer = cb_stream_fail.consumer
      AND NOT c.closed
      AND cb_stream_fail.pos BETWEEN c.from_pos AND c.to_pos;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT m.* INTO _m FROM cb_stream_messages m
    WHERE m.stream = cb_stream_fail.stream AND m.pos = cb_stream_fail.pos;
    IF NOT FOUND THEN
        RETURN; -- pruned mid-claim: nothing left to retry
    END IF;

    -- The base delivery was attempt 1. One-strike (max_attempts = 1) exhausts
    -- it now: the 'keep' policy keeps a failed row, 'delete' keeps nothing.
    _failed := _q.max_attempts <= 1;
    IF _failed AND _q.on_fail = 'delete' THEN
        RETURN;
    END IF;

    INSERT INTO cb_stream_retries
        (stream, subscription, origin_pos, topic, payload, headers, recipients,
         attempt, last_error, failed, claimable_at)
    VALUES (
        cb_stream_fail.stream, cb_stream_fail.subscription, cb_stream_fail.pos,
        _m.topic, _m.payload, _m.headers, _m.recipients,
        1, cb_stream_fail.error, _failed,
        CASE WHEN _failed THEN clock_timestamp()
             ELSE clock_timestamp() + cb_backoff(_q.backoff_kind, _q.backoff_base, _q.backoff_max, 1) END
    )
    -- a concurrent crash may have recorded a silence row first: keep it
    ON CONFLICT ON CONSTRAINT cb_stream_retries_pkey DO NOTHING
    RETURNING true INTO _inserted;

    IF _failed AND coalesce(_inserted, false) THEN
        PERFORM pg_notify(current_schema || '.cb_failed',
            cb_stream_fail.stream || '.' || cb_stream_fail.subscription);
    END IF;
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- Retry every failed row of a subscription: reset each to a fresh, due retry
-- with its full attempt budget. Returns how many rows were revived.
CREATE FUNCTION cb_stream_retry_failed(stream text, subscription text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _n bigint;
BEGIN
    UPDATE cb_stream_retries r
    SET failed         = false,
        attempt      = 0,
        last_error   = NULL,
        consumer     = NULL,
        claimable_at = clock_timestamp()
    WHERE r.stream = cb_stream_retry_failed.stream
      AND r.subscription = cb_stream_retry_failed.subscription
      AND r.failed;
    GET DIAGNOSTICS _n = ROW_COUNT;
    RETURN _n;
END; $$;
-- +goose statementend

-- +goose down

DROP FUNCTION cb_stream_deliver_schedules(int);
DROP FUNCTION cb_stream_prune_messages(text, int);
DROP FUNCTION cb_stream_prune_keys(text, int);
DROP FUNCTION cb_stream_delete_schedule(text, text);
DROP FUNCTION cb_stream_define_schedule(text, text, interval, text, jsonb, jsonb, text[], cb_catch_up_policy, timestamptz);
DROP TABLE cb_stream_schedules;
DROP TYPE cb_catch_up_policy;

DROP FUNCTION cb_stream_retry_failed(text, text);
DROP FUNCTION cb_stream_fail(text, text, text, bigint, text);
DROP FUNCTION _cb_stream_give_up(cb_stream_retries, cb_fail_policy, text);
DROP FUNCTION cb_stream_close_claim(text, text, text, bigint);
DROP FUNCTION _cb_stream_advance_closed_position(text, text);
DROP FUNCTION cb_stream_release_claim(text, text, text, bigint);
DROP FUNCTION cb_stream_extend_claim(text, text, text, bigint, interval);
DROP FUNCTION cb_stream_claim(text, text, text, interval);
DROP FUNCTION _cb_stream_retry(cb_stream_claims);
DROP FUNCTION cb_stream_ensure_subscription(text, text, bigint, interval, int, cb_backoff_kind, interval, interval, cb_fail_policy, int, text, text);
DROP FUNCTION cb_stream_delete_cursor(text, text);
DROP FUNCTION cb_stream_define_cursor(text, text, bigint, text, text);
DROP FUNCTION cb_stream_ensure_cursor(text, text, bigint, text, text);
DROP FUNCTION cb_stream_read_claim(text, text, bigint, bigint);
DROP FUNCTION cb_stream_fetch(text, bigint);
DROP FUNCTION cb_stream_read(text, text, int);
DROP TABLE cb_stream_retries;
DROP TABLE cb_stream_claims;
DROP TABLE cb_stream_subscriptions;
DROP FUNCTION cb_stream_publish_messages(text, jsonb);
DROP FUNCTION cb_stream_publish(text, text, jsonb, jsonb, text[], text, interval, timestamptz);
DROP FUNCTION cb_stream_ensure(text, interval);
DROP FUNCTION _cb_stream_ensure_partition(text);
DROP FUNCTION cb_stream_deliver_pending(int);
DROP FUNCTION cb_stream_assign_positions(text, int);
DROP FUNCTION _cb_stream_notify(text, text);
DROP TABLE cb_stream_messages;
DROP TABLE cb_stream_cursors;
DROP TABLE cb_stream_keys;
DROP TABLE cb_stream_pending;
DROP TABLE cb_streams;
DROP FUNCTION _cb_stream_compile_condition(text);
DROP FUNCTION _cb_stream_compile_topic(text);
DROP TYPE cb_ref_kind;
DROP TYPE cb_fail_policy;
