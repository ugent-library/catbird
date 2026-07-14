-- +goose up

-- Every error this API raises carries one of these SQLSTATE codes, so
-- clients classify by code instead of parsing message text:
--   IRD01  invalid argument  the call itself is malformed
--   IRD02  not defined       the stream, subscription, cursor or schedule does not exist
--   IRD03  not found         the message does not exist
-- Why IRD: the natural prefix CB sits in the SQLSTATE class range the
-- standard reserves for itself (0-4, A-H).

CREATE TYPE cb_ref_kind AS ENUM ('message', 'pending');
CREATE TYPE cb_backoff_kind AS ENUM ('none', 'fixed', 'full_jitter');
CREATE TYPE cb_fail_policy AS ENUM ('dead_letter', 'drop');
CREATE TYPE cb_catch_up_policy AS ENUM ('skip', 'all');

-- +goose statementbegin
CREATE FUNCTION cb_valid_name(name text)
RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
    SELECT name IS NOT NULL
       AND name !~ '__'   -- '__' is reserved, it encodes dots in partition names
       AND name ~ '^[a-z][a-z0-9_]*$'
       AND octet_length(name) <= 20
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION _cb_valid_stream_name(name text)
RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
    SELECT name IS NOT NULL
       AND name ~ '^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){0,2}$'
       AND octet_length(name) <= 44
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION _cb_stream_notify(stream text, payload text)
RETURNS void LANGUAGE sql AS $$
    SELECT pg_notify(current_schema || '.cbs_' || stream, payload);
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_forever() RETURNS interval LANGUAGE sql IMMUTABLE AS $$
    SELECT interval '-1 second'
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
-- Parse a condition into per-column jsonpath predicates, once, at
-- registration. Conjuncts are joined by '&&'; each is either
-- exists($.headers.a.b) / exists($.payload.a.b), or
-- $.headers.a.b == <scalar> with <scalar> a "string", a number, true or
-- false. Anything else raises.
CREATE FUNCTION _cb_stream_compile_condition(
    condition text,
    OUT headers_condition jsonpath,
    OUT payload_condition jsonpath
)
LANGUAGE plpgsql AS $$
DECLARE
    _conjunct text;
    _m text[];
    _pred text;
    _headers text[] := '{}';
    _payload text[] := '{}';
BEGIN
    IF _cb_stream_compile_condition.condition IS NULL
    OR btrim(_cb_stream_compile_condition.condition) = '' THEN
        RAISE EXCEPTION 'catbird: condition cannot be empty' USING ERRCODE = 'IRD01';
    END IF;

    FOREACH _conjunct IN ARRAY regexp_split_to_array(_cb_stream_compile_condition.condition, '\s*&&\s*') LOOP
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
                RAISE EXCEPTION 'catbird: unsupported condition near "%"; use exists($.headers.a.b) or $.payload.a.b == <scalar>, joined with &&',
                    _conjunct USING ERRCODE = 'IRD01';
            END IF;
            _pred := '$' || _m[2] || ' == ' || _m[3];
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
END; $$;
-- +goose statementend

CREATE TABLE cb_streams (
    name text PRIMARY KEY CHECK (_cb_valid_stream_name(name)),
    last_pos bigint NOT NULL DEFAULT 0,
    retention interval NOT NULL DEFAULT cb_forever()
        CHECK (retention = cb_forever() OR retention > interval '0')
);

CREATE TABLE cb_stream_cursors (
    stream text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    name text NOT NULL CHECK (cb_valid_name(name)), -- single segment: dots are streams-only
    pos bigint NOT NULL DEFAULT 0, -- how far this cursor has read: everything at or below this position is acked
    topic text,                 -- topic pattern; NULL reads every topic
    topic_regex text,           -- compiled by _cb_stream_compile_topic at ensure
    condition text,             -- headers/payload expression; NULL reads everything
    headers_condition jsonpath, -- disassembled by _cb_stream_compile_condition at ensure
    payload_condition jsonpath,
    PRIMARY KEY (stream, name)
);

-- Messages that are delayed. _cb_stream_deliver_pending moves them
-- into cb_stream_messages when they are due.
CREATE TABLE cb_stream_pending (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    stream text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    topic text,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(headers) = 'object'),
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
    max_crashes int NOT NULL CHECK (max_crashes > 0),
    backoff_kind cb_backoff_kind NOT NULL,
    backoff_base interval NOT NULL,
    backoff_max interval NOT NULL,
    on_fail cb_fail_policy NOT NULL,
    topic text,                 -- topic pattern; NULL reads every topic
    topic_regex text,           -- compiled by _cb_stream_compile_topic at ensure
    condition text,             -- headers/payload expression; NULL reads everything
    headers_condition jsonpath, -- disassembled by _cb_stream_compile_condition at ensure
    payload_condition jsonpath,
    PRIMARY KEY (stream, name),
    CHECK (closed_pos <= claimed_pos),
    CONSTRAINT cb_stream_subscriptions_retry_batch_size
        CHECK (left(stream, 3) <> 'sr.' OR claim_batch_size = 1),
    CONSTRAINT cb_stream_subscriptions_retry_no_filters
        CHECK (left(stream, 3) <> 'sr.' OR (topic IS NULL AND condition IS NULL))

);

CREATE TABLE cb_stream_claims (
    stream text NOT NULL,
    subscription text NOT NULL,
    from_pos bigint NOT NULL,
    to_pos bigint NOT NULL,
    consumer text NOT NULL,
    closed boolean NOT NULL DEFAULT false,
    released boolean NOT NULL DEFAULT false, -- never a crash
    ttl interval NOT NULL,
    crashes int NOT NULL DEFAULT 0,
    expires_at timestamptz NOT NULL, -- past this moment any consumer may claim again
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, subscription, from_pos),
    FOREIGN KEY (stream, subscription)
        REFERENCES cb_stream_subscriptions(stream, name) ON DELETE CASCADE,
    CHECK (from_pos <= to_pos)
);

CREATE TABLE cb_stream_schedules (
    stream   text NOT NULL REFERENCES cb_streams(name) ON DELETE CASCADE,
    name     text NOT NULL CHECK (cb_valid_name(name)),
    every    interval NOT NULL CHECK (
        every > interval '0'
        AND extract(day   FROM every) = 0   -- fixed durations only: no day/month/year
        AND extract(month FROM every) = 0   -- component, so the epoch math in
        AND extract(year  FROM every) = 0   -- _cb_stream_deliver_schedules is exact
    ),
    catch_up cb_catch_up_policy NOT NULL DEFAULT 'skip',
    topic    text,
    payload  jsonb NOT NULL DEFAULT '{}',
    headers  jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(headers) = 'object'),
    next_at  timestamptz NOT NULL, -- when this schedule fires next
    PRIMARY KEY (stream, name)
);

CREATE INDEX ON cb_stream_schedules (next_at);

-- +goose statementbegin
-- Create the stream's physical partition. Shared by cb_stream_ensure
-- and _cb_stream_ensure.
-- Partition names encode dots as '__'.
CREATE FUNCTION _cb_stream_ensure_partition(stream text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _partition text := 'cbm__' || replace(_cb_stream_ensure_partition.stream, '.', '__');
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('cb_stream_ensure'));
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF cb_stream_messages FOR VALUES IN (%L)',
        _partition, _cb_stream_ensure_partition.stream);
END; $$;
-- +goose statementend

-- +goose statementbegin
-- The internal version accepts the dotted names used by retry and dead letter
-- streams.
CREATE FUNCTION _cb_stream_ensure(stream text, retention interval DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF NOT _cb_valid_stream_name(_cb_stream_ensure.stream) THEN
        RAISE EXCEPTION 'catbird: invalid stream name %', _cb_stream_ensure.stream USING ERRCODE = 'IRD01';
    END IF;

    INSERT INTO cb_streams (name, retention)
    VALUES (_cb_stream_ensure.stream, coalesce(_cb_stream_ensure.retention, cb_forever()))
    ON CONFLICT DO NOTHING;

    PERFORM _cb_stream_ensure_partition(_cb_stream_ensure.stream);
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
BEGIN
    IF NOT cb_valid_name(cb_stream_ensure_cursor.cursor) THEN
        RAISE EXCEPTION 'catbird: invalid cursor name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_ensure_cursor.cursor USING ERRCODE = 'IRD01';
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
        SELECT c.headers_condition, c.payload_condition INTO _headers_cond, _payload_cond
        FROM _cb_stream_compile_condition(cb_stream_ensure_cursor.condition) c;
    END IF;

    INSERT INTO cb_stream_cursors
        (stream, name, pos, topic, topic_regex, condition, headers_condition, payload_condition)
    VALUES (cb_stream_ensure_cursor.stream, cb_stream_ensure_cursor.cursor, _start,
            cb_stream_ensure_cursor.topic, _topic_re,
            cb_stream_ensure_cursor.condition, _headers_cond, _payload_cond)
    ON CONFLICT ON CONSTRAINT cb_stream_cursors_pkey DO NOTHING;
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
    max_crashes    int      DEFAULT NULL,
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
BEGIN
    IF NOT cb_valid_name(cb_stream_ensure_subscription.subscription) THEN
        RAISE EXCEPTION 'catbird: invalid subscription name %; use [a-z][a-z0-9_]*, max 20 bytes',
            cb_stream_ensure_subscription.subscription USING ERRCODE = 'IRD01';
    END IF;

    IF cb_stream_ensure_subscription.stream LIKE '%.%' THEN
        RAISE EXCEPTION 'catbird: % is an internal stream; subscriptions can only be created on a user stream',
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
        SELECT c.headers_condition, c.payload_condition INTO _headers_cond, _payload_cond
        FROM _cb_stream_compile_condition(cb_stream_ensure_subscription.condition) c;
    END IF;

    INSERT INTO cb_stream_subscriptions
        (stream, name, claimed_pos, closed_pos,
         claim_ttl, claim_batch_size, max_attempts, max_crashes,
         backoff_kind, backoff_base, backoff_max, on_fail,
         topic, topic_regex, condition, headers_condition, payload_condition)
    VALUES (
        cb_stream_ensure_subscription.stream,
        cb_stream_ensure_subscription.subscription,
        _start, _start,
        coalesce(cb_stream_ensure_subscription.claim_ttl,        interval '30 seconds'),
        coalesce(cb_stream_ensure_subscription.claim_batch_size, 100),
        coalesce(cb_stream_ensure_subscription.max_attempts, 3),
        coalesce(cb_stream_ensure_subscription.max_crashes,  3),
        coalesce(cb_stream_ensure_subscription.backoff_kind, 'full_jitter'),
        coalesce(cb_stream_ensure_subscription.backoff_base, interval '5 seconds'),
        coalesce(cb_stream_ensure_subscription.backoff_max,  interval '5 minutes'),
        coalesce(cb_stream_ensure_subscription.on_fail,      'dead_letter'),
        cb_stream_ensure_subscription.topic,
        _topic_re,
        cb_stream_ensure_subscription.condition,
        _headers_cond,
        _payload_cond
    )
    ON CONFLICT ON CONSTRAINT cb_stream_subscriptions_pkey DO NOTHING;

    -- The retry stream for this subscription.
    PERFORM _cb_stream_ensure(
        'sr.' || cb_stream_ensure_subscription.stream || '.' || cb_stream_ensure_subscription.subscription,
        interval '7 days');

    INSERT INTO cb_stream_subscriptions
        (stream, name, claimed_pos, closed_pos,
         claim_ttl, claim_batch_size, max_attempts, max_crashes,
         backoff_kind, backoff_base, backoff_max, on_fail)
    SELECT 'sr.' || q.stream || '.' || q.name, q.name, 0, 0,
        q.claim_ttl, 1, q.max_attempts, q.max_crashes,
        q.backoff_kind, q.backoff_base, q.backoff_max, q.on_fail
    FROM cb_stream_subscriptions q
    WHERE q.stream = cb_stream_ensure_subscription.stream AND q.name = cb_stream_ensure_subscription.subscription
    ON CONFLICT ON CONSTRAINT cb_stream_subscriptions_pkey DO NOTHING;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION cb_stream_define_schedule(
    stream   text,
    name     text,
    every    interval,
    topic    text        DEFAULT NULL,
    payload  jsonb       DEFAULT NULL,
    headers  jsonb       DEFAULT NULL,
    catch_up cb_catch_up_policy DEFAULT NULL,
    start_at timestamptz DEFAULT NULL
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

    PERFORM 1 FROM cb_streams s WHERE s.name = cb_stream_define_schedule.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', cb_stream_define_schedule.stream USING ERRCODE = 'IRD02';
    END IF;

    -- Topics are never empty; empty means none.
    INSERT INTO cb_stream_schedules AS sc
        (stream, name, every, topic, payload, headers, catch_up, next_at)
    VALUES (
        cb_stream_define_schedule.stream,
        cb_stream_define_schedule.name,
        cb_stream_define_schedule.every,
        nullif(cb_stream_define_schedule.topic, ''),
        coalesce(cb_stream_define_schedule.payload, '{}'),
        coalesce(cb_stream_define_schedule.headers, '{}'),
        coalesce(cb_stream_define_schedule.catch_up, 'skip'),
        coalesce(cb_stream_define_schedule.start_at,
                 clock_timestamp() + cb_stream_define_schedule.every)
    )
    ON CONFLICT ON CONSTRAINT cb_stream_schedules_pkey DO UPDATE
    SET every    = excluded.every,
        topic    = excluded.topic,
        payload  = excluded.payload,
        headers  = excluded.headers,
        catch_up = excluded.catch_up,
        next_at  = CASE
            WHEN cb_stream_define_schedule.start_at IS NOT NULL
                THEN cb_stream_define_schedule.start_at
            WHEN sc.every IS DISTINCT FROM excluded.every
                THEN clock_timestamp() + excluded.every
            ELSE sc.next_at
        END
    -- an identical declaration writes nothing and notifies nothing
    WHERE (sc.every, sc.topic, sc.payload, sc.headers, sc.catch_up)
          IS DISTINCT FROM
          (excluded.every, excluded.topic, excluded.payload, excluded.headers, excluded.catch_up)
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
    key        text        DEFAULT NULL,
    delay      interval    DEFAULT NULL,
    deliver_at timestamptz DEFAULT NULL,

    OUT ref_kind cb_ref_kind,
    OUT ref_id   bigint,
    OUT existing boolean
)
LANGUAGE plpgsql AS $$
BEGIN
    -- Header keys starting with cb_ are for catbird's own use.
    IF EXISTS (SELECT 1 FROM jsonb_object_keys(cb_stream_publish.headers) AS k
               WHERE left(k, 3) = 'cb_') THEN
        RAISE EXCEPTION 'catbird: header keys starting with cb_ are reserved' USING ERRCODE = 'IRD01';
    END IF;

    SELECT p.ref_kind, p.ref_id, p.existing INTO ref_kind, ref_id, existing
    FROM _cb_stream_publish(cb_stream_publish.stream, cb_stream_publish.topic,
        cb_stream_publish.payload, cb_stream_publish.headers, cb_stream_publish.key,
        cb_stream_publish.delay, cb_stream_publish.deliver_at) p;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- The internal version allows reserved header keys prefixed with cb_ for use by retry
-- and dead letter streams.
CREATE FUNCTION _cb_stream_publish(
    stream     text,
    topic      text,
    payload    jsonb,
    headers    jsonb       DEFAULT '{}',
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
    existing := false;

    -- Validate arguments.
    IF delay IS NOT NULL AND _cb_stream_publish.deliver_at IS NOT NULL THEN
        RAISE EXCEPTION 'catbird: cannot specify both delay and deliver_at' USING ERRCODE = 'IRD01';
    END IF;

    _deliver_at := coalesce(_cb_stream_publish.deliver_at, clock_timestamp() + _cb_stream_publish.delay);
    _future := _deliver_at IS NOT NULL AND _deliver_at > clock_timestamp();

    -- Check the stream exists.
    PERFORM 1 FROM cb_streams s WHERE s.name = _cb_stream_publish.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', _cb_stream_publish.stream USING ERRCODE = 'IRD02';
    END IF;

    ---- Deduplication by key. ----
    IF _cb_stream_publish.key IS NOT NULL THEN
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
            VALUES (_cb_stream_publish.stream,
                    _cb_stream_publish.key,
                    CASE WHEN _future THEN 'pending' ELSE 'message' END::cb_ref_kind, _id)
            ON CONFLICT ON CONSTRAINT cb_stream_keys_pkey
            DO UPDATE SET ref_id = k.ref_id WHERE FALSE
            RETURNING k.ref_kind, k.ref_id
        )
        SELECT ref.ref_kind, ref.ref_id INTO ref_kind, ref_id FROM (
            SELECT w.ref_kind, w.ref_id FROM won w
            UNION ALL
            SELECT k.ref_kind, k.ref_id FROM cb_stream_keys k
            WHERE k.stream = _cb_stream_publish.stream
              AND k.key         = _cb_stream_publish.key
            LIMIT 1
        ) ref;
        -- Handle race edge case.
        IF ref_id IS NULL THEN
            SELECT k.ref_kind, k.ref_id INTO ref_kind, ref_id
            FROM cb_stream_keys k
            WHERE k.stream = _cb_stream_publish.stream
              AND k.key         = _cb_stream_publish.key;
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
            (id, stream, topic, payload, headers, deliver_at, key)
        VALUES (
            ref_id,
            _cb_stream_publish.stream,
            _cb_stream_publish.topic,
            _cb_stream_publish.payload,
            _cb_stream_publish.headers,
            _deliver_at,
            _cb_stream_publish.key
        );

        PERFORM pg_notify(current_schema || '.cb_tick',
            to_char(_deliver_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));

        RETURN;
    END IF;

    ---- Immediate delivery: store as message, notify the assigner. ----
    ref_kind := 'message';
    ref_id   := coalesce(_id, nextval(pg_get_serial_sequence('cb_stream_messages', 'id')));

    -- id is GENERATED BY DEFAULT, so explicit inserts are allowed.
    INSERT INTO cb_stream_messages (id, stream, topic, payload, headers)
    VALUES (
        ref_id,
        _cb_stream_publish.stream,
        _cb_stream_publish.topic,
        _cb_stream_publish.payload,
        _cb_stream_publish.headers
    );

    -- Notify the position assigner.
    PERFORM _cb_stream_notify(_cb_stream_publish.stream, _cb_stream_publish.topic);
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- Batch publish: the equivalent of one cb_stream_publish per message, in
-- one call. messages is a jsonb array of {payload, topic?, headers?, key?,
-- delay?, deliver_at?} envelopes; delay is in seconds. Returns one row per
-- element, in input order.
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

        RETURN QUERY
        SELECT p.ref_kind, p.ref_id, p.existing
        FROM cb_stream_publish(
            cb_stream_publish_messages.stream,
            _m->>'topic',
            _m->'payload',
            coalesce(_m->'headers', '{}'),
            _m->>'key',
            CASE WHEN _m ? 'delay'
                THEN make_interval(secs => (_m->>'delay')::double precision) END,
            (_m->>'deliver_at')::timestamptz) p;
    END LOOP;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION _cb_stream_assign_positions(stream text, batch_size int DEFAULT 5000)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    _n int;
BEGIN
    -- Only one process may number a stream at a time.
    IF NOT pg_try_advisory_xact_lock(hashtext('cb_assign:' || _cb_stream_assign_positions.stream)) THEN
        RETURN 0;
    END IF;

    WITH unassigned AS (
        SELECT m.id, row_number() OVER (ORDER BY m.id) AS pos
        FROM cb_stream_messages m
        WHERE m.stream = _cb_stream_assign_positions.stream AND m.pos IS NULL
        ORDER BY m.id
        LIMIT _cb_stream_assign_positions.batch_size
    ), bump AS (
        UPDATE cb_streams s
        SET last_pos = s.last_pos + (SELECT count(*) FROM unassigned)
        WHERE s.name = _cb_stream_assign_positions.stream
        RETURNING s.last_pos - (SELECT count(*) FROM unassigned) AS last_pos
    ), assigned AS (
        UPDATE cb_stream_messages m
        SET pos = bump.last_pos + unassigned.pos
        FROM unassigned, bump
        WHERE m.stream = _cb_stream_assign_positions.stream
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
        PERFORM _cb_stream_notify(_cb_stream_assign_positions.stream, '');
    END IF;
    RETURN _n;
END;
$$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION _cb_stream_deliver_pending(batch_size int DEFAULT 500)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _p cb_stream_pending;
    _msg_id bigint;
    _n int := 0;
BEGIN
    FOR _p IN
        SELECT * FROM cb_stream_pending
        WHERE deliver_at <= clock_timestamp()
        ORDER BY deliver_at LIMIT _cb_stream_deliver_pending.batch_size
        FOR UPDATE SKIP LOCKED
    LOOP
        DELETE FROM cb_stream_pending WHERE id = _p.id;

        -- Publish the message
        INSERT INTO cb_stream_messages (stream, topic, payload, headers)
        VALUES (_p.stream, _p.topic, _p.payload, _p.headers)
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
CREATE FUNCTION _cb_stream_deliver_schedules(batch_size int DEFAULT 500)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _schedule cb_stream_schedules;
    _due_ticks int;  -- ticks due from next_at through now, inclusive (always >= 1)
    _fire_ticks int; -- how many of those ticks this policy actually emits
    _n int := 0;     -- messages published, the return value (matches _cb_stream_deliver_pending)
BEGIN
    FOR _schedule IN
        SELECT * FROM cb_stream_schedules
        WHERE next_at <= clock_timestamp()
        ORDER BY next_at LIMIT _cb_stream_deliver_schedules.batch_size
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
            INSERT INTO cb_stream_messages (stream, topic, payload, headers)
            SELECT _schedule.stream, _schedule.topic, _schedule.payload, _schedule.headers
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
CREATE FUNCTION _cb_stream_prune_messages(stream text, batch_size int DEFAULT 10000)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _retention interval;
    _n bigint;
BEGIN
    SELECT s.retention INTO _retention
    FROM cb_streams s WHERE s.name = _cb_stream_prune_messages.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', _cb_stream_prune_messages.stream USING ERRCODE = 'IRD02';
    END IF;
    IF _retention < interval '0' THEN  -- cb_forever() sentinel: nothing to prune
        RETURN 0;
    END IF;

    WITH doomed AS (
        SELECT m.ctid FROM cb_stream_messages m
        WHERE m.stream = _cb_stream_prune_messages.stream
          AND m.created_at < clock_timestamp() - _retention
        ORDER BY m.pos
        LIMIT _cb_stream_prune_messages.batch_size
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM cb_stream_messages m USING doomed d
    WHERE m.stream = _cb_stream_prune_messages.stream
      AND m.ctid = d.ctid;

    GET DIAGNOSTICS _n = ROW_COUNT;
    RETURN _n;
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION _cb_stream_prune_keys(stream text, batch_size int DEFAULT 10000)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    _retention interval;
    _n bigint;
BEGIN
    SELECT s.retention INTO _retention
    FROM cb_streams s WHERE s.name = _cb_stream_prune_keys.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', _cb_stream_prune_keys.stream USING ERRCODE = 'IRD02';
    END IF;
    IF _retention < interval '0' THEN  -- cb_forever() sentinel: nothing to prune
        RETURN 0;
    END IF;

    WITH doomed AS (
        SELECT k.ctid FROM cb_stream_keys k
        WHERE k.stream = _cb_stream_prune_keys.stream
          AND k.ref_kind = 'message'  -- keep undelivered 'pending' keys
          AND k.ref_created_at < clock_timestamp() - _retention
        ORDER BY k.ref_created_at
        LIMIT _cb_stream_prune_keys.batch_size
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
    _new_pos bigint;
BEGIN
    -- Get current cursor position and conditions.
    SELECT c.pos, c.topic_regex, c.headers_condition, c.payload_condition
    INTO _pos, _regex, _headers, _payload
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
    ORDER BY m.pos;

    UPDATE cb_stream_cursors c SET pos = _new_pos
    WHERE c.stream = cb_stream_read.stream AND c.name = cb_stream_read.cursor;
END; $$;
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
-- Copies a crashing claim's messages to the retry stream.
-- A message the crashed consumer already failed via cb_stream_fail keeps that
-- retry copy instead of gaining a second one.
-- A message that has exceeded max_crashes is moved to the dead letter stream or
-- dropped. A message that retention already pruned is skipped.
-- max_crashes, backoff and on_fail come from the base subscription row, even when
-- the crashing claim is on the retry stream itself.
CREATE FUNCTION _cb_stream_quarantine(claim cb_stream_claims)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _stream text := _cb_stream_quarantine.claim.stream;
    _subscription text := _cb_stream_quarantine.claim.subscription;
    _from_pos bigint := _cb_stream_quarantine.claim.from_pos;
    _to_pos bigint := _cb_stream_quarantine.claim.to_pos;
    _base_name text;
    _retry_stream text;
    _q cb_stream_subscriptions;
    _m cb_stream_messages;
    _origin_pos bigint;
    _crash int;
BEGIN
    _base_name := CASE WHEN _stream LIKE '%.%' THEN split_part(_stream, '.', 2) ELSE _stream END;
    _retry_stream := 'sr.' || _base_name || '.' || _subscription;

    SELECT q.* INTO _q FROM cb_stream_subscriptions q
    WHERE q.stream = _base_name AND q.name = _subscription;

    FOR _m IN
        SELECT m.* FROM cb_stream_messages m
        WHERE m.stream = _stream AND m.pos BETWEEN _from_pos AND _to_pos
          AND (_q.topic_regex IS NULL OR m.topic ~ _q.topic_regex)
          AND (_q.headers_condition IS NULL OR m.headers @@ _q.headers_condition)
          AND (_q.payload_condition IS NULL OR m.payload @@ _q.payload_condition)
        ORDER BY m.pos
    LOOP
        _origin_pos := coalesce((_m.headers->>'cb_origin_pos')::bigint, _m.pos);
        _crash := coalesce((_m.headers->>'cb_crash')::int, 0) + 1;

        -- the dead consumer reported this one failed before it died: the
        -- retry copy it made carries the message from here
        IF EXISTS (SELECT 1 FROM cb_stream_keys k
                   WHERE k.stream = _retry_stream
                     AND k.key = _subscription || ':' || _origin_pos || ':a'
                         || (coalesce((_m.headers->>'cb_attempt')::int, 0) + 1)) THEN
            CONTINUE;
        END IF;

        IF _crash > _q.max_crashes THEN
            IF _q.on_fail = 'dead_letter' THEN
                PERFORM _cb_stream_dead_letter(_stream, _subscription, _m.pos,
                    NULL, _crash, 'crash limit reached');
            END IF;
            CONTINUE;
        END IF;

        PERFORM _cb_stream_publish(
            _retry_stream,
            _m.topic,
            _m.payload,
            key => _subscription || ':' || _origin_pos || ':c' || _crash,
            headers => _m.headers || jsonb_build_object(
                'cb_crash', _crash,
                'cb_origin_pos', _origin_pos),
            delay => _cb_backoff(_q.backoff_kind, _q.backoff_base, _q.backoff_max, _crash));
    END LOOP;
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
--   3. Crashes count only true expiries. A released claim was handed back
--      on purpose and says nothing about its messages.
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

    ---- Try to adopt an expired claim. ----
    -- A released claim was handed back on purpose: it is handed out again
    -- and nothing is counted. Every other expiry is a crash.
    -- A range below max_crashes is handed out again whole, its crashes
    -- count one higher. A range that reached max_crashes is quarantined:
    -- handing it out whole again is pointless, some message in it keeps
    -- killing consumers. A claim holding one message is quarantined on its
    -- first crash: that crash points at its message alone, so there is
    -- nothing left to narrow down. A quarantined claim closes here; from
    -- then on the count travels on the messages themselves (cb_crash).
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

        IF NOT _c.released
           AND (_c.from_pos = _c.to_pos OR _c.crashes >= _q.max_crashes) THEN
            PERFORM _cb_stream_quarantine(_c);

            UPDATE cb_stream_claims c
            SET closed = true
            WHERE (c.stream, c.subscription, c.from_pos)
                = (_c.stream, _c.subscription, _c.from_pos);

            PERFORM _cb_stream_advance_closed_position(cb_stream_claim.stream, cb_stream_claim.subscription);
            CONTINUE;
        END IF;

        -- Hand out the whole range with a fresh deadline.
        UPDATE cb_stream_claims c
        SET consumer   = cb_stream_claim.consumer,
            ttl        = coalesce(cb_stream_claim.ttl, c.ttl),
            expires_at = clock_timestamp() + coalesce(cb_stream_claim.ttl, c.ttl),
            crashes    = c.crashes + CASE WHEN c.released THEN 0 ELSE 1 END,
            released   = false
        WHERE (c.stream, c.subscription, c.from_pos)
            = (_c.stream, _c.subscription, _c.from_pos)
        RETURNING c.from_pos, c.to_pos, c.expires_at
        INTO from_pos, to_pos, expires_at;
        RETURN;
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
        (stream, subscription, from_pos, to_pos, consumer, ttl, expires_at, crashes)
    VALUES (
        cb_stream_claim.stream,
        cb_stream_claim.subscription,
        cb_stream_claim.from_pos,
        cb_stream_claim.to_pos,
        cb_stream_claim.consumer,
        _ttl,
        cb_stream_claim.expires_at,
        0
    );
END;
$$;
-- +goose statementend

-- +goose statementbegin
-- The messages of a claimed range, in order, honoring the subscription's topic
-- and condition.
CREATE FUNCTION cb_stream_read_claim(stream text, subscription text, from_pos bigint, to_pos bigint)
RETURNS SETOF cb_stream_messages
LANGUAGE sql AS $$
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
    ORDER BY m.pos;
$$;
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
    RETURN _expires_at;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- The claim expires immediately and is marked released, so the next
-- cb_stream_claim call may adopt it.
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
    -- Only the current owner may close.
    UPDATE cb_stream_claims c SET closed = true
    WHERE c.stream   = cb_stream_close_claim.stream
      AND c.subscription    = cb_stream_close_claim.subscription
      AND c.consumer = cb_stream_close_claim.consumer
      AND c.from_pos = cb_stream_close_claim.from_pos
      AND NOT c.closed;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    PERFORM _cb_stream_advance_closed_position(cb_stream_close_claim.stream, cb_stream_close_claim.subscription);
END; $$;
-- +goose statementend

-- +goose statementbegin
CREATE FUNCTION _cb_backoff(kind cb_backoff_kind, base_delay interval, max_delay interval, attempt int)
RETURNS interval LANGUAGE plpgsql AS $$
DECLARE
    _cap interval;
BEGIN
    CASE _cb_backoff.kind
    WHEN 'none' THEN
        RETURN '0';
    WHEN 'fixed' THEN
        RETURN least(_cb_backoff.base_delay, _cb_backoff.max_delay);
    WHEN 'full_jitter' THEN
        _cap := least(_cb_backoff.base_delay * (2 ^ least(_cb_backoff.attempt - 1, 20)),
                      _cb_backoff.max_delay);
        RETURN _cap * random();
    ELSE
        RAISE EXCEPTION 'catbird: unknown backoff kind %', _cb_backoff.kind USING ERRCODE = 'IRD01';
    END CASE;
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Publish a message to the stream's dead letter stream (sd.<base>).
-- Exactly one of attempts/crashes is set: the report says which kind of
-- failure exhausted the message. Silently does nothing when retention
-- already dropped the message. The deduplication key collapses duplicate
-- reports of the same failure.
CREATE FUNCTION _cb_stream_dead_letter(
    stream text,
    subscription text,
    pos bigint, -- 'position' is a keyword: legal for columns, not parameters
    attempts int,
    crashes int,
    error text
)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _m cb_stream_messages;
    _base_name text;
    _origin_pos bigint;
BEGIN
    SELECT m.* INTO _m FROM cb_stream_messages m
    WHERE m.stream = _cb_stream_dead_letter.stream
      AND m.pos    = _cb_stream_dead_letter.pos;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    _base_name := CASE WHEN _cb_stream_dead_letter.stream LIKE '%.%'
                  THEN split_part(_cb_stream_dead_letter.stream, '.', 2)
                  ELSE _cb_stream_dead_letter.stream END;

    _origin_pos := coalesce((_m.headers->>'cb_origin_pos')::bigint,
        _cb_stream_dead_letter.pos);

    PERFORM _cb_stream_ensure('sd.' || _base_name);
    PERFORM _cb_stream_publish(
        'sd.' || _base_name,
        _m.topic,
        _m.payload,
        headers => _m.headers || jsonb_build_object(
            'cb_origin_pos', _origin_pos,
            'cb_queue',    _cb_stream_dead_letter.subscription,
            'cb_error',    _cb_stream_dead_letter.error)
            || CASE WHEN _cb_stream_dead_letter.attempts IS NOT NULL
                    THEN jsonb_build_object('cb_attempts', _cb_stream_dead_letter.attempts)
                    ELSE jsonb_build_object('cb_crashes', _cb_stream_dead_letter.crashes) END,
        key => _cb_stream_dead_letter.subscription || ':' || _origin_pos || ':dead_letter');
END; $$;
-- +goose statementend

-- +goose statementbegin
-- Reports that a handler could not process a message. Called once per
-- failed message. While attempts remain the message is republished to the
-- subscription's retry stream with a backoff delay; at max_attempts it is moved to
-- the dead letter stream or dropped. A call from a consumer that no longer
-- holds the covering claim does nothing: the new holder runs the message
-- again and reports for itself. Failing a message does not change the
-- claim: it still closes through cb_stream_close_claim.
CREATE FUNCTION cb_stream_fail(stream text, subscription text, consumer text, pos bigint, error text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _m cb_stream_messages;
    _q cb_stream_subscriptions;
    _base_name text;
    _retry_stream text;
    _attempt int;
    _origin_pos bigint;
BEGIN
    PERFORM 1 FROM cb_stream_claims c
    WHERE c.stream   = cb_stream_fail.stream
      AND c.subscription    = cb_stream_fail.subscription
      AND c.consumer = cb_stream_fail.consumer
      AND NOT c.closed
      AND cb_stream_fail.pos BETWEEN c.from_pos AND c.to_pos;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT m.* INTO _m FROM cb_stream_messages m
    WHERE m.stream = cb_stream_fail.stream AND m.pos = cb_stream_fail.pos;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: message %.% not found',
            cb_stream_fail.stream, cb_stream_fail.pos USING ERRCODE = 'IRD03';
    END IF;

    _base_name := CASE WHEN cb_stream_fail.stream LIKE '%.%'
                  THEN split_part(cb_stream_fail.stream, '.', 2)
                  ELSE cb_stream_fail.stream END;

    SELECT q.* INTO _q FROM cb_stream_subscriptions q
    WHERE q.stream = _base_name AND q.name = cb_stream_fail.subscription;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: subscription %.% not defined', _base_name, cb_stream_fail.subscription USING ERRCODE = 'IRD02';
    END IF;

    _attempt := coalesce((_m.headers->>'cb_attempt')::int, 0) + 1;
    _origin_pos := coalesce((_m.headers->>'cb_origin_pos')::bigint, cb_stream_fail.pos);

    IF _attempt >= _q.max_attempts THEN
        IF _q.on_fail = 'dead_letter' THEN
            PERFORM _cb_stream_dead_letter(cb_stream_fail.stream, cb_stream_fail.subscription,
                cb_stream_fail.pos, _attempt, NULL, cb_stream_fail.error);
        END IF;
        RETURN;
    END IF;

    _retry_stream := 'sr.' || _base_name || '.' || cb_stream_fail.subscription;

    -- the retry stream exists by declaration: cb_stream_ensure_subscription birthed it
    PERFORM _cb_stream_publish(
        _retry_stream,
        _m.topic,
        _m.payload,
        key => cb_stream_fail.subscription || ':' || _origin_pos || ':a' || _attempt,
        headers => _m.headers || jsonb_build_object(
            'cb_attempt',         _attempt,
            'cb_origin_pos', _origin_pos,
            'cb_error',           cb_stream_fail.error),
        delay => _cb_backoff(_q.backoff_kind, _q.backoff_base, _q.backoff_max, _attempt));
END;
$$;
-- +goose statementend

-- +goose down

DROP FUNCTION _cb_stream_deliver_schedules(int);
DROP FUNCTION _cb_stream_prune_messages(text, int);
DROP FUNCTION _cb_stream_prune_keys(text, int);
DROP FUNCTION cb_stream_delete_schedule(text, text);
DROP FUNCTION cb_stream_define_schedule(text, text, interval, text, jsonb, jsonb, cb_catch_up_policy, timestamptz);
DROP TABLE cb_stream_schedules;
DROP TYPE cb_catch_up_policy;

DROP FUNCTION cb_stream_fail(text, text, text, bigint, text);
DROP FUNCTION _cb_stream_dead_letter(text, text, bigint, int, int, text);
DROP FUNCTION _cb_backoff(cb_backoff_kind, interval, interval, int);
DROP FUNCTION cb_stream_close_claim(text, text, text, bigint);
DROP FUNCTION _cb_stream_advance_closed_position(text, text);
DROP FUNCTION cb_stream_release_claim(text, text, text, bigint);
DROP FUNCTION cb_stream_extend_claim(text, text, text, bigint, interval);
DROP FUNCTION cb_stream_claim(text, text, text, interval);
DROP FUNCTION _cb_stream_quarantine(cb_stream_claims);
DROP FUNCTION cb_stream_ensure_subscription(text, text, bigint, interval, int, cb_backoff_kind, interval, interval, cb_fail_policy, int, int, text, text);
DROP FUNCTION cb_stream_ensure_cursor(text, text, bigint, text, text);
DROP FUNCTION cb_stream_read_claim(text, text, bigint, bigint);
DROP FUNCTION cb_stream_read(text, text, int);
DROP TABLE cb_stream_claims;
DROP TABLE cb_stream_subscriptions;
DROP FUNCTION cb_stream_publish_messages(text, jsonb);
DROP FUNCTION cb_stream_publish(text, text, jsonb, jsonb, text, interval, timestamptz);
DROP FUNCTION _cb_stream_publish(text, text, jsonb, jsonb, text, interval, timestamptz);
DROP FUNCTION cb_stream_ensure(text, interval);
DROP FUNCTION _cb_stream_ensure(text, interval);
DROP FUNCTION _cb_stream_ensure_partition(text);
DROP FUNCTION _cb_stream_deliver_pending(int);
DROP FUNCTION _cb_stream_assign_positions(text, int);
DROP FUNCTION _cb_stream_notify(text, text);
DROP TABLE cb_stream_messages;
DROP TABLE cb_stream_cursors;
DROP TABLE cb_stream_keys;
DROP TABLE cb_stream_pending;
DROP TABLE cb_streams;
DROP FUNCTION cb_forever();
DROP FUNCTION _cb_stream_compile_condition(text);
DROP FUNCTION _cb_stream_compile_topic(text);
DROP FUNCTION _cb_valid_stream_name(text);
DROP FUNCTION cb_valid_name(text);
DROP TYPE cb_ref_kind;
DROP TYPE cb_backoff_kind;
DROP TYPE cb_fail_policy;
