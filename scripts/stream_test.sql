-- Scenario tests for migrations/00006_stream.sql. Run via scripts/stream_test.sh
-- against a freshly built cb_scratch. Plain psql: DO blocks with ASSERT, loud
-- failures, no extensions. When the Go phase lands these scenarios move into
-- the Go test suite.
\set ON_ERROR_STOP on

-- Invariant checker, session-local (pg_temp dies with this connection).
-- The structural rule every claim branch must preserve: open and closed
-- claims exactly tile the region (closed_pos, claimed_pos] — no gaps, no
-- overlaps, first claim right after closed_pos, last claim ending at
-- claimed_pos.
CREATE FUNCTION pg_temp.check_claims(stream text, queue text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _q cb_stream_queues;
    _c record;
    _expected bigint;
BEGIN
    SELECT q.* INTO _q FROM cb_stream_queues q
    WHERE q.stream = check_claims.stream AND q.name = check_claims.queue;
    ASSERT FOUND, format('queue %s.%s not found', check_claims.stream, check_claims.queue);
    ASSERT _q.closed_pos <= _q.claimed_pos,
        format('closed_pos %s > claimed_pos %s', _q.closed_pos, _q.claimed_pos);

    _expected := _q.closed_pos + 1;
    FOR _c IN
        SELECT c.* FROM cb_stream_claims c
        WHERE c.stream = check_claims.stream AND c.queue = check_claims.queue
        ORDER BY c.from_pos
    LOOP
        ASSERT _c.from_pos = _expected,
            format('tiling broken: claim starts at %s, expected %s', _c.from_pos, _expected);
        ASSERT _c.crashes >= 0, 'negative crash count';
        _expected := _c.to_pos + 1;
    END LOOP;
    ASSERT _expected = _q.claimed_pos + 1,
        format('tiling broken: claims end at %s, claimed_pos is %s', _expected - 1, _q.claimed_pos);
END; $$;

\echo '== A. publish / assign / cursor read =='
SELECT cb_stream_ensure('orders');
SELECT p.ref_id FROM generate_series(1,5) g,
    LATERAL cb_stream_publish('orders', 'order.placed', to_jsonb(g)) p;
DO $$ BEGIN
    ASSERT _cb_stream_assign_positions('orders') = 5, 'expected 5 assigned';
END $$;
SELECT cb_stream_ensure_cursor('orders', 'idx', 0);
DO $$ BEGIN
    ASSERT (SELECT count(*) FROM cb_stream_read('orders', 'idx', 10)) = 5;
    ASSERT (SELECT pos FROM cb_stream_cursors WHERE stream = 'orders') = 5;
END $$;
-- re-ensure never moves an existing cursor
SELECT cb_stream_ensure_cursor('orders', 'idx', 0);
DO $$ BEGIN
    ASSERT (SELECT pos FROM cb_stream_cursors WHERE stream = 'orders') = 5;
END $$;

\echo '== B. claims: batch, terms, out-of-order close, release, adopt, fence =='
SELECT cb_stream_ensure_queue('orders', 'mailer', 0);
DO $$
DECLARE a record; b record; c record;
BEGIN
    SELECT * INTO a FROM cb_stream_claim('orders', 'mailer', 'c1', 3);
    ASSERT a.from_pos = 1 AND a.to_pos = 3, format('got %s..%s', a.from_pos, a.to_pos);
    SELECT * INTO b FROM cb_stream_claim('orders', 'mailer', 'c2', 3, ttl => '15 minutes');
    ASSERT b.from_pos = 4 AND b.to_pos = 5, format('got %s..%s', b.from_pos, b.to_pos);
    ASSERT (SELECT ttl FROM cb_stream_claims WHERE stream = 'orders' AND from_pos = 4)
        = interval '15 minutes', 'per-call ttl not stored';
    SELECT * INTO c FROM cb_stream_claim('orders', 'mailer', 'c1', 3);
    ASSERT c.from_pos IS NULL, 'expected NULLs when caught up';
    PERFORM pg_temp.check_claims('orders', 'mailer');

    ASSERT cb_stream_extend_claim('orders', 'mailer', 'c1', 1) IS NOT NULL, 'extend failed';
    -- out-of-order close: the gap at 1..3 holds the floor
    PERFORM cb_stream_close_claim('orders', 'mailer', 'c2', 4);
    ASSERT (SELECT closed_pos FROM cb_stream_queues WHERE name = 'mailer') = 0;
    PERFORM pg_temp.check_claims('orders', 'mailer');
    -- release 1..3; c3 adopts it; c1 is fenced out
    PERFORM cb_stream_release_claim('orders', 'mailer', 'c1', 1);
    SELECT * INTO a FROM cb_stream_claim('orders', 'mailer', 'c3', 10);
    ASSERT a.from_pos = 1 AND a.to_pos = 3, 'adoption failed';
    ASSERT cb_stream_extend_claim('orders', 'mailer', 'c1', 1) IS NULL, 'zombie extend not fenced';
    PERFORM cb_stream_close_claim('orders', 'mailer', 'c1', 1); -- no-op
    ASSERT NOT (SELECT closed FROM cb_stream_claims WHERE from_pos = 1), 'zombie close not fenced';
    PERFORM cb_stream_close_claim('orders', 'mailer', 'c3', 1);
    ASSERT (SELECT closed_pos FROM cb_stream_queues WHERE name = 'mailer') = 5, 'chase failed';
    ASSERT (SELECT count(*) FROM cb_stream_claims) = 0, 'claims not cleaned up';
    PERFORM pg_temp.check_claims('orders', 'mailer');
END $$;
-- expected errors
DO $$
DECLARE ok boolean;
BEGIN
    ok := false;
    BEGIN
        PERFORM cb_stream_claim('orders', 'nope', 'c1', 3);
    EXCEPTION WHEN OTHERS THEN ok := SQLERRM LIKE 'catbird:%'; END;
    ASSERT ok, 'undefined queue did not raise';
    ok := false;
    BEGIN
        PERFORM cb_stream_extend_claim('orders', 'mailer', 'c1', 1, ttl => '0');
    EXCEPTION WHEN OTHERS THEN ok := SQLERRM LIKE 'catbird:%'; END;
    ASSERT ok, 'non-positive ttl did not raise';
    ok := false;
    BEGIN
        PERFORM cb_stream_publish('orders', 't', '1', headers => '{"cb_x": 1}');
    EXCEPTION WHEN OTHERS THEN ok := SQLERRM LIKE 'catbird:%'; END;
    ASSERT ok, 'reserved cb_ header did not raise';
END $$;

\echo '== C. keys: keep-oldest dedup, delayed delivery, ref swap =='
DO $$
DECLARE r record;
BEGIN
    SELECT * INTO r FROM cb_stream_publish('orders', 't', '1', key => 'k1');
    ASSERT NOT r.existing;
    SELECT * INTO r FROM cb_stream_publish('orders', 't', '2', key => 'k1');
    ASSERT r.existing, 'duplicate key not skipped';
    SELECT * INTO r FROM cb_stream_publish('orders', 't', '3', key => 'k2', delay => '1 millisecond');
    ASSERT r.ref_kind = 'pending';
END $$;
SELECT pg_sleep(0.02);
DO $$ BEGIN
    ASSERT _cb_stream_deliver_pending() = 1, 'pending not delivered';
    ASSERT (SELECT ref_kind FROM cb_stream_keys WHERE key = 'k2') = 'message',
        'key not swapped to message';
END $$;

\echo '== D. fail: retry stream, duplicate fail, exhaustion, drop policy =='
SELECT cb_stream_ensure('bills');
SELECT cb_stream_ensure_queue('bills', 'payer', 0,
    max_attempts => 2, backoff_kind => 'fixed', backoff_base => '1 millisecond');
SELECT p.ref_id FROM cb_stream_publish('bills', 't', '{"n":1}') p;
SELECT _cb_stream_assign_positions('bills');
DO $$
DECLARE r record;
BEGIN
    SELECT * INTO r FROM cb_stream_claim('bills', 'payer', 'c1', 10);
    PERFORM cb_stream_fail('bills', 'payer', 1, 'boom');
    PERFORM cb_stream_fail('bills', 'payer', 1, 'boom'); -- duplicate collapses
    ASSERT (SELECT count(*) FROM cb_stream_pending) = 1, 'duplicate fail not deduped';
    PERFORM cb_stream_close_claim('bills', 'payer', 'c1', 1);
END $$;
SELECT pg_sleep(0.02);
SELECT _cb_stream_deliver_pending();
SELECT _cb_stream_assign_positions('sr.bills.payer');
DO $$
DECLARE r record;
BEGIN
    ASSERT (SELECT headers->>'cb_attempt' FROM cb_stream_messages
            WHERE stream = 'sr.bills.payer') = '1', 'attempt header wrong';
    SELECT * INTO r FROM cb_stream_claim('sr.bills.payer', 'payer', 'c1', 10);
    PERFORM cb_stream_fail('sr.bills.payer', 'payer', 1, 'boom again'); -- attempt 2 >= 2
    PERFORM cb_stream_close_claim('sr.bills.payer', 'payer', 'c1', 1);
    ASSERT (SELECT count(*) FROM cb_stream_messages WHERE stream = 'sd.bills') = 1,
        'exhausted message not dead-lettered';
    ASSERT (SELECT headers->>'cb_origin_pos' FROM cb_stream_messages
            WHERE stream = 'sd.bills') = '1', 'origin lost';
    -- auto-created stream retention defaults: retry bounded, DLQ forever
    ASSERT (SELECT retention FROM cb_streams WHERE name = 'sr.bills.payer') = interval '7 days',
        'retry stream default retention wrong';
    ASSERT (SELECT retention FROM cb_streams WHERE name = 'sd.bills') = cb_forever(),
        'dead-letter stream should keep forever';
END $$;
-- on_fail = 'drop': retries stop, nothing archived
SELECT cb_stream_ensure('junk');
SELECT cb_stream_ensure_queue('junk', 'binman', 0, max_attempts => 1, on_fail => 'drop');
SELECT p.ref_id FROM cb_stream_publish('junk', 't', '{}') p;
SELECT _cb_stream_assign_positions('junk');
DO $$
DECLARE r record;
BEGIN
    SELECT * INTO r FROM cb_stream_claim('junk', 'binman', 'c1', 10);
    PERFORM cb_stream_fail('junk', 'binman', 1, 'nope');
    ASSERT (SELECT count(*) FROM cb_streams WHERE name LIKE '%junk%') = 1,
        'drop policy created streams';
END $$;

\echo '== E. crash ladder: redeliver, split, solo trial, archive above limit =='
SELECT cb_stream_ensure('jobs');
SELECT cb_stream_ensure_queue('jobs', 'runner', 0, max_crashes => 1);
SELECT p.ref_id FROM generate_series(1,3) g, LATERAL cb_stream_publish('jobs', 't', to_jsonb(g)) p;
SELECT _cb_stream_assign_positions('jobs');
DO $$
DECLARE r record;
BEGIN
    -- fresh claim, then one whole-range redelivery (crash 1 = the limit)
    SELECT * INTO r FROM cb_stream_claim('jobs', 'runner', 'c1', 10);
    ASSERT r.from_pos = 1 AND r.to_pos = 3;
    UPDATE cb_stream_claims SET expires_at = clock_timestamp() WHERE stream = 'jobs';
    SELECT * INTO r FROM cb_stream_claim('jobs', 'runner', 'c2', 10);
    ASSERT r.from_pos = 1 AND r.to_pos = 3, 'below limit should redeliver whole';
    PERFORM pg_temp.check_claims('jobs', 'runner');
    UPDATE cb_stream_claims SET expires_at = clock_timestamp() WHERE stream = 'jobs';
    -- at the limit: split — caller gets [1,1], tail respawns expired
    SELECT * INTO r FROM cb_stream_claim('jobs', 'runner', 'c3', 10);
    ASSERT r.from_pos = 1 AND r.to_pos = 1, 'expected split head [1,1]';
    PERFORM pg_temp.check_claims('jobs', 'runner');
    PERFORM cb_stream_close_claim('jobs', 'runner', 'c3', 1); -- innocent
    -- message 2: its solo slice, then a solo crash -> archived
    SELECT * INTO r FROM cb_stream_claim('jobs', 'runner', 'c4', 10);
    ASSERT r.from_pos = 2 AND r.to_pos = 2;
    UPDATE cb_stream_claims SET expires_at = clock_timestamp()
    WHERE stream = 'jobs' AND from_pos = 2;
    -- message 3 gets its own solo trial in the same call that archives 2
    SELECT * INTO r FROM cb_stream_claim('jobs', 'runner', 'c5', 10);
    ASSERT r.from_pos = 3 AND r.to_pos = 3, 'innocent last message denied its trial';
    PERFORM pg_temp.check_claims('jobs', 'runner');
    ASSERT (SELECT count(*) FROM cb_stream_messages WHERE stream = 'sd.jobs') = 1,
        'solo crasher not archived';
    PERFORM cb_stream_close_claim('jobs', 'runner', 'c5', 3);
    ASSERT (SELECT closed_pos FROM cb_stream_queues WHERE stream = 'jobs') = 3;
    ASSERT (SELECT count(*) FROM cb_stream_claims WHERE stream = 'jobs') = 0;
    PERFORM pg_temp.check_claims('jobs', 'runner');
END $$;



\echo '== F. batch publish =='
DO $$
DECLARE _n int; _before bigint;
BEGIN
    SELECT last_pos INTO _before FROM cb_streams WHERE name = 'orders';
    SELECT count(*) INTO _n
    FROM cb_stream_publish_payloads('orders', 'bulk', array_fill('1'::jsonb, ARRAY[100]));
    ASSERT _n = 100, format('expected 100 ids, got %s', _n);
    PERFORM _cb_stream_assign_positions('orders');
    ASSERT (SELECT last_pos FROM cb_streams WHERE name = 'orders') >= _before + 100,        'batch not assigned';
END $$;
\echo '== G. schedules: fire, catch-up, skip, re-arm, delete, interval guard =='
SELECT cb_stream_ensure('beats');
DO $$
DECLARE _fired int;
BEGIN
    -- On-time tick: one-hour interval, next_at half an interval ago -> one fires,
    -- and the appended message carries the schedule's template payload.
    PERFORM cb_stream_define_schedule('beats', 'ontime',
        every => '1 hour', topic => 'ontime', payload => '{"k":1}',
        start_at => clock_timestamp() - interval '30 minutes');
    _fired := _cb_stream_deliver_schedules();
    ASSERT _fired = 1, format('on-time expected 1 fired, got %s', _fired);
    ASSERT (SELECT count(*) FROM cb_stream_messages WHERE stream = 'beats' AND topic = 'ontime') = 1,
        'on-time message not appended';
    ASSERT (SELECT payload FROM cb_stream_messages WHERE stream = 'beats' AND topic = 'ontime')
        = '{"k":1}'::jsonb, 'template payload not copied';
    ASSERT (SELECT next_at FROM cb_stream_schedules WHERE name = 'ontime') > clock_timestamp(),
        're-arm left next_at in the past';

    -- 'all' catch-up: 3.5 intervals behind -> 4 ticks fire in one set-based insert.
    PERFORM cb_stream_define_schedule('beats', 'catchup',
        every => '1 hour', topic => 'catchup', catch_up => 'all',
        start_at => clock_timestamp() - interval '3 hours 30 minutes');
    _fired := _cb_stream_deliver_schedules();
    ASSERT _fired = 4, format('all catch-up expected 4, got %s', _fired);
    ASSERT (SELECT count(*) FROM cb_stream_messages WHERE stream = 'beats' AND topic = 'catchup') = 4,
        'all catch-up did not append 4';
    ASSERT (SELECT next_at FROM cb_stream_schedules WHERE name = 'catchup') > clock_timestamp(),
        'catch-up next_at not in the future';

    -- 'skip' with a whole missed tick behind it: fire nothing, jump ahead.
    PERFORM cb_stream_define_schedule('beats', 'skipbeat',
        every => '1 hour', topic => 'skipbeat', catch_up => 'skip',
        start_at => clock_timestamp() - interval '3 hours 30 minutes');
    _fired := _cb_stream_deliver_schedules();
    ASSERT _fired = 0, format('skip backlog expected 0, got %s', _fired);
    ASSERT (SELECT count(*) FROM cb_stream_messages WHERE stream = 'beats' AND topic = 'skipbeat') = 0,
        'skip backlog fired anyway';
    ASSERT (SELECT next_at FROM cb_stream_schedules WHERE name = 'skipbeat') > clock_timestamp(),
        'skip did not jump ahead';

    -- 'skip' still fires an on-time tick: policy governs missed ticks only.
    PERFORM cb_stream_define_schedule('beats', 'skipnow',
        every => '1 hour', topic => 'skipnow', catch_up => 'skip',
        start_at => clock_timestamp() - interval '30 minutes');
    _fired := _cb_stream_deliver_schedules();
    ASSERT _fired = 1, format('skip on-time expected 1, got %s', _fired);
END $$;
-- define: declare semantics, no-op writes, re-arm and delete
DO $$
DECLARE _n1 timestamptz; _n2 timestamptz; _x1 text; _x2 text; ok boolean;
BEGIN
    PERFORM cb_stream_define_schedule('beats', 'stable', every => '2 hours', payload => '{"v":9}');
    SELECT next_at INTO _n1 FROM cb_stream_schedules WHERE name = 'stable';

    -- an identical declaration writes nothing: the row version is untouched
    SELECT xmin::text INTO _x1 FROM cb_stream_schedules WHERE name = 'stable';
    PERFORM cb_stream_define_schedule('beats', 'stable', every => '2 hours', payload => '{"v":9}');
    SELECT xmin::text INTO _x2 FROM cb_stream_schedules WHERE name = 'stable';
    ASSERT _x1 = _x2, 'identical declaration rewrote the row';

    -- the call is the whole schedule: the same cadence keeps the phase,
    -- and the omitted payload resets to its default
    PERFORM cb_stream_define_schedule('beats', 'stable', every => '2 hours', topic => 'stable');
    SELECT next_at INTO _n2 FROM cb_stream_schedules WHERE name = 'stable';
    ASSERT _n1 = _n2, 'same cadence moved next_at';
    ASSERT (SELECT payload FROM cb_stream_schedules WHERE name = 'stable') = '{}'::jsonb,
        'omitted payload was kept, not reset';
    ASSERT (SELECT topic FROM cb_stream_schedules WHERE name = 'stable') = 'stable',
        'declared topic not applied';

    -- changing the cadence re-anchors next_at; the omitted topic is gone again
    PERFORM cb_stream_define_schedule('beats', 'stable', every => '3 hours');
    ASSERT (SELECT next_at FROM cb_stream_schedules WHERE name = 'stable') <> _n2,
        'cadence change did not re-anchor next_at';
    ASSERT (SELECT every FROM cb_stream_schedules WHERE name = 'stable') = interval '3 hours';
    ASSERT (SELECT topic FROM cb_stream_schedules WHERE name = 'stable') IS NULL,
        'omitted topic survived re-declaration';

    -- an explicit start_at wins over the re-anchor: the deliberate state poke
    PERFORM cb_stream_define_schedule('beats', 'stable', every => '4 hours',
        start_at => clock_timestamp() + interval '10 minutes');
    ASSERT (SELECT next_at FROM cb_stream_schedules WHERE name = 'stable')
        < clock_timestamp() + interval '11 minutes', 'start_at did not win over the re-anchor';

    -- a cadence is always required; an explicit NULL gets the catbird error
    ok := false;
    BEGIN
        PERFORM cb_stream_define_schedule('beats', 'stable', every => NULL);
    EXCEPTION WHEN OTHERS THEN ok := SQLERRM LIKE 'catbird:%'; END;
    ASSERT ok, 'define with a NULL cadence was accepted';

    -- delete: true when present, false when absent, row gone
    ASSERT cb_stream_delete_schedule('beats', 'stable'), 'delete of present returned false';
    ASSERT NOT cb_stream_delete_schedule('beats', 'ghost'), 'delete of absent returned true';
    ASSERT (SELECT count(*) FROM cb_stream_schedules WHERE name = 'stable') = 0, 'schedule not deleted';
END $$;
-- the interval guard rejects calendar durations, at the API and at the table
DO $$
DECLARE ok boolean;
BEGIN
    ok := false;
    BEGIN
        PERFORM cb_stream_define_schedule('beats', 'daily', every => '1 day');
    EXCEPTION WHEN OTHERS THEN ok := SQLERRM LIKE 'catbird:%'; END;
    ASSERT ok, 'ensure did not reject a calendar interval';
    -- the table CHECK guards a direct insert too (pure-SQL client protection)
    ok := false;
    BEGIN
        INSERT INTO cb_stream_schedules (stream, name, every, next_at)
        VALUES ('beats', 'direct', '1 month', clock_timestamp());
    EXCEPTION WHEN check_violation THEN ok := true; END;
    ASSERT ok, 'table CHECK did not reject a calendar interval';
END $$;

\echo '== H. retention: initial value, prune, forever, gap read =='
-- 'audit' carries a 7-day retention, an initial value at creation
SELECT cb_stream_ensure('audit', '7 days');
SELECT p.ref_id FROM generate_series(1,10) g,
    LATERAL cb_stream_publish('audit', 'evt', to_jsonb(g)) p;
SELECT _cb_stream_assign_positions('audit');
UPDATE cb_stream_messages SET created_at = clock_timestamp() - interval '10 days'
WHERE stream = 'audit' AND pos <= 6;
SELECT cb_stream_ensure_cursor('audit', 'reader', 0);
-- 'keepall' sets no retention: keep forever
SELECT cb_stream_ensure('keepall');
SELECT p.ref_id FROM generate_series(1,3) g,
    LATERAL cb_stream_publish('keepall', 'evt', to_jsonb(g)) p;
SELECT _cb_stream_assign_positions('keepall');
UPDATE cb_stream_messages SET created_at = clock_timestamp() - interval '10 days'
WHERE stream = 'keepall';
DO $$
DECLARE _deleted bigint; _others_before bigint; _others_after bigint; ok boolean;
BEGIN
    SELECT count(*) INTO _others_before
    FROM cb_stream_messages WHERE stream NOT IN ('audit', 'keepall');

    -- column path: no argument -> the stream's own 7-day retention applies
    _deleted := _cb_stream_prune_messages('audit');
    ASSERT _deleted = 6, format('expected 6 pruned via column, got %s', _deleted);
    ASSERT (SELECT count(*) FROM cb_stream_messages WHERE stream = 'audit') = 4,
        'survivor count wrong';

    -- forever: a new stream defaults to cb_forever(), so old messages are kept
    ASSERT (SELECT retention FROM cb_streams WHERE name = 'keepall') = cb_forever(),
        'new stream did not default to forever';
    ASSERT _cb_stream_prune_messages('keepall') = 0, 'forever stream pruned';
    ASSERT (SELECT count(*) FROM cb_stream_messages WHERE stream = 'keepall') = 3,
        'forever stream lost messages';

    -- cross-partition safety: pruning 'audit' touched no other stream
    SELECT count(*) INTO _others_after
    FROM cb_stream_messages WHERE stream NOT IN ('audit', 'keepall');
    ASSERT _others_after = _others_before,
        format('prune hit other streams: %s -> %s', _others_before, _others_after);

    -- position tracking skips the gap: one read returns the 4 survivors and the
    -- cursor lands on the tail, no churn over the 6 deleted positions
    ASSERT (SELECT count(*) FROM cb_stream_read('audit', 'reader', 100)) = 4,
        'read did not skip the pruned gap';
    ASSERT (SELECT pos FROM cb_stream_cursors WHERE stream = 'audit' AND name = 'reader') = 10,
        'cursor did not land on the surviving tail';

    -- survivors are recent, so a second prune removes nothing
    ASSERT _cb_stream_prune_messages('audit') = 0, 'second prune deleted survivors';

    -- whatever a re-ensure mentions, an existing stream is never modified;
    -- retention changes are plain UPDATEs
    PERFORM cb_stream_ensure('audit');
    ASSERT (SELECT retention FROM cb_streams WHERE name = 'audit') = interval '7 days',
        'bare re-ensure changed retention';
    PERFORM cb_stream_ensure('audit', '14 days');
    ASSERT (SELECT retention FROM cb_streams WHERE name = 'audit') = interval '7 days',
        're-ensure modified an existing stream';
    UPDATE cb_streams SET retention = interval '14 days' WHERE name = 'audit';
    ASSERT (SELECT retention FROM cb_streams WHERE name = 'audit') = interval '14 days';
    UPDATE cb_streams SET retention = cb_forever() WHERE name = 'audit';
    ASSERT (SELECT retention FROM cb_streams WHERE name = 'audit') = cb_forever(),
        'cb_forever did not reset retention to forever';

    -- 0 and any non-sentinel negative are rejected with a catbird error, never
    -- silently coerced to forever
    ok := false;
    BEGIN PERFORM cb_stream_ensure('audit', interval '0');
    EXCEPTION WHEN OTHERS THEN ok := SQLERRM LIKE 'catbird:%'; END;
    ASSERT ok, 'zero retention not rejected';
    ok := false;
    BEGIN PERFORM cb_stream_ensure('audit', interval '-5 days');
    EXCEPTION WHEN OTHERS THEN ok := SQLERRM LIKE 'catbird:%'; END;
    ASSERT ok, 'non-sentinel negative not rejected';
END $$;
\echo '== I. key prune: age out, keep pending, forever, delivery refresh =='
-- 'ledger' holds keyed messages under a 7-day retention
SELECT cb_stream_ensure('ledger', '7 days');
SELECT p.ref_id FROM cb_stream_publish('ledger', 't', '1', key => 'old') p;
SELECT p.ref_id FROM cb_stream_publish('ledger', 't', '2', key => 'young') p;
-- 'stuck' waits undelivered for an hour; its key must outlive any retention
SELECT p.ref_id FROM cb_stream_publish('ledger', 't', '3', key => 'stuck', delay => '1 hour') p;
SELECT _cb_stream_assign_positions('ledger');
UPDATE cb_stream_keys SET ref_created_at = clock_timestamp() - interval '10 days'
WHERE stream = 'ledger' AND key IN ('old', 'stuck');
-- 'orders' never set a retention, so even ancient keys are kept
UPDATE cb_stream_keys SET ref_created_at = clock_timestamp() - interval '10 days'
WHERE stream = 'orders' AND key = 'k1';
DO $$
DECLARE _deleted bigint; _others_before bigint; _others_after bigint; ok boolean;
BEGIN
    SELECT count(*) INTO _others_before FROM cb_stream_keys WHERE stream <> 'ledger';

    _deleted := _cb_stream_prune_keys('ledger');
    ASSERT _deleted = 1, format('expected 1 key pruned, got %s', _deleted);
    ASSERT (SELECT count(*) FROM cb_stream_keys WHERE stream = 'ledger' AND key = 'old') = 0,
        'aged key survived';
    ASSERT (SELECT count(*) FROM cb_stream_keys WHERE stream = 'ledger' AND key = 'young') = 1,
        'young key pruned';
    ASSERT (SELECT ref_kind FROM cb_stream_keys WHERE stream = 'ledger' AND key = 'stuck') = 'pending',
        'undelivered pending key pruned';
    -- messages are the other janitor's job: key prune left them alone
    ASSERT (SELECT count(*) FROM cb_stream_messages WHERE stream = 'ledger') = 2,
        'key prune touched messages';

    -- forever stream: prune is a no-op even for ancient keys
    ASSERT _cb_stream_prune_keys('orders') = 0, 'forever stream pruned keys';

    -- pruning 'ledger' touched no other stream's keys
    SELECT count(*) INTO _others_after FROM cb_stream_keys WHERE stream <> 'ledger';
    ASSERT _others_after = _others_before,
        format('key prune hit other streams: %s -> %s', _others_before, _others_after);

    -- undefined stream raises
    ok := false;
    BEGIN PERFORM _cb_stream_prune_keys('ghost');
    EXCEPTION WHEN OTHERS THEN ok := SQLERRM LIKE 'catbird:%'; END;
    ASSERT ok, 'undefined stream did not raise';
END $$;
-- the delivery refresh: a key that waited out the whole retention window gets a
-- fresh clock when its message is delivered — without the ref_created_at bump
-- in _cb_stream_deliver_pending the next prune would delete this key while the
-- message it guards is minutes old, letting a duplicate publish through
SELECT p.ref_id FROM cb_stream_publish('ledger', 't', '4', key => 'reborn', delay => '1 millisecond') p;
UPDATE cb_stream_keys SET ref_created_at = clock_timestamp() - interval '10 days'
WHERE stream = 'ledger' AND key = 'reborn';
SELECT pg_sleep(0.02);
DO $$ BEGIN
    ASSERT _cb_stream_deliver_pending() = 1, 'reborn not delivered';
    ASSERT _cb_stream_prune_keys('ledger') = 0, 'prune deleted a just-delivered key';
    ASSERT (SELECT ref_kind FROM cb_stream_keys WHERE stream = 'ledger' AND key = 'reborn') = 'message',
        'reborn key gone or not swapped';
END $$;

\echo 'ALL STREAM TESTS PASSED'
