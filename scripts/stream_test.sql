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
    FROM cb_stream_publish_batch('orders', 'bulk', array_fill('1'::jsonb, ARRAY[100]));
    ASSERT _n = 100, format('expected 100 ids, got %s', _n);
    PERFORM _cb_stream_assign_positions('orders');
    ASSERT (SELECT last_pos FROM cb_streams WHERE name = 'orders') >= _before + 100,        'batch not assigned';
END $$;
\echo 'ALL STREAM TESTS PASSED'
