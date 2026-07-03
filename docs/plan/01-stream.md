# 01 — stream: the substrate

An ordered, partitioned, append-only log with two read modes. Everything else in
catbird is a consumer of this. Package `stream`; tables `cb_stream_*`.

## 1. The model in five sentences

Producers `INSERT` messages inside their own transaction — that is the whole write
path. A **sequencer** (a tiny, notify-driven ticker) assigns contiguous, commit-ordered
**ordinals** to newly visible rows; nothing downstream ever reads an unsequenced row.
**Ordered groups** consume by cursor: read after position N, process, advance —
optionally inside one transaction with their own effects (exactly-once processing).
**Work groups** consume by **range lease**: claim `[from, to]` with a single counter
bump, process each message, retry failures through the pending table, close the lease.
Retention drops whole partitions whose ordinals every group has passed, subject to an
age cap that wins over laggards.

## 2. Why a sequencer (D1) — the correctness core

**The problem.** Insert order is not commit order. Transaction A inserts id 100 and
stalls; B inserts id 101 and commits. A cursor that reads 101 and advances past it
silently loses 100 when A finally commits. Any design where cursors compare against
insert-assigned ids has this hole, and *transactional publish makes it mandatory* —
a publish rides inside an arbitrary user transaction, so no fixed time watermark is
safe.

**Options considered, in order of rejection:**

- *"Never read rows younger than ~50ms" (the vision's lever).* Unsound. A user
  transaction holding a publish open for 2 seconds straddles any watermark; the
  message is lost. Rejected outright.
- *xid-horizon gating* (store `xid8`, only advance cursors below
  `pg_snapshot_xmin(pg_current_snapshot())`). Correct for deciding row visibility,
  but insufficient to derive a safe **id** watermark (an in-flight transaction may
  hold an id *below* rows you can already see), and any long-running write
  transaction anywhere in the database stalls all consumption. Rejected.
- *PgQ snapshot-diff batches* (per-tick snapshots; a batch = rows visible in tick N
  but not N−1). Fully correct — it is what PgQ actually does — but the snapshot
  predicate infects every downstream read, delivery order is not total in any
  column, and replay/time-travel loses its simple meaning. For a system one person
  wants to hold in their head, the complexity lands in the worst place: every
  consumer. Rejected.
- *Sequencer* — **chosen**. All correctness machinery is concentrated in one
  ~30-line function that runs in one place; every consumer everywhere reads
  `WHERE ordinal > $cursor ORDER BY ordinal` and is trivially, provably right.

**Mechanism.** Messages insert with `ordinal NULL`. The sequencer runs on the
kernel ticker, per stream with unsequenced rows. Wakeups are staged (D17): a plain
fixed-interval poll first (~50–250ms, configurable — this is the correctness path
and all M1–M4 need), accelerated at M5 by a NOTIFY wake with a small debounce
(~10–25ms) once the kernel notifier lands with wire. Correctness never depends on
a notification arriving; the tick remains the safety net:

```sql
-- Election is per sequencing transaction, not per process: whoever wins the
-- try-lock this tick does the work. The lock is xact-scoped and taken INSIDE
-- this transaction, so lock and work share one connection and one lifetime — a
-- session-level lock on a pooled connection can outlive or predecease the
-- sequencing transaction and let two sequencers interleave, re-stamping already-
-- sequenced rows with new ordinals. (Advisory locks were rejected as a *claim*
-- mechanism, not for election.)
CREATE FUNCTION cb_stream_sequence(stream text, batch int DEFAULT 5000)
RETURNS int   -- rows sequenced; 0 = caught up, or lost the election
LANGUAGE plpgsql AS $$
DECLARE
    _n int;
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtext('cb_seq:' || stream)) THEN
        RETURN 0;
    END IF;

    WITH todo AS (
        SELECT m.id, row_number() OVER (ORDER BY m.id) AS rn
        FROM cb_stream_message m
        WHERE m.stream = cb_stream_sequence.stream AND m.ordinal IS NULL
        ORDER BY m.id
        LIMIT batch
    ), bump AS (
        UPDATE cb_stream s
        SET last_ordinal = s.last_ordinal + (SELECT count(*) FROM todo)
        WHERE s.name = cb_stream_sequence.stream
        RETURNING s.last_ordinal - (SELECT count(*) FROM todo) AS base
    ), stamped AS (
        UPDATE cb_stream_message m
        SET ordinal = bump.base + todo.rn
        FROM todo, bump
        WHERE m.stream = cb_stream_sequence.stream
          AND m.id = todo.id
          AND m.ordinal IS NULL
        -- the trailing "ordinal IS NULL" is a seatbelt: if lock discipline is
        -- ever broken, it turns silent re-stamping into a detectable gap
        RETURNING 1
    )
    SELECT count(*) INTO _n FROM stamped;

    IF _n > 0 THEN
        PERFORM pg_notify(current_schema || '.cb_s_' || stream, '');  -- consumer wake (M5)
    END IF;
    RETURN _n;
END;
$$;
```

Properties that fall out:

- **Ordinals are contiguous per stream** (a counter, not a sequence — no gaps,
  ever). "Am I caught up" and "is this range complete" become integer arithmetic.
- Only *visible* rows get ordinals, so ordinal order = commit-batch order, ties
  broken by insert id. A row that never commits never exists downstream.
- The sequencer is idempotent and crash-safe: it assigns or it doesn't; on restart
  it continues from `last_ordinal`. If no node holds the advisory lock momentarily,
  sequencing pauses — delivery pauses, nothing is lost.
- After sequencing, the SQL fires `pg_notify` per touched stream. Emission is
  there from day one (one line, costs nothing without listeners); processes start
  *listening* at M5 (D17) — until then consumers wake on their own tick.

**The honest costs.** (1) One extra `UPDATE` per message — double heap/WAL traffic
versus insert-only. At hundreds/sec this is noise; it would matter at Kafka scale,
which is a stated non-goal. (2) A delivery latency floor. Poll-only (M1–M4): roughly one to two
tick intervals end-to-end — ~100–500ms depending on configuration. With the NOTIFY
accelerator (M5): **~30–80ms end-to-end** for *all* consumers, work groups
included. This replaces the vision's "sub-50ms" claim (README amendment 4). The audience —
emails, indexers, notifications, flow steps — does not feel 50ms. (3) A latency
spike while a sequencer leader hand-off happens (sub-second).

## 3. Table shape

All static — creating a stream creates a partition, never a table family.

```
cb_stream          name PK · last_ordinal · notify_channel (nullable — the wire
                   nudge fired on actual append, 02 §4) · retention config (§10)
                   · created_at
cb_stream_message  PARTITION BY LIST (stream), then RANGE (created_at) per stream
                   stream · id (bigint identity) · ordinal (bigint, NULL until
                   sequenced) · topic · target_grp (nullable — set on retry
                   re-appends; consumed only by that group, skipped by all
                   others) · payload jsonb · headers jsonb · created_at
                   (clock_timestamp())
                   indexes: (stream, ordinal) btree per partition; nothing else hot
cb_stream_group    stream · name PK(stream,name) · mode ('ordered'|'work') ·
                   position (ordered: cursor; work: claim_next) · watermark (work:
                   highest ordinal below which all leases closed) · retry policy
                   columns (§7) · created_at
cb_stream_lease    stream · grp · from_ord · to_ord · worker · state
                   ('live'|'released'|'closed' — closed rows linger only until
                   the watermark passes, §5) · created_at   (tiny, mostly empty)
cb_stream_worker   worker id PK · last_heartbeat            (UNLOGGED — see §11)
cb_stream_pending  id PK · stream · topic · payload · headers · deliver_at ·
                   coalesce_key (nullable, UNIQUE(stream, coalesce_key)) ·
                   target_grp · attempt · origin_ordinal (nullable) · cron_spec
                   (nullable) · catch_up_policy (nullable) ·
                   UNIQUE(stream, target_grp, origin_ordinal) WHERE
                   origin_ordinal IS NOT NULL — makes cb_stream_fail idempotent
cb_stream_dedup    stream · key PK(stream,key) · ref_kind ('message'|'pending') ·
                   ref_id · created_at
```

Postgres quirk to plan for: a partitioned table's unique constraints must include
all partition key columns, so the message PK is `(stream, created_at, id)`;
addressing is by the `(stream, ordinal)` index in practice. Rows move ordinals via
`UPDATE`, never partitions (partition key is `created_at`, which never changes).

## 4. Ordered groups (cursor mode)

One logical reader per group — order *requires* that; parallelism comes from more
groups or sharding by key (as the flow projection does, 03 §4). The Go API offers
two shapes:

```go
// exactly-once: effects + ack in one transaction (the same-DB superpower)
stream.Consume(ctx, pool, "orders", "indexer", func(ctx context.Context, tx pgx.Tx, batch []stream.Message) error { … })

// at-least-once: for handlers with external effects
stream.ConsumeFunc(ctx, pool, "orders", "mailer", func(ctx context.Context, batch []stream.Message) error { … })
```

Internals of one iteration: `SELECT … FROM cb_stream_group … FOR UPDATE` (pins the
group row — a second competing reader blocks rather than corrupts), read
`ordinal > position AND ordinal <= (SELECT last_ordinal …) ORDER BY ordinal LIMIT n`,
run the handler, `UPDATE … SET position = $high`, commit. Failure semantics per
group config: `block` (default — retry the batch with backoff in place; ordered
means ordered) or `dlq` (append the poison message to the DLQ stream, advance past
it).

The exactly-once loop is small enough to show whole:

```go
func Consume(ctx context.Context, pool *pgxpool.Pool, stream, grp string,
	handler func(context.Context, pgx.Tx, []Message) error,
) error {
	for {
		waitForWakeup(ctx, stream, grp) // tick (D17); + group notify from M5
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			var pos int64
			// pin the group row — a second competing reader blocks here
			// rather than double-processing
			if err := tx.QueryRow(ctx,
				`SELECT position FROM cb_stream_group
				 WHERE stream = $1 AND name = $2 FOR UPDATE`,
				stream, grp).Scan(&pos); err != nil {
				return err
			}
			msgs, err := fetch(ctx, tx, stream, grp, pos, batchSize)
			// fetch: ordinal > pos AND ordinal <= cb_stream.last_ordinal
			//        AND (target_grp IS NULL OR target_grp = grp)
			//        [+ prefix/header filter]  ORDER BY ordinal
			if err != nil || len(msgs) == 0 {
				return err
			}
			if err := handler(ctx, tx, msgs); err != nil {
				return err // rollback: effects AND cursor, together
			}
			_, err = tx.Exec(ctx,
				`UPDATE cb_stream_group SET position = $3
				 WHERE stream = $1 AND name = $2`,
				stream, grp, msgs[len(msgs)-1].Ordinal)
			return err
			// commit = effects + ack atomically: exactly-once processing
		})
		if err != nil {
			applyGroupFailurePolicy(err) // block w/ backoff, or dlq + advance
		}
	}
}
```

**The filtering contract (owned here; 02 and 03 refer back):** SQL evaluates only
cheap predicates — `target_grp IS NULL OR target_grp = $grp` (always), an optional
equality match on one header key (how flow shards route, 03 §4), and an optional
topic *prefix*. Wildcard topic patterns (`?`/`*`) are matched **in Go only**, by
the ported trie, after the batch read — the matcher exists once, never twice.
Either way the cursor advances over skipped rows.

## 5. Work groups (range leases) — the pgmq/pgq re-evaluation (D2)

Your note asked whether the vision's pgmq/pgq hybrid is the optimum. It isn't quite:
once ordinals are contiguous (D1), per-message SKIP-LOCKED claiming — pgmq's core —
becomes unnecessary machinery. Claiming a batch is a **single counter bump**:

```sql
CREATE FUNCTION cb_stream_claim(stream text, grp text, worker text, batch int)
RETURNS TABLE (from_ord bigint, to_ord bigint)
LANGUAGE plpgsql AS $$
DECLARE
    _high bigint; _from bigint; _to bigint;
BEGIN
    -- 1. adopt a released (crashed-worker) lease first — cold path, tiny table
    UPDATE cb_stream_lease l
    SET worker = cb_stream_claim.worker, state = 'live'
    WHERE (l.stream, l.grp, l.from_ord) = (
        SELECT r.stream, r.grp, r.from_ord FROM cb_stream_lease r
        WHERE r.stream = cb_stream_claim.stream AND r.grp = cb_stream_claim.grp
          AND r.state = 'released'
        LIMIT 1 FOR UPDATE SKIP LOCKED)
    RETURNING l.from_ord, l.to_ord INTO _from, _to;
    IF FOUND THEN
        RETURN QUERY VALUES (_from, _to); RETURN;
    END IF;

    -- 2. hot path: one row lock, no scan — bump the counter to the sequenced high
    SELECT s.last_ordinal INTO _high FROM cb_stream s
    WHERE s.name = cb_stream_claim.stream;

    WITH cur AS (
        SELECT g.position FROM cb_stream_group g
        WHERE g.stream = cb_stream_claim.stream AND g.name = grp FOR UPDATE
    )
    UPDATE cb_stream_group g
    SET position = least(cur.position + batch, _high)
    FROM cur
    WHERE g.stream = cb_stream_claim.stream AND g.name = grp
      AND cur.position < _high
    RETURNING cur.position + 1, g.position INTO _from, _to;
    IF _from IS NULL THEN RETURN; END IF;   -- caught up, nothing to claim

    INSERT INTO cb_stream_lease (stream, grp, from_ord, to_ord, worker, state)
    VALUES (stream, grp, _from, _to, worker, 'live');
    RETURN QUERY VALUES (_from, _to);
END;
$$;

-- Closing advances the watermark over contiguous closed ranges. Ranges are
-- contiguous by construction (the counter bump), so this is a simple chase:
CREATE FUNCTION cb_stream_close_lease(stream text, grp text, from_ord bigint)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE _w bigint;
BEGIN
    UPDATE cb_stream_lease SET state = 'closed'
    WHERE (cb_stream_lease.stream, cb_stream_lease.grp, cb_stream_lease.from_ord)
        = (stream, grp, from_ord);

    SELECT g.watermark INTO _w FROM cb_stream_group g
    WHERE g.stream = cb_stream_close_lease.stream AND g.name = grp FOR UPDATE;
    LOOP
        DELETE FROM cb_stream_lease l
        WHERE l.stream = cb_stream_close_lease.stream AND l.grp = grp
          AND l.state = 'closed' AND l.from_ord = _w + 1
        RETURNING l.to_ord INTO _w;
        EXIT WHEN NOT FOUND;
    END LOOP;
    UPDATE cb_stream_group g SET watermark = _w
    WHERE g.stream = cb_stream_close_lease.stream AND g.name = grp;
END;
$$;
```

No scan, no anti-join, no lock queue — one hot row per group. Before bumping the
counter, a worker first adopts any `released` lease (`FOR UPDATE SKIP LOCKED` on the
tiny lease table — cold path, fine). Then per message in the range: run the handler;
on failure, write a retry into `cb_stream_pending` (§6) or the DLQ (§9) per policy.
A lease closes when every message *fetched for it* is handled (succeeded, retried,
or dead-lettered) — defined over what was fetched, not the ordinal range, so a
partition dropped mid-lease (§10) cannot wedge it. On close, delete the lease and
advance the group `watermark` over contiguous closed ranges. The watermark is the
group's retention floor. An adopted lease may re-handle messages the dead worker
already resolved; that is why `cb_stream_fail` is idempotent (§7).

Liveness: a background goroutine per worker process heartbeats `cb_stream_worker`
(one row per worker per interval — decoupled from handler duration, exactly as the
vision demands). A sweeper (kernel ticker) flips leases of stale workers to
`released`. A crashed worker's whole range is redelivered — coarser duplicates than
per-message claims, the price of the simpler mechanism; at-least-once either way.

What survives from each lineage: **pgq** — ordinals, batches, ticker, retention by
rotation; **pgmq** — nothing on the hot path; SKIP LOCKED survives only on the cold
lease-adoption path and in the sweeper. Today's queue semantics (`Send`/`Read`
with hide) are *not* re-exposed; the work-group API replaces them.

## 6. The pending table — one mechanism for four features (D3)

Everything that means "a message that should appear on a stream at time T" is one
table and one sweeper (kernel ticker, wakes on min(`deliver_at`) like today's
`visible_at` timers):

| Feature | Row shape |
|---|---|
| Delayed delivery | `deliver_at` in the future |
| Retry with backoff | `attempt = n`, `origin_ordinal` + `target_grp` set, `deliver_at = now + backoff(n)`. The re-append carries `target_grp` and the attempt count forward: only the failing group sees the retry (other groups on the stream already handled the original — without targeting, every retry would double-deliver to every other group and break the exactly-once headline), and the next failure backs off from `n+1`, not from 1 |
| Coalesce / debounce (keep-newest) | `coalesce_key` set — `INSERT … ON CONFLICT (stream, coalesce_key) DO UPDATE SET payload, deliver_at` |
| Cron | `cron_spec` set — on delivery the sweeper appends the message **and** re-inserts the row at the next occurrence, honoring `catch_up_policy` (`skip`\|`all`, ported from today's scheduler, including the on-time-tick fix from #45) |

The sweeper appends due rows to their stream in one transaction with the delete —
exactly-once handoff. The current `scheduler.go` module dissolves into rows in this
table plus builder sugar (`stream.Cron(...)`, `flow.RunEvery(...)`).

```sql
-- kernel-ticker job, leader-elected; wakes on min(deliver_at) and on the
-- '.cb_pending' notify that publish fires for new earlier rows
CREATE FUNCTION cb_stream_sweep_pending(batch int DEFAULT 500)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _p cb_stream_pending; _mid bigint; _n int := 0;
BEGIN
    FOR _p IN
        SELECT * FROM cb_stream_pending
        WHERE deliver_at <= clock_timestamp()
        ORDER BY deliver_at LIMIT batch
        FOR UPDATE SKIP LOCKED          -- belt over the leader election
    LOOP
        DELETE FROM cb_stream_pending WHERE id = _p.id;

        -- delivery IS the append, so publish's tail duties move here:
        -- message row, dedup-ref swap, sequencer notify, wire nudge (§8)
        INSERT INTO cb_stream_message (stream, topic, payload, headers, target_grp)
        VALUES (_p.stream, _p.topic, _p.payload, _p.headers, _p.target_grp)
        RETURNING id INTO _mid;
        UPDATE cb_stream_dedup d SET ref_kind = 'message', ref_id = _mid
        WHERE d.ref_kind = 'pending' AND d.ref_id = _p.id AND d.stream = _p.stream;
        PERFORM cb_stream_notify_append(_p.stream, _p.topic);  -- cb_seq + notify_channel

        IF _p.cron_spec IS NOT NULL THEN
            -- cron = a pending row that re-inserts itself; next occurrence per
            -- catch_up_policy ('skip' | 'all'), semantics ported from today's
            -- scheduler including the on-time-tick fix from #45
            INSERT INTO cb_stream_pending (stream, topic, payload, headers,
                                           deliver_at, cron_spec, catch_up_policy)
            VALUES (_p.stream, _p.topic, _p.payload, _p.headers,
                    cb_cron_next(_p.cron_spec, _p.deliver_at, _p.catch_up_policy),
                    _p.cron_spec, _p.catch_up_policy);
        END IF;
        _n := _n + 1;
    END LOOP;
    RETURN _n;
END;
$$;
```

(`cb_stream_notify_append` is the tiny shared tail of `cb_stream_publish` — one
place fires the sequencer notify and the per-stream wire nudge, so the two append
paths cannot drift.)

## 7. Retry policy lives in the database (D4)

Your note: robustness machinery is currently Go-side (`handler_opts.go`) and
therefore invisible to non-Go workers and to SQL. Move the *policy* into columns on
`cb_stream_group` (and per-step overrides in flow, 03 §6):

```
max_attempts int · backoff_kind ('none'|'fixed'|'full_jitter') ·
backoff_base interval · backoff_max interval · on_exhaust ('dlq'|'drop')
```

`cb_stream_fail(stream, grp, ordinal, error)` reads the attempt count from the
failing message (carried on its re-append, §6), computes the next `deliver_at`
from these columns, and writes pending or DLQ — with `ON CONFLICT DO NOTHING` on
`(stream, target_grp, origin_ordinal)`, so a crashed-and-adopted lease failing the
same message twice yields one retry, not a multiplying family of them.

```sql
CREATE FUNCTION cb_stream_fail(stream text, grp text, ordinal bigint, error text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _g cb_stream_group; _m cb_stream_message;
    _attempt int; _origin bigint;
BEGIN
    SELECT * INTO _g FROM cb_stream_group g
    WHERE g.stream = cb_stream_fail.stream AND g.name = grp;
    SELECT * INTO _m FROM cb_stream_message m
    WHERE m.stream = cb_stream_fail.stream AND m.ordinal = cb_stream_fail.ordinal;

    _attempt := coalesce((_m.headers->>'attempt')::int, 0) + 1;
    -- retries of retries keep pointing at the first ordinal
    _origin  := coalesce((_m.headers->>'origin_ordinal')::bigint, ordinal);

    IF _attempt >= _g.max_attempts THEN
        IF _g.on_exhaust = 'dlq' THEN
            -- the DLQ is an ordinary stream (§9); the dedup key makes
            -- exhaustion idempotent under duplicate fails — composition pays
            PERFORM cb_stream_publish(
                stream || '.dlq', _m.topic, _m.payload,
                _m.headers || jsonb_build_object(
                    'origin_stream', stream, 'origin_ordinal', _origin,
                    'grp', grp, 'attempts', _attempt, 'last_error', error,
                    'failed_at', clock_timestamp()),
                key := stream || ':' || grp || ':' || _origin);
        END IF;
        RETURN;
    END IF;

    INSERT INTO cb_stream_pending (stream, topic, payload, headers, deliver_at,
                                   target_grp, attempt, origin_ordinal)
    VALUES (stream, _m.topic, _m.payload,
            _m.headers || jsonb_build_object('attempt', _attempt,
                                             'origin_ordinal', _origin),
            clock_timestamp() + cb_backoff(_g.backoff_kind, _g.backoff_base,
                                           _g.backoff_max, _attempt),
            grp, _attempt, _origin)
    ON CONFLICT (stream, target_grp, origin_ordinal) DO NOTHING;   -- idempotent
END;
$$;
```

(`cb_backoff` is a pure function of the policy columns and attempt — fixed or
full-jitter, ported from `backoff.go`.) A Python worker that calls it gets
identical behavior to the Go worker — this is the existing "engine logic in SQL"
principle applied to robustness. Go builders write these columns at ensure-time;
`WithFullJitterBackoff(...)` keeps its API but becomes config, not behavior.
Client-side machinery that protects the *process* (circuit breaker, panic recovery)
stays in Go.

## 8. Dedup (D5)

The vision name-drops "a dedup table"; this is it. Appended rows are immutable, so
the two dedup flavors split cleanly:

- **Keep-oldest** (today's `concurrency_key`): publish with a key does
  `INSERT INTO cb_stream_dedup … ON CONFLICT DO NOTHING`; on conflict the message
  insert is skipped and the existing `message_id` is returned — the atomic
  `WHERE FALSE` + `UNION ALL` pattern from the current codebase carries over
  verbatim. Dedup rows expire after a per-stream window (janitor prunes; the window
  is the guarantee).
- **Keep-newest** is only meaningful *before* append, so it lives in pending as
  coalescing (§6). "Reindex user X, debounced 30s" is the canonical use.

Option combinations, explicitly: a dedup key on a *delayed* publish stores a
reference to the pending row (`ref_kind = 'pending'`), swapped to the message on
delivery — the dedup window spans both stages. A dedup key *and* a coalesce key on
one publish is an error: coalescing already is keep-newest dedup; pick one.

**`cb_stream_publish`, sketched** — every write path in one function: immediate,
deduped, delayed, coalesced.

```sql
CREATE FUNCTION cb_stream_publish(
    stream       text,
    topic        text,
    payload      jsonb,
    headers      jsonb    DEFAULT '{}',
    key          text     DEFAULT NULL,  -- dedup, keep-oldest (§8)
    delay        interval DEFAULT NULL,  -- delayed delivery (§6)
    coalesce_key text     DEFAULT NULL   -- keep-newest / debounce (§6)
) RETURNS TABLE (ref_kind text, ref_id bigint)
LANGUAGE plpgsql AS $$
DECLARE
    _chan text; _id bigint; _at timestamptz; _kind text; _ref bigint;
BEGIN
    IF key IS NOT NULL AND coalesce_key IS NOT NULL THEN
        RAISE EXCEPTION 'catbird: dedup key and coalesce key are mutually exclusive';
    END IF;

    -- One PK read, deliberately on the hot path (not a join): existence check —
    -- a clearer error than the missing-partition one — plus the stream's
    -- optional wire-nudge channel (02 §4).
    SELECT s.notify_channel INTO _chan
    FROM cb_stream s WHERE s.name = cb_stream_publish.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', stream;
    END IF;

    ------------------------------------------------------------------ coalesce
    IF coalesce_key IS NOT NULL THEN
        _at := clock_timestamp() + coalesce(delay, interval '0');
        INSERT INTO cb_stream_pending AS p
            (stream, topic, payload, headers, deliver_at, coalesce_key)
        VALUES (stream, topic, payload, headers, _at, coalesce_key)
        ON CONFLICT (stream, coalesce_key) DO UPDATE SET
            payload    = EXCLUDED.payload,                    -- keep-newest
            headers    = EXCLUDED.headers,
            deliver_at = least(p.deliver_at, EXCLUDED.deliver_at)
            -- least() = throttle semantics: fire at the earliest requested time
            -- with the newest payload — cannot starve under constant traffic.
            -- Trailing debounce (take EXCLUDED.deliver_at instead) postpones
            -- forever under load and would need a max-wait cap; add only if a
            -- real workload asks for it.
        RETURNING p.id INTO _id;
        PERFORM pg_notify(current_schema || '.cb_pending',
            to_char(_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
            -- today's visible_at timestamp encoding, reused: the Go sweeper
            -- parses it and re-arms its timer if this is now the earliest
        RETURN QUERY VALUES ('pending'::text, _id);
        RETURN;
    END IF;

    ------------------------------------------------------------------- delayed
    IF delay IS NOT NULL THEN
        -- pre-allocate the pending id so the dedup claim (same shape as the
        -- immediate path below, ref_kind = 'pending') can reference it; the
        -- sweeper swaps the ref to the delivered message — the dedup window
        -- spans both stages
        _id := nextval(pg_get_serial_sequence('cb_stream_pending', 'id'));
        -- [dedup claim here iff key IS NOT NULL — identical to below]
        _at := clock_timestamp() + delay;
        INSERT INTO cb_stream_pending (id, stream, topic, payload, headers, deliver_at)
        VALUES (_id, stream, topic, payload, headers, _at);
        PERFORM pg_notify(current_schema || '.cb_pending', to_char(_at AT TIME
            ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
        RETURN QUERY VALUES ('pending'::text, _id);
        RETURN;   -- no sequencer notify, no wire nudge: nothing was appended
    END IF;

    ----------------------------------------------------------------- immediate
    _id := nextval(pg_get_serial_sequence('cb_stream_message', 'id'));
    -- (id is GENERATED BY DEFAULT — explicit inserts allowed)

    IF key IS NOT NULL THEN
        -- today's atomic keep-oldest pattern, kept verbatim — do not simplify:
        -- WHERE FALSE blocks mutation on conflict; UNION ALL returns the
        -- existing ref
        WITH won AS (
            INSERT INTO cb_stream_dedup AS d (stream, key, ref_kind, ref_id)
            VALUES (stream, key, 'message', _id)
            ON CONFLICT (stream, key)
            DO UPDATE SET ref_id = d.ref_id WHERE FALSE
            RETURNING d.ref_kind, d.ref_id
        )
        SELECT x.ref_kind, x.ref_id INTO _kind, _ref FROM (
            SELECT w.ref_kind, w.ref_id FROM won w
            UNION ALL
            SELECT d.ref_kind, d.ref_id FROM cb_stream_dedup d
            WHERE d.stream = cb_stream_publish.stream
              AND d.key    = cb_stream_publish.key
            LIMIT 1
        ) x;
        IF _ref IS NULL THEN
            -- rare: we lost to a claim that committed *during* our conflict
            -- wait — invisible to this statement's snapshot. A fresh statement
            -- gets a fresh snapshot; this closes the NULL-return edge the
            -- single-statement form has always had.
            SELECT d.ref_kind, d.ref_id INTO _kind, _ref FROM cb_stream_dedup d
            WHERE d.stream = cb_stream_publish.stream
              AND d.key    = cb_stream_publish.key;
        END IF;
        IF _ref <> _id THEN
            RETURN QUERY VALUES (_kind, _ref);
            RETURN;   -- keep-oldest: no message row, no notify at all (02 §4)
        END IF;
    END IF;

    INSERT INTO cb_stream_message (id, stream, topic, payload, headers)
    VALUES (_id, stream, topic, payload, headers);

    PERFORM pg_notify(current_schema || '.cb_seq', stream);   -- sequencer (D17)
    IF _chan IS NOT NULL THEN
        PERFORM pg_notify(_chan, topic);   -- wire nudge: on actual append only
    END IF;
    RETURN QUERY VALUES ('message'::text, _id);
END;
$$;
```

Sketch-level notes: real code prefixes parameters per the existing
`cb_send.queue`-style qualification convention. The batch variant is the same
shape over `unnest()` with one sequencer notify per touched stream. The pending
sweeper owes three things at delivery time: swap the dedup ref from `pending` to
the delivered message, notify the sequencer, and fire the stream's wire nudge —
delivery *is* the append, so the nudges move with it.

## 9. Dead letters (D6)

A DLQ is an ordinary stream named `<stream>.dlq`, created lazily on first use.
Exhausted messages are appended there with headers
`{origin_stream, origin_ordinal, grp, attempts, last_error, failed_at}`. Replay is
`stream.Redrive(dlq, n)` — republish to the origin stream (new ordinal, attempt
reset, a `redriven_from` header). Because it is just a stream: it has retention,
it can be consumed (alerting), and the dashboard lists it for free. No special
machinery anywhere.

## 10. Partition lifecycle and retention (D7)

**Creation is proactive, and there is no DEFAULT partition.** A kernel-ticker job
pre-creates each stream's next range partition ahead of time (keep two future
partitions standing; alert when the job falls behind), so a publish never meets a
missing partition — and if one ever does, it fails loudly. The tempting
DEFAULT-partition safety net is a trap: once rows land in it, the overlapping
range partition can no longer be created without locking and moving data.

**Granularity is per-stream config, sized to keep each stream at ~2–15
partitions:** weekly by default, daily for hot streams (the bus), monthly for
long-retention flow audit streams. This sizing rule is what keeps cursor reads
cheap with no further machinery — reads filter on `ordinal` while partitions are
ranged on `created_at`, so Postgres cannot prune them, and every read is a
MergeAppend over one index probe per partition of that stream; with a handful of
partitions, that's negligible. Deferred, with an explicit trigger: only if a
stream ever genuinely needs fine granularity × long retention (>20 partitions on
one stream), add a small partition catalog (per-partition min/max ordinal,
maintained by the janitor) so reads can derive a `created_at` prune predicate.
The naive shortcut — `created_at >= last_seen − slack` — is incorrect:
long-running transactions carry old timestamps but receive high ordinals, and
straddle any fixed slack.

**Dropping** is per-stream policy, all floors intersected, evaluated by a janitor
on the kernel ticker:

```
floor = min( ordered-group cursors …, work-group watermarks …, pinned? )
drop partition P when max(ordinal in P) < floor AND age(P) > min_age
AGE CAP: when age(P) > max_age, drop anyway; force-advance every lagging
         consumer — ordered cursors AND work-group position/watermark — to
         max(ordinal in P) + 1, computed inside the drop transaction, and
         publish a `$sys.data_loss` event to the bus
```

The age cap is what makes abandoned consumers survivable and is **structural, not
optional** — under fan-out-on-read (02) a lagging cursor pins storage shared by
everyone. Flow event streams add a third floor: all runs with events in P are
terminal (03 §8). `DROP` is instant and leaves no dead tuples — this is the answer
to pgmq-style delete bloat, unchanged from the vision.

Force-advance mechanics: the target is `max(ordinal in P) + 1` *recomputed at drop
time*, not the next partition's min ordinal — a long-running insert can commit
into P at the last moment and receive a fresh high ordinal, and `DROP`'s ACCESS
EXCLUSIVE lock serializes with exactly that insert, so the recomputation sees it.
Work groups advance both `position` and `watermark` (taking `max` with current
values); a live lease whose fetched rows were dropped treats them as handled (§5).
System events like `$sys.data_loss` are ordinary bus messages under the reserved
`$sys.` topic prefix — anyone can subscribe; the dashboard should.

## 11. Operational notes

- **Postgres 14+** (D16): mature declarative partitioning, `ON CONFLICT` on
  partitioned tables, performance work we rely on. 13 is EOL.
- `cb_stream_worker` is UNLOGGED: after crash recovery **or failover to a replica**
  it comes back empty → all leases sweep to `released` → mass redelivery of
  in-flight work. Legal under at-least-once; document it loudly.
- Hot-path rules kept: no joins on reads (`ordinal > x` range scans), no advisory
  locks outside sequencer election, no N+1. The group row is the only contended
  row; batch sizes amortize it.
- Payloads stay `jsonb` (today's choice); revisit `bytea` only if a real workload
  demands opaque payloads.
- BRIN on `(created_at)` per partition for the janitor; btree `(stream, ordinal)`
  for everything else. Ordinal updates are non-HOT (ordinal is indexed) — accepted,
  see §2 costs.

## 12. Build checklist

1. DDL migration (`cb_stream_*`), goose table `cb_stream_migrations`.
2. `cb_stream_publish(stream, topic, payload, headers, key?, delay?, coalesce_key?)`
   — insert or pending or dedup-return; emits `pg_notify` (no listeners until M5).
   Sketched in §8. Plus the batch variant.
3. Sequencer function + kernel-ticker wiring (poll-only, D17) + advisory-lock
   election.
4. Ordered consume (both Go shapes), group ensure, start positions.
5. Work groups: lease claim / adopt / close, heartbeat goroutine, sweeper.
6. `cb_stream_fail` + policy columns; pending sweeper (delay, retry, coalesce, cron).
7. Dedup table + prune janitor; DLQ append + `Redrive`.
8. Partition pre-creation job (no DEFAULT partition); retention janitor with
   age-cap force-advance.
9. Tests — the ones that gate everything else:
   - **The torture test**: publisher holds a transaction open across N sequencer
     ticks while others publish; assert no loss, no reorder, cursor never passes an
     undelivered ordinal. Run under `-race` with dozens of concurrent publishers.
   - Contiguity: ordinals have no gaps after crash-kill of the sequencer mid-batch.
   - Dual-sequencer exclusion: two sequencing transactions opened concurrently by
     hand — the second must lose the xact-lock try and sequence nothing. (Kill-9
     of the leader does *not* reproduce this race; build it deliberately.)
   - Duplicate fail: `cb_stream_fail` twice for one (group, ordinal) → exactly one
     pending row.
   - Multi-group retry: group A's retry is invisible to group B, in ordered and
     work modes both.
   - Exactly-once: consumer transaction aborts after effects → redelivered → effects
     appear exactly once.
   - Lease crash: kill a worker mid-range; sweeper releases; another worker adopts;
     duplicates ≤ range size.
   - Retention: age cap force-advances an abandoned cursor and emits the loss event.
   - Coalesce: N rapid publishes with one key deliver once with the newest payload.
