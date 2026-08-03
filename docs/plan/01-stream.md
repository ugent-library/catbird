# 01 — stream: the feed layer

> Revised 2026-07-14 (branch experiment). Three changes ripple through this
> chapter (decision log D34–D38). **Queues are renamed subscriptions** — the
> word "queue" was naming two different needs, and work dispatch is the flow
> engine's job now (D37); the rename applies to tables, functions and
> arguments (`cb_stream_subscriptions`, `ensure_subscription`, …) — SQL
> sketches below may still say `queue` until transcription; the migration is
> the naming authority. **Failure state moved from streams to rows**: the
> `sr.*` retry streams and `sd.*` dead letter streams are deleted in favor of
> one `cb_stream_retries` table (D35, D36), which collapses the D22 name
> grammar to plain names and retires `max_crashes` (D38). **Flows no longer
> ride the log** (D34): nothing in this chapter serves the engine anymore,
> and every mechanism that remains has a named consumer in this document.
> Sections §5 (in part), §7 and §9 are rewritten; §2 (the assigner), §4
> (cursors), the claim machinery, filters (02), keys, pending, schedules and
> retention are untouched.

An ordered, partitioned, append-only log with two read modes — the layer that
answers *what happened*. Consumers are cursors (ordered, exactly-once same-DB)
and subscriptions (at-least-once, filtered, with retries). Work dispatch —
*what must still be done* — is the job engine's work row (03, D34, D39);
events become jobs through a trigger (D40), or any consumer that calls
`cb_job_run` in its handler transaction. Package `streams`; tables
`cb_stream_*`.

## 1. The model in five sentences

Producers `INSERT` messages inside their own transaction — that is the whole write
path. An **assigner** (a tiny ticker job) assigns contiguous, commit-ordered
**positions** to newly visible rows. Nothing downstream ever reads an unassigned row.
**Cursors** read after position N, process, advance. A cursor may do that inside
one transaction with its own effects, which gives exactly-once processing.
**Subscriptions** consume by **range claim**: claim `[from, to]` with a single
counter bump, process each message, fail the exceptions by position, close the
claim.
Retention drops whole partitions once every consumer has passed them, subject to an
age cap that wins over laggards.

## 2. Why an assigner (D1) — the correctness core

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
- *Assigner* — **chosen**. All correctness machinery is concentrated in one
  ~30-line function that runs in one place; every consumer everywhere reads
  `WHERE position > $cursor ORDER BY position` and is trivially, provably right.

**Mechanism.** Messages insert with `position NULL`. The assigner runs on the
kernel ticker, per stream with unassigned rows. Wakeups are staged (D17). Until M5
the assigner wakes on a plain fixed-interval poll (~50–250ms, configurable); that
alone is correct, and it is all M1–M4 need. M5 adds a NOTIFY wake with a small
debounce (~10–25ms), once the shared notifier lands with wire. Correctness never
depends on a notification arriving; the tick remains the safety net:

```sql
-- Only one process may number a stream at a time. The try-lock enforces
-- that: if someone else is already at it, return 0 and let them finish.
--
-- The lock must be the _xact variant, which releases automatically when
-- this transaction ends. The session variant would not: it stays attached
-- to the database connection after we return. Our connections live in a
-- pool and get reused by unrelated code, so the lock would leak — held
-- forever by a connection nobody remembers — and every later call here
-- would find the stream "busy" and assign nothing, permanently.
CREATE FUNCTION cb_stream_assign_positions(stream text, batch int DEFAULT 5000)
RETURNS int   -- rows assigned; 0 = caught up, or another node holds the lock
LANGUAGE plpgsql AS $$
DECLARE
    _n int;
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtext('cb_assign:' || stream)) THEN
        RETURN 0;
    END IF;

    WITH todo AS (
        SELECT m.id, row_number() OVER (ORDER BY m.id) AS rn
        FROM cb_stream_messages m
        WHERE m.stream = cb_stream_assign_positions.stream AND m.position IS NULL
        ORDER BY m.id
        LIMIT batch
    ), bump AS (
        UPDATE cb_streams s
        SET last_position = s.last_position + (SELECT count(*) FROM todo)
        WHERE s.name = cb_stream_assign_positions.stream
        RETURNING s.last_position - (SELECT count(*) FROM todo) AS base
    ), stamped AS (
        UPDATE cb_stream_messages m
        SET position = bump.base + todo.rn
        FROM todo, bump
        WHERE m.stream = cb_stream_assign_positions.stream
          AND m.id = todo.id
          AND m.position IS NULL
        -- the trailing "position IS NULL" is a seatbelt: if lock discipline is
        -- ever broken, it turns silent re-stamping into a detectable gap
        RETURNING 1
    )
    SELECT count(*) INTO _n FROM stamped;

    IF _n > 0 THEN
        PERFORM pg_notify(current_schema || '.cbs_' || stream, '');  -- consumer wake (M5)
    END IF;
    RETURN _n;
END;
$$;
```

Properties that fall out:

- **Positions are contiguous per stream** (a counter, not a sequence — no gaps,
  ever). "Am I caught up" and "is this range complete" become integer arithmetic.
- Only *visible* rows get positions, so position order = commit-batch order, ties
  broken by insert id. A row that never commits never exists downstream.
- The assigner is idempotent and crash-safe: it assigns or it doesn't; on restart
  it continues from `last_position`. If no node holds the advisory lock momentarily,
  assignment pauses — delivery pauses, nothing is lost.
- After assignment, the SQL fires `pg_notify` per touched stream. Emission is
  there from day one (one line, costs nothing without listeners); processes start
  *listening* at M5 (D17) — until then consumers wake on their own tick.

**The honest costs.** First: one extra `UPDATE` per message, so double the heap and
WAL traffic of insert-only. At hundreds of messages per second this is noise. It
would matter at Kafka scale, which is a stated non-goal. Second: a delivery latency
floor. Poll-only (M1–M4) it is one to two tick intervals end-to-end, ~100–500ms
depending on configuration. The NOTIFY accelerator (M5) brings it to
**~30–80ms end-to-end** for all consumers, queues included. This replaces the
vision's "sub-50ms" claim (README amendment 4); emails, indexers, notifications and
flow steps do not feel 50ms. Third: a latency spike when the node running the
assigner dies mid-tick. The next tick, on any node, picks up.

## 3. Table shape

All static — creating a stream creates a partition, never a table family.
The partition is the one piece of runtime DDL, and it sets the rule for
callers: ensure at boot, in its own transaction. The whole ensure
serializes on one advisory lock taken before it writes anything, and the
partition DDL briefly locks the whole message table — an ensure that runs
after reads or writes in the same transaction can therefore deadlock with
another process's ensure.

```
cb_streams          name PK · last_position · retention config (§10) · created_at
                   (no notify config: every append notifies a channel named
                   after the stream; listeners pick their channels, 02 §4)
cb_stream_messages  PARTITION BY LIST (stream), then RANGE (created_at) per stream
                   stream · id (bigint identity) · position (bigint, NULL until
                   assigned) · topic · payload jsonb · headers jsonb ·
                   created_at (clock_timestamp())
                   No consumer-targeting column: retries live in the retry
                   table (D35), so the log holds only what was published —
                   literally, now (D36).
                   indexes: (stream, position) btree per partition; nothing else hot
cb_stream_cursors   stream · name PK(stream,name) · position (everything ≤ it is
                   processed — the ack AND the retention floor) · filter columns
                   (§4) · failure_policy ('block'|'dead_letter' — a dead row in
                   cb_stream_retries, §9) · created_at
cb_stream_subscriptions
                   stream · name PK(stream,name) · claimed_position (highest
                   position handed to a claim — NOT an ack) · closed_position
                   (highest position below which all claims are closed — the
                   retention floor) · claim_ttl · claim_batch_size (the claim's
                   terms — how long, how many; subscription policy, no per-call
                   override, D23/D28) · filter columns (§4) · retry policy
                   columns (§7) · created_at
                   Two tables, not one with a mode column: every column means
                   exactly one thing, cross-mode misuse is structurally
                   impossible, and the janitor cannot read the wrong floor. One
                   consumer *name* per stream across both tables — no cross-table
                   PK exists, so ensure enforces it under the setup advisory lock.
cb_stream_claims    stream · subscription · from_position · to_position (immutable
                   after insert — claims are atoms, D28) · consumer ·
                   closed (closed rows linger only until the closed position
                   passes, §5) · released (a voluntary handback — uncharged,
                   D28/D38) · ttl (this claim's terms — what extend renews
                   and adoption inherits) · expires_at (past it, anyone may
                   adopt — D23) · created_at               (tiny, mostly empty)
cb_stream_retries   stream · consumer name · origin_pos PK(stream,consumer,
                   origin_pos) · topic · payload · headers (the envelope,
                   copied — retention-proof) · attempt (row-phase deliveries,
                   minted at solo claim — D38) · last_error (verdict text, or
                   'silence') · dead boolean · claimable_at (visibility, lease
                   and backoff in one column) · claim consumer · created_at
                   The whole unhappy path of §7 and §9: a message's failure
                   state from first verdict or quarantine to resolution
                   (deleted), give-up (dead = true, parked for triage and
                   redrive) or drop. Serves subscriptions (retry + dead) and
                   cursors (dead only, via failure_policy).
cb_stream_pending  id PK · stream · topic · payload · headers · deliver_at ·
                   key (nullable — lets delivery swap the dedup ref on
                   delivery, D19). Purely delayed messages — user delays and
                   embargoes only; retries stopped being publishes (D35)
cb_stream_schedules PK(stream, name) — the identity ensure updates · cron_spec ·
                   catch_up_policy · topic/payload/headers template · next_at
                   (config, not data: survives delivery and re-arms, §6)
cb_stream_keys    stream · key PK(stream,key) · ref_kind ('message'|'pending') ·
                   ref_id · created_at
```

Postgres quirk to plan for: a partitioned table's unique constraints must include
all partition key columns, so the message PK is `(stream, created_at, id)`;
addressing is by the `(stream, position)` index in practice. Rows move positions via
`UPDATE`, never partitions (partition key is `created_at`, which never changes).

Column discipline (settled while writing the first migration):

- **Types** (D20): enums for closed sets, text + CHECK for sets expected to grow.
  Enums validate at the function-parameter boundary — where the cross-language
  contract lives — so a foreign caller's typo dies at bind time.
- **Integrity, three tiers.** References *into the log* — cursor and closed
  positions, claim ranges — are plain numbers, not FKs. Retention drops their
  targets by design, and a partitioned position has no unique index to point at
  anyway. The *hot path* carries no FK either: an FK check is a hidden join on
  every insert, and publish's explicit PK read gives a better error. The *cold
  config tables* do get real integrity: `cursor`, `queue`, `pending` →
  `cb_streams(name) ON DELETE CASCADE`, claim → queue, plus the arithmetic
  CHECKs (`closed_position <= claimed_position`, `from_position <= to_position`,
  `jsonb_typeof(headers) = 'object'`).
- **Column strictness follows who reads the column.** `topic` is nullable:
  absence feeds filters, and SQL's three-valued logic does the right thing with
  NULL there. `payload` is NOT NULL DEFAULT 'null': absence feeds decoders, and
  JSON null is the one canonical "no data". `headers` is NOT NULL DEFAULT '{}'
  with an object CHECK: SQL itself reads keys and merges with `||`, and a scalar
  there would corrupt silently.
- **Naming authority is the migrations**, not these sketches — notably the
  column is `stream_name` in DDL (kills plpgsql parameter ambiguity) while
  sketches abbreviate to `stream`. Semantics here, identifiers there.
  Convention: table names are **plural** (`cb_stream_messages`,
  `cb_stream_keys`) except collectives (`cb_stream_pending`); function names
  stay singular verbs (`cb_stream_publish`). Bonus: the `cb_job_runs` table
  no longer collides with the `cb_job_run()` function.
- **Name validation, one tier** (D36 collapsed the grammar): every stream is
  user-made and single-segment — `cb_valid_name` (`^[a-z][a-z0-9_]*$`, ≤ 20
  bytes; relaxable to ~40 now that nothing composes names, if anyone asks).
  `cb_valid_name` and `cb_forever` move to the kernel's shared SQL unit at
  M4a, names unchanged (D41).
  The D22 code grammar (`fe` `fq` `fr` `fd` `sr` `sd`, arity rules, the
  44-byte budget, `__` partition encoding and its injectivity argument, the
  `split_part` base derivations in the failure paths) is retired whole: the
  flow families never existed (D34), `sr.*` became the retry table (D35) and
  `sd.*` became dead rows (D36). Partition names are plain `cbm_<name>`.
  Consumer names never contain dots: shards are `proj_0`, an app cursor is
  `indexer`. The retired codes stay documented so nothing reuses them with
  new meanings while old databases exist.
- **Function visibility** (PostGIS's convention): `cb_*` is public — listed in
  `docs/sql-api.md`, stable, breaking changes are versioned events. `_cb_*` is
  internal — no stability promise, may change in any migration. Public by
  necessity: the foreign-worker contract (claim / extend / complete / fail /
  signal), `cb_stream_publish`, `cb_stream_fail`, `cb_valid_name` (users
  pre-validate against it). Internal as they land: `_cb_valid_stream_name`,
  the notify-append tail, backoff, cron-next, delivery, janitors — and
  decide the assigner's marker when building it (only catbird's own ticker
  and consume paths call it, so it leans internal).

## 4. Cursors (cursor mode)

One logical reader per cursor: order *requires* that. Parallelism comes from more
cursors, or from sharding by key the way the flow projection does (03 §4). The Go
API offers two shapes:

```go
// exactly-once: effects + ack in one transaction (the same-DB superpower)
stream.Consume(ctx, pool, "orders", "indexer", func(ctx context.Context, tx pgx.Tx, batch []stream.Message) error { … })

// at-least-once: for handlers with external effects
stream.ConsumeFunc(ctx, pool, "orders", "mailer", func(ctx context.Context, batch []stream.Message) error { … })
```

One iteration works like this. `SELECT … FROM cb_stream_cursors … FOR UPDATE` pins
the cursor row, so a second competing reader blocks instead of double-processing.
Read `position > cursor ORDER BY position LIMIT n`. No upper bound is needed: the
assigner stamps rows and bumps the counter atomically, so every visible position
belongs to a complete batch. Run the handler. `UPDATE … SET position = $high`,
commit. On failure the cursor's config decides. `block` (the default) retries the
batch with backoff, in place — ordered means ordered. `dead_letter` writes the
poison message as a dead row in `cb_stream_retries` (§9) and advances past it.

The exactly-once loop is small enough to show whole:

```go
func Consume(ctx context.Context, pool *pgxpool.Pool, stream, cursor string,
	handler func(context.Context, pgx.Tx, []Message) error,
) error {
	for {
		waitForWakeup(ctx, stream, cursor) // tick (D17); + stream notify from M5
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			var pos int64
			// pin the cursor row — a second competing reader blocks here
			// rather than double-processing
			if err := tx.QueryRow(ctx,
				`SELECT position FROM cb_stream_cursors
				 WHERE stream = $1 AND name = $2 FOR UPDATE`,
				stream, cursor).Scan(&pos); err != nil {
				return err
			}
			msgs, err := fetch(ctx, tx, stream, cursor, pos, batchSize)
			// fetch: position > pos — no upper bound; assignment is atomic
			//        [+ the cursor's precompiled topic/condition filter]
			//        ORDER BY position
			if err != nil || len(msgs) == 0 {
				return err
			}
			if err := handler(ctx, tx, msgs); err != nil {
				return err // rollback: effects AND cursor, together
			}
			_, err = tx.Exec(ctx,
				`UPDATE cb_stream_cursors SET position = $3
				 WHERE stream = $1 AND name = $2`,
				stream, cursor, msgs[len(msgs)-1].Ordinal)
			return err
			// commit = effects + ack atomically: exactly-once processing
		})
		if err != nil {
			applyGroupFailurePolicy(err) // block w/ backoff, or dead_letter + advance
		}
	}
}
```

**The filtering contract (owned by 02 as shipped; D29).** A cursor or queue may
carry a filter, fixed at registration — birth policy, because competing
consumers of one queue must agree: a topic pattern (`*` one segment, `#`
trailing zero-or-more) compiled to a regex at ensure, and a condition over
headers and payload compiled into per-column jsonpath (02 §2). Both evaluate
**in SQL** from the precompiled columns — one matcher implementation, identical
for every client. The Go trie survives app-side only, for in-process
dispatchers matching one event against many subscriber rows. Either way the
cursor advances over skipped rows, and claims tile positions regardless of
matches. There is no consumer-targeting predicate at all: retries live in
per-subscription rows (D35), so ownership is a column, not a filter.

Cursors have all three verbs. `cb_stream_ensure_cursor` births: initial
filter and start, never modifying an existing cursor, so a booting fleet is
safe and its `start_pos` means "if new". `cb_stream_define_cursor` declares
the whole config: the filter recompiles when its source text changes, the
position stays put, and `start_pos` — when given — repositions
deliberately, on every call. `cb_stream_delete_cursor` removes one. Define
and delete arrived with the job module's trigger (03 §8), the
define-when-first-needed moment D26 recorded: the trigger owns the cursor
named after it and keeps its filter true through these two functions. Both
verbs stay because their start semantics genuinely differ — a birth seed
versus a deliberate poke.

Freshness is also a per-consumer policy, not a message property. A handler that
doesn't want old messages skips anything whose `created_at` is older than its own
tolerance — one line. Different consumers can differ: the same message may be
worthless to the mailer after five minutes and still wanted by an audit consumer.
This replaces today's per-message `expires_at`. If claim-time skipping ever
proves worth it, it becomes a queue policy column beside the retry columns.
Later, if asked.

## 5. Subscriptions (range claims) — the pgmq/pgq re-evaluation (D2)

Your note asked whether the vision's pgmq/pgq hybrid is the optimum. It isn't quite:
once positions are contiguous (D1), per-message SKIP-LOCKED claiming — pgmq's core —
becomes unnecessary machinery. Claiming a batch is a **single counter bump**:

```sql
CREATE FUNCTION cb_stream_claim(
    stream text, queue text, consumer text,
    ttl interval DEFAULT NULL, -- this call's terms; NULL = the queue's claim_ttl
    OUT from_position bigint, -- NULL when there is nothing to claim
    OUT to_position   bigint,
    OUT expires_at    timestamptz -- the deadline: finish or extend before it
)
LANGUAGE plpgsql AS $$
DECLARE
    _ttl interval;
    _batch int;
    _claimed bigint;
    _last bigint;
BEGIN
    -- batch size is queue policy (D28), not a caller choice
    SELECT coalesce(cb_stream_claim.ttl, q.claim_ttl), q.claim_batch_size
    INTO _ttl, _batch FROM cb_stream_queues q
    WHERE q.stream_name = cb_stream_claim.stream AND q.name = cb_stream_claim.queue;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: queue %.% not defined',
            cb_stream_claim.stream, cb_stream_claim.queue;
    END IF;

    -- 1. adopt an expired claim first: work a dead consumer left behind.
    --    Cold path, tiny table. The terms describe the workload, not the
    --    owner, so adoption inherits the row's ttl unless this call overrides.
    --    The sketch shows the plain re-hand; the full branch also clears the
    --    released flag, bumps crashes only on true expiry, and quarantines
    --    solo or over-the-limit claims (D28, below).
    UPDATE cb_stream_claims c
    SET consumer = cb_stream_claim.consumer,
        ttl = coalesce(cb_stream_claim.ttl, c.ttl),
        expires_at = clock_timestamp() + coalesce(cb_stream_claim.ttl, c.ttl)
    WHERE (c.stream_name, c.queue_name, c.from_position) = (
        SELECT r.stream_name, r.queue_name, r.from_position
        FROM cb_stream_claims r
        WHERE r.stream_name = cb_stream_claim.stream
          AND r.queue_name  = cb_stream_claim.queue
          AND NOT r.closed
          AND r.expires_at <= clock_timestamp()
        ORDER BY r.from_position
        LIMIT 1
        FOR UPDATE SKIP LOCKED)
    RETURNING c.from_position, c.to_position, c.expires_at
    INTO from_position, to_position, expires_at;
    IF FOUND THEN
        RETURN;
    END IF;

    -- 2. hot path: one row lock, no scan — bump the counter to the assigned high
    SELECT q.claimed_position INTO _claimed FROM cb_stream_queues q
    WHERE q.stream_name = cb_stream_claim.stream AND q.name = cb_stream_claim.queue
    FOR UPDATE;

    SELECT s.last_position INTO _last FROM cb_streams s
    WHERE s.name = cb_stream_claim.stream;

    IF _claimed >= _last THEN
        RETURN; -- caught up; from_position stays NULL
    END IF;

    from_position := _claimed + 1;
    to_position := least(_claimed + _batch, _last);
    expires_at := clock_timestamp() + _ttl;

    UPDATE cb_stream_queues q SET claimed_position = cb_stream_claim.to_position
    WHERE q.stream_name = cb_stream_claim.stream AND q.name = cb_stream_claim.queue;

    INSERT INTO cb_stream_claims
        (stream_name, queue_name, from_position, to_position, consumer, ttl, expires_at)
    VALUES (cb_stream_claim.stream, cb_stream_claim.queue,
        cb_stream_claim.from_position, cb_stream_claim.to_position,
        cb_stream_claim.consumer, _ttl, cb_stream_claim.expires_at);
END;
$$;

-- Push the claim's deadline out: expires_at becomes now + the claim's stored
-- ttl. The consume loop calls it on a cadence while work is in progress —
-- between messages, or while a handler is still executing (D27). Extension
-- tracks the work: a loop wedged anywhere stops calling, and the claim
-- expires on its own.
-- The ttl argument applies to this renewal only and must be positive —
-- extending always moves the deadline forward. To give a claim back, use
-- cb_stream_release_claim.
-- Returns the new deadline. Returns NULL when the claim is not yours anymore:
-- it expired and another consumer adopted it. Stop processing the range.
CREATE FUNCTION cb_stream_extend_claim(stream text, queue text, consumer text, from_position bigint,
                                       ttl interval DEFAULT NULL)
RETURNS timestamptz LANGUAGE plpgsql AS $$
DECLARE
    _expires_at timestamptz;
BEGIN
    IF ttl <= '0' THEN
        RAISE EXCEPTION 'catbird: invalid ttl %', ttl;
    END IF;

    UPDATE cb_stream_claims c
    SET expires_at = clock_timestamp() + coalesce(cb_stream_extend_claim.ttl, c.ttl)
    WHERE c.stream_name   = cb_stream_extend_claim.stream
      AND c.queue_name    = cb_stream_extend_claim.queue
      AND c.consumer      = cb_stream_extend_claim.consumer
      AND c.from_position = cb_stream_extend_claim.from_position
      AND NOT c.closed
    RETURNING c.expires_at INTO _expires_at;
    RETURN _expires_at;
END; $$;

-- Hand the whole claim back: it expires immediately and is marked released,
-- so the next cb_stream_claim call may adopt it — without counting a crash
-- (D28): a handback says nothing about any message. Use it when you processed
-- nothing and cannot continue. No-op when the claim is no longer yours.
CREATE FUNCTION cb_stream_release_claim(stream text, queue text, consumer text, from_position bigint)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cb_stream_claims c
    SET expires_at = clock_timestamp(),
        released   = true
    WHERE c.stream_name   = cb_stream_release_claim.stream
      AND c.queue_name    = cb_stream_release_claim.queue
      AND c.consumer      = cb_stream_release_claim.consumer
      AND c.from_position = cb_stream_release_claim.from_position
      AND NOT c.closed;
END; $$;

CREATE FUNCTION cb_stream_close_claim(stream text, queue text, consumer text, from_position bigint)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _closed bigint;
    _to bigint;
BEGIN
    -- Only the current owner may close: a consumer that lost its claim to
    -- adoption gets a no-op, not an error — losing a claim is a legal race
    -- outcome under at-least-once.
    UPDATE cb_stream_claims c SET closed = true
    WHERE c.stream_name   = cb_stream_close_claim.stream
      AND c.queue_name    = cb_stream_close_claim.queue
      AND c.consumer      = cb_stream_close_claim.consumer
      AND c.from_position = cb_stream_close_claim.from_position
      AND NOT c.closed;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Move the closed position forward over adjacent closed claims until an
    -- open claim stops it. Delete every claim it passes: the table only
    -- holds work in progress.
    SELECT q.closed_position INTO _closed FROM cb_stream_queues q
    WHERE q.stream_name = cb_stream_close_claim.stream AND q.name = cb_stream_close_claim.queue
    FOR UPDATE;
    LOOP
        DELETE FROM cb_stream_claims c
        WHERE c.stream_name   = cb_stream_close_claim.stream
          AND c.queue_name    = cb_stream_close_claim.queue
          AND c.from_position = _closed + 1
          AND c.closed
        RETURNING c.to_position INTO _to;
        EXIT WHEN NOT FOUND;
        _closed := _to;
    END LOOP;
    UPDATE cb_stream_queues q SET closed_position = _closed
    WHERE q.stream_name = cb_stream_close_claim.stream AND q.name = cb_stream_close_claim.queue
      AND q.closed_position < _closed;
END; $$;
```

No scan, no anti-join, no lock queue — one hot row per subscription. Before
bumping the counter, a consumer first adopts any expired claim (`FOR UPDATE SKIP
LOCKED` on the tiny claim table — cold path, fine): a released claim is
re-handed whole, an expired one is **quarantined** into retry rows (D28 as
amended by D35/D38, below). Due retry rows are served by the same claim call as
solo pseudo-claims, before ranges (§7). Then per message in the range: run the
handler; on failure, `cb_stream_fail` records a retry row or gives up per
policy (§7, D35).
A claim closes when every message *fetched for it* is handled (succeeded, retried,
or dead-lettered) — defined over what was fetched, not the position range, so a
partition dropped mid-claim (§10) cannot wedge it. On close, mark it closed
and advance the queue's `closed_position` over contiguous closed claims, deleting
them as it passes — the claim table only ever holds the working set. The closed
position is the queue's retention floor. An adopted claim may re-handle messages
the dead consumer already resolved; that is why `cb_stream_fail` is idempotent (§7).

Liveness is per claim, not per process (D23 — KIP-932's acquisition lock): every
claim carries its terms (`ttl`) and its deadline (`expires_at`). The terms
resolve in one direction: the queue's `claim_ttl` is the default, a `ttl`
argument at claim time overrides it, and the row stores the result — extend
renews under the stored terms, adoption inherits them. (SQS's exact scheme:
queue-level default, per-receive override, per-message change.) Finish or extend
before the deadline, or the claim becomes available and the next
`cb_stream_claim` call adopts it. There are no heartbeats, no consumer registry,
and no sweeper. Failure detection happens at the moment another consumer asks
for work — the same way everything else here is decided by a lock at the point
of contention, not by a background monitor. It also catches what heartbeats
cannot. A process that is alive but stuck on one message keeps heartbeating, so
its claim never comes back. A claim that stops being extended always does. One
rule keeps that true: extension is **earned** — the loop extends only while it
is between messages or a handler is actually executing, never as a bare
keep-alive (D27, detailed below). A consumer that must stop early hands work
back instead: `release` returns the whole claim, marked released — a handback
is uncharged (D28, D38). A crashed consumer's range is quarantined into retry
rows — each unresolved message becomes its own solo-claimable row, uncharged
until it is actually delivered again, and a message that keeps failing or
crashing alone runs out its one budget and parks as a dead row (D38, detailed
below).

Two invariants hold across every branch (`checkClaims` in the Go test suite
checks them after each step): open and closed claims exactly tile `(closed_position, claimed_position]` —
no gaps, no overlaps — and both positions only ever grow. Claim boundaries
never change after insert (D28), so the tiling holds structurally; a stalled
closed position would mean messages lost to retention, which is why the checker
stays in the suite anyway.

What survives from each lineage. From **pgq**: positions, batches, the ticker,
retention by rotation. From **pgmq**: nothing on the hot path. Its visibility
timeout returns as the claim TTL, per range instead of per message — and at
`claim_batch_size` 1 the range *is* a message, so the emulation is exact for
the happy path: `read`/`set_vt`/`delete` map to claim/extend/close, and
`release` is "make it available now". One deliberate difference: a crashed
message returns through quarantine with backoff (D28), not immediately. SKIP
LOCKED survives only on the cold claim-adoption path.
Today's queue semantics (`Send`/`Read` with hide) are *not* re-exposed;
subscriptions replace the feed-consumption half, and dispatched work is a
one-step run (D37, 03 §5).

**Filters and claims (D29).** A subscription's topic pattern and condition
never touch the claim machinery: claims are counter bumps over positions,
tiling everything whether it matches or not, and `claim_batch_size` counts
positions, not matches. The filter applies in exactly two places — the
claimed-range fetch (`cb_stream_read_claim`, so no client needs to know the
filter exists) and quarantine (so non-matching messages never become retry
rows). Retry rows are pre-filtered by construction — they hold only their
subscription's own failures, and rows have no filter to misapply. A sparse
filter just means near-empty claims that close fast.

### The consume loop owns the clock (D27, adopted 2026-07-09)

Sizing `claim_ttl` against the slowest handler is a known source of duplicate
execution — pgmq and the old catbird both taught this. The contract is global
(one number in queue config) but the violation is local: a handler grows a
network call months later, and the person making that change never sees the
number it just broke. The failure is probabilistic — only the slow tail, only
under pressure — and surfaces as a duplicate side effect far from the code
that caused it. Knowing the visibility-timeout semantics does not prevent it;
the bug is two pieces of truth kept in two places with nothing enforcing their
relationship. Removing this burden from the programmer is a goal.

The mechanism: **the loop extends the claim, and only ever for work that is
actually in progress.** On a `ttl/2` cadence the consume loop extends
while it is between messages or while a handler is still executing; the
moment neither holds, extending stops — and a claim that stops being extended
always expires. D23's objection does not apply: the timer it forbids tracks
process-aliveness ("not dead yet"); this one tracks the work, and its outer
bound is the process's own mortality. A wedged handler is not extended past
its process: the next deploy, health-check restart, or operator kill turns
the wedge into real silence, and the crash accounting picks up truthfully
from there. The loop logs a handler that is still running at each extend, so
a wedge is never silent — only slow. The Go shape keeps the rule honest: one
select loop owns handler-done, the extend tick and cancellation, so extends
only fire while the loop itself is responsive — a loop wedged in *any* call
(a fetch, a fail, a close) stops extending by construction, not by policy.

A per-message time budget (a `HandlerTimeout` option: stop extending, cancel
the handler's context, fail the message with a truthful timeout error) is the
known opt-in accelerator — it converts a wedge into a verdict at handler
granularity instead of waiting for the process to die. It is deliberately not
in the MVP and must never get a finite default: an admin-started export that
takes hours is correct code, and a default budget would kill it, re-run the
hours from scratch on every retry, then archive it — a default has to protect
the innocent before it polices the guilty. When an extend returns NULL
mid-message — the claim was lost anyway, a pause longer than the ttl — the
loop cancels the handler at once instead of noticing between messages,
shrinking the double-execution window in the residual case. Handler panics
are recovered and reported as ordinary failures: a panic is a verdict, not a
crash.

Consequences: `claim_ttl` demotes to failure-detection latency, a short default
nobody sizes against handlers; claim expiry then means a real death, so the
crash accounting becomes truthful. Nothing changes in SQL — extend, the
consumer fence (close, extend and release act only for the claim's current
consumer; anyone else is a silent no-op), and keyed retries already carry the
mechanism, which is the argument that this is loop policy, not substrate;
thin foreign-language clients keep the manual extend contract. D23's "never
from a background timer" is amended accordingly: extension must track the
work — progress between messages, execution during one — with the process's
own mortality as the outer bound and an optional budget as a tighter one. The
zombie-fail gap this leaves (`cb_stream_fail` unfenced) is closed by D28
below, which adds the `consumer` argument and the ownership fence.

### Claims as atoms — quarantine replaces the crash ladder (D28, adopted 2026-07-09; bookkeeping amended by D35/D38, 2026-07-14)

The range-claim machinery is really two things. Allotment — two counters, FIFO
hand-out, leases with a deadline, close whole, adopt after expiry — is simple
and sound. Repair was the brittle half. The first design (the **crash
ladder**) narrowed a crashing range by splitting it: the adopter got the head
message alone, the tail respawned as an already-expired claim carrying the
count, and a lone message crashing past `max_crashes` was dead-lettered on the
strength of a solo-trial proof. It existed because claims are coarse — when a
range goes bad nobody knows which message is guilty — but it made the tiling
invariant a proof obligation on every present and future branch, with a
silently stalled closed position (then retention eating unprocessed messages)
as the failure mode. And the count it escalated on was polluted: `release`
expired the claim in place, so adoption could not tell a graceful handback
from a death. A rolling deploy — or a worker crash-looping for reasons
unrelated to any message — handed a slow range back repeatedly, every handback
counted as a crash, and an innocent, possibly never-attempted message was
archived as "crash limit reached". A clean lie, and the operator reading the
dead letter stream could not know the proof did not hold.

The mechanism, as amended (D35, D38): **the per-message shape is a row, not a
republished copy.** Claims stay atoms — created whole, owned whole, closed
whole; `from_pos` and `to_pos` never change after insert, so the tiling proof
stays structural. Adoption has exactly two branches and no counter: a
**released** claim is re-handed whole (a handback is uncharged — it says
nothing about any message); an **expired** claim is quarantined — every
unresolved matching message in the range becomes a row in
`cb_stream_retries`, envelope copied, `attempt = 0`, claimable immediately —
and the claim closes. `ON CONFLICT DO NOTHING` on the row's primary key *is*
the old "a message the dead consumer already failed keeps its retry copy"
rule, as a constraint instead of a key-table probe. Quarantine costs row
upserts instead of publishes, which is why it no longer needs a threshold:
the claim-level crash count, `max_crashes`, and the granularity-threshold
doctrine are all retired.

The bookkeeping follows one law, shared with the flow engine (D38): **no
evidence, no charge; solo evidence, one charge; one budget.** The range phase
charges nothing — a range crash proves nothing about any particular message,
so bystanders quarantine at `attempt = 0` and pay nothing. Evidence exists in
exactly two forms, and each mints one count on the row: a **verdict**
(`cb_stream_fail` — the handler returned an error, the loop recovered its
panic, or the future opt-in budget timed it out) creates or advances the row;
a **solo delivery** (the row handed out by claim) is counted as it is handed
out — a retry row is claimed alone, so every charge is attributable to its
message and nothing else. One comparison — `attempt ≥ max_attempts` — decides
terminal at both places it can be discovered: at fail-time for verdicts, at
hand-out-time for silent killers (a row whose budget is spent is marked
`dead` instead of being handed out, or deleted when `on_fail` is `drop`).
Silence and verdicts stopped being separate arithmetic; they survive as
*words in the record* — `last_error` holds the error text or `'silence'` — so
the operator still reads what happened, and nothing masquerades. A lapsed
row lease is repaired at the next claim call — rescheduled to `backoff
(attempt)`, consumer cleared — failure detection at the point of contention,
like everything else here.

What this deletes outright: the `sr.*` stream family (names, eager twin
birth, ballast policy rows born never-consulted, both CHECK pins, the
assigner numbering positions nobody ordered by), the escalation key grammar
(`queue:origin:a<n>` / `c<n>` — row identity does this natively), the
`cb_attempt`/`cb_crash`/`cb_origin_pos` header vocabulary (columns now), the
pending-table hop inside every backoff, and the covering-claim ownership
fence on `cb_stream_fail` — the row's own lease is the fence (§7). The
outage-runway property survives with one budget: a fleet that dies
continuously gives up on a cycling message after `max_attempts` solo rounds,
each backoff-separated — minutes at the defaults, a policy number operators
should know, not a surprise. Solo drain is deliberately slower than ranges
and that is fine — backoff rate-limits the unhappy path by design; if
incident-scale drains ever hurt, batching row claims is an additive policy
knob recorded here as the escape hatch, spent only with evidence, because it
trades away perfect attribution.

**Batch size stays subscription policy** (`claim_batch_size` next to
`claim_ttl`, born at ensure, tuned by raw `UPDATE`), with no per-call
override — batch is a property of the workload, not of a call. It governs
range claims only; retry rows are solo by construction, no pin required. The
Go `SubscriptionOpts` carries workload policy — start position,
`claim_batch_size`, attempts, backoff, give-up policy; `claim_ttl` is engine
mechanics with a row default (exposing it in Go would invite the exact
size-it-against-your-handler mistake D27 removed). Deleting subscriptions
and streams stays raw ops: declared objects have no deathplace in the MVP —
and with no engine-made offspring, a future delete API covers exactly what
the user declared, nothing more.

## 6. Waiting messages and schedules — one delivery job, two tables (D3)

`cb_stream_pending` holds one-shot messages that have not entered the log yet.
Their life is: born, wait until `deliver_at`, delivered, gone.

| Feature | Row shape |
|---|---|
| Delayed delivery | `deliver_at` in the future |
| Retry with backoff | **Not here anymore** (D35): a retry is a `cb_stream_retries` row whose `claimable_at` is the backoff deadline (§7) — no publish, no delivery job, and only the owning subscription ever sees it, by construction. Pending holds user delays only |

Cron does **not** share this table. A schedule is *config*, not data. It has an
identity, `PK (stream, name)`, and that identity is what ensure updates when a
deploy changes a spec. It survives every delivery and re-arms instead of being
deleted. A cron row in pending would have no key to find it by; today's
`cb_task_schedules` got this right. So `cb_stream_schedules`: PK(stream, name)
· `cron_spec` · `catch_up_policy` (`skip`\|`all`, ported from today's scheduler
including the on-time-tick fix from #45) · a topic/payload/headers template ·
`next_at`.

One delivery job serves both — its tick makes two small scans, one job. Delivering a
pending row is delete + append in one transaction (exactly-once handoff);
delivering a schedule is append + re-arm `next_at`. The current `scheduler.go`
module dissolves into the schedule table plus builder sugar (`stream.Cron(...)`,
`flow.RunEvery(...)`).

There is deliberately no cancel API. Messages can be delivered twice or arrive
after the world changed (at-least-once), so every handler must check current
app state before acting — it has no choice. That check also covers everything
a cancel could do, plus the window after the message entered the log, which no
cancel can reach. To undo something, update your own data; the message still
delivers, sees the data, and does nothing. A wasted delivery costs
microseconds. A stuck row (say, a poisoned retry) is removed by hand:
`DELETE FROM cb_stream_pending WHERE id = …` — it's a plain table.

```sql
-- kernel-ticker job on every node — no leadership; SKIP LOCKED below divides
-- the work. Wakes on min(deliver_at) and on the '.cb_tick' notify that
-- publish fires for new earlier rows
CREATE FUNCTION cb_stream_deliver_pending(batch int DEFAULT 500)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _p cb_stream_pending; _mid bigint; _n int := 0;
BEGIN
    FOR _p IN
        SELECT * FROM cb_stream_pending
        WHERE deliver_at <= clock_timestamp()
        ORDER BY deliver_at LIMIT batch
        FOR UPDATE SKIP LOCKED
        -- Any number of copies may run this at once — each locks its own set
        -- of due rows and skips rows another copy is delivering. No
        -- coordination needed beyond the locks.
    LOOP
        DELETE FROM cb_stream_pending WHERE id = _p.id;

        -- A delayed publish stored the message here instead of in the log.
        -- Now that it's due, do what publish would have done at the time:
        -- insert the message, point its key at the new row (until now the
        -- key pointed at this pending row), and ring the stream's channel.
        INSERT INTO cb_stream_messages (stream, topic, payload, headers)
        VALUES (_p.stream, _p.topic, _p.payload, _p.headers)
        RETURNING id INTO _mid;
        UPDATE cb_stream_keys d SET ref_kind = 'message', ref_id = _mid
        WHERE d.ref_kind = 'pending' AND d.ref_id = _p.id AND d.stream = _p.stream;
        PERFORM cb_stream_notify_append(_p.stream, _p.topic);  -- the stream's channel

        _n := _n + 1;
    END LOOP;
    RETURN _n;
END;
$$;
```

(`cb_stream_notify_append` is the tiny shared tail of `cb_stream_publish` — one
place fires the stream's append notify, so the two append paths cannot drift.)

The same tick's second scan — due schedules append their template and re-arm,
nothing is deleted:

```sql
FOR _s IN
    SELECT * FROM cb_stream_schedules
    WHERE next_at <= clock_timestamp()
    FOR UPDATE SKIP LOCKED
LOOP
    INSERT INTO cb_stream_messages (stream_name, topic, payload, headers)
    VALUES (_s.stream_name, _s.topic, _s.payload, _s.headers);
    PERFORM cb_stream_notify_append(_s.stream_name, _s.topic);
    UPDATE cb_stream_schedules
    SET next_at = cb_cron_next(_s.cron_spec, _s.next_at, _s.catch_up_policy)
    WHERE stream_name = _s.stream_name AND name = _s.name;
END LOOP;
```

## 7. Retry policy lives in the database (D4)

Your note: robustness machinery is currently Go-side (`handler_opts.go`) and
therefore invisible to non-Go workers and to SQL. The *policy* is columns on
`cb_stream_subscriptions`:

```
max_attempts int · backoff_kind (enum: 'none'|'fixed'|'full_jitter', D20) ·
backoff_base interval · backoff_max interval ·
on_fail (enum: 'dead_letter'|'drop' — the subscription's single give-up
policy: park the exhausted row as dead for triage and redrive (§9), or
delete it. max_crashes is retired, D38 — one budget)
```

The *mechanism* is the retry row (D35, D38 — the law and its arithmetic live
in §5's amended quarantine section; this is the function contract).
`cb_stream_fail(stream, subscription, consumer, position, error)`:

- **No row exists** — the first verdict for this message: insert one,
  `attempt = 1`, envelope copied from the log (retention-proof),
  `claimable_at = now() + backoff(1)`, `last_error` recorded. The verdict is
  evidence of one delivery, so it counts.
- **The row exists and the caller holds its lease** — a verdict for the
  current solo delivery (the claim already minted this attempt): record the
  error, park the row at `backoff(attempt)`, clear the lease.
- **Anything else** — a superseded report (a zombie whose message was
  quarantined and re-delivered since): silent no-op. **The lease is the
  fence** — no covering-claim probe, no `consumer` fence machinery; the fence
  is a column the row already has, and duplicate fails collapse on the
  primary key instead of a dedup-key grammar.
- At `attempt ≥ max_attempts`: `dead = true` (or delete, on `drop`) — the
  same comparison the claim path applies before handing a due row out, so
  verdicts and silent killers exhaust through one number.

Due retry rows are delivered by `cb_stream_claim` itself, before ranges: a
due row is handed out as a **solo pseudo-claim** — `from_pos = to_pos =
origin_pos`, its `attempt` minted at hand-out, its lease on the row.
`read_claim` returns the row's copied envelope; `extend` renews the row's
lease; `close` resolves what the caller still holds — a row that was failed
(parked) or given up (dead) survives it, a row that succeeded is deleted;
`release` clears the lease. **The worker contract does not change shape**:
claim / read / extend / release / close / fail, one stream, no second claim
loop — the old design's retry-stream leg (`streams := [stream, sr.…]`)
disappears from the consume loop. Disambiguation is by key: claims are looked
up by `from_pos` first, retry rows second; ranges and rows never collide in
time because closed territory is never re-claimed.

(`backoff()` is a pure ten-line function of the policy columns and the
count — fixed or full-jitter, ported from `backoff.go`.) A Python worker
gets identical behavior to the Go worker — the "engine logic in SQL"
principle applied to robustness. Go builders write these columns at
ensure-time; `WithFullJitterBackoff(...)` keeps its API but becomes config,
not behavior. Client-side machinery that protects the *process* (circuit
breaker, panic recovery) stays in Go. One honest wrinkle of charge-at-claim:
a worker that claims a retry row and shuts down before running it keeps the
charge — bounded at one per shutdown, the price of counting deliveries where
no start call exists.

## 8. Keys (D5, D19)

One `key` parameter, no policy option. Two cases:

1. **The key is unknown** → claim it and store the message: into the log now,
   or into pending when its delivery time is in the future.
2. **The key is known** → the publish is skipped and the existing ref is
   returned — today's `concurrency_key` semantics, the atomic `WHERE FALSE` +
   `UNION ALL` pattern verbatim. `ref_kind` tells you what you hit: a delivered
   message (dedup holds until the row is pruned — the per-stream window is the
   idempotency guarantee) or a still-waiting one.

There is no replacement and no cancel — a log doesn't change, and the skip *is*
the dedup contract succeeding: first publish wins, reported explicitly
(`existing = true`, plus the ref). What makes both unnecessary is one pattern:
**payloads carry identifiers, not snapshots.** The handler reads current state
at delivery, so content can't go stale no matter how long a message is held.
And anything no longer wanted — a withdrawn record, an undone send — is skipped
by the same handler check that at-least-once delivery forces you to write
anyway. A message is a poke to look at the data, not the data itself.

Rate caps do **not** live here: throttling an expensive consumer is the
consumer's own cadence (wake interval + the cursor as a free dirty-flag), and
burst collapse is batch-dedupe in the handler — both zero-latency when the
system is idle, which a publish-side window can never be. Patterns, not
primitives.

**Batch publish (D29).** `cb_stream_publish_messages(stream, messages jsonb)`
is the batch form: one jsonb array of `{payload, topic?, headers?, key?,
delay?, deliver_at?}` envelopes, one result row per element in input order —
exactly N × `cb_stream_publish` in one call and one transaction. The parameter
is a single jsonb array, not `jsonb[]`: one JSON text any client language
produces natively, no PG-array-literal escaping. The Go API is `Publish` and
`PublishMessages`, nothing else; the payloads-only batch function is retired.
The shipped implementation loops over the single-publish path; the set-based
fast path is a deferred optimization with a written design (05).

**`cb_stream_publish`, sketched** — every write path in one function: immediate,
waiting, keyed.

```sql
CREATE FUNCTION cb_stream_publish(
    stream     text,
    topic      text,
    payload    jsonb,
    headers    jsonb       DEFAULT '{}',
    key        text        DEFAULT NULL,   -- keep-oldest dedup (the rule above)
    delay      interval    DEFAULT NULL,   -- relative, computed on the DB clock
    deliver_at timestamptz DEFAULT NULL,   -- absolute; exclusive with delay
    -- Exactly one row comes back, so OUT params, not RETURNS TABLE: RETURN
    -- means return (no RETURN QUERY emit-vs-exit traps), and assignments
    -- coerce (no enum casts needed).
    OUT ref_kind cb_ref_kind,   -- what was stored — or what the key hit
    OUT ref_id   bigint,
    OUT existing boolean        -- true: key already taken, nothing stored
) LANGUAGE plpgsql AS $$
#variable_conflict use_column
-- ^ the `key` parameter collides with cb_stream_keys.key inside the
-- ON CONFLICT target, which cannot be table-qualified (found empirically —
-- the target is expression context). Params are referenced qualified, so
-- resolving ambiguity to columns is safe.
DECLARE
    _id bigint; _at timestamptz; _future boolean;
BEGIN
    existing := false;

    IF delay IS NOT NULL AND deliver_at IS NOT NULL THEN
        RAISE EXCEPTION 'catbird: cannot specify both delay and deliver_at';
    END IF;
    _at := coalesce(deliver_at, clock_timestamp() + delay);
    _future := _at IS NOT NULL AND _at > clock_timestamp();
    -- past-due targets (delay '0', an embargo already lifted) append immediately

    -- One PK probe, deliberately on the hot path (not a join): a clearer
    -- error than the missing-partition one.
    PERFORM 1 FROM cb_streams s WHERE s.name = cb_stream_publish.stream;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: stream % not defined', stream;
    END IF;

    -------------------------------------------------------------------- keyed
    IF key IS NOT NULL THEN
        _id := CASE WHEN _future
            THEN nextval(pg_get_serial_sequence('cb_stream_pending', 'id'))
            ELSE nextval(pg_get_serial_sequence('cb_stream_messages', 'id')) END;

        -- Try to claim this key; if it's already claimed, learn who owns it.
        -- One statement, safe under concurrent same-key publishes. Do not
        -- simplify — each part is load-bearing:
        --   * DO UPDATE ... WHERE FALSE: DO NOTHING would neither lock the
        --     existing row nor let us RETURN it; DO UPDATE locks it, and
        --     WHERE FALSE cancels the update itself.
        --   * RETURNING emits a row only if OUR insert won.
        --   * The UNION ALL branch reads the existing owner when we lost.
        -- Outcome test: returned ref_id = _id means claimed; different means
        -- the key was taken. (Full commentary in migration 00006.)
        WITH won AS (
            INSERT INTO cb_stream_keys AS d (stream, key, ref_kind, ref_id)
            VALUES (stream, key,
                    CASE WHEN _future THEN 'pending' ELSE 'message' END, _id)
            ON CONFLICT (stream, key)
            DO UPDATE SET ref_id = d.ref_id WHERE FALSE
            RETURNING d.ref_kind, d.ref_id
        )
        SELECT x.ref_kind, x.ref_id INTO ref_kind, ref_id FROM (
            SELECT w.ref_kind, w.ref_id FROM won w
            UNION ALL
            SELECT d.ref_kind, d.ref_id FROM cb_stream_keys d
            WHERE d.stream = cb_stream_publish.stream
              AND d.key    = cb_stream_publish.key
            LIMIT 1
        ) x;
        IF ref_id IS NULL THEN
            -- rare: we lost to a claim that committed *during* our conflict
            -- wait — invisible to this statement's snapshot. A fresh statement
            -- gets a fresh snapshot; closes the NULL-return edge the
            -- single-statement form has always had.
            SELECT d.ref_kind, d.ref_id INTO ref_kind, ref_id
            FROM cb_stream_keys d
            WHERE d.stream = cb_stream_publish.stream
              AND d.key    = cb_stream_publish.key;
        END IF;

        IF ref_id <> _id THEN
            -- the key is known: skip — first publish wins. No notify (02 §4).
            existing := true;
            RETURN;
        END IF;
        -- claim won — fall through, store under the pre-allocated id
    END IF;

    ------------------------------------------------------------------ waiting
    IF _future THEN
        ref_kind := 'pending';
        ref_id := coalesce(_id,
                        nextval(pg_get_serial_sequence('cb_stream_pending', 'id')));
        -- `key` stored on the row so delivery can swap the dedup ref from
        -- 'pending' to the delivered message — the window spans both stages
        INSERT INTO cb_stream_pending
            (id, stream, topic, payload, headers, deliver_at, key)
        VALUES (ref_id, stream, topic, payload, headers, _at, key);
        PERFORM pg_notify(current_schema || '.cb_tick', to_char(_at AT TIME
            ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
            -- today's visible_at timestamp encoding, reused: the Go delivery job
            -- parses it and re-arms its timer if this is now the earliest
        RETURN;   -- no assigner notify, no wire nudge: nothing was appended
    END IF;

    ---------------------------------------------------------------- immediate
    ref_kind := 'message';
    ref_id := coalesce(_id,
                    nextval(pg_get_serial_sequence('cb_stream_messages', 'id')));
    -- (id is GENERATED BY DEFAULT — explicit inserts allowed)
    INSERT INTO cb_stream_messages (id, stream, topic, payload, headers)
    VALUES (ref_id, stream, topic, payload, headers);

    -- One notify per append, on a channel named after the stream — convention,
    -- not config. Whoever cares listens: the assigner driver on every stream,
    -- wire on the bus (02 §4). Notifies are hints; spurious wakeups are
    -- harmless (D17).
    PERFORM pg_notify(current_schema || '.cbs_' || stream, topic);
    -- (a NULL topic is fine: delivered as an empty payload, same as '')
END;
$$;
```

Sketch-level notes. Real code prefixes parameters, per the existing
`cb_send.queue`-style qualification convention. Function shape rule: **always
exactly one row → OUT params; zero-or-more rows → RETURNS TABLE**.
`cb_stream_claim` follows the OUT form: it always returns one row, and "nothing
to claim" is NULLs, so callers do a null check instead of handling zero rows.
The batch variant is the same shape over `unnest()`, with one assigner notify
per touched stream. `cb_stream_deliver_pending` owes three things per delivered
message: swap the dedup ref from `pending` to the delivered message, notify the
assigner, and fire the stream's wire notify. Delivery *is* the append, so the
notifies move with it.

### Deferred: pending merged into the log, keys as a unique index

`cb_stream_keys` has one job no unique index over the row tables could do:
the claim spans the pending/message split, because a keyed waiting message
owns its key before it is a message. If waiting messages lived in
`cb_stream_messages` itself — a `deliver_at` column and the pending row's
`key` column move in, the assigner skips rows not yet due — one unique index
on `(stream, key)` (legal on the partitioned table: the partition key is
among the index columns) would be the whole dedup mechanism:
one uniqueness home, a window that is exactly the row's lifetime so the
key-row/message-row prune skew dies, and `cb_stream_keys`,
`cb_stream_prune_keys`, the `ref_kind` flip and the `ref_created_at` bump
all deleted. An investigation, not a design — what it must answer first:

- the assigner scans `pos IS NULL`; every waiting message would sit in that
  scan for its whole delay, re-read on every pass;
- retention needs an answer for rows that have not delivered yet — pruning
  by `created_at` would delete a message delayed past its stream's retention
  before it ever delivers (today waiting rows are exempt by living in
  another table);
- the dedup window would start at publish, not delivery — today the
  `ref_created_at` bump gives a delayed message its full window as a
  message;
- publish keeps the claim-or-learn construction either way (`WHERE FALSE` +
  `UNION ALL` against the index), so publish itself gets no simpler.

Parked rather than scheduled because what it would delete is small and
self-contained — publish claims, delivery flips, the janitor prunes, nothing
else touches the table. Worth opening only when the pending path is being
reworked for its own reasons.

## 9. Dead letters are dead rows (D36, supersedes D6)

A dead letter is a retry row that exhausted: `dead = true`, envelope, count,
`last_error` and timestamps all already on the row (§7) — nothing is
published, nothing is born lazily, and cursors' `dead_letter` failure policy
writes the same row (attempt 0, the poison error). The row parks until a
human acts: **redrive is a reset** — `dead = false, attempt = 0, claimable_at
= now()` — and only the owning subscription delivers it again; **dismiss is a
delete**. The old stream-shaped redrive republished to the origin stream,
which re-delivered the message to every cursor and subscription that had
already processed it — a correctness leak the reset shape removes, along with
the `sd.*` family, its forever-retention special case, and the one
lazily-born object in the system. Alerting rides a notify on dead-row insert
plus the dashboard's count; if a workload ever truly wants failure *events as
a feed*, publishing them from `on_fail` handling is an additive policy choice
then. Dead rows have no automatic retention — unhandled failures waiting for
a human are the one thing a timer must never silently drop; triage is the
deathplace.

## 10. Retention (D7)

**Retention is the stream's one storage knob.** `cb_streams.retention` is an
interval set through `cb_stream_ensure(stream, retention)`. One argument carries
three states — the coalesce pattern plus a sentinel for the one plain-`NULL`
can't carry: a **NULL** argument leaves the current setting (like every `ensure`
field), a **positive** argument sets a bounded retention, and **`cb_forever()`**
sets *forever*. Values are stored verbatim — what you pass is what's stored — so
`ensure` is a plain `coalesce`, exactly like `cb_stream_ensure_queue`. Only the
exact sentinel means forever; `0` or any other negative raises rather than
silently becoming "keep forever". The column is **`NOT NULL DEFAULT
cb_forever()`** (a `CHECK` allows only the sentinel or a positive duration):
retention is always a deliberate set value, never `NULL` — a `NULL` would be
indistinguishable from a forgotten write, so the stored sentinel is the more
bug-resistant representation, and it also lets `ensure` reset a stream back to
forever. There is no granularity or partition knob — the user says how
long to keep, never how it is stored.

**MVP mechanism: a batched delete, not a partition drop.**
`cb_stream_prune_messages(stream, retention?, batch)` deletes messages past the
cutoff in bounded `FOR UPDATE SKIP LOCKED` batches; the retention argument is
optional and overrides the column, and NULL either way means forever, so the call
is a no-op. The message table is partitioned **by stream only** (`LIST`) — one
partition per stream, created at ensure, covering all time. No time
sub-partitioning, which buys two properties:

- **Publishing never depends on the janitor.** There is no future partition to
  pre-create, so a missing ticker only means no purge (messages accumulate,
  safe) — never a failed insert. Cleanup degrades; the write path never does.
- **No force-advance.** Reads and claims track *positions*, not rows, so a deleted
  span is transparent: a cursor's next read selects the surviving positions and
  jumps the gap in one step; a queue claim over a gap fetches fewer rows and
  closes normally. (The one cost: a consumer absent longer than the whole window
  churns empty claims once through the dead zone — bounded and one-time.)

The price versus a partition `DROP` is autovacuum. `DELETE` leaves dead tuples;
append-at-tail / delete-at-head is the friendly FIFO case — the heap plateaus at
about one window of live data and freed space is reused — but a busy stream needs
autovacuum tuned to keep pace or it bloats. This is the classic Postgres-as-queue
tradeoff, taken knowingly for MVP simplicity.

**Forever (NULL) is an explicit opt-in** for the log-as-record cases — event
sourcing, audit, dead letters kept until someone triages or redrives them. It maps to the
same one-partition layout with no drop: the stream grows unbounded, by choice.
Bounded is the norm; forever is deliberate.

**Retention is a hard cap.** When it drops data a slow or absent consumer never
reached, that consumer loses the span — by policy, not a bug. Under fan-out (02) a
lagging cursor otherwise pins storage shared by everyone, so the cap has to win.

**There are no auto-created streams anymore** (D35, D36): every stream is
user-ensured, so retention has one rule and no per-family defaults. The old
*drop what's handled, keep what hasn't* asymmetry became structure in the
retry table: resolved rows are deleted on the spot, dead rows sit until
triaged (§9). Retry rows in flight are self-pruning by lifecycle and need no
janitor; whether resolved rows should leave a short audit trail
(`resolved_at` + a sweep) is deferred until someone misses it.

### Deferred: drop-based retention, per stream (the escape hatch)

For a busy, long-lived, retained stream where `DELETE`/autovacuum churn hurts,
range sub-partition **that one stream** so retention becomes an `O(1)` `DROP` with
zero dead tuples. Left out of the MVP because it earns its complexity only there,
and it is additive — one stream at a time, no global tax. The shape when it lands:

- **The window is engine-derived and fixed, sized to *retention*, not lifetime.**
  Live partitions ≈ `retention / window`, so a forever-lived stream does **not**
  accumulate leaves — old ones drop off the back. Long retention → coarse windows
  (monthly); short retention → fine (hourly); aim for ~a dozen live partitions.
  Over-retention is up to one window (a partition survives until its *newest* row
  clears the cutoff), so a large window suits only a coarse retention, and a hard
  "delete within exactly R" rule tolerates no window slack at all.
- **No DEFAULT partition** — once rows land in it, the overlapping range partition
  can no longer be created without moving data. Pre-create ahead instead; a
  missing partition fails loudly.
- **Force-advance returns with the drop.** Recompute the floor as
  `max(position in P) + 1` *inside the drop transaction* — a long-running insert
  can commit a fresh high position into P at the last moment, and `DROP`'s ACCESS
  EXCLUSIVE lock serializes with exactly that — then advance every lagging cursor
  and queue (`claimed`/`closed`) to it and emit a `$sys.data_loss` event.
- Reads filter on `position` while partitions range on `created_at`, so Postgres
  cannot prune partitions for a read; with a handful of partitions the
  MergeAppend-with-one-probe-each is negligible. Only if a stream needs fine
  granularity × long retention (>~20 partitions) add a per-partition min/max
  position catalog so reads derive a `created_at` predicate — the naive
  `created_at >= last_seen − slack` is wrong (long transactions carry old
  timestamps but high positions, straddling any fixed slack).

**Also deferred: a size cap** (a `retention.bytes` analog). Time retention still
lets a burst blow up disk inside the window; naming it so it is a known deferral,
not an omission.

## 11. Operational notes

- **Postgres 14+** (D16): mature declarative partitioning, `ON CONFLICT` on
  partitioned tables, performance work we rely on. 13 is EOL.
- Claims are ordinary logged rows, so failover to a replica keeps in-flight
  claims and their deadlines (D23) — no mass redelivery on promotion.
- Hot-path rules kept: no joins on reads (`position > x` range scans), no advisory
  locks outside the assigner's try-lock, no N+1. The cursor and queue rows are the only contended
  row; batch sizes amortize it.
- Payloads stay `jsonb` (today's choice); revisit `bytea` only if a real workload
  demands opaque payloads.
- BRIN on `(created_at)` per partition for the janitor; btree `(stream, position)`
  for everything else. Ordinal updates are non-HOT (position is indexed) — accepted,
  see §2 costs.

## 12. Build checklist

1. DDL migration (`cb_stream_*`), goose table `cb_stream_migrations`.
2. `cb_stream_publish(stream, topic, payload, headers, key?, delay?, deliver_at?)`
   — the keep-oldest key rule (§8); emits `pg_notify` (no listeners until M5).
   Sketched in §8. Plus the batch variant.
3. Assigner function + kernel-ticker wiring (poll-only, D17); every node calls
   it, the try-lock decides.
4. Ordered consume (both Go shapes); cursor/queue ensure with the
   cross-table name check under the setup advisory lock; start positions.
5. Subscriptions: claim / adopt / extend / release / close — per-claim TTL
   expiry (D23); no heartbeats, no sweeper; released re-hands whole,
   expiry quarantines to retry rows (D28 as amended by D35/D38);
   `claim_batch_size` from the subscription row; due retry rows served by
   the same claim call as solo pseudo-claims, minted at hand-out, lapsed
   leases repaired there too.
6. `cb_stream_fail` + policy columns (one budget — no `max_crashes`);
   `cb_stream_deliver_pending` + the schedule scan (cron re-arm) — user
   delays only, no retry traffic.
7. Dedup table + prune janitor; the retry table with dead rows,
   redrive-as-reset, dismiss-as-delete.
8. Partition pre-creation job (no DEFAULT partition); retention janitor with
   age-cap force-advance.
9. Tests — the ones that gate everything else:
   - **The torture test**: publisher holds a transaction open across N assigner
     ticks while others publish; assert no loss, no reorder, cursor never passes an
     undelivered position. Run under `-race` with dozens of concurrent publishers.
   - Contiguity: positions have no gaps after crash-kill of the assigner mid-batch.
   - Dual-assigner exclusion: two assignment transactions opened concurrently by
     hand — the second must lose the xact-lock try and assign nothing. (Kill-9
     of the assigner does *not* reproduce this race; build it deliberately.)
   - Duplicate fail: `cb_stream_fail` twice for one delivery → one retry row,
     one charge (the second report is fenced by the lease / collapsed by the
     primary key); the main stream gained no rows — retries are never
     published, structurally.
   - Retry isolation: subscription A's retry row redelivers to A only — B and
     every cursor never see it (assert by construction: the row carries A's
     name).
   - Fail-then-quarantine: fail a message, then crash its range before close →
     exactly one live retry row (`ON CONFLICT DO NOTHING` honors the reported
     failure); `last_error` says `'silence'` for crashes and the verdict text
     for verdicts, never one for the other.
   - Release is uncharged: release a claim repeatedly across adoptions → no
     retry rows appear, no counts move; only true expiry quarantines.
   - No evidence, no charge: crash a range of N with one poison message →
     bystanders quarantine at `attempt = 0`, complete on their first solo
     delivery, and end with `attempt = 1`; only the poison row climbs.
   - Outage runway: continuous consumer death gives up on a cycling message only
     after `max_attempts` solo rounds, each backoff-separated — assert the
     give-up latency matches policy, not luck.
   - Dead rows: exhaustion parks the row (`drop` deletes it); redrive resets
     it and only the owning subscription re-delivers — cursors on the origin
     stream see nothing.
   - Exactly-once: consumer transaction aborts after effects → redelivered → effects
     appear exactly once.
   - Claim crash: kill a consumer mid-range; its claim expires; another consumer
     adopts; duplicates ≤ range size.
   - Retention: age cap force-advances an abandoned cursor and emits the loss event.
   - The key rule: any same-key publish while the key is known (waiting or
     delivered) is skipped and returns the existing ref with `existing = true`.
