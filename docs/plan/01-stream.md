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

**Mechanism.** Messages insert with `ordinal NULL`. The sequencer — the kernel
ticker, woken by NOTIFY on publish with a small debounce (~10–25ms) plus a fallback
tick (250ms) — runs, per stream with unsequenced rows:

```sql
-- single writer per stream, guarded by pg_try_advisory_lock(hash(stream));
-- advisory locks were rejected as a *claim* mechanism, not for singleton election
WITH todo AS (
    SELECT id, row_number() OVER (ORDER BY id) AS rn
    FROM cb_stream_message
    WHERE stream = $1 AND ordinal IS NULL
    ORDER BY id
    LIMIT $2
), bump AS (
    UPDATE cb_stream SET last_ordinal = last_ordinal + (SELECT count(*) FROM todo)
    WHERE name = $1
    RETURNING last_ordinal - (SELECT count(*) FROM todo) AS base
)
UPDATE cb_stream_message m
SET ordinal = bump.base + todo.rn
FROM todo, bump
WHERE m.stream = $1 AND m.id = todo.id;
```

Properties that fall out:

- **Ordinals are contiguous per stream** (a counter, not a sequence — no gaps,
  ever). "Am I caught up" and "is this range complete" become integer arithmetic.
- Only *visible* rows get ordinals, so ordinal order = commit-batch order, ties
  broken by insert id. A row that never commits never exists downstream.
- The sequencer is idempotent and crash-safe: it assigns or it doesn't; on restart
  it continues from `last_ordinal`. If no node holds the advisory lock momentarily,
  sequencing pauses — delivery pauses, nothing is lost.
- After sequencing, `pg_notify` per touched stream wakes consumers.

**The honest costs.** (1) One extra `UPDATE` per message — double heap/WAL traffic
versus insert-only. At hundreds/sec this is noise; it would matter at Kafka scale,
which is a stated non-goal. (2) A delivery latency floor of roughly the debounce +
sequencing round: **~30–80ms end-to-end** for *all* consumers, work groups included.
This replaces the vision's "sub-50ms" claim (README amendment 4). The audience —
emails, indexers, notifications, flow steps — does not feel 50ms. (3) A latency
spike while a sequencer leader hand-off happens (sub-second).

## 3. Table shape

All static — creating a stream creates a partition, never a table family.

```
cb_stream          name PK · last_ordinal · retention config (§10) · created_at
cb_stream_message  PARTITION BY LIST (stream), then RANGE (created_at) per stream
                   stream · id (bigint identity) · ordinal (bigint, NULL until
                   sequenced) · topic · payload jsonb · headers jsonb · created_at
                   (clock_timestamp())
                   indexes: (stream, ordinal) btree per partition; nothing else hot
cb_stream_group    stream · name PK(stream,name) · mode ('ordered'|'work') ·
                   position (ordered: cursor; work: claim_next) · watermark (work:
                   highest ordinal below which all leases closed) · retry policy
                   columns (§7) · created_at
cb_stream_lease    stream · grp · from_ord · to_ord · worker · state
                   ('live'|'released') · created_at        (tiny, hot, mostly empty)
cb_stream_worker   worker id PK · last_heartbeat            (UNLOGGED — see §11)
cb_stream_pending  id PK · stream · topic · payload · headers · deliver_at ·
                   coalesce_key (nullable, UNIQUE(stream, coalesce_key)) ·
                   attempt · origin_ordinal (nullable) · cron_spec (nullable) ·
                   catch_up_policy (nullable)
cb_stream_dedup    stream · key PK(stream,key) · message_id · created_at
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
it). Filters: an optional topic pattern on the group row (§ 02) applied in the read
query; the cursor still advances over skipped rows.

## 5. Work groups (range leases) — the pgmq/pgq re-evaluation (D2)

Your note asked whether the vision's pgmq/pgq hybrid is the optimum. It isn't quite:
once ordinals are contiguous (D1), per-message SKIP-LOCKED claiming — pgmq's core —
becomes unnecessary machinery. Claiming a batch is a **single counter bump**:

```sql
UPDATE cb_stream_group
SET position = least(position + $batch, (SELECT last_ordinal FROM cb_stream WHERE name = $1))
WHERE stream = $1 AND name = $2 AND position < (SELECT last_ordinal …)
RETURNING position - /* claimed count */ … ;
-- + INSERT INTO cb_stream_lease (from_ord, to_ord, worker, 'live')
```

No scan, no anti-join, no lock queue — one hot row per group. Before bumping the
counter, a worker first adopts any `released` lease (`FOR UPDATE SKIP LOCKED` on the
tiny lease table — cold path, fine). Then per message in the range: run the handler;
on failure, write a retry into `cb_stream_pending` (§6) or the DLQ (§9) per policy.
When every message in the range is handled (succeeded, retried, or dead-lettered),
delete the lease and advance the group `watermark` over contiguous closed ranges.
The watermark is the group's retention floor.

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
| Retry with backoff | `attempt = n`, `origin_ordinal` set, `deliver_at = now + backoff(n)` |
| Coalesce / debounce (keep-newest) | `coalesce_key` set — `INSERT … ON CONFLICT (stream, coalesce_key) DO UPDATE SET payload, deliver_at` |
| Cron | `cron_spec` set — on delivery the sweeper appends the message **and** re-inserts the row at the next occurrence, honoring `catch_up_policy` (`skip`\|`all`, ported from today's scheduler, including the on-time-tick fix from #45) |

The sweeper appends due rows to their stream in one transaction with the delete —
exactly-once handoff. The current `scheduler.go` module dissolves into rows in this
table plus builder sugar (`stream.Cron(...)`, `flow.RunEvery(...)`).

## 7. Retry policy lives in the database (D4)

Your note: robustness machinery is currently Go-side (`handler_opts.go`) and
therefore invisible to non-Go workers and to SQL. Move the *policy* into columns on
`cb_stream_group` (and per-step overrides in flow, 03 §6):

```
max_attempts int · backoff_kind ('none'|'fixed'|'full_jitter') ·
backoff_base interval · backoff_max interval · on_exhaust ('dlq'|'drop')
```

`cb_stream_fail(stream, grp, ordinal, error)` computes the next `deliver_at` from
these columns and writes pending or DLQ. A Python worker that calls it gets
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

## 9. Dead letters (D6)

A DLQ is an ordinary stream named `<stream>.dlq`, created lazily on first use.
Exhausted messages are appended there with headers
`{origin_stream, origin_ordinal, grp, attempts, last_error, failed_at}`. Replay is
`stream.Redrive(dlq, n)` — republish to the origin stream (new ordinal, attempt
reset, a `redriven_from` header). Because it is just a stream: it has retention,
it can be consumed (alerting), and the dashboard lists it for free. No special
machinery anywhere.

## 10. Retention (D7)

Per-stream policy, all floors intersected, evaluated by a janitor on the kernel
ticker:

```
floor = min( ordered-group cursors …, work-group watermarks …, pinned? )
drop partition P when max(ordinal in P) < floor AND age(P) > min_age
AGE CAP: when age(P) > max_age, drop anyway; force-advance lagging cursors to
         min ordinal of the next partition and append a `consumer.data_loss`
         event to the stream's control topic
```

The age cap is what makes abandoned consumers survivable and is **structural, not
optional** — under fan-out-on-read (02) a lagging cursor pins storage shared by
everyone. Flow event streams add a third floor: all runs with events in P are
terminal (03 §8). Partition granularity: daily by default, configurable; `DROP` is
instant and leaves no dead tuples — this is the answer to pgmq-style delete bloat,
unchanged from the vision.

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
   — insert or pending or dedup-return; NOTIFY. Plus the batch variant.
3. Sequencer function + kernel-ticker wiring + advisory-lock election.
4. Ordered consume (both Go shapes), group ensure, start positions.
5. Work groups: lease claim / adopt / close, heartbeat goroutine, sweeper.
6. `cb_stream_fail` + policy columns; pending sweeper (delay, retry, coalesce, cron).
7. Dedup table + prune janitor; DLQ append + `Redrive`.
8. Retention janitor with age-cap force-advance.
9. Tests — the ones that gate everything else:
   - **The torture test**: publisher holds a transaction open across N sequencer
     ticks while others publish; assert no loss, no reorder, cursor never passes an
     undelivered ordinal. Run under `-race` with dozens of concurrent publishers.
   - Contiguity: ordinals have no gaps after crash-kill of the sequencer mid-batch.
   - Exactly-once: consumer transaction aborts after effects → redelivered → effects
     appear exactly once.
   - Lease crash: kill a worker mid-range; sweeper releases; another worker adopts;
     duplicates ≤ range size.
   - Retention: age cap force-advances an abandoned cursor and emits the loss event.
   - Coalesce: N rapid publishes with one key deliver once with the newest payload.
