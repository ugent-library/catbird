# Raven on the new substrate — usage sketch

Purpose: derive what the spine layer must be from application code we actually
want to write, instead of designing routing machinery in a vacuum. Source
material: the two production apps on catbird — raven (surveyed effect by
effect below) and ingest (its own section) — plus biblio as an outside data
point. Every "today" claim has a file reference into the app's repo.

The method: for each effect of a record change, show how raven does it now,
write the code we would want on the new substrate, and note what that code
demands from the engine. The requirement list at the end is the sum of those
demands and nothing else.

## How raven changes records today

Every write funnels through one transactional closure, `Repo.Revise`
(`revisions.go:275`): begin tx, run the mutation, append the accumulated
change events to the `record_events` table in one `CopyFrom`, commit. A
statement-level trigger (`migrations/00003_cursors.sql`) fires
`cb_notify('record.events')` on commit. The search indexer
(`index_worker.go`) listens on wire, claims a lease on a hand-rolled
`cursors` row, reads `RecordsChangedSince`, indexes, advances the cursor,
with a 5s poll fallback.

Read that list again: an append-only log written in the caller's
transaction, an on-commit notification, a named durable cursor with a lock,
a consumer loop with poll fallback. **Raven hand-built the stream substrate
in app code.** That is the strongest validation M1/M2 could ask for: the
primitive got built because the app needed it and catbird didn't offer it.

Biblio tells the same story with older tools: cron every 10 minutes, a
timestamp file as the cursor, incremental indexing over the Mongo store.
Same pattern — a consumer with a durable cursor over a change feed — no
routing layer anywhere.

## Finding: the publish side of the bindings regressed, silently

`sync_representations` (`representations_task.go`) and `push_orcid`
(`pushorcidtask/task.go`) bind to `record.*.{created,updated,deleted}` /
`record.work.*` via `BindTask`, but nothing in raven calls `Publish`
today. Git history shows this is a regression, not unfinished work:

- **2026-04-16** (`de043f4`): record writes publish
  `record.<type>.<verb>` topics inside the write tx;
  `sync_representations` binds to them. Wired and working.
- **2026-04-27** (`62b3e04`): the `record_events` log arrives; the
  indexer migrates to it.
- **2026-05-09** (`9931e30`): a large domain refactor removes the
  publish helper in the churn. The bindings and the task remain. No
  error, no failing test, no backlog — dormancy begins.
- **2026-05-13** (`baf8607`): `push_orcid` is added to the
  already-severed path, four days after it died. A new consumer was
  bound to a dead topic and nothing said so — not even to its author.

Nobody noticed for two months because the failure is doubly silent:
fan-out-on-write leaves no trace when nothing publishes, and
`app/record_format.go` falls back to marshaling formats live when the
materialized row is missing — a fallback designed to cover task *lag*
has been covering total task *absence*. Output stayed correct; the
materialization just never happened.

Note what the May 9 refactor actually was: raven moving itself onto the
log model — `record_events` replaced the topic publishes for the
indexer. The stranded consumers are exactly the ones the log hadn't
grown primitives for yet: a second projection cursor (representations)
and a filtered queue with retry semantics (orcid).

Recovery is possible only because `record_events` retains everything —
because raven had a log. Under the new substrate the incident becomes a
non-event: attach the consumer to the feed that provably exists (the
indexer already drains it), start its cursor at the beginning, and the
missed history replays. This is the real-world case for keeping late
binding and replay, and against trusting a routing table that no live
code path exercises.

## The sketch, effect by effect

### 1. The change feed itself

Today: `record_events` insert via `CopyFrom` inside `Revise`, plus the
notify trigger.

Wanted:

```go
// inside Repo.Revise, same tx as the record write
_, err := stream.Publish(ctx, tx, "records", "record.work.updated", ev)
```

The stream named `records` *is* the change feed — but a thin one.
`record_events` stays the queryable domain truth: the history pages
(#17/#18) want typed columns, actor joins, and app-owned indexes, which a
generic payload column is strictly worse at. The stream carries thin
change messages (topic + record id + event ref), appended in the same tx.
Consumers lose nothing: the indexer and representations already read
*current* record state, not event payloads.

Settled here: **streams do not grow query APIs.** Persistent NATS drifted
into get-by-subject, KV, object store — rebuilding database features over
a log because no database was nearby. Catbird lives inside one. The
sanctioned query path for a stream is read-only SQL over its partition,
with your own expression indexes; the engine promises layout stability
and retention-honoring pruning, not a query surface.

Demands on the engine: transactional append with a topic — **exists**
(`stream.Publish`). One gap: a `Revise` can emit many events (imports,
batch edits emit hundreds) and today flushes them in one `CopyFrom`.
`PublishPayloads` batches, but takes a single topic for all payloads
(`cb_stream_publish_payloads`). **Gap: batch append with per-message
topics.** Small, real, found only by sketching.

### 2. Search indexer

Today: wire listener + `ClaimCursor("records")` + `RecordsChangedSince` +
`AdvanceCursor` (`index_worker.go:55-152`) — the hand-rolled consumer.

Wanted:

```go
stream.Consume(ctx, pool, "records", "indexer",
    func(ctx context.Context, batch []stream.Message) error {
        // dedup by record id, fetch current state, index
    })
```

The trigger, the `cursors` table, the lease helpers, the wake channel and
the fallback timer all get deleted; they are the engine now.

Note what the indexer does **not** want: a topic filter. It consumes every
record event. The app's biggest, oldest, most battle-tested consumer needs
zero routing.

Demands: ordered consume with a durable cursor — **exists**.

### 3. Representations sync (dormant) — the indexer's twin

Representations are derived data: a representation depends only on the
record's *current* state — public record, render the formats; withdrawn or
deleted, tombstone. That is a projection of the record log, exactly like
the search index. Its binding pattern says so too:
`record.*.{created,updated,deleted}` is the whole feed — the routing layer
was filtering nothing.

Wanted: a second named cursor on the same stream, same shape as §2:

```go
stream.Consume(ctx, pool, "records", "representations",
    func(ctx context.Context, batch []stream.Message) error {
        for _, id := range recordIDs(batch) { // dedup: many events, one rebuild
            // current state decides: public → render, gone → tombstone
        }
        return nil
    })
```

The old `.WithCondition("input.made_public_at exists")` becomes one `if`
on the record's current state in the handler — a state question, not an
event question. Failures here are deterministic (local rendering, no
external party), so retry-with-backoff buys nothing: fix the bug, resweep
the cursor. And a cursor batch coalesces — ten edits to one record is one
rebuild, where the old per-event task ran ten times.

Demands: nothing new. **A consumer that looked like a bindings customer
dissolves into a plain cursor.**

### 4. ORCID push (dormant) — a filtered queue, not a copy

What it wants when it comes alive: a *subset* of the feed
(`record.work.*`), an outbound network call per record with tokens and
rate limits — so transient failures, retries with backoff, dead-letter.
Note what it does *not* need protecting from: other consumers. Cursors
are independent — a stalled orcid consumer stalls only itself, and during
a global ORCID outage pausing its head is not a problem, it's the correct
behavior. The one real hazard is a single poison item (revoked token,
permanently bad record) parking this consumer's head forever.

Queue semantics solve that per message — and they apply directly to the
feed once queues take a server-side filter:

```go
stream.EnsureQueue(ctx, pool, "records", "orcid_push",
    stream.QueueOpts{Topic: "record.work.#", StartPos: stream.At(0)})
stream.ConsumeQueue(ctx, pool, "records", "orcid_push", handlePush)
```

No copy. The claim machinery is position-based and indifferent to
content: claims still tile contiguous ranges, accounting is unchanged,
the filter touches only the message fetch inside a claimed range. The
unhappy path already owns a store — a failed push lands in
`sr.records.orcid_push` with its own retention. Late binding and replay
were always cursor-start semantics, never copy semantics: `StartPos`
replays retained history, which is the mechanical answer to the
regression incident above.

The job does need storage — but *domain* storage, not job
infrastructure: the push returns an ORCID id that must be recorded per
(record, target) — raven's `pushed_records` table. The handler writes it,
and since delivery is at-least-once, that keyed upsert is the idempotency
boundary. The engine's job ends at attempts, backoff, and dead-letter;
the side effect's bookkeeping is an ordinary app row.

Demands: **a server-side topic filter as queue policy** — the one
spine-shaped requirement, replacing autocopy. Matching belongs in SQL:
one implementation, identical semantics for every client, and only
matching messages cross the wire. The shipped `cb_bind`
pattern-to-prefix+regex compiler already implements the `*`/`#` grammar.

### 5. Blob GC

Today: `RunTask(ctx, r.tx, TaskDeleteBlob, …, ConcurrencyKey: blobID)`
inside the write tx (`file.go:338`) — commit-atomic, deduped, explicit.

Wanted: the same shape, no routing:

```go
// inside the write tx: producer knows the consumer, says so directly
_, err := stream.Publish(ctx, r.tx, "blob_deletes", "", in,
    stream.PublishOpts{Key: f.BlobID})
```

Raven already chose explicit same-tx enqueue over topic routing for this
effect, and it's the right call: when the producer knows the consumer,
routing indirection adds nothing. This is also the escape hatch for
commit-atomicity in general — when an effect must exist the instant the
transaction commits, append it in the transaction.

Demands: same-tx append to a work stream with a dedup key — **exists**.
(Semantics note to verify when porting: the old `ConcurrencyKey` dedups
against in-queue messages; the new `Key` is keep-oldest within key
retention. For blob GC either works; not interchangeable in general.)

### 6. Notifications tray (durable inbox)

Today: task handlers call the old `NotifyDurable` with an explicit user
identity; htmx polls `UnseenNotifications(identity)` every 3s and
`MarkSeen`s on render (`app/backoffice_notifications.go`). The new shape
is `wire.Send(recipient, topic, payload)`.

Wanted: the same explicit shape on the M5 inbox. The handler that finishes
a batch edit knows exactly which user asked for it; it writes to that
user's inbox directly. No topic routing, no binding, no `identity_from`
extraction — the identity is a value in the handler's hand, not a segment
to parse out of a topic. Raven never needed identity-in-bindings, which
confirms the plan's own suspicion (02-spine §1) that interpolated
identities were data all along.

Demands: identity-keyed durable store with unseen-query and mark-seen —
M5's business. **Nothing demanded from the spine.**

### 7. Live progress (batch edit)

Today: `catbird.Notify` on ephemeral topics `batch.{runID}.progress`; the
CLI tails `LISTEN cb_wire` and filters client-side
(`batch_edit_cmd.go:307`).

Wanted: the same — NOTIFY on commit, in-process subscribers filtering by
topic. Transient per-connection subscriptions are exactly where an
in-process Go matcher (the trie) belongs: they are not a durable
cross-client contract, they are one process's memory.

Demands: notify-on-append (already fires: `_cb_stream_notify`) plus M5's
in-process fan-out. **Nothing demanded from the spine.**

### 8. Scheduled work

Today: `CreateTaskSchedule(…, "@daily")` for `lift_embargoes`,
`deactivate_users`. Wanted: the stream scheduler feeding a work stream —
**exists** (`stream/schedule.go`).

### 9. On-demand jobs (batch edit, manual import, ORCID bulk push)

Today: web handlers call `RunTask` directly — `batch_edit` and
`manual_import` from the backoffice, `bulk_push_orcid` with
`ConcurrencyKey: userID` (`app/orcid.go:251`). The run id comes back as a
handle: the batch-edit page polls `GetTaskRun(runID)` and reads
`OutputAs`, cancel goes through `CancelTaskRun`
(`app/backoffice_batch_edit.go`), and the handler emits progress on wire
topics plus a durable notification when done.

Wanted: enqueue stays one explicit publish to a work stream —

```go
_, err := stream.Publish(ctx, pool, "batch_edits", "", input,
    stream.PublishOpts{Key: "batch-" + runKey})
```

— and workers drain it with `ConsumeQueue`. No routing anywhere: the
handler knows the destination, same as blob GC.

But the *handle* is the interesting demand. A bare message in a work
stream has no queryable status, no stored output, no cancel. Raven's UI
needs all three, and this is the same missing lookup ingest hit from the
other side (its deliveries correlate to flow runs through a derived
concurrency key plus a capped `ListFlowRuns` scan). Two apps,
independently, want: **a durable run handle — status, output, cancel —
queryable by id or by an application key.** That is the task/flow layer's
business (M4), built *on* work streams, not a spine concern. Recorded
here so M4 starts with it.

One semantics note to carry into M4: `bulk_push_orcid`'s
`ConcurrencyKey: userID` means "one live run per user" — the new `Key`
dedup (keep-oldest within key retention) is close but not identical;
whether run-level singleton semantics live in the job layer or lean on
message keys is an M4 decision.

Demands: work-stream enqueue — **exists**. Run handle with
status/output/cancel — **M4, no routing.**

## The second app: ingest

Ingest (drop-folder packages → Specto/RODA preservation) grounds the other
half of the engine: it uses catbird as a **cron + flow engine and nothing
else**. Seven tasks, every one schedule-triggered with an empty input — pure
cron ticks that re-scan the DB. Three flows with real DAGs: map fan-out over
files (`MapStepOutput`), `IgnoreOutput` ordering-only dependencies, `OnFail`
compensation handlers, 2h step timeouts, one singleton via concurrency key.
Zero topics, queues, bindings, or wire in the live tree.

It did try the routing layer once, and the episode is instructive. Commit
`4d8d125` (2026-03-17) wired live package-status push: flow steps published
`ingest.package.status` inside the transaction, a durable queue was bound
for the worker, and **each browser SSE connection created and bound its own
durable queue**. The next day, `98f2dd5` deleted the whole path in favor of
htmx polling. Read the shape of the failure: an ephemeral need — transient
per-connection subscriptions — was served with durable fan-out machinery,
because the ephemeral primitive didn't exist. On the new substrate that use
case is the NOTIFY path plus an in-process filter: no queue, no binding, no
storage. The appetite for live push was real; the primitive was wrong.

What ingest still runs validates patterns already on the list:

- **Atomic enqueue, again.** `RunFlow` is called on the same transaction
  that creates the package or delivery row — commit-or-nothing, with a
  dedup `ConcurrencyKey`. That's the third production instance of same-tx
  explicit enqueue (raven's blob GC, raven's April publishes, ingest's
  flows). In every live case, the producer knows the consumer.
- **Scan-triggered flows, not topic-triggered.** Even the flow engine's
  only real customer starts every flow from a cron scan plus a
  transactional `RunFlow`. No `BindFlow`, no demand for a flow-kind relay.

What ingest asks for is all M4's business — recorded here as its seed
requirements: transactional run-with-dedup-key (the concurrency key is
load-bearing), DAG dependencies + map fan-out + per-step timeouts, a
terminal failure hook that fires even on hard worker death (today `OnFail`
can be skipped when retries exhaust after a crash — the
`sweep_stuck_deliveries` task exists solely to repair that), and a durable
run handle queryable by an application key (today: a derived concurrency
key, a capped `ListFlowRuns` scan, and the sweeper — three workarounds for
one missing lookup).

Demands on the spine: **none**.

## Roadmap consumers (raven's open issues)

The open issues add future consumers; each lands on the same primitives.

- **LDN receiver (#118)**: accept with `202`, persist the payload
  reproducible as-is plus provenance, process async — an `ldn_inbox`
  stream (payload = message, provenance = headers), drained by workers.
  The processing chains (#132) gate on payload jsonpath — content-based,
  config-driven, app-side; the issue itself scopes catbird to
  "scheduling, retries, backoff, timeouts, cron, DAG workflows".
- **LDN sender (#119)**: the outbox it describes — durable payloads,
  interval-checked, exponential backoff, errors kept — is a work queue
  clause for clause. Same-tx `Publish` to `ldn_outbox` (an Announce must
  not escape a rolled-back edit), `ConsumeQueue` with backoff,
  dead-letter keeps the errors. The interval check dissolves.
- **Per-target external delete (#14)**: "consumes the abandonment
  stream", in the issue's own words — a second filtered queue on the
  feed, same shape as ORCID push. Filtered queues are a recurring family
  (per-target sync and delete), not a one-off.
- **Webhook subscribers (roadmap)**: the first consumer where
  subscribers are *data, not code* — hundreds of rows, churning at
  runtime. One dispatcher cursor over the feed matches every subscriber
  pattern in app code (the topic trie's real home: in-process, built
  from app rows) and enqueues one delivery job per (subscriber, event)
  into a work stream; retries/backoff/dead-letter do webhook delivery
  semantics, and disable-after-N-failures is a janitor consuming the
  dead-letter stream. Two fan-out regimes, one substrate: few static
  developer-defined subsets → filtered queues; many dynamic user-defined
  subscribers → one dispatcher + delivery queue.
- **History pages (#17/#18)**: domain queries over events — served by
  `record_events` the domain table (§1), never by stream query APIs.

## What the sketch demands, in total

| # | Requirement | Status |
|---|---|---|
| 1 | Transactional append with topic | exists (`Publish`) |
| 2 | Batch append with per-message topics | **gap** — small |
| 3 | Ordered consume, durable cursor, whole stream | exists (`Consume`) |
| 4 | Work-stream queue semantics (retry, claims, parallel) | exists (`ConsumeQueue`) |
| 5 | Same-tx explicit enqueue with dedup key | exists (`Publish` w/ `Key`) |
| 6 | Server-side filter: topic pattern + condition, queue policy + cursor reads | **the spine — new** |
| 7 | Identity-keyed inbox, written explicitly by handlers | M5, no routing |
| 8 | Ephemeral topic fan-out in-process | M5, no routing |
| 9 | Durable run handle: status/output/cancel, queryable by app key (raven UI + ingest deliveries) | M4, no routing |

## What the sketch does not demand

Just as important, because each of these was on the table:

- **A general routing layer every message flows through.** No live code
  path in either app uses one: raven's publishes regressed away unnoticed,
  ingest's lasted a day. The feed is consumed whole (indexer), copied
  narrowly (orcid), or targeted explicitly (blob GC, inbox, flows).
- **Autocopy / bindings / relays.** Earlier drafts of this sketch routed
  feed subsets into physical copies so queue semantics could apply to
  them. Filtered queues apply those semantics in place — no copy, no
  relay job, no binding tables, no cursor-per-destination bookkeeping.
  What autocopy alone can do — fan-in from several streams,
  transformation, a copy outliving its source's retention — has no
  customer in either app. (This reverses an earlier claim here that
  filtered reads had no takers: the takers appeared the moment queue
  semantics could ride the filter — ORCID push, #14's per-target
  deletes.)
- **`identity_from` / inbox-kind bindings.** The inbox is written
  explicitly by handlers that hold the identity as a value.
- **Flow-kind and registry machinery.** The flow engine's one real
  customer (ingest) starts every flow from a cron scan and a same-tx
  `RunFlow` — no topic-triggered flow exists anywhere. Let M4 pull the
  flow-kind writer into existence if one ever appears.
- **A separate well-known `bus` stream.** Raven's feed is the domain
  stream `records`. Autocopy should take a source stream argument; whether
  an app also wants one catch-all bus is the app's naming decision, not
  engine structure.

## Consequence for M3

M3 shrinks to exactly requirement 6 plus the small gap 2 — no new tables,
no relay job:

1. **Server-side topic filter.** One `topic_matches` implementation in
   SQL: port the `cb_bind` pattern compiler (validate, split into prefix
   + regex, compile once at ensure), match per row via `~` in the read
   paths. On queues the filter is **policy** — a column on
   `cb_stream_queues`, set at birth, applied in the claim's message
   fetch. Policy, not a read argument, for the same reason as
   `claim_batch_size`: competing consumers of one queue must agree.
   Claims still tile every position; a sparse filter just closes empty
   ranges fast. On cursor reads the filter has one semantic to pin: the
   cursor advances over the whole *scanned* range, `batch_size` counts
   scanned rows, and a read may legitimately return zero messages while
   advancing.
2. **Per-message topics in batch append** — extend the payloads path.

Known residual, not new: a filtered queue lagging past the source
stream's retention loses unclaimed messages — already true today for any
queue on a finite-retention stream.

**Filter cost and indexes.** Steady-state filtered consumption is
tail-following: each new row is scanned once per consumer inside a
pos-bounded batch — no index needed, ever. The index question exists only
for deep replay over a sparse filter (activating `record.work.#` at
`At(0)` against years of feed). Three commitments keep the consequences
contained:

1. **The filter is two small languages, each doing one job.** `topic` — a
   bare pattern, single, least verbose, the fast path, the only dimension
   that can ever be index-assisted — plus an optional `condition` over
   headers and payload: AND-only, parsed once at registration into
   per-column jsonpath, MVP limited to nested-key existence and scalar
   equality (a whitelist that grows deliberately). The condition is the
   relief valve that keeps attributes out of topic strings — topic-only
   filtering would stimulate cramming (`record.work.updated.public…`).
   Documented rule: any condition costs a per-row jsonb evaluation and is
   never index-assisted — topics select, conditions prune. Richer content
   routing (#132's config-driven chains) stays app-side, and no jsonb
   index ever exists on the stream. (`condition` also inherits the old
   task DSL's role, so M4 step conditions can share the language.)
2. **The engine never creates content indexes; correctness never depends
   on one.** Its read SQL is written index-usable (the compiled
   `prefix LIKE` + regex predicate), nothing more.
3. **Indexes are app-owned, per stream partition** — same contract as
   read-only SQL over streams. Only streams with deep-sparse-replay
   consumers pay the write amplification; retry and dead-letter streams
   never do. Contained follow-on once an index exists: the claim
   function can fast-forward `to_pos` over non-matching spans
   (next-matching-pos lookup), turning sparse catch-up from row-scan
   into index-hop — pure SQL, no schema change.

Scale check: both apps run at bibliographic-edit volume. No index is
needed for years; what must be right now is the contract, so the first
consumer that does need one isn't a redesign.

Bindings, relays, autocopy, destination kinds, the kind registry, the
well-known bus: all out — 02-spine's relay chapter reduces to filtered
consumers. Revive autocopy only if a fan-in, transformation, or
copy-outlives-source-retention customer ever appears. Exit test for M3:
ORCID push on paper is `EnsureQueue` with a filter plus `ConsumeQueue`,
and activating it late with `StartPos: At(0)` replays retained history —
the regression incident's mechanical answer, with no copy machinery at
all.

## The collapse

Read filtering is not just M3's replacement — it deletes concepts across
the old catbird and the plan:

- **Routing itself.** Topics stop deciding where messages *go* and become
  metadata consumers *select on*. There is no fan-out — not at write time
  (old `cb_publish` looping over bindings), not at read time (the relay).
  Publish is one insert, always, and never knows its consumers.
- **The old generic-queue half of catbird.** Per-queue tables (`cb_q_*`),
  `cb_bindings` with its compiled prefix/regex columns, `cb_bind`/
  `cb_unbind`, the `cb_publish` fan-out loops — a queue in the new world
  is a policy row plus a filter over a stream, which `cb_stream_queues`
  already is.
- **Topic-triggered tasks and flows (M4).** Old `BindTask`/`BindFlow`
  dissolve the same way: a task triggered by `record.work.*` is a task
  whose work queue has that filter. No binding concept survives in the
  task layer either.
- **The plan's open debate.** Fan-out-on-write vs fan-out-on-read (D8)
  was arguing over *where to copy*. Filtered reads answer: don't copy.
  The spine as a named layer dissolves; 02-spine's remaining content is
  the wire/NOTIFY path.
- **One mental model remains:** a consumer is a filter plus a position
  over a log. Three durability flavors of the same concept — no position
  (wire, ephemeral, in-process match), an ordered position (cursor), a
  claimed range with retries (queue). Everything in both apps and every
  roadmap issue maps onto one of the three.

What survives untouched: the matcher (one SQL `topic_matches`), the Go
trie (app-side dispatchers like webhooks), retry/dead-letter streams,
schedules, and the same-tx explicit enqueue escape hatch.
