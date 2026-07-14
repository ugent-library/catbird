# 05 — milestones

> Revised 2026-07-14 (branch experiment, D34–D38). M0–M3 shipped as written
> and stand as history. New since: **M3r** (the stream revisit — subscriptions,
> retry rows, dead rows) reopens M2's failure half in row terms, and **M4** is
> restated for the engine on its own rows — no stream prerequisite, kernel
> dependency only. M3r and M4 touch disjoint modules and may run in either
> order or in parallel.

The milestones are sequenced for manual implementation. Each one can be verified
on its own. Each one ships behind the previous one's tests. The riskiest bet comes
first. The new code grows in subpackages beside the current implementation. The
existing top-level API keeps working until M6, so raven keeps running. Same repo,
same test harness (`./scripts/test.sh`), same docker-compose database.

**Ordering rationale:** the assigner (M1) is the load-bearing wall of the feed
layer. If the commit-order design has a flaw, everything above it is wasted. So
the assigner went first and got the torture test before anything else existed.
The engine (M4) is the most work; since D34 it rides the kernel only — M2/M3
proved the feed layer, and the engine needs neither: its base is M0's ticker
and migration runner.

---

## Repo plumbing — how to work

Work additively on main. No long-lived rewrite branch. No `/v2` module: 0.x allows
the one breaking release, at M6. Ground rules:

- **Tag first.** Tag the current state before M0 so raven has a stable pin. Every
  milestone PR then lands on main green without raven noticing, because all new
  code is new import paths.
- **The old code is frozen.** Bugfixes only. The first PR adds a note to
  `CLAUDE.md` ("rewrite in progress: new code lives in `stream/`/`flow/`/`wire/`
  per `docs/plan/`; the top-level API is frozen"). The note stops anyone,
  including future sessions, from improving code scheduled for deletion.
- **Copy, don't rework, shared machinery.** The shared notifier starts as a *copy*
  of `worker_notifier.go`, the LISTEN machinery. There is no `notifier.go`,
  despite CLAUDE.md saying so. Fix that stale reference in the first PR's
  CLAUDE.md note. `wire.go` contains a second, embedded listener; M5 consolidates
  it onto the shared notifier. `notify.go` is only the send helper and stays. The
  originals keep serving the old worker untouched until M6 deletes them.
  Temporary duplication is cheaper than destabilizing what raven runs on.
- **Shared machinery lives under `internal/` during the transition**
  (`internal/ticker`, `internal/migrate`; the M5 notifier joins them), not the
  root package. D15 describes the *end state*. The trap this avoids: the final
  layout puts the shared machinery + facade in the root. Then `catbird.Publish`
  delegates to `stream`, and `stream` imports the root for shared types. That is
  an import cycle. Until M6 the root simply stays the old API. Subpackages import
  the `internal/` packages plus the root's existing `Conn`. The cycle is resolved
  once, at M6 (see there).
- **Tests ride the existing rails.** `./scripts/test.sh` already runs
  `go test ./...`. New subpackages are picked up automatically, and the
  drop-and-recreate of `cb_tst` covers them. Flow is the one exception: it
  reuses old flow table names (below), so its suite runs on its own database
  rather than the shared `cb_tst`. Each new package gets its own
  `TestMain` + `sync.Once` setup running its own migrations (the
  `catbird_test.go` pattern). `go test ./...` runs packages in parallel
  processes, so the `internal/migrate` runner takes an advisory lock. That is
  already this codebase's setup convention.
- **Migrations are per module and additive.** Each subpackage embeds its own
  `migrations/` FS with its own goose table (`cb_stream_migrations`, …). The
  existing `migrate.go` and `migrations/` are untouched until M6. Stream's new
  names (`cb_stream_*`, `cb_stream_publish`) cannot collide with the old ones,
  so the stream and old suites share one database. Flow instead reclaims the
  old flow names (`cb_flows`, `cb_flow_schedules`): raven and biblio have zero
  flows (D31), so the old flow tables are empty, dropped at raven's cutover,
  and the two flow schemas are never needed side by side.
- **The migrations are the naming authority.** Plan sketches convey semantics and
  may abbreviate identifiers (e.g. `stream` for the DDL's `stream_name`). When
  code and plan disagree on a *name*, the migration wins and no doc edit is
  needed. When they disagree on *semantics*, that is a plan bug. Fix the doc, or
  the next reader inherits the drift.
- **Torture test first, assigner second.** M1's acceptance test is written
  before the assigner exists, failing on disk. The stress variant sits behind
  `CB_SLOW_TESTS` (the `requireSlowTests()` gate). A fast deterministic variant
  runs in the default run.
- **No `go.work` until M6.** Only the nested `cb` module needs it.

Suggested first PRs, sized for momentum: (1) `internal/ticker` + tests —
small, self-contained, zero schema decisions, poll-only (D17); (2) the
parameterized migration runner; (3) `stream/` DDL + `cb_stream_publish` + the
failing torture test; (4) the assigner — torture test green, M1 latency gate
measured against the tick math.

---

## M0 — ticker + migration runner (small)

Create two `internal/` packages, `internal/ticker` and `internal/migrate`.
First, a **ticker facility**: it
registers periodic jobs, and the assigner, delivery, and janitors all become
registrations. There is **no leader election**. Every node runs every job,
and each job's own locks decide who works: try-lock for the assigner, SKIP LOCKED
for delivery. Second, a per-module goose runner, keyed
by version table + migration FS and advisory-locked.
**Poll-only at this stage** (D17). Design the job interface with a second wake
source in mind: a channel the M5 notifier can feed. Build no LISTEN machinery now;
the notifier copy is deferred to M5. `Conn` stays where it is (root, public).
`topic_trie.go` untouched.

*Exit:* a toy ticker job fires on its interval; two processes ticking the same
lock-guarded job do each unit of work exactly once (the locks decide, no
election). The wake source itself is deferred to M5: it arrives as a third
`select` case in the job loop.

## M1 — stream core: publish, assigner, ordered consume (the wall)

01 §§2–4, 10: DDL, `cb_stream_publish`, the assigner, cursors with both Go
consume shapes, ensure/start-positions, retention janitor with age-cap
force-advance.

*Exit:* the **torture test** passes under `-race` and `CB_SLOW_TESTS=1` stress.
The stress run covers long-held publishing transactions, dozens of concurrent
publishers, kill-9 of the assigner leader, and the hand-built dual-assigner
exclusion test from 01 §12. Zero loss, zero reorder, no gaps. The exactly-once
consume test passes. Retention drops partitions and force-advances an abandoned
cursor.
**Decision gate: measure end-to-end publish→consume latency. Poll-only, the design
predicts one to two tick intervals (~100–200ms at a 100ms tick). If it lands
materially worse than the tick math, stop and revisit D1 before building on it.
The ~30–80ms end-state target is re-measured at M5 when the NOTIFY accelerator
lands (D17).**

## M2 — stream queue mode: claims, pending, retries, dedup, dead letters

01 §§5–9: range claims with per-claim TTL expiry and extend (D23), delivery of the
pending table (delay; retries are delayed publishes to per-queue retry streams, D21)
and the schedule table (cron), DB-side retry policy (`cb_stream_fail`), the
keep-oldest key rule, dead letters + `Redrive`. Amended before M3 by D27/D28:
claims as atoms with quarantine instead of the crash ladder, the loop-owned
clock, `claim_batch_size` as queue policy, eager retry-stream birth.

*Exit:* claim-crash test (kill consumer mid-range, claim expires, duplicates
bounded by range size);
poison message reaches the dead letter stream after policy attempts with correct backoff timing;
the key rule holds (skip-while-known, `existing` reported);
schedules fire and re-arm honoring `catch_up_policy` (port scheduler tests'
semantic core); throughput benchmark ≥ current `BenchmarkQueueThroughput`
envelope.

*Shipped as written. D35–D38 reopen the failure half — see M3r below; the
claim machinery, pending, schedules, keys and the benchmarks stand.*

## M3 — filtered reads (was: spine — bindings and relays) — DONE 2026-07-12

The bindings/relays design was replaced before building (D29; journey in
`spine-usage-sketch.md`). Shipped instead: queues and cursors take a topic
pattern and a condition, parsed once at registration into precompiled
artifacts and evaluated in SQL; the publish API collapsed to `Publish` +
`PublishMessages` (batch envelopes with per-message topic, headers, key,
delay); `publish_payloads` retired.

*Exit (met):* compiler grammar tables ported from `bindings_test.go`
(`TestCompileTopic`/`TestCompileCondition`); filtered cursor and queue through
the real APIs; late binding replays pre-queue history via `StartPos`;
quarantine republishes only matching messages; a live filtered worker end to
end (delivers exactly the matches, a failed match returns through `sr.*`,
closed position keeps up over undelivered ranges); the batch publish matrix.
Suite green under `-race`.

## M3r — stream revisit: subscriptions, retry rows, dead rows (D35–D38)

The rename and the failure-state move, applied to shipped code. Pre-release,
so `stream/migrations/00001` is edited in place and dev databases reset — the
branch is the experiment. Independent of M4 (disjoint modules, D34); do it
before any further stream work. Three commits, each green:

1. **Rename** queues → subscriptions across tables, functions, arguments and
   the Go API (`cb_stream_subscriptions`, `EnsureSubscription`,
   `SubscriptionOpts`, `ConsumeSubscription`) — mechanical; the suite stays
   green throughout.
2. **The retry table** (01 §5 as amended, §7, §9): `cb_stream_retries`
   replaces `sr.*` and `sd.*` — `cb_stream_fail` becomes
   insert / lease-fenced update / no-op with the one-budget check; quarantine
   materializes rows (`ON CONFLICT DO NOTHING` is the keep-existing-copy
   rule), adoption drops to released-vs-expired with no counter; due rows are
   served by the same `cb_stream_claim` as solo pseudo-claims, minted at
   hand-out, lapse-repaired at claim; the consume loop drops its second
   stream; dead rows replace the dead letter stream (`dead` boolean, redrive
   as reset, dismiss as delete; cursors' `dead_letter` policy writes the same
   row). Drops: the `max_crashes` column and policy, the claim crash count,
   eager twin birth in ensure, the `cb_attempt`/`cb_crash`/`cb_origin_pos`
   headers, retry traffic through pending, the escalation-key grammar.
3. **Grammar collapse** (01 §3): `_cb_valid_stream_name` = `cb_valid_name`,
   partitions plain `cbm_<name>`, the `split_part` base derivations deleted,
   the family codes documented as tombstones.

*Exit:* 01 §12's rewritten failure tests green — duplicate fail fenced by the
lease and collapsed by the primary key, no rows on the main stream; retry
isolation structural (the row carries its owner); fail-then-quarantine yields
one live row, `last_error` says `'silence'` for crashes and the verdict text
for verdicts; release uncharged (no rows, no counts); bystanders of a poison
range end at `attempt = 1`; outage runway = `max_attempts` solo rounds,
backoff-separated; dead rows park, redrive resets and re-delivers to the
owner only — cursors see nothing. The torture, cursor, filter, key, pending
and schedule suites do not move; the throughput benchmark re-run confirms the
happy path did not move. `git grep 'sr\.' stream/` finds only tombstone
comments.

## M4 — one engine: single-execution runs, then spawns (was: tasks, then flows)

There is no separate task engine to build (D31): a task is a run whose single
execution enqueues nothing, kept as sugar (`flow.NewTask`). The engine rides
its own rows (D34) — package `flow` depends on the kernel only, so M4 has **no
stream-layer prerequisite** and does not wait for M3r. The 2026-07-12 "tasks
first" sequencing survives as **M4a / M4b** — the single-execution shape is
the smaller customer of the same machinery, so the schema, the fences and the
claim loop get validated at that scale before spawning exists at all.

**M4a — single-execution runs.** The full flow schema (03 §2 — all seven
tables, nothing deferred) and the M4a function set: `define`, `run`, `claim`
(lease repair included), `start` (conviction check included), `extend`,
`complete` — which **raises** on non-empty `spawns` until M4b — `fail`,
`cancel`, `_cb_flow_convict`, `_cb_flow_backoff`; the flow tick job (schedule
delivery + the run-retention janitor). The worker is flow's own loop on the
D27 skeleton: claim → per step start → handler → complete/fail, extend on
cadence, release-on-shutdown (03 §5). Durable run handle — status, output,
error, cancel — queryable by id *and* by application key (raven's job UI
polls it; ingest hand-rolled the lookup via derived concurrency keys — both
apps demanded exactly this); run-with-dedup-key in the caller's transaction;
scheduled runs via `cb_flow_schedules`, delivered exactly-once per slot in
the re-arm transaction; retries and conviction via the one counter (D38). No
conditions on runs (D32), and no topic-bound triggering: a run fed by events
is a filtered subscription whose handler calls `cb_flow_run` — the
composition rule (README).

*Exit (M4a):* raven's on-demand job shape end to end — run in a transaction
with a dedup key, poll status by app key, read output, cancel; a scheduled
run fires and re-arms; a poison run convicts through both roads — verdict at
fail, silence at start — and the run fails; a graceful shutdown mid-handler
spends a start while a never-started lease lapses free (D38's stated
properties); semantic core of `task_test.go` green against the new engine;
step-to-step latency measured — notify + claim, no assigner leg in the path
(03 §4); throughput ≥ the old `BenchmarkTaskThroughput` envelope.

**M4b — spawns.** The `spawns` branch of `cb_flow_complete` (insert on the
identity tuple, queue stamped from the steps map, `steps_remaining`
accounting), barriers (`OnAllDone`, including a second phase), signals
(`OnSignal` + `cb_flow_signal`, one buffered slot per name), `on_fail` at
conviction under the `failing` status — which thereby fires on hard worker
death too (ingest's `sweep_stuck_deliveries` exists because today's OnFail
doesn't) — the Plan buffer surface, `flow.NewTask` sugar. Port from current
code: the reflection/type utilities (`task.go`, `util.go`); `backoff.go` is a
deliberate per-module copy (D34).

*Exit (M4b):* semantic core of `flow_test.go` green against the new engine —
sequential spawn chains, fan-out + barrier (map-as-pattern), signals including
signal-before-step, cancel, `on_fail` receiving the failed step's name, error
and input, dedup, output resolution — plus 03 §10's list (the `failing`
lifecycle, lease-repair idempotence under racing claims, the remaining
formula's barrier phases); the **wide-map stress test** — hundreds of siblings
completing into one run row, the D30 connection-occupancy watch item, deciding
whether the mitigation ladder's first rung suffices, and watching
`cb_flow_steps` autovacuum (the D34 churn note, 03 §4); and the demo foreign
worker: a ~40-line Python script speaking the six-function contract (03 §7),
extend included, deliberately *slower than the claim TTL* — proving D11, D27
and the lease contract for real. `docs/sql-api.md` rewritten as the
normative contract, with the old-vs-new name table for the transition.

## M5 — NOTIFY wake-up, wire + inbox

The shared notifier arrives here: one LISTEN connection per process, subscribers
registered by channel. Per the plumbing rules it is a **copy** of
`worker_notifier.go`, and the original keeps serving the old worker. Wire's
embedded listener is consolidated onto it in the same milestone. Three consumers
of it land together. First, the **ticker wake accelerator** (D17). Assigner,
cursors and subscriptions, and the periodic jobs attach the notifier to the wake
seam built in M0. They wake on the notifications the SQL has emitted since M1.
The poll interval is demoted to safety net. Second, **flow workers and
`WaitForOutput`** attach to the `cbf_*`/`cbfr_*` channels — whose earliest-due
payload contract is exactly the one `worker_notifier.go` already speaks (03 §1),
emitted since M4. Third, **wire** (04): SSE onto the notifier,
inbox `read_at`/`MarkRead`, retention tiers, `NotifyDurable`. Inbox rows are
written explicitly by handlers holding the identity (D29 confirmed the old
suspicion: interpolated identities were data all along) — no relay kind, no
`identity_from`.

*Exit:* ported `wire_test.go`/`notifications_test.go` green; new seen/read and
retention tests; **the M1 latency gate re-measured with the accelerator — target
~30–80ms publish→consume — and flow step-to-step re-measured (notify + claim)**;
the fallback test: kill the LISTEN connection mid-run
and verify delivery degrades to tick latency with zero loss; an end-to-end demo:
one `Publish` in a transaction → worker consumes, flow runs, browser receives SSE,
inbox row appears — the vision's §4 example, running.

## M6 — cutover and cleanup

Migrate **raven**, the motivating workload, module-by-module. Its task/flow
definitions, index worker, and scheduler usages are the acceptance suite. Port
dashboard + TUI to the new tables; they gain the attempt-grained run history (03 §8).
Move them with the CLI to the nested `cb` module. Write migration notes for any
external 0.x users.

Then **empty the root package**. Everything with a shipped successor is deleted:
`queue.go` (→ subscriptions + one-step runs, M1–M4, D37), `task.go`/`flow.go`/
`worker.go`/`cancel.go`/`complete_early.go`/`handler_opts.go`/`backoff.go`
(→ flow, M4), `scheduler.go` (→ the schedule tables, M2/M4a),
`wire.go`/`wire_token.go`/`notifications.go` (→ wire, M5),
`client.go` (retired — the subpackages are the API), the old
`worker_notifier.go`/`notify.go` (ending the transition's duplication), and the
old `migrations/` + `migrate.go` once nothing reads `cb_q_*`/`cb_t_*`. What
remains is the thin umbrella: `Conn` and shared sentinels, the publish facade
(`Publish`/`PublishMessages`), and migration convenience. This is also where the facade
import cycle is resolved. Both options stay open. Option one: the facade wraps
the SQL functions directly, with no Go import of `stream`. Option two: the
shared types move out of `internal/` and the root imports `stream` freely.

Tag before and after. This is the one hard breaking release, even by 0.x
standards.

*Exit:* raven runs production workloads on the new stack; `git grep cb_q_` finds
nothing outside history.

---

## Reuse map (current file → fate)

| Current | Fate |
|---|---|
| `worker_notifier.go` (the LISTEN machinery — `notifier.go` does not exist; CLAUDE.md is stale), `notify.go` (send helper), wire.go's embedded listener | copied/consolidated into a shared `internal/` notifier package (M5, D17); originals serve the old worker until deleted at M6 |
| `topic_trie.go`, `topic.go` | trie kept for app-side in-process dispatchers (one event against many subscriber rows, webhook-style); the engine's matcher is SQL (D29, M3); `topic.go` retires with the old API (M6) |
| `migrate.go` | parameterized per module (M0) |
| `queue.go` + `cb_q_*` SQL | replaced by subscriptions (feed half, M1–M2) and one-step runs (work half, M4 — D37); `Send/Read/Hide/Delete` API retired |
| `scheduler.go` | dissolved into `cb_stream_schedules` (M2) + `cb_flow_schedules` (M4a) + builder sugar |
| `task.go` reflection, `util.go` | ported into `flow` (M4); `backoff.go` deliberately copied per module (M2/M4a, D34) |
| `flow.go`, `worker.go`, `cancel.go`, `complete_early.go` | semantics ported to the row engine (D30–D38); code largely rewritten (M4) — the claim-loop shape and the NOTIFY wake contract are the parts that carry over most directly |
| `wire.go`, `wire_token.go`, `notifications.go` | ported, extended with read_at (M5) |
| `circuit_breaker.go` | stays client-side, unchanged |
| `dashboard/`, `tui/`, `cmd/` | re-pointed at new tables, moved to `cb` module (M6) |

## Deferred optimizations — after the feature set is complete

- **`cb_stream_publish_messages` hybrid fast path.** The shipped function
  loops over `cb_stream_publish` per element: semantically complete, ~6×
  slower than set-based for plain bulk (measured 2026-07-12: 10k messages,
  loop 319ms ≈ 31k msg/s vs raw set-based insert 51ms ≈ 196k msg/s; ~32µs
  per message — invisible at Revise scale, real for imports and backfills).
  Deliberate: complete the feature set, adjust, then optimize. The
  ready-to-apply design, sketched and reviewed: split the batch at
  validation — elements with `key`/`delay`/`deliver_at` keep the
  per-message path (a key claim and a pending insert are row-at-a-time by
  nature), everything else takes one `INSERT … SELECT` over
  `jsonb_array_elements WITH ORDINALITY`, refs zipped back by input
  position. Pairing invariant: an ordered CTE feeds the insert in input
  order and the id column *default* draws the sequence per consumed row
  (never `nextval` in a target list with `ORDER BY` — values draw before
  the sort), so ords-by-ord zip with ids-by-id exactly. The `cb_` header
  guard moves into the shared validation pass so the set-based branch
  enforces it too. Notify is no reason to hurry: NOTIFY dedups identical
  (channel, payload) pairs per transaction, so the loop's effective load
  already equals one-per-distinct-topic. Trigger to un-defer: the first
  bulk customer (raven import, LDN backfill, seeding pain) or the
  pre-release benchmark pass. The temp-table full-set-based variant stays
  rejected: session state breaks transaction-pooled connections, catalog
  churn, and it accelerates bulk *keyed* publishes — a shape with no
  customer.

- **Stepless-run row collapse.** A one-step run (`NewTask`) writes run +
  step + attempt rows where a bare queue would write one; invisible at the
  stated scale, and the run row *is* the feature (handle, dedup, status). If
  a high-volume fire-and-forget workload ever makes the weight measurable,
  fold the stepless case into fewer rows behind the same API (README, D37).
  Trigger: benchmarks against a real bulk workload, not principle.
- **Batched retry-row claims.** Solo row claims keep charge attribution
  perfect (D38); if incident-scale drains ever hurt, batching becomes a
  subscription policy knob (01 §5's recorded escape hatch) — spent only with
  evidence, because it trades attribution away.

## Standing risks to watch while building

- **Assigner latency under load** (M1 gate). This is the one empirical bet in
  the design — feed layer only: flow edges no longer ride it (D34).
- **Steps-table churn** (M4): claim, start and complete each update the step
  row; watch `cb_flow_steps` autovacuum and index bloat during the wide-map
  stress. Per-flow LIST partitioning is the documented escape hatch (03 §4).
- **Ordinal-update index churn** on high-volume streams: watch bloat on
  `(stream, position)` during M2 benchmarks; `fillfactor` tuning is the first lever.
- **Wide fan-in connection occupancy** (M4b): sibling completions serialize on
  one run-row lock while each holds a pool connection (~1k completions/s per
  run, D30). The wide-map stress test decides; the mitigation ladder — tiny
  completion transaction → batched sibling completion → deferred drain
  detection — is written down in D30 and 03 §4.
- **Scope creep at the DSL** (M4): the cross-language boundary (03 §7) and the
  "patterns, not primitives" list are the two lines most likely to erode under
  porting pressure. The plan documents exist precisely so you notice when you're
  crossing them.
