# 05 — milestones

Sequenced for manual implementation: each milestone is independently verifiable,
ships behind the previous one's tests, and front-loads the riskiest bet. The new
code grows in subpackages beside the current implementation — the existing top-level
API keeps working (and keeps raven running) until M6. Same repo, same test harness
(`./scripts/test.sh`), same docker-compose database.

**Ordering rationale:** the sequencer (M1) is the load-bearing wall — if the
commit-order design has a flaw, everything above it is wasted, so it goes first and
gets the torture test before anything else exists. The engine (M4) is the most
work; it lands only on a substrate already proven under M2/M3's tests.

---

## Repo plumbing — how to work

Additive on main; no long-lived rewrite branch, no `/v2` module (0.x allows the one
breaking release, at M6). Ground rules:

- **Tag first.** Tag the current state before M0 so raven has a stable pin. Every
  milestone PR then lands on main green without raven noticing — all new code is
  new import paths.
- **The old code is frozen** — bugfixes only. The first PR adds a note to
  `CLAUDE.md` ("rewrite in progress: new code lives in `stream/`/`flow/`/`wire/`
  per `docs/plan/`; the top-level API is frozen") so nobody — including future
  sessions — improves code scheduled for deletion.
- **Copy, don't rework, shared machinery.** The kernel notifier starts as a *copy*
  of `notifier.go`/`worker_notifier.go` and evolves independently; the originals
  keep serving the old worker untouched. Two notifiers coexist until M6 deletes
  one. Temporary duplication is cheaper than destabilizing what raven runs on.
- **Kernel lives in `internal/kernel/` during the transition**, not the root
  package (D15 describes the *end state*). The trap this avoids: the final layout
  has the root as kernel + facade, but `catbird.Publish` delegating to `stream`
  while `stream` imports the root for kernel types is an import cycle. Until M6
  the root simply stays the old API; subpackages import `internal/kernel` plus the
  root's existing `Conn`. The cycle is resolved once, at M6 (see there).
- **Tests ride the existing rails.** `./scripts/test.sh` already runs
  `go test ./...`, so new subpackages are picked up automatically and the
  drop-and-recreate of `cb_tst` covers them. Each new package gets its own
  `TestMain` + `sync.Once` setup (the `catbird_test.go` pattern) running its own
  migrations. Because `go test ./...` runs packages in parallel processes, the
  kernel migration runner takes an advisory lock — already this codebase's setup
  convention.
- **Migrations are per module and additive.** Each subpackage embeds its own
  `migrations/` FS with its own goose table (`cb_stream_migrations`, …); the
  existing `migrate.go` and `migrations/` are untouched until M6. New names
  (`cb_stream_*`, `cb_stream_publish`) cannot collide with the old ones, so old
  and new suites share one database.
- **Torture test first, sequencer second.** M1's acceptance test is written
  before the sequencer exists — failing on disk — with the stress variant behind
  `CB_SLOW_TESTS` (the `requireSlowTests()` gate) and a fast deterministic variant
  in the default run.
- **No `go.work` until M6** — only the nested `cb` module needs it.

Suggested first PRs, sized for momentum: (1) `internal/kernel/ticker.go` + tests —
small, self-contained, zero schema decisions; (2) the notifier copy + the
parameterized migration runner; (3) `stream/` DDL + `cb_stream_publish` + the
failing torture test; (4) the sequencer — torture test green, M1 latency gate
measured.

---

## M0 — kernel (small)

Create `internal/kernel/`: the notifier as a shared per-process LISTEN relay with
subscriber registration (a **copy** of `notifier.go` + `worker_notifier.go` per the
plumbing rules — the originals keep serving the old worker), a **ticker facility**
(register periodic + wake-on-notify jobs with leader election — sequencer,
sweepers, janitors, relays all become registrations), a per-module goose runner
(by version table + migration FS, advisory-locked). `Conn` stays where it is
(root, public); `topic_trie.go` untouched.

*Exit:* two modules can share one process's LISTEN connection; a toy ticker job
fires on notify and on fallback interval; leader election yields exactly one runner
across N processes.

## M1 — stream core: publish, sequencer, ordered consume (the wall)

01 §§2–4, 10: DDL, `cb_stream_publish`, the sequencer, ordered groups with both Go
consume shapes, ensure/start-positions, retention janitor with age-cap
force-advance.

*Exit:* the **torture test** passes under `-race` and `CB_SLOW_TESTS=1` stress
(long-held publishing transactions, dozens of concurrent publishers, kill-9 of the
sequencer leader — zero loss, zero reorder, no gaps); exactly-once consume test
passes; retention drops partitions and force-advances an abandoned cursor.
**Decision gate: measure end-to-end publish→consume latency; the design predicts
~30–80ms — if it lands materially worse, stop and revisit D1's pacing before
building on it.**

## M2 — stream work mode: leases, pending, retries, dedup, DLQ

01 §§5–9: range leases + heartbeats + sweeper, the pending table (delay, retry,
coalesce, cron), DB-side retry policy (`cb_stream_fail`), dedup, DLQ + `Redrive`.

*Exit:* lease-crash test (kill worker mid-range, duplicates bounded by range size);
poison message reaches DLQ after policy attempts with correct backoff timing;
coalesce delivers newest-once; cron rows fire and reschedule honoring
`catch_up_policy` (port scheduler tests' semantic core); throughput benchmark ≥
current `BenchmarkQueueThroughput` envelope.

## M3 — spine: bindings and relays

02: `bus` stream, `catbird.Publish` facade, binding rows, relay runner with the
`stream` destination kind, NOTIFY hook for wire.

*Exit:* publish-then-rollback delivers nothing (the LiveView property, as a test);
relay crash mid-batch → no duplicates in destinations; late binding replays
history; `bindings_test.go` semantics ported.

## M4 — flow engine (the big one)

03: event streams, projection tables + apply function, sharded projectors, ready
dispatch, worker claim/execute/complete, the Plan DSL, signals, OnFail, dedup,
retries via M2 machinery, `RunEvery` via pending-cron. Port from current code:
reflection/type utilities (`task.go`, `util.go`), worker lifecycle (`worker.go`),
backoff strategies (`backoff.go`).

*Exit:* semantic core of `flow_test.go` + `task_test.go` green against the new
engine (deps, signals, cancel, OnFail with dependency inputs, dedup, outputs,
map-as-pattern); new projection tests (03 §9); a demo foreign worker (a ~50-line
Python script speaking the four SQL functions) claims and completes a dedicated
step — proving D11 for real. `docs/sql-api.md` rewritten as the normative contract.

## M5 — wire + inbox

04: port onto kernel notifier, inbox `read_at`/`MarkRead`, retention tiers,
`NotifyDurable`, `inbox` relay kind.

*Exit:* ported `wire_test.go`/`notifications_test.go` green; new seen/read and
retention tests; an end-to-end demo: one `Publish` in a transaction → worker
consumes, flow runs, browser receives SSE, inbox row appears — the vision's §4
example, running.

## M6 — cutover and cleanup

Migrate **raven** (the motivating workload) module-by-module — its task/flow
definitions, index worker, and scheduler usages are the acceptance suite; port
dashboard + TUI to the new tables (they gain the event-log run history, 03 §8) and
move them with the CLI to the nested `cb` module; write migration notes for any
external 0.x users.

Then **empty the root package**. Everything with a shipped successor is deleted:
`queue.go` (→ stream, M1–M2), `task.go`/`flow.go`/`worker.go`/`cancel.go`/
`complete_early.go`/`handler_opts.go`/`backoff.go` (→ flow, M4), `scheduler.go`
(→ pending-cron, M2), `wire.go`/`wire_token.go`/`notifications.go` (→ wire, M5),
`client.go` (retired — the subpackages are the API), the old
`notifier.go`/`worker_notifier.go` (ending the transition's duplication), and the
old `migrations/` + `migrate.go` once nothing reads `cb_q_*`/`cb_t_*`. What
remains is the thin umbrella: `Conn` and shared sentinels, the spine facade
(`Publish`/`Bind`), and migration convenience. This is also where the facade
import cycle is resolved, with both options open: the facade wraps the SQL
functions directly (no Go import of `stream`), or kernel types move out of
`internal/kernel` and the root imports `stream` freely.

Tag before and after — this is the one hard breaking release, even by 0.x
standards.

*Exit:* raven runs production workloads on the new stack; `git grep cb_q_` finds
nothing outside history.

---

## Reuse map (current file → fate)

| Current | Fate |
|---|---|
| `notifier.go`, `worker_notifier.go` | copied into `internal/kernel` (M0); originals serve the old worker until deleted at M6 |
| `topic_trie.go`, `topic.go` | kept verbatim, used by relays (M3) |
| `migrate.go` | parameterized per module (M0) |
| `queue.go` + `cb_q_*` SQL | replaced by `stream` (M1–M2); `Send/Read/Hide/Delete` API retired |
| `scheduler.go` | dissolved into pending-cron (M2) + builder sugar |
| `task.go` reflection, `util.go`, `backoff.go` | ported into `flow` / `stream` (M4/M2) |
| `flow.go`, `worker.go`, `cancel.go`, `complete_early.go` | semantics ported to event engine; code largely rewritten (M4) |
| `wire.go`, `wire_token.go`, `notifications.go` | ported, extended with read_at (M5) |
| `circuit_breaker.go` | stays client-side, unchanged |
| `dashboard/`, `tui/`, `cmd/` | re-pointed at new tables, moved to `cb` module (M6) |

## Standing risks to watch while building

- **Sequencer latency under load** (M1 gate) — the one empirical bet in the design.
- **Ordinal-update index churn** on high-volume streams: watch bloat on
  `(stream, ordinal)` during M2 benchmarks; `fillfactor` tuning is the first lever.
- **Projection lag** under 200-child spawn bursts (M4): the shard count is the
  lever; measure before raising the default.
- **Scope creep at the DSL** (M4): the cross-language boundary (03 §7) and the
  "patterns, not primitives" list are the two lines most likely to erode under
  porting pressure. The plan documents exist precisely so you notice when you're
  crossing them.
