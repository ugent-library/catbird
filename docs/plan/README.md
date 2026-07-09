# Implementation plan

This directory turns [../vision.md](../vision.md) into a buildable plan. The vision is
the compass; these documents are the map at street level. Where the two disagree, the
plan wins — the disagreements are listed explicitly in [Amendments](#amendments-to-the-vision)
so vision.md can be corrected once, by hand.

The plan is written to be implemented **manually**. Each document ends in a build
checklist, and [05-milestones.md](05-milestones.md) sequences the whole thing into
verifiable stages. Nothing here is code to paste; everything here is a decision you
should be able to defend after implementing it.

## Document map

| Doc | Covers |
|---|---|
| [01-stream.md](01-stream.md) | The substrate: assigned append-only log, ordered + work consumption, dedup, pending (delay / retry), schedules (cron), dead letters, retention |
| [02-spine.md](02-spine.md) | Publish, topics, bindings, relays — the transactional fanout |
| [03-flow.md](03-flow.md) | The engine: event log, sharded projection, dispatch, Plan DSL, cross-language step contract |
| [04-web.md](04-web.md) | wire: SSE + durable inbox, seen/read semantics, durable push |
| [05-milestones.md](05-milestones.md) | Build order, exit criteria, test checklists, reuse map from current code |

## Delivery guarantees (the headline)

These are the guarantees the whole design is arranged around. Every mechanism in
01–03 exists to make one of these rows true.

| Path | Guarantee | How |
|---|---|---|
| Ordered consumer, effects in the same Postgres | **Exactly-once processing** | Handler runs inside a transaction that also advances the cursor; commit applies effects and ack atomically |
| Ordered consumer, external effects | At-least-once, in order | Cursor advances only on success; idempotency is the handler's job |
| Queue consumer (worker pools) | At-least-once, unordered | Range claims with TTL expiry (D23), extended by the consume loop while work is in progress (D27); retries and crash quarantine via per-queue retry streams (D21, D28); dead letters on exhaustion |
| Relay (spine → materialized destination) | Exactly-once materialization | Relays are ordered consumers writing to the same database |
| wire (ephemeral browser push) | At-most-once | `pg_notify`, fires on commit, no storage |

Exactly-once **external** delivery does not exist in any system; we do not claim it.
What we do claim — and it is a real differentiator of the same-database design — is
that any consumer whose side effects are rows in the same Postgres gets exactly-once
processing with no idempotency reasoning at all. The flow engine's projection and all
spine relays use this path.

## Decision log

Decisions taken in this plan, with the document that details each. Rejected
alternatives are recorded in the detailing document — don't relitigate without new
evidence.

| # | Decision | Where |
|---|---|---|
| D1 | The log is **assigned**: a ticker assigns contiguous, commit-ordered positions. No visibility timeouts, no xid arithmetic, no fixed MVCC watermark | 01 §2 |
| D2 | Two read modes on one log: **cursors** (cursor, exactly-once capable) and **queues** (range claims, at-least-once). No SKIP-LOCKED scan on the hot path. Claims are **atoms** (D28): created whole, re-owned whole, closed whole — repair is quarantine, never boundary mutation | 01 §4–5 |
| D3 | One **delivery job** for everything time-based, two tables: `cb_stream_pending` for one-shot waiting messages (delayed publishes — retries are just delayed publishes to retry streams, D21), `cb_stream_schedules` for cron (config with identity, declared whole by `cb_stream_define_schedule` per D26, survives delivery). The scheduler module dissolves into these | 01 §6 |
| D4 | Retry/backoff policy lives **in the database**, per queue / per step; SQL applies it. Go builders only write config rows | 01 §7, 03 §6 |
| D5 | Dedup = keep-oldest at append (unique key table); keep-newest = coalesce in pending. Appended log rows are immutable | 01 §8 |
| D6 | The dead letter stream is an ordinary stream (`sd.<stream>`, `fd.<flow>`) with failure metadata in headers — `cb_attempts` or `cb_crashes`, whichever exhausted the message (D28). Read it with a cursor: queues exist on plain streams only (D28). Replay = republish | 01 §9 |
| D7 | Retention is **one knob on the stream** (`cb_streams.retention` interval, an **initial value** on `cb_stream_ensure` — birth-only per D26; changing it later is a deliberate op). Stored verbatim: NULL=the default (forever), positive=bounded, `cb_forever()` (a negative sentinel)=explicit forever; `0` or any non-sentinel negative raises. Column is `NOT NULL DEFAULT cb_forever()` (`CHECK` = sentinel or positive) — retention is always a set value, never NULL (which would be indistinguishable from omission). MVP mechanism = batched `DELETE` (`_cb_stream_prune_messages` reads the stream's retention, no per-call override); auto-created streams stamp a creation default via `_cb_stream_ensure` (retry `sr.*` = 7 days handled-history, dead letters `sd.*` = forever until triaged — "drop what's handled, keep what hasn't"); message table partitioned by **stream only**, so no time sub-partitioning to pre-create, no ticker dependency for publishing, and **no force-advance** — position tracking self-heals deleted gaps. Retention is a **hard cap** that can outrun a lagging consumer (data loss by policy). Drop-based retention (per-stream range partitions, engine-derived **fixed** window sized to *retention* not lifetime, O(1) `DROP`, no vacuum) is a **deferred per-stream escape hatch** for busy long-lived streams where `DELETE`/autovacuum churn hurts. Size cap (`retention.bytes` analog) also deferred | 01 §10 |
| D8 | **Fan-out-on-read**: publish writes one row into a topic-keyed root stream; bindings are read-side; destinations are materialized by relays | 02 |
| D9 | The engine is a projection over per-flow event streams, **sharded by `hash(run_id)`** — parallel across runs, serial within a run | 03 §4 |
| D10 | Plan mutations are **buffered client-side and applied atomically with step completion** — one SQL call carries completion + spawns + edges | 03 §5 |
| D11 | Cross-language steps are supported at the **SQL API level only**: claim / extend / complete / fail / signal functions + JSON payloads + per-step dedicated ready streams. No cross-language DSL, no schema registry | 03 §7 |
| D12 | wire and the inbox live in **one package** with shared rendering; durable push ships built-in as a composed helper, each half usable alone | 04 |
| D13 | The inbox stores rows in its **own identity-keyed table**, not on the log (retention semantics are incompatible); it rides the spine, not the substrate | 04 §3 |
| D14 | "Swappable implementation" is dropped as a promise. The stable contract is the **SQL API** (`docs/sql-api.md`); internals evolve via migrations | — |
| D15 | Keep the **catbird umbrella**: one module, subpackages `stream`, `flow`, `wire`; the shared machinery in the root package as the end state (under `internal/` until M6). CLI/TUI/dashboard move to a nested module | below, 05 |
| D16 | Postgres floor: **14+** | 01 §11 |
| D17 | **Poll-first staging**: M0–M4 wake on plain interval ticks (the correctness path); the LISTEN notifier + NOTIFY wake accelerator land at M5 with wire. SQL emits `pg_notify` from day one — only the *listening* is staged | 01 §2, 05 |
| D18 | Consumer state is **two tables** — `cb_stream_cursors` and `cb_stream_queues` — not one with a mode column: one meaning per column, cross-mode misuse structurally impossible, the janitor can't read the wrong retention floor. Name uniqueness across both enforced by ensure under the setup advisory lock | 01 §3 |
| D19 | Publish identity is **one `key`, keep-oldest only**: unknown key → store; known key → skip, returning the existing ref + an `existing` flag, until the dedup window expires. **No replacement, no cancel, no expiry column**: payloads carry identifiers, handlers read current state at delivery and skip what's no longer wanted or too old — at-least-once forces that check anyway. Scheduling: `delay` (relative, DB clock) or `deliver_at` (absolute), mutually exclusive — both set is an error; past-due targets append immediately. Appends notify a channel named after the stream — listeners choose; no notify config | 01 §4, §6, §8 |
| D20 | **Enums for closed or slow-growing sets** (`ref_kind`, `backoff_kind`, `after_max_attempts`, `failure_policy`, `catch_up_policy`, claim `outcome` — validated at the function-parameter boundary, where the cross-language contract lives), **text + CHECK for sets that grow freely** (flow statuses). Two-value sets that stay two-value are plain booleans (claim `closed`, D23). Enum warts accepted knowingly: on PG 12+ `ADD VALUE` runs in an ordinary migration, but the adding transaction cannot also use the new value (backfills need NO TRANSACTION or a second migration); values can be renamed, never dropped | 01 §3 |
| D21 | **Retries are delayed publishes to a per-queue retry stream** `sr.<base>.<queue>` — the retry-topic pattern, same shape as the dead letter stream. `cb_stream_fail` = read the original + `cb_stream_publish(retry stream, …, key => queue:origin:a<attempt>, delay => backoff)`. Deletes: `group_name` on messages, the retry trio + CHECK + partial unique on pending, every read filter — the main log holds only what was published, and a queue's retries are its own by *place*, not by stamp. Duplicate-fail idempotency = the dedup key. Exhaustion publishes to the base stream's dead letters (`sd.<base>` / `fd.<flow>`). Workers claim main + retry streams; ordered consumers never see retries, by construction. Amended by D28: retry streams are **born eagerly** by `ensure_queue` (one birthplace per object — no lazy ensure in fail, missing infrastructure fails loudly), claim one message at a time (`claim_batch_size` CHECK-pinned to 1 — solo attribution), and carry crash quarantine too; keys name their kind (`:a<attempt>` verdicts, `:c<crash>` convictions); escalation policy always reads the base queue row | 01 §6–7 |
| D22 | **Generated-identifier grammar**: humans read table names, machines generate the rest. Stream names are `[code.]<name>[.queue]` — max **3 segments**; one segment = user stream, more = the first segment is a two-char family+kind code: `fe` flow events, `fq` flow ready queue, `fr` flow retry, `fd` flow dead letters, `sr` stream retry, `sd` stream dead letters. Base = segment 2, one `split_part`; validation enforces arity per code; segment count keeps user names unreserved. Channels: `cbs_<stream>` (per-stream wake) and `cb_tick` (delivery-tick wake) — dots verbatim, channels are always quoted. Partition names encode dots as `__`: `cbm__<stream-encoded>__<YYYYMMDD\|dev>` (`sr.orders.mailer` → `cbm__sr__orders__mailer__…`) — injective because user names may not contain `__`. (`$` — legal in identifiers and byte-free — was rejected: it interpolates in perl/PHP/shell string contexts.) Budgets: user names ≤ 20 bytes, composed ≤ 44 (46 encoded) — worst partition 5+46+10 = 61 ≤ 63 | 01 §3 |
| D23 | **Claim liveness by expiry, not heartbeats** (KIP-932's acquisition lock): every claim carries `expires_at`, set from the queue's `claim_ttl`; adoption takes any claim past its deadline; the consume loop calls `cb_stream_extend_claim` on a cadence, but extension is **earned** — between messages, or while a handler is still executing (D27). A bare keep-alive timer would rebuild the heartbeat's blind spot (a wedged-but-alive process keeps heartbeating), so extends fire only while the loop itself is responsive, and a claim that stops being extended always expires. `claim_ttl` is failure-detection latency, not a handler budget (D27). Deletes the workers table, the heartbeat function, and the claim sweeper — failure detection happens at the point of contention, like every other job here. Claim state collapses to a `closed` boolean — a claim is open or closed; "available" is never stored, it is `NOT closed AND expires_at <= now`. TTL resolution is three-level (SQS's scheme): queue `claim_ttl` default → per-call `ttl` override → stored on the claim row, which is what extend renews and adoption inherits. Per-claim terms at `claim_batch_size` 1 make the pgmq emulation exact (read/set_vt/delete = claim/extend/close), except that a crashed message returns with backoff via quarantine (D28), not immediately | 01 §5 |
| D24 | **The substrate says consumer, not worker.** The stream layer only knows something takes messages from a queue — a relay, an indexer, a mailer. "Worker" is the flow engine's word for its handler-running processes; they pass their id as the `consumer` argument when claiming. Substrate names what things are; the engine names what they do | 01 §5, 03 §7 |
| D25 | **Header keys starting with `cb_` are reserved for the engine.** The retry chain's bookkeeping (`cb_attempt`, `cb_origin_position`, `cb_error`), the crash chain's (`cb_crash`, D28), the dead letter report (`cb_queue`, and `cb_attempts` or `cb_crashes`), and flow routing (`cb_run_id`, `cb_shard`) ride in message headers; without a reserved lane a user-supplied `attempt` header would silently corrupt retry arithmetic. `cb_stream_publish` (public) rejects `cb_*` keys; engine code publishes through `_cb_stream_publish`, which does not check — the same wrapper/internal split as ensure. Same family of reservations as `__` in names and `$sys.` in topics | 01 §7 |
| D26 | **Ensure births, define declares, state belongs to the engine.** Columns are identity, state, or config. `ensure_*(identity, initial values)` creates if missing and **never modifies an existing object** — so a concurrent fleet ensuring on boot writes nothing: no dead tuples, no WAL, no trigger fires, no notify storms, physically idempotent by construction. Config changes are deliberate and always **whole declarations**: `define_*` upserts the entire config — the call is the truth, an omitted argument means the column default, never "keep" — guarded so an unchanged declaration writes and notifies nothing (standing rule: **never update a row that has no changes**). No partial/patch updates anywhere in the API; the raw SQL `UPDATE` (validated by table CHECKs) is the ops layer below it. Shipped per entity by need: streams/cursors/queues get ensure (policy is born, then deliberate); schedules get define + delete (templates deploy with code and must converge); `define`/`define_queue` arrive when config-from-code is first needed. Neither verb writes state — seeds apply at birth only; define's `start_at` is the one deliberate state poke, and `next_at` keeps its phase unless the cadence itself changes. Go mirrors this with plain variadic option structs, zero = default (birth and truth semantics have only two states, so no pointers and no sentinels; the lone exception is the start-position seed, where 0 is meaningful and differs from the tail default). Supersedes: D7's NULL=leave-alone retention arg (retention is initial-only in ensure), and `cb_stream_ensure_schedule`'s coalesce-update — which was also latently broken: its INSERT proposed NULL `every` into a NOT NULL column, and NOT NULL is checked before ON CONFLICT can act, so the keep-the-cadence path always errored | 01 §3, §6 |
| D27 | **The consume loop owns the clock** — the visibility window is not the programmer's problem (sizing `claim_ttl` against the slowest handler bred double-execution bugs in pgmq and old catbird: config and code drift apart, nothing enforces their relationship). The loop extends on a `ttl/2` cadence while it is between messages or a handler is still executing; one select loop owns handler-done, extend tick and cancellation, so extends fire only while the loop is responsive — anything wedged stops extending by construction. The outer bound is process mortality: a wedged handler becomes real silence at the next deploy/restart/kill and is counted truthfully as a crash (D28); the loop logs a still-running handler at each extend. Panics are recovered into failed attempts. A per-message budget (`HandlerTimeout`: cancel + truthful timeout fail) is a post-MVP opt-in accelerator and must never get a finite default — a default budget would kill correct hours-long work; defaults protect the innocent before policing the guilty. `claim_ttl` demotes to failure-detection latency. Extend-returns-NULL mid-message cancels the handler at once | 01 §5 |
| D28 | **Claims are atoms; quarantine replaces the crash ladder.** Claims are created whole, re-owned whole, closed whole — boundaries never mutate, so the tiling invariant is structural rather than a proof obligation per branch (the ladder's split shipped with a broken proof: releases counted as crashes, so a rolling deploy could dead-letter a never-attempted message). Adoption has two branches: re-hand whole below `max_crashes` (the `released` flag marks voluntary handback — never a crash; only true expiry bumps the count), or **quarantine** — republish the range's messages individually to the retry stream (`cb_crash`+1 in headers, backoff delay, key `queue:origin:c<crash>`; a message whose failure was already reported keeps its existing retry copy; rows already pruned are simply gone), dead-letter past the limit with `cb_crashes` stamped, `on_fail` honored (one give-up policy). A solo claim's first true expiry quarantines immediately — crashed messages return with backoff. `max_crashes` is a granularity threshold, not an execution threshold: wrong trips cost a deferred retry, not an archive; terminal verdicts rest on solo, per-message evidence (conviction runway ~a quarter hour of continuous fleet death at defaults — a documented policy number). `claim_batch_size` is queue policy, no per-call override (retry queues CHECK-pinned to 1). Queues exist on plain streams only; dotted streams are cursor-read. `cb_stream_fail` gains `consumer` third + an ownership fence (covering claim, not closed, no expiry test, silent no-op). Failure vocabulary: **verdict** (error, panic, future timeout) · **silence** (death) · **handback** (release) — each counted once, in one place. Rejected: pure per-message in-flight claims (per-message write amplification and vacuum churn on the happy path; the hybrid degrades to per-message exactly where trouble lives) | 01 §5 |

## Naming and repo structure

One repository, one primary Go module, the `catbird` name kept as the umbrella —
adoption-one-at-a-time is served by *subpackages*, not by repo fission. Go module
pruning means a wire-only user does not download bubbletea as long as heavy UI
dependencies live in a nested module.

```
github.com/ugent-library/catbird          (module — the library)
├── catbird.go, conn.go, …                 kernel: Conn, topic matching, NOTIFY relay,
│                                          ticker facility, migration runner
├── stream/                                substrate (01) — depends on kernel only
├── flow/                                  engine (03) — depends on stream
├── wire/                                  SSE + inbox (04) — depends on kernel only
└── cb/                                    (nested module) CLI, TUI, dashboard
```

- The root package is the kernel **and** the spine facade: `catbird.Publish`,
  `catbird.Bind` are the five-minute API; they delegate to `stream`.
- Dependency rule: `flow → stream → kernel`, `wire → kernel`. One optional extra:
  wire may import stream, solely to register the `inbox` relay kind (02 §3);
  without it, wire simply has no relay. The engine's hard dependency on the
  substrate is accepted and stated (vision open decision 3). The alternative is a
  second embedded log, which means maintaining the correctness-critical
  assignment machinery twice.
- Table naming: static tables per module (`cb_stream_*`, `cb_flow_*`, `cb_wire_*`),
  one goose version table per module (`cb_stream_migrations`, …) so modules install
  and upgrade independently. No collision with the current `cb_q_*`/`cb_t_*` dynamic
  tables, so old and new can coexist in one database during the transition.
- `go.work` at the repo root ties the nested `cb` module in for development.
- **This diagram is the end state (post-M6).** During the transition the root
  package remains the frozen old API and the shared machinery lives under
  `internal/` (`internal/ticker`, `internal/migrate`, the M5 notifier) —
  otherwise the facade (root → `stream`) and the shared machinery
  (`stream` → root) form an import cycle. Working rules and the M6 resolution:
  05 §repo plumbing.

## What is predefined, what is dynamic

The vision was vague here (your note); this is the explicit answer. "Ensured" means
an idempotent upsert the builder performs at startup — safe to run on every deploy,
never a migration.

| Thing | Predefined? | How |
|---|---|---|
| Stream | ensured | config row + `LIST` partition; `stream.Ensure(name, opts)` |
| Cursor | ensured (or late-bound) | cursor row with an explicit start position (`tail` \| `begin` \| position) |
| Queue | ensured | queue row + policy columns |
| Binding | ensured | row in the bindings table; relays pick it up live |
| Retry/backoff policy | ensured | columns on the queue / step-policy row (D4) |
| Flow | ensured | flow row + registered step handlers on workers |
| Steps of a run | **dynamic** | spawned by handlers at runtime (D10) |
| Signals | **dynamic** | appended events, no declaration |
| Cron schedule | ensured | row in `cb_stream_schedules`, updated by ensure via its PK (D3) |
| Queues/streams per priority class | ensured | just streams — priority is composition, as in the vision |

Nothing at runtime performs DDL except `stream.Ensure` (partition creation) — same
cost profile as today's `CreateQueue`, but one table family instead of four.

## Amendments to the vision

Fold these into vision.md by hand; the plan documents assume them.

1. **"Rebuilt for htmx" → "for server-rendered web apps."** SSE and the inbox are
   library-agnostic; htmx is an example, not a target (§ intro, §1, §2).
2. **"Thousands of users" → "thousands of concurrent users"** (§ non-goals, §5 scale
   envelope).
3. **The ~50ms MVCC watermark is unsound and is replaced** by the assigner (D1).
   Transactional publish means messages can be uncommitted for arbitrarily long; a
   time heuristic loses messages. See 01 §2 for the full argument (§5 perf levers).
4. **"Sub-50ms NOTIFY latency" becomes "~30–80ms end-to-end"** for both consumer
   modes — the price of commit-order correctness, acceptable for the audience (§5).
5. **The projection is not single-threaded** — sharded by `hash(run_id)`, D9 (§6).
6. **"Swappable implementation" is dropped**; the stable contract is the SQL API,
   D14 (§5, §8).
7. **"wire and the inbox share no machinery" is false** — they share rendering,
   tokens, and the poll transport; D12 restates the real claim: independent
   *storage and delivery*, shared *presentation* (§7).
8. **"Baking durable push into wire" moves out of the rejected list**: build it in
   as an optional composed helper, D12 (§ appendix).
9. **The spine is a usage pattern of the substrate** (fan-out-on-read, D8), which
   resolves open decisions 1 and 3 (§9).
10. **The inbox does not ride the substrate's log** — retention needs are
    incompatible: an idle user's cursor would pin the log's retention floor
    forever; D13 (§7).
11. **Add the missing feature dispositions**: signals, cron, retries, OnFail, flow
    output — each has an explicit home now (03, 01 §6–7).
12. **`SignalFlow` changes semantics** in the event model: signals are buffered
    per run until a matching step awaits them, and the synchronous
    `ErrSignalNotDelivered` is retired — the call errors only if the run is
    missing or terminal (03 §3).
