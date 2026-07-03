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
| [01-stream.md](01-stream.md) | The substrate: sequenced append-only log, ordered + work consumption, dedup, pending (delay / retry / coalesce / cron), DLQ, retention |
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
| Work consumer (worker pools) | At-least-once, unordered | Range leases + heartbeats; retries via the pending table; DLQ on exhaustion |
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
| D1 | The log is **sequenced**: a ticker assigns contiguous, commit-ordered ordinals. No visibility timeouts, no xid arithmetic, no fixed MVCC watermark | 01 §2 |
| D2 | Two read modes on one log: **ordered groups** (cursor, exactly-once capable) and **work groups** (range leases, at-least-once). No SKIP-LOCKED scan on the hot path | 01 §4–5 |
| D3 | One **pending** mechanism for delayed delivery, retries, coalescing (keep-newest), and cron. The scheduler module dissolves into it | 01 §6 |
| D4 | Retry/backoff policy lives **in the database**, per consumer group / per step; SQL applies it. Go builders only write config rows | 01 §7, 03 §6 |
| D5 | Dedup = keep-oldest at append (unique key table); keep-newest = coalesce in pending. Appended log rows are immutable | 01 §8 |
| D6 | DLQ is an ordinary stream (`<stream>.dlq`) with failure metadata in headers; replay = republish | 01 §9 |
| D7 | Retention is **policy-driven** per stream: consumer floors ∩ age cap; the age cap wins over lagging cursors (force-advance + loss event) | 01 §10 |
| D8 | **Fan-out-on-read**: publish writes one row into a topic-keyed root stream; bindings are read-side; destinations are materialized by relays | 02 |
| D9 | The engine is a projection over per-flow event streams, **sharded by `hash(run_id)`** — parallel across runs, serial within a run | 03 §4 |
| D10 | Plan mutations are **buffered client-side and applied atomically with step completion** — one SQL call carries completion + spawns + edges | 03 §5 |
| D11 | Cross-language steps are supported at the **SQL API level only**: claim / complete / fail / signal functions + JSON payloads + per-step dedicated ready streams. No cross-language DSL, no schema registry | 03 §7 |
| D12 | wire and the inbox live in **one package** with shared rendering; durable push ships built-in as a composed helper, each half usable alone | 04 |
| D13 | The inbox stores rows in its **own identity-keyed table**, not on the log (retention semantics are incompatible); it rides the spine, not the substrate | 04 §3 |
| D14 | "Swappable implementation" is dropped as a promise. The stable contract is the **SQL API** (`docs/sql-api.md`); internals evolve via migrations | — |
| D15 | Keep the **catbird umbrella**: one module, subpackages `stream`, `flow`, `wire`; kernel in the root package as the end state (in `internal/kernel/` until M6). CLI/TUI/dashboard move to a nested module | below, 05 |
| D16 | Postgres floor: **14+** | 01 §11 |

## Naming and repo structure

One repository, one primary Go module, the `catbird` name kept as the umbrella —
adoption-one-at-a-time is served by *subpackages*, not by repo fission. Go module
pruning means a wire-only user does not download bubbletea as long as heavy UI
dependencies live in a nested module.

```
github.com/ugent-library/catbird          (module — the library)
├── catbird.go, conn.go, notifier.go, …   kernel: Conn, topic matching, NOTIFY relay,
│                                          ticker facility, migration runner
├── stream/                                substrate (01) — depends on kernel only
├── flow/                                  engine (03) — depends on stream
├── wire/                                  SSE + inbox (04) — depends on kernel only
└── cb/                                    (nested module) CLI, TUI, dashboard
```

- The root package is the kernel **and** the spine facade: `catbird.Publish`,
  `catbird.Bind` are the five-minute API; they delegate to `stream`.
- Dependency rule: `flow → stream → kernel`, `wire → kernel`. The engine's hard
  dependency on the substrate is accepted and stated (vision open decision 3); the
  alternative — a second embedded log — means maintaining the correctness-critical
  sequencing machinery twice.
- Table naming: static tables per module (`cb_stream_*`, `cb_flow_*`, `cb_wire_*`),
  one goose version table per module (`cb_stream_migrations`, …) so modules install
  and upgrade independently. No collision with the current `cb_q_*`/`cb_t_*` dynamic
  tables, so old and new can coexist in one database during the transition.
- `go.work` at the repo root ties the nested `cb` module in for development.
- **This diagram is the end state (post-M6).** During the transition the root
  package remains the frozen old API and kernel machinery lives in
  `internal/kernel/` — otherwise the facade (root → `stream`) and the kernel
  (`stream` → root) form an import cycle. Working rules and the M6 resolution:
  05 §repo plumbing.

## What is predefined, what is dynamic

The vision was vague here (your note); this is the explicit answer. "Ensured" means
an idempotent upsert the builder performs at startup — safe to run on every deploy,
never a migration.

| Thing | Predefined? | How |
|---|---|---|
| Stream | ensured | config row + `LIST` partition; `stream.Ensure(name, opts)` |
| Ordered consumer group | ensured (or late-bound) | cursor row with an explicit start position (`tail` \| `begin` \| ordinal) |
| Work consumer group | ensured | group row + policy columns |
| Binding | ensured | row in the bindings table; relays pick it up live |
| Retry/backoff policy | ensured | columns on the group / step-policy row (D4) |
| Flow | ensured | flow row + registered step handlers on workers |
| Steps of a run | **dynamic** | spawned by handlers at runtime (D10) |
| Signals | **dynamic** | appended events, no declaration |
| Cron schedule | ensured | self-rescheduling pending row (D3) |
| Queues/streams per priority class | ensured | just streams — priority is composition, as in the vision |

Nothing at runtime performs DDL except `stream.Ensure` (partition creation) — same
cost profile as today's `CreateQueue`, but one table family instead of four.

## Amendments to the vision

Fold these into vision.md by hand; the plan documents assume them.

1. **"Rebuilt for htmx" → "for server-rendered web apps."** SSE and the inbox are
   library-agnostic; htmx is an example, not a target (§ intro, §1, §2).
2. **"Thousands of users" → "thousands of concurrent users"** (§ non-goals, §5 scale
   envelope).
3. **The ~50ms MVCC watermark is unsound and is replaced** by the sequencer (D1).
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
10. **The inbox does not ride the substrate's log** — retention semantics are
    incompatible (idle users pin cursor-floor retention forever); D13 (§7).
11. **Add the missing feature dispositions**: signals, cron, retries, OnFail, flow
    output — each has an explicit home now (03, 01 §6–7).
