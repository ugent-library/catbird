# Long-term direction: catbird on one log primitive

> The integration model of Phoenix LiveView — an action and all its consequences
> are one transactional event — rebuilt for the Go / htmx / Postgres stack, and
> unbundled into libraries you can adopt one at a time.

This is the **long-term vision**: the *why* (the compass) and the *how* (the map).
It is a direction, not a commitment — catbird is 0.x, and breaking changes are on
the table. It exists to keep scope honest: when a feature or a refactor doesn't serve
the one-sentence vision above, it's out. If pursued, it would be reached
incrementally, never as a big-bang rewrite.

Where the design conversation explored and walked back, only the conclusion is
recorded — see the [appendix](#appendix--rejected-and-why-dont-relitigate) for what
was rejected and why, so we don't relitigate it.

---

## 1. The why

### The problem

Small teams shipping real-time web apps want the LiveView feeling — click a button,
and the database write, the background work it kicks off, and the update pushed to
the browser all happen as one coherent event. Today they get there by standing up a
queue, a scheduler, a pub/sub service, and a websocket layer, then wiring them
together across process boundaries. That's a lot of infrastructure and a lot of glue
for a team that just has one Postgres database and wants to keep it that way.

### The vision

Postgres is already the synchronization point. A single `Publish` inside a
transaction fans out — atomically — to everything that cares: worker pools,
orchestrations, and browser pushes. Nothing fires until the database agrees the
transaction committed. No extra services, no extensions, no second datastore.

The pieces are independent libraries, not a framework. You can take the SSE layer
alone, or the flow engine alone, or the queue alone. They become more than the sum
of their parts when they share one spine — but you are never forced to adopt the
whole thing to use one part.

### The bet

**A minimal shared spine — transactional publish + topic bindings — is enough to
get framework-grade cohesion out of independently-useful libraries.**

If that holds: you get the LiveView experience without the lock-in, each piece earns
its own audience, and a small team gets all of it on infrastructure they already
run. If the spine has to know too much about each consumer to make the magic work,
it collapses back into a bundle. So the one thing to be ruthless about is keeping the
spine thin. Everything else is swappable; that isn't.

### The philosophy: subtraction

Every hard design call points the same way — **remove the leaky abstraction so
developers think in domain terms, not plumbing.**

- Visibility timeouts → **cursors.** "How long is my handler? What's my `hide_for`?"
  is not a domain concern.
- Durable-execution replay semantics → **just write idempotent code.**
  At-least-once, no deterministic-replay magic, no wrapped side effects.
- Mandatory step outputs → **side-effect-or-error.** Most real work is "do this
  thing, succeed or fail," not "compute a value."
- Engine primitives (map / generator / reducer / condition) → **patterns in user
  code**, not concepts the engine has to own.

This is the differentiator. The Temporal / Inngest axis competes on power; this
competes on *not having to hold much in your head*.

And subtraction is how we get **more powerful *and* simpler at once**: one general
primitive — an append-only log read by cursor — replaces several special-cased
mechanisms. The queues, the flow engine's dispatch and state, and the durable inbox
all become consumers on that one log. Power per concept goes up; concepts go down.

### Non-goals

Stated plainly, so they can be pointed at:

- **No durable execution.** No deterministic replay, no handler memoization.
  Handlers are idempotent code that may run more than once.
- **No world scale.** The target is single-DB: thousands of users, hundreds of
  events/sec steady with bursts. Modest, not Kafka.
- **No infrastructure beyond Postgres.** No extensions, no
  `shared_preload_libraries`, no second datastore, no required sidecar. A ticker can
  run in the app binary.
- **Not a framework.** No required bundle, no inversion of your app's control flow.
  Libraries you call, not a platform you live inside.

### Who it's for

Small teams building real-time, server-rendered (htmx-style) web apps in Go who
already run Postgres and want to keep their infrastructure footprint at exactly one
box. Not for teams who need cross-datacenter durability guarantees, exactly-once
delivery, or queue throughput measured in the hundreds of thousands per second.

---

## 2. The product, in one model

> A Postgres-backed event bus with a few subscriber types — **queues** (message
> workers), **flows** (orchestrations), and **wire** (browser pushes). One publish,
> transactionally, fans to all of them based on topic bindings.

This is the LiveView integration model — an action and all its consequences are one
transactional event — rebuilt for Go / htmx / Postgres, and **unbundled** into
libraries you can adopt one at a time. The value is in the *routing*, not in owning
any one implementation.

A thin kernel, a shared event spine, and three primitives that stand alone:

- **spine** — transactional, topic-routed `Publish` + bindings. The one piece that
  must stay cohesive. The integration value lives here.
- **substrate** — a low-ceremony Postgres queue/stream with a cursor mental model
  (pgq-*inspired*, our own): partition-`DROP` retention, ack-by-cursor,
  competing-consumer claims layered on only when needed.
- **engine** — a dynamic `*Plan` DSL for tasks and flows: spawn steps at runtime,
  output optional, no durable-execution subtleties.
- **catbird (wire + inbox)** — async web delivery: ephemeral SSE/pub-sub and a
  durable per-identity inbox, as two independent primitives.

---

## 3. The shape

Three standalone primitives over a shared event spine and a thin kernel:

```
                          one transactional publish
                                     │
   ┌─────────────────────────────────▼──────────────────────────────────┐
   │  SPINE — topic-routed Publish + bindings   (the one cohesive piece)  │
   └──┬──────────────────────┬───────────────────────┬───────────────────┘
      │ bind + cursor        │ bind + cursor          │ bind + cursor
   ┌──▼────────────┐   ┌─────▼──────────┐      ┌──────▼───────────────┐
   │  SUBSTRATE    │   │  ENGINE        │      │  CATBIRD             │
   │  queues/      │   │  tasks/flows   │      │  wire (ephemeral) +  │
   │  streams      │   │  Plan DSL      │      │  durable inbox       │
   │  (cursor)     │   │  (no durable   │      │                      │
   │               │   │   execution)   │      │                      │
   └───────────────┘   └────────────────┘      └──────────────────────┘
        each box also has a DIRECT API, usable with the spine deleted
                                     │
   ┌─────────────────────────────────▼──────────────────────────────────┐
   │  KERNEL — Conn · per-module migrations · NOTIFY relay · topic-trie   │
   └─────────────────────────────────────────────────────────────────────┘
                                  PostgreSQL
```

**The independence rule:** each primitive is installable and useful on its own; its
tables and SQL are its own. **The one exception** is the spine — the transactional
fanout is *the* feature, and it is the only thing that must stay cohesive across the
modules. Split along ownership/storage lines; never split the spine. (Corollary:
don't extract a piece merely to avoid a hard call — the code still exists and still
needs maintaining.)

---

## 4. The integration model

Bindings are declared once. A single `Publish` inside the caller's transaction fans
into every bound stream atomically, and consumers react after commit:

```go
// declared once — pure routing; the integration value lives here
spine.Bind("order.placed", toQueue("fulfilment"))   // → a worker pool
spine.Bind("order.placed", toFlow("process_order")) // → the engine
spine.Bind("order.placed", toWire("user."+id))      // → live browser push
spine.Bind("order.placed", toInbox(id))             // → durable notification

// in a request handler — ONE publish, atomic with your own writes
tx := pool.Begin(ctx)
repo.SaveOrder(tx, order)
spine.Publish(tx, "order.placed", order)            // fans into all bound streams
tx.Commit(ctx)                                       // nothing fires until commit
```

Every subscriber type is "a consumer with a callback" — a cursor plus a function
(`RunFlow`, `wire.Notify`, `NotifyDurable`, or your own handler). **Wire only pushes
on commit**, which is the LiveView property that the UI never shows state the
database hasn't agreed to.

---

## 5. Substrate — queues & streams

### The bet: one primitive, more powerful *and* simpler

The goal is a substrate that is at once **more powerful and simpler than the queue we
have today** — and that is only achievable one way: replace several special-cased
mechanisms with one more-general primitive. That primitive is *an ordered,
partitioned, append-only log read by cursor, with competing-consumer claims layered
on only when needed.*

**The power comes from the generality of the log.** Today's impl gives one thing: a
queue you drain. The log gives queues *and* streams from the same substrate — replay,
multiple independent consumers on one stream, fork-style consumers, late binding,
time-travel. And it *unifies* what is bespoke today: generic queues, the engine's
ready-work dispatch, and the durable inbox all become "a consumer on a stream." One
primitive instead of `cb_q_*` + separate task/flow/map claim logic + a separate
inbox.

**The simplicity comes from decomplecting.** Storage, delivery, visibility, priority,
and routing are braided together today, a column and a code path each. The log
un-braids them: storage = the append-only partitioned log; delivery = cursor
(+ optional claim); routing = publish/bind; and *features become compositions, not
columns* — priority → a stream per class; delayed delivery → a `pending` table +
sweeper; dedup → a unique index. Whole mechanisms vanish: ack-by-cursor removes
per-message delete/hide/visibility; retention-by-`DROP` removes the GC-delete +
vacuum-tuning machinery. Code removed *and* capability added.

Two honest caveats:

- **A simpler core can shove work onto users.** "Priority? Run a stream per class" is
  simpler for the substrate and more work for the caller — unless the conveniences
  come back as thin, opt-in helpers above the simple core. Discipline: simple
  powerful substrate at the bottom, ergonomic sugar in a layer you can ignore.
- **Simpler ≠ fewer lines.** The substrate file (~600–1200 lines) is bigger than
  today's ~300-line queue. The yardstick is *concepts a developer must hold* and
  *bespoke mechanisms in the system* — one primitive replaces several, so the whole
  system gets simpler even as that one file grows. Measure in concept-count, not LOC.

The guardrail against over-unification: **don't force everything onto the log.**
Engine *state* stays rows (a projection, not a log); wire *ephemeral* stays
`pg_notify` (no storage needed). One primitive for the log/delivery concern —
deliberately not for state or ephemeral signaling.

### Cursor model, not visibility timeout

The load-bearing decision. Visibility timeouts are a leaky abstraction: every
consumer author ends up reasoning about "how long is my handler / what's my
`hide_for` / what if I exceed it." That is not a domain concern. The cursor model is
mentally simpler and familiar from Kafka and event logs: a consumer is at position N;
the next read returns messages after N; acking is advancing the cursor. No timer to
race, no double-delivery via timeout expiry. This is the differentiator from pgmq, and
it is the *real* reason for the substrate — not throughput.

### Build our own, pgq-*inspired* — don't depend on pgque

pgque is ~2500 lines of PL/pgSQL: depending on it is closer to "depend on a PL/pgSQL
extension" than "depend on a small library." Our row-based queue today is a few
hundred lines we understand. Conclusion: take the *ideas* from PgQ (snapshot-bounded
batches, cursor-tracked consumers, table rotation, a ticker) and implement them
embedded. Expect a **modest** scaling improvement (better p99 under burst, similar
median/peak); the cursor *mental model* is the gain, not raw throughput.

Keep the substrate **swappable via a clean API**: cursor semantics are the API,
SKIP-LOCKED-over-small-tables is the implementation. If we ever hit a wall, swap the
implementation without touching the spine, engine, or catbird.

### Partition-based storage, retention by DROP

One logical `queue_message` table, partitioned `LIST (queue_name)` then
`RANGE (created_at)`. Benefits of queue-per-table (physical isolation, per-queue
autovacuum, per-queue retention) with a unified publish path and partition routing.
**Ack does not `DELETE` messages** — it advances the cursor. Retention is a janitor
that **`DROP`s** partitions whose max id is below every consumer's cursor: instant, no
dead tuples, no vacuum churn. This is the structural answer to the bloat that bites
pgmq-style designs under sustained delete load.

### Two consumer modes — complexity scales with use

- **Single-reader consumer** (the spine's fanout subscribers): just a row in
  `queue_cursor`. No claim, no heartbeat, no batches. Each consumer walks forward
  independently.
- **Competing consumers** (worker pools, flow-step dispatch, map fanout): a small
  `queue_claim` table layered on top. Workers claim a range with `SELECT … FOR UPDATE
  SKIP LOCKED` over a *tiny* table; the cursor advances to the highest contiguous
  completed point.

### Claim liveness — heartbeat, decoupled from handlers

Rejected along the way: holding a transaction open as the claim (long handlers pin
the xmin horizon and starve autovacuum database-wide) and advisory locks as the claim
(a bounded shared-memory resource that demands monitoring and tuning). Landed on: a
small claim table with SKIP LOCKED, plus a **per-worker** heartbeat in an `UNLOGGED
worker_heartbeat` table (one write per worker per interval, not per claim — keeps it
from being chatty). A sweeper resets claims whose worker has gone stale.

Crucially this is **not** the visibility timeout in disguise: the heartbeat is
decoupled from handler duration. A 5-minute handler keeps getting refreshed by a
background goroutine; staleness only triggers when the worker process is actually
dead. Handler authors write `func(ctx, msg) error` and never see a `hide_for` or an
`ExtendLease`.

### Table shape (minimum viable)

`queue` (config) · `queue_message` (partitioned, PK `(queue_name, id)`, `topic`,
`payload`, `created_at` via `clock_timestamp()`, **no status column** — status is
implicit) · `queue_cursor` · `queue_claim` · `queue_retry` · `queue_dlq` ·
`worker_heartbeat` (unlogged). Roughly 200–300 lines DDL, 500–800 lines PL/pgSQL.

Performance levers: NOTIFY for wakeups (sub-50ms latency, no polling floor); BRIN
indexes on the monotonic message table; never batch messages younger than ~50ms (MVCC
visibility watermark for uncommitted inserts); aggressive per-partition autovacuum on
the small hot tables; bulk cursor advance.

### Inherent vs. inherited (pgmq-ism)

| Inherent — keep | pgmq-ism — drop or relocate |
|---|---|
| dedup (unique constraint on a dedup table) | per-message visibility timeout |
| scheduled delivery (a `pending` table + sweeper) | per-message hide / per-message delete |
| expiry (GC concern, shape-independent) | per-message priority within a stream → use a stream per priority class |

### Falls out nearly free

Independent consumers, replay (cursor reset / reading historical positions),
fork-style consumers (different filters on one stream), late binding of consumers,
time-travel debugging. Not free: strict cross-queue ordering, exactly-once, bounded
write latency under arbitrary load — explicit non-goals.

### Scale envelope

Target: single-DB, thousands to tens of thousands of users, hundreds of events/sec
steady with bursts to thousands, dozens of queues and consumers. The motivating real
workload (an institutional repository with a few thousand concurrent backoffice
users) is **not** a queue-throughput workload — the queue carries consequences
(emails, indexers, notifications). SKIP LOCKED with sane hygiene handles this
comfortably; partition-DROP retention is what keeps it healthy as volume grows.

---

## 6. Engine — dynamic tasks & flows

The genuinely novel piece, and the one whose soul is simplicity.

### The `*Plan` DSL

A handler receives a `*Plan` and mutates it — spawn steps, declare edges, complete.
The DAG grows at runtime; there is no precompiled plan.

```go
flow.Step("record_signup", func(ctx context.Context, p *catbird.Plan, user User) error {
    p.Spawn("send_welcome_email", EmailInput{To: user.Email})
    p.Spawn("log_event", SignupEvent{UserID: user.ID})
    p.Spawn("notify_team", user).After("send_welcome_email", "log_event")
    return nil
})
```

### Output is optional — side-effect-or-error

A key lesson from real use: "everything has an output" is principled but wrong. Most
operational work is *do this thing, succeed or fail* — sending email, writing to
storage, calling a webhook. A step completes (with or without output) or it fails.
`SetOutput` is the opt-in for steps that produce a value; `p.OutputOf[T]("step")`
reads one upstream; a plain `.After("step")` edge expresses "just needs upstream to
have succeeded."

### No durable execution

At-least-once, full stop. A step may run more than once; **idempotency is the handler
author's job.** No deterministic replay, no handler memoization, no wrapped side
effects. Rows for state, code for logic. This is the deliberate trade against the
Temporal/Inngest axis: less power, far less to hold in your head.

### Patterns, not primitives

map / generator / reducer / condition / early-completion are **patterns in user
code**, not engine concepts. A map step is a handler that spawns N children plus a
join. A condition is a handler choosing what to spawn. This collapses most of the
current engine's surface area.

### The engine is a projection over the log

This is the keystone of the "one primitive" story: the *dynamic* flow direction lets
the engine graft directly onto the substrate's log, instead of being a separate
machine with its own claim/visibility logic.

A dynamic DAG is naturally append-driven — the flow grows because a handler *appends
events* (`step_spawned`, `step_completed`, `step_failed`). So the engine is three uses
of the same primitive:

- **append** — handlers write events to a per-run stream. A 200-child spawn is one
  bulk append (efficient precisely because we own the substrate — the case that would
  be awkward on pgque).
- **project** — a single cursor consumer reads event batches and maintains the state
  rows: decrement deps, mark dependents ready.
- **dispatch** — newly-ready steps land on a ready-work stream that competing workers
  claim.

**No-durable-execution is what keeps this cheap.** Events are *state-deltas*, not a
basis for replaying handler code. The projection is "apply deltas to rows," not
"deterministically re-run side effects" — event-sourcing's wins without its mental
load.

This reconciles with "engine state stays rows": the rows (`flow_run`, `step_run` with
`idempotency_key = hash(flow_run_id, parent_step, logical_name, spawn_index)` for
spawn dedup, `step_dep`, `step_set_dep`) **remain — but as a projection of the log**,
a materialized view for fast lookups (`WaitForOutput`, status, dep checks). What
dissolves is the bespoke part of today's engine (`cb_claim_steps` /
`cb_claim_map_tasks`, step-level visibility). The log is the engine; the rows are the
view. **Handlers run outside the database transaction** — a slow handler must never
hold a row lock.

Power that falls out for free: the event log *is* the audit trail (today's
row-mutation engine loses history on update); flow runs get replay / time-travel; new
projections (metrics, audit) can be added without touching the engine. The one honest
cost: the projection is a single-threaded consumer with a sub-second tick-latency
floor — fine for flows (steps take seconds-to-minutes), and the same primitive still
offers the direct low-latency claim path for queues that need it. One primitive, two
consumption styles.

---

## 7. catbird — async web delivery

The browser-facing module: **ephemeral + durable**, as two independent primitives.

- **wire stays ephemeral** — SSE, live pub/sub, presence, push-only-on-commit.
  Durable push is deliberately *not* baked into wire; doing so would turn an SSE layer
  into "messaging + scheduling + inbox + SSE."
- **the durable inbox** is its own per-identity, cursor-addressed store, read by
  polling. Durable push to a user is therefore composed, not built-in: persist to the
  inbox, then (optionally) notify wire to re-pull. The two share no machinery and each
  works with the other deleted.

The inbox is, in shape, an **identity-partitioned durable stream** — the same
primitive the substrate offers. Standalone, catbird ships its own minimal store; if
the substrate is also present, the inbox can ride on it. Resonance, not coupling.

---

## 8. Cross-cutting principles

- **Subtraction.** Remove the leaky abstraction so developers think in domain terms
  (cursor over visibility timeout; idempotent code over replay; side-effect over
  mandatory output; patterns over primitives).
- **Transactional fanout is the spine.** Keep it thin; keep it cohesive.
- **Postgres-only.** No extensions, no second datastore, no required sidecar (a ticker
  can live in the app binary).
- **Clean API over swappable implementation.** Especially the substrate.
- **Per-module migrations.** Independent install and upgrade cadence.

---

## 9. Open decisions

1. **Where the spine lives** — a capability of the substrate, or its own thin `bind`
   module above it. The conversation drew it as a separate layer; that is probably
   right if the substrate is to stay a pure queue.
2. **Cross-module consumer contract** — kernel-defined interfaces (dependency
   inversion: a module calls `Runner`/`DurableNotifier`, the app injects the concrete
   other module) vs. thin adapter packages that import two cores.
3. **Engine on the substrate** — the lean is for the engine to *be* a projection over
   the substrate's log (append / project / dispatch), not to keep its own rotating
   event tables. Open question is how much this couples the two: a standalone engine
   still needs a log, so either it embeds a minimal one or the substrate becomes a hard
   dependency of the engine (the one place the independence rule may bend).
4. **Repo layout** — one `go.work` monorepo (independent module paths, shared CI, easy
   cross-module refactors) vs. separate repos.
5. **Naming** — distinct identities (catbird is now one peer, not the umbrella) vs. a
   `catbird/*` family.

---

## Appendix — rejected, and why (don't relitigate)

- **Depending on pgque** — ~2500 lines, a large commitment; our scale doesn't need it
  and the cursor mental model is achievable in ~600–1200 of our own lines.
- **Held transaction as the claim** — pins the xmin horizon, starves database-wide
  autovacuum under long handlers.
- **Advisory locks as the claim** — bounded shared-memory resource; needs monitoring
  and tuning for no real benefit at our scale; small-table SKIP LOCKED is simpler.
- **Visibility timeouts** — leak handler-duration reasoning into every consumer.
- **Durable execution / deterministic replay** — too much mental load; against the
  simplicity niche.
- **Mandatory step outputs** — don't match operational (side-effect) work.
- **Baking durable push into wire** — turns an ephemeral SSE layer into a
  workflow/messaging engine.
- **Splitting modules to avoid a hard call** — the code still exists and still needs
  maintaining; split only along genuine audience/ownership lines.
