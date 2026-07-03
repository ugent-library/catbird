# 03 — flow: the engine as a projection

Dynamic tasks and flows as three uses of the substrate: **append** (handlers emit
events), **project** (sharded consumers maintain state rows), **dispatch** (ready
steps go to work streams). Package `flow`, depends on `stream` (accepted hard
dependency, D15). A task is a one-step flow — one model, not two.

## 1. Streams and tables

- One event stream **per flow name**: `flow.<name>` (a LIST partition each —
  isolation and per-flow retention for free). `run_id` is a field, *not* a stream:
  a stream per run would explode the partition count.
- One ready stream per flow: `flow.<name>.ready` (work mode, 01 §5), plus one per
  dedicated step (§7).
- Projection tables — **rows are the view, the log is the truth**:
  `cb_flow_run (run_id, flow, status, input, output, dedup_key, created/finished)` ·
  `cb_flow_step (run_id, step_id, name, status, input, output, attempt,
  idempotency_key UNIQUE)` · `cb_flow_dep (run_id, step_id, needs_step_id,
  satisfied)`. Lookups (`WaitForOutput`, status, dashboard) hit rows only.

## 2. Event catalog

| Event | Emitted by | Payload core |
|---|---|---|
| `run_requested` | `RunFlow` / relay / cron | input, dedup_key |
| `step_spawned` | entry logic; step completion (D10) | step name, input, `after: [step_ids]`, `await_signal?`, idempotency_key |
| `step_started` | worker on claim | step_id, worker, attempt |
| `step_completed` | worker | step_id, output? |
| `step_failed` | worker | step_id, error, attempt |
| `signal` | `SignalFlow` | run_id, step name, payload |
| `run_output_set` | a handler via `p.SetRunOutput` | value |
| `run_canceled` | `CancelFlow` | reason |

`idempotency_key = hash(run_id, parent_step_id, step_name, spawn_index)` — the
projection's `ON CONFLICT DO NOTHING` on it makes duplicate handler executions
(at-least-once dispatch) spawn each child once. This is the engine's entire
idempotency story; handlers only need their *external* effects idempotent.

## 3. Run lifecycle semantics

- A run starts as its **entry step** — `run_requested` projects into a spawn of
  step `start`, whose handler is the flow's entry handler.
- **Completion**: a run completes when every step is terminal
  (completed/failed-exhausted/canceled/skipped) and none is awaiting a signal.
  Since only handlers spawn, a quiesced run can never grow again — auto-completion
  is safe, no explicit "done" call.
- **Failure/OnFail**: when a step exhausts retries, the run fails; if the flow
  declares an OnFail handler, the projection spawns it as a final step with the
  failed step's name, error, and dependency inputs (ports today's OnFail semantics,
  incl. #43's failed-step dependency inputs).
- **Run output** (a semantic change from today, where it was "output of the final
  step" — ill-defined in a dynamic DAG): explicit `p.SetRunOutput(v)` wins;
  otherwise the output of the last step to complete, if it set one; otherwise null.
  `WaitForOutput` keeps its API.
- **Cancel**: `run_canceled` projects to skipping all non-started steps and marking
  the run; running handlers see context cancellation via the worker's NOTIFY
  wakeup, best-effort, as today.
- **Dedup**: `RunFlow` with a dedup key uses the stream dedup table (01 §8) on the
  event stream — the current `ON CONFLICT … WHERE FALSE + UNION ALL` contract
  (return the existing run) is preserved at the API level.

## 4. The projection — sharded, exactly-once (D9)

Not single-threaded (your note — correctly feared). The projection is N ordered
groups (`flow.<name>#proj.0 … proj.N-1`); events route to shard
`hash(run_id) % N` **at append time** via a shard header the group filter matches.
Events for one run are always in one shard: serial per run (required for
correctness), parallel across runs. `N` is per-flow config, default 4; rebalancing
N is a stop-drain-restart operation, not dynamic.

Each shard iteration is one transaction: read events → apply to rows (decrement
deps, flip ready, complete runs) → append `step_ready` work messages to the ready
stream(s) → advance cursor. Same-database = **exactly-once projection**: apply,
dispatch, and ack commit atomically. No idempotency reasoning inside the projector
beyond the spawn conflict key. This transaction is small and fast (row updates);
handlers never run inside it — a slow handler can never hold a projection lock.

Throughput sanity: a shard applying a few hundred row-updates per batch commits in
single-digit ms; four shards absorb the "burst to thousands of events" envelope
with sub-second lag. The floor remains the sequencer tick (01 §2) — flows feel
~50–100ms of step-to-step latency, irrelevant at seconds-to-minutes step duration.

## 5. The Plan DSL (D10)

The vision's example, specified. The key mechanic: **`*Plan` is a buffer.** Handler
mutations accumulate client-side and are submitted **atomically with completion** in
one SQL call — a handler that crashes mid-way submits nothing, and at-least-once
redelivery replays it cleanly against the idempotency keys.

```go
flow.New("process_order").
    OnFail(failHandler).                          // optional
    Entry(func(ctx context.Context, p *flow.Plan, in Order) error {
        p.Spawn("reserve", in.Items)              // → *SpawnRef
        p.Spawn("charge", in.Payment).After("reserve")
        p.Spawn("confirm", in).After("charge").AwaitSignal()  // waits for SignalFlow
        return nil
    }).
    Step("charge", func(ctx context.Context, p *flow.Plan, items Reservation) error {
        out, err := charge(items)
        if err != nil { return err }              // → step_failed, policy retries
        p.SetOutput(out)                          // optional — side-effect-or-error
        return nil
    }, flow.WithRetry(5, flow.FullJitter(time.Second, time.Minute)),  // config → DB (01 §7)
       flow.WithQueue("payments"))                // dedicated ready stream (§7)
```

Surface (complete): `p.Spawn(name, input) *SpawnRef` · `ref.After(names…)` ·
`ref.AwaitSignal()` · `p.SetOutput(v)` / `p.SetRunOutput(v)` ·
`flow.Input[T](p)` / `flow.OutputOf[T](p, "step")` (fetched from rows, typed via
the existing reflection utilities). Dropped as engine concepts, now user patterns:
map (spawn N + a join step `.After` them), conditions (Go `if` before `Spawn`),
generators/reducers, `Optional[T]` (you only spawn what exists — the concept
dissolves). Signals keep first-class support: they are one event type and one
dependency flavor, nearly free on the event model.

What still must be predefined (your note): the flow name and its step handlers
(registered on workers); everything about the DAG's *shape* is runtime. A step name
with no registered handler parks the step as `unhandled` (visible in the dashboard,
re-dispatched when a worker with that handler appears) — this is deploy-order
tolerance, not an error.

## 6. Retries

Step retry policy is columns on a `cb_flow_step_policy` row (flow, step_name → same
schema as 01 §7), written by builder options. `step_failed` projects through the
same `cb_stream_fail` machinery: pending re-dispatch with backoff, exhaustion →
run failure → OnFail. One robustness mechanism across queues and flows; visible to
every language because it executes in SQL.

## 7. Cross-language steps (D11) — your differentiator, scoped

Not a rabbit hole — **provided it stays at the SQL API level**. The contract a
foreign worker implements is four functions (documented in `docs/sql-api.md`, which
becomes the normative spec):

```
cb_flow_claim(queue, worker, batch)         → ready steps (run_id, step_id, name, input jsonb)
cb_flow_complete(run_id, step_id, output?, spawns jsonb, run_output? )
cb_flow_fail(run_id, step_id, error)
cb_flow_signal(flow, run_id, step, payload)
```

`spawns` carries the buffered Plan mutations — so even *dynamic* spawning is fully
available to a Python worker; the DSL is sugar, not capability. `WithQueue("x")`
routes a step to its own ready stream, so a foreign worker subscribes to exactly
the steps it implements and never claims work it can't run (range leases can't
skip; dedicated streams make filtering structural).

The boundary that keeps it from becoming a rabbit hole — explicitly out of scope:
cross-language flow *definition*, typed payload schemas/registries, SDK parity.
The contract is: step name in, JSON in, JSON out, claim/complete/fail/signal.
Hold that line.

## 8. Retention, audit, history

The event log **is** the audit trail — the current engine mutates rows in place
and loses history; this one gets it free, and the dashboard's run detail view can
render the actual event sequence. Flow event partitions drop when: past the
projection cursors **and** every run with events in the partition is terminal
**and** older than the flow's audit window. Long-parked runs (a signal step waiting
weeks) pin their partition — accept it, and add a `max_run_age` policy that cancels
zombie runs rather than complicating retention. Replay/time-travel claims hold
*within retention*, not absolutely — say so in user docs.

## 9. Build checklist

1. Event types + append helpers; shard header at append time.
2. Projection tables DDL; the apply function (one event batch → row deltas +
   ready appends) — pure SQL, the heart of the engine.
3. Sharded projection groups; per-flow ensure (`flow.New(...).Ensure(pool)` creates
   streams, policies, shards idempotently).
4. Worker: claim ready → decode via reflection utils (port from `task.go`) → run
   handler → complete/fail with buffered Plan. Port worker lifecycle/NOTIFY wiring
   from `worker.go` (much of it survives).
5. DSL surface (§5); `RunFlow`/`WaitForOutput`/`SignalFlow`/`CancelFlow` with
   today's signatures where possible.
6. OnFail, dedup, cron-via-pending (`RunEvery`).
7. `docs/sql-api.md`: rewrite as the normative cross-language contract.
8. Tests: port the semantic core of `flow_test.go` (deps, signals, OnFail, cancel,
   dedup, map-as-pattern, output semantics); new: projection crash mid-batch (no
   lost/duplicate ready dispatch), duplicate handler execution spawns once
   (idempotency keys), shard parallelism (two runs progress while one shard is
   artificially stalled), unhandled step parks and resumes.
