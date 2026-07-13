# 03 — flow: the engine on rows

**One concept.** A run is a group of executions, and every execution may
atomically enqueue follow-on steps when it completes — "a flow is a task that
enqueues follow-ons" (D31). There is no separate task engine: a task is a run
whose single execution enqueued nothing, and it survives only as sugar —
`flow.NewTask(name, fn)` over a plain `func(ctx, In) (Out, error)`. Package
`flow` depends on `stream` (D15, an accepted hard dependency) and installs its
own migrations (version table `cb_flow_migrations`).

**Rows are the truth (D30).** There is no event log and no projection: the run
and step rows are the engine's only state, and `cb_flow_complete` applies
everything — the outcome, the spawned steps, dispatch, the remaining count — in the
completion transaction itself. The old event catalog has no successor: what
used to be events are now function arguments and guarded row updates. History
does not disappear, it changes grain — rows are event-grained, and
`cb_flow_attempts` keeps one row per attempt (§8).

## 1. Streams

- One **ready stream** per flow: `fq.<flow>` (queue mode, 01 §5), plus
  `fq.<flow>.<q>` for each step that declares its own queue via `WithQueue`
  (§7). A ready message is `{run_id, step_id}` with **topic = step name**, so a
  worker that implements a subset of steps filters by topic (D29).
- Every ready stream has a **retry twin** that mirrors its name: `fq.orders` →
  `fr.orders`, `fq.orders.gpu` → `fr.orders.gpu`. There is exactly one queue
  per ready stream, so the queue name never becomes an extra segment (unlike
  `sr.<stream>.<queue>`, where many queues share one stream).
- One **dead letter stream** per flow: `fd.<flow>` (D6, D33) — a pure archive.
  The engine writes it at conviction (§3), with the run and step context in
  the payload; humans and redrive tooling read it. The engine never depends on
  reading it.
- **No `fe.<flow>`**: the flow event streams died with the projection (D30);
  the `fe` code stays reserved (D22).

**Prerequisite changes in the stream layer** (land with M4a, in
`stream/migrations`): the fail path in 00001 is not family-aware — it builds
`sr.<stream>.<queue>` by concatenation and derives the dead letter stream
`sd.<base>` by `split_part`, so failing a message on `fq.orders.gpu` would mint
`sr.fq.orders.gpu`, which breaks D22's arity rule. The fail path must pick the
family from the stream's code (`fq.*` → `fr.*`/`fd.*`, plain → `sr.*`/`sd.*`).
The `fr.*` queues need the same CHECK pins as `sr.*` (claim batch size 1, no
filters), and the run-terminal notify channel joins D22's channel grammar.

## 2. Tables

Five tables, no edges — there is no deps table (D31):

- `cb_flow_defs (name PK, first_step, on_fail, retention NOT NULL)` — the
  flow definition, per-flow config. The entry step is an ordinary step;
  `first_step` just names it. Not `cb_flows`: the old schema's static flow
  registry has that exact name, it exists in every old-catbird database
  (raven's included, flows or not), and old and new must coexist during the
  transition — the one hard name collision between the two worlds, found and
  dodged here.
- `cb_flow_runs (run_id, flow, key, status, input, output, error,
  steps_remaining)` —
  status `running | completed | failed | canceled` (text + CHECK, D20);
  `UNIQUE (flow, key)` (constraint `cb_flow_runs_flow_key_key`) — `key` is the
  dedup point *and* the app-key lookup in one column (raven and ingest both
  demanded a durable run handle by application key); `steps_remaining` counts
  the steps the run still owes (§3).
- `cb_flow_steps (PK (run_id, step_id), name, status, input, output, error,
  attempt, dispatch, key UNIQUE)` — status `waiting | queued | started |
  completed | failed | canceled`; `attempt` is bumped by `cb_flow_start` (§3);
  `dispatch` is `immediate | all_done | signal` — the same three words in the
  Go options, the spawns JSON and this column; `key` (named unique constraint)
  = `hash(run_id, parent step_id, name, ordinal)` — the stream schema's word
  for a dedup identity, reused with the same meaning: a duplicate execution
  replaying its spawn buffer inserts nothing.
- `cb_flow_attempts (PK (run_id, step_id, attempt), consumer, outcome, error)`
  — per-attempt history *and* the fence record; kept when the run turns
  terminal. A NULL outcome is recorded silence: a start that never reported.
- `cb_flow_signals` — the signal buffer, matched under the run lock (§3).

Lookups — `WaitForOutput`, status, lookup by app key, the dashboard — read
these rows directly. There is nothing else to read.

## 3. Lifecycle

**The fence, stated once.** Every engine function follows the same two-step
guard. First it locks the run row (`FOR UPDATE`) and requires status
`running` — anything else is a silent no-op returning false. Then it checks
the step guard: the step's status and attempt must still match what
`cb_flow_start` handed out. Attempts are minted by `cb_flow_start` and never
read from message headers: the step's `attempt` counts *starts*, the stream
layer's `cb_attempt` header counts *verdicts*, and the two diverge on crashes
deliberately — a crashed execution consumed a start but never produced a
verdict. (This gets a comment and a test at implementation.)

**Birth.** `cb_flow_run` inserts the run (`steps_remaining` = 1) and its first step
(`queued`), and makes one direct publish to `fq.<flow>` (delay supported) —
nothing else. It is callable on a `Conn`, so an application can enqueue a run
in the same transaction as its own writes — the blob-GC / ingest pattern,
validated in production. Dedup is the run row itself: today's
`ON CONFLICT … DO UPDATE … WHERE FALSE` + `UNION ALL` pattern (do not
simplify), on constraint `cb_flow_runs_flow_key_key`; an existing run returns
its id and publishes nothing. The stream layer's key table is the wrong tool
here — the run row must exist anyway, and mapping a stream key back to a run
id would be a second lookup against a table retention may have emptied.

**Scheduled runs.** A schedule (01 §6) publishes its template to `fq.<flow>`
with no `run_id`. `cb_flow_start` recognizes that shape and births the run and
its first step right there, at claim time — schedule delivery stays pure
stream machinery, and a scheduled fire is just a message until a worker
starts it.

**Dispatch and the remaining count.** `steps_remaining` counts the steps the
run still owes — queued, started, or waiting on a signal. A spawn with
dispatch `immediate` inserts its step as `queued` and publishes its ready
message in the same completion transaction — the parent is complete by
construction, so a sequential edge needs no bookkeeping beyond the spawn
itself. A barrier step (`OnAllDone`, dispatch `all_done`) inserts as `waiting`
and stays **outside** the count until it dispatches. A signal step
(`OnSignal`, dispatch `signal`) inserts as `waiting` and **counts** — barriers
and the run's completion wait behind an unanswered signal. When a completion
brings `steps_remaining` to zero: if barriers are waiting, they dispatch
together as the next set and become the new remaining steps; otherwise the run
finalizes.

**Completion and run output.** A run finalizes when `steps_remaining` reaches
zero and no barrier is left waiting. Only executions spawn, so such a run can never grow
again — no explicit "done" call exists. Output resolution: an explicit
`run_output` passed to `cb_flow_complete` wins; otherwise the output of the
last execution to complete, if it set one; otherwise null. (Today's "output of
the final step" has no meaning when steps are spawned dynamically.)
`WaitForOutput` keeps its API, woken by the run-terminal notify (§1).

**Failure and conviction (D33).** A failed execution reports through
`cb_flow_fail`: the verdict is recorded (attempt row, step back to `queued`)
and the message is handed to the stream layer's retry machinery — backoff,
`fr.*`, exhaustion arithmetic are M2's call, not the engine's (D21, D28). At
give-up the stream layer does not archive the message: it republishes it once
more to the retry twin, marked with the exhaustion stamp (`cb_attempts` or
`cb_crashes`), and the corpse arrives through the normal claim path.
`cb_flow_start` sees the stamp and convicts instead of starting: step
`failed`, `on_fail` spawned as a final step if declared (receiving the failed
step's name, error and input), the run drains to `failed`, and the engine
writes the `fd.<flow>` archive row — one transaction. Verdict and crash
exhaustion converge here, and a foreign worker convicts without knowing it —
it calls start on every claimed message anyway. (`on_fail` itself lands in
M4b, 05.)

**Cancel.** `cb_flow_cancel` flips every non-started step and the run to
`canceled`. Started handlers get best-effort context cancellation: the consume
loop checks the run's status on its extend cadence (D27) — the same promise as
today.

**Signals are buffered.** `cb_flow_signal` satisfies a waiting signal step if
one exists, otherwise it buffers the payload in `cb_flow_signals` — both under
the run lock, so arrival order does not matter: a signal step that spawns
later consumes its buffered signal as it is applied. The call errors only if
the run is missing or terminal; the synchronous `ErrSignalNotDelivered` is
retired (README amendment 12). A signal step's name must be unique among the
run's unresolved steps — enforced when the spawn is applied — so a signal's
target is never ambiguous.

## 4. The functions — the completion transaction is the engine (D30)

All engine logic is SQL: a thin client in any language calls these functions
and gets identical semantics (D11). Every function begins with §3's fence —
run-row lock, then step guard — so that is not repeated below; only
deviations are. Handlers never run inside any of these transactions, so a slow
handler can never hold an engine lock. Full SQL sketches arrive per function
at implementation; what follows is the contract each sketch must meet.

- **`cb_flow_run(flow, input, key, delay) → (run_id, existing)`** — birth, §3:
  the dedup upsert on the run row, `steps_remaining` = 1, the first step
  `queued`, one publish to `fq.<flow>`. Callable on a `Conn` for
  same-transaction enqueue.
- **`cb_flow_start`** — called by the worker per claimed ready message; flips
  the step `queued | started → started` (started-to-started because a crashed
  execution leaves `started` behind — redelivery must be able to start again),
  bumps `attempt`, inserts the attempt row, and returns what the handler needs
  (name, input, attempt). Returns nothing when the fence fails — a stale ready
  message for a completed or canceled step — and the loop just closes the
  message. Two marked shapes take a branch instead: a message **without** a
  `run_id` is a scheduled fire (§3) — start births the run and its first step
  here, then proceeds normally; a message carrying the **exhaustion stamp** is
  a conviction (§3, D33) — start applies the terminal effects instead of
  starting, returns nothing, and the handler never runs.
- **`cb_flow_complete(run_id, step_id, attempt, output, spawns, run_output) →
  boolean`** — the heart, one transaction: fence → resolve the attempt
  (outcome on the attempt row, step → `completed`, output stored) → insert the
  spawned steps (`key ON CONFLICT DO NOTHING`, so a duplicate execution
  replaying its buffer inserts nothing) → publish every newly dispatchable
  ready message in one batch → decrement `steps_remaining` → at zero, dispatch
  waiting barriers as the next set or finalize the run (output resolution §3,
  run-terminal notify). False means the fence failed and nothing happened —
  the caller's execution was a duplicate.
- **`cb_flow_fail`** — one callable, deliberately: a foreign worker must never
  be required to compose two calls transactionally. It records the verdict
  (outcome and error on the attempt row, step back to `queued`) and hands the
  claimed message to the stream layer's fail machinery in the same
  transaction — backoff, `fr.*`, dead-lettering are M2 policy (D21, D28), not
  the engine's.
- **`cb_flow_cancel(run_id)`** — §3: non-started steps and the run →
  `canceled`; started handlers get best-effort context cancellation from the
  consume loop's status check.
- **`cb_flow_signal`** — §3: satisfy a waiting signal step or buffer the
  payload, under the run lock; errors only if the run is missing or terminal.

The cross-language contract is exactly the stream layer's
claim / extend / close plus flow start / complete / fail / signal — normative
for foreign workers from M4a on (§7).

**Cost and latency.** The completion transaction is small — guarded row
updates and a batch publish; strictly cheaper than the projection it replaced
(no event write, re-read, apply, or cursor advance), and one assigned leg per
edge instead of two: step-to-step latency is the publish→consume figure of
01 §2, measured at the M4 gate. Serialization did not disappear, it moved:
sibling completions in one run serialize on the run-row lock (serial-per-run
was required in both designs). The watch item is **connection occupancy** on
wide fan-in — hundreds of siblings completing at once queue on one row's lock
while each holds a pool connection (~1k completions/s per run). The mitigation
ladder, in order: keep the completion transaction tiny → batch sibling
completions into one call (an array argument, the `PublishMessages` precedent)
→ deferred drain detection. The wide-map stress test in M4b (05) decides
whether the ladder's first rung suffices.

## 5. The Plan DSL (D10, D31)

The key mechanic is unchanged: **`*Plan` is a buffer.** Every method buffers;
nothing blocks; the buffer commits with your completion (`cb_flow_complete`,
§4). A handler that crashes mid-way submits nothing, and at-least-once
redelivery replays it cleanly against the spawn `key`s (§2).

```go
flow.New("process_order").
    FirstStep("reserve").
    Step("reserve", func(ctx context.Context, p *flow.Plan, in Order) error {
        res, err := reserve(in.Items)
        if err != nil { return err }        // → cb_flow_fail, queue policy retries
        p.Spawn("charge", Charge{Order: in, Reservation: res})
        return nil                          //   parent hands its results forward
    }).
    Step("charge", func(ctx context.Context, p *flow.Plan, in Charge) error {
        for _, parcel := range split(in) {
            p.Spawn("ship", parcel)         // fan-out: N siblings, parallel
        }
        p.Spawn("confirm", in.Order, flow.OnAllDone())  // barrier: dispatches
        return nil                          //   when steps_remaining drains to zero
    }, flow.WithQueue("payments"),          // own ready stream + own retry terms (§6)
       flow.WithRetry(5, flow.FullJitter(time.Second, time.Minute))).
    Step("ship", shipFn).
    Step("confirm", confirmFn)
```

Surface, complete: `p.Spawn(name, input, opts...)` — the **one** Plan verb —
with the spawn options `flow.OnAllDone()` and `flow.OnSignal()` ·
`p.SetOutput(v)` / `p.SetRunOutput(v)` · `flow.Input[T](p)` /
`flow.OutputOf[T](p, "step")` (read from the step rows, typed via the existing
reflection utilities; a multi-instance name yields a slice). The rule: **a
spawn dispatches immediately unless an `On*` option defers it** (§3). The
entry step is not special — `FirstStep` names an ordinary step, and
`cb_flow_defs.first_step` records it; there is no `Entry` method.

The naming rule behind the options: name the **dispatch condition from the
step's own viewpoint**, never the engine's mechanics, and nothing may sound
like it acts now. The `On*` idiom never reads as blocking — `OnClick` doesn't
wait. Rejected on that rule: `Then`/`Next` (`p.Then(a); p.Then(b)` misreads as
sequential), `Gather` (sounds like collecting right now), `Join`
(`thread.join` blocks), `AfterAll`; `Entry`/`StartAt`/`Start` lost to
`FirstStep` (`Start` collides with the worker's `Start`). The three dispatch
words are the same in Go, the spawns JSON and the column: `immediate` |
`all_done` | `signal`.

**The input rule:** a step's input is exactly what it was spawned with — the
engine never injects anything. Spawning happens at completion, so a parent
passes its results forward *in* the spawn input; a barrier, whose siblings'
outputs didn't exist when it was spawned, reads them with `OutputOf`. This is
also what `input` means in the SQL contract (§7).

**Dead concepts become user patterns** (D31, D32): edges (`After`,
`*SpawnRef`) — a sequential edge is just a spawn at completion, fan-in is
`OnAllDone`; map — spawn N in a loop plus a barrier; conditions — a Go `if`
before `Spawn` (the engine never guards); `Optional[T]` — you only spawn what
exists; `AwaitSignal` — the `OnSignal` spawn option. And the task engine
itself: `flow.NewTask(name, fn)` wraps a plain `func(ctx, In) (Out, error)` as
a one-step flow — no Plan in the signature, the return value becomes the run
output.

The Go worker is the existing stream machinery, not a new loop: `ConsumeQueue`
on `fq.<flow>` (and on each `WithQueue` stream) wrapping `cb_flow_start` →
handler → `cb_flow_complete` / `cb_flow_fail` — nothing else; convictions ride
the same loop (§3, D33). Handlers are registered per step name and invoked via
the reflection utilities (ported from `task.go`).

```go
type Plan struct {
	spawns    []spawnSpec     // {name, input, dispatch: immediate|all_done|signal}
	output    json.RawMessage // optional — side-effect-or-error stays legal
	runOutput json.RawMessage
}
```

## 6. Retries and exhaustion

A step's retry terms are stream-layer queue policy (D4, 01 §7): the queue row
on its ready stream carries the `max_attempts`, backoff and give-up columns,
written by builder options at ensure. One queue per ready stream means one set
of terms per stream — steps sharing `fq.<flow>` share the flow's terms, and a
step that needs its own (a rate-limited payment call, a GPU stage) declares
`WithQueue` and gets its own stream, queue and terms. There is no step-policy
table.

The machinery is M2's, unchanged: `cb_flow_fail` hands the message to the
stream layer — delayed republish with backoff to the retry twin (`fr.*`, §1) —
and crash quarantine (D28) covers silence the same way. Exhaustion by either
counter is one final marked delivery, convicted by `cb_flow_start` (§3, D33).
One caution on words: the ready queue's `on_fail` column is a different knob
from the flow's `on_fail` step. Ready and retry queues are born with the
marked-final-delivery disposition (the column value's name is open until
transcription; `dead_letter` and `drop` remain the plain-queue values), and
changing it by raw UPDATE would silently break convictions. Queues and flows
share one robustness mechanism, it executes in SQL, and every language sees
it.

## 7. Cross-language workers (D11) — the differentiator, scoped

Not a rabbit hole, **provided it stays at the SQL API level**. A foreign worker
is ~50 lines against a contract of seven functions — three from the stream
layer, four from the engine — documented in `docs/sql-api.md`, which M4a makes
the normative spec:

```
cb_stream_claim(stream, queue, consumer, ttl)   → claimed messages
cb_stream_extend_claim(…)                       -- keep the claim alive (D23, D27)
cb_stream_close_claim(…)                        -- after the batch
cb_flow_start(…)        → (name, input, attempt) or nothing (fence failed,
                        --  or the message was a conviction, D33)
cb_flow_complete(run_id, step_id, attempt, output, spawns, run_output) → bool
cb_flow_fail(…)         → bool                  -- verdict + retry hand-off, one call
cb_flow_signal(…)
```

The loop: claim `fq.<flow>` and its retry twin → per message `cb_flow_start` →
run the handler → `cb_flow_complete` or `cb_flow_fail` → close the claim,
extending between messages and while a handler runs (D27). A worker that never
extends has its slow steps truthfully counted as crashes and quarantine slows
it down — legal, at-least-once, just noisy; the M4b demo worker is
deliberately slower than the claim TTL to prove that path end to end.
`attempt` travels from start through complete/fail — the fence's third
column — so each start resolves at most once, and a false return means the
execution was a duplicate and nothing happened. (`cb_flow_run` and
`cb_flow_cancel` are equally callable — any client with a Postgres connection
starts and cancels runs; the *worker* contract is the seven above.)

`spawns` carries the buffered Plan as JSON — `[{name, input, dispatch}]`, the
same three dispatch words as the column (§2) — so dynamic spawning is fully
available to a Python worker. The DSL is sugar, not capability. `WithQueue`
steps live on their own ready stream (`fq.<flow>.<q>`), so a foreign worker
subscribes to exactly the steps it implements and never claims work it can't
run. Conviction asks nothing of the worker: a marked corpse is just another
claimed message whose `cb_flow_start` hands back no work (D33).

Explicitly out of scope, same line as before: cross-language flow
*definition*, typed payload schemas or registries, SDK parity. The contract is
step name in, JSON in, JSON out, plus the seven calls. Hold that line.

## 8. Retention, audit, history (D30)

Rows are the history. A step row per spawn, an attempt row per start —
`cb_flow_attempts` is kept when the run turns terminal, and a NULL outcome is
itself a record: a start that never reported, which is what a crash looks like
(§3). The dashboard's run detail view reads what actually happened — who
started what, which attempts failed with which errors, what convicted —
without an event log. What died with the log is replay and
state-as-of-time-T reconstruction, which was never a shipped feature.

Terminal runs are pruned past `cb_flow_defs.retention` (NOT NULL, per flow) — the
run row and its step, attempt and signal rows together, in the stream layer's
batched-DELETE shape (01 §10). A long-parked run (a signal step waiting weeks)
now pins nothing but its own rows — the old design's partition-pinning problem
died with the event log. Whether such runs should be canceled by age is a
policy question deferred until a workload asks; the rows make it a one-line
query. The flow's streams follow the stream layer's retention rules unchanged
(D7); `fd.<flow>` holds convictions until triaged.

## 9. Build checklist

Sequencing and exit criteria live in 05 (M4a = single-execution runs, M4b =
spawns, barriers, signals, `on_fail`). The build items:

1. Stream-layer prerequisite (§1): family-aware give-up path (`fq.*` → `fr.*`
   / `fd.*`), the marked-final-delivery disposition (D33), `fr.*` CHECK pins,
   run-terminal notify channel in D22's grammar.
2. Migration `flow/migrations/00001`: the five tables (§2), text + CHECK
   statuses, named constraints (`cb_flow_runs_flow_key_key`, the steps `key`),
   version table `cb_flow_migrations`.
3. Functions (§4), M4a scope: `run`, `start` with its two marked branches,
   `complete` (raises on non-empty `spawns` until M4b), `fail`, `cancel`,
   `signal`. M4b: the spawn/barrier/signal paths in `complete`, `on_fail` at
   conviction.
4. Go: `flow.New` / `flow.NewTask` builders + ensure (streams, queue rows,
   `cb_flow_defs` row); worker = `ConsumeQueue` wrapping start → handler →
   complete/fail (§5); the Plan buffer; reflection utilities ported from
   `task.go`; run lookup by id and by app key; `WaitForOutput` on the
   run-terminal notify.
5. `docs/sql-api.md`: rewrite as the normative worker contract (§7).
6. Tests, the semantic core: dedup + key lookup; duplicate complete/fail
   no-ops; attempt-vs-`cb_attempt` divergence on crash (§3's deliberate pair);
   conviction from both roads — verdict exhaustion and crash exhaustion — and
   the fence states each arrives in; cancel during a retry gap; a scheduled
   fire births exactly one run; a signal arriving before its step spawns is
   buffered and matched; barrier dispatch at `steps_remaining` zero, including
   a second phase; wide-map stress (M4b, the D30 watch item); the deliberately
   slow Python demo worker (M4b).
