# 03 — flow: the engine on rows

> Revised 2026-07-14 (branch experiment). Two moves from the previous design,
> decided in review (decision log D34, D38): the engine no longer rides the stream layer
> at all — **the step row is the work unit**, claimed directly — and the
> failure machinery follows one law shared with subscriptions: *no evidence,
> no charge; solo evidence, one charge; one budget.* Package `flow` depends on
> the kernel only (`internal/ticker`, `internal/migrate`, `Conn`), not on
> `stream`; the two modules install and run independently, and the stream
> layer needs **zero changes** for flows to exist.

**One concept.** A run is a group of executions, and every execution may
atomically enqueue follow-on steps when it completes — "a flow is a task that
enqueues follow-ons" (D31). There is no separate task engine: a task is a run
whose single execution enqueued nothing, and it survives only as sugar —
`flow.NewTask(name, fn)` over a plain `func(ctx, In) (Out, error)`. Own
migrations, version table `cb_flow_migrations`.

**Rows are the truth — and the transport (D30, D34).** There is no event log,
no projection, and no message either: the step row that records the work *is*
the unit a worker claims. `cb_flow_complete` applies everything — the outcome,
the spawned steps, dispatch, the remaining count — in the completion
transaction, and the spawned rows are immediately claimable. What the old
designs wrote three times per edge (a step row, a ready message, an assigned
position), this design writes once. Step-to-step latency is notify + claim —
no assigner leg, no tick floor; the poll interval is the safety net (D17).

**One counter (D38).** The step row's `attempt` column counts *starts* — every
time `cb_flow_start` hands the step to a handler, whatever later becomes of
that execution. A verdict, a crash, a graceful restart of a running handler:
each consumed a start. That one number is the whole retry budget, checked
wherever an execution's fate is decided. There is no crash counter, no header
bookkeeping, and no second exhaustion road: silence is just a lease that
lapsed, and the lapsed row carries its own count.

## 1. The work table — steps are the messages

- A spawned step inserts as a row with `claimable_at = now()` (or `now() +
  delay`) and is claimable the moment its transaction commits. **One
  timestamp column, `claimable_at`, carries visibility, lease, and backoff**:
  spawn sets it to when the step may first be handed out; claim sets it to
  `now() + claim_ttl` (the lease deadline); extend pushes it; fail sets it to
  `now() + backoff(attempt)`; a crash simply lets it lapse.
- **Wakeups** ride the old engine's proven contract: the engine fires
  `pg_notify` on the flow's channel (`cbf_<flow>`) with the earliest
  `claimable_at` in the payload; the notifier (M5) parses it and wakes or arms
  a timer — this is `worker_notifier.go`'s existing protocol, pointed at new
  tables. Until M5, workers poll (D17). A second channel (`cbfr_<flow>`)
  carries run-terminal events for `WaitForOutput` and the dashboard.
- **Queues are a column.** A step's `queue` (default `main`) partitions
  claiming and carries retry terms (§6). `WithQueue("payments")` routes a
  step to the `payments` pool; workers claim `(flow, queue)` pairs they hold
  handlers for. No streams, no filters, no topics — the claim predicate is
  the routing.
- The hot index is partial: `(flow, queue, claimable_at) WHERE status IN
  ('queued', 'started')` — terminal rows leave it, so the index holds only
  the working set. `flow` and `queue` are denormalized onto the step row at
  spawn so the claim query joins nothing (the hot-path rule, kept).

## 2. Tables

Seven tables, no edges — there is no deps table (D31):

Both `cb_flows` and `cb_flow_schedules` reuse names from the old schema,
deliberately. The flow module does not share a database with the old flow
code: raven and biblio have zero flows (D31), so the old flow tables are empty
— dropped at raven's cutover — and nothing needs the two schemas side by side.
The flow test suite runs on its own database, not the shared `cb_tst` (05).

- `cb_flows (name PK, first_step, steps jsonb, on_fail, retention NOT
  NULL)` — the flow definition. `steps` is the authoritative step list and
  the routing map in one: a flat object `{step_name: queue_name | null}`,
  null meaning `main`. `cb_flow_complete` validates every spawn name against
  it and stamps the step's `queue` from it — a typo'd spawn raises, fails the
  spawning handler, and convicts truthfully; a foreign worker spawns with
  zero knowledge of queue layout. `first_step` and `on_fail` name entries in
  `steps`.
- `cb_flow_queues (PK (flow, queue), claim_ttl, claim_batch_size,
  max_attempts, backoff_kind, backoff_base, backoff_max)` — the retry and
  claim terms per pool, written whole by `flow.Define` (D26: config deploys
  with code and must converge). The `main` row always exists; `WithQueue`
  adds rows.
- `cb_flow_schedules (PK (flow, name), every, catch_up, input, next_at)` —
  interval schedules for scheduled runs (`every` is a fixed duration), the
  same shape and define semantics as the stream layer's schedule table; cron
  specs are a later addition in both, deferred. Delivered by the flow tick
  job: a due row calls `cb_flow_run(flow, input)` and re-arms `next_at` in one
  transaction — exactly-once per slot by construction, no message, no
  translation.
- `cb_flow_runs (run_id PK, flow, key, status, input, output, error,
  steps_remaining, next_step_id, created_at, finished_at)` — status
  `running | failing | completed | failed | canceled` (text + CHECK, D20).
  `failing` means the outcome is already decided (failed) and only the
  `on_fail` chain may still execute (§3). `UNIQUE (flow, key)` (constraint
  `cb_flow_runs_flow_key_key`) — `key` is the dedup point *and* the app-key
  lookup in one column; NULLs are distinct. `steps_remaining` counts the
  steps the run still owes (§3). `next_step_id` mints per-run step ids — the
  run row is locked in every engine call anyway, so the counter is free, and
  it gives dense ids and a clean `0` sentinel for "spawned at birth".
- `cb_flow_steps (PK (run_id, step_id), flow, queue, name, parent_step_id
  NOT NULL, ordinal NOT NULL, status, dispatch, input, output, error,
  attempt NOT NULL DEFAULT 0, claimable_at, consumer, created_at,
  finished_at)` — status `waiting | queued | started | completed | failed |
  canceled`; `dispatch` is `immediate | all_done | signal` — the same three
  words in the Go options, the spawns JSON and this column. Replay identity
  is the plain tuple: `UNIQUE (run_id, parent_step_id, name, ordinal)`
  (constraint `cb_flow_steps_identity_key`), where **`ordinal` is the
  spawn's zero-based index in its parent's Plan buffer** — deterministic
  across replays because the buffer is replayed whole. `parent_step_id` is
  `0` for the birth step. A partial unique index `(run_id, name) WHERE
  dispatch = 'signal' AND status NOT IN ('completed', 'failed', 'canceled')`
  enforces §3's signal-name rule race-free.
- `cb_flow_attempts (PK (run_id, step_id, attempt), consumer, started_at,
  finished_at, outcome, error)` — per-attempt history *and* the fence
  record; kept when the run turns terminal. `outcome` is `completed | failed
  | NULL`, and NULL is recorded silence: a start that never reported — a
  crash, or a restart that superseded it.
- `cb_flow_signals (PK (run_id, name), payload, created_at)` — the signal
  buffer, one slot per name: a second signal for a name nobody consumed yet
  overwrites the slot (last signal wins); matching consumes it — deletes the
  row.

Lookups — `WaitForOutput`, status, lookup by app key, the dashboard — read
these rows directly. There is nothing else to read, and nothing else exists:
no message copy, no position, no stream.

## 3. Lifecycle

**The fence, stated once.** Every engine function follows the same two-step
guard. First it locks the run row (`FOR UPDATE`) and checks the run's status:
`running` or `failing` for start / complete / fail / signal (`failing` admits
the `on_fail` chain — conviction canceled everything else, so no per-step
marking is needed), `running` only for cancel. Anything else is a silent
no-op returning false (signal raises instead, below). Then the step guard:
complete and fail require `(status = 'started', attempt = $attempt)`; a
mismatch means the caller was superseded, and nothing happens. Attempts are
minted by `cb_flow_start` only. One lock ordering — run row first, then step
rows — means no deadlocks between engine calls; it is also why conviction
lives in start and fail, never in claim (claim locks step rows without the
run lock, §4).

**Birth.** `cb_flow_run` inserts the run (`steps_remaining` = 1,
`next_step_id` = 2) and its first step (step_id 1, parent 0, ordinal 0,
status `queued`, `queue` from the steps map, input = the run input,
`claimable_at` = now or now + delay), notifies the flow's channel — nothing
else. It is callable on a `Conn`, so an application can enqueue a run in the
same transaction as its own writes — the blob-GC / ingest pattern, validated
in production. Dedup is the run row itself: the `ON CONFLICT … DO UPDATE …
WHERE FALSE` + `UNION ALL` pattern (do not simplify), on constraint
`cb_flow_runs_flow_key_key`; an existing run — live or terminal — returns its
id with `existing = true` and inserts nothing. The dedup window is the run's
retention.

**Scheduled runs.** A row in `cb_flow_schedules` (§2). The flow tick job
delivers due rows — `cb_flow_run` plus re-arm in one transaction — so a fire
is exactly-once per slot with no key gymnastics, and the first step lands on
its own queue like any other birth. `catch_up` keeps the stream scheduler's
semantics (`skip` | `all`).

**Dispatch and the remaining count.** `steps_remaining` counts the steps the
run still owes — queued, started, or waiting on a signal. The arithmetic,
exactly, in `cb_flow_complete`:

```
steps_remaining := steps_remaining − 1 + count(spawns with dispatch IN ('immediate', 'signal'))
```

A spawn with dispatch `immediate` inserts its step as `queued`, claimable at
once — the parent is complete by construction, so a sequential edge needs no
bookkeeping beyond the spawn itself. A barrier step (`OnAllDone`, dispatch
`all_done`) inserts as `waiting` and stays **outside** the count until it
dispatches. A signal step (`OnSignal`, dispatch `signal`) inserts as
`waiting` and **counts** — barriers and the run's completion wait behind an
unanswered signal. When a completion brings `steps_remaining` to zero: if
barriers are waiting, **all of them** dispatch together as the next phase —
`waiting → queued`, claimable now, `steps_remaining := count(dispatched
barriers)`; otherwise the run finalizes. A barrier inserted by the very
completion that drained the count dispatches in that same call — the at-zero
check runs after the inserts. Phases repeat with no special case. The failure
story the formula guards against: birth sets the count to 1; if the first
step's completion only decremented, a run whose first step spawns its
successor would finalize with work still queued.

**Completion and run output.** A run finalizes when `steps_remaining` reaches
zero and no barrier is left waiting — in a `running` run as `completed`, in a
`failing` run as `failed`. Only executions spawn, so such a run can never
grow again — no explicit "done" call exists. Output resolution, in order: an
explicit `run_output` passed to any `cb_flow_complete` wins (last writer
wins — writes are serialized by the run lock); otherwise the step output of
the completion that finalized the run, if it set one; otherwise null. The
finalizing completion is a single, well-defined call, so the default is
deterministic for every sequential shape; a flow that ends in parallel
siblings races for last place and should say what it means with
`SetRunOutput`. Finalization fires the run-terminal notify (`cbfr_<flow>`,
payload `<run_id>:<status>`). `WaitForOutput` keeps its API: it polls the run
row until M5, then wakes on the notify with the poll demoted to safety net.

**Failure, retries, conviction — one counter (D38).** A failed execution
reports through `cb_flow_fail`: the fence admits it, the attempt row records
the verdict (`outcome = 'failed'`, the error), and one comparison decides:

- `attempt < max_attempts` — the step goes back to `queued`, `claimable_at =
  now() + backoff(attempt)`, consumer cleared. The retry is nothing but the
  row becoming claimable later; no copy, no republish, no second object.
- `attempt ≥ max_attempts` — **conviction**, in the same transaction.

Crashes are silence — the handler never reports. A crashed execution's lease
just lapses, and the next claim call repairs it (§4): a `started` row whose
lease has lapsed is rescheduled to `claimable_at = now() + backoff(attempt)`
with its consumer cleared — backoff paced by the same counter, since the
lapsed start *was* attempt N — and when it next reaches a worker,
`cb_flow_start` checks the same comparison: starting would mint `attempt +
1`, and if `attempt ≥ max_attempts` already, start **convicts instead of
starting** and returns no work. Two call sites, one routine
(`_cb_flow_convict`), one counter, one policy home (the queue row). A
conviction arriving late — the run already `failing` or terminal because a
sibling convicted first — hits the fence and no-ops; that is the whole
idempotency story.

`max_attempts` bounds **total starts**, not verdicts: a crash consumed a
start, and so did a graceful shutdown that canceled a running handler — the
step had started, and the fence cannot distinguish a live zombie from a dead
one, so redelivery must mint fresh. Stated plainly: "this step's handler will
begin at most `max_attempts` times." The cost is that frequent deploys spend
starts of long-running handlers — a step like that deserves its own
`WithQueue` with generous terms. The justice half of the same rule: a step
that was **leased but never started** (its worker died before reaching it)
lapses back to claimable with nothing spent — no evidence, no charge. The
worker's graceful path is the same rule on purpose: release clears leases on
unstarted steps, uncharged.

**Conviction's effects** (`_cb_flow_convict`, one transaction): the step
turns `failed` with its error (for the silent road: "attempts exhausted; last
attempt ended in silence"); every other non-terminal step — queued, started,
or waiting — turns `canceled` (a started sibling's later complete/fail hits
the step guard and no-ops; its handler is reaped by the worker's status
check, below). Then:

- **`on_fail` declared**: the run turns `failing`; the `on_fail` step is
  spawned as an ordinary step — parent = the convicted step, dispatch
  immediate, input built by the engine as `{step, error, input}` — and
  `steps_remaining := 1`. The chain then runs under the normal rules: the
  fence admits it because the run is `failing` and everything else is
  canceled; it may spawn, retry on its own terms, even wait on a signal;
  when its chain drains the count to zero the run finalizes as `failed`. If
  the `on_fail` step itself convicts, the run is already `failing`, so
  `_cb_flow_convict` spawns nothing and the run turns `failed` — one rule,
  no regress.
- **No `on_fail`**: the run turns `failed`, `error` set, run-terminal notify
  fired.

`on_fail` firing on crash exhaustion too is the point: today's OnFail misses
hard worker death, which is why ingest hand-rolled `sweep_stuck_deliveries`.

**Cancel.** `cb_flow_cancel(run_id, reason)` — fence (`running` only; a
`failing` run is past canceling: its outcome is decided and its cleanup
should finish) — flips every non-terminal step and the run to `canceled` and
fires the run-terminal notify. Started handlers get best-effort context
cancellation: the worker's handler wrapper checks its own step row on the
extend cadence (D27) and cancels the handler's context when the step is no
longer `started` — one cheap indexed read, covering cancel, a sibling's
conviction, and supersession alike.

**Signals are buffered, and the payload is the input.** An `OnSignal` spawn
carries no input of its own (the Go builder panics on one; SQL raises — §5).
`cb_flow_signal(run_id, name, payload)`: under the run lock, if a signal step
of that name is `waiting`, it is satisfied — the payload becomes the step's
`input`, status `waiting → queued`, claimable now (`steps_remaining`
unchanged: signal steps already count). Otherwise the payload is buffered in
the run's slot for that name (overwriting an unconsumed older one). The
mirror happens when `cb_flow_complete` applies an `OnSignal` spawn: a
buffered slot, if present, is consumed on the spot and the step inserts
`queued` instead of `waiting` — arrival order does not matter. The call
raises only if the run is missing or terminal (`failing` accepts signals: a
cleanup chain may legitimately await an operator). The synchronous
`ErrSignalNotDelivered` is retired (README amendment 12). A signal step's
name must be unique among the run's unresolved steps — the partial unique
index (§2) enforces it at spawn time; violating it fails the spawning
completion, which is a handler bug and convicts truthfully.

## 4. The functions — the completion transaction is the engine (D30)

All engine logic is SQL: a thin client in any language calls these functions
and gets identical semantics (D11). Every function except claim begins with
§3's fence; handlers never run inside any of these transactions. Full SQL
sketches arrive per function at implementation; what follows is the contract
each sketch must meet.

- **`cb_flow_define(flow, first_step, steps, on_fail, retention)`** plus the
  queue-terms and schedule declarations — the whole-declaration upserts
  (D26), guarded so unchanged declarations write nothing. It checks that
  `first_step` and `on_fail` (when set) are keys in `steps` and raises
  otherwise — a deploy-time error instead of a runtime "spawn name not
  declared" on the first run. The Go `flow.Define` performs them in one
  advisory-locked transaction. Deploy-time setup, not part of the worker
  contract.
- **`cb_flow_run(flow, input, key, delay) → (run_id, existing)`** — birth,
  §3. Raises if the flow is not defined. Callable on a `Conn`.
- **`cb_flow_claim(flow, queue, consumer, ttl DEFAULT NULL) → rows of
  (run_id, step_id)`** — the one function without the run fence (it locks
  step rows only, which is why it never convicts). One statement over the
  partial index, two effects: **repair** — `started` rows with a lapsed lease
  and a consumer still stamped are rescheduled to `backoff(attempt)` and
  cleared, not handed out; **hand out** — up to `claim_batch_size` eligible
  rows (`queued`, or `started` with no consumer — a repaired crash due for
  redelivery) get the caller's consumer and a lease of `claim_ttl`,
  `FOR UPDATE SKIP LOCKED`, ordered by `claimable_at`. Leased-but-unstarted
  rows whose lease lapses are simply eligible again — availability needs no
  repair and no charge.
- **`cb_flow_start(run_id, step_id, consumer) → (name, input, attempt)`** —
  per claimed step. Fence, then the conviction check (§3): at the attempt
  limit it convicts (no attempt row — nothing started) and returns nothing.
  Otherwise flips the step `queued | started → started` (started-to-started
  because a crashed execution leaves `started` behind), bumps `attempt`,
  inserts the attempt row, and returns what the handler needs. Returns
  nothing when the fence fails — a stale claim of a resolved step — and the
  loop just moves on.
- **`cb_flow_extend(flow, queue, consumer) → held rows`** — pushes
  `claimable_at` forward on every row the consumer still holds, one
  statement; the loop calls it on the D27 cadence and compares the returned
  set to what it thinks it holds — a missing row means supersession, cancel
  that handler.
- **`cb_flow_complete(run_id, step_id, attempt, output, spawns, run_output)
  → boolean`** — the heart, one transaction, in this order: fence → resolve
  the attempt (outcome, finished_at; step → `completed`, output stored) →
  validate and insert the spawned steps (names against `cb_flows.steps`,
  queue stamped from the map, dispatch words, no input on signal spawns;
  identity `ON CONFLICT DO NOTHING` as defense in depth) → consume signal
  slots for signal spawns → apply §3's remaining formula → at zero, dispatch
  waiting barriers or finalize (output resolution, run-terminal notify) →
  notify the flow channel once with the earliest new `claimable_at`. False
  means the fence failed and nothing happened.
- **`cb_flow_fail(run_id, step_id, attempt, error) → boolean`** — §3:
  verdict on the attempt row, then retry (queued + backoff) or conviction,
  one comparison, one transaction. False = fenced.
- **`cb_flow_cancel(run_id, reason DEFAULT NULL) → boolean`** — §3.
- **`cb_flow_signal(run_id, name, payload)`** — §3: satisfy or buffer, under
  the run lock; raises only if the run is missing or terminal.

The leaner argument lists are deliberate: claim, start, complete and fail
take ids the worker already holds — no stream, topic or header to know, so
nothing to pass wrongly.

**Cost and latency.** One row insert per edge where the previous designs
wrote a step row *and* a message *and* an assigned position; step-to-step
latency is notify + claim — single-digit milliseconds once the notifier
lands, one poll interval before that — with no assigner tick in the path at
all. Serialization did not disappear, it moved where it always was: every
engine call takes the run-row lock, so sibling starts and completions in one
run serialize on it. The watch item is unchanged — **connection occupancy**
on wide fan-in (~1k completions/s per run), with the same mitigation ladder:
keep the completion transaction tiny → batch sibling completions into one
call (an array argument, the `PublishMessages` precedent) → deferred drain
detection. The wide-map stress test in M4b decides whether the first rung
suffices. The steps table is update-hot (claim, start, complete each touch
the row), so it inherits the autovacuum note that 01 §10 attaches to message
partitions; per-flow LIST partitioning of `cb_flow_steps` is the documented
escape hatch if one flow's churn ever dominates.

## 5. The Plan DSL (D10, D31)

The key mechanic is unchanged: **`*Plan` is a buffer.** Every method buffers;
nothing blocks; the buffer commits with your completion (`cb_flow_complete`,
§4). A handler that crashes mid-way submits nothing, and at-least-once
redelivery replays it cleanly against the spawn identity (§2).

```go
flow.New("process_order").
    FirstStep("reserve").
    OnFail("notify_ops").                   // an ordinary declared step (§3)
    WithRetry(3, flow.FullJitter(time.Second, time.Minute)). // main's terms
    Step("reserve", func(ctx context.Context, p *flow.Plan, in Order) error {
        res, err := reserve(in.Items)
        if err != nil { return err }        // → cb_flow_fail; retry or convict (§3)
        p.Spawn("charge", Charge{Order: in, Reservation: res})
        return nil                          //   parent hands its results forward
    }).
    Step("charge", func(ctx context.Context, p *flow.Plan, in Charge) error {
        for _, parcel := range split(in) {
            p.Spawn("ship", parcel)         // fan-out: N siblings, parallel
        }
        p.Spawn("confirm", in.Order, flow.OnAllDone())  // barrier: dispatches
        return nil                          //   when steps_remaining drains to zero
    }, flow.WithQueue("payments"),          // own worker pool + own retry terms (§6)
       flow.WithRetry(5, flow.FullJitter(time.Second, time.Minute))).
    Step("ship", shipFn).
    Step("confirm", confirmFn).
    Step("notify_ops", notifyFn)            // receives {step, error, input}
```

Surface, complete: `p.Spawn(name, input, opts...)` — the **one** Plan verb —
with the spawn options `flow.OnAllDone()` and `flow.OnSignal()` ·
`p.SetOutput(v)` / `p.SetRunOutput(v)` · `flow.Input[T](p)` /
`flow.OutputOf[T](p, "step")` (read from the step rows, typed via the
existing reflection utilities; a multi-instance name yields a slice). The
rule: **a spawn dispatches immediately unless an `On*` option defers it**
(§3). The entry step is not special — `FirstStep` names an ordinary step
recorded in `cb_flows.first_step`; so is the failure step — `OnFail`
names one recorded in `cb_flows.on_fail`.

The naming rule behind the options: name the **dispatch condition from the
step's own viewpoint**, never the engine's mechanics, and nothing may sound
like it acts now. The `On*` idiom never reads as blocking — `OnClick` doesn't
wait. Rejected on that rule: `Then`/`Next` (`p.Then(a); p.Then(b)` misreads
as sequential), `Gather` (sounds like collecting right now), `Join`
(`thread.join` blocks), `AfterAll`; `Entry`/`StartAt`/`Start` lost to
`FirstStep` (`Start` collides with the worker's `Start`). The three dispatch
words are the same in Go, the spawns JSON and the column: `immediate` |
`all_done` | `signal`.

**The input rule:** a step's input is exactly what it was spawned with, with
two stated exceptions where the engine supplies it: a signal step's input is
the signal's payload (§3 — which is why an `OnSignal` spawn takes none), and
an `on_fail` step's input is the engine-built `{step, error, input}`.
Spawning happens at completion, so a parent passes its results forward *in*
the spawn input; a barrier, whose siblings' outputs didn't exist when it was
spawned, reads them with `OutputOf`. This is also what `input` means in the
SQL contract (§7).

**Validation is layered.** The Go side fails fast where it can: `Spawn`
panics on a name not declared in the flow, or on an `OnSignal` spawn with an
input — a panic becomes a failed attempt (D27), close to the bug. The SQL
side enforces the same rules for every language (§4): an invalid spawn
raises, failing the spawning completion; retries won't fix a deterministic
bug, so it convicts — truthful, and visible in the attempt rows.

**Dead concepts become user patterns** (D31, D32): edges (`After`,
`*SpawnRef`) — a sequential edge is just a spawn at completion, fan-in is
`OnAllDone`; map — spawn N in a loop plus a barrier; conditions — a Go `if`
before `Spawn` (the engine never guards); `Optional[T]` — you only spawn
what exists; `AwaitSignal` — the `OnSignal` spawn option; `CompleteEarly` —
a return *is* completion (`p.SetOutput` + `return nil`). And the task engine
itself: `flow.NewTask(name, fn)` wraps a plain `func(ctx, In) (Out, error)`
as a one-step flow — no Plan in the signature, the return value becomes the
run output. A one-step run is also the recommended shape for everything the
old world put on a work queue — it *is* the queue, with a handle (README,
D37).

**The worker** is flow's own claim loop, the same shape the stream consumer
uses (D27) and copied rather than imported, since the two modules are
independent. One `select` waits on three things at once: the handler
returning, an extend tick that pushes the step's lease deadline out while the
handler is still running, and a stop-or-cancel signal. Because one loop both
runs the handler and extends the lease, a worker that hangs or dies stops
extending, and its work falls to another worker. The loop: claim a batch →
per step: start → handler → complete/fail — extending on the cadence,
releasing unstarted leases on shutdown. Handlers are registered per
step name and invoked via the reflection utilities (ported from `task.go`).
At startup the worker checks coverage: for every `(flow, queue)` it claims,
it must hold handlers for **all** steps the steps map routes there, or it
refuses to start — a claim is indiscriminate within its queue, so partial
coverage would strand steps (§7).

```go
type Plan struct {
	spawns    []spawnSpec     // {name, input, dispatch: immediate|all_done|signal}
	output    json.RawMessage // optional — side-effect-or-error stays legal
	runOutput json.RawMessage
}
```

## 6. Retry terms

A step's terms are its queue's row in `cb_flow_queues` (D4: policy in the
database, applied by SQL): `max_attempts`, the backoff triple, `claim_ttl`,
`claim_batch_size` — written whole by `flow.Define`, so a redeploy converges
them. Steps sharing `main` share the flow's terms (`WithRetry` on the flow);
a step that needs its own (a rate-limited payment call, a GPU stage)
declares `WithQueue` and gets its own pool and terms (`WithRetry` on the
step; the builder panics on step-level `WithRetry` without `WithQueue`).
There is no step-policy table, and nothing here touches the stream layer's
policy at all — the two modules' terms live in their own tables with their
own meanings stated in their own documents. `backoff()` is a ten-line pure
function; each module ships its own copy so they install independently.

## 7. Cross-language workers (D11) — the differentiator, scoped

Not a rabbit hole, **provided it stays at the SQL API level**. A foreign
worker is ~40 lines against a contract of six functions, all engine-owned —
documented in `docs/sql-api.md`, which M4a makes the normative spec:

```
cb_flow_claim(flow, queue, consumer, ttl)      → [(run_id, step_id)]
cb_flow_start(run_id, step_id, consumer)       → (name, input, attempt)
                                               --  or nothing (fence, or conviction)
cb_flow_extend(flow, queue, consumer)          → still-held rows (D23, D27)
cb_flow_complete(run_id, step_id, attempt, output, spawns, run_output) → bool
cb_flow_fail(run_id, step_id, attempt, error)  → bool  -- retry or convict, one call
cb_flow_signal(run_id, name, payload)
```

The loop: claim → per step `cb_flow_start` → run the handler →
`cb_flow_complete` or `cb_flow_fail` — extending on the cadence, and a
worker that never extends has its slow steps truthfully counted as crashes
and slowed by backoff — legal, at-least-once, just noisy; the M4b demo
worker is deliberately slower than the claim TTL to prove that path end to
end. `attempt` travels from start through complete/fail — the fence's third
column — so each start resolves at most once, and a false return means the
execution was superseded and nothing happened. (`cb_flow_run` and
`cb_flow_cancel` are equally callable — any client with a Postgres
connection starts and cancels runs; the *worker* contract is the six above.)
No headers to parse, no streams to name, no scheduled-message shapes to
recognize — a scheduled run arrives as an ordinary claimed step.

`spawns` carries the buffered Plan as JSON — `[{name, input, dispatch}]`,
the same three dispatch words as the column (§2) — and the engine routes
each spawn by the steps map, so dynamic spawning is fully available to a
Python worker with zero knowledge of queue layout. The DSL is sugar, not
capability. Explicitly out of scope, same line as before: cross-language
flow *definition*, typed payload schemas or registries, SDK parity. The
contract is step name in, JSON in, JSON out, plus the six calls. Hold that
line.

## 8. Retention, audit, history (D30)

Rows are the history. A step row per spawn, an attempt row per start —
`cb_flow_attempts` is kept when the run turns terminal, and a NULL outcome is
itself a record: a start that never reported (§3). The dashboard's run detail
view reads what actually happened — who started what, which attempts failed
with which errors, what convicted — without an event log; convictions are a
`status = 'failed'` query away, which is also all a redrive tool needs (a new
run from the recorded input). What died with the log is replay and
state-as-of-time-T reconstruction, which was never a shipped feature.

Terminal runs are pruned past `cb_flows.retention` (NOT NULL, default 30
days) — the run row and its step, attempt and signal rows together, batched
deletes by a flow janitor on the kernel ticker. **A parked run pins only its
own rows** — a signal step waits with no other artifact in the system, and a
queued step *is* its own delivery, so there is no separate message to lose
and no wedge-by-pruning hazard at any parking duration: the step outlives
exactly as long as its run does, by construction. Whether ancient
non-terminal runs should be canceled by age remains the deferred policy
question; the rows make it a one-line query.

## 9. Stream-layer prerequisite

**None.** The engine touches no stream table, no stream function, no stream
migration. `fq`, `fr`, `fd` and `fe` join the retired-codes list without ever
existing; the stream layer's own revisit (01, D35–D38) proceeds
independently, and either module ships without the other. This section
exists so its emptiness is a recorded property, not an omission: the
previous two designs required a stream-layer prerequisite precisely because
work rode the log, and every seam item in them — family-aware fail paths,
marked deliveries, CHECK pins, header contracts — is deleted rather than
relocated.

## 10. Build checklist

Sequencing and exit criteria live in 05 (M4a = single-execution runs, M4b =
spawns, barriers, signals, `on_fail`). The build items:

1. Migration `flow/migrations/00001`: the seven tables (§2), text + CHECK
   statuses, named constraints (`cb_flow_runs_flow_key_key`,
   `cb_flow_steps_identity_key`, the signal-name partial unique index), the
   partial claim index, version table `cb_flow_migrations`.
2. Functions (§4), M4a scope: `define`, `run`, `claim` (with lease repair),
   `start` (with the conviction check), `extend`, `complete` (raises on
   non-empty `spawns` until M4b), `fail`, `cancel`, `_cb_flow_convict` (the
   `failing` machinery ships in M4a; only `on_fail` spawning stays dark
   until a def declares one), `_cb_flow_backoff`. M4b: the
   spawn/barrier/signal paths in `complete`, `cb_flow_signal`, `on_fail`.
3. The flow tick job (kernel ticker): schedule delivery (§3), the
   run-retention janitor (§8).
4. Go: `flow.New` / `flow.NewTask` builders + `Define` (defs, queue terms,
   schedules — one transaction); the worker loop on the shared D27 skeleton
   with coverage validation, status-check cancellation and
   release-on-shutdown (§5); the Plan buffer; reflection utilities ported
   from `task.go`; run lookup by id and by app key; `WaitForOutput` polling,
   notify at M5.
5. `docs/sql-api.md`: rewrite as the normative worker contract (§7), with
   the old-vs-new name table for the transition.
6. Tests, the semantic core:
   - dedup + key lookup; duplicate complete/fail no-ops (fence).
   - conviction from both roads — the exact inequalities: verdict at
     `attempt = max_attempts` convicts in `fail`; a lapsed step at
     `attempt = max_attempts` convicts in `start`; total starts never
     exceed `max_attempts`.
   - lease repair: a lapsed `started` row comes back after
     `backoff(attempt)` exactly once (repair is idempotent under racing
     claims); a leased-but-unstarted row lapses back with `attempt`
     untouched; a graceful release redelivers immediately, uncharged.
   - a graceful shutdown mid-handler spends a start (§3's stated property).
   - the `failing` lifecycle: conviction cancels siblings, `on_fail`
     receives `{step, error, input}`, its chain may spawn and signal, its
     exhaustion ends the run `failed` with no second `on_fail`; cancel of a
     `failing` run no-ops.
   - signal slot semantics: signal-before-spawn buffered and consumed;
     overwrite of an unconsumed slot; sequential same-name steps; duplicate
     signal-step name fails the spawning completion.
   - the remaining formula: chains, fan-out + barrier, barrier phases
     (including a barrier spawned by the draining completion), signal steps
     holding barriers and finalization.
   - routing: a `WithQueue` spawn lands in its pool with its terms; an
     undeclared spawn name convicts the spawner; worker coverage validation
     refuses partial pools.
   - schedules: a due row births exactly one run and re-arms in one
     transaction; `catch_up` semantics ported from the scheduler tests.
   - define convergence: changed terms, steps map and schedules apply on
     redeploy; unchanged define writes nothing; a `first_step` or `on_fail`
     not in the steps map is rejected.
   - cancel during a retry gap (step queued with future `claimable_at` →
     canceled, never claimed).
   - step-to-step latency benchmark (notify + claim — no tick in the path);
     throughput ≥ the old `BenchmarkTaskThroughput` / `FlowThroughput`
     envelope; the wide-map stress test (the D30 watch item).
   - the deliberately slow Python demo worker (M4b) — six functions, extend
     included, slower than the claim TTL.

The decision-log entries for this design (D34, D38) live in the README with
the rest of the log; this document is their detailing chapter.
