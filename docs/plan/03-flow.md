# 03 — flow: the engine on rows

> Sections 7–9 below are being rewritten for D30–D32; until then they still
> describe the dead projection design. Trust §1–6 and the README decision log.

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

- `cb_flows (name PK, first_step, on_fail, retention NOT NULL)` — per-flow
  config. The entry step is an ordinary step; `first_step` just names it.
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
`cb_flows.first_step` records it; there is no `Entry` method.

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

## 7. Cross-language steps (D11) — your differentiator, scoped

Not a rabbit hole, **provided it stays at the SQL API level**. A foreign worker
implements a contract of five functions. They are documented in `docs/sql-api.md`,
which becomes the normative spec:

```
cb_flow_claim(queue, worker, batch)          → ready steps (run_id, step_id, attempt, name, input jsonb)
cb_flow_complete(run_id, step_id, attempt, output?, spawns jsonb, run_output?) → bool
cb_flow_fail(run_id, step_id, attempt, error) → bool
cb_flow_signal(flow, run_id, step, payload)
cb_stream_extend_claim(queue, worker, from)  -- claim liveness; call between steps
```

`attempt` travels with the claim and back through complete/fail. It is the claim
key's third column, so each attempt resolves exactly once. The boolean return
surfaces a lost race for logging and metrics: false means the caller's execution
was a duplicate and nothing was appended.

`spawns` carries the buffered Plan mutations, so even *dynamic* spawning is fully
available to a Python worker. The DSL is sugar, not capability. `input` is the
spawn input, verbatim (§5). Extending is not optional. Every claim expires after
the queue's `claim_ttl` (D23). A foreign worker that doesn't call
`cb_stream_extend_claim` between steps loses its claim mid-work. Then any step
slower than the TTL is guaranteed a duplicate execution. The TTL and recommended
extend cadence are part of the sql-api.md spec. `WithQueue("x")`
routes a step to its own ready stream, so a foreign worker subscribes to exactly
the steps it implements and never claims work it can't run. Range claims can't
skip messages; dedicated streams make the filtering structural instead.

**`cb_flow_complete`, sketched** — the worker's commit point. Handlers run outside
any database transaction. This one call is where a step execution becomes real, or
becomes a no-op, atomically:

```sql
CREATE FUNCTION cb_flow_complete(
    run_id     bigint,
    step_id    bigint,
    attempt    int,                 -- from cb_flow_claim
    output     jsonb DEFAULT NULL,
    spawns     jsonb DEFAULT '[]',  -- [{name, input, after: [{name}|{ref}], await_signal}]
    run_output jsonb DEFAULT NULL
) RETURNS boolean                   -- false: lost the race, nothing appended
LANGUAGE plpgsql AS $$
DECLARE
    _flow   text;
    _shards int;
    _stream text;
    _hdrs   jsonb;
BEGIN
    -- run → flow → event stream + projection shard (01 §4: header equality filter)
    SELECT r.flow, f.shards INTO _flow, _shards
    FROM cb_flow_runs r
    JOIN cb_flows f ON f.name = r.flow
    WHERE r.run_id = cb_flow_complete.run_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catbird: run % not found', run_id;
    END IF;
    _stream := 'fe.' || _flow;
    _hdrs   := jsonb_build_object(
        'cb_run_id', run_id,
        'cb_shard',  abs(hashint8(run_id)) % _shards);

    -- Claim this attempt's resolution. Exactly one of complete/fail wins per
    -- (run, step, attempt). The loser appends NOTHING. So a redelivered
    -- execution produces no duplicate events, buffered spawns included, and
    -- handlers need not spawn deterministically. cb_flow_fail claims the same
    -- key with outcome = 'failed'.
    INSERT INTO cb_flow_step_claims (run_id, step_id, attempt, outcome)
    VALUES (run_id, step_id, attempt, 'completed')
    ON CONFLICT (run_id, step_id, attempt) DO NOTHING;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    -- One event batch, atomic with the claim. Spawns first, completion last.
    -- Insert order = id order = position tie-break (01 §2). So the projection
    -- sees the children before the completion that may satisfy their edges,
    -- and resolves everything in a single apply pass.
    INSERT INTO cb_stream_messages (stream, topic, payload, headers)
    SELECT _stream, 'step_spawned',
           jsonb_build_object(
               'name',            s->>'name',
               'input',           s->'input',
               'after',           coalesce(s->'after', '[]'),
               'await_signal',    coalesce((s->>'await_signal')::bool, false),
               'parent',          step_id,
               'idempotency_key', md5(run_id || ':' || step_id || ':' ||
                                      (s->>'name') || ':' || (i - 1))),
           _hdrs
    FROM jsonb_array_elements(spawns) WITH ORDINALITY AS b(s, i);

    INSERT INTO cb_stream_messages (stream, topic, payload, headers)
    VALUES (_stream, 'step_completed',
            jsonb_build_object('step_id', step_id, 'attempt', attempt,
                               'output', output),
            _hdrs);

    IF run_output IS NOT NULL THEN
        INSERT INTO cb_stream_messages (stream, topic, payload, headers)
        VALUES (_stream, 'run_output_set',
                jsonb_build_object('value', run_output, 'step_id', step_id),
                _hdrs);
    END IF;

    -- The engine owns this stream, so it appends directly rather than through
    -- cb_stream_publish: no dedup, no pending on this path. It still owes the
    -- assigner its wake-up.
    PERFORM pg_notify(current_schema || '.cbs_' || _stream, '');

    RETURN true;
END;
$$;
```

Sketch-level notes. Real code prefixes parameters per the existing migration
convention (`cb_send.queue`-style qualification) to dodge column ambiguity. The
`{ref: idx}` edges resolve to sibling idempotency keys; the same hash makes them
computable at append time. Claim accounting for the *ready-stream* message is
the stream layer's job. The worker reports it there separately; this function is
purely flow-level. Note the complete-vs-fail race is asymmetric by design. If a
duplicated execution fails first and completes second, the fail wins, and the
step retries despite having succeeded once. That is legal under at-least-once,
and rarer than the reverse.

The twins are smaller. `cb_flow_fail` is the same claim with the other outcome.
It is deliberately dumb, because retry policy is the projection's job, not the
worker's:

```sql
CREATE FUNCTION cb_flow_fail(run_id bigint, step_id bigint, attempt int, error text)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO cb_flow_step_claims (run_id, step_id, attempt, outcome)
    VALUES (run_id, step_id, attempt, 'failed')
    ON CONFLICT (run_id, step_id, attempt) DO NOTHING;
    IF NOT FOUND THEN RETURN false; END IF;

    INSERT INTO cb_stream_messages (stream, topic, payload, headers)
    VALUES (..., 'step_failed',
            jsonb_build_object('step_id', step_id, 'attempt', attempt,
                               'error', error),
            ...same run→stream→shard resolution as complete...);
    PERFORM pg_notify(current_schema || '.cbs_' || ..., '');
    RETURN true;
END;
$$;
```

`cb_flow_claim` adds no new ideas. It runs `cb_stream_claim` on the ready stream
(01 §5), then a PK fetch of each claimed `step_ready` message's `(run_id, step_id,
attempt, name)` plus the step's `input` from `cb_flow_steps`. No joins, two
indexed reads.
`cb_flow_signal` is `cb_flow_run`'s little sibling: validate the run row exists
and is not terminal, append one `signal` event, notify the assigner.

This boundary keeps it from becoming a rabbit hole. Explicitly out of scope:
cross-language flow *definition*, typed payload schemas/registries, SDK parity.
The contract is: step name in, JSON in, JSON out, claim/complete/fail/signal.
Hold that line.

## 8. Retention, audit, history

The event log **is** the audit trail. The current engine mutates rows in place
and loses history. This one gets history for free, and the dashboard's run detail
view can render the actual event sequence. A flow event partition drops when it
is past the projection cursors, **and** every run with events in the partition is
terminal, **and** it is older than the flow's audit window. A long-parked run (a
signal step waiting weeks) pins its partition. A `max_run_age` policy cancels
zombie runs before that pin grows old. Precedence, explicitly: the age cap of
01 §10 sits above both and still wins on a truly stuck stream. Configure it well
past `max_run_age`. Accept that when it fires, the audit trail has a hole,
recorded by the `$sys.data_loss` event. Replay and time-travel claims hold
*within retention*, not absolutely. Say so in user docs.

## 9. Build checklist

1. Event types + append helpers; shard header at append time.
2. Projection tables DDL; the apply function (one event batch → row deltas +
   ready appends) — pure SQL, the heart of the engine.
3. Sharded projection cursors; per-flow ensure (`flow.New(...).Ensure(pool)` creates
   streams, policies, shards idempotently).
4. Worker: claim ready → decode via reflection utils (port from `task.go`) → run
   handler → complete/fail with buffered Plan. Port worker lifecycle/NOTIFY wiring
   from `worker.go` (much of it survives).
5. DSL surface (§5); `RunFlow`/`WaitForOutput`/`SignalFlow`/`CancelFlow` with
   today's signatures where possible.
6. OnFail, dedup, `RunEvery` via the schedule table.
7. `docs/sql-api.md`: rewrite as the normative cross-language contract.
8. Tests: port the semantic core of `flow_test.go` (deps, signals, OnFail, cancel,
   dedup, map-as-pattern, output semantics); new: projection crash mid-batch (no
   lost/duplicate ready dispatch), duplicate handler execution spawns once
   (idempotency keys), shard parallelism (two runs progress while one shard is
   artificially stalled), unhandled step parks and resumes, a signal arriving
   before its awaiting step spawns is held and matched, a duplicate
   `cb_flow_complete` for the same step appends nothing.
