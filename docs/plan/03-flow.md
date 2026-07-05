# 03 — flow: the engine as a projection

Dynamic tasks and flows as three uses of the substrate: **append** (handlers emit
events), **project** (sharded consumers maintain state rows), **dispatch** (ready
steps go to work streams). Package `flow`, depends on `stream` (accepted hard
dependency, D15). A task is a one-step flow — one model, not two.

## 1. Streams and tables

- One event stream **per flow name**: `fe.<flow>` (a LIST partition each —
  isolation and per-flow retention for free). `run_id` is a field, *not* a stream:
  a stream per run would explode the partition count.
- One ready stream per flow: `fq.<flow>` (work mode, 01 §5), plus `fq.<flow>.<queue>` per
  dedicated step (§7).
- Projection tables — **rows are the view, the log is the truth**:
  `cb_flow_runs (run_id, flow, status, input, output, dedup_key — UNIQUE(flow,
  dedup_key), the RunFlow dedup point (§3), created/finished)` ·
  `cb_flow_steps (run_id, step_id, name, status, input, output, attempt,
  idempotency_key UNIQUE)` · `cb_flow_deps (run_id, step_id, needs_step_id,
  satisfied)` · `cb_flows (name, shards, …)` (per-flow config) ·
  `cb_flow_step_claims (run_id, step_id, attempt, outcome — PK on the triple)`,
  the per-attempt resolution claim (§7), GC'd when the run turns terminal.
  Lookups (`WaitForOutput`, status, dashboard) hit rows only.

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
- **Dedup**: `RunFlow` mints the run id synchronously — it upserts the
  `cb_flow_runs` row (`ON CONFLICT (flow, dedup_key) … WHERE FALSE` + `UNION ALL`,
  today's pattern verbatim) and appends `run_requested` in the same transaction;
  an existing run returns its id and appends nothing. The *stream* dedup table is
  the wrong tool here: mapping its row back to a run id would mean reading an
  event that retention may already have dropped.
- **Signals are buffered**: a `signal` event projects into run state whether or
  not the awaiting step exists yet; when a step with `AwaitSignal` spawns (or is
  already waiting), the projection matches them — arrival order doesn't matter.
  `SignalFlow` therefore errors only if the run is missing or terminal, and the
  synchronous `ErrSignalNotDelivered` is retired (README amendment 12). Steps
  that await a signal must have names unique among the run's live steps —
  enforced at spawn time — so a signal's target is never ambiguous.

**`cb_flow_run`, sketched** — the synchronous entry point:

```sql
CREATE FUNCTION cb_flow_run(flow text, input jsonb, dedup_key text DEFAULT NULL)
RETURNS TABLE (run_id bigint, existing boolean)
LANGUAGE plpgsql AS $$
#variable_conflict use_column   -- params flow/dedup_key vs columns in the
                                -- ON CONFLICT target (see 01 §8's sketch)
DECLARE _id bigint; _won bigint;
BEGIN
    _id := nextval(pg_get_serial_sequence('cb_flow_runs', 'run_id'));

    -- today's dedup pattern, verbatim (WHERE FALSE + UNION ALL — do not simplify)
    WITH won AS (
        INSERT INTO cb_flow_runs AS r (run_id, flow, status, input, dedup_key)
        VALUES (_id, flow, 'requested', input, dedup_key)
        ON CONFLICT (flow, dedup_key) WHERE dedup_key IS NOT NULL
        DO UPDATE SET status = r.status WHERE FALSE
        RETURNING r.run_id
    )
    SELECT x.run_id INTO _won FROM (
        SELECT w.run_id FROM won w
        UNION ALL
        SELECT r.run_id FROM cb_flow_runs r
        WHERE r.flow = cb_flow_run.flow AND r.dedup_key = cb_flow_run.dedup_key
        LIMIT 1
    ) x;

    IF _won <> _id THEN
        RETURN QUERY VALUES (_won, true);  -- existing run: nothing appended
        RETURN;
    END IF;

    -- same event shape as cb_flow_complete's appends, same shard header
    INSERT INTO cb_stream_messages (stream, topic, payload, headers)
    VALUES ('fe.' || flow, 'run_requested',
            jsonb_build_object('run_id', _id, 'input', input),
            jsonb_build_object('run_id', _id,
                'shard', abs(hashint8(_id)) % (SELECT f.shards FROM cb_flows f
                                               WHERE f.name = cb_flow_run.flow)));
    PERFORM pg_notify(current_schema || '.cbs_fe.' || flow, '');
    RETURN QUERY VALUES (_id, false);
END;
$$;
```

## 4. The projection — sharded, exactly-once (D9)

Not single-threaded (your note — correctly feared). The projection is N ordered
groups on `fe.<flow>` (`proj_0 … proj_N-1`); events route to shard
`hash(run_id) % N` **at append time** via a shard header the group filter matches.
Events for one run are always in one shard: serial per run (required for
correctness), parallel across runs. `N` is per-flow config, default 4; rebalancing
N is a stop-drain-restart operation, not dynamic.

Each shard iteration is one transaction: read events → apply to rows (decrement
deps, flip ready, complete runs) → append `step_ready` work messages to the ready
stream(s) → advance cursor. Same-database = **exactly-once projection**: apply,
dispatch, and ack commit atomically. Duplicate *handler* executions are stopped a
layer down: `cb_flow_complete`/`cb_flow_fail` claim the resolution first (an
`INSERT … ON CONFLICT DO NOTHING` on `(run, step, attempt)` — sketched in §7) and
append **nothing** when they lose — a redelivered step therefore produces no duplicate events, buffered spawns
included, and handlers are not required to spawn deterministically. The projector
itself needs no idempotency reasoning beyond the spawn conflict key. This transaction is small and fast (row updates);
handlers never run inside it — a slow handler can never hold a projection lock.

Throughput sanity: a shard applying a few hundred row-updates per batch commits in
single-digit ms; four shards absorb the "burst to thousands of events" envelope
with sub-second lag. The floor is *two* assigned legs per dependency edge —
completion events to the projection, then ready messages to a worker — so
step-to-step latency is about twice the publish→consume figure of 01 §2: ~60–160ms
with the NOTIFY accelerator, two to four tick intervals poll-only. Irrelevant at
seconds-to-minutes step duration; measured at the M4 gate.

**The apply function, sketched** — one shard iteration, one transaction. The
sketch loops per event for clarity; real code batches per event type where it
matters:

```sql
CREATE FUNCTION cb_flow_apply(flow text, shard int, batch int DEFAULT 500)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    _stream text := 'fe.' || flow;
    _grp    text := 'proj_' || shard;
    _pos bigint; _high bigint; _e record; _n int := 0;
    _ready bigint[] := '{}';           -- step_ids flipped ready this batch
BEGIN
    SELECT c.position INTO _pos FROM cb_stream_cursors c
    WHERE c.stream = _stream AND c.name = _grp FOR UPDATE;  -- one writer per shard
    SELECT s.last_position INTO _high FROM cb_streams s WHERE s.name = _stream;

    FOR _e IN
        SELECT m.position, m.topic, m.payload FROM cb_stream_messages m
        WHERE m.stream = _stream AND m.position > _pos AND m.position <= _high
          AND (m.headers->>'shard')::int = shard           -- 01 §4 header filter
        ORDER BY m.position LIMIT batch
    LOOP
        CASE _e.topic
        WHEN 'run_requested' THEN
            -- project the run to 'queued' and spawn the entry step 'start'
            -- with the run input; entry step has no deps → ready immediately
        WHEN 'step_spawned' THEN
            INSERT INTO cb_flow_steps (run_id, name, status, input, idempotency_key)
            VALUES (..., 'pending', ...)
            ON CONFLICT (idempotency_key) DO NOTHING;      -- duplicate spawn = no-op
            IF FOUND THEN
                -- edges: {name} → dep row per *existing instance* of that name
                --        {ref}  → dep row on the sibling's idempotency key
                -- await_signal → one extra dep, satisfied by a buffered or
                --                future signal (§3)
                -- zero unsatisfied deps → append step_id to _ready
            END IF;
        WHEN 'step_completed' THEN
            UPDATE cb_flow_steps SET status = 'completed', output = ... WHERE ...;
            UPDATE cb_flow_deps SET satisfied = true WHERE needs_step_id = ...;
            -- dependents whose last dep this was → _ready
        WHEN 'step_failed' THEN
            -- policy row (§6): attempts left → delayed publish to the ready
            -- stream's retry stream (D21); exhausted → run failed + OnFail
            -- spawn (§3)
        WHEN 'signal' THEN
            -- buffer into run state; a live awaiting step matches → its
            -- signal-dep satisfies, possibly → _ready (§3)
        WHEN 'run_output_set' THEN
            UPDATE cb_flow_runs SET output = _e.payload->'value' WHERE ...;
        WHEN 'run_canceled' THEN
            -- skip all non-started steps, mark the run, clear _ready of them
        END CASE;
        _n := _n + 1; _pos := _e.position;
    END LOOP;

    -- dispatch: one ready message per flipped step, routed to the step's ready
    -- stream (fq.<flow>, or fq.<flow>.<queue> for dedicated steps, §7)
    INSERT INTO cb_stream_messages (stream, topic, payload)
    SELECT ready_stream_of(s), 'step_ready',
           jsonb_build_object('run_id', s.run_id, 'step_id', s.step_id,
                              'attempt', s.attempt, 'name', s.name)
    FROM cb_flow_steps s WHERE s.step_id = ANY (_ready);
    -- + mark them 'ready'; + pg_notify on each touched ready stream's channel

    -- run completion (§3): for touched runs with no live or signal-awaiting
    -- steps left → terminal; resolve run output; wake WaitForOutput watchers

    UPDATE cb_stream_cursors SET position = _pos
    WHERE stream = _stream AND name = _grp;
    RETURN _n;
    -- commit: row deltas + dispatch + cursor advance, atomically — the
    -- exactly-once projection is this transaction, nothing more
END;
$$;
```

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
    Step("charge", func(ctx context.Context, p *flow.Plan, pay Payment) error {
        res := flow.OutputOf[Reservation](p, "reserve")  // dep output: fetched, never injected
        out, err := charge(pay, res)
        if err != nil { return err }              // → step_failed, policy retries
        p.SetOutput(out)                          // optional — side-effect-or-error
        return nil
    }, flow.WithRetry(5, flow.FullJitter(time.Second, time.Minute)),  // config → DB (01 §7)
       flow.WithQueue("payments"))                // dedicated ready stream (§7)
```

Surface (complete): `p.Spawn(name, input) *SpawnRef` · `ref.After(refs or names…)`
· `ref.AwaitSignal()` · `p.SetOutput(v)` / `p.SetRunOutput(v)` ·
`flow.Input[T](p)` / `flow.OutputOf[T](p, "step")` (fetched from rows, typed via
the existing reflection utilities).

**The input rule:** a step's input is exactly what it was spawned with — the
engine never injects dependency outputs (it can't: dependencies haven't run when
the spawn is declared). A handler that wants an upstream output fetches it with
`OutputOf`. This is also what `input` means in the SQL contract (§7).

**Edge addressing:** a `*SpawnRef` names one instance; a string names *every*
instance of that step name existing in the run when the buffer applies — so the
map-join is `p.Spawn("join", …).After("process")` across all N children, and
string edges may reference steps spawned by earlier handlers, not only the
current buffer. `OutputOf` on a multi-instance name returns the outputs as a
slice. Dropped as engine concepts, now user patterns:
map (spawn N + a join step `.After` them), conditions (Go `if` before `Spawn`),
generators/reducers, `Optional[T]` (you only spawn what exists — the concept
dissolves). Signals keep first-class support: they are one event type and one
dependency flavor, nearly free on the event model.

What still must be predefined (your note): the flow name and its step handlers
(registered on workers); everything about the DAG's *shape* is runtime. A step name
with no registered handler parks the step as `unhandled` (visible in the dashboard,
re-dispatched when a worker with that handler appears) — this is deploy-order
tolerance, not an error.

The Go side is deliberately thin — `*Plan` is a buffer and the worker loop is a
claim-execute-report cycle; all semantics live in the SQL above:

```go
type Plan struct {
	runID, stepID int64
	attempt       int
	spawns        []spawnSpec     // applied atomically by cb_flow_complete
	output        json.RawMessage // optional — side-effect-or-error
	runOutput     json.RawMessage
}

func (p *Plan) Spawn(name string, input any) *SpawnRef {
	p.spawns = append(p.spawns, spawnSpec{Name: name, Input: marshal(input)})
	return &SpawnRef{plan: p, idx: len(p.spawns) - 1}
}
func (r *SpawnRef) After(deps ...any) *SpawnRef { /* names or *SpawnRef → edges */ }

// worker loop, one per subscribed ready stream:
for {
	steps := claim(ctx, pool, queue, workerID, batchSize) // cb_flow_claim
	for _, s := range steps {
		p := &Plan{runID: s.RunID, stepID: s.StepID, attempt: s.Attempt}
		err := invoke(handlers[s.Name], ctx, p, s.Input) // reflection utils (task.go)
		if err != nil {
			fail(ctx, pool, s, err) // cb_flow_fail — retries are SQL policy, not Go
			continue
		}
		complete(ctx, pool, s, p)   // cb_flow_complete: completion + spawns +
		                            // outputs, one call, atomic; false = lost race
	}
	closeLease(ctx, pool, queue, steps) // cb_stream_close_lease (01 §5)
}
// + the per-process heartbeat goroutine (01 §5) — one row, every few seconds
```

## 6. Retries

Step retry policy is columns on a `cb_flow_step_policy` row (flow, step_name → same
schema as 01 §7), written by builder options. `step_failed` projects through the
same `cb_stream_fail` machinery: a delayed publish to the ready stream's retry
stream (`fr.<flow>.<grp>`, D22) with backoff, exhaustion →
run failure → OnFail. One robustness mechanism across queues and flows; visible to
every language because it executes in SQL.

## 7. Cross-language steps (D11) — your differentiator, scoped

Not a rabbit hole — **provided it stays at the SQL API level**. The contract a
foreign worker implements is five functions (documented in `docs/sql-api.md`, which
becomes the normative spec):

```
cb_flow_claim(queue, worker, batch)          → ready steps (run_id, step_id, attempt, name, input jsonb)
cb_flow_complete(run_id, step_id, attempt, output?, spawns jsonb, run_output?) → bool
cb_flow_fail(run_id, step_id, attempt, error) → bool
cb_flow_signal(flow, run_id, step, payload)
cb_stream_heartbeat(worker)                  -- lease liveness; call on a timer
```

`attempt` travels with the claim and back through complete/fail — it is the claim
key's third column, so each attempt resolves exactly once. The boolean return
surfaces a lost race (the caller's execution was a duplicate; nothing was
appended) for logging and metrics.

`spawns` carries the buffered Plan mutations — so even *dynamic* spawning is fully
available to a Python worker; the DSL is sugar, not capability. `input` is the
spawn input, verbatim (§5). The heartbeat is not optional: claims are range leases
kept alive per *worker* — the Go worker heartbeats from a background goroutine,
and a foreign worker that doesn't call `cb_stream_heartbeat` gets its lease swept
mid-work, guaranteeing duplicate execution of any step slower than the sweep
interval. Sweep interval and recommended cadence are part of the sql-api.md spec. `WithQueue("x")`
routes a step to its own ready stream, so a foreign worker subscribes to exactly
the steps it implements and never claims work it can't run (range leases can't
skip; dedicated streams make filtering structural).

**`cb_flow_complete`, sketched** — the worker's commit point. Handlers run outside
any database transaction; this one call is where a step execution becomes real, or
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
        'run_id', run_id,
        'shard',  abs(hashint8(run_id)) % _shards);

    -- Claim this attempt's resolution. Exactly one of complete/fail wins per
    -- (run, step, attempt); the loser appends NOTHING — a redelivered execution
    -- produces no duplicate events, buffered spawns included, which is why
    -- handlers need not spawn deterministically. cb_flow_fail claims the same
    -- key with outcome = 'failed'.
    INSERT INTO cb_flow_step_claims (run_id, step_id, attempt, outcome)
    VALUES (run_id, step_id, attempt, 'completed')
    ON CONFLICT (run_id, step_id, attempt) DO NOTHING;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    -- One event batch, atomic with the claim. Spawns first, completion last:
    -- insert order = id order = position tie-break (01 §2), so the projection
    -- sees the children before the completion that may satisfy their edges and
    -- resolves everything in a single apply pass.
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
    -- cb_stream_publish (no dedup, no pending on this path) — but it still owes
    -- the assigner its wake-up.
    PERFORM pg_notify(current_schema || '.cbs_' || _stream, '');

    RETURN true;
END;
$$;
```

Sketch-level notes: real code prefixes parameters per the existing migration
convention (`cb_send.queue`-style qualification) to dodge column ambiguity; the
`{ref: idx}` edges resolve to sibling idempotency keys (computable at append
time, same hash); lease accounting for the *ready-stream* message is the stream
layer's job — the worker reports it there separately, this function is purely
flow-level. Note the complete-vs-fail race is asymmetric by design: if a
duplicated execution fails first and completes second, the fail wins and the step
retries despite having succeeded once — legal under at-least-once, and rarer than
the reverse.

The twins are smaller. `cb_flow_fail` is the same claim with the other outcome —
deliberately dumb, because retry policy is the projection's job, not the worker's:

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

`cb_flow_claim` adds no new ideas: `cb_stream_claim` on the ready stream (01 §5),
then a PK fetch of each claimed `step_ready` message's `(run_id, step_id, attempt,
name)` plus the step's `input` from `cb_flow_steps` — no joins, two indexed reads.
`cb_flow_signal` is `cb_flow_run`'s little sibling: validate the run row exists
and is not terminal, append one `signal` event, notify the assigner.

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
weeks) pin their partition; a `max_run_age` policy cancels zombie runs before that
pin grows old. Precedence, explicitly: the age cap of 01 §10 sits above both and
still wins on a truly stuck stream — configure it well past `max_run_age`, and
accept that when it fires, the audit trail has a hole (recorded by the
`$sys.data_loss` event). Replay/time-travel claims hold
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
6. OnFail, dedup, `RunEvery` via the schedule table.
7. `docs/sql-api.md`: rewrite as the normative cross-language contract.
8. Tests: port the semantic core of `flow_test.go` (deps, signals, OnFail, cancel,
   dedup, map-as-pattern, output semantics); new: projection crash mid-batch (no
   lost/duplicate ready dispatch), duplicate handler execution spawns once
   (idempotency keys), shard parallelism (two runs progress while one shard is
   artificially stalled), unhandled step parks and resumes, a signal arriving
   before its awaiting step spawns is held and matched, a duplicate
   `cb_flow_complete` for the same step appends nothing.
