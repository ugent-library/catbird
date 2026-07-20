# SQL API — the job module

This is the SQL contract of the job module (`jobs/`). All engine logic lives
in these functions: a worker or client in any language calls them over a
plain Postgres connection and gets the same semantics as the Go package. The
signatures and comments in `jobs/migrations/00001_job.sql` are the
authority; this document is their reference.

**Scope of this document.** The job module, plus the
[wire module](#sql-api--the-wire-module) at the end. The stream module's SQL
surface is documented in `docs/plan/01-stream.md` while it is pre-release.
The old root-schema functions (`cb_run_task`, `cb_create_queue`, …) are
frozen and scheduled for replacement; the [name table](#old-names-new-names)
at the end of the job part maps them to their successors.

**What the module does.** A run is a group of steps. A handler completes its
step and may add new steps in the same call — `steps` — each of which can
wait for the run's other steps (`waits_for_steps`) or for an external
signal (`waits_for_signal`)
before it runs. A run whose one step adds nothing is a single-execution run,
the smaller and most common case.

## Vocabulary

Four words, one per table: a **job** is the declared, named thing
(`cb_jobs`); a **run** is one instance of a job — the group and the record
(`cb_job_runs`); a **step** is a unit of owed work inside a run, running a
declared job (`cb_job_steps`); an **attempt** is one execution of a step
(`cb_job_attempts`). The step row that records the work is the unit a
worker claims — there is no separate message, and nothing is copied or
moved when a step retries.

A **queue** is a global pool: it partitions claiming and carries the claim
and retry terms (`cb_job_queues`). A job routes to one pool
(`cb_jobs.queue`, NULL means `default`). The migration seeds the `default`
row, so a bare install works; every other pool is declared, and `default`
itself is redeclarable like any pool.

A **trigger** (`cb_triggers`) turns events into work: every message on a
stream that matches its filter creates one run of a job, delivered by the
module's tick. It is the one feature that reads the stream module's schema
and it refuses loudly without it; everything else here installs and runs
with the job schema alone.

Names — jobs, queues, schedules, triggers — match `[a-z][a-z0-9_]*`, at
most 20 bytes.

## Rules shared by every function

**The checks.** Every function that changes a run locks the run row first
(`FOR UPDATE`) and checks its status: only `running` and `failing` runs
accept changes. `cb_job_complete` and `cb_job_fail` also check that the
step is `started` and that the caller's `attempt` equals the step's. When a
check fails, the function returns false (or nothing) and changes nothing:
the caller is acting on outdated information — the step was finished or
handed to another worker — and its late call must not disturb what already
happened. Only `cb_job_start` increments `attempt`.

**Locks.** The order is: run row first, then step rows. Functions that
update several step rows lock them in `(run_id, id)` order. `cb_job_claim`
and `cb_job_release` lock step rows only and never the run row; claim also
skips rows that are already locked. One shared order plus skip-locked means
engine calls cannot deadlock each other.

**Notifications.** `cbq_<queue>` fires when a step becomes claimable; the
payload is the step's `claimable_at` as an RFC 3339 UTC timestamp.
`cbj_<job>` fires when a run reaches a terminal status; the payload is
`<run_id>:<status>`. Channel names are prefixed with the current schema
(so `LISTEN "public.cbq_default"` on a stock install).

**Terminal steps.** `worker` and `claimable_at` are set to NULL when a step
reaches `completed`, `failed` or `canceled`. The claim index covers only
`queued` and `started` rows, so finished steps drop out of it.

**Errors.** Error texts start with `catbird:`. Three SQLSTATEs: `IRD01`
means the call is invalid, `IRD02` means a named object does not exist,
`IRD03` means a required module is not installed (today: the trigger
functions without the stream schema).

## The worker contract

A worker in any language is one loop against the six functions below.
(`cb_job_signal`, further down, is not part of the loop — it is the call
that unblocks a signal-gated step.)

1. `cb_job_claim` a batch across the queues it serves.
2. Per claimed step: `cb_job_start` — nothing returned means the step moved
   on, skip it. `name` tells the worker which handler to run; a step it
   holds no handler for goes back with `cb_job_release`.
3. Run the handler with the returned `input`.
4. `cb_job_complete` or `cb_job_fail`.

While handlers run, the worker calls `cb_job_extend` on the cadence
`lease_at` implies (half the remaining lease is a good cadence) and cancels
the handler of any step missing from the result. A worker that never
extends is legal — its slow steps are counted as crashes and slowed by
backoff, at-least-once still holds, it is just noisy. `cb_job_release` is
politeness, not obligation: an unreleased lease lapses on its own and comes
back with no attempt spent, just slower.

A runnable version of this loop lives at `examples/pyworker/worker.py`: a
Python worker with no SDK whose handler deliberately takes three times the
claim TTL — the extend cadence is what keeps it alive.

`attempt` travels from start through complete or fail — it is the third
column the checks compare — so each start resolves at most once, and a
false return means the step was taken over and nothing happened.

The argument lists are deliberately short: claim, start, extend, release,
complete and fail take ids the worker already holds — no stream, topic or
header to know, so nothing to pass wrongly. A scheduled run arrives as an
ordinary claimed step.

### cb_job_claim

```sql
cb_job_claim(queues text[], worker text)
    RETURNS TABLE (run_id bigint, step_id bigint, name text, lease_at timestamptz)
```

Hands out steps that are ready to run. Per pool in `queues`: up to
`claim_batch_size` step rows whose `claimable_at` has passed, oldest first,
each stamped with the caller's worker name and leased until `now() +
claim_ttl` (the lease lives in `claimable_at`). Returns the handed-out
steps; `lease_at` is when the lease runs out, and `cb_job_extend` moves it
forward while the handler runs. Raises `IRD02` when a named queue is not
defined.

What happens to a ready row depends on its state:

- `queued`: handed out. A worker stamp still on the row means a worker
  claimed it earlier and died before calling start; no attempt was spent,
  so the row is simply handed out again and the stamp overwritten.
- `started` with a worker stamp: that worker started the step and has not
  reported for a whole lease. It crashed, or it is stuck. Not handed out;
  the row gets what `cb_job_fail` would have done had the worker been able
  to report: stamp cleared, `claimable_at` moved to `now() +
  backoff(attempt)`, or to plain `now()` when no attempts are left. The
  status stays `started` so a worker that was merely stuck can still
  deliver its result: complete and fail accept a `started` step at the same
  attempt.
- `started` without a worker stamp: a crashed row (previous bullet) whose
  backoff has passed. Handed out. `cb_job_start` then spends the next
  attempt, or marks the step failed when none are left.

A crashed row is cleared in one call and handed out in a later one, never
both at once. Reason: the worker most likely to call claim right after a
lease runs out is the stuck worker itself, alive again with the old handler
still running. Given its own step back, it would run the step twice at the
same time. With the stamp cleared first, its next `cb_job_extend` no longer
returns the step, so it cancels the old handler before the step reaches any
worker.

Clearing crashed rows uses batch slots, so a call can return fewer steps
than are ready; the next call picks up the rest.

### cb_job_start

```sql
cb_job_start(run_id bigint, step_id bigint, worker text)
    RETURNS (name text, input jsonb, signal_input jsonb, attempt int)
```

Starts a claimed step: increments `attempt`, inserts the attempt row and
returns what the handler needs. The checks, in order: the run accepts work;
the step is `queued` or `started` (`started` happens when an earlier
attempt crashed); the step still carries this worker's stamp. When a check
fails the step was finished or taken over by another worker in the
meantime: nothing is returned (`name` IS NULL) and the caller moves on to
its next claimed step.

When the step's attempts are already used up, starting it would exceed the
budget: the step is marked failed instead (error `attempts exhausted; last
attempt ended in silence`), no attempt row is written, and nothing is
returned. This check sits here and not in `cb_job_claim` because the
give-up needs the run lock, which claim never takes.

`signal_input` is the payload delivered to a step that waited for one, NULL
for a step that asked for no signal or was not yet signaled.

### cb_job_extend

```sql
cb_job_extend(queues text[], worker text)
    RETURNS TABLE (run_id bigint, step_id bigint, lease_at timestamptz)
```

Moves the lease end (`claimable_at`) forward on every step this worker
holds in the given pools, and returns those steps. A step the worker thinks
it holds but that is missing from the result was taken over after its lease
ran out, or canceled: the worker must cancel that handler. A lease that ran
out but was not yet taken over can still be extended; until someone else
claims the step, it is still this worker's.

### cb_job_release

```sql
cb_job_release(run_id bigint, step_id bigint, worker text, pause interval DEFAULT '0')
    RETURNS boolean
```

Gives back a claimed step this worker has not started. The stamp is cleared
and the step becomes claimable again after `pause` (default 0:
immediately). No attempt is spent. A worker that claims a step it has no
handler for — possible during a rolling deploy — releases it with a short
pause, so that two not-yet-updated workers do not pass it back and forth as
fast as they can. Like claim, no run lock. Returns false when this worker
no longer holds the step.

### cb_job_complete

```sql
cb_job_complete(run_id bigint, step_id bigint, attempt int,
                output jsonb DEFAULT NULL, steps jsonb DEFAULT NULL,
                run_output jsonb DEFAULT NULL)
    RETURNS boolean
```

Records a successful attempt and applies what the handler enqueued: the
attempt row and the step store the result, the new `steps` are inserted,
and the run's owed-step count moves by `−1 + (new steps not waiting for the
run's other steps)`. `run_output`, when given, becomes the run's output no
matter which step this is; when several steps set it, the last one wins. It
is the only way a run gets an output: a run whose completions never pass
one finishes with output NULL — the engine never falls back to a step's
output, which would be an arbitrary pick when several steps finish the run
together. SQL NULL means "not given", which is why an explicit run output
can never be the JSON value `null`.

When the count reaches zero the run either dispatches its next phase or
finishes. If steps are waiting in `waiting_for_steps`, they all dispatch
together as that phase (see [New steps, waits and
barriers](#new-steps-waits-and-barriers)). Otherwise the run is done: a
`running` run becomes `completed`, a `failing` run becomes `failed`, its
output whatever `run_output` calls set along the way. Returns false,
having changed nothing, when the checks fail.

`steps` is the handler's buffer as JSON — `[{name, input, waits_for_steps,
waits_for_signal}]`, both booleans required. See [New steps, waits and
barriers](#new-steps-waits-and-barriers) for the shape and the waits.

### cb_job_fail

```sql
cb_job_fail(run_id bigint, step_id bigint, attempt int, error text)
    RETURNS boolean
```

Records a failed attempt on the attempt row, then one comparison decides.
Attempts left: the step goes back to `queued` and becomes claimable again
after `backoff(attempt)`; the retry is just this row becoming claimable
later, nothing is copied or moved. Attempts used up: the engine gives up,
in the same transaction (below). Returns false, having changed nothing,
when the checks fail.

### cb_job_signal

```sql
cb_job_signal(run_id bigint, name text, payload jsonb DEFAULT NULL)
    RETURNS boolean
```

Delivers a signal to the run's `waiting_for_signal` step of that name, or
buffers the payload in the run's slot for it (a newer signal nobody consumed
yet overwrites the slot; a later match consumes it — arrival order does not
matter). Returns true when the payload landed, whether it satisfied a step
or was buffered. Returns false, changing nothing, when the run is missing or
already finished: the run ended at the same time, which the caller could not
have avoided, so this is not a raise. A `failing` run accepts signals — its
cleanup chain may wait for one.

## Retries and giving up — one counter

The step row's `attempt` column counts **starts**: every time
`cb_job_start` hands the step to a handler, whatever later becomes of that
execution. A reported failure, a crash, a graceful shutdown that canceled a
running handler: each consumed a start. `max_attempts` therefore bounds
total starts — "this step's handler will begin at most `max_attempts`
times" — and it is checked in the two places an execution's fate is
decided: `cb_job_fail` (a reported failure at the limit) and `cb_job_start`
(a crashed row that comes back at the limit). The other half of the same
rule: a step that was leased but never started lapses back to claimable
with no attempt spent — nothing began, nothing is charged.

The give-up (`_cb_job_give_up`, internal — called by start and fail, one
transaction): the step turns `failed` with its error; every other
unfinished step of the run turns `canceled`. Then:

- If the run is `running` and its **birth job** (the job named in
  `cb_job_run`) declares an `on_fail` job, the run turns `failing` and one
  new step is created to run that job, with input `{job, error, input}`
  describing the failed step. The run ends `failed` when that step — and
  whatever steps it adds — is done.
- Otherwise the run ends `failed` now.

A `failing` run never gets a second `on_fail` step, and it keeps its first
error; the `on_fail` step's own failure is recorded on its step and attempt
rows. `on_fail` fires on crash exhaustion too — a run whose worker died
hard still gets its cleanup step.

## Client calls

Everything below works on any connection — the define calls in deploy code,
`cb_job_run` and `cb_job_cancel` from any client. Deploy code calls the
defines in dependency order: queues before the jobs that name them,
`on_fail` targets first. Each call is atomic and validated on its own, so a
deploy that dies mid-way leaves a consistent prefix and the next deploy
converges the rest; an app that wants the whole set to apply together runs
the calls in its own transaction.

### cb_job_define_queue

```sql
cb_job_define_queue(queue text,
                    claim_ttl interval DEFAULT NULL, claim_batch_size int DEFAULT NULL,
                    max_attempts int DEFAULT NULL, backoff_kind cb_backoff_kind DEFAULT NULL,
                    backoff_base interval DEFAULT NULL, backoff_max interval DEFAULT NULL)
    RETURNS void
```

Declares a pool and all its terms in one call. An argument that is not
given gets the stock value — `claim_ttl` 30s, `claim_batch_size` 10,
`max_attempts` 3, full-jitter backoff 1s–1m, the same values the migration
seeds for `default` — it never means "keep the current value". Declaring
the same terms again writes nothing.

`backoff_kind` is `none` (retry immediately), `fixed` (always
`backoff_base`) or `full_jitter` (a random delay up to `backoff_base *
2^(attempt-1)`, capped at `backoff_max`).

There is no queue delete: a pool no longer declared may still have
unfinished steps routed to it, and a stale terms row is inert config, so
removing one is a deliberate op (a raw `DELETE`).

### cb_job_define

```sql
cb_job_define(job text, queue text DEFAULT NULL, on_fail text DEFAULT NULL,
              retention interval DEFAULT NULL)
    RETURNS void
```

Declares a job and all its config in one call: `queue` NULL routes the job
to `default`, `retention` NULL means the stock 30 days (`cb_forever()`
keeps runs forever). `queue` must name a declared pool and `on_fail` a
declared job, raising `IRD02` otherwise; checking this at declaration time
turns a typo into a deploy error instead of a runtime failure. A job may
name itself as its own `on_fail`. Declaring the same config again writes
nothing.

`on_fail` and `retention` matter when the job is a run's birth job; on a
job only ever run as a mid-run step they are inert config.

### cb_job_define_schedule

```sql
cb_job_define_schedule(name text, job text, every interval,
                       catch_up cb_job_catch_up_policy DEFAULT NULL,  -- NULL means 'skip'
                       input jsonb DEFAULT NULL, start_at timestamptz DEFAULT NULL)
    RETURNS void
```

Declares a schedule in one call: from `next_at` on, every `every`, run
`job` with `input`. `every` is a fixed duration — hours or less; days,
months and years need cron, which is not built yet. `next_at` is not
declared config, the engine manages it: a fresh schedule first fires at
`now() + every`, a redeclaration keeps the firing phase, a changed cadence
re-anchors it to `now() + every`, and `start_at` sets it directly. `input`
is stored exactly as given; NULL is a valid job input.

`catch_up` decides what a backlog gets when the tick was down past one or
more slots: `all` fires a run per missed slot, `skip` (the default) drops
the backlog and fires only an on-time slot.

### cb_job_delete_schedule

```sql
cb_job_delete_schedule(name text) RETURNS boolean
```

Deletes a schedule. Returns false when there was none. (Schedules get a
delete because a forgotten one keeps creating runs; the other declarations
are inert when unused.)

### cb_trigger_define

```sql
cb_trigger_define(name text, stream text, job text,
                  topic text DEFAULT NULL, condition text DEFAULT NULL,
                  start_pos bigint DEFAULT NULL)
    RETURNS void
```

Declares a trigger in one call: every message on `stream` that matches the
filter creates one run of `job`. Creating and updating are the same call,
and an identical declaration writes nothing. Needs the stream module's
schema in the same database — without it the call raises `IRD03`
(`catbird: stream schema required`); a broken declaration is refused here,
not discovered by the tick: `IRD02` when the job or the stream is not
defined, `IRD01` when the filter does not compile.

The filter — a `topic` pattern and a `condition`, the same languages
cursors and subscriptions use — is stored on the cursor the trigger owns:
the row in `cb_stream_cursors` named exactly like the trigger, on its
stream. That cursor is the filter's single home and remembers how far
delivery got. A redeclared filter is recompiled there; the position stays
put. `start_pos`, when given, sets the position deliberately: 0 delivers
the stream from the beginning, N from after N; when creating, NULL starts
at the tail — only messages published from now on deliver.

The run a match creates gets the message payload as its input, exactly as
published — a job has one input shape no matter who creates the run — and
`<trigger name>:<stream position>` as its key, so creation is exactly-once
even if the batch is replayed.

### cb_trigger_delete

```sql
cb_trigger_delete(name text) RETURNS boolean
```

Removes the trigger and its cursor. Returns false when there was none.
Matches still sitting between the cursor and the stream's head are gone
with it — the trigger is its cursor's only reader.

### cb_job_run

```sql
cb_job_run(job text, input jsonb DEFAULT NULL, key text DEFAULT NULL,
           delay interval DEFAULT NULL)
    RETURNS (run_id bigint, existing boolean)
```

Creates a run: the run row plus its first step, queued on the job's pool,
in one statement. Works on any connection, so an application can create a
run inside its own transaction — commit the order and enqueue its
confirmation atomically, no outbox glue. Raises `IRD02` when the job is not
defined. `delay` holds the first step back.

`key` deduplicates: when a run of this job with this key already exists —
live or finished, within the job's retention — its id is returned with
`existing = true` and nothing is inserted. The key is also the app-side
lookup handle: `SELECT … FROM cb_job_runs WHERE job = $1 AND key = $2`.

```sql
SELECT * FROM cb_job_run(
    job   => 'send_confirmation',
    input => '{"order_id": 123}'::jsonb,
    key   => 'order-123'
);
```

### cb_job_cancel

```sql
cb_job_cancel(run_id bigint, reason text DEFAULT NULL) RETURNS boolean
```

Cancels a run. A `running` run ends `canceled` with `error = reason`. A
`failing` run already has its verdict and ends `failed`; cancel only stops
the on_fail steps (refusing to cancel a `failing` run would assume its
cleanup always finishes). All unfinished steps become `canceled`. A handler
that is already running is not interrupted here: its worker sees the
canceled step on the next extend and cancels it, and a late complete or
fail fails the checks and changes nothing. Returns false when the run does
not exist or is already finished.

## Reading runs

Rows are the truth, so status, output and history are plain `SELECT`s — no
read functions. The run row is the durable handle, queryable by id or by
`(job, key)` for as long as the job's retention keeps it:

```sql
-- the handle: status, output, error
SELECT status, output, error FROM cb_job_runs WHERE id = $1;

-- by app key
SELECT * FROM cb_job_runs WHERE job = 'send_confirmation' AND key = 'order-123';

-- what actually happened: every start, its worker, its verdict
SELECT s.name, a.attempt, a.worker, a.status, a.error, a.started_at, a.finished_at
FROM cb_job_steps s
JOIN cb_job_attempts a ON (a.run_id, a.step_id) = (s.run_id, s.id)
WHERE s.run_id = $1
ORDER BY s.id, a.attempt;
```

An attempt row with a NULL `status` never reported a result: the worker
crashed, or the step was handed to another worker and the outcome of that
start no longer counts.

## The module's tick

The functions below are engine-internal (the underscore prefix): the
module's ticker (`jobs.StartTicker` in Go) calls them on an interval, and a
thin client without the Go package schedules them itself — pg_cron, a cron
job, any loop. Running the tick from several processes is safe: `FOR
UPDATE SKIP LOCKED` decides who does the work. Without a tick, on-demand
runs keep working; only scheduled runs, triggers and pruning pause.

### _cb_job_run_scheduled

```sql
_cb_job_run_scheduled(batch_size int DEFAULT 500) RETURNS int
```

Fires due schedules: each due row creates runs via `cb_job_run` and
re-arms, in this one transaction — so a slot fires exactly once no matter
how many processes tick. Runs are created without a key: every fired slot
is its own run. Returns the number of runs created.

### _cb_job_run_triggered

```sql
_cb_job_run_triggered(trigger text, batch_size int DEFAULT 100) RETURNS int
```

Delivers one trigger's next batch: read the matching messages after the
trigger's cursor, create one run per message, advance the cursor — one
transaction, so a raise rolls the whole batch back and the trigger stalls
at its cursor with nothing half-done. Creation can only fail
deterministically (the job's row deleted, an input refused); the tick logs
the error every interval and delivery resumes when a define fixes the
cause — the run keys make even a replayed batch idempotent. Returns how
many messages delivered.

The tick calls it once per `cb_triggers` row, so a stalled trigger never
blocks the others. The trigger row is locked `FOR UPDATE SKIP LOCKED`: one
deliverer per trigger, and a concurrent tick skips instead of queueing.
Raises `IRD03` when the stream schema is absent.

### _cb_job_prune_runs

```sql
_cb_job_prune_runs(batch_size int DEFAULT 1000) RETURNS bigint
```

Deletes terminal runs older than their birth job's `retention`, together
with their step, attempt and signal rows — oldest first, up to `batch_size`
runs per call. Runs of a job whose retention is `cb_forever()` are kept.
Live runs are never touched: a queued or waiting step is its own delivery,
so a parked run pins only its own rows and pruning can never wedge it.
Returns the number of runs deleted.

For archiving before rows are pruned, export with a watermark query and
write to your own storage:

```sql
SELECT * FROM cb_job_runs
WHERE finished_at IS NOT NULL AND finished_at > $watermark
ORDER BY finished_at, id
LIMIT $batch_size;
```

## New steps, waits and barriers

`steps` carries the handler's buffer as JSON: `[{name, input,
waits_for_steps, waits_for_signal}]`. The engine validates each `name`
against `cb_jobs` and stamps
the queue from the definition, so a worker adds steps with zero knowledge
of queue layout. Because the steps are written by code, nothing is
defaulted: every step states both booleans, and a missing one raises
`IRD01` — a misspelled key looks the same as an omitted one, and defaulting
would turn that bug into a step that silently waits for nothing.

Each new step states two waits, in the same words the statuses answer
with:

- `waits_for_steps` — run only once everything the run owes has completed
  **successfully**. On any give-up the waiting step is canceled, so it
  never starts.
- `waits_for_signal` — run when a payload arrives for the step's name, via
  `cb_job_signal`.

A step with both `false` is claimable at once — its parent has already
finished, so a sequential chain needs no bookkeeping.

**Status says what a step waits for.** A new step lands directly in one of
three statuses: `queued` (waits for nothing), `waiting_for_steps`, or
`waiting_for_signal`. A step that asked for
both waits for the steps first, then the signal — a signal that arrives
early is buffered, never lost, so a step is only ever waiting for one
thing at a time and its status names it. The `waits_for_signal` boolean is
stored on the row exactly as the wire stated it and is never updated; the
payload lands in `signal_input`, which `cb_job_start` returns.

**Composition.** The two waits combine with no special case. The
approval-after-fan-out shape:

```json
[
    {"name": "resize",  "input": {"img": 1}, "waits_for_steps": false, "waits_for_signal": false},
    {"name": "resize",  "input": {"img": 2}, "waits_for_steps": false, "waits_for_signal": false},
    {"name": "publish", "input": {},         "waits_for_steps": true,  "waits_for_signal": true}
]
```

`publish` runs when both waits are over: the resizes (and everything else
the run owes) are done, **and** someone called `cb_job_signal(run_id,
'publish', payload)`. A signal sent before the fan-out drains buffers in
the run's slot and is consumed at the phase dispatch — arrival order does
not matter.

**The barrier is run-wide.** `waits_for_steps` waits for everything the run
still owes — `steps_remaining` reaching zero — not just for its siblings in
the same `steps` array. Two consequences worth spelling out:

- Position in the buffer carries no dispatch meaning. A barrier added
  *between* two immediate steps waits for both of them — and for a step
  some other branch of the run queued earlier.
- When the count reaches zero, **all** `waiting_for_steps` steps dispatch
  together as the next phase, and the run finalizes only when no step is
  left. Chained barriers therefore run as phases: fan-out, barrier, more
  fan-out, barrier again — with no special case.

A step in `waiting_for_signal` **counts** as owed work: barriers and the
run's completion wait behind an unanswered signal (cancel the run if it
will never come). A step in `waiting_for_steps` does not count — which is
why it can never block its own dispatch.

## Old names, new names

Both schemas are installed during the transition — the old root-schema
functions via `migrations/`, the job module via `jobs/migrations/` — and no
names collide. But several pairs are near-anagrams: the old names put the
verb first (`cb_run_task`), the new names put the module first
(`cb_job_run`). When completing by hand, check the prefix: everything in
the job module starts with `cb_job_`.

| Old | New | What changed |
| --- | --- | --- |
| `cb_create_task`, `cb_create_flow` | `cb_job_define` | One noun: a flow is a job whose steps add more steps; no flow object exists. |
| `cb_delete_task`, `cb_delete_flow` | — | Definitions converge on deploy; removing one is a deliberate op (raw `DELETE`). |
| `cb_create_queue` | `cb_job_define_queue` | A queue is a pool of claim and retry terms, not a message box. The message-queue half of the old API (`cb_send`, `cb_read`, `cb_publish`, `cb_bind`) is the stream module. |
| `cb_run_task`, `cb_run_flow` | `cb_job_run` | Plus dedup and lookup by `key`. |
| `cb_cancel_task`, `cb_cancel_flow` | `cb_job_cancel` | |
| `cb_wait_task_output`, `cb_wait_flow_output` | — | Read `cb_job_runs`; the Go side polls it (`jobs.WaitForOutput`). |
| `cb_signal_flow` | `cb_job_signal` | Buffered; returns a boolean, never raises; the payload arrives beside the input. |
| `cb_claim_tasks`, `cb_claim_steps`, `cb_claim_map_tasks` | `cb_job_claim` | One claim for everything, per pool, queue-array in one call. |
| — | `cb_job_start` | New: starting is its own call and spends the attempt. |
| `cb_hide_tasks`, `cb_hide_steps` | `cb_job_extend` | The lease replaces hiding; one call covers every held step. |
| — | `cb_job_release` | New: hand back an unstarted claim, no attempt spent. |
| `cb_complete_task`, `cb_complete_step` | `cb_job_complete` | New steps and run output ride the completion. |
| `cb_fail_task`, `cb_fail_step` | `cb_job_fail` | Retry or give up, one call; policy from the pool row. |
| `cb_claim_task_on_fail`, `cb_complete_task_on_fail`, `cb_fail_task_on_fail` (and the flow trio) | — | `on_fail` is an ordinary step the engine adds at give-up; no separate machinery. |
| `cb_create_task_schedule`, `cb_create_flow_schedule` | `cb_job_define_schedule` | Interval schedules; cron specs return later. |
| `cb_delete_task_schedule`, `cb_delete_flow_schedule` | `cb_job_delete_schedule` | |
| `cb_execute_due_task_schedules`, `cb_execute_due_flow_schedules` | `_cb_job_run_scheduled` | On the module's tick. |
| `cb_gc`, `cb_purge_task_runs`, `cb_purge_flow_runs` | `_cb_job_prune_runs` | On the module's tick; retention is per job, set at define. |
| `cb_bind_task`, `cb_bind_flow` | — | Triggers (M4c): a declared crossing from a stream to `cb_job_run`. |

---

# SQL API — the wire module

This is the SQL contract of the wire module (`wire/`): delivery of server
events to web clients. The signatures and comments in
`wire/migrations/00001_wire.sql` are the authority; this document is their
reference. Everything in the module starts with `cb_wire_`; the old
root-schema names (`cb_notify`, `cb_notifications`, the `cb_wire` channel)
stay live beside it until the old schema is dropped, and nothing here
reuses them.

**What the module does.** Two delivery stories with independent storage.
An **ephemeral event** is a `pg_notify` on the module's bus and nothing
more: every wire in every process delivers it to its local SSE connections
and Listen handlers, and a process that is down misses it — at-most-once
by design. A **durable notification** is a row in an identity's inbox
(`cb_wire_inbox`): the identity's clients read it by poll, ack what they
rendered, and the module's tick deletes what the identity is done with.
The row is a perishable pointer to a durable fact — the result it points
at lives elsewhere; the row is only the prompt to look.

**The two channels.** Both fixed — channels scale with the declared
catalog, payloads carry the runtime coordinates:

| Channel | Payload |
| --- | --- |
| `<schema>.cbw` | the whole event: JSON `{sent_by, topic, message}` |
| `<schema>.cbw_inbox` | the identity whose inbox grew; its clients re-poll |

**Timestamps, not statuses.** An inbox row carries three timestamps, each
set once and never cleared: `created_at` (the row exists), `seen_at`
(rendered in the identity's list — the unseen count drives badges),
`read_at` (the identity opened or acted on it — drives item styling).
Reading implies seeing: both mark-read functions stamp `seen_at` too, so
an opened row leaves the badge count.

### cb_wire_notify

```sql
cb_wire_notify(topic text, message text DEFAULT NULL, sent_by text DEFAULT NULL)
    RETURNS void
```

Sends an ephemeral event on the bus channel. Nothing is stored. NOTIFY
fires on commit, so a rollback sends nothing. The payload must fit
NOTIFY's 8000-byte limit — send a pointer to state, not the state; an
oversized payload raises in the caller's transaction. `sent_by` names the
sending wire so it can skip the echo of what it already delivered locally.

### cb_wire_notify_durable

```sql
cb_wire_notify_durable(identity text, topic text, message text DEFAULT NULL,
                       expires_at timestamptz DEFAULT NULL)
    RETURNS bigint
```

Appends a row to the identity's inbox and nudges that identity's connected
clients (`cbw_inbox`) in one body, so a caller in any language gets both.
Returns the row's id, the poll cursor value. Callable inside the caller's
transaction: the row commits atomically with the app's writes and the
nudge fires only on commit — a rollback delivers neither. Exactly-once in
the store, at-most-once on the nudge; a client that misses the nudge finds
the row on its next poll. `expires_at` is the relevance window and always
wins over the retention tiers; it must lie after the insert. An empty
`identity` raises `IRD01`: the inbox is identity-keyed, and a row no
identity can address is meaningless.

The caller holds the identity as a value — a handler knows which user
asked for the work it just finished. Nothing is extracted from topics.

### cb_wire_mark_seen_until / cb_wire_mark_seen

```sql
cb_wire_mark_seen_until(identity text, id bigint) RETURNS bigint
cb_wire_mark_seen(identity text, ids bigint[]) RETURNS bigint
```

The acks. `mark_seen_until` is the bounded watermark: it marks the
identity's unseen rows with id at or below `id` as seen and returns how
many. The bound is load-bearing — it must not mark rows that arrived
between a reader's fetch and its ack — and the watermark is whole-inbox
scope only: one inbox holds several topic subsets whose ids interleave,
and a range would clobber a sibling subset's unseen rows. Subset-scoped
acks use `mark_seen`, which marks exactly the named ids.

### cb_wire_mark_read / cb_wire_mark_read_until

```sql
cb_wire_mark_read(identity text, id bigint) RETURNS boolean
cb_wire_mark_read_until(identity text, id bigint) RETURNS bigint
```

The read verbs. `mark_read` marks one row the identity opened or acted
on, stamping `seen_at` too when the row was never seen; each timestamp
keeps its first value. Returns whether the row exists — marking an
already-read row changes nothing and still returns true.
`mark_read_until` is "mark all as read": every unread row at or below the
id turns read (and seen); returns how many.

### Reading the inbox

Rows are the truth, so the poll is a plain `SELECT` — the Go side's
`wire.ReadUnseen`:

```sql
SELECT id, identity, topic, message, created_at, seen_at, read_at, expires_at
FROM cb_wire_inbox
WHERE identity = $1
  AND id > $2              -- the caller's cursor; 0 = from the start
  AND seen_at IS NULL
  AND (expires_at IS NULL OR expires_at > now())
ORDER BY id
LIMIT $3;
```

Ids are assigned at insert, not at commit, so a row from a
still-uncommitted transaction can surface with an id below a cursor a
reader already advanced past. A fresh poll (cursor 0) repairs that, and
the badge count never uses the cursor.

### _cb_wire_prune_inbox

```sql
_cb_wire_prune_inbox(read_older_than interval, seen_older_than interval, max_age interval)
    RETURNS bigint
```

Engine-internal, on the module's tick (`wire.StartTicker` in Go; defaults
read 30d, seen 90d, max age 365d). One `DELETE`: a row leaves when its
explicit `expires_at` has passed — seen or not, a stale prompt is not
worth keeping — or when the identity is done with it: read longer ago
than `read_older_than`, seen longer ago than `seen_older_than`, or older
than `max_age` outright. A NULL timestamp fails its age comparison, so a
row that was never seen and has no expiry lives the full `max_age` — it
waits to be seen. Wire has no definitions table; the windows are the
caller's arguments, configuration per app.

## Old names, new names

| Old | New | What changed |
| --- | --- | --- |
| `cb_notify` | `cb_wire_notify` | Channel `cb_wire` → `cbw`; the reserved `catbird.%` topic guard drops — no system producer publishes on this bus (engine events ride `cbs_*`/`cbj_*`). |
| `cb_notify_durable` | `cb_wire_notify_durable` | The identity-addressed nudge rides in the same body; `collapse_key` does not port (no customer) — keep-newest collapse is a recorded deferred design. |
| `cb_notifications` | `cb_wire_inbox` | Gains `read_at`; loses `collapse_key`. |
| `cb_mark_seen_until`, `cb_mark_seen` | `cb_wire_mark_seen_until`, `cb_wire_mark_seen` | |
| — | `cb_wire_mark_read`, `cb_wire_mark_read_until` | New: the seen/read distinction. |
| `cb_gc` (notification sweep) | `_cb_wire_prune_inbox` | Retention tiers replace expiry-only deletion; on the module's own tick. |
| `cb_wire_nodes`, `cb_wire_presence` | — | Presence does not port (no customer); deferred with its design slot. |
