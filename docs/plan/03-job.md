# 03 — job: the engine on rows

> Revised 2026-07-15 (D39–D43). The vocabulary converges on **job**: the
> claimable work row is a job, a flow is implicit — a job that enqueues more
> jobs — and the run is the group and the record. Jobs are declared
> individually and globally named; queues are global pools; **triggers** (§8)
> make events create jobs declaratively. The 2026-07-14 design (D34, D38)
> carries over whole — the step row claimed directly, the one-counter failure
> law — only the words and the scopes changed. Package `jobs` depends on the
> kernel only (`internal/ticker`, `internal/migrate`, `Conn`), not on
> `streams`; the two modules install and run independently, and the stream
> layer needs **zero changes** for jobs to exist. Packaging per D41: the
> trigger is a feature of this module (§8), and shared pure functions
> (backoff, cron-next) live in the kernel's SQL unit.

**One concept.** A run is a group of job executions, and every execution may
atomically enqueue follow-on jobs when it completes — "a flow is a job that
enqueues more jobs" (D31, D39), so no flow object exists anywhere: not in the
API, not in the schema, not in the vocabulary. A task is not a thing either:
a job whose handler enqueues nothing is just a job, and one registration
shape covers both (§5). Own migrations, version table `cb_job_migrations`.

**Rows are the truth — and the transport (D30, D34).** There is no event log,
no projection, and no message either: the job row that records the work *is*
the unit a worker claims. `cb_job_complete` applies everything — the outcome,
the spawned jobs, dispatch, the remaining count — in the completion
transaction, and the spawned rows are immediately claimable. What the old
designs wrote three times per edge (a step row, a ready message, an assigned
position), this design writes once. Job-to-job latency is notify + claim —
no assigner leg, no tick floor; the poll interval is the safety net (D17).

**One counter (D38).** The job row's `attempt` column counts *starts* — every
time `cb_job_start` hands the job to a handler, whatever later becomes of
that execution. A verdict, a crash, a graceful restart of a running handler:
each consumed a start. That one number is the whole retry budget, checked
wherever an execution's fate is decided. There is no crash counter, no header
bookkeeping, and no second exhaustion road: silence is just a lease that
lapsed, and the lapsed row carries its own count.

## 1. The work table — jobs are the messages

- A spawned job inserts as a row with `claimable_at = now()` (or `now() +
  delay`) and is claimable the moment its transaction commits. **One
  timestamp column, `claimable_at`, carries visibility, lease, and backoff**:
  spawn sets it to when the job may first be handed out; claim sets it to
  `now() + claim_ttl` (the lease deadline); extend pushes it; fail sets it to
  `now() + backoff(attempt)`; a crash simply lets it lapse.
- **Wakeups** ride the old engine's proven contract: the engine fires
  `pg_notify` on the queue's channel (`cbj_<queue>`) with the earliest
  `claimable_at` in the payload; the notifier (M5) parses it and wakes or
  arms a timer — this is `worker_notifier.go`'s existing protocol, pointed
  at new tables, and the channel being the pool means a `payments` worker
  never wakes for `main` spawns. Until M5, workers poll (D17). A second
  channel per birth job (`cbjr_<job>`) carries run-terminal events (payload
  `<run_id>:<status>`) for `WaitForOutput` and the dashboard.
- **Queues are global pools.** A job's `queue` (default `main`) partitions
  claiming and carries retry terms (§6). Pools are deliberately cheap: one
  line declares one, jobs opt in with `WithQueue`, a grouped declaration
  gives a family a shared pool as sugar (§5), and claim and extend take an
  **array of queues**, so a worker serving many small pools pays one round
  trip. No streams, no filters, no topics — the claim predicate is the
  routing.
- The hot index is partial: `(queue, claimable_at) WHERE status IN
  ('queued', 'started')` — terminal rows leave it, so the index holds only
  the working set. `queue` and `name` are denormalized onto the job row at
  spawn so the claim query joins nothing (the hot-path rule, kept).

## 2. Tables

Seven tables, no edges — there is no deps table (D31). None of the names
collide with anything the old schema created (no old table is named
`cb_jobs` or `cb_job_*`), so the job module shares a database with the old
code through the transition and its test suite runs on the shared `cb_tst`
like every other module (05).

- `cb_job_defs (name PK, queue, on_fail, retention NOT NULL)` — one row per
  declared job: the global authority spawns are validated against and the
  routing map in one. `queue` names the job's pool (NULL means `main`) and
  is stamped onto spawned rows by `cb_job_complete` — a typo'd spawn raises,
  fails the spawning handler, and is given up on through the normal budget (§5); a
  foreign worker spawns with zero knowledge of queue layout. `on_fail`
  (naming a declared job) and `retention` matter when the job is a run's
  **birth job**: the run inherits both (§3); on a job only ever spawned
  mid-run they are inert config.
- `cb_job_queues (name PK, claim_ttl, claim_batch_size, max_attempts,
  backoff_kind, backoff_base, backoff_max)` — the retry and claim terms per
  pool, written whole by `jobs.Define` (D26: config deploys with code and
  must converge). The migration seeds the `main` row, so a bare install
  works; every other pool is declared, and `main` itself is redeclarable
  like any pool (§6).
- `cb_job_schedules (name PK, job, every, catch_up, input, next_at)` —
  interval schedules for scheduled runs (`every` is a fixed duration), the
  same shape and define semantics as the stream layer's schedule table; cron
  specs are a later addition in both, deferred. Delivered by the job
  module's tick: a due row calls `cb_job_run(job, input)` and re-arms
  `next_at` in one transaction — exactly-once per slot by construction, no
  message, no translation.
- `cb_job_runs (run_id PK, job, key, status, input, output, error,
  jobs_remaining, next_job_id, created_at, finished_at)` — status
  `running | failing | completed | failed | canceled` (text + CHECK, D20).
  `failing` means the outcome is already decided (failed) and only the
  `on_fail` chain may still execute (§3). `job` is the birth job — the
  run's `on_fail` and retention are read from its def. `UNIQUE (job, key)`
  (constraint `cb_job_runs_job_key_key`) — `key` is the dedup point *and*
  the app-key lookup in one column, scoped per birth job; NULLs are
  distinct. `jobs_remaining` counts the jobs the run still owes (§3).
  `next_job_id` mints per-run job ids — the run row is locked in every
  engine call anyway, so the counter is free, and it gives dense ids and a
  clean `0` sentinel for "spawned at birth".
- `cb_jobs (PK (run_id, job_id), queue, name, parent_job_id NOT NULL,
  ordinal NOT NULL, status, dispatch, input, signal, output, error, attempt
  NOT NULL DEFAULT 0, claimable_at, worker, created_at, finished_at)` — the
  work table. `signal` is the satisfied payload of a signal-gated job,
  NULL until then (§3). Status `waiting | queued | started | completed | failed |
  canceled`; `dispatch` is `immediate | all_done | signal |
  all_done_signal` — two independent gates combined into one word (§3),
  the same in the Go options, the spawns JSON and this column. Replay identity
  is the plain tuple: `UNIQUE (run_id, parent_job_id, name, ordinal)`
  (constraint `cb_jobs_identity_key`), where **`ordinal` is the spawn's
  zero-based index in its parent's Plan buffer** — deterministic across
  replays because the buffer is replayed whole. `parent_job_id` is `0` for
  the birth job. A partial unique index `(run_id, name) WHERE dispatch IN
  ('signal', 'all_done_signal') AND status NOT IN ('completed', 'failed',
  'canceled')` enforces §3's signal-name rule race-free.
- `cb_job_attempts (PK (run_id, job_id, attempt), worker, started_at,
  finished_at, outcome, error)` — per-attempt history *and* the fence
  record; kept when the run turns terminal. `outcome` is `completed | failed
  | NULL`, and NULL is recorded silence: a start that never reported — a
  crash, or a restart that superseded it.
- `cb_job_signals (PK (run_id, name), payload, created_at)` — the signal
  buffer, one slot per name: a second signal for a name nobody consumed yet
  overwrites the slot (last signal wins); matching consumes it — deletes the
  row.

Three foreign keys tie children to their run — `cb_jobs.run_id →
cb_job_runs`, `cb_job_attempts → cb_jobs`, `cb_job_signals → cb_job_runs`.
They are cheap because each is checked only at insert, inside a transaction
that already holds the run lock, and the key columns never change
afterward — the hot claim/start/complete UPDATEs pay nothing; they also
turn the janitor's children-first delete order from convention into a
constraint. The two config references deliberately have **no** FK:
`cb_jobs.queue → cb_job_queues` and `cb_jobs.name → cb_job_defs` would take
a KEY SHARE lock on one shared hot config row per spawn (multixact churn at
volume), and `cb_job_complete` already validates both with better errors —
the migration carries a one-line comment at each spot saying so, so nobody
adds them later.

Lookups — `WaitForOutput`, status, lookup by app key, the dashboard — read
these rows directly. There is nothing else to read, and nothing else exists:
no message copy, no position, no stream.

## 3. Lifecycle

**The fence, stated once.** Every engine function follows the same two-step
guard. First it locks the run row (`FOR UPDATE`) and checks the run's status:
`running` or `failing` for start / complete / fail / signal (`failing` admits
the `on_fail` chain — the give-up canceled everything else, so no per-job
marking is needed) and for cancel (in a `failing` run, cancel stops the
cleanup, not the verdict — below). Anything else is a silent
no-op returning false (signal raises instead, below). Then the job guard:
complete and fail require `(status = 'started', attempt = $attempt)`; a
mismatch means the caller was superseded, and nothing happens. Attempts are
minted by `cb_job_start` only. One lock ordering — run row first, then job
rows — means no deadlocks between engine calls; it is also why giving up happens
in start and fail, never in claim (claim locks job rows without the
run lock, §4).

**Birth.** `cb_job_run` inserts the run (`jobs_remaining` = 1,
`next_job_id` = 2) and its birth job (job_id 1, parent 0, ordinal 0,
status `queued`, `queue` from the job's def, input = the run input,
`claimable_at` = now or now + delay), notifies the queue's channel — nothing
else. Any declared job is an entry point: you run the job you mean, and the
run's `on_fail` and retention come from that job's def. It is callable on a
`Conn`, so an application can enqueue a run in the same transaction as its
own writes — the blob-GC / ingest pattern, validated in production. Dedup is
the run row itself: the `ON CONFLICT … DO UPDATE … WHERE FALSE` +
`UNION ALL` pattern (do not simplify), on constraint
`cb_job_runs_job_key_key`; an existing run — live or terminal — returns its
id with `existing = true` and inserts nothing. The dedup window is the birth
job's retention.

**Scheduled runs.** A row in `cb_job_schedules` (§2). The module's tick
delivers due rows — `cb_job_run` plus re-arm in one transaction — so a fire
is exactly-once per slot with no key gymnastics, and the birth job lands on
its own queue like any other birth. `catch_up` keeps the stream scheduler's
semantics (`skip` | `all`).

**Dispatch is two gates, and the remaining count.** A spawn carries up to
two independent gates: `all_done` (wait for the current phase to drain) and
`signal` (wait for a payload). Dispatch is their combination — `immediate`
means no gates, and the Go options `OnAllDone()` and `OnSignal()` compose
into `all_done_signal`, the approval-gate-after-fan-out shape the old
engine expressed as dependencies plus a signal. (The sequential half of
that shape needs no gate at all: dependency-on-parent is inherent in
spawn-at-completion.) `jobs_remaining` counts the jobs the run still owes —
queued, started, or waiting on a signal. The arithmetic, exactly, in
`cb_job_complete`:

```
jobs_remaining := jobs_remaining − 1 + count(inserted spawns with no all_done gate)
```

A spawn with no gates inserts its job as `queued`, claimable at once — the
parent is complete by construction, so a sequential edge needs no
bookkeeping beyond the spawn itself. A gated spawn inserts as `waiting`. A
job with an **unsatisfied `all_done` gate** stays outside the count until
it dispatches — which is why it can never block its own dispatch; a
signal-only job **counts** — barriers and the run's completion wait behind
an unanswered signal. When a completion brings `jobs_remaining` to zero: if
`all_done`-gated jobs are waiting, **all of them** dispatch together as the
next phase — the gate clears, and each becomes `queued`, claimable now (a
buffered signal slot is consumed on the spot), unless its signal gate is
still unanswered, in which case it stays `waiting` and **now counts**;
`jobs_remaining := count(dispatched)`. Otherwise the run finalizes. A
barrier inserted by the very completion that drained the count dispatches
in that same call — the at-zero check runs after the inserts. Phases repeat
with no special case. Buffer position carries no dispatch meaning —
`ordinal` is identity only: a barrier spawned *between* two ungated spawns
in the same Plan waits for both of them, and for everything else the run
still owes. The failure
story the formula guards against: birth sets the count to 1; if the birth
job's completion only decremented, a run whose first job spawns its
successor would finalize with work still queued. The count comes from the
insert's `RETURNING`, never from the argument: if the identity `ON CONFLICT
DO NOTHING` (§4) ever swallows a row, arithmetic that counted requests would
charge the run for a job that doesn't exist, and the run could never drain
to zero.

**Completion and run output.** A run finalizes when `jobs_remaining` reaches
zero and no barrier is left waiting — in a `running` run as `completed`, in a
`failing` run as `failed`. Only executions spawn, so such a run can never
grow again — no explicit "done" call exists. Output resolution, in order: an
explicit `run_output` passed to any `cb_job_complete` wins (last writer
wins — writes are serialized by the run lock; SQL NULL means "not passed",
so an explicit run output cannot be null — wrap it if you mean it);
otherwise the job output of the completion that finalized the run, if it
set one; otherwise null. The
finalizing completion is a single, well-defined call, so the default is
deterministic for every sequential shape; a run that ends in parallel
siblings races for last place and should say what it means with
`SetRunOutput`. Finalization fires the run-terminal notify (`cbjr_<job>`,
payload `<run_id>:<status>`). `WaitForOutput` keeps its API: it polls the run
row until M5, then wakes on the notify with the poll demoted to safety net.

**Failure, retries, giving up — one counter (D38).** A failed execution
reports through `cb_job_fail`: the fence admits it, the attempt row records
the verdict (`outcome = 'failed'`, the error), and one comparison decides:

- `attempt < max_attempts` — the job goes back to `queued`, `claimable_at =
  now() + backoff(attempt)`, worker cleared. The retry is nothing but the
  row becoming claimable later; no copy, no republish, no second object.
- `attempt ≥ max_attempts` — the engine **gives up**, in the same transaction.

Crashes are silence — the handler never reports. A crashed execution's lease
just lapses, and the next claim call repairs it (§4): a `started` row whose
lease has lapsed is rescheduled to `claimable_at = now() + backoff(attempt)`
with its worker stamp cleared — backoff paced by the same counter, since the
lapsed start *was* attempt N. A row already at `attempt ≥ max_attempts` is
repaired to `now()` instead: its only future is the give-up, and pacing a
give-up helps nobody, and `on_fail` fires one claim sooner. When the
repaired row next reaches a worker,
`cb_job_start` checks the same comparison: starting would mint `attempt +
1`, and if `attempt ≥ max_attempts` already, start **gives up instead of
starting** and returns no work. Two call sites, one routine
(`_cb_job_give_up`), one counter, one policy home (the queue row). A
give-up arriving late — the run already `failing` or terminal because a
sibling's give-up came first — hits the fence and no-ops; that is the whole
idempotency story.

`max_attempts` bounds **total starts**, not verdicts: a crash consumed a
start, and so did a graceful shutdown that canceled a running handler — the
job had started, and the fence cannot distinguish a live zombie from a dead
one, so redelivery must mint fresh. Stated plainly: "this job's handler will
begin at most `max_attempts` times." The cost is that frequent deploys spend
starts of long-running handlers — a job like that deserves its own
`WithQueue` with generous terms. The justice half of the same rule: a job
that was **leased but never started** (its worker died before reaching it)
lapses back to claimable with nothing spent — no evidence, no charge. The
worker's graceful path makes both halves explicit: unstarted claimed rows
are released uncharged (`cb_job_release`, §4 — redelivered immediately),
and started handlers get their context canceled and are reported through
`cb_job_fail` with the error `catbird: worker shutdown` — the same one
charge the start already spent, but redelivery is backoff-paced from now
instead of from lease-lapse, and the attempt row records a verdict instead
of silence. Silence remains what it says: nobody reported, because nobody
could.

**The give-up's effects** (`_cb_job_give_up`, one transaction): the job
turns `failed` with its error (for the silent road: "attempts exhausted; last
attempt ended in silence"); every other non-terminal job — queued, started,
or waiting — turns `canceled` (a started sibling's later complete/fail hits
the job guard and no-ops; its handler is reaped by the worker's status
check, below). Then:

- **`on_fail` declared** (on the run's birth job): the run turns `failing`;
  the `on_fail` job is spawned as an ordinary job — parent = the failed
  job, dispatch immediate, input built by the engine as `{job, error,
  input}` — and `jobs_remaining := 1`. The chain then runs under the normal
  rules: the fence admits it because the run is `failing` and everything
  else is canceled; it may spawn, retry on its own terms, even wait on a
  signal; when its chain drains the count to zero the run finalizes as
  `failed`. If the engine gives up on the `on_fail` job itself, the run is already
  `failing`, so `_cb_job_give_up` spawns nothing and the run turns `failed`
  — one rule, no regress.
- **No `on_fail`**: the run turns `failed`, `error` set, run-terminal notify
  fired.

`on_fail` firing on crash exhaustion too is the point: today's OnFail misses
hard worker death, which is why ingest hand-rolled `sweep_stuck_deliveries`.

**Cancel.** `cb_job_cancel(run_id, reason)` — fence (`running` or
`failing`). In a `running` run it flips every non-terminal job and the run
to `canceled`; in a `failing` run it does not change the verdict, it stops
waiting for cleanup — remaining jobs turn `canceled` and the run finalizes
`failed` at once. (Refusing would assume cleanup terminates; an `on_fail`
chain parked on a signal nobody will send would otherwise leave the run
un-finalizable *and* un-cancelable.) Either way the run-terminal notify
fires. Started handlers get best-effort context
cancellation: the worker's handler wrapper checks its own job row on the
extend cadence (D27) and cancels the handler's context when the job is no
longer `started` — one cheap indexed read, covering cancel, a sibling's
give-up, and supersession alike.

**Signals are buffered, and the payload arrives beside the input.** A
signal-gated spawn carries its input like any other spawn; the payload is
delivered separately — stamped into the job's `signal` column at
satisfaction and returned by `cb_job_start` — so a job can be spawned with
context and still wait for an approval.
`cb_job_signal(run_id, name, payload)`: under the run lock, if a
signal-gated job of that name is `waiting` with its signal gate the only
one left, it is satisfied — the payload lands in `signal`, status
`waiting → queued`, claimable now (`jobs_remaining` unchanged: such a job
already counts). Otherwise — no such job yet, or its `all_done` gate still
pending — the payload is buffered in the run's slot for that name
(overwriting an unconsumed older one). The mirror happens whenever a
signal-gated job's other gates clear — at insert for a signal-only spawn,
at phase dispatch for a combined one: a buffered slot, if present, is
consumed on the spot and the job proceeds `queued` instead of `waiting` —
arrival order does not matter. The call
raises only if the run is missing or terminal (`failing` accepts signals: a
cleanup chain may legitimately await an operator). The synchronous
`ErrSignalNotDelivered` is retired (README amendment 12). A signal-gated
job's name must be unique among the run's unresolved jobs — the partial
unique index (§2) enforces it at spawn time; violating it fails the
spawning completion, which is a handler bug and is given up on through the normal
budget (§5).

## 4. The functions — the completion transaction is the engine (D30)

All engine logic is SQL: a thin client in any language calls these functions
and gets identical semantics (D11). Every function except claim and release
begins with §3's fence; handlers never run inside any of these transactions.
Full SQL sketches arrive per function at implementation; what follows is the
contract each sketch must meet.

- **`cb_job_define(job, queue, on_fail, retention)`** plus the queue-terms
  and schedule declarations — the whole-declaration upserts (D26), guarded
  so unchanged declarations write nothing. It checks that `on_fail` (when
  set) names a declared job and that `queue` (when set) has a terms row,
  raising otherwise — a deploy-time error instead of a runtime "spawn name
  not declared" on the first run. Define never deletes a queue row (§6).
  The Go `jobs.Define` performs the whole declared set — defs, pools,
  schedules — in one advisory-locked transaction. Deploy-time setup, not
  part of the worker contract.
- **`cb_job_run(job, input, key, delay) → (run_id, existing)`** — birth,
  §3. Raises if the job is not defined. Callable on a `Conn`.
- **`cb_job_claim(queues, worker) → rows of (run_id, job_id, name,
  lease_at)`** — `queues` is an array: one call serves every pool the
  worker holds handlers for, up to each pool's `claim_batch_size`. The one
  function without the run fence (it locks job rows only, which is why it
  never gives up on anything). No per-call ttl: D23's override worked because the claim
  row stored the resolved terms for extend to renew; a job row has no claim
  row, and D27's loop owns the clock anyway — the queue row is the only
  source of terms. `name` lets a worker refuse a job it holds no handler
  for (release, below); `lease_at` is the deadline, from which a foreign
  worker derives its extend cadence without reading policy tables. One
  statement over the partial index, two effects: **repair** — `started`
  rows with a lapsed lease and a worker still stamped are rescheduled to
  `backoff(attempt)` (`now()` at the attempt limit, §3) and cleared, not
  handed out; **hand out** — eligible rows (`queued`, or `started` with no
  worker — a repaired crash due for redelivery) get the caller's worker
  stamp and a lease of the pool's `claim_ttl`, `FOR UPDATE SKIP LOCKED`, ordered
  by `claimable_at`. Leased-but-unstarted rows whose lease lapses are
  simply eligible again — availability needs no repair and no charge.
- **`cb_job_start(run_id, job_id, worker) → (name, input, signal,
  attempt)`** —
  per claimed job. Fence, then the give-up check (§3): at the attempt
  limit it gives up (no attempt row — nothing started) and returns nothing.
  Otherwise flips the job `queued | started → started` (started-to-started
  because a crashed execution leaves `started` behind), bumps `attempt`,
  inserts the attempt row, and returns what the handler needs — `signal`
  is the satisfied payload for a signal-gated job, NULL otherwise (§3).
  Returns
  nothing when the fence fails — a stale claim of a resolved job — and the
  loop just moves on.
- **`cb_job_extend(queues, worker) → held rows`** — pushes `claimable_at`
  forward on every row the worker still holds in those pools, one
  statement; the loop calls it on the D27 cadence and compares the returned
  set to what it thinks it holds — a missing row means supersession, cancel
  that handler.
- **`cb_job_release(run_id, job_id, worker, pause DEFAULT '0') →
  boolean`** — hands back an unstarted claim, uncharged. The fence is the
  job row alone (`queued`, this worker) — no run lock, like claim; it
  clears the worker stamp and sets `claimable_at = now() + pause`. Shutdown
  releases each unstarted held row with the default `0` — immediate
  redelivery. A worker that claims a job it holds no handler for — a
  rolling deploy declared a new job before this worker's pool restarted —
  releases it with a short pause, so two old workers don't ping-pong it
  until a new worker arrives. False means the job already moved on.
- **`cb_job_complete(run_id, job_id, attempt, output, spawns, run_output)
  → boolean`** — the heart, one transaction, in this order: fence → resolve
  the attempt (outcome, finished_at; job → `completed`, output stored) →
  validate and insert the spawned jobs (names against `cb_job_defs`, queue
  stamped from the def, dispatch words;
  identity `ON CONFLICT DO NOTHING` as defense in depth) → consume signal
  slots for signal-gated spawns as they become eligible (§3) → apply §3's
  remaining formula over the rows the insert
  actually returned (§3) → at zero, dispatch waiting barriers or finalize
  (output resolution, run-terminal notify) → notify each queue that gained
  claimable rows, once, with its earliest new `claimable_at`. False means
  the fence failed and nothing happened.
- **`cb_job_fail(run_id, job_id, attempt, error) → boolean`** — §3:
  verdict on the attempt row, then retry (queued + backoff) or give-up,
  one comparison, one transaction. False = fenced.
- **`cb_job_cancel(run_id, reason DEFAULT NULL) → boolean`** — §3.
- **`cb_job_signal(run_id, name, payload)`** — §3: satisfy or buffer, under
  the run lock; raises only if the run is missing or terminal.

The leaner argument lists are deliberate: claim, start, release, complete
and fail take ids the worker already holds — no stream, topic or header to
know, so nothing to pass wrongly.

One more rule for the sketches: every multi-row UPDATE — extend, the
give-up and cancel sweeps — locks its rows in one order (subselect
ordered by `(run_id, job_id)`, update by PK). The run-first ordering (§3)
serializes the calls that take the run lock; extend takes none, so without a
shared row order it could deadlock against a sweep.

**Cost and latency.** One row insert per edge where the previous designs
wrote a step row *and* a message *and* an assigned position; job-to-job
latency is notify + claim — single-digit milliseconds once the notifier
lands, one poll interval before that — with no assigner tick in the path at
all. Serialization did not disappear, it moved where it always was: every
engine call takes the run-row lock, so sibling starts and completions in one
run serialize on it. The watch item is unchanged — **connection occupancy**
on wide fan-in (~1k completions/s per run), with the same mitigation ladder:
keep the completion transaction tiny → batch sibling completions into one
call (an array argument, the `PublishMessages` precedent) → deferred drain
detection. The wide-map stress test in M4b decides whether the first rung
suffices. The jobs table is update-hot (claim, start, complete each touch
the row), so it inherits the autovacuum note that 01 §10 attaches to message
partitions; LIST partitioning of `cb_jobs` by queue is the documented
escape hatch if one pool's churn ever dominates.

## 5. The Plan DSL (D10, D31)

The key mechanic is unchanged: **`*Plan` is a buffer.** Every method buffers;
nothing blocks; the buffer commits with your completion (`cb_job_complete`,
§4). A handler that crashes mid-way submits nothing, and at-least-once
redelivery replays it cleanly against the spawn identity (§2).

Declarations are nouns, and one slice is the single source both deploy and
runtime read: `jobs.Define(ctx, conn, defs...)` converges the whole set in
one advisory-locked transaction — no half-deployed state, a new job and its
pool arrive together — and `jobs.NewWorker(pool, defs...)` registers the
same items' handlers and validates coverage against them. The handler
signature decides everything: a handler that takes a `*jobs.Plan` can
enqueue follow-ons; one that doesn't is the whole story — its return value
becomes the run output. `jobs.External(name, opts...)` writes a def with no
handler — which fleet holds a handler is deployment knowledge, deliberately
absent from the schema; coverage validation is the fleet-symmetric
enforcement (§7). (`jobs.Job`, the item constructor, occupies the package's
best identifier on purpose; exported types pick other names — `Def`,
`Plan` — and user model types are unaffected behind the package qualifier.)

```go
var defs = []jobs.Def{
    jobs.Queue("payments",
        jobs.WithRetry(5, jobs.FullJitter(time.Second, time.Minute))),

    jobs.Job("reserve", func(ctx context.Context, p *jobs.Plan, in Order) error {
        res, err := reserve(in.Items)
        if err != nil { return err }        // → cb_job_fail; retry or give up (§3)
        p.Spawn("charge", Charge{Order: in, Reservation: res})
        return nil                          //   parent hands its results forward
    }, jobs.OnFail("notify_ops")),          // the run's failure job, when
                                            //   "reserve" is the birth job (§3)
    jobs.Job("charge", func(ctx context.Context, p *jobs.Plan, in Charge) error {
        for _, parcel := range split(in) {
            p.Spawn("ship", parcel)         // fan-out: N siblings, parallel
        }
        p.Spawn("confirm", in.Order, jobs.OnAllDone())  // barrier: dispatches
        return nil                          //   when jobs_remaining drains to zero
    }, jobs.WithQueue("payments")),         // own pool + own retry terms (§6)
    jobs.Job("ship", shipFn),               // plain func(ctx, Parcel) (Out, error):
    jobs.Job("confirm", confirmFn),         //   no Plan, enqueues nothing
    jobs.Job("notify_ops", notifyFn),       // receives {job, error, input}
    jobs.External("transcode",              // def row, no handler — handled by
        jobs.WithQueue("gpu")),             //   another fleet (§7)
}

jobs.Define(ctx, conn, defs...)             // deploy: converge config, one transaction
w := jobs.NewWorker(pool, defs...)          // runtime: register handlers, check coverage

runID, _, err := client.RunJob(ctx, "reserve", order, jobs.WithKey(order.ID))
```

Surface, complete: `p.Spawn(name, input, opts...)` — the **one** Plan verb —
with the spawn options `jobs.OnAllDone()` and `jobs.OnSignal()` (combinable —
§3's gates) ·
`p.SetOutput(v)` / `p.SetRunOutput(v)` · `jobs.Input[T](p)` /
`jobs.Signal[T](p)` (the satisfied payload, NULL until then) /
`jobs.OutputOf[T](p, "name")` (read from the job rows, typed via the
existing reflection utilities; a multi-instance name yields a slice). The
rule: **a spawn dispatches immediately unless an `On*` option defers it**
(§3). There is no entry-point declaration — `RunJob` names the job you mean
— and no special failure job: `OnFail` on a declaration names another
declared job, consulted when that declaration is a run's birth job.

The naming rule behind the options: name the **dispatch condition from the
job's own viewpoint**, never the engine's mechanics, and nothing may sound
like it acts now. The `On*` idiom never reads as blocking — `OnClick` doesn't
wait. Rejected on that rule: `Then`/`Next` (`p.Then(a); p.Then(b)` misreads
as sequential), `Gather` (sounds like collecting right now), `Join`
(`thread.join` blocks), `AfterAll`. Also rejected: any flow or group noun in
the API — the group is a declaration-time convenience, not an engine
concept, and giving it a name would grow it back into one. The dispatch
words are the same in Go, the spawns JSON and the column, and the two
gates compose: `immediate` | `all_done` | `signal` | `all_done_signal`.

**The input rule:** a job's input is exactly what it was spawned with, with
one stated exception where the engine supplies it: an `on_fail` job's input
is the engine-built `{job, error, input}`. A signal-gated job is no
exception: its input is its spawn input, and the signal's payload arrives
beside it — `jobs.Signal[T](p)`, NULL until satisfied (§3). Spawning
happens at completion, so a parent passes its results forward *in* the
spawn input; a barrier, whose siblings' outputs didn't exist when it was
spawned, reads them with `OutputOf`. This is also what `input` and `signal`
mean in the SQL contract (§7). (A trigger-born run's input is the message
envelope — the trigger is the caller there, not an exception here, §8.)

**Validation is layered.** The Go side fails fast where it can: `Spawn`
panics on a name not declared — a panic becomes a failed attempt (D27),
close to the bug. The SQL
side enforces the same rules for every language (§4): an invalid spawn
raises, failing the spawning completion — the worker reports it through
`cb_job_fail` like any handler error, and the normal budget applies, so a
deterministic bug is given up on after `max_attempts` backoff-paced tries.
Deliberately no fast-path give-up flag: that would be a second exhaustion
road (D38), and the cost of walking the normal one is a few wasted retries
on a bug the attempt rows make visible immediately.

**Dead concepts become user patterns** (D31, D32, D39): edges (`After`,
`*SpawnRef`) — a sequential edge is just a spawn at completion, fan-in is
`OnAllDone`; map — spawn N in a loop plus a barrier; conditions — a Go `if`
before `Spawn` (the engine never guards); `Optional[T]` — you only spawn
what exists; `AwaitSignal` — the `OnSignal` spawn option; `CompleteEarly` —
a return *is* completion (`p.SetOutput` + `return nil`); the task engine and
the flow object — one registration shape, the Plan parameter optional. A
one-job run is also the recommended shape for everything the old world put
on a work queue — it *is* the queue, with a handle (README, D37).

**The worker** runs on the kernel's claim-loop skeleton (`internal/`, D27,
D41) — the same skeleton the stream consumer uses: the modules share the
loop's mechanics while each owns its contract. One `select` waits on three
things at once: the handler
returning, an extend tick that pushes the job's lease deadline out while the
handler is still running, and a stop-or-cancel signal. Because one loop both
runs the handler and extends the lease, a worker that hangs or dies stops
extending, and its work falls to another worker. The loop: claim a batch
across its queues → per job: start → handler → complete/fail — extending on
the cadence; on shutdown it releases unstarted leases and fails canceled
handlers (§3). Handlers are registered per job name and invoked via the
reflection utilities (ported from `task.go`). At startup the worker checks
coverage: for every queue it claims, it must hold handlers for **all** jobs
`cb_job_defs` routes there, or it refuses to start — a claim is
indiscriminate within its pool, so partial coverage would strand jobs (§7).
Coverage is checked at startup only, and the defs converge on deploy while
old workers still run — the release-with-pause path (§4) is what carries a
new job across that skew window.

```go
type Plan struct {
	spawns    []spawnSpec     // {name, input, dispatch: immediate|all_done|signal|all_done_signal}
	output    json.RawMessage // optional — side-effect-or-error stays legal
	runOutput json.RawMessage
}
```

## 6. Retry terms — pools and pacing

A job's terms are its pool's row in `cb_job_queues` (D4: policy in the
database, applied by SQL): `max_attempts`, the backoff triple, `claim_ttl`,
`claim_batch_size` — written whole by `jobs.Define`, so a redeploy converges
them. Terms are pool properties, full stop: `WithRetry` is an option of
`jobs.Queue`, never of a job — a job that needs its own terms (a
rate-limited payment call, a GPU stage) declares its own pool and routes to
it with `WithQueue`. Pools being global and cheap is the production
isolation story: the migration seeds `main` for the bare install, and every
family that matters declares its own pool with its own terms — one line —
so one family's backlog or backoff never paces another's. Define never
deletes a queue row — a row dropped from the declaration may still have
non-terminal jobs routed to it, and a stale terms row is inert config, so
removing one is a deliberate op.

`main` is seeded with stated terms — `max_attempts` 3, full-jitter backoff
1s–1m, `claim_ttl` 30s, `claim_batch_size` 10 — and stays an ordinary row:
`jobs.Queue("main", …)` redeclares it from the app's own code like any
pool; only its existence is guaranteed.

Pacing has two knobs, both live today: **which workers claim the pool**
(fleet sizing per queue) and `claim_batch_size`. Two more are **deferred
with this note**, same trigger for both (evidence from a real workload, not
principle): `max_inflight` — a per-pool concurrency cap, enforced in claim
by refusing hand-outs while the pool's `started` rows are at the cap — and
`rate_limit` — starts per window for rate-limited APIs, a token count on
the pool row, tolerable because claims are per-pool and far rarer than
per-job. Each is one column and one predicate in claim, no new machinery.
Until then a rate-limited API gets a dedicated pool, `claim_batch_size` 1,
a small fleet, and a handler-side limiter.

Nothing here touches the stream layer's policy at all — the two modules'
terms live in their own tables with their own meanings stated in their own
documents. `backoff()` is a pure function in the kernel's SQL unit (D41),
shared by both modules' terms; `cb_cron_next` joins it when cron lands —
one torture-tested implementation serving both schedule tables.

## 7. Cross-language workers (D11) — the differentiator, scoped

Not a rabbit hole, **provided it stays at the SQL API level**. A foreign
worker is ~40 lines against a contract of seven functions, all engine-owned —
documented in `docs/sql-api.md`, which M4a makes the normative spec:

```
cb_job_claim(queues, worker)                  → [(run_id, job_id, name, lease_at)]
cb_job_start(run_id, job_id, worker)          → (name, input, signal, attempt)
                                              --  or nothing (fence, or give-up)
cb_job_extend(queues, worker)                 → still-held rows (D23, D27)
cb_job_release(run_id, job_id, worker, pause) → bool -- unstarted handback, uncharged
cb_job_complete(run_id, job_id, attempt, output, spawns, run_output) → bool
cb_job_fail(run_id, job_id, attempt, error)   → bool  -- retry or give up, one call
cb_job_signal(run_id, name, payload)
```

The loop: claim → per job `cb_job_start` → run the handler →
`cb_job_complete` or `cb_job_fail` — extending on the cadence `lease_at`
implies (no policy table to read), and a
worker that never extends has its slow jobs truthfully counted as crashes
and slowed by backoff — legal, at-least-once, just noisy; the M4b demo
worker is deliberately slower than the claim TTL to prove that path end to
end. `cb_job_release` is politeness, not obligation: a worker that never
releases is legal too — its leases lapse and come back uncharged, just
slower. `attempt` travels from start through complete/fail — the fence's
third column — so each start resolves at most once, and a false return means
the execution was superseded and nothing happened. (`cb_job_run` and
`cb_job_cancel` are equally callable — any client with a Postgres
connection starts and cancels runs; the *worker* contract is the seven
above.)
No headers to parse, no streams to name, no scheduled-message shapes to
recognize — a scheduled run arrives as an ordinary claimed job.

`spawns` carries the buffered Plan as JSON — `[{name, input, dispatch}]`,
the same dispatch words as the column (§2) — and the engine routes
each spawn by `cb_job_defs`, so dynamic spawning is fully available to a
Python worker with zero knowledge of queue layout. The DSL is sugar, not
capability. Explicitly out of scope, same line as before: cross-language
job *definition*, typed payload schemas or registries, SDK parity. The
contract is job name in, JSON in, JSON out, plus the seven calls. Hold that
line.

## 8. Triggers — events become jobs (D40)

The log answers *what happened*; the job answers *what is still owed*; the
**trigger** is the declared crossing between them. D29 rightly deleted
routing — messages are never sent *to* places — but the spine was also
carrying the answer to "events cause work", and when D34 took jobs off the
log, that answer shrank to a hand-written consumer per binding: a deployed
Go loop whose handler calls `cb_job_run`. The trigger makes the common case
declarative — the outbox pattern with zero glue code, which is the
same-database design's front door.

- **A trigger is a row, not a process**: `cb_triggers (name PK, stream,
  pattern, condition, job, deduplicate, start_pos, created_at)` — declared
  whole by `cb_trigger_define` (D26 semantics; `cb_trigger_delete` removes
  one), validated at define time: the stream exists, the job is declared,
  the filter compiles (the D29 topic-pattern and condition languages, the
  same compiler subscriptions and cursors use). Each trigger owns a cursor
  bearing its filter; `start_pos` seeds it (`tail` | `begin` | position).
- **Delivery is a tick on the kernel ticker**, per trigger, one
  transaction: read the cursor's next batch of matching messages, call
  `cb_job_run(job, envelope, key)` per message, advance the cursor.
  Exactly-once event→job creation by cursor semantics — the composition
  rule made mechanical. No deployed consumer code, and cross-language by
  construction: a Python-only shop declares triggers through SQL and gets
  outbox-triggered jobs without writing a consumer loop.
- **The input is the envelope whole** — `{stream, position, topic, key,
  headers, payload}` — the engine-supplies-it rule (§5): when the engine
  builds an input, it is self-describing. Handlers read `.payload`.
- **Every match births one run** by default: the dedup key is
  `<trigger>:<position>`, so creation stays idempotent even across a
  cursor reset. `jobs.Deduplicate()` (column `deduplicate`, default false)
  collapses instead: messages sharing a publish key create one run within
  the dedup window — right for "reindex record X" where fifty updates
  deserve one pending job, wrong for "send confirmation" where every event
  matters; keyless messages still get one run each. The surprise that
  makes it opt-in: the dedup window is the run's retention, so a
  terminal-but-retained run swallows later same-key events —
  deduplicating triggers want short-retention jobs.
- **Failure is loud and ordered.** Run creation can only fail
  deterministically (job undefined, invalid input), and a raise rolls back
  the batch and stalls the trigger at its cursor — visible lag, no silent
  skips, fixed by a define or a deploy. Execution failures are the job's
  own retry / `on_fail` story; the trigger never learns of them.
  Backpressure is the pool's problem by design: a burst of events becomes a
  burst of queued jobs, paced by §6.
- **Packaging**: a feature of this module, not a module of its own (D41).
  `cb_triggers` and the trigger functions live in job's migrations —
  PL/pgSQL bodies are late-bound, so the job schema installs cleanly
  without the stream schema present — and `cb_trigger_define` and the
  delivery tick raise `catbird: stream schema required` at use. The
  composition is one-directional and recorded: job's SQL calls stream's
  public SQL API (cursor read and advance), never the reverse, and the Go
  dependency rule stands untouched. Declared with the jobs it feeds:
  `jobs.Trigger(name, stream, filter, jobName, opts...)` inside
  `jobs.Define`.

The outbox example, end to end: the application publishes `order.created`
in its own transaction — the log *is* the outbox, that's what transactional
publish means — a trigger creates `send_confirmation` exactly once, and the
job executes at-least-once with retries and `on_fail`. Three declarations,
no glue code.

The hand-written subscription consumer remains the right tool when the
bridge needs application logic — transformation, enrichment, conditional
fan-out in code. The trigger is the declarative 90% case, not a replacement
for the composition rule.

## 9. Retention, audit, history (D30)

Rows are the history. A job row per spawn, an attempt row per start —
`cb_job_attempts` is kept when the run turns terminal, and a NULL outcome is
itself a record: a start that never reported (§3). The dashboard's run detail
view reads what actually happened — who started what, which attempts failed
with which errors, what was given up on — without an event log; give-ups are a
`status = 'failed'` query away, which is also all a redrive tool needs (a new
run from the recorded input). What died with the log is replay and
state-as-of-time-T reconstruction, which was never a shipped feature.

Terminal runs are pruned past the birth job's `retention` — D7's convention
exactly: `NOT NULL`, `cb_forever()` (the kernel sentinel) keeps a run
forever, zero or any other negative raises, NULL is never stored; the
default deliberately differs from streams' (30 days, not forever — work
rows are gone when done; the run row is a handle, not an archive). Pruning
is the run row and its job, attempt and signal rows together,
batched deletes by a janitor on the module's tick. **A parked run pins only
its own rows** — a signal job waits with no other artifact in the system,
and a queued job *is* its own delivery, so there is no separate message to
lose and no wedge-by-pruning hazard at any parking duration: the job
outlives exactly as long as its run does, by construction. Whether ancient
non-terminal runs should be canceled by age remains the deferred policy
question; the rows make it a one-line query.

## 10. Stream-layer prerequisite

**None for the engine core.** Claim, start, complete, fail, cancel, signal
and the run lifecycle touch no stream table, no stream function, no stream
migration. `fq`, `fr`, `fd` and `fe` join the retired-codes list without
ever existing; the stream layer's own revisit (01, D35–D38) proceeds
independently, and the job schema installs and runs without the stream
schema present. The one deliberate composition is the trigger (§8) — a
recorded, one-directional exception that raises loudly when the stream
schema is absent. A second is deferred with a note: run-lifecycle events
published to a stream by policy (the audit/chaining feed D36 parked), using
the same pattern when a workload asks. This section exists so the boundary
is a recorded property, not an omission: the previous designs required a
stream-layer prerequisite precisely because work rode the log, and every
seam item in them — family-aware fail paths, marked deliveries, CHECK pins,
header contracts — is deleted rather than relocated.

## 11. Build checklist

Sequencing and exit criteria live in 05 (M4a = single-execution runs, M4b =
spawns, barriers, signals, `on_fail`; M4c = triggers). The build items:

1. The kernel SQL unit (D41): its own migration dir and version table,
   auto-applied by `internal/migrate` before any module's migrations; seeds
   `cb_backoff`; stream's `cb_valid_name` and `cb_forever` move into it in
   the same pass, names unchanged (pre-release edit-in-place, the M3r
   precedent).
2. Migration `jobs/migrations/00001`: the seven tables (§2), text + CHECK
   statuses, named constraints (`cb_job_runs_job_key_key`,
   `cb_jobs_identity_key`, the signal-name partial unique index), the
   partial claim index `(queue, claimable_at)`, the seeded `main` queue
   row, version table `cb_job_migrations`.
3. Functions (§4), M4a scope: `define`, `run`, `claim` (lease repair,
   queue array), `start` (with the give-up check), `extend`, `release`,
   `complete` (raises on non-empty `spawns` until M4b; counts inserted
   spawns via `RETURNING`), `fail`, `cancel`, `_cb_job_give_up` (the
   `failing` machinery ships in M4a; only `on_fail` spawning stays dark
   until a def declares one) — backoff from the kernel unit. M4b: the
   spawn/barrier/signal paths in `complete`, `cb_job_signal`, `on_fail`.
4. The module's tick (kernel ticker): schedule delivery (§3), the
   run-retention janitor (§9).
5. Go: the declaration nouns — `jobs.Job` / `jobs.Queue` / `jobs.External`
   / `jobs.Schedule` / `jobs.Trigger` — plus `Define` (the whole set, one
   advisory-locked transaction); one defs slice feeds `Define` and
   `NewWorker`; the worker
   loop on the kernel's D27 skeleton (extracted to `internal/`, the stream
   consumer rebased on it, suites green — D41) with queue-set claims,
   coverage validation, status-check cancellation and release-plus-fail on
   shutdown (§5); the Plan buffer; reflection utilities ported from
   `task.go` (including the with-Plan / without-Plan handler shapes); run
   lookup by id and by app key; `WaitForOutput` polling, notify at M5.
6. `docs/sql-api.md`: rewrite as the normative worker contract (§7), with
   the old-vs-new name table for the transition, and worked examples for
   gate composition and the run-wide barrier rule (§3) — the two places a
   reader's intuition is most likely wrong.
7. M4c — triggers (§8, D41): migration `jobs/migrations/00002_trigger.sql`
   (`cb_triggers`), `cb_trigger_define` / `cb_trigger_delete` — define-time
   validation plus the loud stream-schema-required check — the delivery
   tick on the module's ticker, `jobs.Trigger` in `Define`.
8. Tests, the semantic core:
   - dedup + key lookup; duplicate complete/fail no-ops (fence).
   - give-up from both roads — the exact inequalities: a verdict at
     `attempt = max_attempts` is given up in `fail`; a lapsed job at
     `attempt = max_attempts` is given up in `start`; total starts never
     exceed `max_attempts`.
   - lease repair: a lapsed `started` row comes back after
     `backoff(attempt)` exactly once (repair is idempotent under racing
     claims), and unpaced (`now()`) at the attempt limit; a
     leased-but-unstarted row lapses back with `attempt`
     untouched; a graceful release redelivers immediately, uncharged.
   - a graceful shutdown mid-handler spends exactly one start, as a verdict:
     the attempt row reads `worker shutdown` and redelivery follows
     `backoff(attempt)` with no lease-lapse wait (§3).
   - the `failing` lifecycle: the give-up cancels siblings, `on_fail`
     receives `{job, error, input}`, its chain may spawn (barriers
     included) and signal, its exhaustion ends the run `failed` with no
     second `on_fail`; cancel of a `failing` run cancels the cleanup chain
     and finalizes `failed` at once.
   - signal slot semantics: signal-before-spawn buffered and consumed;
     overwrite of an unconsumed slot; sequential same-name jobs; duplicate
     signal-job name fails the spawning completion.
   - the additive payload: a signal-gated job spawned with input receives
     both — spawn input unchanged, payload in `signal` (start returns it;
     `jobs.Signal[T]` NULL until satisfied).
   - external defs: `jobs.External` converges like any declaration; a
     worker claiming its pool without the handler refuses to start.
   - the remaining formula: chains, fan-out + barrier, barrier phases
     (including a barrier spawned by the draining completion), signal jobs
     holding barriers and finalization.
   - combined gates (`OnAllDone()` + `OnSignal()` → `all_done_signal`): the
     job dispatches at phase drain and then waits for its signal, counting;
     a signal buffered before the phase drains is consumed at dispatch;
     finalization waits behind a dispatched-but-unsignaled job; the builder
     composes the two options into one dispatch word.
   - supersession via extend: a repaired job's original worker extends,
     the returned set is missing that row, the handler is canceled; its late
     complete returns false and changes nothing.
   - cancel mid-handler: `cb_job_cancel` while a handler runs — the
     status-check wrapper cancels the context, the late complete no-ops.
   - output resolution: an explicit `SetRunOutput` from a non-final job
     beats the finalizing job's output.
   - routing: a `WithQueue` spawn lands in its pool with its terms; an
     undeclared spawn name walks the spawner to give-up through the normal budget
     (§5); worker coverage validation refuses partial pools; a worker
     claiming a job it holds no handler for releases it with a pause,
     uncharged; a queue-array claim serves several pools in one call; a
     grouped `jobs.Define` declares jobs, pools and schedules in one
     transaction.
   - schedules: a due row births exactly one run and re-arms in one
     transaction; `catch_up` semantics ported from the scheduler tests.
   - define convergence: changed terms, defs and schedules apply on
     redeploy; unchanged define writes nothing; an `on_fail` naming an
     undeclared job, or a `queue` without a terms row, is rejected.
   - cancel during a retry gap (job queued with future `claimable_at` →
     canceled, never claimed).
   - triggers (M4c): exactly-once creation — kill the tick mid-batch, the
     batch rolls back, no duplicate runs; an undefined job stalls the
     trigger loudly at its cursor and delivery resumes after the define;
     the position key stays idempotent across a cursor reset; message-key
     deduplication creates one run for N same-key messages; `start_pos`
     honored; only matching messages deliver; the job schema installs
     without the stream schema, and `cb_trigger_define` there raises
     `catbird: stream schema required` (D41).
   - job-to-job latency benchmark (notify + claim — no tick in the path);
     throughput ≥ the old `BenchmarkTaskThroughput` / `FlowThroughput`
     envelope; the wide-map stress test (the D30 watch item).
   - the deliberately slow Python demo worker (M4b) — the worker contract,
     extend included, release skipped (legal), slower than the claim TTL.

The decision-log entries for this design (D34, D38, D39, D40) live in the
README with the rest of the log; this document is their detailing chapter.
