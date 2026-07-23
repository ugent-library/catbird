# 03 — job: the engine on rows

> Revised 2026-07-15 (D39–D43). The vocabulary converges on **job**: jobs are
> declared individually and globally named; queues are global pools;
> **triggers** (§8) make events create jobs declaratively. The 2026-07-14
> design (D34, D38) carries over whole — the work row claimed directly, the
> one-counter failure law — only the words and the scopes changed. Package
> `jobs` depends on the kernel only (`internal/ticker`, `internal/migrate`,
> `Conn`), not on `streams`; the two modules install and run independently,
> and the stream layer needs **zero changes** for jobs to exist. Packaging
> per D41: the trigger is a feature of this module (§8), and shared pure
> functions (backoff, cron-next) live in the kernel's SQL unit.
> Re-revised same day (D44): the work row is a **step** — four words, one
> per table: a **job** is the declared thing (`cb_jobs`), a **run** is one
> instance of a job, a **step** is a unit of owed work inside a run, an
> **attempt** is one execution of a step. The seeded pool is `default`, and
> channels take one prefix per noun: `cbq_<queue>`, `cbj_<job>`.

**One concept.** A run is one instance of a job: a group of steps, and every
step may atomically add new steps when it completes — "a flow is a job
whose steps add more steps" (D31, D39, D44), so no flow object exists
anywhere: not in the API, not in the schema, not in the vocabulary. A task
is not a thing either: a job whose handler enqueues nothing is just a job,
and one registration shape covers both (§5). Own migrations, version table
`cb_job_migrations`.

**Rows are the truth — and the transport (D30, D34).** There is no event log,
no projection, and no message either: the step row that records the work *is*
the unit a worker claims. `cb_job_complete` applies everything — the outcome,
the new steps, dispatch, the remaining count — in the completion
transaction, and the new rows are immediately claimable. What the old
designs wrote three times per edge (a step row, a ready message, an assigned
position), this design writes once. Step-to-step latency is notify + claim —
no assigner leg, no tick floor; the poll interval is the safety net (D17).

**One counter (D38).** The step row's `attempt` column counts *starts* — every
time `cb_job_start` hands the step to a handler, whatever later becomes of
that execution. A verdict, a crash, a graceful restart of a running handler:
each consumed a start. That one number is the whole retry budget, checked
wherever an execution's fate is decided. There is no crash counter, no header
bookkeeping, and no second exhaustion road: a crash is just a lease that
lapsed, and the lapsed row carries its own count.

## 1. The work table — steps are the messages

- A new step inserts as a row with `claimable_at = now()` (or `now() +
  delay`) and is claimable the moment its transaction commits. **One
  timestamp column, `claimable_at`, carries visibility, lease, and backoff**:
  insert sets it to when the step may first be handed out; claim sets it to
  `now() + claim_ttl` (the lease deadline); extend pushes it; fail sets it to
  `now() + backoff(attempt)`; a crash simply lets it lapse.
- **Wakeups** ride the old engine's proven contract: the engine fires
  `pg_notify` on the queue's channel (`cbq_<queue>`) with the step's
  `claimable_at` in the payload; the worker's notifier parses it and wakes
  the claim loop at once or arms a timer for the earliest future time —
  `worker_notifier.go`'s existing protocol, pointed at new tables — and the
  channel being the pool means a `payments` worker never wakes for
  `default` steps. Fallback polling stays as the safety net (D17). A second
  channel per birth job (`cbj_<job>`) carries run-terminal events (payload
  `<run_id>:<status>`) for `WaitForOutput` (which polls until M5) and the
  dashboard.
- **Queues are global pools.** A job's `queue` (NULL means `default`)
  partitions claiming and carries retry terms (§6). Pools are deliberately
  cheap: one call declares one, a job opts in with one field
  (`JobOpts.Queue`), and claim and
  extend take an **array of queues**, so a worker serving many small pools
  pays one round trip. No streams, no filters, no topics — the claim
  predicate is the routing.
- The hot index is partial: `(queue, claimable_at) WHERE status IN
  ('queued', 'started')` — terminal rows leave it, so the index holds only
  the working set. `queue` and `name` are denormalized onto the step row at
  insert so the claim query joins nothing (the hot-path rule, kept).

## 2. Tables

Seven tables, no edges — there is no deps table (D31). None of the names
collide with anything the old schema created (no old table is named
`cb_jobs` or `cb_job_*`), so the job module shares a database with the old
code through the transition and its test suite runs on the shared `cb_tst`
like every other module (05).

- `cb_jobs (name PK, queue, on_fail, retention NOT NULL)` — one row per
  declared job: the global authority a handler's new steps are validated
  against and the routing map in one. `queue` names the job's pool (NULL
  means `default`) and is stamped onto new steps by `cb_job_complete` — a
  typo'd step name raises, fails the handler that added it, and is given up
  on through the normal budget (§5); a foreign worker adds steps with zero
  knowledge of queue layout. `on_fail` (naming a declared job) and
  `retention` matter when the job is a run's **birth job**: the run
  inherits both (§3); on a job only ever run as a mid-run step they are
  inert config.
- `cb_job_queues (name PK, claim_ttl, claim_batch_size, max_attempts,
  backoff_kind, backoff_base, backoff_max)` — the retry and claim terms per
  pool, written whole by `jobs.DefineQueue` (D26: config deploys with code
  and must converge). The migration seeds the `default` row, so a bare install
  works; every other pool is declared, and `default` itself is redeclarable
  like any pool (§6).
- `cb_job_schedules (name PK, job, every, catch_up, input, next_at)` —
  interval schedules for scheduled runs (`every` is a fixed duration), the
  same shape and define semantics as the stream layer's schedule table; cron
  specs are a later addition in both, deferred. Delivered by the job
  module's tick: a due row calls `cb_job_run(job, input)` and re-arms
  `next_at` in one transaction — exactly-once per slot by construction, no
  message, no translation.
- `cb_job_runs (id PK, job, key, status, input, output, error,
  steps_remaining, next_step_id, created_at, finished_at)` — status
  `running | failing | completed | failed | canceled` (text + CHECK, D20).
  `failing` means the outcome is already decided (failed) and only the
  `on_fail` chain may still execute (§3). `job` is the birth job — the
  run's `on_fail` and retention are read from its definition. `UNIQUE (job,
  key)` (constraint `cb_job_runs_job_key_key`) — `key` is the dedup point
  *and* the app-key lookup in one column, scoped per birth job; NULLs are
  distinct. `steps_remaining` counts the steps the run still owes (§3).
  `next_step_id` hands out per-run step ids — the run row is locked in every
  engine call anyway, so the counter is free, and it gives dense ids and a
  clean `0` sentinel for "born with the run".
- `cb_job_steps (PK (run_id, id), queue, name, parent_step_id NOT NULL,
  ordinal NOT NULL, status, signal, input, signal_input,
  output, error, attempt NOT NULL DEFAULT 0, claimable_at, worker,
  created_at, finished_at)` — the work table. `name` is the declared job this
  step runs. Status `waiting_for_steps | waiting_for_signal | queued |
  started | completed | failed | canceled` — a waiting step's status says
  what it waits for (§3); there are no gate columns. `signal` is a boolean
  stored exactly as the wire stated it — whether the step asks for a
  signal — and `signal_input` is what `cb_job_signal` delivered, NULL until
  the signal arrives. Replay identity is the plain tuple:
  `UNIQUE (run_id, parent_step_id, name, ordinal)` (constraint
  `cb_job_steps_identity_key`), where **`ordinal` is the step's zero-based
  position among the steps its parent's completion added** — deterministic
  across replays because the whole list is replayed. `parent_step_id` is `0`
  for the run's first step, the one running the birth job. A partial unique
  index `(run_id, name) WHERE signal AND status NOT IN
  ('completed', 'failed', 'canceled')` enforces §3's signal-name rule
  race-free.
- `cb_job_attempts (PK (run_id, step_id, attempt), worker, started_at,
  finished_at, status, error)` — per-attempt history, kept when the run
  turns terminal. `status` is `completed | failed | NULL`, and NULL means
  the attempt never reported a result: the worker crashed, or the step was
  handed to another worker and this start no longer counts.
- `cb_job_signals (PK (run_id, name), payload, created_at)` — the signal
  buffer, one slot per name: a second signal for a name nobody consumed yet
  overwrites the slot (last signal wins); matching consumes it — deletes the
  row.

Three foreign keys tie children to their run — `cb_job_steps.run_id →
cb_job_runs`, `cb_job_attempts → cb_job_steps`, `cb_job_signals →
cb_job_runs`. They are cheap because each is checked only at insert, inside
a transaction that already holds the run lock, and the key columns never
change afterward — the hot claim/start/complete UPDATEs pay nothing; they
also turn the janitor's children-first delete order from convention into a
constraint. The two config references deliberately have **no** FK:
`cb_job_steps.queue → cb_job_queues` and `cb_job_steps.name → cb_jobs` would
take a KEY SHARE lock on one shared hot config row per insert (multixact
churn at volume), and `cb_job_complete` already validates both with better
errors — the migration carries a one-line comment at each spot saying so, so
nobody adds them later.

Lookups — `WaitForOutput`, status, lookup by app key, the dashboard — read
these rows directly. There is nothing else to read, and nothing else exists:
no message copy, no position, no stream.

## 3. Lifecycle

**The checks.** Every engine function follows the same two-step guard.
First it locks the run row (`FOR UPDATE`) and checks the run's status:
`running` or `failing` for start / complete / fail / signal (`failing` admits
the `on_fail` chain — the give-up canceled everything else, so no per-step
marking is needed) and for cancel (in a `failing` run, cancel stops the
cleanup, not the verdict — below). Anything else is a silent no-op returning
false — signal included: a signal for a run that ended is a race the caller
could not avoid, so it too returns false rather than raising (below). Then
the step guard:
complete and fail require `(status = 'started', attempt = $attempt)`; a
mismatch means the caller is acting on outdated information — the step was
finished or handed to another worker — and nothing happens. Only
`cb_job_start` increments `attempt`. One lock ordering — run row first, then step
rows — means no deadlocks between engine calls; it is also why giving up happens
in start and fail, never in claim (claim locks step rows without the
run lock, §4).

**Birth.** `cb_job_run` inserts the run (`steps_remaining` = 1,
`next_step_id` = 2) and its first step (step id 1, parent 0, ordinal 0,
status `queued`, `queue` from the birth job's definition, input = the run
input, `claimable_at` = now or now + delay), notifies the queue's channel —
nothing else. Any declared job is an entry point: you run the job you mean,
and the run's `on_fail` and retention come from that job's definition. It is
callable on a `Conn`, so an application can enqueue a run in the same
transaction as its own writes — the blob-GC / ingest pattern, validated in
production. Dedup is the run row itself: the `ON CONFLICT … DO UPDATE …
WHERE FALSE` + `UNION ALL` pattern (do not simplify), on constraint
`cb_job_runs_job_key_key`; an existing run — live or terminal — returns its
id with `existing = true` and inserts nothing. The dedup window is the birth
job's retention.

**Scheduled runs.** A row in `cb_job_schedules` (§2). The module's tick
delivers due rows — `cb_job_run` plus re-arm in one transaction — so a fire
is exactly-once per slot with no key gymnastics, and the run's first step
lands on the birth job's queue like any other birth. `catch_up` keeps the
stream scheduler's semantics (`skip` | `all`).

**Two waits, and the remaining count.** A new step states two waits, each a
required boolean in the steps JSON, in the same words the statuses answer
with: `waits_for_steps` — run only once everything
the run owes has completed **successfully** (any give-up cancels the
waiting step, so it never starts) — and `waits_for_signal` — wait for a
payload delivered by name. A waiting step's **status says what it waits for**:
`waiting_for_steps` or `waiting_for_signal`, never both — a step that asked
for both waits for the steps first, and a signal that arrives early is
buffered (below), so the waits are sequential on the row (D42; the old
engine's `waiting_for_dependencies` / `waiting_for_signal` statuses are the
same design). A step with both booleans off is claimable at once. Both
together are the approval-after-fan-out shape the old engine expressed as
dependencies plus a signal; the sequential half of that shape needs no wait
at all — dependency-on-parent is inherent in add-at-completion.
`steps_remaining` counts the steps the run still owes — queued, started, or
waiting for a signal. The arithmetic, exactly, in `cb_job_complete`:

```
steps_remaining := steps_remaining − 1 + count(inserted steps not in waiting_for_steps)
```

A step with both waits off inserts as `queued`, claimable at once — the
parent has already finished, so a sequential edge needs no bookkeeping. A
step in **`waiting_for_steps`** stays outside the count until it
dispatches — which is why it can never block its own dispatch; a
`waiting_for_signal` step **counts** — barriers and the run's completion
wait behind an unanswered signal. When a completion brings
`steps_remaining` to zero: if steps are waiting in `waiting_for_steps`,
**all of them** dispatch together as the next phase — each becomes
`queued`, claimable now (a buffered signal slot is consumed on the spot),
unless its signal is still unanswered, in which case it moves to
`waiting_for_signal` and **now counts**; `steps_remaining :=
count(dispatched)`. Otherwise the run finalizes. A barrier inserted by the
very completion that drained the count dispatches in that same call — the
at-zero check runs after the inserts. Phases repeat with no special case.
Buffer position carries no dispatch meaning — `ordinal` is identity only: a
barrier added *between* two immediate steps in the same Plan waits for both
of them, and for everything else the run still owes. The failure story the
formula guards against: birth sets the count to 1; if the first step's
completion only decremented, a run whose first step adds its successor
would finalize with work still queued. The count comes from the insert's
`RETURNING`, never from the argument: if the identity `ON CONFLICT DO
NOTHING` (§4) ever skips a row, arithmetic that counted requests would
charge the run for a step that doesn't exist, and the run could never drain
to zero.

**Completion and run output.** A run finalizes when `steps_remaining` reaches
zero and no barrier is left waiting — in a `running` run as `completed`, in a
`failing` run as `failed`. Only executions add steps, so such a run can never
grow again — no explicit "done" call exists. Output resolution: the run's
output is what `run_output` calls set — an explicit `run_output` passed to
any `cb_job_complete` (last writer wins — writes are serialized by the run
lock; SQL NULL means "not passed", so an explicit run output cannot be
null — wrap it if you mean it). There is no fallback: a step's output
stays on its row, and a run whose completions never pass `run_output`
finishes with output null. A fallback would have been the finalizing
completion's step output — deterministic for sequential shapes but an
arbitrary pick when several steps finish the run together, so the rule is
explicit-or-null for every shape.
Finalization fires the run-terminal notify (`cbj_<job>`,
payload `<run_id>:<status>`). `WaitForOutput` keeps its API: it polls the run
row until M5, then wakes on the notify with the poll demoted to safety net.

**Failure, retries, giving up — one counter (D38).** A failed execution
reports through `cb_job_fail`: the checks admit it, the attempt row records
the verdict (attempt-row `status = 'failed'`, the error), and one comparison decides:

- `attempt < max_attempts` — the step goes back to `queued`, `claimable_at =
  now() + backoff(attempt)`, worker cleared. The retry is nothing but the
  row becoming claimable later; no copy, no republish, no second object.
- `attempt ≥ max_attempts` — the engine **gives up**, in the same transaction.

A crash means the handler never reports. The crashed execution's lease
just lapses, and the next claim call clears the row (§4): a `started` row
whose lease has lapsed gets its worker stamp cleared and `claimable_at`
moved to `now() + backoff(attempt)` — the same pacing a reported failure
gets, since the lapsed start *was* attempt N. A row already at `attempt ≥
max_attempts` is moved to `now()` instead: its only future is the give-up,
delaying it would gain nothing, and `on_fail` fires one claim sooner. When
the cleared row next reaches a worker,
`cb_job_start` checks the same comparison: starting would spend `attempt +
1`, and if `attempt ≥ max_attempts` already, start **gives up instead of
starting** and returns no work. Two call sites, one routine
(`_cb_job_give_up`), one counter, one policy home (the queue row). A
give-up arriving late — the run already `failing` or terminal because a
sibling's give-up came first — fails the checks and changes nothing; that is
the whole idempotency story.

`max_attempts` bounds **total starts**, not verdicts: a crash consumed a
start, and so did a graceful shutdown that canceled a running handler — the
step had started, and the checks cannot tell a stuck worker that may still
report from a dead one, so redelivery must spend a fresh start. Stated
plainly: "this step's handler
will begin at most `max_attempts` times." The cost is that frequent deploys
spend starts of long-running handlers — a job like that deserves its own
pool with generous terms. The other half of the same rule: a step
that was **leased but never started** (its worker died before reaching it)
lapses back to claimable with no attempt spent — nothing began, nothing is
charged. The
worker's graceful path makes both halves explicit: unstarted claimed rows
are released with no attempt spent (`cb_job_release`, §4 — redelivered
immediately),
and started handlers get their context canceled and are reported through
`cb_job_fail` with the error `catbird: worker shutdown` — the same one
start already spent, but redelivery is backoff-paced from now
instead of from lease-lapse, and the attempt row records a verdict instead
of a NULL status. The NULL keeps meaning what it says: nobody reported,
because nobody could.

**The give-up's effects** (`_cb_job_give_up`, one transaction): the step
turns `failed` with its error (when nobody ever reported: "attempts
exhausted; last
attempt ended in silence"); every other non-terminal step — queued, started,
or waiting — turns `canceled` (a started sibling's later complete/fail fails
the step guard and changes nothing; its handler is stopped by the worker's
status check, below). Then:

- **`on_fail` declared** (on the run's birth job): the run turns `failing`;
  the `on_fail` job is added as an ordinary step — parent = the failed
  step, no waits, input built by the engine as `{job, error,
  input}` — and `steps_remaining := 1`. The chain then runs under the normal
  rules: the checks admit it because the run is `failing` and everything
  else is canceled; it may add steps, retry on its own terms, even wait for
  a signal; when its chain drains the count to zero the run finalizes as
  `failed`. If the engine gives up on the `on_fail` step itself, the run is
  already `failing`, so `_cb_job_give_up` adds nothing and the run turns
  `failed` — one rule, no regress.
- **No `on_fail`**: the run turns `failed`, `error` set, run-terminal notify
  fired.

`on_fail` firing on crash exhaustion too is the point: today's OnFail misses
hard worker death, which is why ingest hand-rolled `sweep_stuck_deliveries`.

**Cancel.** `cb_job_cancel(run_id, reason)` — the checks admit `running` and
`failing`. In a `running` run it flips every non-terminal step and the run
to `canceled`; in a `failing` run it does not change the verdict, it stops
waiting for cleanup — remaining steps turn `canceled` and the run finalizes
`failed` at once. (Refusing would assume cleanup terminates; an `on_fail`
chain parked on a signal nobody will send would otherwise leave the run
un-finalizable *and* un-cancelable.) Either way the run-terminal notify
fires. Started handlers get best-effort context
cancellation: the worker's handler wrapper checks its own step row on the
extend cadence (D27) and cancels the handler's context when the step is no
longer `started` — one cheap indexed read, covering cancel, a sibling's
give-up, and supersession alike.

**Signals are buffered, and the payload arrives beside the input.** A
signal-waiting step carries its input like any other step; the payload is
delivered separately — stamped into the step's `signal_input` column at
satisfaction and returned by `cb_job_start` — so a step can be added with
context and still wait for an approval.
`cb_job_signal(run_id, name, payload)` returns a boolean: under the run lock,
if the run holds a step of that name in `waiting_for_signal`, it is
satisfied — the payload lands in `signal_input`, status
`waiting_for_signal → queued`, claimable now
(`steps_remaining` unchanged: such a step already counts). Otherwise — no
such step yet, or it is still in `waiting_for_steps` — the payload is
buffered in the run's slot for that name (overwriting an unconsumed older
one). Either way it returns true: the payload landed. The mirror happens
whenever a step stops waiting for anything else — at insert for a
signal-only step, at phase dispatch for one that waited for the run's other
steps first: a buffered slot, if present, is consumed on the spot and the
step proceeds `queued` — arrival order does not matter. The call returns
**false**, changing nothing, only when the run is missing or already
finished: the run ended at the same time, which the caller could not have
avoided, so this is not a raise (`failing` accepts signals: a cleanup chain
may legitimately await an operator). The synchronous
`ErrSignalNotDelivered` is retired (README amendment 12). A signal-waiting
step's name must be unique among the run's unresolved steps — the partial
unique index (§2) enforces it at insert time; violating it fails the
completion that added the step, which is a handler bug and is given up on
through the normal budget (§5).

## 4. The functions — the completion transaction is the engine (D30)

All engine logic is SQL: a thin client in any language calls these functions
and gets identical semantics (D11). Every function except claim and release
begins with §3's checks; handlers never run inside any of these transactions.
Full SQL sketches arrive per function at implementation; what follows is the
contract each sketch must meet.

- **`cb_job_define(job, queue, on_fail, retention)`** plus the queue-terms
  and schedule declarations — the whole-declaration upserts (D26), guarded
  so unchanged declarations write nothing. It checks that `on_fail` (when
  set) names a declared job and that `queue` (when set) has a terms row,
  raising otherwise — a deploy-time error instead of a runtime "step name
  not declared" on the first run. Define never deletes a queue row (§6).
  The Go side mirrors these one to one — `jobs.Define`, `jobs.DefineQueue`,
  `jobs.DefineSchedule`, `jobs.DefineTrigger` — and deploy code calls them
  in dependency order: queues before the jobs that name them, `on_fail`
  targets first, triggers after the jobs and streams they bind. Each call
  is atomic and validated on its own, so a deploy that dies mid-way leaves
  a consistent prefix and the next deploy converges the rest; an app that
  wants the whole set to apply together runs the calls in its own
  transaction. Deploy-time setup, not part of the worker contract.
- **`cb_job_run(job, input, key, delay) → (run_id, existing)`** — birth,
  §3. Raises if the job is not defined. Callable on a `Conn`.
- **`cb_job_claim(queues, worker) → rows of (run_id, step_id, name,
  lease_at)`** — `queues` is an array: one call serves every pool the
  worker holds handlers for, up to each pool's `claim_batch_size`. The one
  function without the run lock (it locks step rows only, which is why it
  never gives up on anything). No per-call ttl: D23's override worked because the claim
  row stored the resolved terms for extend to renew; a step row has no claim
  row, and D27's loop owns the clock anyway — the queue row is the only
  source of terms. `name` lets a worker refuse a step it holds no handler
  for (release, below); `lease_at` is the deadline, from which a foreign
  worker derives its extend cadence without reading policy tables. One
  statement over the partial index, two effects: **clear crashed rows** —
  `started` rows with a lapsed lease and a worker still stamped get what
  `cb_job_fail` would have done had the worker been able to report: stamp
  cleared, rescheduled to
  `backoff(attempt)` (`now()` at the attempt limit, §3), not
  handed out; **hand out** — ready rows (`queued`, or `started` with no
  worker — a cleared crash due for redelivery) get the caller's worker
  stamp and a lease of the pool's `claim_ttl`, `FOR UPDATE SKIP LOCKED`, ordered
  by `claimable_at`. Leased-but-unstarted rows whose lease lapses are
  simply ready again — no clearing needed, no attempt spent.
- **`cb_job_start(run_id, step_id, worker) → (name, input, signal_input,
  attempt)`** —
  per claimed step. The checks, then the give-up comparison (§3): at the attempt
  limit it gives up (no attempt row — nothing started) and returns nothing.
  Otherwise flips the step `queued | started → started` (started-to-started
  because a crashed execution leaves `started` behind), bumps `attempt`,
  inserts the attempt row, and returns what the handler needs — `signal_input`
  is the satisfied payload for a step that waited for one, NULL otherwise (§3).
  Returns
  nothing when a check fails — a stale claim of a finished step — and the
  loop just moves on.
- **`cb_job_extend(queues, worker) → held rows`** — pushes `claimable_at`
  forward on every row the worker still holds in those pools, one
  statement; the loop calls it on the D27 cadence and compares the returned
  set to what it thinks it holds — a missing row was taken over or
  canceled: cancel that handler.
- **`cb_job_release(run_id, step_id, worker, pause DEFAULT '0') →
  boolean`** — hands back an unstarted claim, no attempt spent. The check is
  on the step row alone (`queued`, this worker) — no run lock, like claim; it
  clears the worker stamp and sets `claimable_at = now() + pause`. Shutdown
  releases each unstarted held row with the default `0` — immediate
  redelivery. A worker that claims a step it holds no handler for — a
  rolling deploy declared a new job before this worker's fleet restarted —
  releases it with a short pause, so two old workers don't ping-pong it
  until a new worker arrives. False means the step already moved on.
- **`cb_job_complete(run_id, step_id, attempt, output, steps, run_output)
  → boolean`** — the heart, one transaction, in this order: checks → resolve
  the attempt (status, finished_at; step → `completed`, output stored) →
  validate and insert the new steps (names against `cb_jobs`, queue
  stamped from the definition, both booleans required;
  identity `ON CONFLICT DO NOTHING` as a backstop) → consume signal
  slots for signal-only steps as they insert (§3) → apply §3's
  remaining formula over the rows the insert
  actually returned (§3) → at zero, dispatch waiting barriers or finalize
  (output resolution, run-terminal notify) → notify each queue that gained
  claimable rows, once, with its earliest new `claimable_at`. Returns
  false, having changed nothing, when the checks at the top fail.
- **`cb_job_fail(run_id, step_id, attempt, error) → boolean`** — §3:
  verdict on the attempt row, then retry (queued + backoff) or give-up,
  one comparison, one transaction. Returns false, having changed nothing,
  when the checks at the top fail.
- **`cb_job_cancel(run_id, reason DEFAULT NULL) → boolean`** — §3.
- **`cb_job_signal(run_id, name, payload) → boolean`** — §3: satisfy or
  buffer under the run lock, returning true; returns false, changing nothing,
  when the run is missing or already finished.

The leaner argument lists are deliberate: claim, start, release, complete
and fail take ids the worker already holds — no stream, topic or header to
know, so nothing to pass wrongly.

One more rule for the sketches: every multi-row UPDATE — extend, the
give-up and cancel sweeps — locks its rows in one order (subselect
ordered by `(run_id, step_id)`, update by PK). The run-first ordering (§3)
serializes the calls that take the run lock; extend takes none, so without a
shared row order it could deadlock against a sweep.

**Cost and latency.** One row insert per edge where the previous designs
wrote a step row *and* a message *and* an assigned position; step-to-step
latency is notify + claim — single-digit milliseconds once the notifier
lands, one poll interval before that — with no assigner tick in the path at
all. Serialization did not disappear, it moved where it always was: every
engine call takes the run-row lock, so sibling starts and completions in one
run serialize on it. The watch item is unchanged — **connection occupancy**
on wide fan-in (~1k completions/s per run, D30), with the same mitigation
ladder: keep the completion transaction tiny → batch sibling completions
into one call (an array argument, the `PublishMessages` precedent) →
deferred drain detection. The wide-map stress test in M4b decides whether
the first rung suffices. The steps table is update-hot (claim, start and
complete each touch the row), so it inherits the autovacuum note that 01 §10
attaches to message partitions; LIST partitioning of `cb_job_steps` by
queue is the documented escape hatch if one pool's churn ever dominates.

## 5. The Plan DSL (D10, D31)

The key mechanic is unchanged: **`*Plan` is a buffer.** Every method buffers;
nothing blocks; the buffer commits with your completion (`cb_job_complete`,
§4). A handler that crashes mid-way submits nothing, and at-least-once
redelivery replays it cleanly against the step identity (§2).

Declarations mirror the SQL one to one (§4): deploy code calls
`jobs.Define`, `jobs.DefineQueue`, `jobs.DefineSchedule` and
`jobs.DefineTrigger`, each call the whole config — an opts struct whose
zero fields mean the stock values, never "keep". The worker registers handlers by name (`w.Handle(job, fn)`)
and reads everything else from `cb_jobs` at startup: which queues to claim
is routing knowledge, and the database is its authority. The handler
signature decides everything: a handler that takes a `*jobs.Plan` can add
steps and set the run's output; one that doesn't is the whole story. A
handler that returns `(Out, error)` records the return value as its
**step's** output — the function metaphor itself, and internal: it stays
on the step row, read by `StepOutput(s)` and kept for debugging; the
error-only shapes record none. The run's public output is a separate,
deliberate act — `p.SetRunOutput` — and nothing else: the engine never
promotes a step's output to the run, which would be an arbitrary pick
when several steps finish the run together (§3). A defined job nobody `Handle`s is simply handled
by another fleet — which fleet holds a handler is deployment knowledge,
deliberately absent from the schema; the worker's startup checks are the
fleet-symmetric enforcement (§7).

```go
// deploy: each call converges one declaration
jobs.DefineQueue(ctx, conn, "payments", jobs.QueueOpts{
    MaxAttempts: 5, Backoff: jobs.FullJitterBackoff(time.Second, time.Minute)})

jobs.Define(ctx, conn, "notify_ops")        // receives {job, error, input}
jobs.Define(ctx, conn, "reserve",           // on_fail = the run's failure job,
    jobs.JobOpts{OnFail: "notify_ops"})     //   when "reserve" is the birth job (§3)
jobs.Define(ctx, conn, "charge",
    jobs.JobOpts{Queue: "payments"})        // own pool + own retry terms (§6)
jobs.Define(ctx, conn, "ship")
jobs.Define(ctx, conn, "confirm")
jobs.Define(ctx, conn, "transcode",         // no Handle call anywhere in this
    jobs.JobOpts{Queue: "gpu"})             //   fleet — another fleet handles it (§7)

// runtime: register handlers, then Start reads cb_jobs and checks coverage
w := jobs.NewWorker(pool)
w.Handle("reserve", func(ctx context.Context, p *jobs.Plan, in Order) error {
    res, err := reserve(in.Items)
    if err != nil { return err }            // → cb_job_fail; retry or give up (§3)
    p.Step("charge", Charge{Order: in, Reservation: res})
    return nil                              //   parent hands its results forward
})
w.Handle("charge", func(ctx context.Context, p *jobs.Plan, in Charge) error {
    for _, parcel := range split(in) {
        p.Step("ship", parcel)              // fan-out: N sibling steps, parallel
    }
    p.After().Step("confirm", in.Order)     // barrier: runs when steps_remaining
    return nil                              //   drains to zero
})
w.Handle("ship", shipFn)                    // plain func(ctx, Parcel) (Out, error):
w.Handle("confirm", confirmFn)              //   no Plan, enqueues nothing
w.Handle("notify_ops", notifyFn)
w.Start(ctx)

runID, _, err := jobs.Run(ctx, conn, "reserve", order, jobs.RunOpts{Key: order.ID})
```

Surface, complete: `p.Step(name, input, opts...)` — the **one** verb that
adds a step — and `p.After().Step(...)` — `After` scopes what the step
waits for, bare meaning everything the run owes (§3); its return value's
only method is `Step`, so a half-built chain has nothing else to call and
buffers nothing. `jobs.WaitsForSignal()` is the lone step option and composes
in both positions: `p.Step(n, in, jobs.WaitsForSignal())` waits for the signal
only, `p.After().Step(n, in, jobs.WaitsForSignal())` waits for the steps first,
then the signal. The payload is sent with `jobs.Signal(ctx, conn, runID,
name, payload)`, mirroring `cb_job_signal` name for name — which is why the
reader below is `SignalInput`, the column it reads: one package cannot hold
a delivery function and a generic reader under one name. Also:
`p.SetRunOutput(v)` ·
`jobs.RunInput[T](p)` (the run's birth input; a step's own input is the
handler argument) / `jobs.SignalInput[T](p)` (the satisfied payload, NULL
until then) / `jobs.StepOutput[T](p, "name")` and `jobs.StepOutputs[T](p,
"name")` (read from the step rows: `StepOutput` wants the run's one
completed step of that name, `StepOutputs` returns one element per step —
the fan-out reader; a single name reading both ways would be ambiguous
when the one step's output is itself an array, so the call states the
shape). One naming convention across the surface: **every reader and
writer names its subject** — `Run`, `Step`, `Signal` — in the function
name; the only unnamed channels are the two the function metaphor carries,
the handler's argument (this step's input) and its return value (this
step's output).
The rule: **a step runs at once unless it says what it waits for** (§3).
There is no entry-point declaration — `RunJob` names the job you mean — and
no special failure job: `OnFail` on a declaration names another declared
job, consulted when that declaration is a run's birth job.

The naming rules behind the surface. **The API speaks the engine's own
noun**: a step is the glossary word, the table, the `step_id` in every
signature, so `p.Step(name, input)` is the glossary sentence — "a unit of
owed work, running a declared job" — as a call; the earlier `Spawn` was a
coined verb that appeared nowhere in the schema and needed translation
sentences ("spawns become step rows") wherever it met SQL. **`After`'s
argument slot always means "what is waited on"**, never "what is added" —
bare today, a group later (partial joins are deferred until a real workload
asks, D31), so grouped waits arrive without a rename; that slot rule is
also why `p.After(name, input)` as a second adding verb was rejected — its
first argument misreads as the dependency. Dropping *succeed* from the name (the option was
`WhenAllSucceed` for a day) is safe **here**, unlike in Airflow: the engine
has exactly one failure behavior — a give-up cancels every waiting step,
and cleanup-despite-failure has its own road, `on_fail` — so there is no
run-despite-failures alternative for `After` to be confused with; one
contract sentence carries it. `WaitsForSignal` states its bit in the same
words as the wire key and the status (the old API's `WithSignal` was
carried at first, but stopped echoing its bit once the keys were renamed).
Rejected en route: `OnAllDone`/`Last` (imply
run-despite-failure), `Gather` (sounds like collecting right now), `Join`
(`thread.join` blocks), `Then`/`Next` (misread as sequential), and any flow
or group noun — the group is a declaration-time convenience, not an engine
concept, and naming it would grow it back into one. The two waits are
independent booleans in the steps JSON, `waits_for_steps` and
`waits_for_signal` — the request states what the step waits for in the
same words the status answers it with (the earlier `after_all` key slowed
readers down). The row needs no `waits_for_steps` column at all, and
`waits_for_signal` lands on it as the boolean it arrived as (§2); there is
no combined word, because every combination already works (D42).

**The input rule:** a step's input is exactly what it was added with, with
one stated exception where the engine supplies it: an `on_fail` step's input
is the engine-built `{job, error, input}`. A signal-waiting step is no
exception: its input is what it was added with, and the signal's payload
arrives beside it — `jobs.SignalInput[T](p)`, NULL until satisfied (§3). Steps
are added at completion, so a parent passes its results forward *in* the
step input; a barrier, whose siblings' outputs didn't exist when it was
added, reads them with `StepOutputs`. This is also what `input` and `signal`
mean in the SQL contract (§7). (A trigger-born run is no exception either:
its input is the message payload, exactly as published — the publisher is
the caller there, §8.)

**Validation is layered.** The Go side fails fast where it can: `Step`
panics on a name not declared — a panic becomes a failed attempt (D27),
close to the bug. The SQL
side enforces the same rules for every language (§4): an invalid step
raises, failing the completion that added it — the worker reports it through
`cb_job_fail` like any handler error, and the normal budget applies, so a
deterministic bug is given up on after `max_attempts` backoff-paced tries.
Deliberately no fast-path give-up flag: that would be a second exhaustion
road (D38), and the cost of walking the normal one is a few wasted retries
on a bug the attempt rows make visible immediately.

**Dead concepts become user patterns** (D31, D32, D39): edges (the old
engine's `DependsOn`, `*SpawnRef`) — a sequential edge is just a step added
at completion, fan-in is `After()`; map — add N in a loop plus a barrier;
conditions — a Go `if` before `Step` (the engine never guards);
`Optional[T]` — you only add what exists; `AwaitSignal` — the `WaitsForSignal`
step option; `CompleteEarly` — a return *is* completion (`p.SetRunOutput` +
`return nil`); the task engine and the flow object — one registration
shape, the Plan parameter optional. A one-step run is also the recommended
shape for everything the old world put on a work queue — it *is* the queue,
with a handle (README, D37).

**The worker** runs on the kernel's claim-loop skeleton (`internal/`, D27,
D41) — the same skeleton the stream consumer uses: the modules share the
loop's mechanics while each owns its contract. One `select` waits on three
things at once: the handler
returning, an extend tick that pushes the step's lease deadline out while the
handler is still running, and a stop-or-cancel signal. Because one loop both
runs the handler and extends the lease, a worker that hangs or dies stops
extending, and its work falls to another worker. The loop: claim a batch
across its queues → per step: start → handler → complete/fail — extending on
the cadence; on shutdown it releases unstarted leases and fails canceled
handlers (§3). Handlers are registered per job name (`Handle`) and invoked via the
reflection utilities (ported from `task.go`). At startup the worker reads
`cb_jobs` for the jobs it handles — the queues they route to are the queues
it claims — and checks coverage both ways: it refuses to start when it
handles a job nobody defined (a typo, caught at boot), and when a queue it
claims routes a job it holds no handler for — a claim is
indiscriminate within its pool, so partial coverage would strand steps (§7).
Both checks run at startup only, and the definitions converge on deploy
while old workers still run — the release-with-pause path (§4) is what
carries a new job across that skew window.

```go
type Plan struct {
	steps     []step          // {name, input, waits_for_steps bool, waits_for_signal bool}
	runOutput json.RawMessage // SetRunOutput's buffer; the step's own output is the handler's return value
}
```

## 6. Retry terms — pools and pacing

A job's terms are its pool's row in `cb_job_queues` (D4: policy in the
database, applied by SQL): `max_attempts`, the backoff, `claim_ttl`,
`claim_batch_size` — written whole by `jobs.DefineQueue`, so a redeploy
converges
them. Terms are pool properties, full stop: retry terms live in
`QueueOpts`, never on a job — a job that needs its own terms (a
rate-limited payment call, a GPU stage) declares its own pool and routes to
it with `JobOpts.Queue`. Pools being global and cheap is the production
isolation story: the migration seeds `default` for the bare install, and
every family that matters declares its own pool with its own terms — one
call — so one family's backlog or backoff never paces another's. There is
no queue delete — a pool no longer declared may still
have non-terminal steps routed to it, and a stale terms row is inert config,
so removing one is a deliberate op.

`default` is seeded with stated terms — `max_attempts` 3, full-jitter
backoff 1s–1m, `claim_ttl` 30s, `claim_batch_size` 10 — and stays an
ordinary row: `jobs.DefineQueue(ctx, conn, "default", …)` redeclares it from
the app's own code like any pool; only its existence is guaranteed.

Pacing has two knobs, both live today: **which workers claim the pool**
(fleet sizing per queue) and `claim_batch_size`. Two more are **deferred
with this note**, same trigger for both (evidence from a real workload, not
principle): `max_inflight` — a per-pool concurrency cap, enforced in claim
by refusing hand-outs while the pool's `started` rows are at the cap — and
`rate_limit` — starts per window for rate-limited APIs, a token count on
the pool row, tolerable because claims are per-pool and far rarer than
per-step. Each is one column and one predicate in claim, no new machinery.
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
cb_job_claim(queues, worker)                  → [(run_id, step_id, name, lease_at)]
cb_job_start(run_id, step_id, worker)         → (name, input, signal_input, attempt)
                                              --  or nothing (a check failed, or give-up)
cb_job_extend(queues, worker)                 → still-held rows (D23, D27)
cb_job_release(run_id, step_id, worker, pause) → bool -- unstarted handback, no attempt spent
cb_job_complete(run_id, step_id, attempt, output, steps, run_output) → bool
cb_job_fail(run_id, step_id, attempt, error)  → bool  -- retry or give up, one call
cb_job_signal(run_id, name, payload)          → bool  -- deliver or buffer; false on a finished run
```

The loop: claim → per step `cb_job_start` → run the handler →
`cb_job_complete` or `cb_job_fail` — extending on the cadence `lease_at`
implies (no policy table to read), and a
worker that never extends has its slow steps truthfully counted as crashes
and slowed by backoff — legal, at-least-once, just noisy; the M4b demo
worker is deliberately slower than the claim TTL to prove that path end to
end. `cb_job_release` is politeness, not obligation: a worker that never
releases is legal too — its leases lapse and come back with no attempt
spent, just
slower. `attempt` travels from start through complete/fail — the third
column the checks compare — so each start resolves at most once, and a
false return means
the step was taken over and nothing happened. (`cb_job_run` and
`cb_job_cancel` are equally callable — any client with a Postgres
connection starts and cancels runs; the *worker* contract is the seven
above.)
No headers to parse, no streams to name, no scheduled-message shapes to
recognize — a scheduled run arrives as an ordinary claimed step.

`steps` carries the handler's buffer as JSON — `[{name, input,
waits_for_steps, waits_for_signal}]`, both booleans required (§3) — and
the engine routes each new
step by `cb_jobs`, so adding steps is fully available to a Python worker
with zero knowledge of queue layout. The DSL is sugar, not capability. Explicitly out of scope, same line as before: cross-language
job *definition*, typed payload schemas or registries, SDK parity. The
contract is job name in, JSON in, JSON out, plus the seven calls. Hold that
line.

## 8. Triggers — events become jobs (D40)

The log answers *what happened*; the job answers *what is still owed*; the
**trigger** is the declared crossing between them. D29 rightly deleted
routing — messages are never sent *to* places — but the spine was also
carrying the answer to "events cause work", and when D34 took the engine off
the log, that answer shrank to a hand-written consumer per binding: a
deployed Go loop whose handler calls `cb_job_run`. The trigger makes the
common case declarative — the outbox pattern with zero glue code, which is
the same-database design's front door.

- **A trigger is a row, not a process**: `cb_job_triggers (name PK, stream,
  job, created_at)` — declared whole by `cb_job_define_trigger` (D26
  semantics; `cb_job_delete_trigger` removes one), validated at define time:
  the stream exists, the job is declared, the filter compiles (the D29
  topic-pattern and condition languages, the same compiler subscriptions
  and cursors use). The trigger owns the cursor named after it on its
  stream, and that cursor is the filter's single home — source text and
  compiled form together, kept true on every redeclare through
  `cb_stream_define_cursor`; the trigger row stores no copy that could
  drift. `start_pos` is a define parameter, not a column — the one
  deliberate poke at the cursor's position, exactly
  `cb_job_define_schedule.start_at`: 0 delivers the stream from the
  beginning, N from after N, NULL at create starts at the tail, NULL on
  redeclare keeps the position.
- **Delivery is a tick on the module's ticker**, per trigger, one
  transaction: read the cursor's next batch of matching messages, call
  `cb_job_run(job, payload, key)` per message, advance the cursor
  (`_cb_job_run_triggered`; the Go tick calls it once per trigger row, so
  a stalled trigger never blocks the others). Exactly-once event→job
  creation by cursor semantics — the composition rule made mechanical. No
  deployed consumer code, and cross-language by construction: a
  Python-only shop declares triggers through SQL and gets outbox-triggered
  jobs without writing a consumer loop.
- **The input is the message payload, exactly as published.** The engine
  passes app-authored input through verbatim — the publisher authors a
  triggered run's input the way a caller authors `cb_job_run`'s and a
  schedule its declared `input` — and synthesizes one only where none
  exists (`on_fail`, §5). Provenance does not ride the input: the run key
  records it, and a handler needing the event's own fields wants them in
  the payload, where every caller can supply them.
- **Every match births one run**: the run key is `<trigger>:<position>`,
  so creation stays idempotent even across a cursor reset. A bare suffix
  is always a position; a later key kind must name itself (`:key:` is
  reserved), so no future format can collide with position keys. There is
  deliberately no burst-collapse option here: collapsing same-key events
  belongs to the stream layer — keep-newest coalescing in pending (D5),
  beside the delay and dedup machinery it composes with — not to run
  creation, whose only dedup window (run retention) answers a different
  question.
- **Failure is loud and ordered.** Run creation can only fail
  deterministically (the job's row deleted, invalid input), and a raise
  rolls back the batch and stalls the trigger at its cursor — visible
  lag, the tick logs it every interval, no silent skips, fixed by a
  define or a deploy. Execution failures are the job's own retry /
  `on_fail` story; the trigger never learns of them. Backpressure is the
  pool's problem by design: a burst of events becomes a burst of queued
  steps, paced by §6.
- **Packaging**: a feature of this module, not a module of its own (D41).
  `cb_job_triggers` and the trigger functions live in job's migrations —
  PL/pgSQL bodies are late-bound, so the job schema installs cleanly
  without the stream schema present — and `cb_job_define_trigger` and the
  delivery tick raise `catbird: stream schema required` at use (SQLSTATE
  `IRD03`, `jobs.ErrStreamsRequired`). The composition is one-directional
  and recorded: job's SQL calls stream's public SQL API
  (`cb_stream_define_cursor`, `cb_stream_delete_cursor`,
  `cb_stream_read`), never the reverse, and the Go dependency rule stands
  untouched. Declared with the jobs it feeds:
  `jobs.DefineTrigger(ctx, conn, name, stream, job, opts...)`, one call
  per trigger like the other declarations (§5); `jobs.DeleteTrigger`
  removes one.

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

Rows are the history. A row per step, an attempt row per start —
`cb_job_attempts` is kept when the run turns terminal, and a NULL status is
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
is the run row and its step, attempt and signal rows together,
batched deletes by a janitor on the module's tick. **A parked run pins only
its own rows** — a signal step waits with no other artifact in the system,
and a queued step *is* its own delivery, so there is no separate message to
lose and no wedge-by-pruning hazard at any parking duration: the step
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
new steps, barriers, signals, `on_fail`; M4c = triggers). The build items:

1. The kernel SQL unit (D41): its own migration dir and version table,
   auto-applied by `internal/migrate` before any module's migrations; seeds
   `cb_backoff`; stream's `cb_valid_name` and `cb_forever` move into it in
   the same pass, names unchanged (pre-release edit-in-place, the M3r
   precedent).
2. Migration `jobs/migrations/00001`: the seven tables (§2), text + CHECK
   statuses, named constraints (`cb_job_runs_job_key_key`,
   `cb_job_steps_identity_key`, the signal-name partial unique index), the
   partial claim index `(queue, claimable_at)`, the seeded `default` queue
   row, version table `cb_job_migrations`.
3. Functions (§4), M4a scope: `define`, `run`, `claim` (crashed-row
   clearing, queue array), `start` (with the give-up check), `extend`, `release`,
   `complete` (raises on non-empty `steps` until M4b; counts inserted
   steps via `RETURNING`), `fail`, `cancel`, `_cb_job_give_up` (the
   `failing` machinery ships in M4a; only the `on_fail` step stays dark
   until a job declares one) — backoff from the kernel unit. M4b: the
   new-step/barrier/signal paths in `complete`, `cb_job_signal`, `on_fail`.
4. The module's tick (kernel ticker): schedule delivery (§3), the
   run-retention janitor (§9).
5. Go: the per-call declarations mirroring the SQL — `jobs.Define`,
   `jobs.DefineQueue`, `jobs.DefineSchedule` (each an opts struct, zero =
   stock; `jobs.DefineTrigger` at M4c); the worker — `NewWorker(pool)`,
   `Handle(job, fn)`, `Start` — with the queue set read from `cb_jobs` and
   coverage checked both ways at startup (§5); the worker
   loop on the kernel's D27 skeleton (extracted to `internal/`, the stream
   consumer rebased on it, suites green — D41) with queue-set claims,
   status-check cancellation and release-plus-fail on
   shutdown (§5); the Plan buffer; reflection utilities ported from
   `task.go` (including the with-Plan / without-Plan handler shapes); run
   lookup by id and by app key; `WaitForOutput` polling, notify at M5.
6. `docs/sql-api.md`: rewrite as the normative worker contract (§7), with
   the old-vs-new name table for the transition, and worked examples for
   combining the two waits and the run-wide barrier rule (§3) — the two
   places a reader's intuition is most likely wrong.
7. M4c — triggers (§8, D41): migration `jobs/migrations/00002_trigger.sql`
   (`cb_job_triggers` — name, stream, job; the filter lives on the trigger's
   cursor), `cb_job_define_trigger` / `cb_job_delete_trigger` — define-time
   validation plus the loud stream-schema-required check (IRD03) — the
   delivery tick `_cb_job_run_triggered` on the module's ticker, one call
   per trigger; `jobs.DefineTrigger` / `jobs.DeleteTrigger`, per-call like
   the other declarations. The stream layer gains
   `cb_stream_define_cursor` and `cb_stream_delete_cursor` in the same
   pass (D26's define-when-first-needed, 01 §4).
8. Tests, the semantic core:
   - dedup + key lookup; duplicate complete/fail no-ops (the checks).
   - give-up from both roads — the exact inequalities: a verdict at
     `attempt = max_attempts` is given up in `fail`; a lapsed step at
     `attempt = max_attempts` is given up in `start`; total starts never
     exceed `max_attempts`.
   - crashed-row clearing: a lapsed `started` row comes back after
     `backoff(attempt)` exactly once (the clearing is idempotent under
     racing claims), and at once (`now()`) at the attempt limit; a
     leased-but-unstarted row lapses back with `attempt`
     untouched; a graceful release redelivers immediately, no attempt
     spent.
   - a graceful shutdown mid-handler spends exactly one start, as a verdict:
     the attempt row reads `worker shutdown` and redelivery follows
     `backoff(attempt)` with no lease-lapse wait (§3).
   - the `failing` lifecycle: the give-up cancels siblings, `on_fail`
     receives `{job, error, input}`, its chain may add steps (barriers
     included) and signal, its exhaustion ends the run `failed` with no
     second `on_fail`; cancel of a `failing` run cancels the cleanup chain
     and finalizes `failed` at once.
   - signal slot semantics: signal-before-step buffered and consumed;
     overwrite of an unconsumed slot; sequential same-name steps; a
     duplicate signal-step name fails the completion that added it.
   - the additive payload: a signal-waiting step added with input receives
     both — its input unchanged, payload in `signal_input` (start returns it;
     `jobs.SignalInput[T]` NULL until satisfied).
   - external jobs: a defined job with no `Handle` call converges like any
     declaration; a worker claiming its pool without the handler refuses
     to start, and so does a worker handling a job nobody defined.
   - the remaining formula: chains, fan-out + barrier, barrier phases
     (including a barrier added by the draining completion), signal steps
     holding barriers and finalization.
   - both waits on one step (`p.After().Step(n, in, jobs.WaitsForSignal())`):
     the step moves `waiting_for_steps → waiting_for_signal` at the phase
     drain and now counts; a signal buffered before the drain is consumed
     at dispatch; finalization waits behind a dispatched-but-unsignaled
     step. No combined word: the two waits compose in sequence.
   - takeover via extend: after a lapsed step is cleared, its original
     worker's next extend is missing that row and the handler is canceled;
     its late complete returns false and changes nothing.
   - cancel mid-handler: `cb_job_cancel` while a handler runs — the
     status-check wrapper cancels the context, the late complete no-ops.
   - output resolution: the run's output is explicit or null — a
     `SetRunOutput` from any step lands (last completion wins), and a run
     whose steps never set one finishes with output null, the finishing
     step's own output untouched on its row.
   - routing: a new step lands in its job's pool with that pool's terms; an
     undeclared step name walks its parent to give-up through the normal
     budget (§5); worker coverage validation refuses partial pools; a worker
     claiming a step it holds no handler for releases it with a pause, no
     attempt spent; a queue-array claim serves several pools in one call.
   - schedules: a due row births exactly one run and re-arms in one
     transaction; `catch_up` semantics ported from the scheduler tests.
   - define convergence: changed terms, jobs and schedules apply on
     redeploy; unchanged define writes nothing; an `on_fail` naming an
     undeclared job, or a `queue` without a terms row, is rejected.
   - cancel during a retry gap (step queued with future `claimable_at` →
     canceled, never claimed).
   - triggers (M4c): exactly-once creation — kill the tick mid-batch, the
     batch rolls back, no duplicate runs; a deleted job stalls the
     trigger loudly at its cursor and delivery resumes after the define;
     the position key stays idempotent across a cursor reset; the run's
     input equals the payload as published; `start_pos`
     honored; only matching messages deliver; the job schema installs
     without the stream schema, and `cb_job_define_trigger` there raises
     `catbird: stream schema required` (D41).
   - step-to-step latency benchmark (notify + claim — no tick in the path);
     throughput ≥ the old `BenchmarkTaskThroughput` / `FlowThroughput`
     envelope; the wide-map stress test (the D30 watch item).
   - the deliberately slow Python demo worker (M4b) — the worker contract,
     extend included, release skipped (legal), slower than the claim TTL.

The decision-log entries for this design (D34, D38–D40, D44) live in the
README with the rest of the log; this document is their detailing chapter.
