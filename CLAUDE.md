# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What is Catbird Lite?

A PostgreSQL-backed job queue, stream, and small workflow engine. Four tables
plus a migrations record, plain SQL, no PL/pgSQL, no extensions. Postgres is
the only coordinator; workers scale by starting more processes.

Start with `docs/architecture.md` for the system design, `docs/usage.md` for
application guidance, and `docs/decisions.md` for deliberate boundaries. The
code and its package comments are the exact API contract.

## The earlier versions

Catbird was rewritten. The released versions before the rewrite — the v0.2.x
series and everything under it — live on the `v0.2` branch, which is where
their bugfix releases are made and tagged. The new series on `main` starts at
v0.3.0.

An unreleased intermediate rewrite — `streams/`, `jobs/`, `wire/`,
`notify/`, the PL/pgSQL SQL API, the dashboard, the TUI, the `cmd/cb` CLI, and
about six thousand lines of plan and decision-log documents — lives on the
`streams` branch and is reachable from there:

```bash
git show origin/streams:docs/plan/03-job.md
```

Keep that branch. It is where the reasoning behind rulings this code still
follows is written down. Nothing on it is being built any more, so read it for
history and never as instructions.

## Development

```bash
docker compose up -d                          # postgres 16 on 5432
psql postgres://postgres:postgres@localhost:5432/postgres -c 'CREATE DATABASE cb_tst'
go test ./...
go test ./... -run TestEnqueueBatch
```

Tests use a hardcoded DSN, `postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable`,
and no env vars. The compose file creates no database, so `cb_tst` is made once
by hand. `setupTestDB` runs `MigrateDownTo(0)` and `MigrateUp` on every test,
so tests do not share state and every down section is exercised. The
root and wire packages test against the same database and `go test ./...` runs
their binaries at once, so each `TestMain` holds advisory lock
`(hashtext('catbird'), 2)` for the binary's life and the two suites take
turns.

`bench_hot.sh` measures index bloat on `cb_jobs` under sustained update churn.
It talks to the same database directly and needs no Go build.

## Layout

Eight files and one migration in the root package, and one package under it:

- `job_type.go` — the two declarations an application writes. `Queue` is a name
  and how work runs under it: `BatchSize`, `ClaimDuration`, `HandlerTimeout`, `PollInterval`.
  It decides who competes with whom, and it is the claim key. `JobType` is a kind of job:
  its name, its queue, and how a run of it is retried (`Signal`, `Schedule`,
  `MaxAttempts`, `MinBackoff`, `MaxBackoff`, `OnFailed`). Both are plain Go values; nothing about
  them is written to the database, and only their names reach a row. The
  handler is not on either — everything on a job type is written on a job row or
  decided about a run, and a handler is neither, so it is given at
  registration.
- `client.go` — every statement a caller runs, as package functions:
  `Publish`, `PublishBatch`, `Enqueue`, `EnqueueBatch`, `Complete`, `Signal`,
  `Cancel`, `Status`, `GroupStatus`, `Queues`, `GC`, and the stream reads
  `ReadAfter`, `LastPosition` and `OldestPosition`. Each takes a `Conn` — a pool, a
  connection, or a transaction — and holds no state; what a process runs lives
  on the `Runtime`. It also holds the values a caller handles: `Message`,
  a published message as a reader gets it; `Job`, one claimed unit of work as a
  handler is given it, which carries `SetOutput`, `Enqueue`, `EnqueueAfter` and
  `DependencyOutputs` as methods; `Cursor`, a name and the patterns read
  under it, which carries `Read` and `Ack` as methods and holds no connection
  either; the status objects `JobStatus` and `JobGroupStatus`, which share the
  one `State` enum, and `QueueInfo`, the per-queue counts `Queues` returns; and `Outputs`, recorded results as rows that unmarshal into
  the caller's types with `Scan`, `Get` and `GetAll`.
- `runtime.go` — `New`, `Handle`, `HandleFunc`, `Start`, the two loops every process runs
  whether or not anything is registered: the position assigner and the one
  `LISTEN` connection that wakes the rest, and the GC loop a process with
  `Options.Retention` set runs beside them. One `Runtime` per process owns the
  pool and one goroutine per queue, trigger and consumer.
- `worker.go` — the claim loop, dispatch by job type, completion, retries,
  `OnFailed`, and the claim renewal a queue with `HandlerTimeout` above `ClaimDuration` runs.
  One worker per queue, however many job types run on it. The handler
  runs on a context of its own carrying the queue's `HandlerTimeout`; the worker's own
  context is what tells a shutdown from a failure, so the two are never the same
  variable.
- `trigger.go` — `Runtime.Trigger`: the loop that turns stream messages into
  jobs. The reads it uses live in `client.go` with the other statements.
- `consumer.go` — `Runtime.Consume`: the loop that hands stream messages to a
  handler in batches, one process at a time. The claim on the cursor row, its
  renewal on a consumer whose `HandlerTimeout` exceeds `ClaimDuration`, and the
  ack that matches on it live here; the read is `ReadAfter`. A consumer has no
  retry policy: a failed batch is handed out again until it passes.
- `periodic.go` — the schedule parser and the tick loop `Handle` starts for a
  job type declared with `Schedule`, so a scheduled type ticks in the
  processes that can run it. A tick is an `Enqueue` with the minute's
  deduplication key, in `client.go` with the others.
- `migrate.go` — the runner and the three ways a caller applies the schema:
  `MigrateUp`/`MigrateDownTo` for a caller with nothing, `MigrationsFS` for a
  caller with goose, `Migrations()` (parsed up and down sections) for any
  other tool. Each migration runs in its own transaction with its
  `cb_migrations` row; concurrency is an advisory lock (key 3 under
  `hashtext('catbird')`) plus the insert guard. A migration is two files,
  `<number>_<name>.up.sql` and `<number>_<name>.down.sql`, each run whole;
  `Migrations()` pairs them by version and refuses a version missing either.
- `migrations/00001_schema.up.sql` and `00001_schema.down.sql` — the whole
  schema, and the drops that undo it.
- `wire/` — the browser layer, a package of its own so the core keeps no
  `net/http`; `docs/architecture.md` and `docs/usage.md` describe its boundary.
  `renderer.go` holds
  `Renderer`, a process-global mux from topic patterns to handlers: the
  pattern grammar is the stream's plus `{name}`, which matches one segment in
  Go and never reaches SQL, and dispatch runs each rule once per distinct
  variable binding with that binding's messages, so a batch of fifty edits
  renders a record once. A handler error drops that call's fragments and the
  rest still go out. `wire.go` holds the `Wire` value — pool, renderer, token
  secret, one per process — and the token: `Token` signs the cursor a page
  acks and the topics it may read, HMAC-SHA256 over the JSON, base64url, no
  expiry; `Verify` answers `ErrInvalidToken` for everything else, one error on
  purpose. `serve.go` holds the first transport: `ServePoll` verifies — 401
  otherwise — and hands the grant to `Serve`, which reads after the token's
  cursor with the token's topics, dispatches through the renderer, writes
  every fragment as one HTML response or a 204 when nothing came out, and
  acks the last position read. The ack comes after the response on a context
  the client cannot cancel — sent is seen — and an empty poll acks nothing,
  so idle pages write no rows. The form without a cursor, where the page
  holds the position and polls with `?after=`, and SSE are deferred until a
  page needs them; `docs/decisions.md` records those rulings.

## Architecture

**Tables.** `cb_messages` holds every job's payload and every published message,
one row, written once. `cb_jobs` holds one narrow row per job that still has
to run, rewritten on every claim and retry, deleted by the statement that ends
the job; it carries the queue, the job type, the workflow, what the job waits
for, the signal payload once one arrives, what the last failed attempt
returned, and the key at most one live job of its type carries.
`cb_job_results` holds one row per job that ended, written by the
same statement that deleted its job row: how it ended — completed, failed or
canceled — when it ended, the attempts it spent, the last error, and the
output it recorded.
`cb_cursors` is one row per stream consumer. `cb_migrations` is one row per
schema change that ran, created by the runner rather than by a migration, and
touches no hot path.

**Queue and job type are two things.** The queue is the claim key and the
concurrency bound — it decides who competes with whom for `BatchSize` slots.
The job type decides what code runs and how a run of it is retried. Several
types share a queue, so a process handling thirty kinds of work runs one claim
loop, not thirty, and no handler switches on a topic to find out what it is.

**Statements live in Go, not in the database.** There are no SQL functions and
no PL/pgSQL. A client in another language reimplements the same statements
against the same schema; the schema plus `client.go` is the whole contract. The
cross-client contract is not written down yet and will not be until a second
client exists.

**Invariants that edits must not break:**

- A job's completion is `DELETE FROM cb_jobs WHERE message_id = $1 AND attempts = $2`
  and, in the same statement, the insert of its result. The delete matches on
  `attempts`, so two workers may run the same job and only the attempt that
  still holds the claim deletes it. The last failed attempt and `Cancel` end a
  job the same way, with a result that says failed or canceled, so a job is in
  `cb_jobs` or in `cb_job_results` and never in both. A handler is given no connection: it runs
  `Complete` in its own transaction to end the job in the same commit as its
  writes, or returns `nil` and lets the worker complete it afterwards. The
  worker holds no transaction and no connection while a handler runs.
- Everything a handler asked for hangs off that one delete, in the same
  statement: the result `SetOutput` recorded, the countdown of the jobs waiting
  for this one, and the jobs `Enqueue` and `EnqueueAfter` recorded. All three are
  buffered on `Job` and written by the completion, so an attempt that lost its
  claim writes no result, counts nothing down and creates no jobs, and a handler
  that fails halfway retries with an empty buffer.
- Nothing outside catbird holds a dependency count. `EnqueueAfter` takes the
  count of the buffer's other jobs and their ids in `dependency_job_ids`, and each
  of those carries the waiting job's id in `dependent_job_ids`; all three are
  derived inside the completion from the ids the same statement hands out. The
  ids are what `DependencyOutputs` reads, so a joining job takes the results of
  the jobs it waited for and not every job of their type in the workflow. The
  CTE that hands them out is `MATERIALIZED` because `nextval` is volatile and an
  inlined CTE would give a message and its claim different ids.
- A job waiting for a signal has `claimable_at = 'infinity'`, so waiting is a
  delay and needs no place in the ready index. `Signal` writes the payload and
  sets `claimable_at` to `now()`.
- What a live job is doing is not stored. `Status` derives its six states
  from `claimable_at`, `attempts`, `dependencies`, `awaits_signal` and `error`,
  because three of them end by the clock and not by a write: scheduled,
  waiting to retry and a lapsed claim all become queued when `claimable_at`
  passes. A stored column would be stale across those boundaries or need a
  sweeper. There is deliberately no `status` column on `cb_jobs`: the word
  names the derived answer, and a column of the same name meant something
  narrower in the same statement, which is what a second client stumbles
  over. How a job ended is stored, as `cb_job_results.state` — `completed`,
  `failed` or `canceled`, the same words `Status` reports — because nothing
  about a job that ended changes with time.
- Retention runs from when a job ended. `GC` deletes results by
  `cb_job_results.ended_at` and then the messages no job row and no result
  refers to, so a job that took a month to run is inspectable for as long
  after it ended as one that took a second. Both deletes go by an index on
  that column, so a run that finds nothing reads a few pages. A runtime with
  `Options.Retention` set runs `GC` once at start and then hourly, in every
  process, behind `pg_try_advisory_xact_lock` in one transaction so runs never
  overlap; `Retention` is not stored, so processes that disagree give the
  database the shorter one. History beyond retention, and anything per
  attempt, is the application's own table.
- `cb_jobs` declares its columns widest first, so the fixed-width ones pack
  with no padding between them: 72 bytes against 75, on the row every claim and
  retry rewrites. A column added in the middle by role rather than by width
  costs padding.
- The failure writes `error` and the claim clears it, which is the only
  thing that tells `StateRunning` from `StateWaitingToRetry`: both are a live
  claim with `claimable_at` in the future and an attempt spent. The write carries
  matches on `attempts` like the retry it rides on, so a late attempt
  records no error text either, and the 256-character cut keeps a job row out
  of TOAST. It is not a run history: the next failure overwrites it, and the
  statement that ends the job copies the last one to the result.
- A workflow is `coalesce(group_id, message_id)` of the job that started it.
  `group_id` is NULL on a job that stands alone, which keeps the volume of
  single-shot jobs out of `cb_jobs_group_idx` and off its write cost.
- A consumer claims its cursor before it reads: `claimable_at` is set to
  `now() + ClaimDuration` where it has passed, and the ack matches on the value
  the claim returned, as every write on a job matches on `attempts`. A process
  whose claim was taken over moves nothing and releases nothing. Renewal moves
  the deadline and returns the new one, and a renewal that matches nothing
  cancels the handler with `ErrClaimLost`. `ClaimDuration` and `HandlerTimeout`
  default from each other by the queue's rule, in `defaultDurations`.
  `Cursor.Ack` takes no claim; it is for a reader that runs in one place, like
  wire's poll.
- Stream readers go by `position`, never by `id`. Positions are set after commit
  by the assigner, so they follow commit order.
- A topic pattern is a topic, a prefix with `.#`, or `#`, and each pattern
  compiles to its own comparison. A list compared with `= ANY` or `LIKE ANY`
  cannot be read as index arms and walks the position index instead.
- The assigner only sets positions that are still empty, so two assigners
  running at once cannot move a position a reader may already have passed.
- A scheduled type's tick is an `Enqueue` with two keys: the deduplication
  key `periodic:<type>:<minute>` makes every process ticking in the same
  minute one job, and the type's name as `UniqueKey`, which `Enqueue` gives
  every job of a scheduled type, makes a tick during a live run write
  nothing — at most one job of a scheduled type is live, a run that outlives
  its schedule swallows the ticks it covers rather than queueing them, and
  two manual enqueues cannot overlap. A batch, a trigger and a handler's
  `Enqueue` cannot give a job that key, so they refuse a scheduled type as
  they refuse a type that waits for a signal.
- `BatchSize`, `ClaimDuration` and `HandlerTimeout` are queue settings; `Schedule`,
  `MaxAttempts`, `MinBackoff`, `MaxBackoff` and `OnFailed` are job type settings. `ClaimDuration` is on
  the queue because the claim sets it for a whole batch in one statement, and
  `HandlerTimeout` because its comparison with `ClaimDuration` is itself a setting: `HandlerTimeout`
  above `ClaimDuration` makes the worker renew the claims of its running jobs every
  half `ClaimDuration`, which is how a queue runs jobs longer than its claim. Renewal
  follows the handler's context — past `HandlerTimeout` a job is renewed no further —
  matches on `attempts` like every write, and cancels the handler with
  `ErrClaimLost` when it matches no row. Each duration left unset defaults
  from the other so the claim covers the handler; renewal only ever runs on a
  queue that set both. The handler belongs to
  neither: it is the process's, and `rt.Handle(jobType, handler)` is where the
  two meet.
- Hot-path SQL takes no joins, no advisory locks, and no N+1 loops. Advisory
  locks belong to the loops that let one process do a run while the rest skip
  — the assigner's (key 1) and GC's (key 4) — and to the migration runner
  (key 3), all under `hashtext('catbird')`; key 2 is the test binaries'.
- The unique indexes on `deduplication_key`, `position` and `unique_key` are
  partial. Deduplicating inserts must name the predicate —
  `ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING`,
  `ON CONFLICT (job_type, unique_key) WHERE unique_key IS NOT NULL DO NOTHING`
  — or they stop matching the index.
- `UniqueKey` is unique among live jobs because a job row exists only while
  the job is live: the statement that ends a job frees the key by deleting the
  row, so no ending writes anything for it and the claim, retry and completion
  statements never see it. Only `Enqueue` sets it. Its message insert probes
  the index first, so the common duplicate writes nothing, and the job
  insert's `ON CONFLICT` catches two enqueues racing on one key; the loser
  leaves a message with no job, which GC deletes with the rest. Triggers and
  `EnqueueBatch` leave the column NULL: a trigger must never drop a message,
  and many messages about one record becoming one run is `Consume`.

## Conventions

**SQL.** Four spaces, no tabs. A primary key is `id`; a reference is
`<thing>_id`; no abbreviations. Every migration's `down` section drops
everything its `up` created.

**Names and comments.** Plain language, no metaphors, no coined vocabulary. Name
what a thing does in the sentence a reader would use for it. A comment states
the rule and then, when it earns its place, the concrete failure it prevents —
with the measurement when there is one. Comments never narrate history: what
changed belongs in the commit message, not in the file.

**Job, job type and message.** A job is one claimed unit of work; a job type is
the kind of job it is, declared once and used by both the enqueue and the
worker; a message is a row of `cb_messages`, and a published message is what a
consumer reads. A result is how a job ended, one row of `cb_job_results`; an
output is the value a handler recorded with `SetOutput`, a column on the
result. The two words are never swapped. An attempt fails and the job waits to
retry; when the last attempt fails the job has failed; `Cancel` cancels a job;
a job that succeeded completed. The word dead is not used. The word job belongs where a claim does, and the type carries it
rather than the name: the handler type is `Handler` and takes a `*Job`, and the
completion is `Complete`. `Signal`, `Cancel` and `GroupStatus` address a
workflow, so their parameter is `groupID`, while the column it matches stays
`group_id`.

**Errors.** Sentinels are prefixed with `catbird:` and checked with
`errors.Is`.
