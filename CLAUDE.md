# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What is Catbird Lite?

A PostgreSQL-backed job queue, stream, and small workflow engine. Four tables,
plain SQL, no PL/pgSQL, no extensions, one dependency (pgx). Postgres is the
only coordinator; workers scale by starting more processes.

`DESIGN.md` is the specification and the first thing to read. It describes what
is built and, under "Planned additions", what is not — do not treat anything in
that section as existing code.

## The earlier design

Catbird was rewritten. The previous version — `streams/`, `jobs/`, `wire/`,
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
by hand. `setupTestDB` drops the four tables and applies
`migrations/00001_lite.sql` on every test, so tests do not share state.

`bench_hot.sh` measures index bloat on `cb_claims` under sustained update churn.
It talks to the same database directly and needs no Go build.

## Layout

Five files, one migration, no packages:

- `job_type.go` — the two declarations an application writes. `Queue` is a name
  and how work runs under it: `BatchSize`, `Lease`, `Timeout`, `PollInterval`.
  It decides who competes with whom, and it is the claim key. `JobType` is a kind of job:
  its name, its queue, and how a run of it is retried (`Signal`,
  `MaxAttempts`, `MinBackoff`, `MaxBackoff`, `OnDead`). Both are plain Go values; nothing about
  them is written to the database, and only their names reach a row. The
  handler is not on either — everything on a job type is stamped on a claim or
  decided about a run, and a handler is neither, so it is given at
  registration.
- `client.go` — every statement a caller runs, as package functions:
  `Publish`, `PublishBatch`, `Enqueue`, `EnqueueBatch`, `Complete`, `Signal`,
  `Cancel`, `Status`, `GC`, `Output`, `Outputs`, and the stream reads
  `ReadAfter`, `LastPosition` and `OldestPosition`. Each takes a `Conn` — a pool, a
  connection, or a transaction — and holds no state; what a process runs lives
  on the `Runtime`. It also holds the three values a caller handles: `Message`,
  a published message as a reader gets it; `Job`, one claimed unit of work as a
  handler is given it, which carries `SetOutput`, `Enqueue`, `EnqueueAfter` and
  `DependencyOutputs` as methods; and `Cursor`, a name and the patterns read
  under it, which carries `Read` and `Ack` as methods and holds no connection
  either.
- `runtime.go` — `New`, `Handle`, `Start`, and the two loops every process runs
  whether or not anything is registered: the position assigner and the one
  `LISTEN` connection that wakes the rest. One `Runtime` per process owns the
  pool and one goroutine per queue and trigger.
- `worker.go` — the claim loop, dispatch by job type, completion, retries,
  `OnDead`. One worker per queue, however many job types run on it. The handler
  runs on a context of its own carrying the queue's `Timeout`; the worker's own
  context is what tells a shutdown from a failure, so the two are never the same
  variable.
- `trigger.go` — `Runtime.Trigger`: the loop that turns stream messages into
  jobs. The reads it uses live in `client.go` with the other statements.
- `migrations/00001_lite.sql` — the whole schema. Goose markers, no goose
  dependency; the tests split the file on `-- +goose down`.

## Architecture

**Tables.** `cb_messages` holds every job's payload and every published message,
one row, written once. `cb_claims` holds one narrow row per job that still has
to run, rewritten on every claim and retry, deleted on completion; it carries
the queue, the job type, the workflow, what the job waits for, the signal
payload once one arrives, when it died if no worker will claim it again, and
what the last failed attempt returned. `cb_cursors` and `cb_outputs` are one row each per
stream consumer and per job result.

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

- A job's completion is `DELETE FROM cb_claims WHERE message_id = $1 AND attempts = $2`.
  `attempts` is the lease token. Two workers may run the same job; only the
  lease holder deletes the claim. A handler is given no connection: it runs
  `Complete` in its own transaction to end the job in the same commit as its
  writes, or returns `nil` and lets the worker complete it afterwards. The
  worker holds no transaction and no connection while a handler runs.
- Everything a handler asked for hangs off that one delete, in the same
  statement: the result `SetOutput` recorded, the countdown of the jobs waiting
  for this one, and the jobs `Enqueue` and `EnqueueAfter` recorded. All three are
  buffered on `Job` and written by the completion, so an attempt that lost its
  lease writes no result, counts nothing down and creates no jobs, and a handler
  that fails halfway retries with an empty buffer.
- Nothing outside catbird holds a dependency count. `EnqueueAfter` takes the
  count of the buffer's other jobs and their ids in `dependency_job_ids`, and each
  of those carries the waiting job's id in `dependent_job_ids`; all three are
  derived inside the completion from the ids the same statement hands out. The
  ids are what `DependencyOutputs` reads, so a joining job takes the results of
  the jobs it waited for and not every job of their type in the workflow. The
  CTE that hands them out is `MATERIALIZED` because `nextval` is volatile and an
  inlined CTE would give a message and its claim different ids.
- A job waiting for a signal has `visible_at = 'infinity'`, so waiting is a
  delay and needs no place in the ready index. `Signal` writes the payload and
  sets `visible_at` to `now()`.
- What a job is doing is not stored. `cb_claims.died_at` is one timestamp —
  when the job died, never to be claimed again, and what `GC`'s age test runs
  from; NULL while it lives — and `Status` derives the eight states a caller
  sees from `visible_at`, `attempts`, `dependencies`, `awaits_signal` and
  `last_error`. There is deliberately no `status` column: the word names the
  derived answer, and a column of the same name meant something narrower in the
  same statement, which is what a second client stumbles over.
- `cb_claims` declares its columns widest first, so the fixed-width ones pack
  with no padding between them: 74 bytes against 78, on the row every claim and
  retry rewrites. A column added in the middle by role rather than by width
  costs padding.
- The failure writes `last_error` and the claim clears it, which is the only
  thing that tells `StatusRunning` from `StatusWaitingToRetry`: both are a live
  claim with `visible_at` in the future and an attempt spent. The write carries
  the `attempts` lease token like the retry it rides on, so a late attempt
  records no error text either, and the 256-character cut keeps a claim row out
  of TOAST. It is not a run history: the next failure overwrites it and the
  completion deletes it with the row.
- A workflow is `coalesce(group_id, message_id)` of the job that started it.
  `group_id` is NULL on a job that stands alone, which keeps the volume of
  single-shot jobs out of `cb_claims_group_idx` and off its write cost.
- Stream readers go by `position`, never by `id`. Positions are set after commit
  by the assigner, so they follow commit order.
- A topic pattern is a topic, a prefix with `.#`, or `#`, and each pattern
  compiles to its own comparison. A list compared with `= ANY` or `LIKE ANY`
  cannot be read as index arms and walks the position index instead.
- The assigner only sets positions that are still empty, so two assigners
  running at once cannot move a position a reader may already have passed.
- `BatchSize`, `Lease` and `Timeout` are queue settings; `MaxAttempts`,
  `MinBackoff`, `MaxBackoff` and `OnDead` are job type settings. `Lease` is on
  the queue because the claim sets it for a whole batch in one statement, and
  `Timeout` because it has to agree with `Lease`. The handler belongs to
  neither: it is the process's, and `rt.Handle(jobType, handler)` is where the
  two meet.
- Hot-path SQL takes no joins, no advisory locks (the assigner's is the one
  exception), and no N+1 loops.
- The unique indexes on `dedup_key` and `position` are partial. Deduplicating
  inserts must name the predicate — `ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING`
  — or they stop matching the index.

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
consumer reads. The word job belongs where a claim does, and the type carries it
rather than the name: the handler type is `Handler` and takes a `*Job`, and the
completion is `Complete`. `Signal`, `Cancel`, `Output` and `Outputs` address a
workflow, so their parameter is `groupID`, while the column it matches stays
`group_id`.

**Errors.** Sentinels are prefixed with `catbird:` and checked with
`errors.Is`.
