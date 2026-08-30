# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What is Catbird Lite?

A PostgreSQL-backed job queue, stream, and small workflow engine. Five tables,
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
by hand. `setupTestDB` drops the five tables and applies
`migrations/00001_lite.sql` on every test, so tests do not share state.

`bench_hot.sh` measures index bloat on `cb_claims` under sustained update churn.
It talks to the same database directly and needs no Go build.

## Layout

Five files, one migration, no packages:

- `client.go` — every statement a caller runs, as package functions:
  `Publish`, `PublishBatch`, `Enqueue`, `EnqueueBatch`, `Complete`, `Cancel`,
  `GC`, `ResolveDependency`, `DeliverSignal`, `SetOutput`, `Output`, and the
  stream reads `ReadAfter`, `LastPosition` and `OldestPosition`. Each takes a
  `Conn` — a pool, a connection, or a transaction — and holds no state; what a
  process configures lives on the `Runtime`. It also holds the three values a
  caller handles: `Message`, a published message as a reader gets it; `Job`,
  one claimed unit of work as a handler is given it; and `Cursor`, a name and
  the patterns read under it, which carries `Read` and `Ack` as methods and
  holds no connection either.
- `runtime.go` — `New`, `Start`, and the two loops every process runs whether or
  not anything is declared: the position assigner and the one `LISTEN`
  connection that wakes the rest. One `Runtime` per process owns the pool and
  one goroutine per declared worker and trigger.
- `worker.go` — the claim loop, completion, retries, `OnDead`.
- `trigger.go` — `Trigger`: the loop that turns stream messages into jobs. The
  reads it uses live in `client.go` with the other statements.
- `migrations/00001_lite.sql` — the whole schema. Goose markers, no goose
  dependency; the tests split the file on `-- +goose down`.

## Architecture

**Tables.** `cb_messages` holds every job's message and every published
message, one row, written once. `cb_claims` holds one narrow row per job that still has to
run, rewritten on every claim and retry, deleted on completion. `cb_cursors`,
`cb_signals` and `cb_outputs` are one row each per consumer, per delivered
signal, per job result.

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
- Stream readers go by `position`, never by `id`. Positions are set after commit
  by the assigner, so they follow commit order.
- A topic pattern is a topic, a prefix with `.#`, or `#`, and each pattern
  compiles to its own comparison. A list compared with `= ANY` or `LIKE ANY`
  cannot be read as index arms and walks the position index instead.
- The assigner only sets positions that are still empty, so two assigners
  running at once cannot move a position a reader may already have passed.
- `Lease`, `MaxAttempts` and `Backoff` are worker settings, not job settings.
  All workers on one queue must agree on them.
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

**Job and message.** A job is one claimed unit of work; a message is a row of
`cb_messages`, and a published message is what a consumer reads. The word job
belongs where a claim does, and the type carries it rather than the name: the
handler type is `Handler` and takes a `*Job`, and the completion is `Complete`.
`ResolveDependency`, `DeliverSignal`, `SetOutput` and `Output` address a job, so
their parameter is `jobID`, while the column it matches stays `message_id`.

**Errors.** Sentinels are prefixed with `catbird:` and checked with
`errors.Is`.
