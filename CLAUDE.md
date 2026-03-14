# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Catbird?

Catbird is a PostgreSQL-backed message queue with task and workflow (DAG) execution engine written in Go. PostgreSQL is the sole coordinator — no external services needed. Workers scale horizontally; the database handles message distribution and state management.

## Development Commands

```bash
# Start test PostgreSQL (required before running tests)
docker compose up -d

# Run all tests
./scripts/test.sh

# Run specific test(s)
./scripts/test.sh -run TestQueueCreate
./scripts/test.sh -run "TestBind.*"

# Slow/stress tests
CB_SLOW_TESTS=1 ./scripts/test.sh

# Benchmarks
go test -run '^$' -bench 'Benchmark(QueueThroughput|TaskThroughput|FlowThroughput)$' -benchtime=10s .

# Reset database completely
docker compose down -v && docker compose up -d
```

There is no Makefile. Tests use a hardcoded DSN (`postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable`) with no env vars needed.

## Architecture

**Two independent systems:**

1. **Generic Message Queues** — `Send()`, `Publish()`, `Read()` operations (SQS-like). Topic routing via bindings with wildcard support (`?` single token, `*` multi-token tail).
2. **Task & Flow Execution** — Tasks are single handlers; Flows are DAGs of steps with dependencies. `RunTask()`/`RunFlow()` create run entries; workers poll and execute handlers.

**Key components:**
- `client.go` — Public API facade
- `worker.go` — Polls and executes task/flow handlers
- `flow.go` — Flow DSL, step construction, dependency validation
- `task.go` — Task builder and handler reflection
- `queue.go` — Queue send/read/publish/bind operations
- `scheduler.go` — Cron-based scheduling
- `dashboard/` — Web UI for monitoring/management
- `tui/` — Terminal UI (Bubble Tea) for operational visibility
- `cmd/cb/` — CLI entry point (Cobra)

**Dynamic table naming:** Runtime tables are created per queue/task/flow:
- `cb_q_{name}` (queues), `cb_t_{name}` (tasks), `cb_f_{name}` (flows), `cb_s_{name}` (steps), `cb_m_{name}` (map tasks)

## Design Constraints

- **No PostgreSQL extensions** — only standard SQL and PL/pgSQL
- **Concurrent-safe SQL** — advisory locks for setup; `SKIP LOCKED`, atomic CTEs for hot paths
- **Hot path SQL must avoid** joins, advisory locks, and N+1 queries
- **Reflection-based handlers** — no type parameters; input/output types discovered at runtime. Handler signature: `func(ctx context.Context, input T) (output U, error)`
- **Builder pattern** for all public APIs: `NewTask(name).WithCondition(...).Do(fn, opts...)`
- **Idempotent API semantics** — if the requested effect is already true, return success (no-op)

## Deduplication Pattern (Critical)

Task/flow deduplication uses an atomic `ON CONFLICT DO UPDATE WHERE FALSE` + `UNION ALL` pattern. **Do not simplify this.** The `WHERE FALSE` prevents mutation on conflict; the `UNION ALL` fallback returns the existing row's ID. Without both parts, you get NULL returns or race conditions. See `.github/copilot-instructions.md` for the full SQL pattern.

## Migrations

Goose-managed in `migrations/` with version table `cb_goose_db_version`. Current schema version tracked by `SchemaVersion` constant in `migrate.go`.

When adding migrations:
1. Create `migrations/00NNN_description.sql` with `-- +goose up` / `-- +goose down` sections
2. Update `SchemaVersion` in `migrate.go`
3. Wrap multi-line PL/pgSQL in `-- +goose statementbegin` / `-- +goose statementend`
4. Use `LANGUAGE plpgsql AS $$` in CREATE, terminate with `$$;` (not `$$ LANGUAGE plpgsql;`)
5. SQL indentation: 4 spaces, no tabs
6. The `down` section must clean up everything the `up` section created
7. Update `docs/sql-api-reference.md` for any SQL function changes

## Flow Patterns

- **Conditions**: Tasks use `input.field` prefix; flow steps use `step_name.field` or `signal.field`
- **Optional dependencies**: When depending on conditional steps, use `Optional[T]` parameter type + `.OptionalDependency()`. Flow construction panics if violated.
- **Signals**: Steps with `.WithSignal()` wait for external input via `client.SignalFlow()`
- **Map steps**: `.MapFlowInput()` or `.MapStepOutput("step")` for array processing
- **Flow output**: The unwrapped output of the final step (not an aggregated object)

## Testing Notes

- Test harness uses `sync.Once` for one-time DB setup per suite (`catbird_test.go`)
- `./scripts/test.sh` drops and recreates the `cb_tst` database each run
- Dynamic tables persist across tests within a run — use unique identifiers to avoid stale data
- Use `startTestWorker()` helper with automatic cleanup
- `requireSlowTests()` gates stress tests behind `CB_SLOW_TESTS=1`

## Status Constants

Use Go constants (`StatusQueued`, `StatusStarted`, `StatusCompleted`, `StatusFailed`, `StatusSkipped`, `StatusCanceled`, etc.) — not raw string literals.
