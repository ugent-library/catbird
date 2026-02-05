# Catbird Copilot Instructions

Catbird is a PostgreSQL-based message queue with task and workflow execution engine. Below is critical context for productive development.

## Architecture Overview

**Core Pattern**: Message-driven workers using PostgreSQL as the queue backend (adapted from pgmq and pgflow projects). **Database is the sole coordinator**—no external service discovery or locking needed; scale workers horizontally and PostgreSQL handles message distribution & state management.

**Three Main Components**:
1. **Client** (`client.go`): Facade delegating to standalone functions; call `catbird.New(conn)` to create
2. **Worker** (`worker.go`): Runs tasks, flows, and scheduled jobs; initialized with `catbird.NewWorker(ctx, conn, opts...)`. Multiple workers can run concurrently; DB ensures each message is processed exactly once.
3. **Dashboard** (`dashboard/`): Web UI for starting task/flow runs, monitoring progress in real-time, and viewing results; served via CLI `cb dashboard`

**Two Independent Systems**:
1. **Generic Message Queues**: `Send()`, `Dispatch()`, `Read()` operations similar to pgmq/SQS. Messages stored in queue tables; independent from tasks/flows.
2. **Task & Flow Execution**: Task/flow definitions describe shape; `RunTask()` or `RunFlow()` create entries in task_run/step_run tables (which act as queues themselves). Worker reads from these tables and executes handlers. State tracked via constants (created, started, completed, failed).

## Database Schema

All schema is version-controlled in `migrations/` (goose-managed):
- **Queues** (v1): `cb_queues` table + message functions; custom types `cb_message` (7 fields including id, topic, payload)
- **Tasks/Flows** (v2): Task definitions, runs, flows; custom types `cb_task_message`, `cb_step_message`
- **GC** (v3): Garbage collection routines

Key: Migrations use goose with `DisableVersioning` + embedded FS. Schema version = 3.

## Handler Pattern & Generics

**Task handlers use generic codegen**:
```go
// Handler fn: (context.Context, InputType) -> (OutputType, error)
task := catbird.NewTask("my_task", func(ctx context.Context, input MyInput) (MyOutput, error) {
    return MyOutput{}, nil
}, catbird.WithConcurrency(5), catbird.WithRetries(3))
```
- Input/output marshaled as JSON automatically
- Options are applied via `HandlerOpt` interface (see `concurrencyOpt`, `retriesOpt`, etc. pattern)
- Payloads are `json.RawMessage`; handlers receive `[]byte`

**Flows**: Multi-step DAGs with dependencies. Steps execute when their dependencies complete (simple DAG semantics). Flow output is the combined JSON object of all step outputs; step names are unique within a flow and form the output keys.
```go
NewFlow("workflow", 
    InitialStep("step1", func(ctx context.Context, in string) (string, error) {
        return in + " processed by step 1", nil
    }),
    StepWithOneDependency("step2",
        Dependency("step1"),
        func(ctx context.Context, in string, step1Out string) (string, error) {
            return step1Out + " and by step 2", nil
        }),
    StepWithOneDependency("step3",
        Dependency("step2"),
        func(ctx context.Context, in string, step2Out string) (string, error) {
            return step2Out + " and by step 3", nil
        }),
)
// Flow output: { "step1": "...", "step2": "...", "step3": "..." }
```

## Key Conventions

- **Worker lifecycle**: `client.NewWorker(ctx, opts...)` → `worker.Start(ctx)` → `worker.Wait()` (graceful shutdown with timeout)
- **Options pattern**: All configs use receiver interface `apply(&target)` (HandlerOpt, WorkerOpt, etc.)
- **Conn interface**: Abstracts pgx; accepts `*pgxpool.Pool`, `*pgx.Conn`, or `pgx.Tx`
- **Logging**: Uses stdlib `log/slog`; workers accept custom logger via `WithLogger()`
- **Scheduled tasks/flows**: Use robfig/cron syntax; `WithScheduledTask("name", "@hourly")` or `WithGC()`
- **Deduplication**: Messages support `DeduplicationID` field to prevent duplicates
- **Task/Flow execution**: `client.RunTask()` or `client.RunFlow()` return handles with `WaitForOutput()` to block until completion

## Developer Workflows

**Setup**: 
```bash
go mod download
# Requires PostgreSQL with `CB_CONN` env var (connstring)
```

**Testing**: 
- Unit tests in `*_test.go` files (see `catbird_test.go`)
- Uses testcontainers for Postgres integration

**Dashboard**: 
- Start task and flow runs from web UI
- Monitor real-time progress and state updates  
- View task/flow results once completed
- Access via: `CB_CONN="postgres://..." go run ./cmd/cb/main.go dashboard --port 8080`

**Add migrations**: Create new `.sql` file in `migrations/`, use goose syntax (`+goose up`/`+goose down`)

## Critical Files

- [catbird.go](/catbird.go): Message, Task, Flow, Step, Options definitions
- [worker.go](/worker.go): Worker struct, scheduling, polling logic
- [client.go](/client.go): Public API (delegation layer)
- [dashboard/handler.go](/dashboard/handler.go): HTTP routes & templating
- [migrations/](/migrations/): Database schema (versioned)

## Common Patterns to Replicate

1. **Errors**: Use `ErrTaskFailed`, `ErrFlowFailed` package-level errors
2. **Context propagation**: All DB ops accept `context.Context` first param
3. **JSON payloads**: Custom types → JSON via generics; validation happens in handler
4. **Retries**: Built-in with exponential backoff + jitter; configured per handler
5. **Concurrency**: Default 1 per handler; tweak with `WithConcurrency(n)`
