# Catbird Copilot Instructions

Catbird is a PostgreSQL-based message queue with task and workflow execution engine. Below is critical context for productive development.

## Architecture Overview

**Core Pattern**: Message-driven workers using PostgreSQL as the queue backend (adapted from pgmq and pgflow projects). **Database is the sole coordinator**—no external service discovery or locking needed; scale workers horizontally and PostgreSQL handles message distribution & state management.

**Design Goals**:
1. **No PostgreSQL extensions**: All functionality implementable using only standard PostgreSQL features (PL/pgSQL, built-in functions, standard data types). Keeps deployment simple and maximizes compatibility.
2. **Concurrent update safe**: All SQL code must be race-condition free. Use advisory locks (`pg_advisory_xact_lock`), atomic operations (CTE with RETURNING), and proper isolation levels. Multiple workers can safely execute the same operations concurrently without data corruption.

**SQL Performance Patterns**:
- **Setup functions** (e.g., `cb_create_flow`, `Bind`): Safe to use advisory locks and joins. These are called once during initialization, not in hot paths.
- **Hot path runtime functions** (e.g., `Read()`, `Dispatch()`, message polling): Avoid expensive operations:
  - **No joins**: Filter by indexed keys only (e.g., `cb_queue.name`, `cb_queue.expires_at`)
  - **No advisory locks**: Use lock-free techniques instead (`SKIP LOCKED` for row-level concurrency, CTEs with `FOR UPDATE`, or atomic compare-and-swap logic)
  - **No N+1 queries**: Bulk operations with `RETURNING` clauses
  - Rationale: Runtime functions execute millions of times per worker; contention must stay minimal

**Main Components**:
1. **Client** (`client.go`): Facade delegating to standalone functions; call `catbird.New(conn)` to create
2. **Worker** (`worker.go`): Runs tasks and flows; initialized with `catbird.NewWorker(ctx, conn, opts...)`. Multiple workers can run concurrently; DB ensures each message is processed exactly once.
3. **Scheduler** (`scheduler.go`): Manages cron-based task and flow scheduling using robfig/cron; created internally by worker when using `WithScheduledTask` or `WithScheduledFlow`. Can also be used standalone.
4. **Dashboard** (`dashboard/`): Web UI for starting task/flow runs, monitoring progress in real-time, and viewing results; served via CLI `cb dashboard`

**Two Independent Systems**:
1. **Generic Message Queues**: `Send()`, `Dispatch()`, `Read()` operations similar to pgmq/SQS. Messages stored in queue tables; independent from tasks/flows. **Topic routing** via explicit bindings with wildcard support (`?` for single token, `*` for multi-token tail). Bindings stored in `cb_bindings` table with pattern type (exact/wildcard), prefix extraction for indexed filtering, and precompiled regexes.
2. **Task & Flow Execution**: Task/flow definitions describe shape; `RunTask()` or `RunFlow()` create entries in task_run/step_run tables (which act as queues themselves). Worker reads from these tables and executes handlers. State tracked via constants (created, started, completed, failed).

## Database Schema

All schema is version-controlled in `migrations/` (goose-managed):
- **Queues** (v1): `cb_queues` table (name PK, expires_at) + `cb_bindings` table (queue_name FK, pattern, pattern_type, prefix, regex) + message functions; custom types `cb_message` (7 fields including id, topic, payload). Bindings use exact match fast path (indexed) or wildcard (prefix filter + regex).
- **Tasks/Flows** (v2): Task definitions, runs, flows; custom types `cb_task_message`, `cb_step_message`; `cb_create_flow()` handles step dependencies; `cb_start_steps()` uses LOOP for cascading dependency resolution
- **GC** (v3): Garbage collection routines (`cb_gc()` deletes queues with `expires_at <= now()` and removes workers with stale heartbeats > 5 minutes old)
- **Conditions** (v4): Conditional branching support with `cb_parse_condition()`, `cb_evaluate_condition()`/`cb_evaluate_condition_expr()`, and condition columns on `cb_step_dependencies`
- **Conditions Integration** (v5): Modified `cb_create_flow()` to handle ConditionalDependency JSON and populate condition columns
- **Conditions Validation** (v6): Added `cb_check_reconvergence()` for validating flow structure and enforcing no-reconvergence rule

Key: Migrations use goose with `DisableVersioning` + embedded FS. Current schema version = 6.

**Table Name Construction**:
All runtime tables (messages, task runs, flow runs, step runs) are created dynamically using the `cb_table_name(name, prefix)` function:
- **Queues**: `cb_q_{name}` - Message tables for generic queues (prefix 'q')
- **Tasks**: `cb_t_{name}` - Task run tables (prefix 't')
- **Flows**: `cb_f_{name}` - Flow run tables (prefix 'f')
- **Steps**: `cb_s_{name}` - Step run tables for flows (prefix 's')

The function validates names (a-z, 0-9, _ only; max 58 chars) and returns `cb_{prefix}_{lowercased_name}`. When building queries that access these tables, always use the same construction pattern:
```go
tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(taskName))  // Tasks
tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(flowName))  // Flows
tableName := fmt.Sprintf("cb_s_%s", strings.ToLower(flowName))  // Steps
tableName := fmt.Sprintf("cb_q_%s", strings.ToLower(queueName)) // Queues
```

## Handler Pattern & Reflection-Based API

**Task handlers use reflection and a builder pattern**:
```go
// Handler fn: (context.Context, InputType) -> (OutputType, error)
task := catbird.NewTask("my_task").
  Handler(func(ctx context.Context, input MyInput) (MyOutput, error) {
    return MyOutput{}, nil
  }, &catbird.HandlerOpts{
    Concurrency: 5,
    MaxRetries:  3,
    MinDelay:    500 * time.Millisecond,
    MaxDelay:    10 * time.Second,
    CircuitBreaker: &catbird.CircuitBreaker{
      FailureThreshold: 5,
      OpenTimeout:      30 * time.Second,
    },
  })
```

**Key characteristics**:
- **No type parameters**: Input/output types are discovered at runtime via reflection (handlers receive `[]byte` payloads internally)
- **Builder pattern**: Construction via method chaining: `NewTask(name).Condition(...).Handler(fn, opts)`
- **Execution options**: Applied via `HandlerOpts` struct with public fields (Concurrency, MaxRetries, MinDelay, MaxDelay, CircuitBreaker, BatchSize, Timeout)
- **HandlerOpts validation**: Worker validates all task and flow step HandlerOpts at initialization time, catching configuration errors early before database operations
- **Task/step metadata**: Conditions applied via `.Condition(expr)` method chain

**Flows**: Multi-step DAGs with dependencies using builder pattern:
```go
flow := catbird.NewFlow("my_flow").
  AddStep(catbird.NewStep("step1").
    Handler(func(ctx context.Context, in string) (string, error) {
      return in + " modified", nil
    }, nil)).
  AddStep(catbird.NewStep("step2").
    DependsOn("step1").
    Handler(func(ctx context.Context, in string, step1Out string) (string, error) {
      return step1Out + " from step2", nil
    }, nil))
```

**CRITICAL - Flow Output Design**: Flow output is **the unwrapped output value of the final step** (the step with no dependents after completion). This is NOT an aggregated object. The flow's remaining_steps counter reaches 0 when the last step completes; that step's output becomes the flow's output. When the flow completes in `cb_complete_step()`, it directly selects that step's output and stores it as the flow's output.

**Conditional Execution**: Both tasks and flow steps support conditional execution via `.Condition(expression)` builder method. When a condition evaluates to false or a referenced field is missing, the task/step is skipped (status='skipped') instead of executed.
Use `not <expr>` to negate any condition expression (e.g., `not input.is_premium`).

**Task conditions** reference input fields with `input.*` prefix:
```go
task := catbird.NewTask("premium_processing").
  Condition("input.is_premium"). // Skipped if is_premium = false
  Handler(func(ctx context.Context, req ProcessRequest) (string, error) {
    return "processed", nil
  }, nil)
// Other examples: "input.amount gte 1000", "input.env eq \"production\""
```

**Flow step conditions** reference step outputs with `step_name.*` prefix and can also reference signal input via `signal.*` when present:
```go
NewFlow("risk-check").
  AddStep(NewStep("validate").
    Handler(func(ctx context.Context, amount int) (int, error) {
      return amount, nil
    }, nil)).
  AddStep(NewStep("audit").
    DependsOn("validate").
    Condition("validate gt 1000"). // Conditional step
    Handler(func(ctx context.Context, in int, validateOut int) (int, error) {
      return validateOut * 2, nil  // expensive check
    }, nil)).
  AddStep(NewStep("finalize").
    DependsOn("audit").
    Handler(func(ctx context.Context, in int, auditResult catbird.Optional[int]) (int, error) {
      if auditResult.IsSet {
        return auditResult.Value, nil  // used audit result
      }
      return in, nil  // audit was skipped
    }, nil))
// Flow input: 500 → audit skipped → finalize gets Optional[int]{IsSet: false}
// Flow input: 2000 → audit runs → finalize gets Optional[int]{IsSet: true, Value: 4000}
```

**Signals** enable human-in-the-loop workflows: steps can optionally wait for external input via `.Signal()` builder method before executing:
```go
NewFlow("workflow").
  AddStep(NewStep("step1").
    Handler(func(ctx context.Context, in string) (string, error) {
      return in + " processed by step 1", nil
    }, nil)).
  AddStep(NewStep("approve").
    DependsOn("step1").
    Signal(catbird.NewSignal[ApprovalInput](nil)). // Wait for signal
    Handler(func(ctx context.Context, in string, approval ApprovalInput, step1Out string) (string, error) {
      return step1Out + " approved by " + approval.ApproverID, nil
    }, nil))
// Signal delivery: client.SignalFlow(ctx, "workflow", flowRunID, "approve", ApprovalInput{...})
```

**Key Flow Patterns**:
- **Conditions work for both tasks and steps**: Use `.Condition("expression")` builder method. Tasks use `input.field` to reference input; steps use `step_name.field` to reference outputs; steps with signals can use `signal.field` to reference signal input.
- **Dependency tracking**: `dependency_count` includes all deps (required + optional); `remaining_dependencies` decrements for both completed and skipped steps
- **Optional outputs**: When a conditional step is skipped, dependent steps receive `Optional[T]{IsSet: false}`. When executed, `Optional[T]{IsSet: true, Value: result}`
- **Cascading resolution**: `cb_start_steps()` loops until no more steps unblock; handles chains like step2 skips → step3 unblocks → step4 unblocks
- **Validation**: Flow construction panics if a step depends on a conditional step without using `.OptionalDependency()` variant and `Optional[T]` parameter type
- **Builder methods**: All construction through chainable methods: `NewStep(name).DependsOn(...).Condition(...).Signal(...).Handler(fn, opts)`

## Key Conventions

- **Worker lifecycle**: `client.NewWorker(ctx, opts...)` → `worker.Start(ctx)` → `worker.Wait()` (graceful shutdown with timeout)
- **HandlerOpts validation**: Worker validates all task and flow step HandlerOpts at initialization time. Invalid configs (negative concurrency/batch size, invalid backoff, invalid circuit breaker) are caught immediately with descriptive errors before reaching database operations. This ensures type safety at construction time.
- **Options pattern**: HandlerOpts uses a public struct with public fields (Concurrency, BatchSize, Timeout, MaxRetries, MinDelay, MaxDelay, CircuitBreaker). WorkerOpts and other configs use closure functional options (WithScheduledTask, WithFlow, etc.).
- **Conn interface**: Abstracts pgx; accepts `*pgxpool.Pool`, `*pgx.Conn` or `pgx.Tx`
- **Logging**: Uses stdlib `log/slog`; workers accept custom logger via `WithLogger()`
- **Scheduled tasks/flows**: Use robfig/cron syntax; `WithScheduledTask("name", "@hourly")`
- **Automatic garbage collection**: All workers automatically run GC every 5 minutes (cleans up expired queues and stale worker heartbeats); no configuration needed
- **Deduplication strategies**: Two strategies available:
  - **ConcurrencyKey**: Prevents concurrent/overlapping runs (deduplicates `queued`/`started` status). After completion or failure, same key can be used again.
  - **IdempotencyKey**: Ensures exactly-once execution (deduplicates `queued`/`started`/`completed` status). After successful completion, same key permanently rejected.
  - **Return behavior**: When a duplicate is detected, `RunTask()`/`RunFlow()` return the **existing row's ID**, not 0 or an error. This allows callers to wait on the existing execution.
  - **Failure retries**: Both strategies allow retries when a task/flow fails (`status: failed`).
  - **Mutually exclusive**: Cannot specify both keys simultaneously (returns error).
- **Topic bindings**: Explicit via `Bind(queue, pattern)`; wildcards `?` (single token) and `*` (multi-token tail as `.*`). Foreign key CASCADE deletes bindings when queue is deleted. Pattern validation at bind time; regex precompiled in PostgreSQL.
- **Task/Flow execution**: `client.RunTask()` or `client.RunFlow()` return handles with `WaitForOutput()` to block until completion. When deduplication detects an existing run, the handle contains the existing run's ID.
- **Workflow signals**: Steps can require signals (external input) before executing. Use `.Signal()` builder method. Signal delivered via `client.SignalFlow(ctx, flowName, flowRunID, stepName, input)`. Steps with both dependencies and signals wait for **both** conditions before starting. Enables approval workflows, webhooks, and human-in-the-loop patterns.
- **Optional dependencies**: When a step depends on a conditional step (one with `.Condition()`), use dependent step parameter as `Optional[T]`. The `Optional[T]` type has `IsSet bool` and `Value T` fields. Flow construction validates this constraint and panics if violated. Enables reconvergence patterns where multiple branches merge back together.

## Developer Workflows

**Docker-Based Testing (Best Practice)**:

The test setup uses Docker Compose to provide a clean, isolated PostgreSQL instance that's independent of your local environment. This prevents interference from previous runs and ensures consistency across machines.

```bash
# Start the test environment (one-time or when needed)
docker compose up -d

# Run tests with automatic cleanup and fresh database
./scripts/test.sh

# Run specific tests
./scripts/test.sh -run TestQueueCreate

# Stop the test environment
docker compose down

# Full reset with volume deletion (nuclear option)
docker compose down -v && docker compose up -d
```

**How It Works**:
1. `docker compose up -d` starts PostgreSQL in a container
2. `./scripts/test.sh` automatically:
   - Connects to the container using hardcoded connection string
   - Drops and recreates `cb_tst` database (fresh state)
   - Runs migrations via `getTestClient()`
   - Executes tests
3. Docker volumes persist data between runs; use `docker compose down -v` to wipe

**Connection Details** (hardcoded, no env vars needed):
- Host: `localhost`
- Port: `5432`
- User: `postgres`
- Password: `postgres`
- Database: `cb_tst`
- URL: `postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable`

**Verification**:
```bash
# Check if PostgreSQL is running
docker compose ps

# Connect to database manually
psql -U postgres -h localhost -d cb_tst

# View logs
docker compose logs -f postgres
```

**Test Database Setup (sync.Once Pattern)**:
- The test harness uses `sync.Once` to initialize the database exactly once per test suite run
- In `catbird_test.go`, `testOnce.Do()` calls `getTestClient()` which:
  1. Opens connection to `cb_tst` database
  2. Runs `MigrateDownTo(0)` to clean state (may fail on first run)
  3. Runs `MigrateUpTo(SchemaVersion)` to apply all migrations
  4. Creates connection pool and test client
- **Key consequence**: Dynamic tables (e.g., `cb_f_myflow`, `cb_s_myflow`) and data persist across all tests in the suite
- **Data isolation impact**: If a test uses a hardcoded deduplication key (e.g., `IdempotencyKey: "order-123"`), subsequent test runs will retrieve the OLD flow run (with potentially outdated data formats)
- **Solutions**:
  - Use unique identifiers per test run: `fmt.Sprintf("key-%d", time.Now().UnixNano())`
  - Or reset specific tables/flows in test setup if needed
  - Or use `docker compose down -v && docker compose up -d` to wipe database between manual test iterations

**Troubleshooting**:
- "Connection refused" → Ensure `docker compose up -d` is running
- "Database doesn't exist" → Run `./scripts/test.sh` once to initialize
- "Schema mismatch / old data format" → Database was not fully dropped between runs; use `docker compose down -v` and re-run
- "Test retrieves wrong data" → Check deduplication keys; if hardcoded, old rows from previous runs may be returned
- "Need clean slate" → `docker compose down -v && docker compose up -d` (removes volume)

**Add migrations**: 
1. Create new `.sql` file in `migrations/`, use goose syntax (`+goose up`/`+goose down`)
2. Update `SchemaVersion` constant in `migrate.go` to match the new migration version number
3. Migrations are embedded via `//go:embed migrations/*.sql` and use `goose.WithDisableVersioning(true)`

**CRITICAL: Migration Versioning System**:
- Catbird uses goose with `DisableVersioning(true)` - there is **NO goose version table** in the database
- Goose tracks which migrations have run by executing them in order; state is NOT persisted
- This means:
  - Migrations run exactly once when first applied during test initialization
  - In test harness, `testOnce.Do()` in `catbird_test.go` ensures migrations run only on first test in suite
  - **OLD DATABASE STATE PERSISTS BETWEEN TEST RUNS** - If you change a migration, old data in dynamic tables (e.g., `cb_f_myflow`, `cb_s_myflow`) remains until explicitly dropped
  - Problem: Test may retrieve old rows from previous runs with outdated schemas/formats
  - Solution: Either (a) Use unique test identifiers to avoid hitting old data, or (b) Drop tables explicitly in `-- +goose down` sections

**Goose SQL Syntax Rules**:
- Each migration file must start with `-- +goose up` and end with `-- +goose down`
- **CRITICAL UP/DOWN Structure**: Goose selectively executes sections:
  - When applying migrations (rolling forward): only `-- +goose up` section is executed
  - When rolling back: only `-- +goose down` section is executed
  - When using `psql -f` to manually run a migration file, **BOTH sections execute sequentially**! This is a debugging antipattern.
  - **Always use the migration API** (`MigrateUpTo`, `MigrateDownTo`) for testing, never manual `psql` execution
- **The `-- +goose down` section MUST clean up what the `up` section created**, including:
  - All tables created in `up` (use `DROP TABLE IF EXISTS` statements)
  - All custom types created in `up` (use `DROP TYPE IF EXISTS` statements)
  - All functions created in `up` (DROP FUNCTION statements are usually in `down`)
  - Without proper cleanup, old tables persist when goose can't roll back partially-applied migrations
- Use `-- +goose statementbegin` / `-- +goose statementend` to wrap multi-line SQL statements (especially PL/pgSQL functions)
- **For PL/pgSQL functions with `LANGUAGE ... AS $$` syntax**:
  - DO NOT use `$$ LANGUAGE plpgsql;` at the end (creates duplicate LANGUAGE clause)
  - Use `$$;` to terminate (language already specified in CREATE statement)
  - Correct example:
  ```sql
  -- +goose statementbegin
  CREATE OR REPLACE FUNCTION my_func() RETURNS void
  LANGUAGE plpgsql AS $$
  BEGIN
    -- function body
  END;
  $$;  -- <- Just $$; (no LANGUAGE here, already in CREATE)
  -- +goose statementend
  ```
  - Without statementbegin/statementend, goose treats each line separately, causing syntax errors on multi-line statements

## Critical Files

- [catbird.go](../catbird.go): Message, Task, Flow, Step, Options definitions
- [optional.go](../optional.go): Optional[T] generic type for conditional dependency outputs
- [flow.go](../flow.go): Flow DSL, step constructors, dependency validation
- [worker.go](../worker.go): Worker struct, task/flow execution, polling logic
- [scheduler.go](../scheduler.go): Cron-based scheduling for tasks and flows
- [client.go](../client.go): Public API (delegation layer)
- [dashboard/handler.go](../dashboard/handler.go): HTTP routes & templating
- [migrations/](../migrations/): Database schema (versioned)

## Common Patterns to Replicate

1. **Errors**: Use `ErrTaskFailed`, `ErrFlowFailed` package-level errors
2. **Context propagation**: All DB ops accept `context.Context` first param
3. **JSON payloads**: Custom types → JSON via generics; validation happens in handler
4. **Retries**: Built-in with configurable exponential backoff with full jitter (see `WithBackoff(min, max)`)
5. **Circuit breaker**: Optional per-handler protection for external dependencies (see `WithCircuitBreaker(failures, openTimeout)`)
6. **Concurrency**: Default 1 per handler; tweak with `WithConcurrency(n)`
7. **Conditional execution**: Use `WithCondition("expression")` as a task/step option for both tasks and flow steps. Tasks use `input.field` syntax (e.g., `WithCondition("input.is_premium")`), flow steps use `step_name.field` syntax (e.g., `WithCondition("validate.score gte 50")`)
8. **Optional dependencies**: Use `Optional[T]` + `OptionalDependency()` pair when depending on conditional steps. Validation at flow construction time enforces type safety.
9. **SQL parameter/column conflicts**: Use `#variable_conflict use_column` directive in PL/pgSQL when parameter names match column names (prevents "column ambiguous" errors)
10. **Atomic deduplication with UNION ALL**: For `RunTask()` and `RunFlow()` deduplication (concurrency_key / idempotency_key), use the atomic ON CONFLICT DO UPDATE pattern with UNION ALL fallback. **DO NOT remove the UNION ALL or simplify to plain `RETURNING id`**. The pattern is:
```sql
WITH ins AS (
    INSERT INTO table_name (key_col, data_col)
    VALUES ($1, $2)
    ON CONFLICT (key_col) WHERE condition
    DO UPDATE SET col = EXCLUDED.col WHERE FALSE
    RETURNING id
)
SELECT id FROM ins
UNION ALL
SELECT id FROM table_name
WHERE key_col = $1 AND condition
LIMIT 1
```
**Why this pattern is essential**:
- `WHERE FALSE` prevents the update from executing (no state mutation on conflict), but still locks the row atomically
- The UNION ALL fallback returns the conflicting row's ID if INSERT fails, handling the conflict case
- Together they guarantee exactly one row ID is returned atomically—no race window between INSERT and SELECT
- Simplifying to bare `DO UPDATE ... WHERE FALSE RETURNING id` causes NULL returns on conflict (RETURNING doesn't fire in DO UPDATE branch)
- Plain `DO UPDATE SET col = col RETURNING id` has ambiguous column references under `#variable_conflict use_column` directive
**Used in**: `cb_run_task()`, `cb_run_flow()`, `cb_send()` for both concurrency_key and idempotency_key variants
**Reference**: https://stackoverflow.com/a/35953488
