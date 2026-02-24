[![Go Reference](https://pkg.go.dev/badge/github.com/ugent-library/catbird.svg)](https://pkg.go.dev/github.com/ugent-library/catbird)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.25+-blue.svg)](https://golang.org)
[![Go Report Card](https://goreportcard.com/badge/github.com/ugent-library/catbird)](https://goreportcard.com/report/github.com/ugent-library/catbird)

![CatBird](catbird-banner.svg "CatBird banner")

# Catbird

A PostgreSQL-powered message queue and task execution engine. Catbird brings reliability and simplicity to background job processing by using your database as the single source of truth—no extra services to manage, just your database coordinating everything.

## Why Catbird?

- **Transactional by default**: enqueue messages in the same DB transaction as your app writes; rollback means no message.
- **Exactly-once within a visibility window**: safe retries after crashes, no duplicate processing.
- **Database as coordinator**: horizontal workers, PostgreSQL handles distribution and state.
- **Workflows as DAGs**: dependencies, branching, and data passing between steps.
- **Persistence and auditability**: queues, runs, and results live in PostgreSQL.
- **Resiliency baked in**: retries, backoff, optional circuit breakers.
- **Operational UX**: web dashboard for runs, queues, and workers.

<p align="center">
  <img src="screenshots/dashboard-flows.png" alt="Flow Visualization" width="800" />
</p>

## Quick Start
```go
client := catbird.New(conn)
ctx := context.Background()

// Queue: create, send, read, delete
err := client.CreateQueue(ctx, "my-queue", nil)
err = client.Send(ctx, "my-queue", map[string]any{"user_id": 123}, &catbird.SendOpts{
    IdempotencyKey: "user-123",
})
messages, err := client.Read(ctx, "my-queue", 10, 30*time.Second)
for _, msg := range messages {
    err = client.Delete(ctx, "my-queue", msg.ID)
}

// Task + flow definitions
task := catbird.NewTask("send-email").
    Handler(func(ctx context.Context, input string) (string, error) {
        return "sent", nil
    }, nil)

flow := catbird.NewFlow("double-add").
    AddStep(catbird.NewStep("double").
        Handler(func(ctx context.Context, input int) (int, error) {
            return input * 2, nil
        }, nil)).
    AddStep(catbird.NewStep("add").
        DependsOn("double").
        Handler(func(ctx context.Context, input int, doubled int) (int, error) {
            return doubled + 1, nil
        }, nil))

worker, err := client.NewWorker(ctx, nil)
worker.AddTask(task)
worker.AddFlow(flow)
go worker.Start(ctx)

taskHandle, err := client.RunTask(ctx, "send-email", "hello", nil)
var taskOut string
err = taskHandle.WaitForOutput(ctx, &taskOut)

flowHandle, err := client.RunFlow(ctx, "double-add", 10, nil)
var flowOut int
err = flowHandle.WaitForOutput(ctx, &flowOut)

// Ensure definitions exist before usage; this is not necessary if you
// just want to run a worker, definitions will be created for you on
// Start
err := client.CreateTask(ctx, taskA, taskB)
err := client.CreateFlow(ctx, flowA, flowB)

// Direct package-level usage (no Client), for example in a transaction:
taskHandle, err := catbird.RunTask(ctx, tx, "send-email", "hello", nil)
```

# Deduplication Strategies

Catbird supports two deduplication strategies for tasks and flows.

## ConcurrencyKey (Temporary)

Prevents overlapping runs; allows re-runs after completion or failure.

## IdempotencyKey (Permanent)

Ensures exactly-once execution; blocks reuse after completion.

```go
// ConcurrencyKey: prevent overlap
_, err := client.RunTask(ctx, "process-user", userID, &catbird.RunOpts{
    ConcurrencyKey: fmt.Sprintf("user-%d", userID),
})

// IdempotencyKey: exactly once
_, err = client.RunTask(ctx, "charge-payment", payment, &catbird.RunOpts{
    IdempotencyKey: fmt.Sprintf("payment-%s", payment.ID),
})
```

### Comparison Table

| Feature | ConcurrencyKey | IdempotencyKey |
|---------|----------------|----------------|
| **Purpose** | Prevent overlapping runs | Ensure exactly-once execution |
| **Deduplicates** | `queued`, `started` | `queued`, `started`, `completed` |
| **After completion** | Allows re-run | Rejects duplicate |
| **After failure** | Allows retry | Allows retry |
| **Use for** | Rate limiting, resource locking, scheduled tasks | Payments, orders, webhooks, audit logs |

## Important Notes

- **Mutually exclusive**: You cannot provide both `ConcurrencyKey` and `IdempotencyKey` for the same run (returns error)
- **Return value on duplicate**: `RunTask()`/`RunFlow()` return a handle to the existing run ID
- **Failure retries**: Both strategies allow retries on failed runs
- **No key = no deduplication**: If you don't provide either key, duplicates are allowed
- **Queue messages**: Use `IdempotencyKey` in `SendOpts` for exactly-once message delivery

# Topic-Based Routing

```go
err := client.CreateQueue(ctx, "user-events", nil)
err = client.CreateQueue(ctx, "audit-log", nil)

err = client.Bind(ctx, "user-events", "events.user.created")
err = client.Bind(ctx, "user-events", "events.?.updated")
err = client.Bind(ctx, "audit-log", "events.*")

err = client.Publish(ctx, "events.user.created", map[string]any{
    "user_id": 123,
    "email":   "user@example.com",
}, nil)
err = client.Unbind(ctx, "user-events", "events.?.updated")
```

Wildcard rules:
- `?` matches a single token (e.g., `events.?.created` matches `events.user.created`)
- `*` matches one or more tokens at the end (e.g., `events.user.*` matches `events.user.created.v1`)
- `*` must appear as `.*` at the end of the pattern
- Tokens are separated by `.` and can contain `a-z`, `A-Z`, `0-9`, `_`, `-`

# Task Execution

```go
// Define task (scheduling is separate)
task := catbird.NewTask("send-email").
    Handler(func(ctx context.Context, input EmailRequest) (EmailResponse, error) {
        return EmailResponse{SentAt: time.Now()}, nil
    }, &catbird.HandlerOpts{
        Concurrency: 5,
        MaxRetries:  3,
        Backoff:     catbird.NewFullJitterBackoff(500*time.Millisecond, 10*time.Second),
        CircuitBreaker: catbird.NewCircuitBreaker(5, 30*time.Second),
    })

// Create a schedule for the task (optional; can run manually via RunTask)
client.CreateTaskSchedule(ctx, "send-email", "@hourly", nil)

// Or with static input
client.CreateTaskSchedule(ctx, "send-report", "@hourly", &catbird.ScheduleOpts{
    Input: EmailRequest{To: "ops@example.com", Subject: "Hourly report"},
})

// Define a task with a condition (skipped when condition is false)
conditionalTask := catbird.NewTask("premium-processing").
    Condition("input.is_premium"). // Skipped if is_premium = false
    Handler(func(ctx context.Context, input ProcessRequest) (string, error) {
        return "processed", nil
    }, nil)

// Create worker
worker, err := client.NewWorker(ctx)
// Add tasks
worker.AddTask(task)
worker.AddTask(conditionalTask)
go worker.Start(ctx)

// Run the task
handle, err := client.RunTask(ctx, "send-email", EmailRequest{
    To:      "user@example.com",
    Subject: "Hello",
}, nil)

// Get result
var result EmailResponse
err = handle.WaitForOutput(ctx, &result)
```

# Flow Execution

A **flow** is a **directed acyclic graph (DAG)** of steps that execute when their dependencies are satisfied.

## Summary

- Steps with no dependencies start immediately; independent branches run in parallel.
- A flow has exactly one final step; the output of the final step is the output of the flow.
- Conditions can skip steps; downstream handlers must accept `Optional[T]` for any conditional dependency.
- A step with a signal waits for both its dependencies and the signal input.
- `WaitForOutput()` returns the final step output once the flow completes.

## Examples: Workflows

```go
flow := catbird.NewFlow("order-processing").
    Schedule("0 2 * * *", nil). // Daily at 2 AM
    AddStep(catbird.NewStep("validate").
        Handler(func(ctx context.Context, order Order) (ValidationResult, error) {
            if order.Amount <= 0 {
                return ValidationResult{Valid: false, Reason: "Invalid amount"}, nil
            }
            return ValidationResult{Valid: true}, nil
        }, nil)).
    AddStep(catbird.NewStep("charge").
        DependsOn("validate").
        Handler(func(ctx context.Context, order Order, validated ValidationResult) (ChargeResult, error) {
            if !validated.Valid {
                return ChargeResult{}, fmt.Errorf("cannot charge invalid order")
            }
            return ChargeResult{
                TransactionID: "txn-" + order.ID,
                Amount:        order.Amount,
            }, nil
        }, nil)).
    AddStep(catbird.NewStep("check-inventory").
        DependsOn("validate").
        Handler(func(ctx context.Context, order Order, validated ValidationResult) (InventoryCheck, error) {
            return InventoryCheck{
                InStock: true,
                Qty:     order.Amount,
            }, nil
        }, nil)).
    AddStep(catbird.NewStep("ship").
        DependsOn("charge", "check-inventory").
        Handler(func(ctx context.Context, order Order, chargeResult ChargeResult, inventory InventoryCheck) (ShipmentResult, error) {
            if !inventory.InStock {
                return ShipmentResult{}, fmt.Errorf("out of stock")
            }
            return ShipmentResult{
                TrackingNumber: "TRK-" + chargeResult.TransactionID,
                EstimatedDays:  3,
            }, nil
        }, nil))

// Create worker
worker, err := client.NewWorker(ctx)
// Add flow
worker.AddFlow(flow)
go worker.Start(ctx)
```

## Example: Signals & Human-in-the-Loop

Signals enable workflows that wait for external input before proceeding, such as approval workflows or webhooks.

```go
flow := catbird.NewFlow("document_approval").
    AddStep(catbird.NewStep("submit").
        Handler(func(ctx context.Context, doc Document) (string, error) {
            return doc.ID, nil
        }, nil)).
    AddStep(catbird.NewStep("approve").
        DependsOn("submit").
        Signal(true).
        Handler(func(ctx context.Context, doc Document, approval ApprovalInput, docID string) (ApprovalResult, error) {
            if !approval.Approved {
                return ApprovalResult{}, fmt.Errorf("approval denied by %s: %s", approval.ApproverID, approval.Notes)
            }
            return ApprovalResult{
                Status:     "approved",
                ApprovedBy: approval.ApproverID,
                Timestamp:  time.Now().Format(time.RFC3339),
            }, nil
        }, nil)).
    AddStep(catbird.NewStep("publish").
        DependsOn("approve").
        Handler(func(ctx context.Context, doc Document, approval ApprovalResult) (PublishResult, error) {
            return PublishResult{
                PublishedAt: time.Now().Format(time.RFC3339),
                URL:         "https://example.com/docs/" + approval.ApprovedBy,
            }, nil
        }, nil))
```

A step with both dependencies and a signal waits for **both** conditions: all dependencies must complete **and** the signal must be delivered before the step executes.

# Conditional Execution

Both tasks and flow steps support conditional execution via `Condition` on the builder methods. If the condition evaluates to false (or a referenced field is missing), the task/step is marked `skipped` and its handler does not run.

## Rules at a Glance

- **Prefixes**: tasks use `input.*`; flow steps use `input.*`, `step_name.*`, or `signal.*`.
- **Operators**: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `exists`, `contains`, plus `not <expr>`.
- **Optional outputs**: if a step can be skipped, downstream handlers must accept `Optional[T]` for that dependency.
- **No AND/OR**: only one expression per task/step; compute a derived field upstream if needed.

## Tasks with Conditions

Tasks can use conditions to skip execution based on input fields.

```go
type ProcessRequest struct {
    UserID     int    `json:"user_id"`
    IsPremium  bool   `json:"is_premium"`
    Amount     int    `json:"amount"`
    Environment string `json:"environment"`
}

// Only process premium users
premiumTask := catbird.NewTask("premium_processing").
    Condition("input.is_premium"). // Skipped if is_premium = false
    Handler(func(ctx context.Context, req ProcessRequest) (string, error) {
        return fmt.Sprintf("Processed premium user %d", req.UserID), nil
    }, nil)

// Run task - may be skipped based on input
client.RunTask(ctx, "premium_processing", ProcessRequest{UserID: 123, IsPremium: false}, nil)
// This task run will be skipped (is_premium = false)
```

## Flows with Conditions

Flow steps can branch based on prior outputs. Use `Optional[T]` to handle skipped dependencies.

```go
flow := catbird.NewFlow("payment_processing").
    AddStep(catbird.NewStep("validate").
        Handler(func(ctx context.Context, order Order) (ValidationResult, error) {
            return ValidationResult{Valid: order.Amount > 0}, nil
        }, nil)).
    AddStep(catbird.NewStep("charge").
        DependsOn("validate").
        Condition("validate.valid").
        Handler(func(ctx context.Context, order Order, validation ValidationResult) (ChargeResult, error) {
            return ChargeResult{TransactionID: "txn-123"}, nil
        }, nil)).
    AddStep(catbird.NewStep("finalize").
        DependsOn("charge").
        Handler(func(ctx context.Context, order Order, charge catbird.Optional[ChargeResult]) (FinalResult, error) {
            if charge.IsSet {
                return FinalResult{Status: "charged", TxnID: charge.Value.TransactionID}, nil
            }
            return FinalResult{Status: "free_order", TxnID: ""}, nil
        }, nil))
```

# Resiliency

Catbird includes multiple resiliency layers for runtime failures. Handler-level retries are configured with `HandlerOpts` (`MaxRetries`, `Backoff`), and external calls can be protected with `HandlerOpts.CircuitBreaker` (typically created via `NewCircuitBreaker(...)`) to avoid cascading outages. In worker database paths, PostgreSQL reads/writes are retried with bounded attempts and full-jitter backoff; retries stop immediately on context cancellation or deadline expiry.

## Be aware of side effects

Catbird deduplication (`ConcurrencyKey`/`IdempotencyKey`) controls duplicate run creation, while handler retries can still re-attempt the same run after transient failures. For non-repeatable side effects (payments, email, webhooks), use idempotent write patterns or upstream idempotency keys so retry attempts remain safe.

# Naming Rules

- **Queue, task, flow, and step names**: Lowercase letters, digits, and underscores only (`a-z`, `0-9`, `_`). Max 58 characters. Step names must be unique within a flow. Reserved step names: `input`, `signal`.
- **Topics/Patterns**: Letters (upper/lower), digits, dots, underscores, and hyphens (`a-z`, `A-Z`, `0-9`, `.`, `_`, `-`, plus wildcards `?`, `*`).

# Query Helpers

Use query builders when you want SQL + args directly (for `pgx.Batch` or custom execution):

- `SendQuery(queue, payload, opts)`
- `PublishQuery(topic, payload, opts)`
- `RunTaskQuery(name, input, opts)`
- `RunFlowQuery(name, input, opts)`

```go
// Queue into a batch
var batch pgx.Batch
q1, args1, err := catbird.SendQuery("my-queue", map[string]any{"user_id": 123}, nil)
if err != nil {
    return err
}
batch.Queue(q1, args1...)
```

# PostgreSQL API Reference

Catbird is built on PostgreSQL functions, so you can use the API directly from any language or tool with PostgreSQL support (psql, Python, Node.js, Ruby, etc.).

## Queues

```sql
-- Create a queue
SELECT cb_create_queue(name => 'my_queue', expires_at => null, unlogged => false);

-- Send a message
SELECT cb_send(queue => 'my_queue', payload => '{"user_id": 123, "action": "process"}'::jsonb, 
               topic => null, idempotency_key => null, deliver_at => null);

-- Publish to topic-bound queues
SELECT cb_publish(topic => 'events.user.created', payload => '{"user_id": 456}'::jsonb,
                   idempotency_key => 'user-456-created', deliver_at => null);

-- Read messages (with 30 second visibility timeout)
SELECT * FROM cb_read(queue => 'my_queue', limit => 10, hide_for => 30);

-- Delete a message
SELECT cb_delete(queue => 'my_queue', id => 1);

-- Bind queue to topic pattern
SELECT cb_bind(queue_name => 'user_events', pattern => 'events.user.*');
SELECT cb_unbind(queue_name => 'user_events', pattern => 'events.user.*');
```

## Tasks

```sql
-- Create a task definition
SELECT cb_create_task(name => 'send_email');

-- Run a task
SELECT * FROM cb_run_task(name => 'send_email', input => '{"to": "user@example.com"}'::jsonb, 
                          concurrency_key => null, idempotency_key => null);
```

## Workflows

```sql
-- Create a flow definition
SELECT cb_create_flow(name => 'order_processing', steps => '[
  {"name": "validate"},
  {"name": "charge", "depends_on": [{"name": "validate"}]},
  {"name": "ship", "depends_on": [{"name": "charge"}]}
]'::jsonb);

-- Run a flow
SELECT * FROM cb_run_flow(name => 'order_processing', input => '{"order_id": 123}'::jsonb,
                          concurrency_key => null, idempotency_key => null);
```

## Monitoring Task and Flow Runs

You can query task and flow run information directly:

```sql
-- List recent task runs (replace send_email with your task name)
SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at
FROM cb_t_send_email
ORDER BY started_at DESC
LIMIT 20;

-- Get flow run (replace order_processing with your flow name)
SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at
FROM cb_f_order_processing
WHERE id = $1;
```

# Dashboard

The dashboard provides a web UI for monitoring queues, tasks, flows, and workers. You can run it standalone with the `cb` CLI or embed it as an `http.Handler`.

```bash
go install github.com/ugent-library/catbird/cmd/cb@latest
export CB_CONN="postgres://user:pass@localhost:5432/mydb?sslmode=disable"
cb dashboard
```

The dashboard is a standard `http.Handler` and can be embedded in any Go web application:

```go
import (
    "log/slog"
    "net/http"
    
    "github.com/ugent-library/catbird"
    "github.com/ugent-library/catbird/dashboard"
)

func main() {
    client := catbird.New(conn)
    dash := dashboard.New(dashboard.Config{
        Client:     client,
        Log:        slog.Default(), // Optional: provide custom logger
        PathPrefix: "",              // Optional: mount at a subpath (e.g., "/admin")
    })
    http.ListenAndServe(":8080", dash.Handler())
}
```

# Documentation

- **[Go API Documentation](https://pkg.go.dev/github.com/ugent-library/catbird)**: Complete reference for all public types and functions
- **[Copilot Instructions](/catbird-instructions.md)**: Architecture overview and developer guidance

# Acknowledgments

SQL code is taken from or inspired by the excellent [pgmq](https://github.com/pgmq) and [pgflow](https://github.com/pgflow-dev/pgflow) projects.
