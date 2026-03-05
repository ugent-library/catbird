[![Go Reference](https://pkg.go.dev/badge/github.com/ugent-library/catbird.svg)](https://pkg.go.dev/github.com/ugent-library/catbird)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.25+-blue.svg)](https://golang.org)
[![Go Report Card](https://goreportcard.com/badge/github.com/ugent-library/catbird)](https://goreportcard.com/report/github.com/ugent-library/catbird)

![CatBird](assets/banner.svg "CatBird banner")

# Catbird

A PostgreSQL-powered message queue and task execution engine. Catbird brings reliability and simplicity to background job processing by using your database as the single source of truth—no extra services to manage, just your database coordinating everything.

## Why Catbird?

- **Transactional by default**: enqueue messages in the same DB transaction as your app writes; rollback means no message.
- **Exactly-once within a visibility window**: safe retries after crashes, no duplicate processing.
- **Database as coordinator**: horizontal workers, PostgreSQL handles distribution and state.
- **Workflows as DAGs**: dependencies, branching, and data passing between steps.
- **Persistence and auditability**: queues, runs, and results live in PostgreSQL.
- **Resiliency baked in**: retries, backoff, optional circuit breakers.
- **Operational UX**: web dashboard and tui for runs, queues, and workers.

<p align="center">
  <img src="assets/screenshots/dashboard-flows.png" alt="Flow Visualization" width="800" />
</p>

## Quick Start
```go
client := catbird.New(conn)
ctx := context.Background()

// Queues
err := client.CreateQueue(ctx, "my-queue")
err = client.Send(ctx, "my-queue", map[string]any{"user_id": 123}, catbird.SendOpts{
    IdempotencyKey: "user-123",
})
messages, err := client.Read(ctx, "my-queue", 10, 30*time.Second)
for _, msg := range messages {
    err = client.Delete(ctx, "my-queue", msg.ID)
}

// Delayed send
client.Send(ctx, "my_queue", map[string]any{"job": "cleanup"}, catbird.SendOpts{VisibleAt: time.Now().Add(30 * time.Minute)})

// Tasks and flows
task := catbird.NewTask("send-email").
    WithDescription("Send a transactional email to a user").Do(func(ctx context.Context, input string) (string, error) {
        return "sent", nil
    })

flow := catbird.NewFlow("double-add")
flow.AddStep(catbird.NewStep("double").Do(func(ctx context.Context, input int) (int, error) {
    return input * 2, nil
}))
flow.AddStep(catbird.NewStep("add").
    DependsOn("double").Do(func(ctx context.Context, input int, doubled int) (int, error) {
    return doubled + 1, nil
}))

worker := client.NewWorker().
    AddTask(task).
    AddFlow(flow)
go worker.Start(ctx)

taskHandle, err := client.RunTask(ctx, "send-email", "hello")
var taskOut string
err = taskHandle.WaitForOutput(ctx, &taskOut)

flowHandle, err := client.RunFlow(ctx, "double-add", 10)
var flowOut int
err = flowHandle.WaitForOutput(ctx, &flowOut)

// Delayed execution
client.RunTask(ctx, "process-user", userID, catbird.RunTaskOpts{VisibleAt: time.Now().Add(5 * time.Minute)})
client.RunFlow(ctx, "order_processing", map[string]any{"order_id": 123}, catbird.RunFlowOpts{VisibleAt: time.Now().Add(30 * time.Second)})

// Ensure definitions exist before usage; this is not necessary if you
// just want to run a worker, definitions will be created for you on
// Start
err := client.CreateTask(ctx, taskA)
err = client.CreateTask(ctx, taskB)
err = client.CreateFlow(ctx, flowA)
err = client.CreateFlow(ctx, flowB)

// Direct package-level usage (no Client), for example in a transaction:
taskHandle, err := catbird.RunTask(ctx, tx, "send-email", "hello")
```

# Controlling Migrations

If your application already uses Goose migrations, you can register Catbird's schema migration as a normal app migration:

Create a migration file such as `00003_add_catbird.go`:

```go
package migrations

import (
    "context"
    "database/sql"

    "github.com/pressly/goose/v3"
    "github.com/ugent-library/catbird"
)

func init() {
    goose.AddMigrationNoTxContext(addCatbirdUp, addCatbirdDown)
}

func addCatbirdUp(ctx context.Context, db *sql.DB) error {
    return catbird.MigrateUpTo(ctx, db, 13)
}

func addCatbirdDown(ctx context.Context, db *sql.DB) error {
    return catbird.MigrateDownTo(ctx, db, 0)
}
```

Keep an explicit pinned version (for example `13`) in this migration file. Do not use `catbird.SchemaVersion`.

# Deduplication Strategies

Catbird supports two deduplication strategies for tasks and flows.

## ConcurrencyKey (Temporary)

Prevents overlapping runs; allows re-runs after completion or failure.

## IdempotencyKey (Permanent)

Ensures exactly-once execution; blocks reuse after completion.

```go
// ConcurrencyKey: prevent overlap
_, err := client.RunTask(ctx, "process-user", userID, catbird.RunTaskOpts{
    ConcurrencyKey: fmt.Sprintf("user-%d", userID),
})

// IdempotencyKey: exactly once
_, err = client.RunTask(ctx, "charge-payment", payment, catbird.RunTaskOpts{
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
err := client.CreateQueue(ctx, "user-events")
err = client.CreateQueue(ctx, "audit-log")

err = client.Bind(ctx, "user-events", "events.user.created")
err = client.Bind(ctx, "user-events", "events.*.updated")
err = client.Bind(ctx, "audit-log", "events.#")

_, err = client.Publish(ctx, "events.user.created", map[string]any{
    "user_id": 123,
    "email":   "user@example.com",
})
_, err = client.Unbind(ctx, "user-events", "events.*.updated")
```

Wildcard rules:
- `*` matches a single token (e.g., `events.*.created` matches `events.user.created`)
- `#` matches zero or more tokens at the end (e.g., `events.user.#` matches `events.user` and `events.user.created.v1`)
- `#` must appear as `.#` at the end of the pattern, or as `#` by itself
- Tokens are separated by `.` and can contain `a-z`, `A-Z`, `0-9`, `_`, `-`

# Task Execution

```go
// Define task (scheduling is separate)
task := catbird.NewTask("send-email").Do(func(ctx context.Context, input EmailRequest) (EmailResponse, error) {
        return EmailResponse{SentAt: time.Now()}, nil
    },
        catbird.WithConcurrency(5),
        catbird.WithMaxRetries(3),
        catbird.WithFullJitterBackoff(500*time.Millisecond, 10*time.Second),
        catbird.WithCircuitBreaker(5, 30*time.Second),
    )

// Create a schedule for the task (optional; can run manually via RunTask)
client.CreateTaskSchedule(ctx, "send-email", "@hourly")

// Or with static input
client.CreateTaskSchedule(
    ctx,
    "send-report",
    "@hourly",
    catbird.WithInput(EmailRequest{To: "ops@example.com", Subject: "Hourly report"}),
)

// Define a task with a condition (skipped when condition is false)
conditionalTask := catbird.NewTask("premium-processing").
    WithCondition("input.is_premium"). // Skipped if is_premium = false
    Do(func(ctx context.Context, input ProcessRequest) (string, error) {
        return "processed", nil
    })

// Create worker
worker := client.NewWorker().
    WithLogger(slog.Default()).
    WithShutdownTimeout(10 * time.Second)
// Add tasks
worker.AddTask(task)
worker.AddTask(conditionalTask)
go worker.Start(ctx)

// Run the task
handle, err := client.RunTask(ctx, "send-email", EmailRequest{
    To:      "user@example.com",
    Subject: "Hello",
})

// Get result
var result EmailResponse
err = handle.WaitForOutput(ctx, &result)
```

## OnFail Handlers

On-fail handlers run after a task reaches a failed state (after its own retries).
They execute with their own `HandlerOpt` retry and backoff settings, and receive the
original input plus rich failure context.

`OnFail` semantics (tasks and flows):
- `OnFail` runs only after the main task/flow run reaches `failed` (after normal handler retries are exhausted).
- `OnFail` has independent retry/backoff via its own `HandlerOpt` values.
- A successful `OnFail` marks on-fail handling complete, but the original run remains `failed`.
- If `OnFail` retries are exhausted, on-fail handling remains failed and no further retries are scheduled.

```go
task := catbird.NewTask("charge-payment").Do(func(ctx context.Context, input ChargeRequest) (ChargeResult, error) {
        return ChargeResult{}, fmt.Errorf("gateway timeout")
    }).
    OnFail(func(ctx context.Context, input ChargeRequest, failure catbird.TaskFailure) error {
        // Send alert, enqueue compensation, or record audit log.
        return nil
    },
        catbird.WithMaxRetries(3),
        catbird.WithFullJitterBackoff(200*time.Millisecond, 5*time.Second),
    )
```

# Flow Execution

A **flow** is a **directed acyclic graph (DAG)** of steps that execute when their dependencies are satisfied.

## Summary

- Steps with no dependencies start immediately; independent branches run in parallel.
- Flow output is selected by output priority (configured with `OutputPriority(...)` or inferred from terminal steps).
- Conditions can skip steps; downstream handlers must accept `Optional[T]` for any conditional dependency.
- A step with a signal waits for both its dependencies and the signal input.
- `WaitForOutput()` returns the selected flow output once the flow completes.

## Examples: Workflows

```go
flow := catbird.NewFlow("order-processing")
flow.AddStep(catbird.NewStep("validate").Do(func(ctx context.Context, order Order) (ValidationResult, error) {
    if order.Amount <= 0 {
        return ValidationResult{Valid: false, Reason: "Invalid amount"}, nil
    }
    return ValidationResult{Valid: true}, nil
}))
flow.AddStep(catbird.NewStep("charge").
    DependsOn("validate").Do(func(ctx context.Context, order Order, validated ValidationResult) (ChargeResult, error) {
    if !validated.Valid {
        return ChargeResult{}, fmt.Errorf("cannot charge invalid order")
    }
    return ChargeResult{
        TransactionID: "txn-" + order.ID,
        Amount:        order.Amount,
    }, nil
}))
flow.AddStep(catbird.NewStep("check-inventory").
    DependsOn("validate").Do(func(ctx context.Context, order Order, validated ValidationResult) (InventoryCheck, error) {
    return InventoryCheck{
        InStock: true,
        Qty:     order.Amount,
    }, nil
}))
flow.AddStep(catbird.NewStep("ship").
    DependsOn("charge", "check-inventory").Do(func(ctx context.Context, order Order, chargeResult ChargeResult, inventory InventoryCheck) (ShipmentResult, error) {
    if !inventory.InStock {
        return ShipmentResult{}, fmt.Errorf("out of stock")
    }
    return ShipmentResult{
        TrackingNumber: "TRK-" + chargeResult.TransactionID,
        EstimatedDays:  3,
    }, nil
}))

    // Create a schedule for the flow (optional; can run manually via RunFlow)
    client.CreateFlowSchedule(ctx, "order-processing", "0 2 * * *") // Daily at 2 AM

// Create worker
worker := client.NewWorker()
// Add flow
worker.AddFlow(flow)
go worker.Start(ctx)
```

## Example: Multi-branch Output Ownership

Flows can have multiple terminal steps.

```go
flow := catbird.NewFlow("approval-or-escalation")
flow.OutputPriority("approve", "escalate")

flow.AddStep(catbird.NewStep("validate").Do(func(ctx context.Context, req Request) (Validation, error) {
    return Validation{Score: req.Score}, nil
}))
flow.AddStep(catbird.NewStep("approve").
    DependsOn("validate").
    WithCondition("validate.score gte 80").Do(func(ctx context.Context, req Request, v Validation) (Decision, error) {
    return Decision{Status: "approved"}, nil
}))
flow.AddStep(catbird.NewStep("escalate").
    DependsOn("validate").
    WithCondition("validate.score lt 80").Do(func(ctx context.Context, req Request, v Validation) (Decision, error) {
    return Decision{Status: "escalated"}, nil
}))
```

If you omit `OutputPriority(...)`, Catbird uses terminal steps in definition order as the default priority.

```go
flow := catbird.NewFlow("default-terminal-priority")
flow.AddStep(catbird.NewStep("a").Do(func(ctx context.Context, in int) (int, error) { return in, nil }))
flow.AddStep(catbird.NewStep("left").DependsOn("a").Do(func(ctx context.Context, in int, a int) (int, error) { return a + 1, nil }))
flow.AddStep(catbird.NewStep("right").DependsOn("a").Do(func(ctx context.Context, in int, a int) (int, error) { return a + 2, nil }))

// Effective priority: left, then right.
```

## OnFail Handlers

On-fail handlers run after a flow reaches a failed state (after its own retries).
They execute with their own `HandlerOpt` retry and backoff settings, and receive the
original input plus rich failure context.

```go
flow := catbird.NewFlow("order-processing")
flow.AddStep(catbird.NewStep("charge").Do(func(ctx context.Context, order Order) (string, error) {
    return "", fmt.Errorf("charge failed")
}))
flow.OnFail(func(ctx context.Context, order Order, failure catbird.FlowFailure) error {
    var failedInput Order
    if err := failure.FailedStepInputAs(&failedInput); err == nil {
        // failedInput has the step input that caused the error
    }

    var chargeResult ChargeResult
    if err := failure.OutputAs("charge", &chargeResult); err == nil {
        // access completed step output when available
    }

    return nil
})
```

## Example: Signals & Human-in-the-Loop

Signals enable workflows that wait for external input before proceeding, such as approval workflows or webhooks.

```go
flow := catbird.NewFlow("document_approval")
flow.AddStep(catbird.NewStep("submit").Do(func(ctx context.Context, doc Document) (string, error) {
    return doc.ID, nil
}))
flow.AddStep(catbird.NewStep("approve").
    DependsOn("submit").
    WithSignal().Do(func(ctx context.Context, doc Document, approval ApprovalInput, docID string) (ApprovalResult, error) {
    if !approval.Approved {
        return ApprovalResult{}, fmt.Errorf("approval denied by %s: %s", approval.ApproverID, approval.Notes)
    }
    return ApprovalResult{
        Status:     "approved",
        ApprovedBy: approval.ApproverID,
        Timestamp:  time.Now().Format(time.RFC3339),
    }, nil
}))
flow.AddStep(catbird.NewStep("publish").
    DependsOn("approve").Do(func(ctx context.Context, doc Document, approval ApprovalResult) (PublishResult, error) {
    return PublishResult{
        PublishedAt: time.Now().Format(time.RFC3339),
        URL:         "https://example.com/docs/" + approval.ApprovedBy,
    }, nil
}))
```

A step with both dependencies and a signal waits for **both** conditions: all dependencies must complete **and** the signal must be delivered before the step executes.

## Example: Early Completion

Use `CompleteEarly(ctx, output, reason)` inside a flow step handler when you already have the final business output and want to stop remaining branches.

```go
flow := catbird.NewFlow("fraud-check")
flow.AddStep(catbird.NewStep("quick_guard").Do(func(ctx context.Context, in Order) (string, error) {
    if in.IsKnownSafe {
        return "", catbird.CompleteEarly(ctx, Decision{Approved: true}, "known-safe fast path")
    }
    return "continue", nil
}))
flow.AddStep(catbird.NewStep("slow_analysis").Do(func(ctx context.Context, in Order) (string, error) {
    time.Sleep(2 * time.Second)
    return "done", nil
}))
flow.AddStep(catbird.NewStep("final").
    DependsOn("quick_guard", "slow_analysis").Do(func(ctx context.Context, in Order, guard string, analysis string) (Decision, error) {
    return Decision{Approved: guard == "continue" && analysis == "done"}, nil
}))
```

When early completion wins the race, the flow run becomes `completed` with the provided output, and in-flight sibling work is stopped cooperatively.

## Map Steps

Map steps fan out array processing into per-item SQL-coordinated work and aggregate results back in item order.

- Define map steps with `AddStep(NewStep("name").MapFlowInput()...)` or `AddStep(NewStep("name").MapStepOutput("source")...)`
- Use `MapFlowInput()` to map over flow input (flow input must be a JSON array)
- Use `MapStepOutput("step_name")` to map over a dependency step output array
- Each mapped item runs as its own task, so retries happen per item instead of rerunning the whole step.
- To fold mapped item outputs without materializing a full `[]Out`, add an explicit reducer step with `AddStep(NewStep("name").ReduceStep("mapped_step").Reduce(initial, fn))`.

### Map flow input

```go
flow := catbird.NewFlow("double-input")
flow.AddStep(catbird.NewStep("double").
    MapFlowInput().
    Map(func(ctx context.Context, n int) (int, error) {
    return n * 2, nil
}))

handle, _ := client.RunFlow(ctx, "double-input", []int{1, 2, 3})
var out []int
_ = handle.WaitForOutput(ctx, &out)
// out == []int{2, 4, 6}
```

### Map dependency output

```go
flow := catbird.NewFlow("double-numbers")
flow.AddStep(catbird.NewStep("numbers").Do(func(ctx context.Context, _ string) ([]int, error) {
    return []int{1, 2, 3}, nil
}))
flow.AddStep(catbird.NewStep("double").
    MapStepOutput("numbers").
    Map(func(ctx context.Context, _ string, n int) (int, error) {
    return n * 2, nil
}))

// Reduce mapped outputs with an explicit reducer step
flow = catbird.NewFlow("double-numbers-reduced")
flow.AddStep(catbird.NewStep("numbers").Do(func(ctx context.Context, _ string) ([]int, error) {
    return []int{1, 2, 3}, nil
}))
flow.AddStep(catbird.NewStep("double").
    MapStepOutput("numbers").
    Map(func(ctx context.Context, _ string, n int) (int, error) {
    return n * 2, nil
}))
flow.AddStep(catbird.NewStep("sum").
    ReduceStep("double").
    Reduce(0, func(ctx context.Context, acc int, out int) (int, error) {
    return acc + out, nil
}))
```

## Generator Steps

Generator steps act like normal flow steps with an extra trailing `yield` callback for streaming items; yielded items are processed by a per-item handler.

- Define the step with `flow.AddStep(NewStep("name").Generate(...).Map(...))`
- Optionally add `DependsOn(...)` and/or `WithSignal()` like a normal step
- Provide a generator with signature `func(context.Context, In[, Signal][, Dep1, Dep2, ...], func(ItemType) error) error`
- Provide an item handler with signature `func(context.Context, ItemType) (OutType, error)`
- To fold yielded item outputs, add an explicit reducer step with `AddStep(NewStep(...).ReduceStep("generator_step").Reduce(...))`
- Generator steps do not support `MapFlowInput()` or `MapStepOutput()`

```go
flow := catbird.NewFlow("generate-double-sum")
flow.AddStep(catbird.NewStep("seed").Do(func(ctx context.Context, in int) (int, error) {
    return in, nil
}))
flow.AddStep(catbird.NewStep("generate").
    DependsOn("seed").
    Generate(func(ctx context.Context, in int, seed int, yield func(int) error) error {
    for i := 0; i < seed; i++ {
        if err := yield(i); err != nil {
            return err
        }
    }
    return nil
}).
    Map(func(ctx context.Context, item int) (int, error) {
    return item * 2, nil
}))
flow.AddStep(catbird.NewStep("sum").
    DependsOn("generate").Do(func(ctx context.Context, in int, generated []int) (int, error) {
    total := 0
    for _, v := range generated {
        total += v
    }
    return total, nil
}))

handle, _ := client.RunFlow(ctx, "generate-double-sum", 5)
var out int
_ = handle.WaitForOutput(ctx, &out)
// out == 20
```

Use an explicit reducer step when you want bounded generator output instead of storing all item outputs as `[]Out`:

```go
flow := catbird.NewFlow("generate-double-sum-reduced")
flow.AddStep(catbird.NewStep("generate").
    Generate(func(ctx context.Context, input int, yield func(int) error) error {
    for i := 0; i < input; i++ {
        if err := yield(i); err != nil {
            return err
        }
    }
    return nil
}).
    Map(func(ctx context.Context, item int) (int, error) {
    return item * 2, nil
}))
flow.AddStep(catbird.NewStep("sum").
    ReduceStep("generate").
    Reduce(0, func(ctx context.Context, acc int, out int) (int, error) {
    return acc + out, nil
}))

handle, _ := client.RunFlow(ctx, "generate-double-sum-reduced", 5)
var out int
_ = handle.WaitForOutput(ctx, &out)
// out == 20
```

## Status Values

| Status | Meaning | Used by |
|--------|---------|---------|
| `queued` | Runnable and never picked up by a worker | Task runs, flow step runs, map item runs |
| `waiting_for_dependencies` | Not runnable yet because one or more dependencies are still incomplete | Flow step runs |
| `waiting_for_signal` | Dependencies are resolved, but required signal input has not been delivered yet | Flow step runs |
| `waiting_for_map_tasks` | Parent map/reducer step is waiting for spawned map item runs to finish | Flow step runs |
| `started` | Picked up by a worker at least once (including retries) | Task runs, flow runs, flow step runs, map item runs |
| `completed` | Finished successfully and output is available | Task runs, flow runs, flow step runs, map item runs |
| `failed` | Finished with an error | Task runs, flow runs, flow step runs, map item runs |
| `skipped` | Intentionally skipped (typically due to a condition evaluating false) | Task runs, flow step runs |
| `canceling` | Cancellation requested; run is transitioning to canceled | Task runs, flow runs |
| `canceled` | Run was canceled before completing normally | Task runs, flow runs |

## Advanced: Step Communication at Runtime

```go
flow := catbird.NewFlow("parallel_watch_flow")
flow.AddStep(catbird.NewStep("long_job").Do(func(ctx context.Context, in Order) (string, error) {
    time.Sleep(500 * time.Millisecond)
    return "job-finished", nil
}))
flow.AddStep(catbird.NewStep("watch_job").Do(func(ctx context.Context, in Order) (string, error) {
    step, err := catbird.WaitForStep(ctx, "long_job", catbird.WaitOpts{PollInterval: 25 * time.Millisecond})
    if err != nil {
        return "", err
    }
    if !step.IsCompleted() {
        return "", fmt.Errorf("long_job ended with status=%s", step.Status)
    }
    return "watcher-confirmed:" + step.Status, nil
}))
```

This runs `long_job` and `watch_job` in parallel. `watch_job` blocks on `WaitForStep(...)` until `long_job` reaches a terminal state, then exits immediately.

```go
flow := catbird.NewFlow("loop_until_peer_done")
flow.AddStep(catbird.NewStep("controller").Do(func(ctx context.Context, in string) (string, error) {
    time.Sleep(2 * time.Second)
    return "stop-now", nil
}))
flow.AddStep(catbird.NewStep("worker_loop").Do(func(ctx context.Context, in string) (string, error) {
    for {
        controller, err := catbird.GetStep(ctx, "controller")
        if err != nil {
            return "", err
        }
        if controller.IsDone() {
            return "loop-stopped:" + controller.Status, nil
        }

        select {
        case <-ctx.Done():
            return "", ctx.Err()
        case <-time.After(100 * time.Millisecond):
        }
    }
}))
```

This pattern keeps `worker_loop` alive until `controller` reaches any terminal state, then exits cleanly.

# Conditional Execution

Both tasks and flow steps support conditional execution via `WithCondition` on the builder methods. If the condition evaluates to false (or a referenced field is missing), the task/step is marked `skipped` and its handler does not run.

## Rules at a Glance

- **Prefixes**: tasks use `input.*`; flow steps use `input.*`, `step_name.*`, or `signal.*`.
- **Operators**: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `exists`, `contains`, plus `not <expr>`.
- **Optional outputs**: if a step can be skipped, downstream handlers must accept `Optional[T]` for that dependency.
- **Map steps**: define with `AddStep(NewStep("name")...)`, then use `MapFlowInput()` or `MapStepOutput("step_name")`; map source values must be arrays.
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
    WithCondition("input.is_premium"). // Skipped if is_premium = false
    Do(func(ctx context.Context, req ProcessRequest) (string, error) {
        return fmt.Sprintf("Processed premium user %d", req.UserID), nil
    })

// Run task - may be skipped based on input
client.RunTask(ctx, "premium_processing", ProcessRequest{UserID: 123, IsPremium: false})
// This task run will be skipped (is_premium = false)
```

## Flows with Conditions

Flow steps can branch based on prior outputs. Use `Optional[T]` to handle skipped dependencies.

```go
flow := catbird.NewFlow("payment_processing")
flow.OutputPriority("charge", "free_order")

flow.AddStep(catbird.NewStep("validate").Do(func(ctx context.Context, order Order) (ValidationResult, error) {
    return ValidationResult{Valid: order.Amount > 0}, nil
}))
flow.AddStep(catbird.NewStep("charge").
    DependsOn("validate").
    WithCondition("validate.valid").Do(func(ctx context.Context, order Order, validation ValidationResult) (FinalResult, error) {
    return FinalResult{Status: "charged", TxnID: "txn-123"}, nil
}))
flow.AddStep(catbird.NewStep("free_order").
    DependsOn("validate").
    WithCondition("not validate.valid").Do(func(ctx context.Context, order Order, validation ValidationResult) (FinalResult, error) {
    return FinalResult{Status: "free_order", TxnID: ""}, nil
}))
```

# Resiliency

Catbird includes multiple resiliency layers for runtime failures. Handler-level retries are configured with `HandlerOpt` values such as `WithMaxRetries(...)` and `WithFullJitterBackoff(...)`, and external calls can be protected with `WithCircuitBreaker(failureThreshold, openTimeout)` to avoid cascading outages. In worker database paths, PostgreSQL reads/writes are retried with bounded attempts and full-jitter backoff; retries stop immediately on context cancellation or deadline expiry.

For reducer-step workflows, retries are two-phase: item handlers retry first per item, then reducer-step finalization retries at the reducer step.
If retries are exhausted in either phase, the parent step fails and task/flow `OnFail` handlers run with the same terminal failure semantics as non-reduced steps.

## Be aware of side effects

Catbird deduplication (`ConcurrencyKey`/`IdempotencyKey`) controls duplicate run creation, while handler retries can still re-attempt the same run after transient failures. For non-repeatable side effects (payments, email, webhooks), use idempotent write patterns or upstream idempotency keys so retry attempts remain safe.

# Cancellation

`Cancellation` semantics:
- Cancellation is a distinct terminal outcome (`canceled`), separate from `failed` and `completed`.
- Task cancellation moves `queued`/`started` runs directly to `canceled`; already-terminal task runs are unchanged.
- Flow cancellation is two-phase: flow goes to `canceling`, queued child work is canceled, then flow becomes `canceled` after in-flight started work drains.
- Cancellation requests are idempotent: repeated requests are successful no-ops.
- A cancel request does not rewrite an already-terminal run.

External cancellation:

```go
taskHandle, _ := client.RunTask(ctx, "send-email", "hello")
_, _ = client.CancelTaskRun(ctx, "send-email", taskHandle.ID, catbird.CancelOpts{Reason: "operator requested stop"})

flowHandle, _ := client.RunFlow(ctx, "order-processing", map[string]any{"order_id": 123})
_, _ = client.CancelFlowRun(ctx, "order-processing", flowHandle.ID, catbird.CancelOpts{Reason: "customer canceled order"})
```

Internal cancellation from handlers:

```go
task := catbird.NewTask("validate-order").Do(func(ctx context.Context, input Order) (string, error) {
        if input.Amount <= 0 {
            if err := catbird.Cancel(ctx, catbird.CancelOpts{Reason: "invalid amount"}); err != nil {
                return "", err
            }
            return "", nil
        }
        return "ok", nil
    })

flow := catbird.NewFlow("order-processing")
flow.AddStep(catbird.NewStep("guard").Do(func(ctx context.Context, input Order) (string, error) {
    if input.Amount <= 0 {
        if err := catbird.Cancel(ctx, catbird.CancelOpts{Reason: "invalid amount"}); err != nil {
            return "", err
        }
        return "", nil
    }
    return "proceed", nil
}))
```

# Naming Rules

- **Queue, task, flow, and step names**: Lowercase letters, digits, and underscores only (`a-z`, `0-9`, `_`). Max 58 characters. Step names must be unique within a flow. Reserved step names: `input`, `signal`.
- **Topics/Patterns**: Letters (upper/lower), digits, dots, underscores, and hyphens (`a-z`, `A-Z`, `0-9`, `.`, `_`, `-`, plus wildcards `*`, `#`).

# Query Helpers

Use query builders when you want SQL + args directly (for `pgx.Batch` or custom execution):

- `SendQuery(queue, payload, opts)`
- `PublishQuery(topic, payload, opts)`
- `RunTaskQuery(name, input, opts)`
- `RunFlowQuery(name, input, opts)`

```go
// Queue into a batch
var batch pgx.Batch
q1, args1, err := catbird.SendQuery("my-queue", map[string]any{"user_id": 123})
if err != nil {
    return err
}
batch.Queue(q1, args1...)
```

# PostgreSQL API Reference

Catbird is built on PostgreSQL functions, so you can use the API directly from any language or tool with PostgreSQL support (psql, Python, Node.js, Ruby, etc.).

For the full SQL function reference and practical SQL examples (queues, tasks, workflows, and run monitoring), see the [SQL API reference](docs/sql-api-reference.md).

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

# Terminal UI

The terminal UI provides an interactive dashboard-like view in your terminal.

```bash
go install github.com/ugent-library/catbird/cmd/cb@latest
export CB_CONN="postgres://user:pass@localhost:5432/mydb?sslmode=disable"
cb ui
```

You can also start it from the root command using interactive mode:

```bash
cb -i
```

# Data Retention

Set a retention period on a task or flow definition to have `cb_gc()` automatically
delete terminal runs older than that duration. GC runs opportunistically from the
worker heartbeat and can also be triggered manually via `client.GC(ctx)` or the
standalone purge helpers below.

```go
gcInfo, err := client.GC(ctx)
if err != nil {
    // handle error
}

_ = gcInfo.ExpiredQueuesDeleted
_ = gcInfo.StaleWorkersDeleted
_ = gcInfo.TaskRunsPurged
_ = gcInfo.FlowRunsPurged
```

```go
task := catbird.NewTask("send-email").
    RetentionPeriod(7 * 24 * time.Hour). // NULL by default = no cleanup
    Do(func(ctx context.Context, in EmailInput) (string, error) {
        return "sent", nil
    })

flow := catbird.NewFlow("order-processing")
flow.RetentionPeriod(90 * 24 * time.Hour)
flow.AddStep(catbird.NewStep("step1").Do(func(ctx context.Context, in OrderInput) (string, error) {
    return "done", nil
}))
```

- **Task runs cleaned up**: `completed`, `failed`, `skipped`, `canceled` older than the retention period
- **Flow runs cleaned up**: `completed`, `failed`, `canceled` older than the retention period; associated step runs and map tasks are removed automatically via cascade
- **Non-terminal rows are never touched**: `queued`, `started`, `waiting_*`, `canceling` are left alone

## Purge helpers

For targeted or ad-hoc cleanup independent of the retention period:

```go
// Delete task runs older than 30 days
taskPurged, err := client.PurgeTaskRuns(ctx, "send-email", 30*24*time.Hour)

// Delete flow runs older than 90 days
flowPurged, err := client.PurgeFlowRuns(ctx, "order-processing", 90*24*time.Hour)

_ = taskPurged
_ = flowPurged

```

## External archiving

For SQL-based archiving patterns and example queries, see the
`External archiving` section in the [SQL API reference](docs/sql-api-reference.md).

# Documentation

- **[Go API Documentation](https://pkg.go.dev/github.com/ugent-library/catbird)**
- **[SQL API Reference](docs/sql-api-reference.md)**: SQL function reference and practical SQL usage examples
- **[Testing Guide](docs/testing.md)**
- **[Copilot Instructions](.github/copilot-instructions.md)**

# Acknowledgments

SQL code is taken from or inspired by the excellent [pgmq](https://github.com/pgmq) and [pgflow](https://github.com/pgflow-dev/pgflow) projects.
