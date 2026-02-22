[![Go Reference](https://pkg.go.dev/badge/github.com/ugent-library/catbird.svg)](https://pkg.go.dev/github.com/ugent-library/catbird)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.25+-blue.svg)](https://golang.org)
[![Go Report Card](https://goreportcard.com/badge/github.com/ugent-library/catbird)](https://goreportcard.com/report/github.com/ugent-library/catbird)

![CatBird](catbird-banner.svg "CatBird banner")

# Catbird

A PostgreSQL-powered message queue and task execution engine. Catbird brings reliability and simplicity to background job processing by using your database as the single source of truth—no extra services to manage, just your database coordinating everything.

## Why Catbird?

- **Transactional operations**: All Catbird operations are transactional by design. Enqueue a message, update application state, and commit atomically—if the transaction rolls back, the message never existed. This eliminates the dual-write problem that plagues external message queues.
- **Exactly-once execution within a timeframe**: PostgreSQL ensures each message is processed exactly once per visibility window, preventing duplicates even with multiple workers. If a worker crashes, the message becomes visible again after the configured timeout and can be retried.
- **Database as coordinator**: PostgreSQL manages message distribution, deduplication, and state. Scale workers horizontally; the database handles the rest.
- **Type-safe tasks**: Generic Go handlers with automatic JSON marshaling for inputs and outputs.
- **Flexible workflows**: Multi-step DAGs with dependency tracking and cascading data between steps.
- **Built-in persistence**: All state (queues, tasks, flows, runs) lives in PostgreSQL for auditability and recovery.
- **Worker management**: Simple worker lifecycle with graceful shutdown, configurable concurrency, and built-in retries.
- **Resiliency features**: Handler retries with backoff, optional circuit breaker protection, and bounded PostgreSQL retries with full-jitter backoff in worker database paths.
- **Dashboard**: Web UI to trigger task/flow runs, monitor progress in real-time, and view results.

<p align="center">
  <img src="screenshots/dashboard-flows.png" alt="Flow Visualization" width="800" />
</p>

## Quick Start

### Basic Queue

```go
ctx := context.Background()
client := catbird.New(conn)

// Create a queue
err := client.CreateQueue(ctx, "my-queue", nil)

// Create a queue that expires after 1 hour and is unlogged for better performance
err = client.CreateQueue(ctx, "temp-queue", &catbird.QueueOpts{
    ExpiresAt: time.Now().Add(1 * time.Hour),
    Unlogged:  true, // Faster but not crash-safe; useful for transient work
})

// Send a message
err = client.Send(ctx, "my-queue", map[string]any{
    "user_id": 123,
    "action":  "process",
}, nil)

// Send a message with delayed delivery (available in 5 minutes)
err = client.Send(ctx, "my-queue", map[string]any{
    "type":    "reminder",
    "user_id": 456,
}, &catbird.SendOpts{
    DeliverAt: time.Now().Add(5 * time.Minute),
})

// Send a message with idempotency key to prevent duplicates
err = client.Send(ctx, "my-queue", map[string]any{
    "order_id": 789,
    "action":   "process",
}, &catbird.SendOpts{
    IdempotencyKey: "order-789-process",
})

// Read messages (hidden from other readers for 30 seconds)
messages, err := client.Read(ctx, "my-queue", 10, 30*time.Second)
for _, msg := range messages {
    // Process message
    client.Delete(ctx, "my-queue", msg.ID)
}
```

## Deduplication Strategies

Catbird supports two deduplication strategies for tasks and flows, allowing you to control when duplicate executions are prevented:

### ConcurrencyKey (Temporary Deduplication)

**Use case**: Prevent concurrent/overlapping executions but allow re-runs after completion.

**Semantics**: Deduplicates runs in `queued` or `started` status. Once a run completes (success) or fails, the same key can be used again.

**Example scenarios**:
- Rate limiting: "Don't process the same user request multiple times concurrently"
- Resource locking: "Only one indexing job per dataset at a time"
- Scheduled tasks: "Prevent overlap between hourly cron runs"

```go
// Task: Prevent concurrent executions
handle, err := client.RunTask(ctx, "process-user", userID, &catbird.RunOpts{
    ConcurrencyKey: fmt.Sprintf("user-%d", userID),
})

// Flow: Prevent concurrent workflow runs
handle, err := client.RunFlow(ctx, "order-processing", order, &catbird.RunFlowOpts{
    ConcurrencyKey: fmt.Sprintf("order-%s", order.ID),
})

// Scheduled tasks automatically use IdempotencyKey (UTC-normalized for multi-worker dedup)
worker, err := client.NewWorker(ctx,
    catbird.WithTask(hourlyTask),
    catbird.WithScheduledTask("hourly_task", "@hourly"), // Uses IdempotencyKey internally
)
```

**Behavior**:
- Run 1 (status: `started`) → Run 2 with same key → **Rejected** (duplicate)
- Run 1 (status: `completed`) → Run 2 with same key → **Allowed** (new execution)
- Run 1 (status: `failed`) → Run 2 with same key → **Allowed** (retry)

### IdempotencyKey (Permanent Deduplication)

**Use case**: Ensure exactly-once execution—prevent all duplicates including after completion.

**Semantics**: Deduplicates runs in `queued`, `started`, or `completed` status. Once a run completes successfully, the same key can never be used again (unless the completed run is manually deleted from the database).

**Example scenarios**:
- Payment processing: "Charge this payment exactly once, even if retried"
- Order fulfillment: "Ship this order exactly once"
- Webhook delivery: "Process this webhook event exactly once"
- Audit logging: "Record this event exactly once"

```go
// Task: Ensure payment is processed exactly once
handle, err := client.RunTask(ctx, "charge-payment", payment, &catbird.RunOpts{
    IdempotencyKey: fmt.Sprintf("payment-%s", payment.ID),
})

// Flow: Ensure order is processed exactly once
handle, err := client.RunFlow(ctx, "fulfill-order", order, &catbird.RunFlowOpts{
    IdempotencyKey: fmt.Sprintf("order-%s-fulfillment", order.ID),
})
```

**Behavior**:
- Run 1 (status: `started`) → Run 2 with same key → **Rejected** (duplicate)
- Run 1 (status: `completed`) → Run 2 with same key → **Rejected** (already executed)
- Run 1 (status: `failed`) → Run 2 with same key → **Allowed** (retry on failure)

### Comparison Table

| Feature | ConcurrencyKey | IdempotencyKey |
|---------|----------------|----------------|
| **Purpose** | Prevent overlapping runs | Ensure exactly-once execution |
| **Deduplicates** | `queued`, `started` | `queued`, `started`, `completed` |
| **After completion** | Allows re-run | Rejects duplicate |
| **After failure** | Allows retry | Allows retry |
| **Use for** | Rate limiting, resource locking, scheduled tasks | Payments, orders, webhooks, audit logs |

### Important Notes

- **Mutually exclusive**: You cannot provide both `ConcurrencyKey` and `IdempotencyKey` for the same run (returns error)
- **Return value on duplicate**: When a duplicate is detected, `RunTask()`/`RunFlow()` return a handle with the **existing run's ID**, not an error. This allows you to wait on the existing execution:
  ```go
  // First call creates run with ID 123
    h1, _ := client.RunTask(ctx, "task", input, &RunOpts{IdempotencyKey: "key-1"})
  // h1.ID = 123
  
  // Duplicate call returns handle to same run
    h2, _ := client.RunTask(ctx, "task", input, &RunOpts{IdempotencyKey: "key-1"})
  // h2.ID = 123 (same as h1)
  
  // Both handles can wait for the same result
  h1.WaitForOutput(ctx, &result)
  h2.WaitForOutput(ctx, &result)  // Returns immediately with same result
  ```
- **Failure retries**: Both strategies allow retries when a task/flow fails (`status: failed`). A new run is created (different ID) when retrying a failed execution.
- **No key = no deduplication**: If you don't provide either key, duplicates are allowed
- **Scheduled tasks**: Automatically use `ConcurrencyKey` to prevent schedule overlap
- **Queue messages**: Use `IdempotencyKey` in `SendOpts` for exactly-once message delivery

### Topic-Based Routing

```go
// Create queues
err := client.CreateQueue(ctx, "user-events", nil)
err = client.CreateQueue(ctx, "audit-log", nil)

// Bind queues to topic patterns
// Exact match
err = client.Bind(ctx, "user-events", "events.user.created")

// Single-token wildcard (? matches exactly one token)
err = client.Bind(ctx, "user-events", "events.?.updated")
// Matches: events.user.updated, events.order.updated

// Multi-token wildcard (* matches one or more tokens at end)
err = client.Bind(ctx, "audit-log", "events.*")
// Matches: events.user.created, events.order.shipped, etc.

// Dispatch a message to all matching queues
err = client.Dispatch(ctx, "events.user.created", map[string]any{
    "user_id": 123,
    "email":   "user@example.com",
}, nil)
// Message is delivered to both "user-events" (exact) and "audit-log" (wildcard)

// Unbind a pattern
err = client.Unbind(ctx, "user-events", "events.?.updated")
```

Wildcard rules:
- `?` matches a single token (e.g., `events.?.created` matches `events.user.created`)
- `*` matches one or more tokens at the end (e.g., `events.user.*` matches `events.user.created.v1`)
- `*` must appear as `.*` at the end of the pattern
- Tokens are separated by `.` and can contain `a-z`, `A-Z`, `0-9`, `_`, `-`

### Task Execution

```go
// Define the task
task := catbird.NewTask("send-email",
    func(ctx context.Context, input EmailRequest) (EmailResponse, error) {
        // Send email logic here
        return EmailResponse{SentAt: time.Now()}, nil
    },
    &catbird.TaskOpts{
        Concurrency:    5, // Allow up to 5 concurrent executions
        MaxRetries:     3, // Retry 3 times
        MinDelay:       500*time.Millisecond,
        MaxDelay:       10*time.Second, // Exponential backoff between 500ms and 10s
        CircuitBreaker: catbird.NewCircuitBreaker(5, 30*time.Second), // Open after 5 consecutive failures
    },
)

// Define a task with a condition (skipped when condition is false)
conditionalTask := catbird.NewTask("premium-processing",
    func(ctx context.Context, input ProcessRequest) (string, error) {
        return "processed", nil
    },
    &catbird.TaskOpts{
        Condition: "input.is_premium", // Skipped if is_premium = false
    },
)

// Start a worker that handles the tasks
worker, err := client.NewWorker(ctx, catbird.WithTask(task), catbird.WithTask(conditionalTask))
go worker.Start(ctx)

// Run the task
handle, err := client.RunTask(ctx, "send-email", EmailRequest{
    To:      "user@example.com",
    Subject: "Hello",
}, nil)

// Get result
var result EmailResponse
err = handle.WaitForOutput(ctx, &result)

// Schedule a task to run periodically (using cron syntax)
worker, err = client.NewWorker(ctx,
    catbird.WithTask(sendEmailsTask),
    catbird.WithScheduledTask("send-emails", "@hourly"), // Run every hour
)
go worker.Start(ctx)
```

## Flow Semantics

A **flow** (also called a workflow) is a **directed acyclic graph (DAG)** of steps that execute in dependency order. Understanding flow execution is critical for building reliable workflows.

### Core Concepts

**What is a flow?**
- A multi-step unit of work where each step is a function that accepts inputs and produces outputs
- Steps can depend on other steps, creating execution constraints
- A flow has an input (provided at run-time) that all steps can access
- **A flow has exactly ONE output: the unwrapped output of the final step** (the step with no dependents)

**How does execution work?**
1. **Initialization**: When `RunFlow()` is called, all steps are created with status `created`
2. **Dependency resolution**: Steps with no dependencies immediately start (status transitions to `started` then `completed`)
3. **Cascading activation**: As each step completes, steps that depend on it are checked:
   - If all dependencies are satisfied → step transitions to `started`
   - If any dependency was skipped → the step checks if it uses `Optional[T]` for a conditional dependency
4. **Flow completion**: When the final step completes, the flow is marked `completed` with the final step's output
5. **Output retrieval**: `WaitForOutput()` retrieves the unwrapped output of the final step

**Key invariant**: Catbird ensures there is always **exactly one final step** (a step with no dependents). This is validated at flow construction time.

**Parallel execution**: Steps with independent dependencies can execute in parallel. Workers poll for ready steps and execute them concurrently.

**Skipped steps**: When a step has a condition that evaluates to false, it transitions directly to `skipped` status. Dependent steps must use `Optional[T]` parameter types to handle skipped dependencies.

### Example: Linear Chain

```
Input: 100
        ↓
 ┌──────────────┐
 │ step1(100)   │  Returns 100 * 2 = 200
 └──────────────┘
        ↓
 ┌──────────────┐
 │ step2(200)   │  Returns 200 + 50 = 250
 └──────────────┘
        ↓
 ┌──────────────┐
 │ step3(250)   │  Returns 250 (FINAL STEP OUTPUT)
 └──────────────┘
        ↓
  Output: 250
```

Each step receives the flow's input PLUS outputs from its dependencies.

### Example: Parallel Steps

```
Input: [user1, user2, user3]
            ↓
    ┌──────────────┐
    │  validate    │
    └──────────────┘
     ↓            ↓
  charge      inventory    ← These run in PARALLEL (both depend only on validate)
     ↓            ↓
    ┌──────────────┐
    │    ship      │  (Waits for BOTH charge AND inventory)
    └──────────────┘
            ↓
  Output: ship's result
```

**Execution timeline**:
1. `validate` runs immediately
2. When `validate` completes, both `charge` and `inventory` start immediately (in parallel)
3. When BOTH complete, `ship` starts
4. When `ship` completes, flow is done

### Example: Conditional Steps with Reconvergence

```
Input: amount=50

        ↓
    ┌───────────────┐
    │  assess_risk  │  Returns RiskScore: 20
    └───────────────┘
        ↓
    [Condition: score > 30?]
        ↓                ↓
       NO                YES
        ↓                ↓
    (skipped)        ┌─────────┐
        ↓            │ audit   │
        ↓            └─────────┘
        ↓                ↓
    ┌─────────────────────────┐
    │  finalize (Optional!)    │  Uses Optional[T] to handle both cases
    └─────────────────────────┘
            ↓
  Output: finalize's result
```

**Execution**:
- `assess_risk` runs and returns `RiskScore: 20`
- The `audit` step condition `assess_risk.score > 30?` evaluates to false
- `audit` transitions to `skipped`
- `finalize` uses `Optional[AuditLog]` parameter so it receives `Optional[T]{IsSet: false}`
- The handler checks `if !audit.IsSet { /* handle skipped case */ }`

**Critical requirement**: When a step depends on a conditional step, use `Optional[T]` parameter type:
```go
NewStep1Dep("finalize",
    "audit",
    func(ctx context.Context, in Order, audit catbird.Optional[AuditLog]) (Result, error) {
        if audit.IsSet {
            return audit.Value, nil
        }
        return DefaultResult, nil
    }, nil)
```

The `Optional[T]` parameter type automatically signals to the flow that this is an optional dependency. Without `Optional[T]`, flow construction will panic: "step depends on conditional step but does not use Optional[T]".

### Workflow (Multi-Step Flow)

```go
type Order struct {
    ID     string  `json:"id"`
    Amount int     `json:"amount"`
}

type ValidationResult struct {
    Valid  bool   `json:"valid"`
    Reason string `json:"reason"`
}

type ChargeResult struct {
    TransactionID string `json:"transaction_id"`
    Amount        int    `json:"amount"`
}

type InventoryCheck struct {
    InStock bool `json:"in_stock"`
    Qty     int  `json:"qty"`
}

type ShipmentResult struct {
    TrackingNumber string `json:"tracking_number"`
    EstimatedDays int    `json:"estimated_days"`
}

// Define the flow with steps and dependencies
flow := catbird.NewFlow[Order, ShipmentResult]("order-processing").
    AddStep(catbird.NewStep("validate", func(ctx context.Context, order Order) (ValidationResult, error) {
        // Validate order
        if order.Amount <= 0 {
            return ValidationResult{Valid: false, Reason: "Invalid amount"}, nil
        }
        return ValidationResult{Valid: true}, nil
    }, nil)).
    AddStep(catbird.NewStep1Dep("charge",
        "validate",
        func(ctx context.Context, order Order, validated ValidationResult) (ChargeResult, error) {
            // Charge payment and return transaction ID
            if !validated.Valid {
                return ChargeResult{}, fmt.Errorf("cannot charge invalid order")
            }
            return ChargeResult{
                TransactionID: "txn-" + order.ID,
                Amount:        order.Amount,
            }, nil
        }, nil)).
    AddStep(catbird.NewStep1Dep("check-inventory",
        "validate",
        func(ctx context.Context, order Order, validated ValidationResult) (InventoryCheck, error) {
            // Check inventory (independent of charge, runs in parallel)
            return InventoryCheck{
                InStock: true,
                Qty:     order.Amount,
            }, nil
        }, nil)).
    AddStep(catbird.NewStep2Deps("ship",
        "charge",
        "check-inventory",
        func(ctx context.Context, order Order, chargeResult ChargeResult, inventory InventoryCheck) (ShipmentResult, error) {
            // Ship order only if both charge and inventory check passed
            if !inventory.InStock {
                return ShipmentResult{}, fmt.Errorf("out of stock")
            }
            return ShipmentResult{
                TrackingNumber: "TRK-" + chargeResult.TransactionID,
                EstimatedDays:  3,
            }, nil
        }, nil))

// Start a worker that handles the flow
worker, err := client.NewWorker(ctx,
    catbird.WithFlow(flow),
)
go worker.Start(ctx)

// Run the flow
handle, err := client.RunFlow(ctx, "order-processing", Order{
    ID:     "order-123",
    Amount: 9999,
}, nil)

// Get the final step's output (ShipmentResult)
var result ShipmentResult
err = handle.WaitForOutput(ctx, &result)
if err != nil {
    // Handle error
}

// result.TrackingNumber contains the shipment tracking number
fmt.Println("Shipped with tracking:", result.TrackingNumber)

// Schedule a flow to run periodically (using cron syntax)
worker, err = client.NewWorker(ctx,
    catbird.WithFlow(flow),
    catbird.WithScheduledFlow("order-processing", "0 2 * * *"), // Run daily at 2 AM
)
go worker.Start(ctx)
```

### Workflow with Signals (Human-in-the-Loop)

Signals enable workflows that wait for external input before proceeding, such as approval workflows or webhooks.

```go
type Document struct {
    ID      string `json:"id"`
    Content string `json:"content"`
}

type ApprovalInput struct {
    ApproverID string `json:"approver_id"`
    Approved   bool   `json:"approved"`
    Notes      string `json:"notes"`
}

type ApprovalResult struct {
    Status     string `json:"status"`
    ApprovedBy string `json:"approved_by"`
    Timestamp  string `json:"timestamp"`
}

type PublishResult struct {
    PublishedAt string `json:"published_at"`
    URL         string `json:"url"`
}

// Define a flow with an approval step that requires a signal
flow := catbird.NewFlow[Document, PublishResult]("document_approval").
    AddStep(catbird.NewStep("submit", func(ctx context.Context, doc Document) (string, error) {
        // Submit document for review
        return doc.ID, nil
    }, nil)).
    AddStep(catbird.NewStepSignal1Dep("approve",
        "submit",
        func(ctx context.Context, doc Document, approval ApprovalInput, docID string) (ApprovalResult, error) {
            if !approval.Approved {
                return ApprovalResult{}, fmt.Errorf("approval denied by %s: %s", approval.ApproverID, approval.Notes)
            }
            return ApprovalResult{
                Status:     "approved",
                ApprovedBy: approval.ApproverID,
                Timestamp:  time.Now().Format(time.RFC3339),
            }, nil
        }, nil)).
    AddStep(catbird.NewStep1Dep("publish",
        "approve",
        func(ctx context.Context, doc Document, approval ApprovalResult) (PublishResult, error) {
            // Publish the approved document
            return PublishResult{
                PublishedAt: time.Now().Format(time.RFC3339),
                URL:         "https://example.com/docs/" + approval.ApprovedBy,
            }, nil
        }, nil))

// Start worker
worker, err := client.NewWorker(ctx, catbird.WithFlow(flow))
go worker.Start(ctx)

// Run the flow - it will wait at the "approve" step until a signal is sent
handle, err := client.RunFlow(ctx, "document_approval", Document{
    ID:      "doc-123",
    Content: "Important document",
}, nil)

// Later, when approval is received (e.g., from a webhook, HTTP endpoint, or UI):
err = client.SignalFlow(ctx, "document_approval", handle.ID, "approve", ApprovalInput{
    ApproverID: "user-456",
    Approved:   true,
    Notes:      "LGTM",
})

// Wait for flow completion
var result PublishResult
err = handle.WaitForOutput(ctx, &result)
if err != nil {
    log.Fatal(err)
}
// result.PublishedAt and result.URL are now populated
```

Signal variants available:
- `NewStepSignal` - step with no dependencies + signal
- `NewStepSignal1Dep` - step with 1 dependency + signal
- `NewStepSignal2Deps` - step with 2 dependencies + signal
- `NewStepSignal3Deps` - step with 3 dependencies + signal

A step with both dependencies and a signal waits for **both** conditions: all dependencies must complete **and** the signal must be delivered before the step executes.

## Conditional Execution

Both tasks and flow steps support conditional execution via the `Condition` field on `TaskOpts` and `StepOpts`. When a condition evaluates to false (or a referenced field is missing), the task/step is skipped instead of executed. This enables patterns like environment-specific processing, risk-based branching, and optional side effects.

### Basic Concepts

**How conditions work**:
- Set `Condition: "<expression>"` on `TaskOpts` (tasks) or `StepOpts` (flow steps)
- If the condition evaluates to true, the handler executes normally
- If the condition evaluates to false or a referenced field is missing, the task/step is marked as `skipped`
- Skipped tasks/steps do not execute their handler function

**What conditions can reference**:
- **Tasks**: Use `input.field_name` to reference fields from the task input
- **Flow steps**: Can reference `input.field_name` (flow input), `step_name.field_name` (prior step outputs), and `signal.field_name` (signal input for signal-enabled steps)

**Flow-specific considerations**:
- **Key rule**: If a step can be skipped, downstream steps must accept `Optional[T]` parameters to handle missing outputs
- **Optional outputs**: When a conditional step is skipped, dependent steps receive `Optional[T]{IsSet: false}`
- **Reconvergence**: Multiple conditional branches can merge back together using `Optional[T]` parameter types

### Condition Expression Syntax

Condition expressions follow the format: `[prefix].[field] [operator] [value]`

The **prefix** differs based on context:
- **Tasks**: Use `input` to reference fields from the task input (e.g., `input.priority`, `input.amount`)
- **Flow steps**: Use `input` for flow input (e.g., `input.priority`, `input.amount`) or a step name for step outputs (e.g., `validate.score`, `assess_risk.category`)
- **Flow steps with signals**: Use `signal` to reference the signal input when present (e.g., `signal.approver_id`)

Supported operators:
- `not <expr>` - negates any condition expression
- `eq` - equals
- `ne` - not equals
- `gt` - greater than
- `gte` - greater than or equal
- `lt` - less than
- `lte` - less than or equal
- `in` - value in array
- `exists` - field exists (true if field is non-null)
- `contains` - string contains substring or array/object containment

**For scalar inputs/outputs** (simple values like `bool`, `int`, or `string`):
```go
// Task input or step output is a boolean
// TaskOpts{Condition: "input.is_premium"}      // task: executes if input.is_premium is true
// TaskOpts{Condition: "not input.is_premium"}  // task: executes if input.is_premium is false
// StepOpts{Condition: "validate"}              // flow: executes if validate step output is true

// Task input or step output is a number
// TaskOpts{Condition: "input.count ne 0"}  // task: executes if input.count != 0
// StepOpts{Condition: "score gt 50"}       // flow: executes if score step output > 50
```

**For struct inputs/outputs** (objects with multiple fields):
```go
// Task input has fields
// TaskOpts{Condition: "input.risk_score gte 30"}  // access field from task input struct

// Flow step output has fields
// StepOpts{Condition: "assess_risk.risk_score gte 30"}  // access field from step output struct
```

### Path Syntax

Condition expressions support rich path syntax for accessing nested data:

**Simple fields**:
- `input.field_name` — Access top-level field from task input
- `step_name.field_name` — Access top-level field from step output
- `signal.field_name` — Access top-level field from signal input (steps with signals only)

**Nested fields** (using dot notation):
- `input.profile.address.zip` — Access deeply nested task input fields
- `step_name.field1.field2.field3` — Access deeply nested step output fields
- Dots separate each level in the object hierarchy

**Array elements** (using bracket notation):
- `input.tags[0]` — Access first array element from task input
- `step_name.items[2].priority` — Access field on 3rd array element from step output
- Works with zero-based indexing

**Complete examples**:
- `input.is_premium` — task: scalar boolean input
- `input.amount lt 100` — task: scalar number input
- `input.user.email exists` — task: nested field from input
- `input.tags[0] eq "urgent"` — task: first array element from input
- `validate` — flow: scalar boolean step output
- `count eq 0` — flow: scalar integer step output
- `assess_risk.risk_score lt 30` — flow: struct field from step output
- `user.profile.age gte 18` — flow: nested struct fields from step output
- `results[0].status eq "approved"` — flow: field on array element from step output
- `signal.approver_id eq "user123"` — flow: field from signal input

### Tasks with Conditions

Tasks can use conditions to skip execution based on input fields. This is useful for environment-specific processing, feature flags, or filtering work based on input criteria.

```go
type ProcessRequest struct {
    UserID     int    `json:"user_id"`
    IsPremium  bool   `json:"is_premium"`
    Amount     int    `json:"amount"`
    Environment string `json:"environment"`
}

// Only process premium users
premiumTask := catbird.NewTask("premium_processing",
    func(ctx context.Context, req ProcessRequest) (string, error) {
        return fmt.Sprintf("Processed premium user %d", req.UserID), nil
    },
    &catbird.TaskOpts{
        Condition: "input.is_premium", // Skipped if is_premium = false
    },
)

// Run task - may be skipped based on input
client.RunTask(ctx, "premium_processing", ProcessRequest{UserID: 123, IsPremium: false}, nil)
// This task run will be skipped (is_premium = false)
```

**Task condition patterns**:
- Feature flags: `TaskOpts{Condition: "input.feature_enabled"}`
- Environment checks: `TaskOpts{Condition: "input.env eq \"production\""}`
- Priority filtering: `TaskOpts{Condition: "input.priority eq \"high\""}`
- Value thresholds: `TaskOpts{Condition: "input.amount gte 100"}`
- User segments: `TaskOpts{Condition: "input.user_tier in [\"premium\", \"enterprise\"]"}`

### Flows with Conditions

Flow steps can use conditions to create optional side effects and divergent branching based on previous step outputs.

#### Pattern 1: Optional Side Effects

Steps that enhance but aren't required for the main flow:

```go
type RiskAssessment struct {
    RiskScore int    `json:"risk_score"`
    RiskLevel string `json:"risk_level"`
}

type AuditLog struct {
    Timestamp time.Time `json:"timestamp"`
    Message   string    `json:"message"`
}

type ProcessResult struct {
    Status string `json:"status"`
}

// Define a flow that only audits high-risk transactions
flow := catbird.NewFlow[Order, ProcessResult]("checkout").
    AddStep(catbird.NewStep("assess_risk", func(ctx context.Context, order Order) (RiskAssessment, error) {
        score := calculateRiskScore(order)
        return RiskAssessment{
            RiskScore: score,
            RiskLevel: levelFromScore(score),
        }, nil
    }, nil)).
    AddStep(catbird.NewStep1Dep("audit",
        "assess_risk",
        func(ctx context.Context, order Order, risk RiskAssessment) (AuditLog, error) {
            // Only audit high-risk transactions
            return AuditLog{
                Timestamp: time.Now(),
                Message:   fmt.Sprintf("High-risk order %s with score %d", order.ID, risk.RiskScore),
            }, nil
        },
        &catbird.StepOpts{Condition: "assess_risk.risk_score gte 30"})).  // Skip if risk_score < 30
    AddStep(catbird.NewStep1Dep("process",
        "assess_risk",  // Safe: depends on unconditional pre-branch step
        func(ctx context.Context, order Order, risk RiskAssessment) (ProcessResult, error) {
            // Process order regardless of audit execution
            return ProcessResult{Status: "ORDER_PROCESSED"}, nil
        }, nil))

// When assess_risk returns risk_score: 20:
// - audit step is skipped (condition evaluates to false)
// - process step executes normally (depends on unconditional assess_risk, not on audit)
// - flow output = process step output
```

When `assess_risk` completes with `risk_score: 20`, the `audit` step is skipped. The `process` step executes normally because it depends on the unconditional `assess_risk` step, not on the conditional `audit` step.

#### Pattern 2: Divergent Branching

Different conditions lead to different execution paths. Since Catbird requires exactly one final step, divergent branches must reconverge using `Optional[T]` parameters:

```go
type Request struct {
    ID     string `json:"id"`
    Amount int    `json:"amount"`
}

type RiskAssessment struct {
    RiskScore int    `json:"risk_score"`
    Category  string `json:"category"`
}

type ApprovalResult struct {
    Status string `json:"status"`
    Notes  string `json:"notes"`
}

// Define a flow with divergent branches that reconverge
flow := catbird.NewFlow[Request, ApprovalResult]("approval_workflow").
    AddStep(catbird.NewStep("assess", func(ctx context.Context, request Request) (RiskAssessment, error) {
        score := calculateRisk(request)
        return RiskAssessment{
            RiskScore: score,
            Category:  categorize(score),
        }, nil
    }, nil)).
    // Low-risk: auto-approve (executes when category is "low")
    AddStep(catbird.NewStep1Dep("auto_approve",
        "assess",
        func(ctx context.Context, req Request, assessment RiskAssessment) (ApprovalResult, error) {
            return ApprovalResult{Status: "APPROVED", Notes: "Auto-approved"}, nil
        },
        &catbird.StepOpts{Condition: "assess.category eq \"low\""})).
    // Medium-risk: manager approval (executes when category is "medium")
    AddStep(catbird.NewStep1Dep("manager_review",
        "assess",
        func(ctx context.Context, req Request, assessment RiskAssessment) (ApprovalResult, error) {
            return ApprovalResult{Status: "PENDING", Notes: "Awaiting manager review"}, nil
        },
        &catbird.StepOpts{Condition: "assess.category eq \"medium\""})).
    // High-risk: executive approval (executes when category is "high")
    AddStep(catbird.NewStep1Dep("executive_review",
        "assess",
        func(ctx context.Context, req Request, assessment RiskAssessment) (ApprovalResult, error) {
            return ApprovalResult{Status: "PENDING", Notes: "Awaiting executive review"}, nil
        },
        &catbird.StepOpts{Condition: "assess.category eq \"high\""})).
    // Reconvergence step: REQUIRED to have exactly one final step
    AddStep(catbird.NewStep3Deps("finalize",
        "auto_approve",
        "manager_review",
        "executive_review",
        func(ctx context.Context, req Request, 
            autoApprove catbird.Optional[ApprovalResult],
            managerReview catbird.Optional[ApprovalResult],
            executiveReview catbird.Optional[ApprovalResult]) (ApprovalResult, error) {
            // Exactly one branch executed - return whichever is set
            if autoApprove.IsSet {
                return autoApprove.Value, nil
            }
            if managerReview.IsSet {
                return managerReview.Value, nil
            }
            if executiveReview.IsSet {
                return executiveReview.Value, nil
            }
            return ApprovalResult{}, fmt.Errorf("no approval branch executed")
        }, nil))

// Based on the risk category, exactly one approval path executes:
// - Category "low": auto_approve runs → finalize receives Optional{IsSet: true} for auto_approve
// - Category "medium": manager_review runs → finalize receives Optional{IsSet: true} for manager_review
// - Category "high": executive_review runs → finalize receives Optional{IsSet: true} for executive_review
// The finalize step is the single final step (required by validation)
```

**Why reconvergence is required**: Catbird validates at construction time that flows have exactly one final step (a step with no dependents). Even though only one branch executes at runtime, all three conditional branches are visible as final steps at construction time, violating this rule. The `finalize` step solves this by depending on all branches.

#### Pattern 3: Reconvergent Branching (Using Optional)

When conditional branches need to merge back together, use `Optional[T]` parameter types:

```go
type Order struct {
    ID     string `json:"id"`
    Amount int    `json:"amount"`
}

type ValidationResult struct {
    Valid bool `json:"valid"`
}

type ChargeResult struct {
    TransactionID string `json:"transaction_id"`
}

type FinalResult struct {
    Status string `json:"status"`
    TxnID  string `json:"txn_id"`
}

flow := catbird.NewFlow[Order, FinalResult]("payment_processing").
    AddStep(catbird.NewStep("validate", func(ctx context.Context, order Order) (ValidationResult, error) {
        return ValidationResult{Valid: order.Amount > 0}, nil
    }, nil)).
    AddStep(catbird.NewStep1Dep("charge",
        "validate",
        func(ctx context.Context, order Order, validation ValidationResult) (ChargeResult, error) {
            return ChargeResult{TransactionID: "txn-123"}, nil
        },
        &catbird.StepOpts{Condition: "validate.valid"})).  // Skip for invalid orders
    AddStep(catbird.NewStep1Dep("finalize",
        "charge",
        func(ctx context.Context, order Order, charge catbird.Optional[ChargeResult]) (FinalResult, error) {
            if charge.IsSet {
                return FinalResult{Status: "charged", TxnID: charge.Value.TransactionID}, nil
            }
            return FinalResult{Status: "free_order", TxnID: ""}, nil
        }, nil))

// When order.amount <= 0:
// - validate returns Valid: false
// - charge condition evaluates to false → charge is skipped
// - finalize receives Optional[ChargeResult]{IsSet: false}
// - finalize returns FinalResult{Status: "free_order"}

// When order.amount > 0:
// - validate returns Valid: true
// - charge condition evaluates to true → charge executes
// - finalize receives Optional[ChargeResult]{IsSet: true, Value: ...}
// - finalize returns FinalResult{Status: "charged", TxnID: ...}
```

The `finalize` step executes whether or not `charge` runs, checking `charge.IsSet` to handle both cases.

**Important**: This pattern requires:
1. Parameter type must be `Optional[ChargeResult]` (not just `ChargeResult`)
2. Handler must check `charge.IsSet` before using `charge.Value`

The `Optional[T]` parameter type automatically signals to the flow that this is an optional dependency. Failure to use `Optional[T]` when depending on a conditional step will cause a panic during flow construction: "step depends on conditional step but does not use Optional[T]".

### Design Philosophy

✅ **Optional side effects** - Steps that enhance but aren't required
```
main_path → [Condition] → optional_logging
       ↓
       next_step  ✓ next_step depends on main_path, not optional step
```

✅ **Divergent branching** - Different conditions lead to different paths, must reconverge
```
assess → [Condition score < 50] → auto_approve ┐
    → [Condition 50 ≤ score < 80] → manager_review ├─► finalize (reconvergence step, Optional[T] for all)
    → [Condition score ≥ 80] → executive_review   ┘
```

✅ **Reconvergent branching (explicit)** - Merge paths using Optional[T] parameters
```
assess → [Condition score < 50] → auto_approve ┐
                                ├─► reconcile (Optional[T] parameters for both)
assess → [Condition score ≥ 50] → manager_review ┘
```

✅ **Sequential conditional** - Chain of optional steps
```
validate → [Condition !valid] → enrich → [Condition premium] → analyze
```

**When to use Conditions**:
- **Tasks**: Feature flags, environment checks, priority filtering, value thresholds, user segments
- **Flows**: Audit logging for high-value transactions, approval tiers, environment-specific steps, A/B testing, conditional notifications, cache warming

**When to be cautious**:
- Reconvergent logic that assumes outputs are always present → use `Optional[T]` checks (flows only)
- Business rules that should fail fast → return errors instead of skipping

### Advanced Examples

#### Nested Field Access

Conditions can reference nested fields using dot notation:

```go
type CheckResult struct {
    Inventory struct {
        InStock bool `json:"in_stock"`
        Qty     int  `json:"qty"`
    } `json:"inventory"`
}

// Flow example
flow := catbird.DefineFlow("order_processing",
    catbird.NewStep("check_stock", func(ctx context.Context, order Order) (CheckResult, error) {
        return CheckResult{Inventory: struct{...}{InStock: true, Qty: 10}}, nil
    }, nil),
    catbird.NewStep1Dep("ship",
        "check_stock",
        func(ctx context.Context, order Order, check CheckResult) (string, error) {
            return "SHIPPED", nil
        },
        &catbird.StepOpts{Condition: "check_stock.inventory.qty gte 5"}),
)

// Task example with nested input
type TaskInput struct {
    User struct {
        Profile struct {
            Age int `json:"age"`
        } `json:"profile"`
    } `json:"user"`
}

task := catbird.NewTask("age_restricted",
    func(ctx context.Context, input TaskInput) (string, error) {
        return "processed", nil
    },
    &catbird.TaskOpts{
        Condition: "input.user.profile.age gte 18",
    },
)
```

### Troubleshooting Conditions

**Q: My condition never evaluates to true. Why?**
- **Tasks**: Check that you're using `input.field_name` prefix and the field name matches JSON struct tags
- **Flows**: Check that your prefix is valid for the context (`input.*`, `step_name.*`, or `signal.*`) and that field names match JSON struct tags
- Confirm the operator is supported (eq, ne, gt, gte, lt, lte, in, exists, contains)
- For nested fields, use dot notation (e.g., `input.user.profile.age` or `step_name.user.profile.age`)

**Q: Can I use multiple conditions or AND/OR logic?**
- No. Each task/step has a single `Condition` field, so only one condition expression can be configured per task/step.
- AND/OR logic is not currently supported in condition expressions.
- **Workaround**: Have the task input or upstream step compute a categorical field (e.g., "low", "medium", "high") and use that in conditions instead of trying to express ranges.

**Q: My task/step didn't run. Did it fail or skip?**
- Check the dashboard or query the database to see the status:
  ```sql
  -- For tasks
  SELECT id, status, skipped_at FROM cb_t_<task_name> WHERE id = $1;
  
  -- For flow steps
  SELECT name, status, skipped_at FROM cb_s_<flow_name> WHERE flow_run_id = $1 ORDER BY started_at;
  ```
- `status = 'skipped'` means the condition was false (or field missing)
- `status = 'failed'` means the execution returned an error

**Q: Can I use flow input fields in step conditions?**
- Yes. Use the `input.` prefix directly in the step condition.
- Example: `&catbird.StepOpts{Condition: "input.amount gt 1000"}`
- Step conditions can also reference prior step outputs (`step_name.field`) and signal input (`signal.field`) when present.

**Q: Can I skip a flow step in the critical path and have downstream steps handle both cases?**
- Yes, but use `Optional[T]` in the handler parameter for the conditional dependency:
  ```go
    NewStep1Dep("charge",
      "validate",
      func(ctx context.Context, in Input, validation ValidationResult) (ChargeResult, error) {
          return ChargeResult{Amount: 100, ID: "txn-123"}, nil
      },
    &catbird.StepOpts{Condition: "validate.amount gt 0"}),
    NewStep1Dep("next",
      "charge",
      func(ctx context.Context, in Input, charge Optional[ChargeResult]) (Result, error) {
          if charge.IsSet {
              return Result{Status: "charged", TxnID: charge.Value.ID}, nil
          }
          return Result{Status: "skipped_payment"}, nil
      }, nil),
  )
  ```

## Resiliency

Catbird includes multiple resiliency layers for runtime failures. Handler-level retries are configured with `TaskOpts` (`MaxRetries`, `MinDelay`, `MaxDelay`), and external calls can be protected with `TaskOpts.CircuitBreaker` (typically created via `NewCircuitBreaker(...)`) to avoid cascading outages. In worker database paths, PostgreSQL reads/writes are retried with bounded attempts and full-jitter backoff; retries stop immediately on context cancellation or deadline expiry.

## Naming Rules

- **Queue, task, flow, and step names**: Lowercase letters, digits, and underscores only (`a-z`, `0-9`, `_`). Max 58 characters. Step names must be unique within a flow. Reserved step names: `input`, `signal`.
- **Topics/Patterns**: Letters (upper/lower), digits, dots, underscores, and hyphens (`a-z`, `A-Z`, `0-9`, `.`, `_`, `-`, plus wildcards `?`, `*`).

## Using the PostgreSQL API Directly

Catbird is built on PostgreSQL functions, so you can use the API directly from any language or tool with PostgreSQL support (psql, Python, Node.js, Ruby, etc.).

### Queues

```sql
-- Create a queue
SELECT cb_create_queue(name => 'my_queue', expires_at => null, unlogged => false);

-- Send a message
SELECT cb_send(queue => 'my_queue', payload => '{"user_id": 123, "action": "process"}'::jsonb, 
               topic => null, idempotency_key => null, deliver_at => null);

-- Send with idempotency and delayed delivery
SELECT cb_send(queue => 'my_queue', payload => '{"order_id": 789}'::jsonb,
               topic => null, idempotency_key => 'order-789', deliver_at => now() + '5 minutes'::interval);

-- Dispatch to topic-bound queues
SELECT cb_dispatch(topic => 'events.user.created', payload => '{"user_id": 456}'::jsonb,
                   idempotency_key => 'user-456-created', deliver_at => null);

-- Read messages (with 30 second visibility timeout)
SELECT * FROM cb_read(queue => 'my_queue', limit => 10, hide_for => 30);

-- Delete a message
SELECT cb_delete(queue => 'my_queue', id => 1);

-- Bind queue to topic pattern
SELECT cb_bind(queue_name => 'user_events', pattern => 'events.user.*');
SELECT cb_unbind(queue_name => 'user_events', pattern => 'events.user.*');
```

### Tasks

```sql
-- Create a task definition
SELECT cb_create_task(name => 'send_email');

-- Run a task
SELECT * FROM cb_run_task(name => 'send_email', input => '{"to": "user@example.com"}'::jsonb, 
                          concurrency_key => null, idempotency_key => null);

-- Run a task with ConcurrencyKey (prevent concurrent runs)
SELECT * FROM cb_run_task(name => 'send_email', input => '{"to": "user@example.com"}'::jsonb, 
                          concurrency_key => 'email-user123', idempotency_key => null);

-- Run a task with IdempotencyKey (ensure exactly-once)
SELECT * FROM cb_run_task(name => 'send_email', input => '{"to": "user@example.com"}'::jsonb, 
                          concurrency_key => null, idempotency_key => 'email-user123-welcome');
```

### Workflows

```sql
-- Create a flow definition
SELECT cb_create_flow(name => 'order_processing', steps => '[
  {"name": "validate"},
  {"name": "charge", "depends_on": [{"name": "validate"}]},
  {"name": "ship", "depends_on": [{"name": "charge"}]}
]'::jsonb);

-- Create a flow with conditional branching (charge only for high-risk orders)
SELECT cb_create_flow(name => 'checkout_with_conditions', steps => '[
  {"name": "assess_risk"},
  {"name": "charge", "depends_on": [{"name": "assess_risk"}], "condition": "assess_risk.risk_score gte 30"},
  {"name": "ship", "depends_on": [{"name": "charge"}]}
]'::jsonb);

-- Run a flow
SELECT * FROM cb_run_flow(name => 'order_processing', input => '{"order_id": 123}'::jsonb,
                          concurrency_key => null, idempotency_key => null);

-- Run a flow with ConcurrencyKey (prevent concurrent workflow runs)
SELECT * FROM cb_run_flow(name => 'order_processing', input => '{"order_id": 123}'::jsonb,
                          concurrency_key => 'order-123', idempotency_key => null);

-- Run a flow with IdempotencyKey (ensure exactly-once execution)
SELECT * FROM cb_run_flow(name => 'order_processing', input => '{"order_id": 123}'::jsonb,
                          concurrency_key => null, idempotency_key => 'order-123-processing');
```

### Monitoring Task and Flow Runs

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

## Dashboard

The dashboard provides a web UI for monitoring and managing queues, tasks, flows, and workers. Monitor execution status in real-time, trigger new runs, and inspect results.

### Running the Dashboard Standalone

The easiest way to run the dashboard is using the `cb` CLI tool:

```bash
# Install the CLI
go install github.com/ugent-library/catbird/cmd/cb@latest

# Set your database connection string
export CB_CONN="postgres://user:pass@localhost:5432/mydb?sslmode=disable"

# Start the dashboard (default port 8080)
cb dashboard

# Or specify a custom port
cb dashboard --port 3000
```

The dashboard will be available at `http://localhost:8080` (or your specified port).

### Embedding the Dashboard in Your Application

The dashboard is a standard `http.Handler` and can be embedded in any Go web application:

```go
import (
    "log/slog"
    "net/http"
    
    "github.com/ugent-library/catbird"
    "github.com/ugent-library/catbird/dashboard"
)

func main() {
    // Create a Catbird client
    client := catbird.New(conn)
    
    // Create the dashboard
    dash := dashboard.New(dashboard.Config{
        Client:     client,
        Log:        slog.Default(), // Optional: provide custom logger
        PathPrefix: "",              // Optional: mount at a subpath (e.g., "/admin")
    })
    
    // Option 1: Use the dashboard as your main handler
    http.ListenAndServe(":8080", dash.Handler())
    
    // Option 2: Mount at a specific path
    mux := http.NewServeMux()
    mux.Handle("/admin/catbird/", http.StripPrefix("/admin/catbird", dash.Handler()))
    mux.HandleFunc("/api/health", healthHandler)
    http.ListenAndServe(":8080", mux)
}
```

## Documentation

- **[Go API Documentation](https://pkg.go.dev/github.com/ugent-library/catbird)**: Complete reference for all public types and functions
- **[Copilot Instructions](/catbird-instructions.md)**: Architecture overview and developer guidance

## Acknowledgments

SQL code is taken from or inspired by the excellent [pgmq](https://github.com/pgmq) and [pgflow](https://github.com/pgflow-dev/pgflow) projects.
