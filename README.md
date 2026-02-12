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
- **Resiliency features**: Handler retries with backoff, optional circuit breaker protection, and PostgreSQL retry logic for transient errors.
- **Dashboard**: Web UI to trigger task/flow runs, monitor progress in real-time, and view results.

## Quick Start

### Basic Queue

```go
ctx := context.Background()
client := catbird.New(conn)

// Create a queue
err := client.CreateQueue(ctx, "my-queue")

// Create a queue that expires after 1 hour and is unlogged for better performance
err = client.CreateQueueWithOpts(ctx, "temp-queue", catbird.QueueOpts{
    ExpiresAt: time.Now().Add(1 * time.Hour),
    Unlogged:  true, // Faster but not crash-safe; useful for transient work
})

// Send a message
err = client.Send(ctx, "my-queue", map[string]any{
    "user_id": 123,
    "action":  "process",
})

// Send a message with delayed delivery (available in 5 minutes)
err = client.SendWithOpts(ctx, "my-queue", map[string]any{
    "type":    "reminder",
    "user_id": 456,
}, catbird.SendOpts{
    DeliverAt: time.Now().Add(5 * time.Minute),
})

// Send a message with idempotency key to prevent duplicates
err = client.SendWithOpts(ctx, "my-queue", map[string]any{
    "order_id": 789,
    "action":   "process",
}, catbird.SendOpts{
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
handle, err := client.RunTaskWithOpts(ctx, "process-user", userID, catbird.RunOpts{
    ConcurrencyKey: fmt.Sprintf("user-%d", userID),
})

// Flow: Prevent concurrent workflow runs
handle, err := client.RunFlowWithOpts(ctx, "order-processing", order, catbird.RunFlowOpts{
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
handle, err := client.RunTaskWithOpts(ctx, "charge-payment", payment, catbird.RunOpts{
    IdempotencyKey: fmt.Sprintf("payment-%s", payment.ID),
})

// Flow: Ensure order is processed exactly once
handle, err := client.RunFlowWithOpts(ctx, "fulfill-order", order, catbird.RunFlowOpts{
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
  h1, _ := client.RunTaskWithOpts(ctx, "task", input, RunOpts{IdempotencyKey: "key-1"})
  // h1.ID = 123
  
  // Duplicate call returns handle to same run
  h2, _ := client.RunTaskWithOpts(ctx, "task", input, RunOpts{IdempotencyKey: "key-1"})
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
err := client.CreateQueue(ctx, "user-events")
err = client.CreateQueue(ctx, "audit-log")

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
})
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
task := catbird.NewTask("send-email", func(ctx context.Context, input EmailRequest) (EmailResponse, error) {
    // Send email logic here
    return EmailResponse{SentAt: time.Now()}, nil
},
    catbird.WithConcurrency(5), // Allow up to 5 concurrent executions
    catbird.WithMaxRetries(3), // Retry 3 times
    catbird.WithBackoff(500*time.Millisecond, 10*time.Second), // Exponential backoff between 500ms and 10s
    catbird.WithCircuitBreaker(5, 30*time.Second), // Open after 5 consecutive failures
)

// Define a task with a condition (skipped when condition is false)
conditionalTask := catbird.NewTask("premium-processing", func(ctx context.Context, input ProcessRequest) (string, error) {
    return "processed", nil
},
    catbird.WithCondition("input.is_premium"), // Skipped if is_premium = false
)

// Start a worker that handles the tasks
worker, err := client.NewWorker(ctx, catbird.WithTask(task), catbird.WithTask(conditionalTask))
go worker.Start(ctx)

// Run the task
handle, err := client.RunTask(ctx, "send-email", EmailRequest{
    To:      "user@example.com",
    Subject: "Hello",
})

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

### Workflow (Multi-Step Flow)

Flow structure:
```
     validate
     /      \
  charge   check
     \      /
       ship
```

```go
// Define the flow with steps and dependencies
flow := catbird.NewFlow("order-processing",
    catbird.InitialStep("validate", func(ctx context.Context, order Order) (bool, error) {
        // Validate order
        return true, nil
    }),
    catbird.StepWithDependency("charge",
        catbird.Dependency("validate"),
        func(ctx context.Context, order Order, validated bool) (int, error) {
            // Charge payment and return amount
            return 9999, nil
        }),
    catbird.StepWithDependency("check",
        catbird.Dependency("validate"),
        func(ctx context.Context, order Order, validated bool) (bool, error) {
            // Inventory check
            return true, nil
        }),
    catbird.StepWithTwoDependencies("ship",
        catbird.Dependency("charge"),
        catbird.Dependency("check"),
        func(ctx context.Context, order Order, chargeAmount int, inStock bool) (string, error) {
            // Ship order only if charged and in stock
            return "TRK123", nil
        }),
)

// Start a worker that handles the flow
worker, err := client.NewWorker(ctx,
    catbird.WithFlow(flow),
)
go worker.Start(ctx)

// Schedule a flow to run periodically (using cron syntax)
worker, err = client.NewWorker(ctx,
    catbird.WithFlow(flow),
    catbird.WithScheduledFlow("order-processing", "0 2 * * *", nil), // Run daily at 2 AM
)
go worker.Start(ctx)

// Run the flow
handle, err := client.RunFlow(ctx, "order-processing", myOrder)

// Get combined results from all steps
var results map[string]any
err = handle.WaitForOutput(ctx, &results)
// results contains output from all steps: validate, charge, check, ship
```

### Workflow with Signals (Human-in-the-Loop)

Signals enable workflows that wait for external input before proceeding, such as approval workflows or webhooks.

```go
type ApprovalInput struct {
    ApproverID string `json:"approver_id"`
    Approved   bool   `json:"approved"`
}

// Define a flow with an approval step that requires a signal
flow := catbird.NewFlow("document_approval",
    catbird.InitialStep("submit", func(ctx context.Context, doc Document) (string, error) {
        // Submit document for review
        return doc.ID, nil
    }),
    catbird.StepWithDependencyAndSignal("approve",
        catbird.Dependency("submit"),
        func(ctx context.Context, doc Document, approval ApprovalInput, docID string) (string, error) {
            if !approval.Approved {
                return "", fmt.Errorf("approval denied by %s", approval.ApproverID)
            }
            return fmt.Sprintf("Approved by %s: %s", approval.ApproverID, docID), nil
        }),
    catbird.StepWithDependency("publish",
        catbird.Dependency("approve"),
        func(ctx context.Context, doc Document, status string) (string, error) {
            // Publish the approved document
            return "Published: " + status, nil
        }),
)

// Start worker
worker, err := client.NewWorker(ctx, catbird.WithFlow(flow))
go worker.Start(ctx)

// Run the flow
handle, err := client.RunFlow(ctx, "document_approval", myDocument)

// Later, when approval is received (e.g., from a webhook or UI):
err = client.SignalFlow(ctx, "document_approval", handle.ID, "approve", ApprovalInput{
    ApproverID: "user123",
    Approved:   true,
})

// Wait for completion
var results map[string]any
err = handle.WaitForOutput(ctx, &results)
// results["approve"] contains "Approved by user123: doc-id"
// results["publish"] contains "Published: Approved by user123: doc-id"
```

Signal variants available:
- `InitialStepWithSignal` - first step requires signal
- `StepWithDependencyAndSignal` - step with 1 dependency + signal
- `StepWithTwoDependenciesAndSignal` - step with 2 dependencies + signal
- `StepWithThreeDependenciesAndSignal` - step with 3 dependencies + signal

A step with both dependencies and a signal waits for **both** conditions: all dependencies must complete **and** the signal must be delivered before the step executes.

## Conditional Execution

Both tasks and flow steps support conditional execution via `WithCondition(expression)`. When a condition evaluates to false (or a referenced field is missing), the task/step is skipped instead of executed. This enables patterns like environment-specific processing, risk-based branching, and optional side effects.

### Basic Concepts

**How conditions work**:
- `WithCondition(expression)` - A handler option (works for both tasks and flow steps) that makes execution conditional
- If the condition evaluates to true, the handler executes normally
- If the condition evaluates to false or a referenced field is missing, the task/step is marked as `skipped`
- Skipped tasks/steps do not execute their handler function

**What conditions can reference**:
- **Tasks**: Use `input.field_name` to reference fields from the task input
- **Flow steps**: Use `step_name.field_name` to reference outputs from previous steps in the flow

**Flow-specific considerations**:
- **Key rule**: If a step can be skipped, downstream steps must use `OptionalDependency(name)` and accept `Optional[T]` parameters to handle missing outputs
- **Optional outputs**: When a conditional step is skipped, dependent steps receive `Optional[T]{IsSet: false}`
- **Reconvergence**: Multiple conditional branches can merge back together using `OptionalDependency` and `Optional[T]` checks

### Condition Expression Syntax

Condition expressions follow the format: `[prefix].[field] [operator] [value]`

The **prefix** differs based on context:
- **Tasks**: Use `input` to reference fields from the task input (e.g., `input.priority`, `input.amount`)
- **Flow steps**: Use the step name to reference that step's output (e.g., `validate.score`, `assess_risk.category`)
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
WithCondition("input.is_premium")  // task: executes if input.is_premium is true
WithCondition("not input.is_premium")  // task: executes if input.is_premium is false
WithCondition("validate")          // flow: executes if validate step output is true

// Task input or step output is a number
WithCondition("input.count ne 0")  // task: executes if input.count != 0
WithCondition("score gt 50")       // flow: executes if score step output > 50
```

**For struct inputs/outputs** (objects with multiple fields):
```go
// Task input has fields
WithCondition("input.risk_score gte 30")  // access field from task input struct

// Flow step output has fields
WithCondition("assess_risk.risk_score gte 30")  // access field from step output struct
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
    catbird.WithCondition("input.is_premium"), // Skipped if is_premium = false
)

// Run task - may be skipped based on input
client.RunTask(ctx, "premium_processing", ProcessRequest{UserID: 123, IsPremium: false})
// This task run will be skipped (is_premium = false)
```

**Task condition patterns**:
- Feature flags: `WithCondition("input.feature_enabled")`
- Environment checks: `WithCondition("input.env eq \"production\"")`
- Priority filtering: `WithCondition("input.priority eq \"high\"")`
- Value thresholds: `WithCondition("input.amount gte 100")`
- User segments: `WithCondition("input.user_tier in [\"premium\", \"enterprise\"]")`

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

// Define a flow that only audits high-risk transactions
flow := catbird.NewFlow("checkout",
    catbird.InitialStep("assess_risk", func(ctx context.Context, order Order) (RiskAssessment, error) {
        score := calculateRiskScore(order)
        return RiskAssessment{
            RiskScore: score,
            RiskLevel: levelFromScore(score),
        }, nil
    }),
    catbird.StepWithDependency("audit",
        catbird.Dependency("assess_risk"),
        func(ctx context.Context, order Order, risk RiskAssessment) (AuditLog, error) {
            // Only audit high-risk transactions
            return AuditLog{
                Timestamp: time.Now(),
                Message:   fmt.Sprintf("High-risk order %s with score %d", order.ID, risk.RiskScore),
            }, nil
        },
        catbird.WithCondition("assess_risk.risk_score gte 30")),  // Skip if risk_score < 30
    catbird.StepWithDependency("process",
        catbird.Dependency("assess_risk"),  // Safe: depends on unconditional pre-branch step
        func(ctx context.Context, order Order, risk RiskAssessment) (string, error) {
            // Process order regardless of audit execution
            return "ORDER_PROCESSED", nil
        }),
)
```

When `assess_risk` completes with `risk_score: 20`, the `audit` step is skipped. The `process` step executes normally because it depends on the unconditional `assess_risk` step, not the conditional `audit` step.

#### Pattern 2: Divergent Branching

#### Pattern 2: Divergent Branching

Different conditions lead to different execution paths. Each branch handles a specific scenario:

```go
type RiskAssessment struct {
    RiskScore int    `json:"risk_score"`
    Category  string `json:"category"`
}

// Define a flow with divergent branches (no reconvergence)
flow := catbird.NewFlow("approval_workflow",
    catbird.InitialStep("assess", func(ctx context.Context, request Request) (RiskAssessment, error) {
        score := calculateRisk(request)
        return RiskAssessment{
            RiskScore: score,
            Category:  categorize(score),
        }, nil
    }),
    // Low-risk: auto-approve (executes when category is "low")
    catbird.StepWithDependency("auto_approve",
        catbird.Dependency("assess"),
        func(ctx context.Context, req Request, assessment RiskAssessment) (string, error) {
            return fmt.Sprintf("AUTO-APPROVED: %s", req.ID), nil
        },
        catbird.WithCondition("assess.category eq \"low\"")),
    // Medium-risk: manager approval (executes when category is "medium")
    catbird.StepWithDependency("manager_review",
        catbird.Dependency("assess"),
        func(ctx context.Context, req Request, assessment RiskAssessment) (string, error) {
            return fmt.Sprintf("MANAGER-REVIEW: %s", req.ID), nil
        },
        catbird.WithCondition("assess.category eq \"medium\"")),
    // High-risk: executive approval (executes when category is "high")
    catbird.StepWithDependency("executive_review",
        catbird.Dependency("assess"),
        func(ctx context.Context, req Request, assessment RiskAssessment) (string, error) {
            return fmt.Sprintf("EXECUTIVE-REVIEW: %s", req.ID), nil
        },
        catbird.WithCondition("assess.category eq \"high\"")),
)
```

Based on the risk category, exactly one approval path executes:
- Category "low": `auto_approve` runs, others skip
- Category "medium": `manager_review` runs, others skip
- Category "high": `executive_review` runs, others skip

This is divergent branching—no reconvergence needed.

#### Pattern 3: Reconvergent Branching (Using Optional)

When conditional branches need to merge back together, use `OptionalDependency` and `Optional[T]`:

```go
flow := catbird.NewFlow("payment_processing",
    catbird.InitialStep("validate", func(ctx context.Context, order Order) (ValidationResult, error) {
        return ValidationResult{Amount: order.Amount, Valid: true}, nil
    }),
    catbird.StepWithDependency("charge",
        catbird.Dependency("validate"),
        func(ctx context.Context, order Order, validation ValidationResult) (ChargeResult, error) {
            return ChargeResult{Amount: 100, ID: "txn-123"}, nil
        },
        catbird.WithCondition("validate.amount gt 0")),  // Skip for zero-amount orders
    catbird.StepWithDependency("finalize",
        catbird.OptionalDependency("charge"),  // Required because charge is conditional
        func(ctx context.Context, order Order, charge catbird.Optional[ChargeResult]) (Result, error) {
            if charge.IsSet {
                return Result{Status: "charged", TxnID: charge.Value.ID}, nil
            }
            return Result{Status: "free_order"}, nil
        }),
)
```

The `finalize` step executes whether or not `charge` runs, checking `charge.IsSet` to handle both cases.

### Design Philosophy

✅ **Optional side effects** - Steps that enhance but aren't required
```
main_path → [Condition] → optional_logging
       ↓
       next_step  ✓ next_step depends on main_path, not optional step
```

✅ **Divergent branching** - Different conditions lead to different endpoints
```
assess → [Condition score < 50] → auto_approve
    → [Condition 50 ≤ score < 80] → manager_review
    → [Condition score ≥ 80] → executive_review
```

✅ **Reconvergent branching (explicit)** - Merge paths using OptionalDependency
```
assess → [Condition score < 50] → auto_approve ┐
                                ├─► reconcile (OptionalDependency on both)
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
flow := catbird.NewFlow("order_processing",
    catbird.InitialStep("check_stock", func(ctx context.Context, order Order) (CheckResult, error) {
        return CheckResult{Inventory: struct{...}{InStock: true, Qty: 10}}, nil
    }),
    catbird.StepWithDependency("ship",
        catbird.Dependency("check_stock"),
        func(ctx context.Context, order Order, check CheckResult) (string, error) {
            return "SHIPPED", nil
        },
        catbird.WithCondition("check_stock.inventory.qty gte 5")),
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
    catbird.WithCondition("input.user.profile.age gte 18"),
)
```

### Troubleshooting Conditions

**Q: My condition never evaluates to true. Why?**
- **Tasks**: Check that you're using `input.field_name` prefix and the field name matches JSON struct tags
- **Flows**: Check that the step name matches exactly (case-sensitive) and field names match JSON struct tags
- Confirm the operator is supported (eq, ne, gt, gte, lt, lte, in, exists, contains)
- For nested fields, use dot notation (e.g., `input.user.profile.age` or `step_name.user.profile.age`)

**Q: Can I use multiple conditions or AND/OR logic?**
- No. Only one `WithCondition()` call per task/step is supported. If you call it multiple times, only the **last one takes effect**.
- AND/OR logic is not currently supported in condition expressions.
- **Workaround**: Have the task input or upstream step compute a categorical field (e.g., "low", "medium", "high") and use that in conditions instead of trying to express ranges.

**Q: My task/step didn't run. Did it fail or skip?**
- Check the dashboard or query the database to see the status:
  ```sql
  -- For tasks
  SELECT id, status, skipped_at FROM cb_t_<task_name> WHERE id = $1;
  
  -- For flow steps
  SELECT name, status, skipped_at FROM cb_step_runs WHERE flow_run_id = $1 ORDER BY started_at;
  ```
- `status = 'skipped'` means the condition was false (or field missing)
- `status = 'failed'` means the execution returned an error

**Q: Can I use flow input fields in step conditions?**
- No. Flow step conditions reference step outputs, not flow input. 
- If you need to branch on flow input, use an initial step that processes the input and returns a value to check:
  ```go
  InitialStep("check_input", func(ctx context.Context, input MyInput) (MyOutput, error) {
      return MyOutput{ShouldProcess: input.Flag}, nil
  }),
    StepWithDependency("next",
      Dependency("check_input"),
      func(ctx context.Context, input MyInput, check MyOutput) (Result, error) {
          return Result{}, nil
      },
      WithCondition("check_input.should_process")),
  ```

**Q: Can I skip a flow step in the critical path and have downstream steps handle both cases?**
- Yes, but make the dependency explicit with `OptionalDependency` and accept `Optional[T]` in the handler:
  ```go
    StepWithDependency("charge",
      Dependency("validate"),
      func(ctx context.Context, in Input, validation ValidationResult) (ChargeResult, error) {
          return ChargeResult{Amount: 100, ID: "txn-123"}, nil
      },
      WithCondition("validation.amount gt 0")),
    StepWithDependency("next",
      OptionalDependency("charge"),
      func(ctx context.Context, in Input, charge Optional[ChargeResult]) (Result, error) {
          if charge.IsSet {
              return Result{Status: "charged", TxnID: charge.Value.ID}, nil
          }
          return Result{Status: "skipped_payment"}, nil
      },
  )
  ```

## Resiliency

Catbird includes multiple resiliency layers for transient failures. Handlers can retry with [WithMaxRetries](https://pkg.go.dev/github.com/ugent-library/catbird#WithMaxRetries) and exponential jittered backoff via [WithBackoff](https://pkg.go.dev/github.com/ugent-library/catbird#WithBackoff), and you can wrap external calls with a circuit breaker using [WithCircuitBreaker](https://pkg.go.dev/github.com/ugent-library/catbird#WithCircuitBreaker) to avoid cascading outages. On the infrastructure side, PostgreSQL operations are issued with retry logic for transient errors to keep workers making progress even if the database briefly hiccups.

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

<p align="center">
  <img src="screenshots/dashboard-flows.png" alt="Flow Visualization" width="800" />
</p>

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
