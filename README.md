[![Go Reference](https://pkg.go.dev/badge/github.com/ugent-library/catbird.svg)](https://pkg.go.dev/github.com/ugent-library/catbird)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.25+-blue.svg)](https://golang.org)
[![Go Report Card](https://goreportcard.com/badge/github.com/ugent-library/catbird)](https://goreportcard.com/report/github.com/ugent-library/catbird)

![CatBird](catbird-banner.svg "CatBird banner")

# Catbird

A PostgreSQL-powered distributed message queue and task execution engine. Catbird brings reliability and simplicity to background job processing by using your database as the single source of truth—no extra services to manage, just your database coordinating everything.

## Why Catbird?

- **Exactly-once execution within a timeframe**: PostgreSQL ensures each message is processed exactly once per visibility window, preventing duplicates even with multiple workers. If a worker crashes, the message becomes visible again after the configured timeout and can be retried.
- **Database as coordinator**: PostgreSQL manages message distribution, deduplication, and state. Scale workers horizontally; the database handles the rest.
- **Type-safe tasks**: Generic Go handlers with automatic JSON marshaling for inputs and outputs.
- **Flexible workflows**: Multi-step DAGs with dependency tracking and cascading data between steps.
- **Built-in persistence**: All state (queues, tasks, flows, runs) lives in PostgreSQL for auditability and recovery.
- **Worker management**: Simple worker lifecycle with graceful shutdown, configurable concurrency, and built-in retries.
- **Dashboard**: Web UI to trigger task/flow runs, monitor progress in real-time, and view results.

## Quick Start

### Basic Queue

```go
ctx := context.Background()
client := catbird.New(conn)

// Create a queue
err := client.CreateQueue(ctx, "my-queue")

// Send a message
err = client.Send(ctx, "my-queue", map[string]any{
    "user_id": 123,
    "action":  "process",
})

// Read messages (hidden from other readers for 30 seconds)
messages, err := client.Read(ctx, "my-queue", 10, 30*time.Second)
for _, msg := range messages {
    // Process message
    client.Delete(ctx, "my-queue", msg.ID)
}
```

### Task Execution

```go
// Define the task
task := catbird.NewTask("send-email", func(ctx context.Context, input EmailRequest) (EmailResponse, error) {
    // Send email logic here
    return EmailResponse{SentAt: time.Now()}, nil
}, catbird.WithRetries(3))

// Start a worker that handles the send-email task
worker, err := client.NewWorker(ctx, catbird.WithTask(task))
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
    catbird.StepWithOneDependency("charge",
        catbird.Dependency("validate"),
        func(ctx context.Context, order Order, validated bool) (int, error) {
            // Charge payment and return amount
            return 9999, nil
        }),
    catbird.StepWithOneDependency("check",
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

// Run the flow
handle, err := client.RunFlow(ctx, "order-processing", myOrder)

// Get combined results from all steps
var results map[string]any
err = handle.WaitForOutput(ctx, &results)
// results contains output from all steps: validate, charge, check, ship
```

## Documentation

- **[Go API Documentation](https://pkg.go.dev/github.com/ugent-library/catbird)**: Complete reference for all public types and functions
- **[Copilot Instructions](/catbird-instructions.md)**: Architecture overview and developer guidance

## Acknowledgments

SQL code is taken from or inspired by the excellent [pgmq](https://github.com/pgmq) and [pgflow](https://github.com/pgflow-dev/pgflow) projects.
