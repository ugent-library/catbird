# Catbird TODO

## API Improvements

Choose one API approach and implement:

- [ ] **Option A**: Typed Generics API (see TYPED_GENERICS_API_DESIGN.md)
  - Zero reflection overhead, full compile-time type safety
  - Moderate function explosion (3-4 step constructor variants)
  - Familiar Go generics patterns
  - Recommended approach
  
- [ ] **Option B**: Reflection-based builder API (see REFLECTION_API_DESIGN.md)
  - Minimal overhead (~1μs), runtime type validation
  - No function explosion (single builder)
  - Fluent API with cached reflection

### Naming & API Cleanup

- [ ] prefix all options: NewHander(..., WithHandlerConcurrency), NewStep(..., WithStepDescription()) or use setter methods

- [ ] **Use standard "visibility timeout" terminology for queue operations**
  - Current: `hideFor` parameter is non-standard and unclear
  - Proposed: Rename to `visibility` or `visibilityTimeout` (industry standard)
  - Affects: `Read()`, `ReadPoll()`, `Hide()`, `HideMany()` methods
  - Reference: AWS SQS, Azure Service Bus, Google Cloud Tasks all use "visibility timeout"
  - Benefit: Self-documenting, searchable, familiar to developers

- [ ] **Eliminate ...WithOpts method duplication**
  - Current: Duplicate methods like `Send()` + `SendWithOpts()`, `RunTask()` + `RunTaskWithOpts()`
  - Proposed: Single method with optional `*ConfigStruct` parameter (nil for defaults)
  - Pattern: `Send(ctx, queue, payload, *SendConfig)` where `nil` uses defaults
  - Benefits: One method per operation, idiomatic Go, easier to extend
  - Examples:
    - `CreateQueue(ctx, name, *QueueConfig)` instead of `CreateQueue()` + `CreateQueueWithOpts()`
    - `Send(ctx, queue, payload, *SendConfig)` instead of `Send()` + `SendWithOpts()`
    - `RunTask(ctx, name, input, *RunConfig)` instead of variadic + `RunTaskWithOpts()`

## Features

- [ ] Versioning?
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Database coordinated scheduling (see SCHEDULING_ADVANCED.md)
- [ ] Allow multi-language/mixed execution of a flow
- [ ] Reuse queue topic bindings implementation for event triggered task execution
- [ ] Task coördination and dynamic tasks (see COORDINATION_PATTERNS.md)
- [ ] Task cancellation and early exit
- [ ] Wait for output long polling
- [ ] Description and other meta fields (part of reflection API)
- [ ] Metrics
- [ ] Schemas for task/flow input/output? (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)
- [ ] OnFail handlers

## Project Management

- [ ] Keep project focused. Queues largely duplicate pgmq.

## Client Implementations

- [ ] Full Typescript client implementation (with workers)
- [ ] Slim client implementations (no workers, facade only) for popular languages

