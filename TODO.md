# Catbird TODO

## API Improvements

### Naming & API Cleanup

- [ ] prefix all options? NewHander(..., WithHandlerConcurrency), NewStep(..., WithStepDescription())

- [ ] review sql function return values

- [ ] **Use standard "visibility timeout" terminology for queue operations**
  - Current: `hideFor` parameter is non-standard and unclear
  - Proposed: Rename to `visibility` or `visibilityTimeout` (industry standard)
  - Affects: `Read()`, `ReadPoll()`, `Hide()`, `HideMany()` methods
  - Reference: AWS SQS, Azure Service Bus, Google Cloud Tasks all use "visibility timeout"
  - Benefit: Self-documenting, searchable, familiar to developers

## Features

- [ ] Versioning?
- [ ] Fair queueing? (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] Database coordinated scheduling (see SCHEDULING_ADVANCED.md)
- [ ] Allow multi-language/mixed execution of a flow
- [ ] Reuse queue topic bindings implementation for event triggered task execution
- [ ] Task coördination and dynamic tasks (see COORDINATION_PATTERNS.md)
- [ ] Task cancellation and early exit
- [ ] Wait for output long polling
- [ ] Description and other meta fields 
- [ ] Metrics
- [ ] Schemas for task/flow input/output? (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)
- [ ] OnFail handlers
- [ ] Unlogged tasks and flows

## Project Management

- [ ] Keep project focused. Queues largely duplicate pgmq.

## Client Implementations

- [ ] Full Typescript client implementation (with workers)
- [ ] Slim client implementations (no workers, facade only) for popular languages

