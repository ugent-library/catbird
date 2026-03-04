# Event Emission

Opt-in publishing of task/flow/step state transitions to a reserved topic namespace,
using Catbird's own queue transport as the outbox. No new schema required.

## Enabling

```go
worker := client.NewWorker().
    EmitEvents() // opt-in; disabled by default
```

When enabled, the worker publishes a message to the `catbird.event.*` topic on each
state transition. Users bind their own queues to the patterns they care about and
consume them like any other Catbird queue.

## Topic conventions

```
catbird.event.task.started      payload: {task, run_id, input}
catbird.event.task.completed    payload: {task, run_id, output}
catbird.event.task.failed       payload: {task, run_id, error}
catbird.event.task.skipped      payload: {task, run_id}
catbird.event.task.canceled     payload: {task, run_id}

catbird.event.flow.started      payload: {flow, run_id, input}
catbird.event.flow.completed    payload: {flow, run_id, output}
catbird.event.flow.failed       payload: {flow, run_id, error}
catbird.event.flow.canceled     payload: {flow, run_id}

catbird.event.step.started      payload: {flow, run_id, step, step_run_id}
catbird.event.step.completed    payload: {flow, run_id, step, step_run_id, output}
catbird.event.step.failed       payload: {flow, run_id, step, step_run_id, error}
catbird.event.step.skipped      payload: {flow, run_id, step, step_run_id}
```

## Consumer example

```go
// Bind a queue to all catbird events
client.CreateQueue(ctx, "audit-log")
client.Bind(ctx, "audit-log", "catbird.event.#")

// Or just failures
client.CreateQueue(ctx, "failure-alerts")
client.Bind(ctx, "failure-alerts", "catbird.event.*.failed")
```

Consumers read the queue like any other, writing to their own audit table, triggering
alerts, feeding dashboards, or chaining into other flows via `AddTaskTrigger` /
`AddFlowTrigger`.

## Delivery semantics

- At-least-once: publish happens inside the same transaction as the state update where
  possible, so events are not lost on crash.
- No deduplication is applied to the event queue itself; consumers should be tolerant
  of occasional duplicates (e.g. idempotent inserts using `run_id` as a key).

## Notes

- Event queues are subject to the same cleanup policy as any other queue.
- The `catbird.event.*` topic namespace is reserved; user queues should not use it.
- When `EmitEvents()` is not set, no publish calls are made and no queues are created.
