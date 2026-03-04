# Event-Triggered Task/Flow Runs

Enable tasks and flows to be triggered automatically when a message is published
to a matching topic, reusing existing queue/topic routing with no changes to the
hot-path `cb_publish` SQL.

## Approach

1. Worker creates an internal queue (`cb_evt_t_<task>` / `cb_evt_f_<flow>`) and
   binds it to the topic pattern at startup.
2. Worker polls that queue alongside normal task/flow polling.
3. For each message: call `RunTask` / `RunFlow` with the message payload as input.
4. On success: delete the message. On failure: leave it; visibility timeout retries.

No new schema required. Trigger lifecycle mirrors task/flow registration — declared
on the worker, created automatically at startup, cleaned up by dropping the queue.

## Worker API

Registration mirrors `AddTask` / `AddFlow`:

```go
worker.AddTask(task)
worker.AddFlow(flow)
worker.AddTaskTrigger("send-email", "events.email.*")
worker.AddFlowTrigger("order-processing", "events.order.#",
    WithTriggerIdempotency(), // derives key from evt:<queue>:<msg_id>
)
```

Trigger options:
- `WithTriggerIdempotency()` — enables idempotency key `evt:<queue>:<msg_id>` to
  prevent duplicate runs on redelivery
- `WithTriggerConcurrency(n)` — number of concurrent dispatch goroutines (default 1)

## Delivery semantics

- At-least-once: the queue transport may redeliver on crash.
- With `WithTriggerIdempotency()`: effectively exactly-once task/flow execution.
- Without it: duplicate runs are possible on redelivery; acceptable for idempotent
  handlers.

## Failure model

| Scenario | Outcome |
|---|---|
| Crash after `RunTask` enqueue, before `Delete` | Message redelivered; idempotency key prevents duplicate run |
| Crash before `RunTask` enqueue | Message retries via visibility timeout |
| `RunTask` returns error | Message not deleted; retried after visibility timeout |
