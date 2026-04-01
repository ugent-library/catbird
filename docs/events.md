# Events

SQL functions emit events via `cb_notify` on state transitions. Events flow
through Wire -- real-time, transactional, zero storage. Always on.

Payloads are lightweight JSON with just enough to identify the resource.
Consumers query for details if needed.

| Topic | Payload |
|---|---|
| `catbird.worker.started` | `{worker_id}` |
| `catbird.worker.stopped` | `{worker_id}` |
| `catbird.queue.created` | `{queue}` |
| `catbird.queue.deleted` | `{queue}` |
| `catbird.task.created` | `{task}` |
| `catbird.task.deleted` | `{task}` |
| `catbird.flow.created` | `{flow}` |
| `catbird.flow.deleted` | `{flow}` |
| `catbird.task.started` | `{task, run_id}` |
| `catbird.task.completed` | `{task, run_id}` |
| `catbird.task.failed` | `{task, run_id}` |
| `catbird.task.skipped` | `{task, run_id}` |
| `catbird.task.canceled` | `{task, run_id}` |
| `catbird.task.expired` | `{task, run_id}` |
| `catbird.flow.started` | `{flow, run_id}` |
| `catbird.flow.completed` | `{flow, run_id}` |
| `catbird.flow.failed` | `{flow, run_id}` |
| `catbird.flow.canceled` | `{flow, run_id}` |
| `catbird.flow.expired` | `{flow, run_id}` |

The `catbird.*` topic namespace is reserved; user topics should not use it.
