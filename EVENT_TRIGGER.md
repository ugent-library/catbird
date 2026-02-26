# Event-Triggered Task/Flow Runs (MVP)

This document describes the **lowest-effort MVP** for event-triggered task and flow execution in Catbird.

## Goal

Enable users to subscribe tasks/flows to event topics, while reusing existing queue/topic routing semantics.

## Non-Goals (MVP)

- No changes to hot-path routing SQL in `cb_publish`.
- No polymorphic binding target table (queue/task/flow union target).
- No new cross-system consistency protocol.
- No exactly-once guarantee end-to-end (at-least-once is acceptable for MVP).

## Core Approach

Reuse queue bindings as the trigger transport:

1. Create an internal queue for each trigger subscription.
2. Bind queue to topic pattern using existing `cb_bind`.
3. Worker polls that queue.
4. For each message, call `RunTask` or `RunFlow` using message payload as input.
5. On success, delete message from trigger queue.
6. On failure, rely on hide/retry behavior.

This keeps PostgreSQL queue routing unchanged and leverages existing queue durability/retry mechanics.

## Why this is lowest effort

- No migration required for `cb_bindings` schema.
- No additional complexity in `cb_publish` hot path.
- Reuses existing worker, queue, retry, and dedup abstractions.
- Clear rollback path: remove trigger queue + binding.

## Public API (MVP)

Add high-level helpers:

- `CreateTaskTrigger(ctx, taskName, queueName, topicPattern, opts...)`
- `CreateFlowTrigger(ctx, flowName, queueName, topicPattern, opts...)`
- `DeleteTaskTrigger(ctx, taskName, queueName, topicPattern)`
- `DeleteFlowTrigger(ctx, flowName, queueName, topicPattern)`

### Trigger options (minimal)

- `HideFor time.Duration` (default: 30s)
- `BatchSize int` (default: 10)
- `Concurrency int` (default: 1)
- `IdempotencyKeyFn func(msg Message) string` (optional)

If `IdempotencyKeyFn` is set:
- task trigger uses `RunTaskOpts.IdempotencyKey`
- flow trigger uses `RunFlowOpts.IdempotencyKey`

## Naming convention

Default internal queue names when `queueName` is empty:

- Task trigger: `evt_t_<task_name>`
- Flow trigger: `evt_f_<flow_name>`

(Uses existing queue naming constraints.)

## Worker runtime model

For each registered trigger:

1. Poll trigger queue with `Read(queue, batchSize, hideFor)`.
2. For each message:
   - Task trigger: `RunTask(taskName, payload, runOpts...)`
   - Flow trigger: `RunFlow(flowName, payload, runOpts...)`
3. If run enqueue succeeds: `Delete(queue, msg.ID)`.
4. If enqueue fails: do not delete; message becomes visible again after hide timeout.

### Delivery semantics

- Queue transport: at-least-once.
- Trigger dispatch: at-least-once.
- Effective duplicate protection possible through task/flow idempotency keys.

## Optional dedup strategy

Default idempotency key derivation (if enabled):

`evt:<queue_name>:<message_id>`

This guarantees stable rerun behavior when message delivery repeats.

## Minimal persistence (MVP)

No new DB table required.

Trigger declarations can be:
- in worker bootstrap code (simplest), or
- persisted later as `cb_task_triggers` / `cb_flow_triggers` (future enhancement).

## Failure model

- Dispatch process crash after enqueue, before delete:
  - message may be reprocessed
  - idempotency key should prevent duplicate task/flow runs
- Dispatch process crash before enqueue:
  - message retries naturally via queue visibility timeout
- Trigger queue deleted externally:
  - worker reports error and keeps retrying setup/polling (implementation choice)

## Metrics/observability (MVP)

Expose counters (log/metrics):

- `trigger_messages_read_total`
- `trigger_dispatch_success_total`
- `trigger_dispatch_failure_total`
- `trigger_messages_deleted_total`

## Test plan

1. **Task trigger happy path**
   - publish event to topic
   - task run appears and completes
2. **Flow trigger happy path**
   - publish event to topic
   - flow run appears and completes
3. **Retry behavior**
   - force dispatch failure
   - message is not deleted and retries
4. **Idempotency behavior**
   - duplicate dispatch attempts with same derived key
   - only one effective run
5. **Wildcard bindings**
   - `?` and `*` patterns trigger correctly

## Future upgrades (post-MVP)

- Persisted trigger registry tables and dashboard UI.
- Trigger-level static input templates / mapping functions.
- Backpressure controls and per-trigger rate limits.
- Dead-letter handling for permanently failing dispatch.
