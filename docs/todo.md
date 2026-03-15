# Catbird TODO

> Priority: `!!!` high · `!!` medium · `!` low

- [ ] [!!] MCP server

## Reliability

- [ ] [!!] Replay failed runs — `client.RetryTaskRun(ctx, name, id)` / `client.RetryFlowRun(ctx, name, id)` re-enqueues a failed run using its original input; no new schema needed since failed rows already persist in the live tables (distinct from on-fail handlers, which run automatically — this is manual operator-initiated replay).
Start reply from dashboard and tui.
- [ ] [!!] Queue dead letter handling — add `dead boolean` column to message tables; when `deliveries >= max_deliveries`, `cb_read` marks the message dead and skips it permanently; `client.CreateQueue(ctx, name, WithMaxDeliveries(5))` opts in; `client.RedriveQueue(ctx, name)` resets `dead = false` and `deliveries = 0` for replay
- [ ] [!] Flow checkpointing — resume a partially completed flow run after a deploy/crash rather than restarting from scratch
- [ ] [!!] Input/output schemas for task/flow validation (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)

## Performance

- [ ] [!!] Batch step completion — complete multiple map tasks in a single `cb_complete_map_tasks(ids[], outputs[])` call to reduce round-trips on large map steps
- [ ] [!!] Priority queues — per-task/flow priority column with weighted polling
- [ ] [!] Fair queueing (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] [!] Partitioned run tables — partition `cb_t_*` / `cb_f_*` by `created_at` for large-volume deployments; GC just drops old partitions (consider pg_partman for lifecycle management)
- [ ] [!] `pop()` — atomic read+delete in one operation; at-most-once delivery semantics for use cases where redelivery is never wanted
- [ ] [!] `peek()` — non-destructive read (no hide, no delete) for diagnostics/inspection; returns visible messages without altering delivery state
- [ ] [!] Long-lived queue consumer — `Read`/`ReadPoll` with `pg_sleep` are correct for one-shot thin-client use. For long-lived consumers (outbox readers, event forwarders), a lightweight LISTEN-based reader would benefit from the existing `cb_send` NOTIFY. Should be a simple standalone construct, not coupled to the worker.

## Flow DSL

- [ ] [!!] Event-triggered task/flow runs — `worker.AddTaskTrigger(name, pattern)` / `worker.AddFlowTrigger(name, pattern)`; worker creates internal queue + binding at startup, polls it, dispatches `RunTask`/`RunFlow` per message (see `docs/event-triggers.md`)
- [ ] Return a stream of results (for example llm streaming chat repsonses)

## Observability

- [ ] [!!!] OpenTelemetry traces — span per task/step execution with flow run ID as trace root; `worker.WithTracerProvider(...)`
- [ ] [!!] Queue/task/flow metrics — add `cb_queue_metrics(name)`, `cb_task_metrics(name)`, and `cb_flow_metrics(name)` for core depth/state/latency signals; prerequisite for Prometheus endpoint
- [ ] [!!] Event emission — opt-in `worker.EmitEvents()` publishes state transitions to `catbird.event.*` topic; users bind their own queues to consume, audit, or chain into other flows (see `docs/event-emission.md`)
- [ ] [!!!] Prometheus metrics endpoint — queue depths, step latencies, failure rates; expose via `catbird/metrics` package

## Developer Experience

- [ ] [!!!] `catbird/testing` package — in-process synchronous worker harness; no Docker needed for unit tests against user-defined task/flow handlers (see `docs/testing-package-sketch.md`)
- [ ] [!!] Schema drift detection on startup — warn if registered task/flow definitions drift from what's in `cb_tasks`/`cb_flows`/`cb_steps`
- [ ] [!] `cb` CLI improvements — `cb flow replay <run-id>`, `cb task retry <run-id>`, `cb queue drain <name>`

## Client Implementations

- [ ] [!!!] Full TypeScript client implementation (with workers)
- [ ] [!!] Slim client implementations (no workers, facade only) for popular languages
- [ ] [!!] Versioning strategy for client/server compatibility
