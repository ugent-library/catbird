# Catbird TODO

> Priority: `!!!` high · `!!` medium · `!` low

## Reliability

- [ ] [!!] Replay failed runs — `client.RetryTaskRun(ctx, name, id)` / `client.RetryFlowRun(ctx, name, id)` re-enqueues a failed run using its original input; no new schema needed since failed rows already persist in the live tables (distinct from on-fail handlers, which run automatically — this is manual operator-initiated replay)
- [ ] [!!] Queue dead letter handling — add `dead boolean` column to message tables; when `deliveries >= max_deliveries`, `cb_read` marks the message dead and skips it permanently; `client.CreateQueue(ctx, name, WithMaxDeliveries(5))` opts in; `client.RedriveQueue(ctx, name)` resets `dead = false` and `deliveries = 0` for replay
- [ ] [!] Flow checkpointing — resume a partially completed flow run after a deploy/crash rather than restarting from scratch
- [ ] [!!!] Data cleanup — `.RetentionPeriod(d)` on task/flow DSL; persisted to `cb_tasks`/`cb_flows`; `cb_gc()` extended to delete terminal run rows older than stored retention; `(status, finished_at)` index added at table creation time (see DATA_RETENTION.md)
- [ ] [!!] Input/output schemas for task/flow validation (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)

## Performance

- [ ] [!!] Batch step completion — complete multiple map tasks in a single `cb_complete_map_tasks(ids[], outputs[])` call to reduce round-trips on large map steps
- [ ] [!!] Priority queues — per-task/flow priority column with weighted polling
- [ ] [!] Fair queueing (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] [!] Partitioned run tables — partition `cb_t_*` / `cb_f_*` by `created_at` for large-volume deployments; GC just drops old partitions (consider pg_partman for lifecycle management)
- [ ] [!] `pop()` — atomic read+delete in one operation; at-most-once delivery semantics for use cases where redelivery is never wanted
- [ ] [!!] `PG_NOTIFY` wakeup — replace polling sleep with `LISTEN`/`NOTIFY` on queue insert to reduce latency on low-volume queues; fall back to polling when no notification arrives within the poll interval

## Flow DSL

- [ ] [!!] Event-triggered task/flow runs — `worker.AddTaskTrigger(name, pattern)` / `worker.AddFlowTrigger(name, pattern)`; worker creates internal queue + binding at startup, polls it, dispatches `RunTask`/`RunFlow` per message (see EVENT_TRIGGER.md)

## Observability

- [ ] [!!!] OpenTelemetry traces — span per task/step execution with flow run ID as trace root; `worker.WithTracerProvider(...)`
- [ ] [!!] Queue metrics — `cb_queue_metrics(name)` returning queue length, visible length, oldest/newest message age; prerequisite for Prometheus endpoint
- [ ] [!!] Event emission — opt-in `worker.EmitEvents()` publishes state transitions to `catbird.event.*` topic; users bind their own queues to consume, audit, or chain into other flows (see EVENT_EMISSION.md)
- [ ] [!!!] Prometheus metrics endpoint — queue depths, step latencies, failure rates; expose via `catbird/metrics` package
- [ ] [!!] Flow run replay — re-execute a completed/failed flow run with the same input from the dashboard

## Developer Experience

- [ ] [!!!] `catbird/testing` package — in-process synchronous worker harness; no Docker needed for unit tests against user-defined task/flow handlers
- [ ] [!!] Schema drift detection on startup — warn if registered task/flow definitions drift from what's in `cb_tasks`/`cb_flows`/`cb_steps`
- [ ] [!] `cb` CLI improvements — `cb flow replay <run-id>`, `cb task retry <run-id>`, `cb queue drain <name>`
- [ ] [!!!] Review SQL function return values and use named constraints

## Client Implementations

- [ ] [!!!] Full TypeScript client implementation (with workers)
- [ ] [!!] Slim client implementations (no workers, facade only) for popular languages
- [ ] [!!] Versioning strategy for client/server compatibility
