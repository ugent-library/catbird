# Catbird TODO

> Priority: `!!!` high · `!!` medium · `!` low

## Reliability

- [ ] [!!] Replay failed runs — `client.RetryTaskRun(ctx, name, id)` / `client.RetryFlowRun(ctx, name, id)` re-enqueues a failed run using its original input; no new schema needed since failed rows already persist in the live tables (distinct from on-fail handlers, which run automatically — this is manual operator-initiated replay)
- [ ] [!] Flow checkpointing — resume a partially completed flow run after a deploy/crash rather than restarting from scratch
- [ ] [!!!] Data archival and cleanup (see DATA_RETENTION.md)
- [ ] [!!] Input/output schemas for task/flow validation (see https://opensource.googleblog.com/2026/01/a-json-schema-package-for-go.html)

## Performance

- [ ] [!!] Batch step completion — complete multiple map tasks in a single `cb_complete_map_tasks(ids[], outputs[])` call to reduce round-trips on large map steps
- [ ] [!!] Priority queues — per-task/flow priority column with weighted polling
- [ ] [!] Fair queueing (see https://docs.hatchet.run/blog/multi-tenant-queues)
- [ ] [!] Partitioned run tables — partition `cb_t_*` / `cb_f_*` by `created_at` for large-volume deployments; GC just drops old partitions (consider pg_partman for lifecycle management)
- [ ] [!] `pop()` — atomic read+delete in one operation; at-most-once delivery semantics for use cases where redelivery is never wanted
- [ ] [!!] `PG_NOTIFY` wakeup — replace polling sleep with `LISTEN`/`NOTIFY` on queue insert to reduce latency on low-volume queues; fall back to polling when no notification arrives within the poll interval

## Flow DSL

- [ ] [!!] Fan-out/fan-in steps — a step that spawns named sub-flows and waits for all to complete before continuing the parent flow
- [ ] [!!] Event-triggered task/flow runs via queue bindings (see EVENT_TRIGGER.md)

## Observability

- [ ] [!!!] OpenTelemetry traces — span per task/step execution with flow run ID as trace root; `worker.WithTracerProvider(...)`
- [ ] [!!] Structured events table — `cb_events` append-only log of state transitions for audit trails and replay debugging
- [ ] [!!!] Prometheus metrics endpoint — queue depths, step latencies, failure rates; expose via `catbird/metrics` package
- [ ] [!!] Flow run replay — re-execute a completed/failed flow run with the same input from the dashboard

## Developer Experience

- [ ] [!!!] `catbird/testing` package — in-process synchronous worker harness; no Docker needed for unit tests against user-defined task/flow handlers
- [ ] [!!] Schema drift detection on startup — warn if registered task/flow definitions drift from what's in `cb_tasks`/`cb_flows`/`cb_steps`
- [ ] [!] `cb` CLI improvements — `cb flow replay <run-id>`, `cb task retry <run-id>`, `cb queue drain <name>`
- [ ] [!] OpenAPI spec generation — auto-generate API docs from task/flow input/output types via reflection
- [ ] [!!] Review SQL function return values and use named constraints

## Client Implementations

- [ ] [!!!] Full TypeScript client implementation (with workers)
- [ ] [!!] Slim client implementations (no workers, facade only) for popular languages
- [ ] [!!] Versioning strategy for client/server compatibility
