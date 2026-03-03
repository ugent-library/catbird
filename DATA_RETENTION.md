# Data Retention + Cleanup

Retention is declared on each task/flow definition and enforced by `cb_gc()`,
which already runs opportunistically from the worker heartbeat and `client.GC(ctx)`.
No separate cleanup function, client method, or worker option is needed.

## DSL

```go
task := catbird.NewTask("send-email").
    RetentionPeriod(7 * 24 * time.Hour). // NULL by default = no cleanup
    Do(fn)

flow := catbird.NewFlow("order-processing").
    RetentionPeriod(90 * 24 * time.Hour).
    AddStep(...)
```

When `RetentionPeriod` is set, `cb_create_task` / `cb_create_flow` persists it as a
`retention interval` column on `cb_tasks` / `cb_flows`. `NULL` means the run
table is never cleaned up by GC.

## What cb_gc() deletes

Extended to cover terminal run rows per task/flow:

- Task runs: `completed`, `failed`, `skipped`, `canceled` older than `cb_tasks.retention`
- Flow runs: `completed`, `failed`, `canceled` older than `cb_flows.retention`;
  step/map children are deleted together with their parent flow row
- Non-terminal rows (`queued`, `started`, `waiting_*`, `canceling`) are never touched

## Indexing

`cb_create_task` / `cb_create_flow` adds a `(status, finished_at)` index to each
dynamic run table automatically, so GC scans remain fast without manual setup.
