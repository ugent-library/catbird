# Data Retention + Cleanup

Keep runtime tables small by periodically deleting old terminal run rows in batches.
Long-term archiving is out of scope — teams that need it should export rows to their
own storage (S3, data warehouse, etc.) before cleanup runs.

## Retention model

Age-based deletion of terminal rows only. Non-terminal rows (`queued`, `started`,
`waiting_*`, `canceling`) are never touched.

Terminal states:
- Task runs: `completed`, `failed`, `skipped`, `canceled`
- Flow runs: `completed`, `failed`, `canceled`
- Step/map rows: same terminal states as applicable

Default retention:
- Task runs: 30 days
- Flow runs: 30 days
- Step/map rows: 14 days

## SQL function

Single entrypoint; enumerates registered tasks/flows from `cb_tasks`/`cb_flows` and
cleans up their dynamic run tables (`cb_t_*`, `cb_f_*`, `cb_s_*`, `cb_m_*`).

```sql
CREATE OR REPLACE FUNCTION cb_cleanup(
    task_retention interval DEFAULT interval '30 days',
    flow_retention interval DEFAULT interval '30 days',
    step_retention interval DEFAULT interval '14 days',
    batch_size      int      DEFAULT 1000
)
RETURNS TABLE(
    tasks_deleted bigint,
    flows_deleted bigint,
    steps_deleted bigint,
    maps_deleted  bigint
);
```

Behavior:
- Deletes only rows where `status IN (terminal states) AND finished_at < now() - retention`.
- For flows, deletes step/map children before the parent flow row (or relies on FK cascade if present).
- Batched to `batch_size` rows per table per call to keep transactions short.
- Idempotent; safe to call concurrently with active workers.

## Indexing

Dynamic run tables should include a `(status, finished_at)` index created automatically
by `cb_create_task` / `cb_create_flow`, so cleanup scans remain fast without manual setup.

## Go API

Mirrors the existing `client.GC(ctx)` pattern:

```go
client.Cleanup(ctx)
client.Cleanup(ctx, WithTaskRetention(7*24*time.Hour), WithBatchSize(500))
```

```go
type CleanupResult struct {
    TasksDeleted int64
    FlowsDeleted int64
    StepsDeleted int64
    MapsDeleted  int64
}
```

## Worker integration

Opt-in periodic cleanup on the worker, disabled by default:

```go
worker := client.NewWorker(ctx).
    CleanupInterval(15 * time.Minute) // opt-in
```

Runs on a separate ticker, not on the heartbeat, to keep heartbeat lightweight.
Manual invocation via `client.Cleanup(ctx)` is always available regardless.

## External archiving

If rows need to be retained beyond the cleanup window, export them first using a
standard `SELECT` query before cleanup runs:

```sql
SELECT *
FROM cb_t_my_task
WHERE status IN ('completed', 'failed', 'skipped', 'canceled')
  AND finished_at < now() - interval '30 days'
  AND finished_at > $watermark
ORDER BY finished_at, id
LIMIT $batch_size;
```

Catbird does not manage the export destination or cursor state — that is the
responsibility of the consuming application or pipeline.
