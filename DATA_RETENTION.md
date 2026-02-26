# Data Retention + Cleanup (MVP)

This document defines a simple, low-risk approach for data cleanup in Catbird.

## Goal

Keep runtime tables small and predictable by deleting old terminal runs in batches, with an optional archive phase later.

## Scope (MVP)

### In scope
1. Cleanup of old terminal task/flow/step/map run rows.
2. Batched deletes to avoid long locks.
3. One callable SQL function and one Go API wrapper.

### Out of scope (MVP)
- Compression.
- Cross-database export.
- Full historical analytics schema.

## Retention model

Use age-based retention for terminal rows only.

Default terminal states:
- Task runs: `completed`, `failed`, `skipped`, `canceled` (if cancellation lands).
- Flow runs: `completed`, `failed`, `canceled`.
- Step/map rows: same terminal states as applicable.

Suggested default retention:
- Task runs: 30 days
- Flow runs: 30 days
- Step/map rows: 14 days

## SQL API (exact signatures)

## 1) Main cleanup entrypoint
```sql
CREATE OR REPLACE FUNCTION cb_cleanup(
    task_retention interval DEFAULT interval '30 days',
    flow_retention interval DEFAULT interval '30 days',
    step_retention interval DEFAULT interval '14 days',
    batch_size int DEFAULT 1000
)
RETURNS TABLE(
    tasks_deleted bigint,
    flows_deleted bigint,
    steps_deleted bigint,
    maps_deleted bigint
);
```

Behavior:
- Deletes only rows older than retention threshold.
- Deletes in batches per invocation.
- Returns counts for observability.

## 2) Optional per-entity helpers (if preferred)
```sql
CREATE OR REPLACE FUNCTION cb_cleanup_task_runs(name text, retention interval, batch_size int)
RETURNS bigint;

CREATE OR REPLACE FUNCTION cb_cleanup_flow_runs(name text, retention interval, batch_size int)
RETURNS TABLE(flows_deleted bigint, steps_deleted bigint, maps_deleted bigint);
```

These helpers are useful because run tables are dynamic (`cb_t_*`, `cb_f_*`, `cb_s_*`, `cb_m_*`).

## Runtime strategy

1. Enumerate task/flow definitions from metadata tables (`cb_tasks`, `cb_flows`).
2. Build dynamic table name with the same existing naming convention.
3. Delete terminal rows with `ORDER BY <terminal_at> ASC LIMIT batch_size`.
4. For flows, delete children first (`cb_m_*`, `cb_s_*`) then parent (`cb_f_*`) if not already cascaded by FK.
5. Return deletion counts.

## Scheduling

Simple options:
- Call from worker heartbeat every N heartbeats.
- Or run via scheduler/cron once every 5-15 minutes.

Recommended MVP: separate periodic call (not on every heartbeat) to keep heartbeat lightweight.

## Indexing guidance

For each dynamic table, add terminal-time indexes to keep cleanup fast:

- Task table: `(status, completed_at)`, `(status, failed_at)`, `(status, skipped_at)`.
- Flow table: `(status, completed_at)`, `(status, failed_at)`.
- Step/map tables: same pattern as applicable.

If index count feels high, use one expression index per table in a later pass.

## Go API

Add a small facade:

```go
func (c *Client) Cleanup(ctx context.Context, opts ...CleanupOpts) (*CleanupResult, error)
```

```go
type CleanupOpts struct {
    TaskRetention time.Duration
    FlowRetention time.Duration
    StepRetention time.Duration
    BatchSize     int
}

type CleanupResult struct {
    TasksDeleted int64
    FlowsDeleted int64
    StepsDeleted int64
    MapsDeleted  int64
}
```

## Safety and concurrency rules

1. Cleanup must be idempotent.
2. Never delete non-terminal rows.
3. Use small batches to avoid long transactions.
4. Keep operations lock-friendly and retry-safe.
5. If a row is concurrently updated, status predicates prevent accidental deletion.

## Archive options

You have two practical options:

1. Built-in archive tables inside the same PostgreSQL database.
2. User-managed export pipeline to your own archive system (recommended for long-term retention at scale).

## Option A: built-in archive tables (same DB)

### Archive table design

Use static archive tables with a normalized shape and enough metadata to trace source rows.

Suggested tables:
- `cb_task_runs_archive`
- `cb_flow_runs_archive`
- `cb_step_runs_archive`
- `cb_map_runs_archive`

Suggested common columns:
- `archived_at timestamptz not null default now()`
- `source_table text not null` (for example `cb_t_invoice_task`)
- `source_id bigint not null`
- `source_name text not null` (task name or flow name)
- run status/time fields (`started_at`, `completed_at`, `failed_at`, etc.)
- payload fields (`input`, `output`, `error_message`)

Recommended uniqueness guard:
- `UNIQUE (source_table, source_id)` to make archival idempotent.

### Archive move algorithm

Per entity/table, do this in small batches:

1. Select eligible terminal rows older than retention (`ORDER BY terminal_at ASC LIMIT N`).
2. Insert into archive table using `INSERT ... ON CONFLICT DO NOTHING`.
3. Delete only rows that were archived (same transaction).

This avoids data loss if the process crashes between insert and delete.

### SQL function shape (optional)

```sql
CREATE OR REPLACE FUNCTION cb_archive_and_cleanup(
    task_retention interval DEFAULT interval '30 days',
    flow_retention interval DEFAULT interval '30 days',
    step_retention interval DEFAULT interval '14 days',
    batch_size int DEFAULT 1000
)
RETURNS TABLE(
    tasks_archived bigint,
    flows_archived bigint,
    steps_archived bigint,
    maps_archived bigint,
    tasks_deleted bigint,
    flows_deleted bigint,
    steps_deleted bigint,
    maps_deleted bigint
);
```

### Pros / cons

Pros:
- Very simple operations.
- No external infra.
- Queryable with normal SQL.

Cons:
- Archive data still consumes primary DB storage.
- Large historical volume can affect maintenance and backup size.

## Option B: user-managed export to external archive

This keeps Catbird minimal while allowing teams to choose S3/data lake/warehouse/search archive.

### Recommended export contract

Expose an export query shape per entity:

```sql
SELECT ...
FROM <dynamic_run_table>
WHERE <terminal_condition>
  AND <terminal_at> < now() - $retention
  AND <terminal_at> > $watermark
ORDER BY <terminal_at>, id
LIMIT $batch_size;
```

Exporter responsibilities:
1. Read batch using watermark cursor.
2. Write rows to destination (Parquet/JSON/warehouse table/etc.).
3. Commit destination write.
4. Advance watermark in exporter state table.
5. Trigger `cb_cleanup(...)` with a lag buffer (for example cleanup data older than exporter watermark minus 24h).

### Minimal state table for exporter

```sql
CREATE TABLE IF NOT EXISTS cb_archive_export_state (
    source_name text primary key,
    last_terminal_at timestamptz,
    last_source_id bigint,
    updated_at timestamptz not null default now()
);
```

Use `(last_terminal_at, last_source_id)` as a stable resume cursor.

### Consistency model

- At-least-once export is acceptable.
- Destination should deduplicate using `(source_table, source_id)`.
- Keep cleanup retention behind export watermark to avoid premature deletion.

### Pros / cons

Pros:
- Scales better for long-term retention.
- Flexible destination and schema evolution outside core runtime.
- Smaller primary DB footprint.

Cons:
- Requires external job orchestration.
- Requires exporter observability and retry logic.

## Observability

Expose at least:
- `cleanup_runs_total`
- `cleanup_deleted_total{entity=task|flow|step|map}`
- `cleanup_duration_seconds`
- `cleanup_errors_total`
- `archive_rows_written_total` (if Option A or internal exporter exists)
- `archive_export_lag_seconds` (if Option B is used)

## Test matrix

1. Deletes only terminal rows.
2. Respects retention thresholds.
3. Respects batch size.
4. Handles dynamic task/flow tables correctly.
5. Safe under concurrent worker activity.
6. Idempotent when rerun immediately.

## Rollout plan

1. Add migration with cleanup function(s).
2. Add client wrapper.
3. Add focused integration tests.
4. Wire periodic invocation in worker/scheduler.
5. Add metrics/logging.
