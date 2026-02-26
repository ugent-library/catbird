# Cancellation MVP (Task + Flow)

This document defines a concrete MVP for run cancellation in Catbird.

## Goal

Allow callers (and future internal systems) to request cancellation of task and flow runs in a race-safe way, with cooperative stop behavior for in-flight handlers and immediate stop for queued/pending work.

## Definitions

- Cancellation = request a run to stop before normal completion.
- Cooperative stop = running handlers observe cancellation through context/state and return.
- Terminal cancellation = run ends in `canceled` status (not `failed`, not `completed`).

## Scope (MVP)

### In scope
1. Cancel task runs by ID.
2. Cancel flow runs by ID.
3. Prevent new step/map claims after flow enters canceling/canceled.
4. Mark queued/pending step/map rows canceled without executing.
5. Expose cancellation state via existing run-read APIs.

### Out of scope (MVP)
- Preemptive process/thread kill (handlers must cooperate).
- Cancel-by-key bulk APIs (can be added later).
- Hierarchical partial cancellation of a subset of branches.

## Status model

### Task run status
Current: `queued`, `started`, `completed`, `failed`, `skipped`

Add:
- `canceling` (optional transitional status)
- `canceled` (terminal)

Recommended MVP simplification:
- transition directly to `canceled` for queued runs
- transition to `canceling` for started runs, then worker finalizes to `canceled`

### Flow run status
Current: `started`, `completed`, `failed`

Add:
- `canceling`
- `canceled`

### Step/map status
Current step: `pending`, `queued`, `started`, `completed`, `failed`, `skipped`
Current map: `queued`, `started`, `completed`, `failed`

Add to both:
- `canceled`

## Schema changes

## Dynamic task table (`cb_t_{task}`)
Add columns:
- `cancel_requested_at timestamptz`
- `canceled_at timestamptz`
- `cancel_reason text`

Update constraints/indexes:
- include `canceling`/`canceled` in status check
- ensure terminal timestamp consistency (`canceled_at` mutually exclusive with completed/failed/skipped)
- optional index: `(status, cancel_requested_at)` for maintenance/visibility

## Dynamic flow table (`cb_f_{flow}`)
Add columns:
- `cancel_requested_at timestamptz`
- `canceled_at timestamptz`
- `cancel_reason text`

Update constraints/indexes:
- include `canceling`/`canceled` in status check
- mutual exclusivity across terminal timestamps (`completed_at`, `failed_at`, `canceled_at`)

## Dynamic step/map tables (`cb_s_{flow}`, `cb_m_{flow}`)
Add column:
- `canceled_at timestamptz`

Update status checks to include `canceled`.

## SQL API (exact signatures)

## Task
```sql
CREATE OR REPLACE FUNCTION cb_cancel_task_run(
    name text,
    run_id bigint,
    reason text DEFAULT NULL
)
RETURNS TABLE(changed boolean, final_status text);
```

Behavior:
- `queued` -> `canceled` (`canceled_at=now()`)
- `started` -> `canceling` (`cancel_requested_at=now()`)
- terminal states unchanged (`changed=false`)

```sql
CREATE OR REPLACE FUNCTION cb_finalize_task_canceled(
    name text,
    run_id bigint
)
RETURNS boolean;
```

Behavior:
- `canceling` -> `canceled` atomically by worker when handler exits due to cancellation.

## Flow
```sql
CREATE OR REPLACE FUNCTION cb_cancel_flow_run(
    name text,
    run_id bigint,
    reason text DEFAULT NULL
)
RETURNS TABLE(changed boolean, final_status text);
```

Behavior (single transaction):
1. Flow `started` -> `canceling` (or `canceled` if no started steps).
2. Mark non-started step rows (`pending`,`queued`) as `canceled`.
3. Mark non-started map rows (`queued`) as `canceled`.
4. If no step/map rows in `started`, set flow `canceled` immediately.

```sql
CREATE OR REPLACE FUNCTION cb_finalize_flow_canceled(
    name text,
    run_id bigint
)
RETURNS boolean;
```

Behavior:
- when flow is `canceling` and no started step/map rows remain, set flow `canceled`.

## Claim guards (hot path)

Update polling functions to skip canceled/canceling parents:
- `cb_poll_steps(...)`
- `cb_poll_map_tasks(...)`

Pattern:
- keep existing indexed filters
- add parent-status guard via direct key predicates (no broad joins in scan path)
- continue using `FOR UPDATE SKIP LOCKED`

## Worker behavior

## Task worker
1. After claim, check cancellation status before handler invocation.
2. Execute handler with cancellable context.
3. On context cancellation due to cancel request:
   - call `cb_finalize_task_canceled`.
4. Ignore late `complete/fail` transitions (status-guarded SQL already protects correctness).

## Flow/step/map workers
1. Before handling claimed step/map row, verify parent flow not in `canceling/canceled`.
2. If canceled, mark row `canceled` and return.
3. Running handlers should observe context cancellation and return promptly.
4. After each stop/completion/failure path, call `cb_finalize_flow_canceled` best-effort.

## Go API

## Client-level APIs
```go
func (c *Client) CancelTaskRun(ctx context.Context, taskName string, runID int64, opts ...CancelOpts) error
func (c *Client) CancelFlowRun(ctx context.Context, flowName string, runID int64, opts ...CancelOpts) error
```

```go
type CancelOpts struct {
    Reason string
}
```

## Optional errors
- `ErrAlreadyTerminal`
- `ErrNotFound`

(Or return nil for idempotent no-op; choose one style and keep it consistent.)

## Idempotency and race rules

1. Cancellation APIs must be idempotent (repeated calls safe).
2. Terminal transitions remain status-guarded (`WHERE status='started'` etc.).
3. Late worker writes after cancellation must not resurrect run state.
4. No advisory locks in runtime hot path.
5. Use existing atomic update patterns and `SKIP LOCKED` claiming.

## Interaction with retries/backoff/on-fail

- Task/step retries stop once canceled.
- OnFail handlers should not be queued from cancellation paths in MVP.
- If cancellation occurs while on-fail is already running, allow it to finish in MVP (documented behavior).

## Dashboard/API read model

Expose:
- run status `canceling|canceled`
- `cancel_requested_at`
- `canceled_at`
- `cancel_reason`

## Test matrix

1. Cancel queued task -> canceled immediately.
2. Cancel started task -> canceling then canceled when handler exits.
3. Cancel started flow with parallel branches -> queued/pending branches canceled; running branches stop cooperatively.
4. Cancel flow while map tasks active -> no new map claims; run reaches canceled.
5. Duplicate cancel requests are no-op/idempotent.
6. Race: cancel vs complete/fail; exactly one terminal state persists.
7. Wait APIs return canceled result consistently.

## Rollout order

1. Add schema/status changes in migrations.
2. Add SQL cancellation functions.
3. Add worker checks + finalize hooks.
4. Add client APIs.
5. Add integration tests.
6. Add dashboard fields.
