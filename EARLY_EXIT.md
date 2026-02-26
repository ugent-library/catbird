# Early Exit MVP (Flow)

This document defines a concrete MVP for early completion of flows with an explicit final output, while safely stopping remaining work.

## Goal

Allow a flow to finish before all branches complete when business logic has enough information to produce the final output expected from the final-step contract.

## Definition

Early exit = transition a flow run to `completed` with output supplied by an explicit early-exit signal, then cancel remaining pending/queued/running work.

## Why separate from cancellation

- Cancellation ends as `canceled` and produces no normal business output.
- Early exit ends as `completed` and **does** produce business output.

## Scope (MVP)

### In scope
1. Flow-only early exit (tasks out of scope).
2. Explicit output payload for completion.
3. Stop remaining step/map execution after early exit decision.
4. Idempotent conflict handling between early-exit and normal completion/failure.

### Out of scope (MVP)
- Automatic early-exit condition DSL in SQL.
- Multiple competing early-exit values merge semantics.
- Task-level early-exit API.

## Status and metadata model

Flow `status` remains existing `started|completed|failed` (no new terminal status needed).

Add columns to `cb_f_{flow}`:
- `early_exited boolean NOT NULL DEFAULT false`
- `early_exit_at timestamptz`
- `early_exit_step_name text`
- `early_exit_reason text`

Step/map tables add `canceled` status support (shared with cancellation MVP), because leftover work is canceled after early exit.

## SQL API (exact signatures)

```sql
CREATE OR REPLACE FUNCTION cb_early_exit_flow(
    flow_name text,
    flow_run_id bigint,
    step_name text,
    output jsonb,
    reason text DEFAULT NULL
)
RETURNS TABLE(changed boolean, status text);
```

Behavior (single transaction):
1. If flow `status='started'`, set:
   - `status='completed'`
   - `completed_at=now()`
   - `output=$4`
   - `early_exited=true`
   - `early_exit_at=now()`
   - `early_exit_step_name=$3`
   - `early_exit_reason=$5`
2. Mark remaining non-started step/map rows as `canceled`.
3. For currently started step/map rows, set cancellation-request signal via parent flow state (workers observe and stop).
4. Return `changed=true` if transition won; else `changed=false`.

Optional helper:
```sql
CREATE OR REPLACE FUNCTION cb_flow_is_still_started(
    flow_name text,
    flow_run_id bigint
)
RETURNS boolean;
```

(Useful for cheap worker-side guard checks before writing normal completion.)

## Worker behavior

## Triggering early exit

MVP option A (recommended): typed control error from step handler.

```go
type EarlyExit struct {
    Output any
    Reason string
}

func ExitFlow(output any, reason string) error
```

When step handler returns `ExitFlow(...)`:
1. Worker marshals output.
2. Calls `cb_early_exit_flow`.
3. Treats current step as done from worker perspective (no further dependent scheduling).

## Competing transitions

- If another worker already completed/failed/early-exited flow, `cb_early_exit_flow` returns `changed=false`.
- Worker treats this as benign race outcome and exits normally.

## Guarding normal step completion

`cb_complete_step` and `cb_fail_step` already gate on parent status; ensure they only mutate parent flow when parent still `started`. This prevents post-exit writes from overriding early-exit completion.

## Go API

## Optional explicit client API
```go
func (c *Client) EarlyExitFlow(
    ctx context.Context,
    flowName string,
    runID int64,
    stepName string,
    output any,
    opts ...EarlyExitOpts,
) error
```

```go
type EarlyExitOpts struct {
    Reason string
}
```

MVP can ship only handler-driven early exit first; client API may come later.

## Type/contract semantics

- Early-exit output must be compatible with flow `WaitForOutput` decode target.
- Conceptually, early-exit output stands in for final-step output.
- Document that when `early_exited=true`, final-step handler may not have run.

## Interaction with cancellation

- If flow already `canceling/canceled`, early exit must not apply (`changed=false`).
- If early exit wins first, subsequent cancel is no-op on terminal completed run.

## Idempotency and race rules

1. Early exit is idempotent for a run: first winner decides output.
2. Use status-guarded atomic update on flow row (`WHERE status='started'`).
3. Do not rely on in-memory locks; DB is source of truth.
4. Keep polling hot paths lock-free with current `SKIP LOCKED` approach.

## Observability

Expose in run views/API:
- `early_exited`
- `early_exit_at`
- `early_exit_step_name`
- `early_exit_reason`

Dashboard hints:
- badge: “Completed (early exit)”
- show deciding step and reason

## Test matrix

1. Step triggers early exit -> flow completed with expected output.
2. Parallel branch still running when early exit occurs -> branch stops/cancels, no output override.
3. Race: normal final completion vs early exit -> exactly one winner, stable output.
4. Race: fail vs early exit -> exactly one terminal state.
5. WaitForOutput decodes early-exit output correctly.
6. Duplicate early-exit attempts are no-op.

## Rollout order

1. Add metadata columns + canceled step/map status support.
2. Add `cb_early_exit_flow` SQL function.
3. Add worker control-error path and guards.
4. Add API exposure for early-exit metadata.
5. Add integration race tests.

## Relation to event triggering

- Event triggering decides when flow runs begin.
- Early exit decides that a triggered run can complete sooner with a valid final output.
- This is especially useful for event-driven fast-paths (e.g. duplicate/already-processed events).
