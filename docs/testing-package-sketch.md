# `catbird/testing` Package Sketch

## Goal

Provide an in-process, synchronous test harness for user-defined task/flow handlers so most unit tests can run **without Docker/PostgreSQL**, while preserving the Catbird handler ergonomics (`NewTask`, `NewFlow`, `Optional[T]`, conditions, signals, map/reducer/generator behavior).

## Non-goals (v1)

- Exact SQL/runtime parity for locking, visibility timeouts, and polling internals.
- Queue/topic broker simulation (`cb_send`, `cb_read`, `cb_publish`) beyond minimal stubs.
- Performance benchmarking (this remains integration-level, Docker-backed).

## Proposed API

```go
package testing

type Harness struct { /* internal state */ }

type Option func(*config)

func NewHarness(opts ...Option) *Harness

func (h *Harness) RegisterTask(task *catbird.Task) *Harness
func (h *Harness) RegisterFlow(flow *catbird.Flow) *Harness

func (h *Harness) RunTask(ctx context.Context, taskName string, input any, opts ...RunOpt) (*TaskRunResult, error)
func (h *Harness) RunFlow(ctx context.Context, flowName string, input any, opts ...RunOpt) (*FlowRunResult, error)

func (h *Harness) SignalFlow(ctx context.Context, flowRunID int64, stepName string, signal any) error

func (h *Harness) Reset()
```

### Results

```go
type TaskRunResult struct {
    ID           int64
    Status       string // queued|started|completed|failed|skipped|canceled
    Output       json.RawMessage
    Error        error
    Attempts     int
    StartedAt    time.Time
    CompletedAt  time.Time
    FailedAt     time.Time
}

type FlowRunResult struct {
    ID           int64
    Status       string // started|completed|failed|canceled
    Output       json.RawMessage
    Error        error
    FailedStep   string
    StepRuns     []StepRunResult
    StartedAt    time.Time
    CompletedAt  time.Time
    FailedAt     time.Time
}

type StepRunResult struct {
    StepName     string
    StepType     string
    Status       string // waiting_for_dependencies|waiting_for_signal|queued|started|waiting_for_map_tasks|completed|failed|skipped|canceled
    Output       json.RawMessage
    Error        error
    Attempts     int
}
```

## Execution model

### Core behavior

- Synchronous by default: `RunTask`/`RunFlow` executes immediately in-process and returns a terminal result.
- Deterministic step scheduling: topological order with immediate activation of newly-unblocked steps.
- Optional parallel mode (later): configurable worker pool for map items only.

### Task semantics

- Execute task handler via existing reflection path (same decoding/encoding behavior as runtime code).
- Apply task condition before invocation; false => `skipped`.
- Apply retry/backoff policy in-process (with overridable sleep/clock hooks for deterministic tests).
- Execute `OnFail` with separate retry policy if the task ends `failed`.

### Flow semantics

- Build flow DAG from registered flow definition.
- Track per-step state and dependency counters in memory.
- Evaluate step conditions using the same expression language (`input.*`, `step.*`, `signal.*`).
- Support signals (`WithSignal`) with explicit `SignalFlow(...)` API.
- Map/generator/reducer:
  - Map over flow input or dependency output arrays.
  - Preserve item order in aggregated outputs.
  - Reducer consumes mapped outputs and emits folded result.
- Flow output selection follows `OutputPriority` (or inferred terminal-step order).

## Minimal extension points

- `WithNow(func() time.Time)` for deterministic timestamps.
- `WithSleep(func(context.Context, time.Duration) error)` to skip real sleeping in retries.
- `WithIDGenerator(func() int64)` for reproducible IDs.
- `WithLogger(*slog.Logger)` for test diagnostics.

## Internal design

### Suggested package layout

- `testing/harness.go` — public API + registration + lifecycle.
- `testing/task_runner.go` — task execution, retries, on-fail.
- `testing/flow_runner.go` — DAG activation, condition handling, output selection.
- `testing/condition.go` — pure-Go condition parse/evaluate mirror.
- `testing/types.go` — result/status types.

### Reuse strategy

Prefer reusing existing logic where practical:

- Reflection-based handler invocation utilities from core package.
- Shared status constants from `statuses.go`.
- Existing `Optional[T]` semantics.

Mirror SQL-only behavior in Go where needed for unit scope:

- Condition parsing/evaluation currently in SQL functions should have a Go twin.
- Keep parser semantics aligned with `cb_parse_condition` and `cb_evaluate_condition`.

## Test strategy for the harness itself

- Golden tests for condition parser parity (`expr + input => bool`).
- Table-driven flow DAG tests (branching, skips, reconvergence).
- Map/reducer/generator correctness tests (ordering and failure propagation).
- Retry/backoff tests with fake sleep and deterministic clock.
- Signal gating tests (`waiting_for_signal` until signaled).

## Phased rollout

### Phase 1 (MVP)

- `RegisterTask`, `RegisterFlow`, synchronous `RunTask`, `RunFlow`.
- Conditions + Optional dependencies.
- Map steps (flow-input and step-output variants).
- Basic retries for handler and on-fail.

### Phase 2

- Signals API + waiting semantics.
- Generator + reducer support.
- Deterministic clock/sleep/id hooks.

### Phase 3

- Optional parallel map execution mode.
- Better assertion helpers (`RequireCompleted`, `RequireStepStatus`, etc.).
- Light queue simulation helpers if needed.

## Example usage (target UX)

```go
func TestCheckoutFlow(t *testing.T) {
    h := testing.NewHarness()

    flow := catbird.NewFlow("checkout")
    flow.AddStep(catbird.NewStep("validate").Do(func(ctx context.Context, in Order) (bool, error) {
        return in.Amount > 0, nil
    }))
    flow.AddStep(catbird.NewStep("charge").
        DependsOn("validate").
        WithCondition("validate eq true").
        Do(func(ctx context.Context, in Order, ok bool) (string, error) {
            return "txn-123", nil
        }))

    h.RegisterFlow(flow)

    res, err := h.RunFlow(t.Context(), "checkout", Order{Amount: 10})
    if err != nil {
        t.Fatal(err)
    }
    if res.Status != catbird.StatusCompleted {
        t.Fatalf("expected completed, got %s", res.Status)
    }
}
```

## Open questions

- Should v1 include queue-message simulation, or remain task/flow-only?
- Should condition parsing in Go be strict-parity tested against SQL fixtures in CI?
- Should `catbird/testing` expose assertion helpers, or keep it framework-agnostic?
