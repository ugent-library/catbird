# Flow Output Model (Final Decision)

This document records the final output-selection model for Catbird flows.

## Decision

Catbird uses **priority-based output selection** with explicit early-completion override.

## Go API (proposed)

### Flow builder

```go
// Declares ordered output candidates.
func (f *Flow) Output(stepNames ...string) *Flow
```

Semantics:

- `Output(...)` defines ownership order for terminal output selection.
- `Output()` (no step names) is invalid and fails at flow creation time.
- If `Output(...)` is not set, structural terminal steps are used in flow definition order.

### Early completion

```go
// Existing control signal API.
func CompleteEarly(ctx context.Context, output any, reason string) error
```

Semantics:

- Returning `CompleteEarly(...)` from a step handler writes final flow output immediately.
- Early completion supersedes priority scanning for that run.

### Validation/runtime errors

```go
var ErrNoOutputCandidate = errors.New("no output candidate produced output")
```

Creation-time validation errors include:

- empty output priority,
- unknown step references,
- duplicate step names in priority,
- missing structural terminal candidates in priority.

Runtime behavior when no candidate produced output:

- flow transitions to `failed` with `ErrNoOutputCandidate` semantics.

## Core semantics

1. Flow definition declares `output_priority` (ordered step names).
2. Flow termination is independent from output ownership and occurs when:
   - normal completion (`remaining_steps == 0`), or
   - explicit early completion.
3. Output resolution order:
   - if early completion won, its payload is the flow output;
   - otherwise, scan `output_priority` in order and pick the first candidate step that is `completed` with output.
4. If no candidate in the list produced output, flow transitions to `failed` with an explicit no-output error.
5. Current behavior: `completed` with null/empty output is valid output.

## Creation-time validation (required)

- `output_priority` is non-empty.
- All listed steps exist.
- No duplicates.
- All structural terminal candidates (graph sinks) are included in `output_priority`.

## Runtime boundary

- Topology/reference checks are creation-time.
- Condition truth values are runtime-only.
- Therefore, creation-time guarantees are structural; runtime still determines which priority candidate actually produced output in a given run.

## Multi-language compatibility

Output policy is persisted with flow definition metadata in DB (language-agnostic), so mixed-language workers execute the same ownership rules.

Minimum metadata:

- `output_priority` (`text[]`, ordered step names)

## Early completion interaction

- Early completion remains deterministic and explicit.
- It writes the canonical flow output directly and supersedes priority scanning for that run.
- Remaining in-flight work stops cooperatively.

## Rollout

### Phase 1

- Add `output_priority` metadata + validation.
- Implement terminal output selection (first completed candidate wins; fail if none).
- Keep early completion precedence.

### Phase 2

- Add optional `output_schema` metadata.
- Validate output payloads at write points.
- With schemas: null/empty validity follows schema rules.

### API surface

- Keep a single public method: `Output(stepNames ...string)`.
