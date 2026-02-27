# Remaining Patterns Design: GeneratorStep + Signal/Watermark Coordination

This document defines the **final implementation scope** for Catbird coordination primitives, focused on simplicity.

## Scope

### Implement now
1. **GeneratorStep**
   - For streaming/pagination/agentic discovery where item count is unknown
2. **Signal + Target Watermark coordination**
   - For simple multi-step coordination (e.g. zero-downtime index alias switch)

### Explicitly out of scope (for now)
- Generic task pools with producer lifecycle and drain filters
- Recursive task-spawning APIs
- Complex multi-source pool quiescence state machines

---

## Design Goals

- Keep Catbird operationally simple
- Keep API surface minimal and explicit
- Preserve PostgreSQL as source of truth for flow state
- Require idempotent handlers for crash/retry safety
- Support pragmatic correctness (eventual convergence), with optional strict gating via watermark waits

---

## Pattern A: GeneratorStep

## Problem it solves
Process large/unknown streams without materializing full arrays.

Examples:
- paginating database records
- content discovery loops (crawl/search/discover)
- long-running bounded ingestion loops

## Proposed API (conceptual)

```go
flow := NewFlow("discover-content").
    AddStep(NewGeneratorStep("discover").
        DependsOn("seed").
        Generator(func(ctx context.Context, seed Seed, yield func(URL) error) error {
            // user loop; yield discovered URLs
            return nil
        }).
        Handler(func(ctx context.Context, url URL) (PageSummary, error) {
            // process each yielded item
            return PageSummary{}, nil
        }, HandlerOpts{Concurrency: 8}))
```

## Runtime model

- One worker claims a generator step run
- Generator function yields items to a buffered channel
- Worker batches yielded items and inserts tasks into `cb_t_{flow}`
- Normal task workers process inserted tasks in parallel
- Step completes when:
  - generator finished, and
  - all spawned tasks are terminal (`completed` or `failed`)

## Minimal persisted state

Add step-run metadata (dynamic step table `cb_s_{flow}`):
- `generator_status` (`started`, `complete`, `failed`)
- `map_tasks_spawned` (int)
- `map_tasks_completed` (int)

## Failure model

- Generator crash/restart may re-run generator logic from start
- Therefore generator + task handler must be idempotent
- Task retries use existing Catbird retry semantics

## Extended implementation guide

### A1. API and type-system changes

1. Extend `Step` metadata with a generator descriptor:
    - `isGenerator bool`
    - `generatorFn any` (validated via reflection to expected signature)
    - `generatorHandler any` (item processor)
2. Add constructor/builder:
    - `NewGeneratorStep(name string)`
    - `.Generator(fn)`
    - `.Handler(fn, opts...)`
3. Reflection validation rules at flow registration:
    - generator signature: `func(context.Context, DepType, func(ItemType) error) error`
    - handler signature: `func(context.Context, ItemType) (ResultType, error)`
    - `ItemType` must match between generator and handler
4. Reuse existing `HandlerOpts` validation for handler execution settings.

### A2. Migration and schema updates

Add generator columns to dynamic step-run tables (`cb_s_{flow}`):

```sql
ALTER TABLE cb_s_{flow}
     ADD COLUMN generator_status text,
    ADD COLUMN map_tasks_spawned int NOT NULL DEFAULT 0,
    ADD COLUMN map_tasks_completed int NOT NULL DEFAULT 0,
     ADD COLUMN generator_error text;

ALTER TABLE cb_s_{flow}
     ADD CONSTRAINT cb_s_generator_status_chk
     CHECK (
          generator_status IS NULL
        OR generator_status IN ('started', 'complete', 'failed')
     );

CREATE INDEX cb_s_generator_started_idx
     ON cb_s_{flow}(status, generator_status)
     WHERE status IN ('created', 'started', 'aggregating');
```

Notes:
- Non-generator steps keep `generator_status = NULL`.
- Existing map/fixed steps remain unchanged.

### A3. Worker runtime algorithm

`runGeneratorStep(...)` lifecycle:

1. Claim eligible generator step run (`created`, deps satisfied) with `FOR UPDATE SKIP LOCKED`.
2. Transition:
    - `status='started'`
    - `generator_status='started'`
3. Start generator goroutine with bounded channel buffer.
4. Spawner loop:
    - read yielded items
    - batch into N items (configurable, default 100)
    - insert into task table `cb_t_{flow}`
    - increment `map_tasks_spawned` by inserted rows
5. On generator completion:
    - set `generator_status='complete'`
    - if `map_tasks_completed == map_tasks_spawned`, complete step immediately
6. On generator error:
    - set `generator_status='failed'`
    - fail step run with error message
7. Task completion hook:
    - increment `map_tasks_completed`
    - if `generator_status='complete'` and counts match, complete step

### A4. SQL function sketches

```sql
-- claim generator step run
CREATE OR REPLACE FUNCTION cb_claim_generator_step(...)
RETURNS TABLE (...) AS $$ ... $$ LANGUAGE plpgsql;

-- spawn yielded batch
CREATE OR REPLACE FUNCTION cb_spawn_generator_tasks(...)
RETURNS int AS $$ ... $$ LANGUAGE plpgsql;

-- mark generator done
CREATE OR REPLACE FUNCTION cb_generator_done(...)
RETURNS void AS $$ ... $$ LANGUAGE plpgsql;

-- increment completion and maybe finalize
CREATE OR REPLACE FUNCTION cb_generator_task_completed(...)
RETURNS boolean AS $$ ... $$ LANGUAGE plpgsql;
```

Guidance:
- Keep each transition atomic (single statement/transaction).
- Avoid joins in hot polling paths; use indexed key filters.
- Use existing Catbird status transitions and error conventions.

### A5. Failure and recovery semantics

- Generator worker crash before `complete`:
  - step can be reclaimed and generator re-run
  - users must ensure generator yields idempotent work items
- Duplicate yielded work:
  - rely on handler idempotency and/or downstream dedup keys
- Partial batch insert failure:
  - no counter increment unless insert succeeds
  - batch retry is safe

### A6. Observability

Expose per generator step-run:
- `generator_status`
- `map_tasks_spawned`
- `map_tasks_completed`
- in-flight estimate (`map_tasks_spawned - map_tasks_completed`)
- last error

### A7. Test matrix

1. **Happy path**: multiple batches, successful completion.
2. **Empty stream**: zero yielded items, step completes.
3. **Generator failure**: step fails, error surfaced.
4. **Task failure**: retry policy respected, final state correct.
5. **Crash/restart**: generator re-run does not corrupt state.
6. **Concurrency**: multiple workers, single generator claimant.
7. **Cancellation**: context cancellation exits cleanly.

---

## Pattern B: Signal + Target Watermark Coordination

## Problem it solves
Simple cross-step coordination without pool machinery.

Primary target use case:
- zero-downtime alias switch for OpenSearch/Elasticsearch-like systems
- writes are versioned (`external_gte`), alias switch is atomic

## Proposed primitives

### 1) Flow signals/state
Per flow-run key/value coordination state:
- `backfill_done` (bool)
- `switch_done` (bool)
- `target_watermark` (string/int64)
- `consumer_offset` (string/int64)
- `error` (text, optional)

### 2) Signal write
```go
stepCtx.Signal(ctx, "backfill_done", true)
stepCtx.Signal(ctx, "target_watermark", watermark)
```

### 3) Predicate wait
```go
stepCtx.WaitUntil(ctx, func(s FlowSignals) bool {
    return s.BackfillDone && s.ConsumerOffset >= s.TargetWatermark
})
```

## Canonical flow shape

```text
[create-index]
   ├─> [backfill] -----------(signals backfill_done)
   └─> [consume-changes] ----(updates consumer_offset continuously)

[switch-alias] depends on [backfill]
  1) capture target_watermark
  2) wait consumer_offset >= target_watermark
  3) atomic alias switch
  4) signal switch_done

[consume-changes] exits after switch_done + optional short post-switch drain
```

## Why this is enough in this scenario

- Both old writer and reindex consumer receive topic events (multiplexed)
- Alias switch is atomic
- Versioned writes make duplicate/out-of-order updates safe
- Watermark gate reduces stale-at-switch risk without introducing pool complexity

## Minimal SQL shape (conceptual)

```sql
CREATE TABLE cb_flow_signals (
    flow_name text NOT NULL,
    flow_run_id bigint NOT NULL,
    key text NOT NULL,
    value jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (flow_name, flow_run_id, key)
);

CREATE INDEX cb_flow_signals_run_idx
    ON cb_flow_signals(flow_name, flow_run_id);
```

## Extended implementation guide

### B1. API surface

Add to `StepContext`:

```go
Signal(ctx context.Context, key string, value any) error
GetSignal(ctx context.Context, key string, out any) (bool, error)
WaitUntil(ctx context.Context, pred func(SignalReader) (bool, error), opts ...WaitOpt) error
```

`WaitUntil` options:
- `PollInterval` (default 250ms)
- `Timeout` (optional)
- `StableChecks` (default 2)

### B2. Storage model

`cb_flow_signals` is sufficient as KV state per flow-run.

Recommended conventions:
- signal key namespace:
    - `coord.backfill_done`
    - `coord.target_watermark`
    - `coord.consumer_offset`
    - `coord.switch_done`
    - `coord.error`
- store scalar values as JSONB (`true`, number, string)

### B3. SQL functions

```sql
-- upsert signal
CREATE OR REPLACE FUNCTION cb_signal_set(
        p_flow_name text,
        p_flow_run_id bigint,
        p_key text,
        p_value jsonb
) RETURNS void AS $$
BEGIN
        INSERT INTO cb_flow_signals(flow_name, flow_run_id, key, value)
        VALUES (p_flow_name, p_flow_run_id, p_key, p_value)
        ON CONFLICT (flow_name, flow_run_id, key)
        DO UPDATE SET value = EXCLUDED.value, updated_at = now();
END;
$$ LANGUAGE plpgsql;

-- read signal
CREATE OR REPLACE FUNCTION cb_signal_get(
        p_flow_name text,
        p_flow_run_id bigint,
        p_key text
) RETURNS jsonb AS $$
DECLARE
        _value jsonb;
BEGIN
        SELECT value INTO _value
        FROM cb_flow_signals
        WHERE flow_name = p_flow_name
            AND flow_run_id = p_flow_run_id
            AND key = p_key;
        RETURN _value;
END;
$$ LANGUAGE plpgsql;
```

### B4. Canonical reindex flow implementation

#### Step 1: create-index
- create target index
- signal optional metadata (`coord.target_index`)

#### Step 2: backfill
- scan source records
- bulk index into target index with external versioning
- `Signal(coord.backfill_done, true)`

#### Step 3: consume-changes
- subscribe to topic
- for each message:
    - apply to target index with external versioning
    - update `coord.consumer_offset`
- periodically check `coord.switch_done`
- if switched, optionally drain to final watermark (or short stability window), then return

#### Step 4: switch-alias
- wait until `coord.backfill_done == true`
- capture topic head as `W` (`coord.target_watermark = W`)
- wait until `coord.consumer_offset >= W` (with `StableChecks`)
- atomically switch alias
- `Signal(coord.switch_done, true)`

### B5. Watermark semantics

Use monotonic comparable offsets:
- Kafka offset
- stream sequence number
- log position

Predicate:

$$
	ext{ready} \iff \text{consumer\_offset} \ge \text{target\_watermark}
$$

`StableChecks > 1` avoids switching on transient reads.

### B6. Failure handling

- Any step may set `coord.error` and return error.
- `WaitUntil` should abort early if `coord.error` is present.
- `switch-alias` must be idempotent (safe if retried after partial failure).
- `consume-changes` should commit offsets only after successful index write.

### B7. Test matrix

1. Signal upsert/read correctness.
2. Wait predicate timeout/cancel behavior.
3. Watermark gate correctness under concurrent updates.
4. Alias switch idempotent retry.
5. Listener stop behavior after `switch_done`.
6. Out-of-order/duplicate message safety with versioned writes.
7. End-to-end integration: backfill + listener + switch.

### B8. Operational guidance

- Start with this lightweight pattern for simplicity.
- Add strict quiescence machinery only if hard no-lag-at-cutover is required.
- Document expected temporary staleness envelope (if any) as an SLO.

---

## Combined Delivery Plan

### Milestone 1: Signal/Watermark primitives (fastest value)
- `cb_flow_signals` storage
- `Signal`, `GetSignal`, `WaitUntil`
- end-to-end reindex sample using alias switch gate

### Milestone 2: GeneratorStep
- generator metadata on step runs
- worker generator execution and spawn pipeline
- completion accounting and retries

### Milestone 3: Hardening
- observability hooks (signal transitions, generator counters)
- docs and examples
- failure-injection tests

### Milestone 4: Production checklist
- runbook for alias-switch rollback
- alerting on stalled watermark progression
- dashboard cards for generator spawn/completion counters
- chaos tests (worker restart, topic hiccups, transient OpenSearch failures)

---

## Non-goals and upgrade path

If stricter guarantees are later required (hard quiescence across multiple independent producers), add the stricter pool-drain model as an optional extension. Keep current API compatible by layering stricter primitives on top.

---

## Acceptance Criteria

1. Users can implement reindex cutover with only signal/watermark coordination primitives.
2. Users can process unknown-size discovery streams via `GeneratorStep` without array materialization.
3. No new mandatory abstraction beyond these two patterns is required for common production scenarios.
