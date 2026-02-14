# Coordination Patterns: A Complete Guide

This document consolidates all dynamic task spawning and coordination patterns in catbird. It serves as a reference guide for choosing the right pattern for your use case.

**Related Documents:**
- [TYPED_COORDINATION.md](TYPED_COORDINATION.md) - Strong typing foundation (START HERE)
- [POOL_COORDINATION_CLARIFICATIONS.md](POOL_COORDINATION_CLARIFICATIONS.md) - Multi-source pools and ProducerStep variants
- [DYNAMIC_TASKS_EXPLORATION.md](DYNAMIC_TASKS_EXPLORATION.md) - Comparison of 4 streaming approaches
- [MAP_STEPS_DESIGN.md](MAP_STEPS_DESIGN.md) - Fixed-size array processing

---

## Core Pattern: Typed Coordination

All patterns build on **strong compile-time typing**:

```go
// Dependencies are function parameters (type-safe)
// Side effects go through StepContext (explicit)
func(
    ctx context.Context,
    stepCtx StepContext,  // Framework-provided side effects
    config IndexConfig,   // Type-safe dependency injection
    indexID IndexID,      // Another type-safe dependency
) (OutputType, error)
```

**Key principle**: The handler function signature is **fully type-checked at compile time**. Wrong dependency type = compile error, not runtime failure.

---

## Pattern Selection Matrix

| Use Case | Array Size | Status | Pattern | Reference |
|----------|-----------|--------|---------|-----------|
| Process API response items | 1-5,000 | Fixed | **MapStep** | [MAP_STEPS_DESIGN.md](MAP_STEPS_DESIGN.md) |
| Paginate through database / Stream items | Unknown size | Dynamic | **GeneratorStep** | [DYNAMIC_TASKS_EXPLORATION.md](DYNAMIC_TASKS_EXPLORATION.md#approach-1) |
| Reindex + listen changes | Multi-source | Concurrent | **ProducerStep + Pool** | [POOL_COORDINATION_CLARIFICATIONS.md](POOL_COORDINATION_CLARIFICATIONS.md) |
| Crawl website recursively | Unbounded | Recursive | **TaskSpawning API** | [DYNAMIC_TASKS_EXPLORATION.md](DYNAMIC_TASKS_EXPLORATION.md#approach-2) |
| Process streaming events | Continuous | Long-running | **ProducerStep** | [TYPED_COORDINATION.md](TYPED_COORDINATION.md) |

---
## Architecture Trade-offs: PostgreSQL vs Client Complexity

Patterns vary in how much coordination lives in PostgreSQL vs the worker/client. **Preference**: PostgreSQL-heavy patterns (thin client) when possible.

### Coordination Complexity Spectrum

#### ⭐⭐⭐ PostgreSQL-Heavy (Recommended)

**MapStep**
- **PostgreSQL**: All state tracked in `cb_t_{flow}` table; framework spawns, workers poll, SQL aggregates
- **Client/Worker**: None (fully automatic)
- **Rationale**: Single state machine in PostgreSQL; horizontal scaling is trivial
- **Crash-safety**: Native (all state in tables, already persistent)
- **Suited for**: Fixed-size arrays, 99% of use cases

**Multi-Source Pool (Diamond Pattern)**
- **PostgreSQL**: Complex multi-step coordination with pool phases (CREATING, DRAINING, COMPLETED)
- **Worker**: Calls `stepCtx.SpawnTask()` and `stepCtx.PoolStartDrain()` (framework-provided; user doesn't manage)
- **Client side**: None (coordination is SQL state machine)
- **Rationale**: Diamond pattern requires multiple spawners → complex logic best kept in SQL; pool lifecycle is deterministic
- **Crash-safety**: Native (worker failures don't corrupt pool state)
- **Suited for**: Reindex + listen, any multi-source scenario

#### ⚠️ Moderate Complexity (Worker Support Needed)

**GeneratorStep** (handles pagination, events, streaming)
- **PostgreSQL**: Task tracking in `cb_t_{flow}` (standard); extended generator metadata tracks state
- **Worker**: Framework runs handler's generator function, reads from channel, spawns tasks continuously
- **Client side**: Moderate (handler must yield to channel; framework manages buffering)
- **Rationale**: Streaming is inherently iterative; channel/yield is idiomatic Go; also handles cursor pagination directly
- **Crash-safety**: Generator state lost on restart (must be idempotent)
- **Suited for**: Cursor/page-based pagination, event streams, unbounded sizes with natural endpoints
- **Note**: CursorStep is redundant; use GeneratorStep for pagination by writing the loop yourself

#### ⚠️⚠️ Worker-Heavy (Complex State Management)

**TaskSpawning API**
- **PostgreSQL**: Pool metadata and task table (standard)
- **Worker**: Handler explicitly loops, calls `stepCtx.SpawnTask()` for each task, manages flow control
- **Client side**: Significant (handler must implement spawning logic, retry loops, error handling)
- **Rationale**: Maximum flexibility; user controls when/how/how-many to spawn
- **Crash-safety**: Handler state lost; must be idempotent (retry-safe)
- **Suited for**: Recursive spawning, complex conditional logic, rare use cases needing fine control

**ProducerStep**
- **PostgreSQL**: Pool metadata (standard)
- **Worker**: Framework manages drain/idle timeout detection, auto-calls `PoolStartDrain()` on exit
- **Client side**: Moderate (handler runs unbounded loop; framework manages timeouts, but handler must write correctly)
- **Rationale**: Event sources have no natural endpoint; worker must manage drain semantics
- **Crash-safety**: Event source must re-subscribe on restart (subscription state not persistent)
- **Suited for**: Webhook listeners, message queue subscribers, unbounded with explicit drain

---

### Recommendation Priority (by client simplicity)

1. **MapStep** ← Use by default for arrays
2. **Multi-Source Pool** ← Use when multiple sources spawn to same downstream
3. **GeneratorStep** ← Use for pagination and streaming
4. **TaskSpawning/ProducerStep** ← Use only if above patterns don't fit (rare)

**Rationale**: 
- Options 1-2 keep coordination in PostgreSQL (horizontal scaling, crash safety, testability)
- Option 3 is moderate (streaming/pagination is inherent to the problem; GeneratorStep is universal)
- Options 4+ require careful handler implementation (crash-safety, idempotency)

---
## Pattern 1: MapStep (Fixed Arrays)

**Use when**: Processing 1-5,000 items from a known array.

**Complexity**: ⭐⭐⭐ **PostgreSQL-heavy** (thin client) - **PREFERRED**
- PostgreSQL: All spawning, polling, aggregation via SQL
- Worker: None (fully automatic)
- Client: Just call `RunFlow()`

**Characteristics**:
- ✅ Automatically detected by `DependsOn[[]ItemType]()`
- ✅ Index-based ordering
- ✅ Independent retry per item
- ✅ Simple aggregation
- ✅ Crash-safe (all state in tables)
- ✅ Horizontal scaling trivial

**Example**:
```go
NewStep("process-orders").
    DependsOn[[]Order]("fetch-orders").
    WithHandler(func(ctx context.Context, order Order) (OrderResult, error) {
        return processOrder(ctx, order)
    }, WithConcurrency(10))
```

**Database**: `cb_t_{flow}` table with `task_index` for ordering.

**See**: [MAP_STEPS_DESIGN.md](MAP_STEPS_DESIGN.md)

---

## Pattern 2: GeneratorStep (Streaming Arrays & Pagination)

**Use when**: Items arrive async (pagination, streaming, unknown total count).

**Complexity**: ⚠️⚠️ **Moderate worker support** (streaming is inherent)
- PostgreSQL: Task tracking as normal (`cb_t_{flow}` + extended metadata)
- Worker: Manages generator loop, reads channel, spawns tasks
- Client: Handler must yield to channel (idiomatic Go, not too complex)

**Characteristics**:
- ✅ Universal pattern: works for cursor pagination, page-based pagination, streaming, events
- ✅ Yields items via channel at handler's pace
- ✅ Bounded memory (no materializing full array)
- ✅ Progressive discovery of item count
- ✅ Natural termination (channel close)
- ⚠️ Handler state lost on crash (must be idempotent)
- ⚠️ Handler implements the loop (pagination, fetching, etc.)

**Examples**:

*Cursor-based pagination:*
```go
NewGeneratorStep("index-records").
    DependsOn[Config]("config-step").
    WithGenerator(
        // Generator: yields items via channel
        func(ctx context.Context, config Config, yield chan<- Record) error {
            cursor := ""
            count := 0
            for {
                records, next := db.FetchPage(ctx, cursor, 1000)
                for _, r := range records {
                    select {
                    case yield <- r:
                        count++
                    case <-ctx.Done():
                        return ctx.Err()
                    }
                }
                if next == "" { 
                    break  // No more pages
                }
                cursor = next
            }
            return nil
        },
        // Task handler: processes each yielded record
        func(ctx context.Context, record Record) (IndexResult, error) {
            return searchIndex.Index(ctx, record)
        },
        WithConcurrency(10),
    )
```

**Database**: Extended generator metadata with `generator_status` tracking.

**See**: [DYNAMIC_TASKS_EXPLORATION.md - Approach 1](DYNAMIC_TASKS_EXPLORATION.md)

---

## Pattern 3: TaskSpawning API (Explicit Control)

**Use when**: Tasks spawn other tasks conditionally or dynamically based on runtime data.

**Complexity**: ⚠️⚠️ **Heavy worker support** (explicit spawning logic)
- PostgreSQL: Pool metadata and task tables (standard)
- Worker: Handler explicitly spawns; needs loop, condition logic, error handling
- Client: Significant (handler controls spawning, flow control, retry)

**Characteristics**:
- ✅ Explicit `stepCtx.SpawnTask()` calls
- ✅ Conditional spawning
- ✅ Recursive task generation
- ✅ Fine-grained control
- ⚠️ Handler state lost on crash (must be idempotent)
- ⚠️ Most verbose to implement
- ⚠️ Requires careful error handling

**Example**:
```go
NewStep("crawl-site").
    DependsOn[URL]("start-url").
    WithTaskPool("crawl-ops").
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, baseURL URL, pageURL URL) (CrawlResult, error) {
            content := fetchPage(ctx, pageURL)
            
            // Discover and spawn tasks for new links
            for _, link := range content.Links {
                if !visited.Contains(link) {
                    if _, err := stepCtx.SpawnTask(ctx, CrawlOp{URL: link}); err != nil {
                        return CrawlResult{}, err
                    }
                }
            }
            
            if err := stepCtx.PoolStartDrain(ctx); err != nil {
                return CrawlResult{}, err
            }
            
            return CrawlResult{Visited: len(content.Links)}, nil
        },
    )
```

**See**: [DYNAMIC_TASKS_EXPLORATION.md - Approach 2](DYNAMIC_TASKS_EXPLORATION.md)

---

## Pattern 4: ProducerStep (Long-Running Sources)

**Use when**: A step continuously spawns tasks for an unbounded time (listening to events, webhooks, etc.).

**Complexity**: ⚠️⚠️ **Heavy worker support** (drain/idle timeouts, auto-lifecycle)
- PostgreSQL: Pool metadata (standard)
- Worker: Manages drain/idle timeout detection, auto-calls `PoolStartDrain()` on any exit
- Client: Moderate (handler runs unbounded loop; framework manages timeouts)

**Characteristics**:
- ✅ Automatically calls `PoolStartDrain()` on exit
- ✅ Drain/idle timeout configuration
- ✅ Designed for event sources with no natural endpoint
- ✅ Integrates with multi-step pools
- ⚠️ Event source state lost on crash (must tolerate duplicate events on restart)
- ⚠️ Drain semantics require worker-level timeout management

**Example**:
```go
NewProducerStep("listen-changes").
    DependsOn[IndexID]("create-index").
    WithTaskPool("index-ops").
    WithDrainTimeout(5*time.Minute).
    WithIdleTimeout(30*time.Second).
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, indexID IndexID) (Summary, error) {
            changes := db.SubscribeChanges(ctx)
            spawned := 0
            
            for change := range changes {
                if _, err := stepCtx.SpawnTask(ctx, IndexOp{...}); err != nil {
                    return Summary{}, err
                }
                spawned++
            }
            
            // Auto-calls PoolStartDrain on return
            return Summary{Spawned: spawned}, nil
        },
    )
```

**See**: [TYPED_COORDINATION.md - ProducerStep](TYPED_COORDINATION.md)

---

## Pattern 5: Multi-Source Pool (Diamond)

**Use when**: Multiple steps spawn tasks to the same pool, and convergence needs to wait for all.

**Complexity**: ⭐⭐ **PostgreSQL-centric** (thin client) - **PREFERRED for multi-source**
- PostgreSQL: Complex pool state machine (CREATING, DRAINING, COMPLETED phases)
- Worker: Calls `stepCtx.SpawnTask()` and `stepCtx.PoolStartDrain()` (framework-provided, deterministic)
- Client: None (coordination is SQL state machine)

**Characteristics**:
- ✅ Multiple spawning steps share one pool
- ✅ Explicit phase coordination (CREATING → DRAINING → COMPLETED)
- ✅ Crash-safe (all state in tables; worker crashes don't corrupt pool)
- ✅ Deterministic (SQL state machine, not procedural)
- ✅ `PoolStartDrain()` from each spawner is idempotent
- ✅ Convergence waits via `WaitTaskPool()` (handles failures correctly)

**Example**: Reindex + Listen Pattern
```go
flow := NewFlow("reindex",
    // Phase 1: Setup
    InitialStep("create-index", func(ctx context.Context, config Config) (IndexID, error) {
        return createIndex(ctx, config)
    }),
    
    // Phase 2a: Reindex (spawner 1)
    NewStep("reindex-pages").
        DependsOn[IndexID]("create-index").
        WithTaskPool("index-ops").
        WithHandler(func(ctx context.Context, stepCtx StepContext, indexID IndexID) (Summary, error) {
            // ... page through DB, spawn tasks ...
            stepCtx.PoolStartDrain(ctx)
            return Summary{Spawned: n}, nil
        }, WithConcurrency(1)),
    
    // Phase 2b: Listen (spawner 2 - producer)
    NewProducerStep("listen-changes").
        DependsOn[IndexID]("create-index").
        WithTaskPool("index-ops").  // SAME POOL!
        WithDrainTimeout(5*time.Minute).
        WithHandler(func(ctx context.Context, stepCtx StepContext, indexID IndexID) (Summary, error) {
            // ... subscribe and spawn tasks ...
            // Auto-calls PoolStartDrain on exit
            return Summary{Spawned: m}, nil
        }),
    
    // Phase 3: Convergence (waiter)
    NewStep("switch-index").
        DependsOn[Summary]("reindex-pages").
        DependsOn[Summary]("listen-changes").
        WithTaskPool("index-ops").
        WithHandler(func(ctx context.Context, stepCtx StepContext, r1 Summary, r2 Summary) error {
            // Both spawners done, pool in DRAINING phase
            stepCtx.WaitTaskPool(ctx)  // Blocks until all tasks done
            return switchIndex(ctx)
        }),
)
```

**Pool Lifecycle**:
```
CREATED:      reindex-pages and listen-changes spawn tasks
DRAINING:     Both spawners called PoolStartDrain
COMPLETED:    All tasks done, switch-index unblocks
```

**See**: [POOL_COORDINATION_CLARIFICATIONS.md](POOL_COORDINATION_CLARIFICATIONS.md)

---

## StepContext API

All patterns use `StepContext` for side effects:

```go
type StepContext interface {
    // Metadata (read-only)
    FlowRunID() int64
    StepName() string
    FlowName() string
    
    // Pool operations (optional - only if WithTaskPool)
    SpawnTask(ctx context.Context, input any) (taskID int64, error)
    PoolStartDrain(ctx context.Context) error
    WaitTaskPool(ctx context.Context) error
    PoolStatus(ctx context.Context) (PoolStatus, error)
}
```

**Key design**:
- Dependencies → function parameters (type-safe)
- Framework services → `stepCtx` methods (explicit)
- No runtime lookups or dynamic typing

---

## Comparison: When to Use Each Pattern

**MapStep**: Order 100 API responses
```
✅ Use: Fixed array, small count, output order matters
❌ Not: Unknown count, unbounded, recursive
```

**GeneratorStep**: Index 10M database records or paginate APIs
```
✅ Use: Streaming, pagination (cursor/page), unknown count, bounded memory
❌ Not: Fixed array (use MapStep), needs framework to hide pagination logic (use GeneratorStep directly)
```

**TaskSpawning**: Recursive crawl or conditional spawning
```
✅ Use: Recursive, conditional, explicit control needed
❌ Not: Simple arrays (use MapStep), simple pagination (use GeneratorStep)
```
✅ Use: Recursive, conditional, explicit control
❌ Not: Simple batching (use MapStep), event listening (use ProducerStep)
```

**ProducerStep**: Webhook event listener
```
✅ Use: Long-running, unbounded time, event sources
❌ Not: Fixed arrays (use MapStep), pagination (use GeneratorStep)
```

**Multi-Source Pool**: Combined reindex + listen
```
✅ Use: Multiple sources, shared downstream processing, convergence
❌ Not: Single source (use simpler patterns)
```

---

## Implementation Details

### Array Detection

The framework automatically detects array types:
```go
DependsOn[[]Order]("fetch")  // → MapStep
DependsOn[OrderList]("fetch") // If OrderList == []Order → MapStep
DependsOn[Order]("fetch")     // Regular (not map)
```

### Task Spawning

All spawning goes through task pools:
```sql
-- cb_task_pool_tasks table (generic across flows)
CREATE TABLE cb_task_pool_tasks (
    id bigint PK,
    pool_name text,        -- "index-ops", "crawl-ops", etc.
    flow_run_id bigint,
    input jsonb,
    output jsonb,
    status text,           -- created, started, completed, failed
    ...
);
```

### Pool Phases

```
CREATING:   Spawning steps running
            └─ Can call SpawnTask()
            └─ Can call PoolStartDrain() (early termination)

DRAINING:   All spawners called PoolStartDrain()
            └─ Cannot spawn new tasks
            └─ Existing tasks still processing
            └─ Workers polling for tasks

COMPLETED:  All tasks done
            └─ WaitTaskPool() returns success
            └─ Flow continues

FAILED:     ≥1 task failed permanently
            └─ WaitTaskPool() returns error
```

---

## Error Handling

**Per-item failures** (MapStep, GeneratorStep, TaskSpawning):
```go
// Independent retry per item
// Doesn't affect other items
// Aggregation skips failed items (or includes error)
```

**Pool failures** (Multi-Source):
```go
// If any task fails permanently, pool fails
stepCtx.WaitTaskPool(ctx)  // Returns error
// Convergence step must handle failure
```

**Handler failures**:
```go
// Step handler error → entire step fails
// Dependent steps are skipped (or use OptionalDependency)
```

---

## Best Practices

1. **Use MapStep for fixed arrays** - Simplest, most optimized
2. **Use ProducerStep for event sources** - Designed for unbounded time
3. **Explicit PoolStartDrain()** - Signal "no more spawns", don't rely on implicit
4. **Type-safe dependencies** - Catch errors at compile time
5. **Test pool transitions** - CREATING → DRAINING → COMPLETED states
6. **Monitor task indices** - For ordering guarantee in MapStep

---

## Architecture Comparison: PostgreSQL vs Worker/Client

| Pattern | PostgreSQL Coordination | Worker Support | Client Complexity | Crash-Safe | Notes |
|---------|------------------------|-----------------|-------------------|------------|-------|
| MapStep | Full logic in SQL | None | None | Native | **USE BY DEFAULT** for arrays |
| Multi-Pool | Complex state machine | Minimal (framework) | None | Native | **USE for multi-source** |
| GeneratorStep | Standard + metadata | Moderate (channel mgmt) | Moderate | Handler recoverable | **USE for pagination & streaming**; universal pattern |
| TaskSpawning | Minimal | Heavy (explicit loop) | Significant | Handler recoverable | Maximum control, use rarely |
| ProducerStep | Metadata only | Heavy (drain/timeout) | Moderate | Event recoverable | Unbounded event sources |

**Interpretation**:
- PostgreSQL coordination: How much of the state machine is in SQL vs handler
- Worker support: How much framework code needed in worker to support pattern
- Client complexity: How much the handler needs to manage
- Crash-safe: Can we recover from worker crash without data loss

---

## Quick Reference

| Need | Pattern | API |
|------|---------|-----|
| Static array | MapStep | `DependsOn[[]T]()` + task handler |
| Pagination or streaming | GeneratorStep | `WithGenerator(generatorFn, taskFn)` - generator yields items |
| Recursive spawning | TaskSpawning | `stepCtx.SpawnTask()` |
| Event listening | ProducerStep | `NewProducerStep()` |
| Multi-source convergence | Multi-Pool | `WithTaskPool(same)` |
