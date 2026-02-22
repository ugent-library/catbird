# Clarifications: Pool Arguments, Step I/O, and Temporal Coordination

> **Note**: This document explores design patterns conceptually. Code examples use pseudo-generic method syntax (e.g., `DependsOn[T]()`) for clarity.

> **Updated**: This document uses **strong typing patterns** from [TYPED_COORDINATION.md](TYPED_COORDINATION.md). Dependencies are injected as function parameters; side effects and pool operations go through `StepContext`.

## Issue 1: How Pool Argument Coexists with Step Input/Output

### The Problem

In the diamond example:

```go
StepWithDependency("listen-changes", Dependency("create-index"),
    func(ctx context.Context, cfg IndexConfig, pool TaskPool) (???, error) {
        // What goes here? Output??
        changes := db.SubscribeChanges(ctx)
        for change := range changes {
            pool.Spawn(ctx, IndexOp{...})
        }
    },
    WithTaskPool(pool),
)
```

**Ambiguities:**
1. What is the return type/output of the step?
   - The step spawns tasks but doesn't process them
   - Should output be the spawned task IDs? `[]int64`?
   - Or just `Summary{TasksSpawned: 1250}`?

2. How does `pool` get injected into the function signature?
   - Current flow design: dependencies are type-checked at compile time via generics
   - `pool` is not a dependency - it's a side-effect interface
   - How to extend Go function codegen to handle it?

3. What's the semantic difference between:
   - **Dependency input**: Data from previous step output (required)
   - **Pool argument**: Reference to shared resource (optional side-effect)
   - Should they use different mechanisms?

### Proposed Solution: Explicit Step Context

Instead of injecting pool into function signature, use explicit **step context** parameter for side effects, with dependencies injected as function parameters:

```go
type StepContext interface {
    // Metadata
    FlowRunID() int64
    StepName() string
    FlowName() string
    
    // Task pool operations (optional - only available if step has pool)
    SpawnTask(ctx context.Context, input any) (taskID int64, error)
    PoolStartDrain(ctx context.Context) error
    WaitTaskPool(ctx context.Context) error
    PoolStatus(ctx context.Context) (PoolStatus, error)
}

// Step handler signature: dependencies as parameters, context as explicit param
func(ctx context.Context, stepCtx StepContext, cfg IndexConfig) (Output, error)

// Usage with reflection-based builder:
StepWithDependency("listen-changes", Dependency("create-index"),
    func(ctx context.Context, stepCtx StepContext, cfg IndexConfig) (SummaryOutput, error) {
        // cfg is dependency - injected directly with type safety
        
        // Use pool via context
        changes := db.SubscribeChanges(ctx)
        for change := range changes {
            if _, err := stepCtx.SpawnTask(ctx, IndexOp{...}); err != nil {
                return SummaryOutput{}, err
            }
        }
        
        // Return step output (independent from pool)
        return SummaryOutput{
            ListeningComplete: true,
            TasksSpawned: numSpawned,
        }, nil
    },
    WithTaskPool("index-ops"),
)
```

**How this resolves ambiguities:**

1. **Return type is clear**: Step output is the function's return value, not related to pool tasks
2. **Pool access is explicit**: `stepCtx.SpawnTask()` makes it clear this is framework-provided
3. **Dependencies are typed**: Injected as function parameters with full type safety - no `GetDependency()` calls needed
4. **Compile-time checking**: Wrong dependency type = compile error (caught before runtime)
5. **Works with all features**: Conditions (task/step opts), retries, and other handler opts are supported

**Implementation:**

```go
// at runtime, worker creates StepContext before calling handler

type stepContextImpl struct {
    conn           Conn
    flowName       string
    flowRunID      int64
    stepName       string
    taskPoolName   string  // If this step has a pool
    logger         *slog.Logger
}

func (sc *stepContextImpl) FlowRunID() int64 {
    return sc.flowRunID
}

func (sc *stepContextImpl) SpawnTask(ctx context.Context, input any) (int64, error) {
    if sc.taskPoolName == "" {
        return 0, fmt.Errorf("step %q not connected to a task pool", sc.stepName)
    }
    
    inputJSON, _ := json.Marshal(input)
    var taskID int64
    err := sc.conn.QueryRow(ctx,
        "SELECT cb_spawn_pool_task($1, $2, $3)",
        sc.taskPoolName, sc.flowRunID, inputJSON).Scan(&taskID)
    
    return taskID, err
}

func (sc *stepContextImpl) PoolStartDrain(ctx context.Context) error {
    if sc.taskPoolName == "" {
        return nil  // No pool, noop
    }
    
    _, err := sc.conn.Exec(ctx,
        "SELECT cb_start_pool_drain($1, $2)",
        sc.taskPoolName, sc.flowRunID)
    return err
}

func (sc *stepContextImpl) WaitTaskPool(ctx context.Context) error {
    if sc.taskPoolName == "" {
        return nil  // No pool, nothing to wait for
    }
    
    _, err := sc.conn.Exec(ctx,
        "SELECT cb_wait_pool($1, $2)",
        sc.taskPoolName, sc.flowRunID)
    return err
}
```

---

## Issue 2: How Does "listen-changes" Know When to Stop Listening?

### The Problem

```go
StepWithDependency("listen-changes", Dependency("create-index"),
    func(ctx context.Context, stepCtx StepContext) error {
        changes := db.SubscribeChanges(ctx)
        for change := range changes {
            stepCtx.SpawnTask(ctx, IndexOp{...})
        }
        // When does this loop exit?
        // Changes keep arriving forever...
        return nil  // Never reached?
    },
    WithTimeout(5*time.Minute),  // <-- Timeout here
)
```

**Problem**: There's a mismatch between:
- **Reindex branch**: Completes when database fully indexed (definite endpoint)
- **Listen-changes branch**: Runs indefinitely, spawning tasks as events arrive (no natural endpoint)

The `WithTimeout(5*time.Minute)` is a hack—it just kills the step after 5 minutes, but:
- ❌ What if reindexing takes 10 minutes? Listening stops too early.
- ❌ What if reindexing finishes in 1 minute? Listening continues 4 more minutes unnecessarily.
- ❌ The `switch-index` step's `WaitTaskPool()` must ensure ALL tasks are done—timeout doesn't help.

### Root Issue: Missing Coordination Mechanism

The diamond pattern has **three temporal phases** that need coordination:

```
Phase 1: Preparation
  create-index step → [creates index]

Phase 2: Dual processing  
  [reindex-existing] ────┐
                         ├─→ Spawn tasks to pool
  [listen-changes] ──────┘
  (these run in parallel)

Phase 3: Convergence
  [switch-index] → WaitTaskPool() → switch when done
```

Currently there's **no signal between phases**. Missing patterns:

1. **Phase 1→2 transition**: Automatic (dependencies complete) ✅
2. **2→3 transition**: NOT automatic! ❌
   - Both branch steps must complete BEFORE switch-index starts
   - But listen-changes never completes (unless we force timeout)
   - And switch-index's WaitTaskPool() waits for tasks, not steps

### Solution: Multi-Level Coordination Pattern

Separate **step completion** from **task pool completion**:

#### Pattern A: Explicit Convergence Step

```go
flow := catbird.NewFlow("reindex",
    InitialStep("create-index",
        func(ctx context.Context, config IndexConfig) (IndexID, error) {
            return searchIndex.Create(ctx, config)
        },
    ),
    
    // Branch 1: Reindex existing records
    NewStep("reindex-pages").
        DependsOn(catbird.Dep[IndexID]("create-index")).
        WithTaskPool("index-ops").
        WithHandler(
            func(ctx context.Context, stepCtx StepContext, cfg IndexConfig, indexID IndexID) (Summary, error) {
                cursor := ""
                for {
                    records, next := db.QueryPage(ctx, cursor, 1000)
                    for _, r := range records {
                        if _, err := stepCtx.SpawnTask(ctx, IndexOp{Type: "insert", Data: r}); err != nil {
                            return Summary{}, err
                        }
                    }
                    if next == "" { break }
                    cursor = next
                }
                
                // Signal no more spawns
                if err := stepCtx.PoolStartDrain(ctx); err != nil {
                    return Summary{}, err
                }
                
                return Summary{Indexed: count}, nil
            },
            WithConcurrency(1),
        ),
    
    // Branch 2: Listen for changes (producer step)
    NewProducerStep("listen-changes").
        DependsOn(catbird.Dep[IndexID]("create-index")).
        WithTaskPool("index-ops").
        WithHandler(
            func(ctx context.Context, stepCtx StepContext, cfg IndexConfig, indexID IndexID) (Summary, error) {
                // ProducerStep auto-calls PoolStartDrain on exit
                changes := db.SubscribeChanges(ctx)
                spawned := 0
                
                for change := range changes {
                    if _, err := stepCtx.SpawnTask(ctx, IndexOp{Type: change.Type, Data: change.Record}); err != nil {
                        return Summary{}, err
                    }
                    spawned++
                }
                
                return Summary{Spawned: spawned}, nil
            },
            WithDrainTimeout(5*time.Minute),
            WithIdleTimeout(30*time.Second),
        ),
    
    // Phase 3: Convergence
    NewStep("switch-index").
        DependsOn(catbird.Dep[Summary]("reindex-pages")).
        DependsOn(catbird.Dep[Summary]("listen-changes")).
        WithTaskPool("index-ops").
        WithHandler(
            func(ctx context.Context, stepCtx StepContext, cfg IndexConfig, reindexResult Summary, listenResult Summary) error {
                // Both spawning steps completed and called PoolStartDrain
                // Pool is in DRAINING phase
                
                // Wait for all tasks to complete
                if err := stepCtx.WaitTaskPool(ctx); err != nil {
                    return err
                }
                
                // Get final status for logging
                status, _ := stepCtx.PoolStatus(ctx)
                log.Printf("Reindexing complete: %d tasks, %d completed",
                    status.Spawned, status.Completed)
                
                // Now safe to switch
                return searchIndex.SwitchActive(ctx, indexID)
            },
        ),
)
```

**How this works:**

1. `reindex-existing`: Completes when database fully paged (natural endpoint)
2. `listen-and-spawn`: Completes after timeout or channel close (explicit endpoint)
3. Flow engine waits for BOTH steps to reach status='completed'
4. Then `switch-index` starts and calls `WaitTaskPool()`

**But this still has issues:**
- How does worker know what the "timeout duration" should be?
- If new changes arrive during wait, they should spawn (but step already completed)
- Asymmetric: one branch has natural end, other needs timeout

#### Pattern B: "Producer" Steps (Better Design)

Introduce concept of **producer step** that's explicitly designed for long-running event sources:

```go
// ProducerStep: runs until event stream ends OR drain timeout expires
// Returns a typed value (not awaiting task completion)
func NewProducerStep[Out any](
    name string,
    opts ...HandlerOpt,
) *ProducerStepBuilder[Out] {
    return &ProducerStepBuilder[Out]{
        name:          name,
        drainTimeout:  5 * time.Minute,   // Defaults
        idleTimeout:   30 * time.Second,
    }
}

type ProducerStepBuilder[Out any] struct {
    name         string
    dep1Name     string
    taskPoolName string
    drainTimeout time.Duration
    idleTimeout  time.Duration
}

func (b *ProducerStepBuilder[Out]) DependsOn[D1 any](name string) *ProducerStepBuilder[Out] {
    b.dep1Name = name
    return b
}

func (b *ProducerStepBuilder[Out]) WithTaskPool(poolName string) *ProducerStepBuilder[Out] {
    b.taskPoolName = poolName
    return b
}

func (b *ProducerStepBuilder[Out]) WithDrainTimeout(d time.Duration) *ProducerStepBuilder[Out] {
    b.drainTimeout = d
    return b
}

func (b *ProducerStepBuilder[Out]) WithIdleTimeout(d time.Duration) *ProducerStepBuilder[Out] {
    b.idleTimeout = d
    return b
}

func (b *ProducerStepBuilder[Out]) WithHandler[D1 any](
    fn func(context.Context, StepContext, D1) (Out, error),
    opts ...HandlerOpt,
) *Step {
    // Builder creates a step with producer semantics
    // Automatically calls PoolStartDrain on handler exit
    // ...
}
```

**Usage:**

```go
listenStep := NewProducerStep[Summary]("listen-changes").
    DependsOn(catbird.Dep[IndexID]("create-index")).
    WithTaskPool("index-ops").
    WithDrainTimeout(5*time.Minute).
    WithIdleTimeout(30*time.Second).
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, indexID IndexID) (Summary, error) {
            // ProducerStep auto-calls PoolStartDrain on exit
            changes := db.SubscribeChanges(ctx)
            spawned := 0
            
            for change := range changes {
                if _, err := stepCtx.SpawnTask(ctx, IndexOp{...}); err != nil {
                    return Summary{}, err
                }
                spawned++
            }
            
            return Summary{Spawned: spawned}, nil
        },
    )
```

**Semantics:**

- Step runs normally until:
  - Event stream ends (channel closes) → complete immediately, OR
  - No new events for `IdleTimeout` → complete early, OR
  - Total runtime exceeds `DrainTimeout` → forcefully complete
- On ANY exit path: `PoolStartDrain()` is called automatically
- Enables graceful shutdown and predictable pool completion

// Usage:
ProducerStep("listen-and-spawn",
    []StepDependency{Dependency("create-index")},
    func(ctx context.Context, stepCtx StepContext) (Summary, error) {
        changes := db.SubscribeChanges(ctx)
        spawned := 0
        
        for change := range changes {
            stepCtx.SpawnTask(ctx, IndexOp{Type: change.Type, Data: change.Record})
            spawned++
        }
        
        return Summary{Spawned: spawned}, nil
    },
    WithDrainTimeout(5*time.Minute),   // After deps complete, wait this long for more events
    WithIdleTimeout(10*time.Second),   // If no events for 10s, stop early
    WithTaskPool("index-ops"),
)
```

**Semantics:**

- Step runs normally until:
  - Event stream ends (channel closes) → complete immediately, OR
  - No new events for `IdleTimeout` → complete early, OR
  - Total runtime exceeds `DrainTimeout` → forcefully complete

**Implementation:**

```go
func (w *Worker) executeProducerStep(ctx context.Context, step *Step) error {
    stepRun, deps := getStepRunWithDependencies(step)
    
    // Create drain context
    stepCtx := newStepContext(stepRun, deps, step.taskPoolName)
    
    // Run step with drain behavior
    drainCtx, cancel := context.WithTimeout(ctx, step.ProducerConfig.DrainTimeout)
    defer cancel()
    
    idleTimer := time.NewTimer(step.ProducerConfig.IdleTimeout)
    defer idleTimer.Stop()
    
    // Run handler in goroutine so we can monitor activity
    handlerCtx, handlerCancel := context.WithCancel(drainCtx)
    defer handlerCancel()
    
    var output []byte
    var handlerErr error
    done := make(chan struct{})
    
    go func() {
        out, err := step.Handler.fn(handlerCtx, stepCtx)
        output, _ = json.Marshal(out)
        handlerErr = err
        close(done)
    }()
    
    // Monitor for events/idle/drain timeout
    for {
        select {
        case <-done:
            // Handler finished naturally
            if handlerErr != nil {
                return failStep(handlerErr)
            }
            return completeStep(output)
        
        case <-drainCtx.Done():
            // Drain timeout exceeded
            handlerCancel()
            <-done  // Wait for handler to finish
            
            return completeStep(output)  // Return whatever we have
        
        case <-idleTimer.C:
            // No activity for IdleTimeout
            // Check if tasks are still being spawned
            recentSpawns := stepCtx.GetRecentSpawnCount(step.ProducerConfig.IdleTimeout)
            
            if recentSpawns == 0 {
                // Nothing happened, probably done
                handlerCancel()
                <-done
                return completeStep(output)
            }
            
            // Reset idle timer
            idleTimer.Reset(step.ProducerConfig.IdleTimeout)
        
        case <-ctx.Done():
            handlerCancel()
            return ctx.Err()
        }
    }
}
```

**Advantages:**

- ✅ Clear semantics: producer runs until drain timeout OR idle or stream ends
- ✅ No timeout guessing: framework picks reasonable defaults
- ✅ Automatic early exit: if stream ends before drain timeout
- ✅ Works with dependencies: phase 1 completes, phase 2 starts producer
- ✅ Enables phase 3: once producer completes, convergence step starts

---

## Issue 3: Task Pool Completion vs Step Completion Semantics

### The Problem

Current design conflates two distinct concepts:

```go
StepWithDependencies("switch-index", Dependencies("reindex-existing", "listen-and-spawn"),
    func(ctx context.Context, stepCtx StepContext) error {
        if err := stepCtx.WaitTaskPool(ctx); err != nil {
            return err
        }
        return searchIndex.SwitchActive(ctx)
    },
    WithTaskPool("index-ops"),
)
```

**What needs to happen:**

1. ✅ Both `reindex-existing` and `listen-and-spawn` steps complete (dependency manager handles this)
2. ✅ `switch-index` step starts (dependency manager handles this)
3. ❌ But tasks spawned by steps #1 and #2 might still be running!
4. ❌ `WaitTaskPool()` blocks... but is this different from task retries or failures?
5. ❌ What if a task spawned from `reindex-existing` fails after 2 retries? Does the entire pool fail?

### Root Issue: Missing Task Pool Lifecycle

Need explicit semantics for:

```
Task Pool Lifecycle:

1. CREATED - pool exists, steps can spawn
2. SPAWNING - steps are still executing (spawning new tasks)
3. DRAINING - no more spawns, existing tasks processing
4. COMPLETED - all tasks done, no spawns
5. FAILED - one or more tasks permanently failed
```

Current design doesn't distinguish between "spawning" and "draining" phases.

### Solution: Explicit Pool Lifecycle

```sql
-- Track pool state
CREATE TABLE cb_task_pool_runs (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pool_name text NOT NULL REFERENCES cb_task_pools(name),
    flow_run_id bigint NOT NULL,
    status text NOT NULL DEFAULT 'creating',
    tasks_spawned int NOT NULL DEFAULT 0,
    tasks_completed int NOT NULL DEFAULT 0,
    tasks_failed int NOT NULL DEFAULT 0,
    drain_started_at timestamptz,      -- NEW: when spawning stopped
    drain_completed_at timestamptz,    -- NEW: when all tasks done
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Transitions:
-- creating → draining (via cb_start_pool_drain)
-- draining → completed (when all tasks done)
-- draining → failed (when task fails permanently)
```

```go
type TaskPool interface {
    // Spawn new tasks (only in spawning phase)
    Spawn(ctx context.Context, input any) (taskID int64, error)
    
    // Signal that no more tasks will be spawned (move to drain phase)
    StartDrain(ctx context.Context) error
    
    // Wait for all spawned tasks to complete (blocking)
    WaitAll(ctx context.Context) error
    
    // Get pool status
    Status(ctx context.Context) (PoolStatus, error)
}

type PoolStatus struct {
    Phase       string  // "creating", "draining", "completed", "failed"
    Spawned     int
    Completed   int
    Failed      int
    InFlight    int
}
```

**New flow structure:**

```go
flow := catbird.NewFlow("reindex",
    InitialStep("create-index", ...),
    
    // Branch 1: Spawns tasks, has natural end
    StepWithDependency("reindex-existing",
        Dependency("create-index"),
        func(ctx context.Context, stepCtx StepContext) error {
            cursor := ""
            for {
                records, next := db.QueryPage(cursor, 1000)
                for _, r := range records {
                    stepCtx.SpawnTask(ctx, IndexOp{...})
                }
                if next == "" {
                    // Natural end - signal no more spawns from this step
                    stepCtx.PoolStartDrain(ctx)
                    return nil
                }
                cursor = next
            }
        },
        WithTaskPool("index-ops"),
    ),
    
    // Branch 2: Producer that runs until timeout
    ProducerStep("listen-and-spawn",
        []StepDependency{Dependency("create-index")},
        func(ctx context.Context, stepCtx StepContext) error {
            changes := db.SubscribeChanges(ctx)
            for change := range changes {
                stepCtx.SpawnTask(ctx, IndexOp{...})
            }
            // When returned, signals no more spawns from this step
            stepCtx.PoolStartDrain(ctx)
            return nil
        },
        WithDrainTimeout(5*time.Minute),
        WithTaskPool("index-ops"),
    ),
    
    // Convergence: Waits for both steps to call StartDrain, then all tasks
    StepWithDependencies("switch-index",
        Dependencies("reindex-existing", "listen-and-spawn"),
        func(ctx context.Context, stepCtx StepContext) error {
            // Both branch steps completed, which means both called PoolStartDrain
            // Pool is now in "draining" phase
            
            // This will block until all tasks complete
            if err := stepCtx.WaitTaskPool(ctx); err != nil {
                return err  // Pool failed (a task failed permanently)
            }
            
            // Now pool is in "completed" phase
            return searchIndex.SwitchActive(ctx)
        },
        WithTaskPool("index-ops"),
    ),
)
```

**Semantics clarified:**

1. **Spawning phase** (steps 1 & 2 running):
   - Multiple steps can call `SpawnTask()`
   - Each step completes independently (when it returns)
   - Upon completion, step calls `PoolStartDrain()` (implicit or explicit)

2. **Drain phase** (all spawning steps completed):
   - No new tasks can be spawned
   - Existing tasks continue processing
   - Workers continue polling and processing

3. **Completion phase** (all tasks done):
   - Pool transitions to "completed"
   - `WaitTaskPool()` in convergence step returns success
   - Flow can continue

4. **Failure handling**:
   - If any task fails after retries → pool transitions to "failed"
   - `WaitTaskPool()` returns error
   - Flow fails

---

## Revised Example: Complete & Clear

```go
flow := catbird.NewFlow("reindex-with-changes",
    // Phase 1: Setup
    InitialStep("create-index",
        func(ctx context.Context, config IndexConfig) (IndexID, error) {
            return searchIndex.Create(ctx, config)
        }),
    
    // Phase 2a: Paginate and index existing records
    StepWithDependency("reindex-pages",
        Dependency("create-index"),
        func(ctx context.Context, stepCtx StepContext) (Summary, error) {
            // Get dependency
            configJSON, _ := stepCtx.GetDependency("create-index")
            var indexID IndexID
            json.Unmarshal(configJSON, &indexID)
            
            // Spawn tasks for all records
            cursor := ""
            spawned := 0
            for {
                records, nextCursor := db.QueryPage(ctx, cursor, 1000)
                
                for _, record := range records {
                    stepCtx.SpawnTask(ctx, IndexOp{
                        Type: "insert",
                        IndexID: indexID,
                        Record: record,
                    })
                    spawned++
                }
                
                if nextCursor == "" {
                    break
                }
                cursor = nextCursor
            }
            
            // Explicit signal: no more tasks from this step
            stepCtx.PoolStartDrain(ctx)
            
            return Summary{
                RecordsSpawned: spawned,
                Phase: "reindex",
            }, nil
        },
        WithTaskPool("index-ops"),
    ),
    
    // Phase 2b: Listen for changes and spawn tasks
    ProducerStep("listen-changes",
        []StepDependency{Dependency("create-index")},
        func(ctx context.Context, stepCtx StepContext) (Summary, error) {
            // Get dependency
            configJSON, _ := stepCtx.GetDependency("create-index")
            var indexID IndexID
            json.Unmarshal(configJSON, &indexID)
            
            // Listen until timeout or stream ends
            changes := db.SubscribeChanges(ctx)
            spawned := 0
            
            for change := range changes {
                stepCtx.SpawnTask(ctx, IndexOp{
                    Type: change.OperationType,  // insert/update/delete
                    IndexID: indexID,
                    Record: change.Record,
                })
                spawned++
            }
            
            // ProducerStep automatically calls PoolStartDrain on exit
            return Summary{
                ChangesSpawned: spawned,
                Phase: "listen",
            }, nil
        },
        WithDrainTimeout(5*time.Minute),
        WithIdleTimeout(30*time.Second),
        WithTaskPool("index-ops"),
    ),
    
    // Phase 3: Convergence - wait for all tasks then switch
    StepWithDependencies("switch-index",
        Dependencies("reindex-pages", "listen-changes"),
        func(ctx context.Context, stepCtx StepContext) error {
            // Both spawning steps have completed
            // Pool is now in DRAINING phase (no more spawns)
            
            // Wait for all spawned tasks to drain
            if err := stepCtx.WaitTaskPool(ctx); err != nil {
                return fmt.Errorf("task pool failed: %w", err)
            }
            
            // Get the pool status to log
            status, _ := stepCtx.PoolStatus(ctx)
            log.Printf("All tasks completed: %d spawned, %d completed",
                status.Spawned, status.Completed)
            
            // Now safe to switch
            return searchIndex.SwitchActive(ctx, indexID)
        },
        WithTaskPool("index-ops"),
    ),
)

// Run the flow
worker := catbird.NewWorker(ctx, conn, catbird.WithFlow(flow))
go worker.Start(ctx)

handle, _ := client.RunFlow(ctx, "reindex-with-changes", IndexConfig{
    Name: "articles_v2",
})

// Timeline:
// T=0s:   create-index step completes
// T=0s:   reindex-pages and listen-changes start (concurrent)
// T=15s:  reindex-pages finishes indexing 100K records, calls PoolStartDrain
// T=5m:   listen-changes timeout expires, step exits, auto-calls PoolStartDrain
// T=5m:   Pool now in DRAINING phase
// T=5m:   switch-index starts immediately (both deps done)
// T=5m:   switch-index calls WaitTaskPool, blocks...
// T=5m30s: Last task completes
// T=5m30s: switch-index unblocks, calls searchIndex.SwitchActive
// T=5m31s: Flow completes
```

---

## Summary of Clarifications

### 1. Pool + Step I/O Coexistence

**Solution**: Use `StepContext` interface for framework services, dependencies as function parameters:

```go
// Old (ambiguous, dynamic dependency access):
func(ctx context.Context, stepCtx StepContext) (Output, error) {
    cfg, _ := stepCtx.GetDependency("create-index")   // Runtime lookup
    stepCtx.SpawnTask(ctx, item)                       // Side effects
    err := stepCtx.WaitTaskPool(ctx)                   // Coordination
    return Output{...}, nil                            // Return value
}

// New (clear, typed dependencies):
func(ctx context.Context, stepCtx StepContext, cfg IndexConfig) (Output, error) {
    // cfg is injected with full type safety by the framework
    if _, err := stepCtx.SpawnTask(ctx, item); err != nil {  // Side effects
        return Output{}, err
    }
    err := stepCtx.WaitTaskPool(ctx)                   // Coordination
    return Output{...}, nil                            // Return value
}
```

**Benefits:**
- ✅ Dependencies are compile-time typed (no runtime lookups)
- ✅ Return type is always step output (no ambiguity)
- ✅ Framework services via context (SpawnTask, WaitTaskPool, etc.)
- ✅ Works with conditionals, signals, retries (all via context)
- ✅ Full type safety (wrong dependency type = compile error)

### 2. Producer Step Termination

**Solution**: Introduce `ProducerStep` with drain semantics:

```go
ProducerStep("listen-changes",
    []StepDependency{Dependency("create-index")},
    func(ctx context.Context, stepCtx StepContext) error {
        for event := range events {
            stepCtx.SpawnTask(ctx, item)
        }
        return nil  // Auto-calls PoolStartDrain on exit
    },
    WithDrainTimeout(5*time.Minute),
    WithIdleTimeout(30*time.Second),
)
```

**Semantics:**
- ✅ Runs until: stream ends, or drain timeout, or idle timeout (whichever first)
- ✅ Explicitly signals "no more spawns" via `PoolStartDrain` on exit
- ✅ Regular steps also call `PoolStartDrain` when they return
- ✅ Convergence step knows when to start waiting

### 3. Task Pool Lifecycle

**Solution**: Explicit pool state transitions:

```
CREATING
  ├─ Steps spawn tasks
  └─ When last step returns → PoolStartDrain

DRAINING
  ├─ No new spawns accepted
  ├─ Existing tasks process
  └─ Last task completes → COMPLETED

COMPLETED ✓
  └─ Safe to use pool result

FAILED ✗
  └─ Task failed permanently
```

**Design enables:**
- ✅ Multiple steps can spawn to same pool (without race conditions)
- ✅ Each step independently signals when it's done spawning
- ✅ Pool automatically transitions through phases
- ✅ Convergence step explicitly waits for drain completion

---

## Real-World Example: Zero-Downtime Search Index Rebuild

This section demonstrates the **diamond pattern** (Pattern 5: Multi-Source Pool) applied to a practical problem: rebuilding a search index without downtime while the application continues to accept writes.

### Problem Statement

Replace an old search index with a new one, processing **millions of existing records** while simultaneously handling **live changes** as they arrive. Both must complete before switching to the new index. The old index remains active until the switch, guaranteeing zero query downtime.

```
  inputs:
        +------ [create-index]  
       /               |         \
  config              /           \
                      /             \
         [reindex-records]      [listen-changes]
              |                      |
          spawn 10M           spawn changes
          index tasks         as they arrive
              |                      |
              \________ ____________/
                      |
                 [switch-index]
                      |
            (wait for all tasks
             from both pools,
             then update alias)
```

### Key Challenges

1. **Unknown record count**: Can't fetch all records into memory (millions exist). Must stream/paginate through the database. → **Use GeneratorStep**

2. **Can't materialize array**: Even if record count were known, storing 10M records in memory during flow execution is impractical. → **Generator's progressive yielding is essential**

3. **Concurrent spawning from two sources**: 
   - Reindex spawns tasks by reading historical records
   - Listen spawns tasks as changes arrive in real-time
   - Both spawn to the **same pool** independently (no coordination between spawners)
   - → **Requires Multi-Source Pool with explicit drain coordination**

4. **Stop listening when reindex completes** (Critical Challenge):
   - The listener must keep accepting changes during reindex
   - But must **exit gracefully** once reindex finishes and drain is signaled
   - Can't be "pull-based" (listener controls when to exit); must be "push-based" (framework signals when to stop)
   - → **ProducerStep with explicit drain timeout**

5. **Convergence waits for all sources**: The final step cannot proceed until:
   - Reindex finished spawning (called `PoolStartDrain`)
   - Listener finished spawning (called `PoolStartDrain` or timeout)
   - **All spawned tasks completed** (hundreds of thousands of index operations)
   - → **Multi-Source Pool lifecycle (CREATING → DRAINING → COMPLETED)**

### Design: The Diamond Flow

```go
import (
    "time"
    "catbird"
)

type IndexConfig struct {
    OldIndex string
    NewIndex string
    BatchSize int
}

type Change struct {
    ID int64
    Data json.RawMessage
}

type IndexResult struct {
    IndexedID int64
    Success bool
}

flow := catbird.NewFlow("reindex",
    // ============ Phase 1: Setup ============
    catbird.InitialStep("create-index",
        func(ctx context.Context, cfg IndexConfig) (IndexID, error) {
            // Create new empty index (e.g., Elasticsearch, Meilisearch, etc.)
            newIndex := searchEngine.CreateIndex(ctx, cfg.NewIndex)
            return IndexID{Name: cfg.NewIndex, CreatedAt: time.Now()}, nil
        },
    ),

    // ============ Phase 2a: Reindex Spawner ============
    // Reads existing records from primary DB using pagination
    // Spawns indexed tasks for each record
    catbird.NewGeneratorStep("reindex-records").
        catbird."DependsOn(catbird.Dep[IndexID]("create-index").
        catbird.WithTaskPool("index-ops").
        catbird.WithGenerator(
            // GENERATOR: Stream through records (millions)
            // Must not load all into memory → yields progressively
            func(ctx context.Context, indexID IndexID, yield chan<- Change) error {
                cursor := ""
                batchSize := 1000
                totalSpawned := 0
                
                for {
                    // Fetch next batch (cursor-based pagination)
                    records, nextCursor, err := db.QueryPageByCursor(
                        ctx, 
                        "SELECT id, data FROM records", 
                        cursor, 
                        batchSize,
                    )
                    if err != nil {
                        return fmt.Errorf("pagination error: %w", err)
                    }
                    
                    // Yield each record as a task
                    for _, rec := range records {
                        select {
                        case yield <- Change{ID: rec.ID, Data: rec.Data}:
                            totalSpawned++
                        case <-ctx.Done():
                            return ctx.Err()
                        }
                    }
                    
                    // Check if more pages exist
                    if nextCursor == "" {
                        break  // Exhausted all records
                    }
                    cursor = nextCursor
                }
                
                // Logger at end of reindex phase
                log.Printf("Reindex spawned %d tasks", totalSpawned)
                return nil
            },
            // TASK HANDLER: Process each record
            // Runs concurrently (many workers)
            func(ctx context.Context, change Change) (IndexResult, error) {
                result, err := searchEngine.Index(ctx, indexID, change)
                if err != nil {
                    return IndexResult{IndexedID: change.ID, Success: false}, err
                }
                return IndexResult{IndexedID: change.ID, Success: true}, nil
            },
            catbird.WithConcurrency(50),  // Process 50 records in parallel
        ),

    // ============ Phase 2b: Listen Spawner (Producer) ============
    // Subscribes to change stream (e.g., PostgreSQL LISTEN, Kafka, etc.)
    // Spawns tasks for each arriving change
    // Must exit **gracefully** when reindex is done, even if changes keep arriving
    catbird.NewProducerStep("listen-changes").
        catbird."DependsOn(catbird.Dep[IndexID]("create-index").
        catbird.WithTaskPool("index-ops").  // SAME POOL as reindex
        catbird.WithDrainTimeout(5 * time.Minute).  // Max time to wait for tasks
        catbird.WithHandler(
            func(ctx context.Context, stepCtx catbird.StepContext, indexID IndexID) (Summary, error) {
                changesChan := make(chan Change, 100)
                errChan := make(chan error, 1)
                spawned := 0

                // Start listener goroutine
                go func() {
                    if err := db.ListenChanges(ctx, changesChan); err != nil {
                        errChan <- err
                    }
                }()

                listenCtx, cancel := context.WithCancel(ctx)
                defer cancel()

            ListenLoop:
                for {
                    select {
                    // New change arrived
                    case change, ok := <-changesChan:
                        if !ok {
                            // Channel closed, listener done
                            break ListenLoop
                        }
                        
                        // Spawn task for this change
                        _, err := stepCtx.SpawnTask(listenCtx, change)
                        if err != nil {
                            return Summary{}, fmt.Errorf("spawn failed: %w", err)
                        }
                        spawned++

                    // Either:
                    // (a) stepCtx got PoolStartDrain signal from reindex spawner
                    //     → ctx will be cancelled, triggering context expiry below
                    // (b) Explicit timeout (drain timeout reached)
                    case <-ctx.Done():
                        log.Printf("Drain signal received (reindex done or timeout), stopping listener")
                        cancel()  // Stop accepting new changes
                        break ListenLoop

                    case err := <-errChan:
                        return Summary{}, err
                    }
                }

                // Important: ProducerStep automatically calls PoolStartDrain on exit
                // (framework handles this, no need to call explicitly)
                
                log.Printf("Listener spawned %d tasks", spawned)
                return Summary{TasksSpawned: spawned}, nil
            },
        ),

    // ============ Phase 3: Convergence ============
    // Both spawners have finished. Pool is in DRAINING phase.
    // Wait for ALL tasks from both sources to complete.
    catbird.NewStep("switch-index").
        catbird."DependsOn(catbird.Dep[IndexResult]("reindex-records").  // awaits reindex completion
        catbird."DependsOn(catbird.Dep[Summary]("listen-changes").  // awaits listener completion
        catbird.WithTaskPool("index-ops").
        catbird.WithHandler(
            func(ctx context.Context, stepCtx catbird.StepContext) error {
                // Both spawners have called PoolStartDrain (or timeout)
                // Pool is in DRAINING state
                // Now wait for ALL spawned tasks to complete
                
                log.Printf("Waiting for index tasks to complete...")
                err := stepCtx.WaitTaskPool(ctx)
                if err != nil {
                    return fmt.Errorf("task pool failed: %w", err)
                }

                log.Printf("All index tasks done!")
                
                // Switch query traffic to new index
                return db.UpdateIndexAlias(ctx, "search", "old_index", "new_index")
            },
        ),
)
```

### Pool Lifecycle State Machine

```
State Progression:
│
├─ CREATING
│  ├─ [reindex-records] spawns tasks
│  ├─ [listen-changes] spawns tasks
│  └─ Both proceed independently (no sync needed)
│
├─ DRAINING (triggered when both spawners finish)
│  ├─ reindex-records calls PoolStartDrain() (end of generator)
│  ├─ listen-changes calls PoolStartDrain() (ProducerStep auto-exit after drain signal/timeout)
│  └─ NEW spawns are rejected (pool is "closing")
│
└─ COMPLETED
   ├─ All spawned tasks finished
   ├─ [switch-index] unblocks from WaitTaskPool(ctx)
   └─ Flow proceeds: update index alias, switch traffic

Concurrent Spawning (No Race):
  reindex-records (1 worker)       listen-changes (1 producer worker)
        |                               |
  spawn tasks (10M)  ←parallel→  spawn tasks (N as arrivals)
        |                               |
   call PoolStartDrain()          auto-exit (drain signal)
        |_______________  _______________| 
                        ↓
              Pool enters DRAINING

Task Completion (Hundreds of Thousands):
  Index tasks execute in parallel (concurrency: 50)
  Pool counts down remaining tasks
  When all done → state → COMPLETED
  [switch-index] WaitTaskPool() unblocks
```

### Key Design Decisions

1. **GeneratorStep for reindex** (not MapStep):
   - Records are not pre-fetched into array
   - Generator yields progressively via channel
   - Memory bounded (buffered channel, not array)
   - Natural for cursor-based pagination

2. **ProducerStep for listen-changes** (not regular step):
   - Long-running listener
   - Must exit gracefully when "drain" signal arrives
   - Drain signal = "reindex is done, stop accepting new changes"
   - ProducerStep handles this: exits when stepCtx context is cancelled
   - Automatic `PoolStartDrain()` on exit (framework-provided)

3. **Explicit PoolStartDrain calls** (not implicit):
   - Each spawner explicitly signals "I'm done spawning"
   - Reindex: end of generator function (explicit call in finally/defer)
   - Listener: ProducerStep auto-calls on exit
   - Ensures convergence only waits when all sources finished
   - No ambiguity about "when is draining phase entered"

4. **Multi-Source Pool (not two separate pools)**:
   - Both spawners use `WithTaskPool("index-ops")`
   - Pool is shared, counts aggregate (10M + N)
   - `WaitTaskPool()` blocks until ALL tasks done
   - Simpler than managing two pools separately

5. **Drain timeout on ProducerStep**:
   - If reindex stalls indefinitely, listener should timeout and exit
   - `WithDrainTimeout(5*time.Minute)` acts as safety valve
   - Prevents listener from holding resources forever
   - After timeout, listener exits → PoolStartDrain() called → pool enters DRAINING

### Failure Scenarios

**Scenario 1: Reindex fails halfway**
```
reindex-records exits with error
├─ PoolStartDrain still called
├─ listen-changes continues accepting changes
├─ Pool enters DRAINING (even though reindex failed)
├─ switch-index unblocks from dependencies, sees reindex error
└─ Flow fails (no blindswitching to partial index)
```

**Scenario 2: Listener crashes (network loss)**
```
listen-changes exits (e.g., connection dropped)
├─ ProducerStep auto-calls PoolStartDrain()
├─ Pool enters DRAINING
├─ reindex-records still spawning? No, already done → also calls PoolStartDrain
└─ switch-index waits for remaining tasks, proceeds (changes after crash are lost, but that's acceptable)
```

**Scenario 3: Too many changes arrive during reindex**
```
listen-changes spawns tasks continuously
├─ But pool has concurrency limit (only 50 workers at a time)
├─ Task queue grows (bounded by DB auto-incrementing, not memory)
├─ Eventually all tasks complete as workers churn through queue
└─ OK: pool and DB ensure no loss, just gradual processing
```

**Scenario 4: One spawner calls PoolStartDrain twice (idempotent)**
```
reindex-records calls PoolStartDrain() at end
listen-changes (ProducerStep) auto-calls PoolStartDrain() on exit
├─ First call: pool transitions CREATING → DRAINING
├─ Second call: pool already DRAINING, call is no-op
└─ OK: PoolStartDrain() is designed to be idempotent
```

### Comparison with Naive Approaches

| Approach | Result | Why Not |
|----------|--------|---------|
| **Two separate pools** | Each spawner to own pool, then merge results | Requires bespoke merge logic; two independent task queues hard to coordinate; can lead to inconsistency if one finishes before the other signals |
| **Load all records into array first** | MapStep with []Records dependency | 10M records in memory = crash/slowness; defeats progressive indexing benefit; doesn't handle changes arriving during reindex |
| **Run reindex first, then listen** | Sequential (not concurrent) | Downtime: index is incomplete during reindex; old index is stale when switched (gap in changes) |
| **Dual-index (shadow)** | Keep both indices updated, never switch | Perpetual consistency overhead; doesn't solve the zero-downtime rebuild problem |
| **Multi-Source Pool (this design)** | Single pool, dual spawners, explicit drain coordination | ✅ Bounded memory, concurrent processing, deterministic state machine, crash-safe coordination |

---

## Open Questions Remaining

1. **Multiple poolsnin single flow**: Can a step spawn to multiple pools?
   - Probably not needed (pools are per-coordination scope)

2. **Pool result aggregation**: Should pool tasks contribute to step output?
   - Currently: No (pool is side-effect, step output is independent)
   - Alternative: Pool could aggregate and step gets that as output

3. **Nested flows**: Can producer steps contain flows that spawn to parent pool?
   - Probably no (scope confusion), keep pools flow-local

4. **Cancellation**: If flow is cancelled mid-drain, what happens to spawned tasks?
   - Option A: Tasks continue until completion (graceful)
   - Option B: Tasks are force-killed (fast)

5. **Pool timeout**: How long should WaitTaskPool timeout be?
   - Should it inherit from step timeout?
   - Or configurable separately?
