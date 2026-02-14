# Strong Typing for Coordination: Typed Builders + StepContext

Building on [GENERIC_API_DESIGN.md](GENERIC_API_DESIGN.md), this document applies typed builder patterns to dynamic task spawning, producer steps, and task pools for full compile-time type safety.

## Core Insight: Dependencies as Function Parameters

From GENERIC_API_DESIGN, dependencies are part of the **function signature**, not accessed dynamically:

```go
// StepBuilder2[ValidationResult, FraudCheckResult]
func(ctx context.Context, in Input, v ValidationResult, f FraudCheckResult) (Output, error)
```

We extend this: **StepContext is an additional typed parameter** that carries side-effect APIs:

```go
// StepBuilder2 WITH side effects
func(ctx context.Context, stepCtx StepContext, in Input, v ValidationResult, f FraudCheckResult) (Output, error) {
    // Dependencies are type-safe (in signature)
    // Side effects via context
}
```

## StepContext: Typed Interface for Side Effects

### Design Principle

`StepContext` is NOT for accessing dependencies (those are parameters). Instead:
- Dependencies: injected as **function parameters** (compile-time typed)
- Side effects: accessed via **StepContext methods** (typed at step level)

### Basic API

```go
// Minimal StepContext - works for all steps
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
```

### Implementation at Runtime

```go
type stepContextImpl struct {
    flowRunID   int64
    flowName    string
    stepName    string
    taskPoolName string  // "" if no pool
    conn        Conn
    logger      *slog.Logger
}

func (sc *stepContextImpl) SpawnTask(ctx context.Context, input any) (int64, error) {
    if sc.taskPoolName == "" {
        return 0, fmt.Errorf("step %q not connected to task pool", sc.stepName)
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
        return nil  // No pool, noop
    }
    
    _, err := sc.conn.Exec(ctx,
        "SELECT cb_wait_pool($1, $2)",
        sc.taskPoolName, sc.flowRunID)
    return err
}
```

## Typed Builders: StepContextAware

Extend the typed builder pattern to support pool attachment:

### StepBuilder Extensions

```go
// Add pool support to all builders
type StepBuilder0 struct {
    name         string
    taskPoolName string  // NEW
}

func (s *StepBuilder0) WithTaskPool(poolName string) *StepBuilder0 {
    s.taskPoolName = poolName
    return s
}

type StepBuilder1[D1 any] struct {
    name         string
    dep1Name     string
    taskPoolName string  // NEW
}

func (s *StepBuilder1[D1]) WithTaskPool(poolName string) *StepBuilder1[D1] {
    s.taskPoolName = poolName
    return s
}

// ... same for StepBuilder2[D1, D2], etc.

// When building handler, pass pool info
func (s *StepBuilder1[D1]) WithHandler[In, Out any](
    fn func(context.Context, StepContext, In, D1) (Out, error),
    opts ...HandlerOpt,
) *Step {
    dep1Name := s.dep1Name
    poolName := s.taskPoolName  // May be ""
    
    wrapper := func(ctx context.Context, msg stepMessage, stepCtx StepContext) ([]byte, error) {
        var input In
        var dep1 D1
        
        json.Unmarshal(msg.Input, &input)
        json.Unmarshal(msg.Dependencies[dep1Name], &dep1)
        
        output, err := fn(ctx, stepCtx, input, dep1)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{
        name:         s.name,
        handler:      wrapper,
        dependencies: []string{dep1Name},
        taskPoolName: poolName,  // NEW
    }
}
```

## Usage: Reindexing Flow with Strong Types

```go
// Phase 1: Setup
createIndexStep := catbird.NewStep("create-index").
    WithHandler(
        func(ctx context.Context, config IndexConfig) (IndexID, error) {
            return searchIndex.Create(ctx, config)
        },
    )

// Phase 2a: Reindex existing records (spawns tasks)
reindexStep := catbird.NewStep("reindex-pages").
    DependsOn[IndexID]("create-index").
    WithTaskPool("index-ops").  // ← Connect to pool
    WithHandler(
        func(
            ctx context.Context,
            stepCtx StepContext,  // ← Type parameter for context
            config IndexConfig,
            indexID IndexID,      // ← Type parameter for dependency
        ) (Summary, error) {
            cursor := ""
            for {
                records, next := db.QueryPage(ctx, cursor, 1000)
                
                for _, record := range records {
                    // Spawn to typed context
                    if _, err := stepCtx.SpawnTask(ctx, IndexOp{
                        Type:    "insert",
                        IndexID: indexID,
                        Record:  record,
                    }); err != nil {
                        return Summary{}, err
                    }
                }
                
                if next == "" {
                    break
                }
                cursor = next
            }
            
            // Signal no more spawns
            if err := stepCtx.PoolStartDrain(ctx); err != nil {
                return Summary{}, err
            }
            
            return Summary{RecordsSpawned: 0}, nil
        },
        catbird.WithConcurrency(1),  // Only one worker should reindex
    )

// Phase 2b: Listen for changes (producer step)
listenStep := catbird.NewProducerStep("listen-changes").
    DependsOn[IndexID]("create-index").
    WithTaskPool("index-ops").  // ← Same pool!
    WithHandler(
        func(
            ctx context.Context,
            stepCtx StepContext,
            config IndexConfig,
            indexID IndexID,
        ) error {
            // ProducerStep auto-calls PoolStartDrain on exit
            changes := db.SubscribeChanges(ctx)
            
            for change := range changes {
                if _, err := stepCtx.SpawnTask(ctx, IndexOp{
                    Type:    change.OperationType,
                    IndexID: indexID,
                    Record:  change.Record,
                }); err != nil {
                    return err
                }
            }
            
            return nil
        },
        catbird.WithDrainTimeout(5*time.Minute),
        catbird.WithIdleTimeout(30*time.Second),
    )

// Phase 3: Convergence (wait for all tasks)
switchStep := catbird.NewStep("switch-index").
    DependsOn[Summary]("reindex-pages").
    DependsOn[bool]("listen-changes").  // Now DependsOn the Producer step too!
    WithTaskPool("index-ops").
    WithHandler(
        func(
            ctx context.Context,
            stepCtx StepContext,
            config IndexConfig,
            reindexSummary Summary,
            listenComplete bool,  // Producer steps return a value
        ) error {
            // Both spawning steps are done (called PoolStartDrain)
            // Pool is in DRAINING phase
            
            // Wait for all tasks to complete
            if err := stepCtx.WaitTaskPool(ctx); err != nil {
                return fmt.Errorf("task pool failed: %w", err)
            }
            
            // Get final status for logging
            status, _ := stepCtx.PoolStatus(ctx)
            log.Printf("Reindexing complete: %d tasks, %d completed",
                status.Spawned, status.Completed)
            
            // Now safe to switch
            return searchIndex.SwitchActive(ctx, indexID)
        },
    )

// Build flow
flow := catbird.NewFlow("reindex-with-changes").
    AddStep(createIndexStep).
    AddStep(reindexStep).
    AddStep(listenStep).
    AddStep(switchStep)
```

## ProducerStep: Typed Builder Variant

ProducerStep is a builder variant that includes drain configuration:

```go
type ProducerStepBuilder[D1 any] struct {
    name          string
    dep1Name      string
    taskPoolName  string
    drainTimeout  time.Duration
    idleTimeout   time.Duration
}

func (s *StepBuilder1[D1]) AsProducer() *ProducerStepBuilder[D1] {
    return &ProducerStepBuilder[D1]{
        name:         s.name,
        dep1Name:     s.dep1Name,
        taskPoolName: s.taskPoolName,
        drainTimeout: 5 * time.Minute,  // Default
        idleTimeout:  30 * time.Second, // Default
    }
}

func (p *ProducerStepBuilder[D1]) WithDrainTimeout(d time.Duration) *ProducerStepBuilder[D1] {
    p.drainTimeout = d
    return p
}

func (p *ProducerStepBuilder[D1]) WithIdleTimeout(d time.Duration) *ProducerStepBuilder[D1] {
    p.idleTimeout = d
    return p
}

func (p *ProducerStepBuilder[D1]) WithHandler[In any](
    fn func(context.Context, StepContext, In, D1) error,  // Returns error, not output
    opts ...HandlerOpt,
) *ProducerStep {
    // Creates a ProducerStep that auto-drains on exit
    // ...
}
```

Alternative API (simpler):

```go
// Use NewProducerStep instead of converting
func NewProducerStep(name string) *ProducerStepBuilder0 { ... }

type ProducerStepBuilder0 struct {
    name         string
    drainTimeout time.Duration
    idleTimeout  time.Duration
}

func (p *ProducerStepBuilder0) DependsOn[D1 any](name string) *ProducerStepBuilder1[D1] {
    return &ProducerStepBuilder1[D1]{
        name:         p.name,
        dep1Name:     name,
        drainTimeout: p.drainTimeout,
        idleTimeout:  p.idleTimeout,
    }
}

// Usage:
listenStep := catbird.NewProducerStep("listen").
    WithDrainTimeout(5*time.Minute).
    DependsOn[IndexID]("create-index").
    WithTaskPool("index-ops").
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, config IndexConfig, indexID IndexID) error {
            // ...
        },
    )
```

## GeneratorStep: Typed Builder with Channel

GeneratorStep yields items via typed channel:

```go
type GeneratorStepBuilder0 struct {
    name string
    taskPoolName string
}

func (g *GeneratorStepBuilder0) DependsOn[D1 any](name string) *GeneratorStepBuilder1[D1] {
    return &GeneratorStepBuilder1[D1]{
        name:     g.name,
        dep1Name: name,
    }
}

type GeneratorStepBuilder1[D1 any] struct {
    name         string
    dep1Name     string
    taskPoolName string
}

func (g *GeneratorStepBuilder1[D1]) WithTaskPool(poolName string) *GeneratorStepBuilder1[D1] {
    g.taskPoolName = poolName
    return g
}

// WithGenerator specifies the generator function and task handler
func (g *GeneratorStepBuilder1[D1]) WithGenerator[In, Item, Result any](
    generatorFn func(context.Context, In, D1, chan<- Item) error,  // Generator yields items
    taskHandlerFn func(context.Context, Item) (Result, error),     // Process each item
    opts ...HandlerOpt,
) *Step {
    // Creates tasks as items are yielded
    // Results aggregated into []Result
    // ...
}

// Usage:
indexStep := catbird.NewGeneratorStep("index-records").
    DependsOn[IndexID]("create-index").
    WithTaskPool("index-ops").
    WithGenerator(
        // Generator function: yields items
        func(ctx context.Context, config IndexConfig, indexID IndexID, yield chan<- Record) error {
            cursor := ""
            for {
                records, next := db.QueryPage(ctx, cursor, 1000)
                
                for _, record := range records {
                    select {
                    case yield <- record:  // Typed channel!
                    case <-ctx.Done():
                        return ctx.Err()
                    }
                }
                
                if next == "" {
                    return nil
                }
                cursor = next
            }
        },
        // Task handler function: processes each yielded item
        func(ctx context.Context, record Record) (IndexResult, error) {
            return searchIndex.Index(ctx, record)
        },
        catbird.WithConcurrency(50),
    )
```

## TaskPool API: Typed

```go
// Untyped pool definition (shared across flows)
type TaskPool interface {
    SpawnTask(ctx context.Context, input any) error
    WaitAll(ctx context.Context) error
}

// At runtime, create typed wrapper for each flow
func createTypedPool[Item any](poolName string, handler func(context.Context, Item) error) TaskPool {
    // Returns pool that validates input type at spawn time
    // (Optional - provides additional type safety)
}

// Usage stays untyped (inherent limitation of task pools)
pool := catbird.CreateTaskPool("index-ops", indexOpHandler)

// But steps are strongly typed:
step := catbird.NewStep("spawn-tasks").
    WithTaskPool("index-ops").  // Pool name is string (untyped at compile time)
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, config Config) error {
            // stepCtx.SpawnTask enforces types at runtime
            stepCtx.SpawnTask(ctx, IndexOp{...})
        },
    )
```

## Complete Flow Example: Compile-Time Safety

```go
package main

import (
    "context"
    "log"
    "time"
    "catbird"
)

// Types
type IndexConfig struct {
    Name string
}

type IndexID string

type Record struct {
    ID    string
    Data  []byte
}

type IndexOp struct {
    Type    string  // "insert", "update", "delete"
    IndexID IndexID
    Record  Record
}

type Summary struct {
    RecordsSpawned int
}

// Flow definition with STRONG TYPES at compile time
func buildReindexFlow() *catbird.Flow {
    // Phase 1: Setup
    createStep := catbird.NewStep("create-index").
        WithHandler(
            func(ctx context.Context, cfg IndexConfig) (IndexID, error) {
                // ✅ Compile time: cfg type is IndexConfig
                // ✅ Compile time: return type must be IndexID
                log.Printf("Creating index: %s", cfg.Name)
                return IndexID("v2"), nil
            },
        )

    // Phase 2a: Reindex
    // ✅ Type safety enforced by builder!
    reindexStep := catbird.NewStep("reindex").
        DependsOn[IndexID]("create-index").             // ← Builder now is StepBuilder1[IndexID]
        WithTaskPool("index-ops").
        WithHandler(
            // ✅ If you write this wrong:
            // func(..., id string) // ❌ Compile error! Expected IndexID, got string
            // ✅ If dependency is missing:
            // func(ctx context.Context, cfg IndexConfig) // ❌ Compile error! Missing IndexID parameter
            // ✅ Only valid signature:
            func(
                ctx context.Context,
                stepCtx catbird.StepContext,
                cfg IndexConfig,
                indexID IndexID,  // ← Type parameter enforced by builder type
            ) (Summary, error) {
                log.Printf("Reindexing to: %s", indexID)
                
                // ✅ Compile time: stepCtx has SpawnTask method
                _, err := stepCtx.SpawnTask(ctx, IndexOp{
                    Type:    "insert",
                    IndexID: indexID,
                    Record:  Record{ID: "1"},
                })
                
                // ✅ Compile time: PoolStartDrain exists
                _ = stepCtx.PoolStartDrain(ctx)
                
                return Summary{RecordsSpawned: 1}, nil
            },
        )

    // Phase 2b: Listen
    listenStep := catbird.NewProducerStep("listen").
        WithDrainTimeout(5*time.Minute).
        DependsOn[IndexID]("create-index").
        WithTaskPool("index-ops").
        WithHandler(
            func(
                ctx context.Context,
                stepCtx catbird.StepContext,
                cfg IndexConfig,
                indexID IndexID,
            ) error {
                log.Printf("Listening for changes to: %s", indexID)
                // ProducerStep auto-calls PoolStartDrain on return
                return nil
            },
        )

    // Phase 3: Convergence
    // ✅ Now depends on BOTH spawning steps, each with proper output type
    switchStep := catbird.NewStep("switch").
        DependsOn[Summary]("reindex").               // ← Summary output type
        DependsOn[error]("listen").                  // ← Wait, what type does ProducerStep return?
        WithTaskPool("index-ops").
        WithHandler(
            func(
                ctx context.Context,
                stepCtx catbird.StepContext,
                cfg IndexConfig,
                reindexSummary Summary,
                listenResult error,  // ← Hmm, types don't quite work here
            ) error {
                log.Printf("Switching index, processed: %d", reindexSummary.RecordsSpawned)
                _ = stepCtx.WaitTaskPool(ctx)
                return nil
            },
        )

    return catbird.NewFlow("reindex").
        AddStep(createStep).
        AddStep(reindexStep).
        AddStep(listenStep).
        AddStep(switchStep)
}

func main() {
    ctx := context.Background()
    flow := buildReindexFlow()
    log.Printf("Flow: %+v", flow)
}
```

## Issue: Producer Step Output Type

**Problem**: What output type does ProducerStep have?

Current spec: ProducerStep handler returns just `error`, not `(Output, error)`

```go
func(ctx context.Context, stepCtx StepContext, in Input, dep1 D1) error
```

But DependsOn requires a type for the step's output:

```go
DependsOn[Summary]("reindex")      // ✅ Reindex outputs Summary
DependsOn[???]("listen")            // ❌ Listen outputs error? Unit? bool?
```

### Solution Options

**Option A: Producer returns unit output**

```go
type Unit struct{}

listenStep := catbird.NewProducerStep("listen").
    DependsOn[IndexID]("create-index").
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, cfg IndexConfig, indexID IndexID) (Unit, error) {
            // ...
            return Unit{}, nil
        },
    )

// Then:
switchStep := catbird.NewStep("switch").
    DependsOn[Summary]("reindex").
    DependsOn[Unit]("listen").       // ← Explicit but verbose
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, cfg IndexConfig, s Summary, _ Unit) error {
            // Ignore the Unit
        },
    )
```

**Option B: Producer is not a dependable step**

Producer steps run concurrently, but you don't depend on their output. Convergence step waits differently:

```go
// Flow execution:
// 1. create-index completes
// 2. reindex and listen START TOGETHER (no dependency relationship)
// 3. Both spawn to shared pool
// 4. Both call PoolStartDrain when done
// 5. switch only DependsOn steps that feed data to it

switchStep := catbird.NewStep("switch").
    DependsOn[Summary]("reindex").  // Get data from reindex
    WithTaskPool("index-ops").
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, cfg IndexConfig, s Summary) error {
            // listen-changes is NOT a dependency
            // But it was configured to spawn to same pool
            // Both complete, then switch starts and waits for pool
        },
    )

// Flow engine must track:
// 1. Reindex and listen can START together IF:
//    - They're both ready (dependencies met)
//    - They're both configured with same TaskPool
// 2. They run in parallel to completion
// 3. Switch can START once reindex completes (its only dep)
// 4. Switch WAITS for pool (which handles listen's tasks too)
```

This requires new flow definition semantics - we need to mark steps that should run together as "producers" to the pool.

**Option C: Explicit "WaitForProducers"**

```go
switchStep := catbird.NewStep("switch").
    DependsOn[Summary]("reindex").
    WithTaskPool("index-ops").
    WaitForProducers("listen-changes").  // Wait for this producer to complete
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, cfg IndexConfig, s Summary) error {
            // Both reindex and listen done
            _ = stepCtx.WaitTaskPool(ctx)
            return nil
        },
    )
```

Then "listen-changes" doesn't need an output type (producer steps return error only).

## Recommended Solution: Option B + Improved Flow Semantics

Producer steps should be defined separately from regular data-flow dependencies:

```go
// CURRENT (wrong semantics)
switchStep := DependsOn[Summary]("reindex").
    DependsOn[Unit]("listen")  // Looks like listen produces Summary

// BETTER (clear semantics)
switchStep := DependsOn[Summary]("reindex").
    WaitForPool("index-ops")   // Wait for pool to drain (includes all producers)

// Or even:
pool := TaskPool("index-ops",
    Producers("reindex", "listen-changes"),  // Both spawn to this pool
    Consumers("switch-index"),               // Consumer waits for pool
)
```

But this is beyond the scope of GENERIC_API_DESIGN patterns.

## Summary: Strong Typing Benefits

With typed builders + StepContext:

✅ **Compile-time type safety**
```go
// Handler signature MUST match dependency types
NewStep("x").DependsOn[ValidationResult]("v").WithHandler(
    func(ctx context.Context, stepCtx StepContext, in Request, v ValidationResult) error {
        // ✅ v is known to be ValidationResult (compile-time guarantee)
        // ❌ This won't compile: func(..., v FraudResult)
    },
)
```

✅ **Zero reflection**
```go
// Types known at compile time, direct function calls
// No reflection.Value.Call() overhead
```

✅ **IDE support**
```go
// Full autocomplete, jump-to-definition, inline docs
// All dependency types visible in signature
```

✅ **Side effects explicit**
```go
// StepContext parameter makes side-effect APIs clear
// pool.SpawnTask() vs dependency parameters vs signal
```

✅ **No package prefix repetition**
```go
// Methods instead of functions
step.WithTaskPool("pool").WithHandler(fn)
// vs
catbird.NewStep(...,
    catbird.WithTaskPool(...),
    catbird.WithHandler(...),
)
```

## Open Design Question: Producer Step Output

**Remaining ambiguity**: What type do ProducerSteps contribute to the type system?

- ✅ Regular steps: `StepBuilder1[D1]` → output type is part of signature
- ❌ Producer steps: Return `error` only → what output type?

Recommend:
1. Producer steps return `(Unit, error)` for consistency, or
2. Producer steps are not dependencies (don't use `Depends On`), or
3. Separate `WithProducerPool()` semantic that doesn't create dependencies

This needs further design but doesn't block the strong typing approach.
