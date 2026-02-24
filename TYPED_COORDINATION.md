# Strong Typing for Coordination: StepContext & Advanced Patterns

> **Status**: Conceptual exploration of advanced coordination patterns (dynamic task spawning, pools, producers)

This document explores advanced coordination patterns for flow steps that need side effects beyond simple data transformation:
- **Dynamic task spawning**: Steps that create tasks during execution
- **Task pools**: Steps that coordinate parallel task execution
- **Producer patterns**: Steps that generate work dynamically
- **Typed coordination**: StepContext interface for side effects

## Core Insight: Dependencies as Parameters, Side Effects via StepContext

**Dependencies** are injected as **function parameters** (type-checked at build time via reflection):

```go
NewStep("process").
    DependsOn(Dep[ValidationResult]("validate")).
    WithHandler(
        func(ctx context.Context, in Input, v ValidationResult) (Output, error) {
            // v parameter type is validated via reflection at build time
            return processWithDependencies(v), nil
        }
```

**Side effects** are accessed via **context.WithValue()** using `catbird.FromContext(ctx)`:

```go
NewStep("index-pages").
    DependsOn(Dep[IndexID]("create-index")).
    WithTaskPool("index-ops").
    WithHandler(
        func(ctx context.Context, cfg Config, indexID IndexID) (Summary, error) {
            // Extract StepContext from context
            stepCtx := catbird.FromContext(ctx)
            for _, page := range fetchPages(cfg) {
                stepCtx.SpawnTask(ctx, IndexTask{PageID: page.ID, IndexID: indexID})
            }
            stepCtx.PoolStartDrain(ctx)
            stepCtx.WaitTaskPool(ctx)
            return Summary{Indexed: len(pages)}, nil
        })
```

**Key design**: StepContext is NOT for dependencies (those are parameters). It's accessed via `catbird.FromContext(ctx)` for side effects. Handlers keep plain `context.Context` as the first parameter (idiomatic Go).

## StepContext: Typed Interface for Side Effects

### Design Principle

`StepContext` is NOT for accessing dependencies (those are parameters). Instead:
- **Dependencies**: injected as **function parameters** (type-checked at build time via reflection)
- **Side effects**: accessed via `catbird.FromContext(ctx)` (runtime lookup)
- **Handler signature**: plain `context.Context` as first parameter (idiomatic Go, compatible with all Go libraries)

### Basic API

```go
// StepContext interface - obtained via catbird.FromContext(ctx)
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

// Extract StepContext from context (idiomatic Go pattern)
stepCtx := catbird.FromContext(ctx)  // Returns StepContext or nil if not available
```

### Implementation at Runtime

StepContext is attached to the handler context via `context.WithValue()` before invocation:

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

// At step invocation time (step_worker.go), wrap context before calling handler
func invokeStepHandler(ctx context.Context, handler func(context.Context, ...) error, stepCtxImpl *stepContextImpl, args ...any) error {
    // Attach StepContext to context via standard Go pattern
    ctx = catbird.WithHandlerContext(ctx, stepCtxImpl)
    return handler(ctx, args...)
}

// Handlers extract via:
func(ctx context.Context, in Input) (Output, error) {
    stepCtx := catbird.FromContext(ctx)  // Get StepContext
    // use stepCtx for side effects
}
```

### StepContext Support via Context Values

The reflection-based builder pattern supports pool attachment without signature changes:

### Step Builder with Pool Support

```go
// Step builder accumulates metadata including pool configuration
type Step struct {
    Name         string
    dependencies []*StepDependency
    taskPoolName string  // Optional: attached pool
    handler      *stepHandler
}

func NewStep(name string) *Step {
    return &Step{Name: name}
}

func (s *Step) DependsOn(deps ...*StepDependency) *Step {
    s.dependencies = append(s.dependencies, deps...)
    return s
}

func (s *Step) WithTaskPool(poolName string) *Step {
    s.taskPoolName = poolName
    return s
}

// WithHandler uses reflection to validate handler signature
// No StepContext parameter required; handlers extract via catbird.FromContext(ctx)
func (s *Step) WithHandler(fn any, opts ...HandlerOpt) *Step {
    // Reflection validates:
    // 1. First param: context.Context
    // 2. Next param: input type
    // 3. Following params: dependency types (validated against s.dependencies)
    // 4. Last param if signal: signal type
    // 5. Returns: (output, error)
    // Pool attachment has NO signature impact
    
    wrapper, err := makeStepWrapper(fn, s)
    if err != nil {
        panic(fmt.Errorf("invalid handler for step %s: %w", s.Name, err))
    }
    
    s.handler = &stepHandler{fn: wrapper}
    return s
}
```

### Usage Example

```go
// Step WITHOUT pool (regular handler)
normalStep := catbird.NewStep("process").
    DependsOn(catbird.Dep[ValidationResult]("validate")).
    WithHandler(
        func(ctx context.Context, in Request, v ValidationResult) (Response, error) {
            return process(v), nil
        },
    )

// Step WITH pool (extract from context)
poolStep := catbird.NewStep("index-pages").
    DependsOn(catbird.Dep[IndexID]("create-index")).
    WithTaskPool("index-ops").
    WithHandler(
        // No StepContext parameter; extract from context as needed
        func(ctx context.Context, cfg Config, indexID IndexID) (Summary, error) {
            stepCtx := catbird.FromContext(ctx)  // Extract StepContext
            pages := fetchPages(cfg)
            for _, page := range pages {
                    stepCtx.SpawnTask(ctx, IndexTask{PageID: page.ID, IndexID: indexID})
            }
            stepCtx.PoolStartDrain(ctx)
            stepCtx.WaitTaskPool(ctx)
            return Summary{Indexed: len(pages)}, nil
        },
    )
```
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

## Usage: Reindexing Flow Example

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
    DependsOn(catbird.Dep[IndexID]("create-index")).
    WithTaskPool("index-ops").  // ← Connect to pool
    WithHandler(
        func(
            ctx context.Context,
            stepCtx StepContext,  // ← Reflection validates this is present when pool attached
            config IndexConfig,
            indexID IndexID,      // ← Dependency type validated via reflection
        ) (Summary, error) {
            cursor := ""
            for {
                records, next := db.QueryPage(ctx, cursor, 1000)
                
                for _, record := range records {
                    // Spawn to pool
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
    DependsOn(catbird.Dep[IndexID]("create-index")).
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
    DependsOn(catbird.Dep[Summary]("reindex-pages")).
    DependsOn(catbird.Dep[bool]("listen-changes")).  // Now DependsOn the Producer step too!
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

## ProducerStep: Conceptual Extension (Not Yet Implemented)

> **Note**: This section describes a potential extension for specialized producer scenarios. Regular steps with `WithTaskPool()` and drain configuration already handle most producer patterns. This shows how specialized producer steps could simplify common patterns if needed.

ProducerStep would be a specialized builder that auto-drains on completion:

```go
// Conceptual design - not yet implemented
type ProducerStep struct {
    name          string
    dependencies  []*StepDependency
    taskPoolName  string
    drainTimeout  time.Duration
    idleTimeout   time.Duration
    handler       *stepHandler
}

func NewProducerStep(name string) *ProducerStep {
    return &ProducerStep{
        name:         name,
        drainTimeout: 5 * time.Minute,  // Default
        idleTimeout:  30 * time.Second, // Default
    }
}

func (p *ProducerStep) DependsOn(deps ...*StepDependency) *ProducerStep {
    p.dependencies = append(p.dependencies, deps...)
    return p
}

func (p *ProducerStep) WithTaskPool(poolName string) *ProducerStep {
    p.taskPoolName = poolName
    return p
}

func (p *ProducerStep) WithDrainTimeout(d time.Duration) *ProducerStep {
    p.drainTimeout = d
    return p
}

func (p *ProducerStep) WithIdleTimeout(d time.Duration) *ProducerStep {
    p.idleTimeout = d
    return p
}

// WithHandler uses reflection to validate signature
// Producer handlers return error instead of (output, error)
func (p *ProducerStep) WithHandler(fn any, opts ...HandlerOpt) *ProducerStep {
    // Reflection validates producer signature:
    // func(ctx, stepCtx, input, ...deps) error  // No output
    wrapper, err := makeProducerWrapper(fn, p)
    if err != nil {
        panic(err)
    }
    p.handler = &stepHandler{fn: wrapper}
    return p
}

// Usage:
listenStep := catbird.NewProducerStep("listen").
    WithDrainTimeout(5*time.Minute).
    DependsOn(catbird.Dep[IndexID]("create-index")).
    WithTaskPool("index-ops").
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, config IndexConfig, indexID IndexID) error {
            // Producer logic - auto-drains on exit
        },
    )
```

## GeneratorStep: Conceptual Extension (Not Yet Implemented)

> **Note**: This section describes a potential extension for channel-based item generation. Regular steps that spawn tasks in loops already support generator patterns. This shows how a specialized generator abstraction could simplify common patterns.

GeneratorStep would abstract channel-based task generation:

```go
// Conceptual design - not yet implemented
type GeneratorStep struct {
    name         string
    dependencies []*StepDependency
    taskPoolName string
    generator    any  // Generator function
    taskHandler  any  // Task handler function
}

func NewGeneratorStep(name string) *GeneratorStep {
    return &GeneratorStep{name: name}
}

func (g *GeneratorStep) DependsOn(deps ...*StepDependency) *GeneratorStep {
    g.dependencies = append(g.dependencies, deps...)
    return g
}

func (g *GeneratorStep) WithTaskPool(poolName string) *GeneratorStep {
    g.taskPoolName = poolName
    return g
}

// WithGenerator uses reflection to validate both functions:
// generatorFn: func(ctx, input, ...deps, chan<- Item) error
// taskHandlerFn: func(ctx, Item) (Result, error)
func (g *GeneratorStep) WithGenerator(generatorFn any, taskHandlerFn any, opts ...HandlerOpt) *GeneratorStep {
    // Reflection validates signatures and creates wrapper
    // ...
    return g
}

// Usage:
indexStep := catbird.NewGeneratorStep("index-records").
    DependsOn(catbird.Dep[IndexID]("create-index")).
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

## TaskPool API: Common Interface

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

// But steps use reflection for type validation:
step := catbird.NewStep("spawn-tasks").
    WithTaskPool("index-ops").  // Pool name is string (validated at runtime)
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, config Config) error {
            // stepCtx.SpawnTask validates types at runtime
            stepCtx.SpawnTask(ctx, IndexOp{...})
        },
    )
```

## Complete Flow Example: Reflection-Based API

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

// Flow definition with reflection-based API
func buildReindexFlow() *catbird.Flow {
    // Phase 1: Setup
    createStep := catbird.NewStep("create-index").
        WithHandler(
            func(ctx context.Context, cfg IndexConfig) (IndexID, error) {
                // Reflection validates: ctx, cfg, returns (IndexID, error)
                log.Printf("Creating index: %s", cfg.Name)
                return IndexID("v2"), nil
            },
        )

    // Phase 2a: Reindex
    reindexStep := catbird.NewStep("reindex").
        DependsOn(catbird.Dep[IndexID]("create-index")).  // Reflection validates dependency type
        WithTaskPool("index-ops").
        WithHandler(
            func(
                ctx context.Context,
                stepCtx catbird.StepContext,
                cfg IndexConfig,
                indexID IndexID,  // Reflection validates against Dep[IndexID]
            ) (Summary, error) {
                log.Printf("Reindexing to: %s", indexID)
                
                // Runtime: stepCtx has SpawnTask method
                _, err := stepCtx.SpawnTask(ctx, IndexOp{
                    Type:    "insert",
                    IndexID: indexID,
                    Record:  Record{ID: "1"},
                })
                
                _ = stepCtx.PoolStartDrain(ctx)
                
                return Summary{RecordsSpawned: 1}, nil
            },
        )

    // Phase 2b: Listen
    listenStep := catbird.NewProducerStep("listen").
        WithDrainTimeout(5*time.Minute).
        DependsOn(catbird.Dep[IndexID]("create-index")).
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
    // Depends on BOTH spawning steps
    switchStep := catbird.NewStep("switch").
        "DependsOn(catbird.Dep[Summary]("reindex").               // ← Summary output type
        "DependsOn(catbird.Dep[error]("listen").                  // ← Wait, what type does ProducerStep return?
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
DependsOn(catbird.Dep[Summary]("reindex"))      // ✅ Reindex outputs Summary
DependsOn(catbird.Dep[???]("listen"))            // ❌ Listen outputs error? Unit? bool?
```

### Solution Options

**Option A: Producer returns unit output**

```go
type Unit struct{}

listenStep := catbird.NewProducerStep("listen").
    DependsOn(catbird.Dep[IndexID]("create-index")).
    WithHandler(
        func(ctx context.Context, stepCtx StepContext, cfg IndexConfig, indexID IndexID) (Unit, error) {
            // ...
            return Unit{}, nil
        },
    )

// Then:
switchStep := catbird.NewStep("switch").
    DependsOn(catbird.Dep[Summary]("reindex")).
    DependsOn(catbird.Dep[Unit]("listen")).       // ← Explicit but verbose
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
    DependsOn(catbird.Dep[Summary]("reindex")).  // Get data from reindex
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
    DependsOn(catbird.Dep[Summary]("reindex")).
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
switchStep := DependsOn(catbird.Dep[Summary]("reindex")).
    DependsOn(catbird.Dep[Unit]("listen"))  // Looks like listen produces Summary

// BETTER (clear semantics)
switchStep := DependsOn(catbird.Dep[Summary]("reindex")).
    WaitForPool("index-ops")   // Wait for pool to drain (includes all producers)

// Or even:
pool := TaskPool("index-ops",
    Producers("reindex", "listen-changes"),  // Both spawn to this pool
    Consumers("switch-index"),               // Consumer waits for pool
)
```

But this is beyond the scope of reflection-based API patterns.

## Summary: Type Safety with StepContext

The reflection API provides strong type safety for coordination patterns:

✅ **Build-time type validation via reflection**
```go
// Handler signature validated via reflection against dependency types
NewStep("process").
    DependsOn(Dep[ValidationResult]("validate")).
    WithTaskPool("ops").
    WithHandler(
        func(ctx context.Context, in Request, v ValidationResult) (Response, error) {
            // ✅ v is validated to be ValidationResult at build time via reflection
            // ❌ Wrong signature detected: func(..., v FraudResult) → panic at build time
            stepCtx := catbird.FromContext(ctx)
            stepCtx.SpawnTask(ctx, task)
            return Response{}, nil
        },
    )
```

✅ **Minimal reflection overhead**
```go
// Type validation at build time (once)
// Runtime uses cached reflect.Value wrappers (~1μs overhead vs 1-5ms I/O)
```

✅ **IDE support**
```go
// Full autocomplete, jump-to-definition for API
// Handler function types checked normally (just as any function literal)
```

✅ **Side effects explicit**
```go
// StepContext parameter makes side-effect APIs clear
// pool.SpawnTask() vs dependency parameters vs signal
```

✅ **Clean fluent API**
```go
// Fluent methods without function explosion
step.DependsOn(...).WithTaskPool("pool").WithHandler(fn)
// vs current 8 function variants:
// InitialStep, StepWithDependency, StepWith2Dependencies, etc.
```

## Open Design Question: Producer Step Output

**Remaining ambiguity**: What type do ProducerSteps contribute to the type system?

- ✅ Regular steps: Reflection validates output type against dependencies
- ❌ Producer steps: Return `error` only → what output type?

Recommend:
1. Producer steps return `(Unit, error)` for consistency, or
2. Producer steps are not dependencies (don't use DependsOn), or
3. Separate `WithProducerPool()` semantic that doesn't create dependencies

This needs further design but doesn't block the reflection-based approach.

