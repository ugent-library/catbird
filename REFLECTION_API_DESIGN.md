# Catbird API Redesign: Builder Methods + Cached Reflection

**Date:** February 2026  
**Status:** Approved Design  
**Goal:** Eliminate step function explosion while maintaining performance and usability

**Decision:** Reflection API selected over generic methods (impossible in Go) and hybrid approaches (doesn't solve function explosion).

## Problem Statement

Current Catbird API has several issues:

1. **Function explosion**: 8+ step constructor variants (`InitialStep`, `StepWithDependency`, `StepWithTwoDependencies`, etc.)
2. **Handler always required**: Cannot create task/step without handler (metadata-only use cases)
3. **Handler options mixed**: Handler execution options (concurrency, retries) mixed with task/step metadata options
4. **Not extensible**: Adding 4+ dependencies requires 8 new functions (with/without signal variants)

## Solution: Builder Methods + Cached Reflection

### Core Principles

1. ✅ **Handler is optional** - tasks/steps can be created without handlers
2. ✅ **Handler gets its own options** - scoped inside `WithHandler(fn, opts...)`
3. ✅ **Divergent options** - Task, Step, Flow each have their own configuration methods
4. ✅ **Single package** - all in `catbird` package (worker can access private state)
5. ✅ **Zero runtime reflection cost** - cached wrappers built once at construction time

### API Overview

```go
// TASK - Builder with methods
task := catbird.NewTask("name").
    WithPriority(10).
    WithTags("tag1", "tag2").
    WithHandler(
        func(ctx context.Context, in InputType) (OutputType, error) {
            return output, nil
        },
        catbird.WithConcurrency(10),
        catbird.WithMaxRetries(3),
        catbird.WithCondition("input.amount gte 100"),
    )

// STEP - Builder with methods
step := catbird.NewStep("name").
    DependsOn(catbird.Dependency("step1")).
    RequiresSignal().
    WithHandler(
        func(ctx context.Context, in InputType, dep1 Dep1Type) (OutputType, error) {
            return output, nil
        },
        catbird.WithConcurrency(5),
    )

// FLOW - Builder with methods
flow := catbird.NewFlow("name").
    WithDescription("Description").
    WithTimeout(5 * time.Minute).
    AddStep(step1).
    AddStep(step2).
    AddStep(step3)
```

## Key Design Decisions

### 1. Methods Eliminate Package Prefix Repetition

**Before (functional options):**
```go
catbird.NewTask("name",
    catbird.WithPriority(10),      // catbird. repeated
    catbird.WithTags("a"),         // catbird. repeated
    catbird.TaskWithHandler(...)   // catbird. repeated
)
```

**After (builder methods):**
```go
catbird.NewTask("name").
    WithPriority(10).      // No prefix!
    WithTags("a").         // No prefix!
    WithHandler(...)       // No prefix!
```

### 2. Methods Eliminate Name Collisions

**Before (function collision):**
```go
func TaskWithHandler(...) TaskOpt       // Prefix required
func StepWithHandler(...) StepOpt       // to avoid collision
```

**After (method - no collision):**
```go
func (t *Task) WithHandler(...) *Task   // Different receiver
func (s *Step) WithHandler(...) *Step   // No collision!
```

### 3. Single Package Solves Private State

All types in `catbird` package means worker can access private fields:

```go
// worker.go
func (w *Worker) executeTask(task *Task, msg taskMessage) error {
    // ✅ Can access task.handler (private field, same package)
    return task.handler.fn(ctx, msg)
}
```

### 4. Cached Reflection for Minimal Runtime Cost

**Key Strategy:** Reflection used ONCE at construction time, cached for all runtime executions.

Handler wrappers are created **once** during construction:

```go
func (t *Task) WithHandler(fn any, opts ...HandlerOpt) *Task {
    // PHASE 1: Reflection used ONCE here (build/construction time)
    // - Validate signature
    // - Extract type information
    // - Build optimized wrapper
    wrapper, err := makeTaskWrapper(fn, t.Name)
    if err != nil {
        panic(err) // Fail fast at build time
    }
    
    // PHASE 2: Store cached wrapper - zero reflection at runtime
    t.handler = &taskHandler{fn: wrapper}
    return t
}

// makeTaskWrapper uses reflection once to extract types and create cached wrapper
func makeTaskWrapper(fn any, name string) (func(context.Context, taskMessage) ([]byte, error), error) {
    fnType := reflect.TypeOf(fn)
    fnVal := reflect.ValueOf(fn)
    
    // Validate signature at build time
    if fnType.Kind() != reflect.Func {
        return nil, fmt.Errorf("handler must be a function")
    }
    if fnType.NumIn() != 2 || fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
        return nil, fmt.Errorf("handler must have signature (context.Context, In) (Out, error)")
    }
    if fnType.NumOut() != 2 || !fnType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
        return nil, fmt.Errorf("handler must return (Out, error)")
    }
    
    // Extract types once (build time) - CACHED
    inputType := fnType.In(1)
    
    // Return wrapper with all type info cached in closure
    return func(ctx context.Context, msg taskMessage) ([]byte, error) {
        // RUNTIME PATH - minimal overhead:
        // 1. Allocate input value using cached type
        inputVal := reflect.New(inputType)
        
        // 2. Unmarshal JSON (unavoidable cost)
        if err := json.Unmarshal(msg.Input, inputVal.Interface()); err != nil {
            return nil, fmt.Errorf("unmarshal input: %w", err)
        }
        
        // 3. Call handler via cached reflect.Value (~0.5-1μs overhead)
        results := fnVal.Call([]reflect.Value{
            reflect.ValueOf(ctx),
            inputVal.Elem(),
        })
        
        // 4. Check error
        if !results[1].IsNil() {
            return nil, results[1].Interface().(error)
        }
        
        // 5. Marshal output (unavoidable cost)
        return json.Marshal(results[0].Interface())
    }, nil
}
```

**Performance Characteristics:**

| Phase | Cost | When | Notes |
|-------|------|------|-------|
| **Build time** (once) | ~10-50μs | `WithHandler()` call | Reflection introspection + validation |
| **Runtime** (per execution) | ~0.5-1μs | Handler execution | `reflect.Value.Call()` overhead only |
| **Context** | ~1-5ms | Handler execution | PostgreSQL I/O (dominates) |
| **Overhead** | 0.02-0.1% | Per execution | Negligible vs total latency |

**Optimization:** All reflection introspection happens once. Runtime only pays for `reflect.Value.Call()`, which is ~50-100x faster than re-introspecting types.

## Type Structure

```go
package catbird

// ========================================
// TASK
// ========================================

type Task struct {
    Name     string
    priority int
    tags     []string
    handler  *taskHandler  // private
}

func NewTask(name string) *Task

func (t *Task) WithPriority(p int) *Task
func (t *Task) WithTags(tags ...string) *Task
func (t *Task) WithHandler(fn any, opts ...HandlerOpt) *Task

// ========================================
// STEP
// ========================================

type Step struct {
    Name         string
    dependencies []*StepDependency
    hasSignal    bool
    handler      *stepHandler  // private
}

func NewStep(name string) *Step

func (s *Step) DependsOn(deps ...*StepDependency) *Step
func (s *Step) RequiresSignal() *Step
func (s *Step) WithHandler(fn any, opts ...HandlerOpt) *Step

// ========================================
// FLOW
// ========================================

type Flow struct {
    Name        string
    description string
    version     string
    timeout     time.Duration
    Steps       []*Step
}

func NewFlow(name string) *Flow

func (f *Flow) WithDescription(desc string) *Flow
func (f *Flow) WithVersion(v string) *Flow
func (f *Flow) WithTimeout(d time.Duration) *Flow
func (f *Flow) AddStep(step *Step) *Flow

// ========================================
// HANDLER OPTIONS (shared)
// ========================================

type HandlerOpt func(*handlerOpts)

func WithConcurrency(n int) HandlerOpt
func WithMaxRetries(n int) HandlerOpt
func WithBackoff(min, max time.Duration) HandlerOpt
func WithCircuitBreaker(failures int, timeout time.Duration) HandlerOpt
func WithCondition(expr string) HandlerOpt
```

## Usage Examples

### Task Without Handler
```go
// Metadata-only task (manual processing)
task := catbird.NewTask("manual_review").
    WithPriority(10).
    WithTags("manual", "review")

catbird.CreateTask(ctx, client, task)
```

### Task With Handler
```go
task := catbird.NewTask("process_payment").
    WithPriority(5).
    WithHandler(
        func(ctx context.Context, req PaymentRequest) (PaymentResult, error) {
            return PaymentResult{Status: "completed"}, nil
        },
        catbird.WithConcurrency(10),
        catbird.WithMaxRetries(3),
        catbird.WithCondition("input.amount gte 100"),
    )

catbird.CreateTask(ctx, client, task)
```

### Step Without Handler
```go
// Manual approval step (requires signal, no handler)
step := catbird.NewStep("manual_approval").
    DependsOn(catbird.Dependency("validate")).
    RequiresSignal()
```

### Step With Handler
```go
step := catbird.NewStep("validate").
    WithHandler(
        func(ctx context.Context, req PaymentRequest) (ValidationResult, error) {
            return ValidationResult{Valid: req.Amount > 0}, nil
        },
        catbird.WithConcurrency(5),
    )
```

### Step With Dependencies and Signal
```go
step := catbird.NewStep("finalize").
    DependsOn(
        catbird.OptionalDependency("fraud_check"),  // Optional (conditional step)
        catbird.Dependency("manual_approval"),      // Required
    ).
    WithHandler(
        func(
            ctx context.Context,
            req PaymentRequest,
            approval ApprovalInput,                        // Signal input
            fraud catbird.Optional[FraudCheckResult],      // Optional dependency
            manualResult string,                           // Required dependency
        ) (PaymentResult, error) {
            status := "completed"
            if fraud.IsSet && fraud.Value.Flagged {
                status = "flagged"
            }
            return PaymentResult{Status: status}, nil
        },
    )
```

### Flow Composition
```go
// Build steps separately
validateStep := catbird.NewStep("validate").
    WithHandler(validateFn, catbird.WithConcurrency(5))

fraudStep := catbird.NewStep("fraud").
    DependsOn(catbird.Dependency("validate")).
    WithHandler(fraudFn)

approvalStep := catbird.NewStep("approve").
    DependsOn(catbird.Dependency("validate")).
    RequiresSignal()

// Compose into flow
flow := catbird.NewFlow("payment_workflow").
    WithDescription("Payment processing").
    WithTimeout(5 * time.Minute).
    AddStep(validateStep).
    AddStep(fraudStep).
    AddStep(approvalStep)

catbird.CreateFlow(ctx, client, flow)
```

### Step Reusability
```go
// Same steps used in multiple flows
validateStep := catbird.NewStep("validate").WithHandler(...)
fraudStep := catbird.NewStep("fraud").DependsOn(...).WithHandler(...)

basicFlow := catbird.NewFlow("basic").
    AddStep(validateStep)

premiumFlow := catbird.NewFlow("premium").
    AddStep(validateStep).
    AddStep(fraudStep)
```

### Conditional Flow Assembly
```go
flow := catbird.NewFlow("dynamic").
    AddStep(validateStep)

if isProduction() {
    flow.AddStep(fraudStep)
}

if needsApproval() {
    flow.AddStep(approvalStep)
}

flow.AddStep(finalizeStep)
```

## Implementation Sketch

### Task Handler Wrapper
```go
// makeTaskWrapper creates cached wrapper for task handlers
// Reflection used ONCE during this call, all type info cached in returned closure
func makeTaskWrapper(fn any, taskName string) (func(context.Context, taskMessage) ([]byte, error), error) {
    fnType := reflect.TypeOf(fn)
    
    // Validate signature: (context.Context, In) -> (Out, error)
    if fnType.Kind() != reflect.Func {
        return nil, fmt.Errorf("task %s: handler must be a function, got %T", taskName, fn)
    }
    if fnType.NumIn() != 2 {
        return nil, fmt.Errorf("task %s: handler must have 2 inputs (context.Context, In), got %d", taskName, fnType.NumIn())
    }
    if fnType.NumOut() != 2 {
        return nil, fmt.Errorf("task %s: handler must have 2 outputs (Out, error), got %d", taskName, fnType.NumOut())
    }
    if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
        return nil, fmt.Errorf("task %s: first parameter must be context.Context, got %v", taskName, fnType.In(0))
    }
    errorType := reflect.TypeOf((*error)(nil)).Elem()
    if !fnType.Out(1).Implements(errorType) {
        return nil, fmt.Errorf("task %s: second return value must be error, got %v", taskName, fnType.Out(1))
    }
    
    // CACHE: Extract type info once - stored in closure
    inputType := fnType.In(1)
    fnVal := reflect.ValueOf(fn)
    
    // Return wrapper that uses cached fnVal and inputType
    return func(ctx context.Context, msg taskMessage) ([]byte, error) {
        // Allocate input using cached type
        inputVal := reflect.New(inputType)
        
        if err := json.Unmarshal(msg.Input, inputVal.Interface()); err != nil {
            return nil, fmt.Errorf("unmarshal input for task %s: %w", taskName, err)
        }
        
        // Call using cached fnVal (minimal overhead)
        results := fnVal.Call([]reflect.Value{
            reflect.ValueOf(ctx),
            inputVal.Elem(),
        })
        
        if !results[1].IsNil() {
            return nil, results[1].Interface().(error)
        }
        
        return json.Marshal(results[0].Interface())
    }, nil
}
```

### Step Handler Wrapper
```go
// makeStepWrapper creates cached wrapper for step handlers
// Reflection used ONCE during this call, all type info cached in returned closure
func makeStepWrapper(fn any, step *Step) (func(context.Context, stepMessage) ([]byte, error), error) {
    fnType := reflect.TypeOf(fn)
    
    if fnType.Kind() != reflect.Func {
        return nil, fmt.Errorf("step %s: handler must be a function, got %T", step.Name, fn)
    }
    
    // Expected signature: (context.Context, In, [SigIn if hasSignal], [Dep1, Dep2, ...])
    expectedInputs := 2
    if step.hasSignal {
        expectedInputs++
    }
    expectedInputs += len(step.dependencies)
    
    if fnType.NumIn() != expectedInputs {
        return nil, fmt.Errorf("step %s: expected %d inputs, got %d", step.Name, expectedInputs, fnType.NumIn())
    }
    
    // Validate context.Context is first parameter
    if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
        return nil, fmt.Errorf("step %s: first parameter must be context.Context, got %v", step.Name, fnType.In(0))
    }
    
    // Validate error return
    errorType := reflect.TypeOf((*error)(nil)).Elem()
    if fnType.NumOut() != 2 || !fnType.Out(1).Implements(errorType) {
        return nil, fmt.Errorf("step %s: must return (Out, error)", step.Name)
    }
    
    // Determine parameter indices
    depStartIdx := 2
    if step.hasSignal {
        depStartIdx = 3
    }
    
    // Validate Optional[T] for optional dependencies
    for i, dep := range step.dependencies {
        paramType := fnType.In(depStartIdx + i)
        // Check if type name contains "Optional" (simplified check)
        isOptional := paramType.Kind() == reflect.Struct && paramType.NumField() >= 2 &&
            paramType.Field(0).Name == "IsSet" && paramType.Field(1).Name == "Value"
        
        if dep.Optional && !isOptional {
            return nil, fmt.Errorf("step %s: dependency %s is optional but parameter %d is not Optional[T]", 
                step.Name, dep.Name, depStartIdx+i)
        }
        if !dep.Optional && isOptional {
            return nil, fmt.Errorf("step %s: dependency %s is required but parameter %d is Optional[T]", 
                step.Name, dep.Name, depStartIdx+i)
        }
    }
    
    // CACHE: Extract all type info once - stored in closure
    inputType := fnType.In(1)
    var signalType reflect.Type
    if step.hasSignal {
        signalType = fnType.In(2)
    }
    
    depTypes := make([]reflect.Type, len(step.dependencies))
    for i := range step.dependencies {
        depTypes[i] = fnType.In(depStartIdx + i)
    }
    
    fnVal := reflect.ValueOf(fn)
    deps := step.dependencies // Cache dependencies slice
    hasSignal := step.hasSignal
    
    // Return wrapper with all type info cached
    return func(ctx context.Context, msg stepMessage) ([]byte, error) {
        args := []reflect.Value{reflect.ValueOf(ctx)}
        
        // Unmarshal input using cached type
        inputVal := reflect.New(inputType)
        if err := json.Unmarshal(msg.Input, inputVal.Interface()); err != nil {
            return nil, fmt.Errorf("unmarshal input: %w", err)
        }
        args = append(args, inputVal.Elem())
        
        // Unmarshal signal if present (uses cached signalType)
        if hasSignal {
            signalVal := reflect.New(signalType)
            if err := json.Unmarshal(msg.SignalInput, signalVal.Interface()); err != nil {
                return nil, fmt.Errorf("unmarshal signal: %w", err)
            }
            args = append(args, signalVal.Elem())
        }
        
        // Unmarshal dependencies using cached depTypes
        for i, dep := range deps {
            if dep.Optional {
                // Handle Optional[T] - construct directly
                optVal := reflect.New(depTypes[i]).Elem()
                if depOutput, exists := msg.StepOutputs[dep.Name]; exists && len(depOutput) > 0 {
                    // Set IsSet = true
                    optVal.FieldByName("IsSet").SetBool(true)
                    // Unmarshal into Value field
                    valueField := optVal.FieldByName("Value")
                    valuePtr := reflect.New(valueField.Type())
                    if err := json.Unmarshal(depOutput, valuePtr.Interface()); err != nil {
                        return nil, fmt.Errorf("unmarshal optional dependency %s: %w", dep.Name, err)
                    }
                    valueField.Set(valuePtr.Elem())
                }
                // If not exists, optVal stays as Optional{IsSet: false}
                args = append(args, optVal)
            } else {
                // Handle required dependency
                depVal := reflect.New(depTypes[i])
                if err := json.Unmarshal(msg.StepOutputs[dep.Name], depVal.Interface()); err != nil {
                    return nil, fmt.Errorf("unmarshal dependency %s: %w", dep.Name, err)
                }
                args = append(args, depVal.Elem())
            }
        }
        
        // Call handler using cached fnVal
        results := fnVal.Call(args)
        
        if !results[1].IsNil() {
            return nil, results[1].Interface().(error)
        }
        
        return json.Marshal(results[0].Interface())
    }, nil
}
```

## Migration Strategy

### Phase 1: Add New API (Parallel)
- Implement new builder methods alongside existing API
- Mark existing functions as deprecated
- Update documentation with new patterns

### Phase 2: Internal Migration
- Migrate tests to new API
- Update examples in README
- Verify performance benchmarks

### Phase 3: Deprecation
- Remove old step constructor variants
- Keep backward compatibility via adapter layer (optional)
- Publish migration guide

## Benefits Summary

### ✅ Addresses All Constraints
1. **Handler optional**: Just don't call `.WithHandler()`
2. **Handler own options**: Scoped inside `WithHandler(fn, opts...)`
3. **Divergent options**: Different methods on Task, Step, Flow

### ✅ API Improvements
- No package prefix repetition (methods)
- No name collisions (different receivers)
- No function explosion (single `NewStep`)
- Step reusability (build once, use in multiple flows)
- Conditional flow assembly (dynamic `AddStep`)

### ✅ Implementation Benefits
- Single package (no subpackage complexity)
- Private state accessible (worker in same package)
- Cached reflection (acceptable ~1μs overhead)
- Build-time validation (fail fast with clear errors)

### ✅ Extensibility
- Add new options as methods (no breaking changes)
- Support 4+ dependencies (no new functions needed)
- Easy to add task/step/flow-specific features

## Open Questions

1. **Validation timing**: When to validate flow structure?
   - Option A: At `CreateFlow()` time (current)
   - Option B: At `AddStep()` time (fail fast)
   - Option C: Lazy (at first `RunFlow()`)

2. **Immutability**: Should builders return new instances?
   - Current: Mutable (modifies in place)
   - Alternative: Immutable (returns copy)

3. **Zero values**: What if `WithHandler` called twice?
   - Current: Last one wins
   - Alternative: Panic on second call

## Next Steps

1. [ ] Implement core types (Task, Step, Flow)
2. [ ] Implement cached wrapper functions
3. [ ] Add validation logic
4. [ ] Write comprehensive tests
5. [ ] Benchmark performance vs current API
6. [ ] Update documentation
7. [ ] Create migration guide

---

**End of Design Sketch**
