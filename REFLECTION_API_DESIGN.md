# Catbird API Redesign: Builder Methods + Cached Reflection

**Date:** February 2026  
**Status:** Design Proposal  
**Goal:** Eliminate step function explosion while maintaining performance and usability

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

### 4. Cached Reflection for Zero Runtime Cost

Handler wrappers are created **once** during construction:

```go
func (t *Task) WithHandler(fn any, opts ...HandlerOpt) *Task {
    // Reflection used ONCE here (build time)
    wrapper, err := makeTaskWrapper(fn, t.Name)
    if err != nil {
        panic(err)
    }
    
    // Store cached wrapper - no reflection at runtime
    t.handler = &taskHandler{fn: wrapper}
    return t
}

// makeTaskWrapper uses reflection once to extract types
func makeTaskWrapper(fn any, name string) (func(context.Context, taskMessage) ([]byte, error), error) {
    fnType := reflect.TypeOf(fn)
    fnVal := reflect.ValueOf(fn)
    
    // Extract types once (build time)
    inputType := fnType.In(1)
    
    // Return wrapper that uses cached type info
    return func(ctx context.Context, msg taskMessage) ([]byte, error) {
        // Runtime: just JSON unmarshal + direct call
        inputVal := reflect.New(inputType).Interface()
        json.Unmarshal(msg.Input, inputVal)
        
        results := fnVal.Call([]reflect.Value{
            reflect.ValueOf(ctx),
            reflect.ValueOf(inputVal).Elem(),
        })
        
        if !results[1].IsNil() {
            return nil, results[1].Interface().(error)
        }
        return json.Marshal(results[0].Interface())
    }, nil
}
```

**Performance:**
- Build time: ~10-50μs per handler (reflection + validation)
- Runtime: ~0.5-1μs for `reflect.Value.Call()` overhead
- Context: PostgreSQL I/O dominates at ~1-5ms
- Overhead: ~0.02-0.1% of total latency (acceptable)

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
func makeTaskWrapper(fn any, taskName string) (func(context.Context, taskMessage) ([]byte, error), error) {
    fnType := reflect.TypeOf(fn)
    
    // Validate signature: (context.Context, In) -> (Out, error)
    if fnType.NumIn() != 2 || fnType.NumOut() != 2 {
        return nil, fmt.Errorf("invalid signature")
    }
    
    // Extract type info once
    inputType := fnType.In(1)
    fnVal := reflect.ValueOf(fn)
    
    // Return cached wrapper
    return func(ctx context.Context, msg taskMessage) ([]byte, error) {
        inputVal := reflect.New(inputType).Interface()
        if err := json.Unmarshal(msg.Input, inputVal); err != nil {
            return nil, err
        }
        
        results := fnVal.Call([]reflect.Value{
            reflect.ValueOf(ctx),
            reflect.ValueOf(inputVal).Elem(),
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
func makeStepWrapper(fn any, step *Step) (func(context.Context, stepMessage) ([]byte, error), error) {
    fnType := reflect.TypeOf(fn)
    
    // Expected: context.Context, In, [SigIn if hasSignal], [Dep1, Dep2, ...]
    expectedInputs := 2
    if step.hasSignal {
        expectedInputs++
    }
    expectedInputs += len(step.dependencies)
    
    if fnType.NumIn() != expectedInputs {
        return nil, fmt.Errorf("expected %d inputs, got %d", expectedInputs, fnType.NumIn())
    }
    
    // Validate Optional[T] for optional dependencies
    depStartIdx := 2
    if step.hasSignal {
        depStartIdx = 3
    }
    
    for i, dep := range step.dependencies {
        paramType := fnType.In(depStartIdx + i)
        isOptional := paramType.Name() == "Optional"
        
        if dep.Optional && !isOptional {
            return nil, fmt.Errorf("dependency %s is optional but parameter is not Optional[T]", dep.Name)
        }
    }
    
    // Extract type info once
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
    
    // Return cached wrapper
    return func(ctx context.Context, msg stepMessage) ([]byte, error) {
        args := []reflect.Value{reflect.ValueOf(ctx)}
        
        // Unmarshal input
        inputVal := reflect.New(inputType).Interface()
        json.Unmarshal(msg.Input, inputVal)
        args = append(args, reflect.ValueOf(inputVal).Elem())
        
        // Unmarshal signal if present
        if step.hasSignal {
            signalVal := reflect.New(signalType).Interface()
            json.Unmarshal(msg.SignalInput, signalVal)
            args = append(args, reflect.ValueOf(signalVal).Elem())
        }
        
        // Unmarshal dependencies
        for i, dep := range step.dependencies {
            if dep.Optional {
                // Handle Optional[T]
                optVal := reflect.New(depTypes[i]).Elem()
                if depOutput, exists := msg.StepOutputs[dep.Name]; exists && len(depOutput) > 0 {
                    optVal.FieldByName("IsSet").SetBool(true)
                    valueField := optVal.FieldByName("Value")
                    valuePtr := reflect.New(valueField.Type()).Interface()
                    json.Unmarshal(depOutput, valuePtr)
                    valueField.Set(reflect.ValueOf(valuePtr).Elem())
                }
                args = append(args, optVal)
            } else {
                // Handle required dependency
                depVal := reflect.New(depTypes[i]).Interface()
                json.Unmarshal(msg.StepOutputs[dep.Name], depVal)
                args = append(args, reflect.ValueOf(depVal).Elem())
            }
        }
        
        // Call handler
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
