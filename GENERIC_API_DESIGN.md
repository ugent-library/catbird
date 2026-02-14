# Catbird API Redesign: Typed Generic Builder Pattern (Zero Reflection)

**Date:** February 2026  
**Status:** Complete Design Proposal  
**Goal:** Solve API problems with compile-time type safety via typed builder that carries dependency types

## Problem Statement

Same problems as builder design:

1. **Function explosion**: 8+ step constructor variants (`InitialStep`, `StepWithDependency`, `StepWithTwoDependencies`, etc.)
2. **Handler always required**: Cannot create task/step without handler (metadata-only use cases)
3. **Handler options mixed**: Handler execution options (concurrency, retries) mixed with task/step metadata options
4. **Not extensible**: Adding 4+ dependencies requires exponential function growth

## Solution: Typed Builder Pattern (Zero Reflection!)

### Core Principles

1. ✅ **Handler is optional** - just don't call `WithHandler` method
2. ✅ **Handler gets its own options** - scoped inside `WithHandler(fn, opts...)`
3. ✅ **Divergent options** - Different builder types for Task, Step, Flow
4. ✅ **Generic type safety** - Input/Output types enforced at compile time
5. ✅ **Dependency type safety** - Builder type carries dependency types
6. ✅ **ZERO reflection** - Builder type ensures handler signature matches at compile time
7. ✅ **No function explosion** - Single builder, type changes as you add deps

### API Overview

```go
// TASK - Typed builder
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

// STEP - Typed builder (type changes as dependencies added!)
step := catbird.NewStep("name").
    DependsOn[Dep1Type]("step1").          // Returns StepBuilder1[Dep1Type]
    DependsOn[Dep2Type]("step2").          // Returns StepBuilder2[Dep1Type, Dep2Type]
    WithHandler(                           // Handler signature MUST match dependency types
        func(ctx context.Context, in InputType, dep1 Dep1Type, dep2 Dep2Type) (OutputType, error) {
            return output, nil
        },
        catbird.WithConcurrency(5),
    )

// FLOW - Standard builder (no generics needed)
flow := catbird.NewFlow("name").
    WithDescription("Description").
    WithTimeout(5 * time.Minute).
    AddStep(step1).
    AddStep(step2).
    AddStep(step3)
```

## Key Design Decisions

### 1. Typed Builder Pattern Eliminates Reflection Entirely

The builder's **type changes** as you add dependencies, carrying type information at compile time:

```go
// StepBuilder0 - no dependencies
type StepBuilder0 struct {
    name string
    // ... other metadata
}

func NewStep(name string) *StepBuilder0

// StepBuilder1 - one dependency
type StepBuilder1[D1 any] struct {
    name string
    dep1Name string
    // ... other metadata
}

func (s *StepBuilder0) DependsOn[D1 any](name string) *StepBuilder1[D1] {
    return &StepBuilder1[D1]{
        name:     s.name,
        dep1Name: name,
    }
}

// StepBuilder2 - two dependencies
type StepBuilder2[D1, D2 any] struct {
    name string
    dep1Name string
    dep2Name string
}

func (s *StepBuilder1[D1]) DependsOn[D2 any](name string) *StepBuilder2[D1, D2] {
    return &StepBuilder2[D1, D2]{
        name:     s.name,
        dep1Name: s.dep1Name,
        dep2Name: name,
    }
}

// WithHandler on each builder - handler signature MUST match!
func (s *StepBuilder0) WithHandler[In, Out any](
    fn func(context.Context, In) (Out, error),
    opts ...HandlerOpt,
) *Step {
    // NO REFLECTION! Handler type known at compile time
    wrapper := func(ctx context.Context, msg stepMessage) ([]byte, error) {
        var input In
        if err := json.Unmarshal(msg.Input, &input); err != nil {
            return nil, err
        }
        
        output, err := fn(ctx, input)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{name: s.name, handler: wrapper}
}

func (s *StepBuilder1[D1]) WithHandler[In, Out any](
    fn func(context.Context, In, D1) (Out, error),  // Signature enforced by D1 type!
    opts ...HandlerOpt,
) *Step {
    // NO REFLECTION! Types known at compile time
    wrapper := func(ctx context.Context, msg stepMessage) ([]byte, error) {
        var input In
        var dep1 D1
        
        if err := json.Unmarshal(msg.Input, &input); err != nil {
            return nil, err
        }
        if err := json.Unmarshal(msg.Dependencies[s.dep1Name], &dep1); err != nil {
            return nil, err
        }
        
        output, err := fn(ctx, input, dep1)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{
        name:         s.name,
        handler:      wrapper,
        dependencies: []string{s.dep1Name},
    }
}

func (s *StepBuilder2[D1, D2]) WithHandler[In, Out any](
    fn func(context.Context, In, D1, D2) (Out, error),  // Enforced by D1, D2 types!
    opts ...HandlerOpt,
) *Step {
    // NO REFLECTION! Types known at compile time
    wrapper := func(ctx context.Context, msg stepMessage) ([]byte, error) {
        var input In
        var dep1 D1
        var dep2 D2
        
        json.Unmarshal(msg.Input, &input)
        json.Unmarshal(msg.Dependencies[s.dep1Name], &dep1)
        json.Unmarshal(msg.Dependencies[s.dep2Name], &dep2)
        
        output, err := fn(ctx, input, dep1, dep2)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{
        name:         s.name,
        handler:      wrapper,
        dependencies: []string{s.dep1Name, s.dep2Name},
    }
}
```

**Key Insight**: The type parameter `D1, D2, ...` on the builder **forces** the handler signature to match. Wrong signature = compile error!

```go
step := catbird.NewStep("finalize").
    DependsOn[ValidationResult]("validate").  // Builder is now StepBuilder1[ValidationResult]
    WithHandler(
        // ✅ This compiles - signature matches
        func(ctx context.Context, in PaymentRequest, v ValidationResult) (Result, error) {
            return Result{}, nil
        },
    )

step := catbird.NewStep("finalize").
    DependsOn[ValidationResult]("validate").  // Builder is StepBuilder1[ValidationResult]
    WithHandler(
        // ❌ Compile error! Expected ValidationResult, got FraudResult
        func(ctx context.Context, in PaymentRequest, f FraudResult) (Result, error) {
            return Result{}, nil
        },
    )
```

### 2. No Function Explosion Despite Multiple Builder Types

Unlike the old approach with 8+ `InitialStep`, `StepWithDependency` functions, this has:
- **One constructor**: `NewStep(name)` 
- **One dependency method**: `DependsOn[T](name)` (generic, works for any type)
- **One handler method per builder**: `WithHandler[In, Out](fn)` (generic, works for any In/Out)

The explosion is in **types** (StepBuilder0, StepBuilder1, ...) not **function names** the user sees.

### 3. Builder Methods Eliminate Package Prefix Repetition

**Before (functional options):**
```go
catbird.NewTask("name",
    catbird.WithPriority(10),      // catbird. repeated
    catbird.WithTags("a"),         // catbird. repeated
    catbird.WithTaskHandler(...)   // catbird. repeated
)
```

**After (typed builder):**
```go
catbird.NewTask("name").
    WithPriority(10).      // No prefix!
    WithTags("a").         // No prefix!
    WithHandler(...)       // No prefix!
```

### 4. Signal Support

Signals are handled via separate builder variants:

```go
// StepBuilder0WithSignal[S]
func (s *StepBuilder0) RequiresSignal[S any]() *StepBuilder0WithSignal[S]

func (s *StepBuilder0WithSignal[S]) WithHandler[In, Out any](
    fn func(context.Context, In, S) (Out, error),  // Signature includes S!
    opts ...HandlerOpt,
) *Step

// StepBuilder1WithSignal[D1, S]
func (s *StepBuilder1[D1]) RequiresSignal[S any]() *StepBuilder1WithSignal[D1, S]

func (s *StepBuilder1WithSignal[D1, S]) WithHandler[In, Out any](
    fn func(context.Context, In, S, D1) (Out, error),  // Signature includes S and D1!
    opts ...HandlerOpt,
) *Step
```

Usage:
```go
step := catbird.NewStep("approve").
    DependsOn[ValidationResult]("validate").
    RequiresSignal[ApprovalInput]().  // Builder now has both D1 and S types
    WithHandler(
        // Signature MUST be: (ctx, In, ApprovalInput, ValidationResult) -> (Out, error)
        func(ctx context.Context, in PaymentRequest, sig ApprovalInput, v ValidationResult) (Result, error) {
            return Result{ApprovedBy: sig.ApproverID}, nil
        },
    )
```

### 5. Optional Dependencies

Optional dependencies use a different builder method and type:

```go
func (s *StepBuilder1[D1]) DependsOnOptional[D2 any](name string) *StepBuilder2Optional[D1, D2]

func (s *StepBuilder2Optional[D1, D2]) WithHandler[In, Out any](
    fn func(context.Context, In, D1, catbird.Optional[D2]) (Out, error),  // D2 wrapped in Optional!
    opts ...HandlerOpt,
) *Step
```

Usage:
```go
step := catbird.NewStep("finalize").
    DependsOn[ValidationResult]("validate").           // Required
    DependsOnOptional[FraudResult]("fraud_check").     // Optional (conditional step)
    WithHandler(
        // Signature MUST include Optional[FraudResult]
        func(
            ctx context.Context,
            in PaymentRequest,
            v ValidationResult,
            fraud catbird.Optional[FraudResult],  // Enforced by type!
        ) (Result, error) {
            if fraud.IsSet {
                // Use fraud.Value
            }
            return Result{}, nil
        },
    )
```

## Type Structure

```go
package catbird

// ========================================
// TASK
// ========================================

type TaskBuilder struct {
    name string
    priority int
    tags []string
}

func NewTask(name string) *TaskBuilder

func (t *TaskBuilder) WithPriority(p int) *TaskBuilder {
    t.priority = p
    return t
}

func (t *TaskBuilder) WithTags(tags ...string) *TaskBuilder {
    t.tags = tags
    return t
}

func (t *TaskBuilder) WithHandler[In, Out any](
    fn func(context.Context, In) (Out, error),
    opts ...HandlerOpt,
) *Task

// ========================================
// STEP BUILDERS (No Dependencies)
// ========================================

type StepBuilder0 struct {
    name string
    // ... other metadata
}

func NewStep(name string) *StepBuilder0

func (s *StepBuilder0) DependsOn[D1 any](name string) *StepBuilder1[D1]
func (s *StepBuilder0) DependsOnOptional[D1 any](name string) *StepBuilder1Optional[D1]
func (s *StepBuilder0) RequiresSignal[S any]() *StepBuilder0WithSignal[S]
func (s *StepBuilder0) WithHandler[In, Out any](
    fn func(context.Context, In) (Out, error),
    opts ...HandlerOpt,
) *Step

// ========================================
// STEP BUILDERS (One Dependency)
// ========================================

type StepBuilder1[D1 any] struct {
    name     string
    dep1Name string
}

func (s *StepBuilder1[D1]) DependsOn[D2 any](name string) *StepBuilder2[D1, D2]
func (s *StepBuilder1[D1]) DependsOnOptional[D2 any](name string) *StepBuilder2Optional[D1, D2]
func (s *StepBuilder1[D1]) RequiresSignal[S any]() *StepBuilder1WithSignal[D1, S]
func (s *StepBuilder1[D1]) WithHandler[In, Out any](
    fn func(context.Context, In, D1) (Out, error),
    opts ...HandlerOpt,
) *Step

type StepBuilder1Optional[D1 any] struct {
    name     string
    dep1Name string
}

func (s *StepBuilder1Optional[D1]) DependsOn[D2 any](name string) *StepBuilder2Mixed[D1, D2]
func (s *StepBuilder1Optional[D1]) WithHandler[In, Out any](
    fn func(context.Context, In, Optional[D1]) (Out, error),
    opts ...HandlerOpt,
) *Step

// ========================================
// STEP BUILDERS (Two Dependencies)
// ========================================

type StepBuilder2[D1, D2 any] struct {
    name     string
    dep1Name string
    dep2Name string
}

func (s *StepBuilder2[D1, D2]) DependsOn[D3 any](name string) *StepBuilder3[D1, D2, D3]
func (s *StepBuilder2[D1, D2]) RequiresSignal[S any]() *StepBuilder2WithSignal[D1, D2, S]
func (s *StepBuilder2[D1, D2]) WithHandler[In, Out any](
    fn func(context.Context, In, D1, D2) (Out, error),
    opts ...HandlerOpt,
) *Step

type StepBuilder2Optional[D1, D2 any] struct {
    name     string
    dep1Name string
    dep2Name string
}

func (s *StepBuilder2Optional[D1, D2]) WithHandler[In, Out any](
    fn func(context.Context, In, D1, Optional[D2]) (Out, error),
    opts ...HandlerOpt,
) *Step

// ... StepBuilder3, StepBuilder4, etc. up to reasonable limit (8? 12?)

// ========================================
// STEP BUILDERS WITH SIGNAL
// ========================================

type StepBuilder0WithSignal[S any] struct {
    name        string
    signalType  reflect.Type  // cached for metadata
}

func (s *StepBuilder0WithSignal[S]) DependsOn[D1 any](name string) *StepBuilder1WithSignal[D1, S]
func (s *StepBuilder0WithSignal[S]) WithHandler[In, Out any](
    fn func(context.Context, In, S) (Out, error),
    opts ...HandlerOpt,
) *Step

type StepBuilder1WithSignal[D1, S any] struct {
    name     string
    dep1Name string
}

func (s *StepBuilder1WithSignal[D1, S]) DependsOn[D2 any](name string) *StepBuilder2WithSignal[D1, D2, S]
func (s *StepBuilder1WithSignal[D1, S]) WithHandler[In, Out any](
    fn func(context.Context, In, S, D1) (Out, error),
    opts ...HandlerOpt,
) *Step

// ... more WithSignal variants

// ========================================
// STEP (Final Type)
// ========================================

type Step struct {
    name         string
    dependencies []string
    hasSignal    bool
    handler      func(context.Context, stepMessage) ([]byte, error)
}

// ========================================
// FLOW
// ========================================

type FlowBuilder struct {
    name        string
    description string
    timeout     time.Duration
    steps       []*Step
}

func NewFlow(name string) *FlowBuilder

func (f *FlowBuilder) WithDescription(desc string) *FlowBuilder
func (f *FlowBuilder) WithTimeout(d time.Duration) *FlowBuilder
func (f *FlowBuilder) AddStep(step *Step) *FlowBuilder
func (f *FlowBuilder) Build() *Flow  // Optional finalization

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
// Metadata-only task
task := catbird.NewTask("manual_review").
    WithPriority(10).
    WithTags("manual")
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
    )
```

### Step Without Handler (Metadata Only)
```go
step := catbird.NewStep("manual_approval").
    DependsOn[ValidationResult]("validate").
    RequiresSignal[ApprovalInput]()
    // No handler - manual approval via signal
```

### Step With Single Dependency
```go
step := catbird.NewStep("validate").
    WithHandler(
        func(ctx context.Context, req PaymentRequest) (ValidationResult, error) {
            return ValidationResult{Valid: req.Amount > 0}, nil
        },
        catbird.WithConcurrency(5),
    )
```

### Step With Multiple Dependencies
```go
step := catbird.NewStep("finalize").
    DependsOn[ValidationResult]("validate").
    DependsOn[FraudCheckResult]("fraud").
    DependsOn[ApprovalResult]("approval").
    WithHandler(
        func(
            ctx context.Context,
            in PaymentRequest,
            validateOut ValidationResult,
            fraudOut FraudCheckResult,
            approvalOut ApprovalResult,
        ) (PaymentResult, error) {
            return PaymentResult{Status: "completed"}, nil
        },
    )
```

### Step With Optional Dependency
```go
step := catbird.NewStep("finalize").
    DependsOn[ValidationResult]("validate").
    DependsOnOptional[FraudCheckResult]("fraud_check").
    WithHandler(
        func(
            ctx context.Context,
            in PaymentRequest,
            validateOut ValidationResult,
            fraudOut catbird.Optional[FraudCheckResult],
        ) (PaymentResult, error) {
            status := "completed"
            if fraudOut.IsSet && fraudOut.Value.Flagged {
                status = "flagged"
            }
            return PaymentResult{Status: status}, nil
        },
    )
```

### Step With Signal and Dependencies
```go
step := catbird.NewStep("approve").
    DependsOn[ValidationResult]("validate").
    DependsOn[FraudCheckResult]("fraud").
    RequiresSignal[ApprovalInput]().
    WithHandler(
        func(
            ctx context.Context,
            in PaymentRequest,
            sig ApprovalInput,
            validateOut ValidationResult,
            fraudOut FraudCheckResult,
        ) (PaymentResult, error) {
            return PaymentResult{
                Status:     "approved",
                ApprovedBy: sig.ApproverID,
            }, nil
        },
    )
```

### Flow Composition
```go
// Build steps separately
validStep := catbird.NewStep("validate").
    WithHandler(
        func(ctx context.Context, in PaymentRequest) (ValidationResult, error) {
            return ValidationResult{Valid: in.Amount > 0}, nil
        },
        catbird.WithConcurrency(5),
    )

fraudStep := catbird.NewStep("fraud").
    DependsOn[ValidationResult]("validate").
    WithHandler(
        func(ctx context.Context, in PaymentRequest, v ValidationResult) (FraudCheckResult, error) {
            return FraudCheckResult{Flagged: false}, nil
        },
    )

approvalStep := catbird.NewStep("approve").
    DependsOn[ValidationResult]("validate").
    RequiresSignal[ApprovalInput]()

// Compose into flow
flow := catbird.NewFlow("payment_workflow").
    WithDescription("Payment processing").
    WithTimeout(5 * time.Minute).
    AddStep(validStep).
    AddStep(fraudStep).
    AddStep(approvalStep)
```

### Initial Step Pattern
```go
// Initial step has no dependencies, just input
initialStep := catbird.NewStep("initial").
    WithHandler(
        func(ctx context.Context, in PaymentRequest) (PaymentRequest, error) {
            // Just pass through or enrich
            return in, nil
        },
    )
```

## Implementation Sketch

### Zero-Reflection Handler Wrappers

Each builder's `WithHandler` creates a type-specific wrapper with zero reflection:

```go
// StepBuilder0 - No dependencies
func (s *StepBuilder0) WithHandler[In, Out any](
    fn func(context.Context, In) (Out, error),
    opts ...HandlerOpt,
) *Step {
    // NO REFLECTION! All types known at compile time
    wrapper := func(ctx context.Context, msg stepMessage) ([]byte, error) {
        var input In
        if err := json.Unmarshal(msg.Input, &input); err != nil {
            return nil, err
        }
        
        output, err := fn(ctx, input)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{
        name:    s.name,
        handler: wrapper,
    }
}

// StepBuilder1 - One dependency
func (s *StepBuilder1[D1]) WithHandler[In, Out any](
    fn func(context.Context, In, D1) (Out, error),
    opts ...HandlerOpt,
) *Step {
    // NO REFLECTION! D1 type known at compile time
    dep1Name := s.dep1Name
    
    wrapper := func(ctx context.Context, msg stepMessage) ([]byte, error) {
        var input In
        var dep1 D1
        
        if err := json.Unmarshal(msg.Input, &input); err != nil {
            return nil, err
        }
        if err := json.Unmarshal(msg.Dependencies[dep1Name], &dep1); err != nil {
            return nil, err
        }
        
        output, err := fn(ctx, input, dep1)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{
        name:         s.name,
        handler:      wrapper,
        dependencies: []string{dep1Name},
    }
}

// StepBuilder2 - Two dependencies
func (s *StepBuilder2[D1, D2]) WithHandler[In, Out any](
    fn func(context.Context, In, D1, D2) (Out, error),
    opts ...HandlerOpt,
) *Step {
    // NO REFLECTION! D1, D2 types known at compile time
    dep1Name := s.dep1Name
    dep2Name := s.dep2Name
    
    wrapper := func(ctx context.Context, msg stepMessage) ([]byte, error) {
        var input In
        var dep1 D1
        var dep2 D2
        
        json.Unmarshal(msg.Input, &input)
        json.Unmarshal(msg.Dependencies[dep1Name], &dep1)
        json.Unmarshal(msg.Dependencies[dep2Name], &dep2)
        
        output, err := fn(ctx, input, dep1, dep2)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{
        name:         s.name,
        handler:      wrapper,
        dependencies: []string{dep1Name, dep2Name},
    }
}

// StepBuilder1WithSignal - One dependency + signal
func (s *StepBuilder1WithSignal[D1, S]) WithHandler[In, Out any](
    fn func(context.Context, In, S, D1) (Out, error),
    opts ...HandlerOpt,
) *Step {
    // NO REFLECTION! D1, S types known at compile time
    dep1Name := s.dep1Name
    
    wrapper := func(ctx context.Context, msg stepMessage) ([]byte, error) {
        var input In
        var signal S
        var dep1 D1
        
        json.Unmarshal(msg.Input, &input)
        json.Unmarshal(msg.SignalInput, &signal)
        json.Unmarshal(msg.Dependencies[dep1Name], &dep1)
        
        output, err := fn(ctx, input, signal, dep1)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{
        name:         s.name,
        handler:      wrapper,
        dependencies: []string{dep1Name},
        hasSignal:    true,
    }
}

// StepBuilder1Optional - One optional dependency
func (s *StepBuilder1Optional[D1]) WithHandler[In, Out any](
    fn func(context.Context, In, Optional[D1]) (Out, error),
    opts ...HandlerOpt,
) *Step {
    // NO REFLECTION! D1 type known, wrapped in Optional
    dep1Name := s.dep1Name
    
    wrapper := func(ctx context.Context, msg stepMessage) ([]byte, error) {
        var input In
        var dep1Opt Optional[D1]
        
        json.Unmarshal(msg.Input, &input)
        
        // Check if optional dependency was executed
        if depData, exists := msg.Dependencies[dep1Name]; exists && len(depData) > 0 {
            var dep1Value D1
            if err := json.Unmarshal(depData, &dep1Value); err != nil {
                return nil, err
            }
            dep1Opt = Optional[D1]{IsSet: true, Value: dep1Value}
        }
        
        output, err := fn(ctx, input, dep1Opt)
        if err != nil {
            return nil, err
        }
        
        return json.Marshal(output)
    }
    
    return &Step{
        name:         s.name,
        handler:      wrapper,
        dependencies: []string{dep1Name},
    }
}
```

### Compile-Time Type Safety Example

```go
// ✅ This compiles
step := catbird.NewStep("process").
    DependsOn[ValidationResult]("validate").  // StepBuilder1[ValidationResult]
    WithHandler(
        func(ctx context.Context, in Request, v ValidationResult) (Response, error) {
            // v is ValidationResult - compiler knows this!
            return Response{}, nil
        },
    )

// ❌ Compile error: cannot use func literal (type func(context.Context, Request, FraudResult) (Response, error))
//     as type func(context.Context, Request, ValidationResult) (Response, error)
step := catbird.NewStep("process").
    DependsOn[ValidationResult]("validate").  // StepBuilder1[ValidationResult]
    WithHandler(
        func(ctx context.Context, in Request, f FraudResult) (Response, error) {
            //                                    ^^^^^^^^^^^ Wrong type!
            return Response{}, nil
        },
    )

// ❌ Compile error: cannot use func literal (2 params) as type needing 3 params
step := catbird.NewStep("process").
    DependsOn[ValidationResult]("validate").  // StepBuilder1[ValidationResult]
    WithHandler(
        func(ctx context.Context, in Request) (Response, error) {
            // Missing ValidationResult parameter!
            return Response{}, nil
        },
    )
```

## Comparison: Typed Generic Builder vs Reflection Builder

| Aspect | Typed Generic Builder (This Design) | Reflection Builder (BUILDER_API_DESIGN.md) |
|--------|-------------------------------------|---------------------------------------------|
| **Type safety** | ✅ Full compile-time for everything | ⚠️ Runtime validation for handler signature |
| **Reflection at runtime** | ✅ ZERO - all types known at compile time | ⚠️ ~0.5-1μs per call via `reflect.Value.Call()` |
| **Function count** | ✅ Single `NewStep()`, single `DependsOn[T]()` | ✅ Single `NewStep()`, single `WithHandler()` |
| **Handler optional** | ✅ Via `WithHandler` method | ✅ Via `WithHandler` method |
| **Handler options scoped** | ✅ Nested in `WithHandler` | ✅ Nested in `WithHandler` |
| **Divergent options** | ✅ Via methods on different builders | ✅ Via methods on different receivers |
| **Type safety guarantees** | ✅ Wrong handler signature = compile error | ⚠️ Wrong handler signature = runtime panic |
| **Package prefix** | ✅ Eliminated via methods | ✅ Eliminated via methods |
| **Name collisions** | ✅ Eliminated via receiver types | ✅ Eliminated via receiver types |
| **Extensibility** | ✅ Add methods without breaking changes | ✅ Add methods without breaking changes |
| **Implementation complexity** | ⚠️ More builder types (0, 1, 2, ..., N deps) | ✅ Single Step type with reflection |
| **Binary size** | ⚠️ Larger (instantiated for each type combo) | ✅ Smaller (one reflection path) |
| **Build time** | ⚠️ Slower (more generics instantiation) | ✅ Faster (less generic code) |
| **Error messages** | ✅ Clear compile errors with exact types | ⚠️ Runtime panic messages |
| **API Style** | Builder methods (typed) | Builder methods (reflection) |

## Trade-offs

### Typed Generic Builder Advantages
1. **Zero reflection at runtime**: All types known at compile time, direct function calls
2. **Compile-time type safety**: Wrong handler signature = compile error before you even run
3. **Better IDE support**: Full autocomplete with exact types, instant error detection
4. **Predictable performance**: No reflection overhead, no reflection-based surprises
5. **Easier debugging**: Stack traces show explicit types, not `reflect.Value.Call`
6. **No package prefixes**: Methods eliminate repeated `catbird.`
7. **No name collisions**: Different receivers allow same method names

### Typed Generic Builder Disadvantages
1. **More builder types**: Need StepBuilder0, StepBuilder1, ..., StepBuilderN (but only N types, not N! functions)
2. **Implementation code**: More code to write (but mechanical/templatable)
3. **Larger binaries**: Generic instantiation for each type combination
4. **Longer builds**: More generic code to instantiate
5. **Upper limit on dependencies**: Practical limit on how many builder types to implement (8? 12? 16?)
6. **Learning curve**: Users need to understand builder type changes

### Reflection Builder Advantages (from BUILDER_API_DESIGN.md)
1. **Simpler implementation**: Single Step type, one reflection wrapper function
2. **Unlimited dependencies**: No practical limit on dependency count
3. **Smaller binaries**: One reflection code path vs many generic instantiations
4. **Faster builds**: Less generic code to compile

### Reflection Builder Disadvantages
1. **Runtime overhead**: ~0.5-1μs reflection cost per handler call
2. **Runtime errors**: Wrong signature detected at construction time but via panic, not compile error
3. **Harder debugging**: Stack traces include `reflect.Value.Call` frames
4. **Less IDE support**: Handler signature errors only shown at runtime

## When to Choose Typed Generic Builder

Choose this approach if:
- You want **zero runtime reflection** and maximum performance
- You want **compile-time type safety** for handler signatures
- You're okay with more builder types (StepBuilder0, StepBuilder1, etc.)
- You want the best error messages (compile-time, not runtime panics)
- You want the best IDE experience (full type information)
- You don't need more than 8-12 dependencies per step (very rare)

## When to Choose Reflection Builder

Choose this approach if:
- You want **simpler implementation** (single Step type)
- You want **unlimited dependencies** (though rare in practice)
- You want **smaller binaries and faster builds**
- Runtime reflection overhead is acceptable
- You prefer runtime validation over compile-time checks

## Migration Strategy

### Phase 1: Add New API (Parallel)
```go
// Old API (deprecated)
step := catbird.StepWithDependency("name", dep, handlerFn)

// New API (typed builder)
step := catbird.NewStep("name").
    DependsOn[DepType]("dep").
    WithHandler(handlerFn)
```

### Phase 2: Internal Migration
- Migrate tests to new builder API
- Update examples in README
- Benchmark performance

### Phase 3: Remove Old API
- Delete `InitialStep`, `StepWithDependency`, etc.
- Update migration guide
- Document breaking changes

## Benefits Summary

### ✅ Solves All Core Problems
1. **Handler optional**: Just don't call `.WithHandler()`
2. **Handler own options**: Scoped inside `.WithHandler(fn, opts...)`
3. **Divergent options**: Different builder types for Task, Step, Flow
4. **Function explosion**: Single `NewStep()` with `.DependsOn[T]()` replaces 8+ functions

### ✅ Type Safety
- Input/Output types inferred from handler function signature
- Dependency types carried in builder type parameters (D1, D2, ...)
- Handler signature validated at **compile time** (cannot compile if wrong)
- Optional dependencies enforced via `Optional[T]` pattern

### ✅ Extensibility
- Add new builder methods without breaking changes
- Support 8-12+ dependencies via additional builder types (StepBuilder0, 1, 2, ...)
- Builder type system ensures type safety automatically

## Open Questions

1. **Should dependency order matter?**
   - Current: Order in `.DependsOn[T]()` chain must match handler parameter order
   - Alternative: Named dependencies matched by name (requires different builder)

2. **How many builder types to implement?**
   ```go
   // Practical limit?  8 deps? 12 deps?
   StepBuilder0, StepBuilder1, StepBuilder2, ..., StepBuilder8
   // vs
   StepBuilder0, StepBuilder1, StepBuilder2, ..., StepBuilder12
   ```

3. **Validation timing?**
   - Current: At `.WithHandler()` call (compile-time check impossible, but types guide you)
   - No validation needed: compiler enforces signature via type parameters

## Next Steps

1. [ ] Decide between typed generic builder (zero reflection) vs reflection builder
2. [ ] If typed generic builder chosen:
   - Implement `StepBuilder0`, `StepBuilder1`, ..., `StepBuilderN` types
   - Implement each builder's `DependsOn[T]`, `RequiresSignal[S]`, `WithHandler` methods
   - Decide reasonable upper limit on dependency count (suggest 8 or 12)
3. [ ] If reflection builder chosen:
   - Implement Step type from BUILDER_API_DESIGN.md
   - Implement handler signature validation
   - Implement cached reflection wrappers
4. [ ] Write comprehensive tests for both scenarios
5. [ ] Benchmark both approaches
6. [ ] Make final decision based on team priorities
7. [ ] Update documentation

---

**End of Design Sketch**

---

## Summary

This document presents a **typed generic builder pattern** that achieves:

1. ✅ **Zero reflection** - All types known at compile time, pure Go function calls
2. ✅ **Compile-time type safety** - Wrong handler signature = compile error
3. ✅ **No API explosion** - Single `NewStep()`, single `DependsOn[T]()` methods
4. ✅ **Optional handler** - Just don't call `WithHandler`
5. ✅ **Scoped handler options** - Nested inside `WithHandler`
6. ✅ **No package prefixes** - Methods eliminate repeated `catbird.`
7. ✅ **No name collisions** - Different receiver types allow same names
8. ✅ **Ergonomic fluent API** - Chain methods naturally

The key innovation is that **the builder's type changes as you add dependencies**. This forces type safety at compile time:

```go
// This compiles (types match)
NewStep("x").DependsOn[ValidationResult]("v").WithHandler(
    func(ctx context.Context, in Request, v ValidationResult) (Response, error) { ... }
)

// This doesn't compile (types don't match)
NewStep("x").DependsOn[ValidationResult]("v").WithHandler(
    func(ctx context.Context, in Request, f FraudResult) (Response, error) { ... }
)  // ❌ Compile error!
```

Compared to the reflection builder in BUILDER_API_DESIGN.md, this trades off implementation complexity and binary size for zero runtime reflection overhead and compile-time type safety.
