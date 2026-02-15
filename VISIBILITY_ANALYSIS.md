# Type Visibility Analysis - Typed Generics API

## Question
Should `TypedTask`, `TypedStep0/1/2/3`, `TypedFlow`, and `Handle` be public or private? Should constructors return concrete types or interfaces?

## Current Design

| Type | Visibility | Constructor Returns |
|------|------------|-------------------|
| `TypedTask[In, Out]` | Public | `*TypedTask[In, Out]` |
| `TypedStep0/1/2/3[...]` | Public | `Step` interface ✅ |
| `TypedFlow[In, Out]` | Public | `*TypedFlow[In, Out]` |
| `Handle[Out]` | Public | `Handle[Out]` |

## Analysis

### TypedTask[In, Out]

**Current Usage**:
```go
task := catbird.NewTask[Input, Output]("process", handler, opts)
// task is *TypedTask[Input, Output]

handle, err := task.Run(ctx, client, input)
// Run() method needs type parameters for input/output
```

**Can we make it private?**

**Option A: Return interface** ❌
```go
func NewTask[In, Out any](...) Task  // Returns interface
// Problem: Task interface doesn't have Run() method
// Task interface has no type parameters, can't have typed Run(In) -> Handle[Out]
```

**Option B: Package-level Run function** ❌
```go
func RunTask[In, Out any](ctx, client, task Task, input In) (Handle[Out], error)
// Problem: How does compiler know task matches In/Out type parameters?
// Task interface doesn't carry type info!
```

**Option C: Keep public** ✅
```go
func NewTask[In, Out any](...) *TypedTask[In, Out]
// Users can call task.Run(ctx, client, input) with full type safety
// Compiler verifies input type matches In, output matches Out
```

**Verdict**: **Must be PUBLIC**
- Users need to call typed `Run()` method
- Type parameters are essential for type safety
- Interface doesn't carry type parameters

### TypedStep0/1/2/3

**Current Usage**:
```go
flow := catbird.NewFlow[Input, Output]("flow").
    AddStep(catbird.InitialStep("step1", handler, opts)).  // Returns Step interface
    AddStep(catbird.StepWithDependency("step2", "step1", handler, opts))  // Returns Step interface
```

**Can we make it private?**

**Already returns interface!** ✅
```go
func InitialStep[In, Out any](...) Step {
    return &typedStep0[In, Out]{...}  // lowercase = private
}
```

**Users never reference concrete types**:
- They work only with `Step` interface
- Flow builder accepts `Step` interface
- Worker uses `Step` interface with unexported methods

**Verdict**: **SHOULD BE PRIVATE** (lowercase: `typedStep0/1/2/3`)
- Better encapsulation
- Users don't need to see implementation details
- Already returns interface in design

### TypedFlow[In, Out]

**Current Usage**:
```go
flow := catbird.NewFlow[Input, Output]("flow").
    AddStep(...).
    Build()
// flow is *TypedFlow[Input, Output]

handle, err := flow.Run(ctx, client, input)
// Run() method needs type parameters for input/output
```

**Same reasoning as TypedTask**:
- Users call typed `Run()` method
- Need type parameters for type safety
- Interface doesn't carry type parameters

**Verdict**: **Must be PUBLIC**
- Same reasoning as TypedTask
- Typed `Run()` method is essential

### Handle[Out]

**Current Usage**:
```go
handle, err := task.Run(ctx, client, input)
// handle is Handle[Output]

output, err := handle.WaitForOutput(ctx)
// output is Output type
```

**Can we make it private?**

**Users reference it in type signatures**:
```go
func processOrder(ctx context.Context) (catbird.Handle[Receipt], error) {
    return task.Run(ctx, client, order)
}
```

**No benefit to hiding**:
- Simple value type with methods
- No encapsulation benefit
- Would complicate API

**Verdict**: **Keep PUBLIC**
- Users need to reference it
- Simple value type
- No reason to hide

## Recommendations

### Apply These Changes:

1. **Make TypedStep0/1/2/3 private** (lowercase):
   ```go
   // Before (public)
   type TypedStep0[In, Out any] struct { ... }
   type TypedStep1[In, D1, Out any] struct { ... }
   
   // After (private)
   type typedStep0[In, Out any] struct { ... }
   type typedStep1[In, D1, Out any] struct { ... }
   ```

2. **Keep TypedTask public**:
   ```go
   type TypedTask[In, Out any] struct { ... }  // Public - users call Run()
   ```

3. **Keep TypedFlow public**:
   ```go
   type TypedFlow[In, Out any] struct { ... }  // Public - users call Run()
   ```

4. **Keep Handle public**:
   ```go
   type Handle[Out any] struct { ... }  // Public - users reference it
   ```

## Summary

| Type | Visibility | Rationale |
|------|------------|-----------|
| `TypedTask[In, Out]` | ✅ **Public** | Users call typed `Run()` method; type params needed |
| `typedStep0/1/2/3[...]` | ✅ **Private** | Already returns `Step` interface; users never reference concrete type |
| `TypedFlow[In, Out]` | ✅ **Public** | Users call typed `Run()` method; type params needed |
| `Handle[Out]` | ✅ **Public** | Users reference in type signatures; simple value type |

**Key Principle**: 
- **Public**: Types users directly construct and call type-parameterized methods on
- **Private**: Implementation details hidden behind interfaces
