package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

// Prototype tests validate the core patterns for the typed generics API design.
//
// These tests demonstrate:
// - Generic types with non-generic methods (core pattern)
// - Optional[T] for conditional dependencies
// - Reflection for build-time type validation
// - Flow builders with type parameters
// - Multiple dependency types
// - JSON round-trip with generics
// - Array type detection for MapStep
// - Interface composition with heterogeneous collections
//
// All tests pass, proving the design is implementable in Go 1.26.0.
// See TYPED_GENERICS_API_DESIGN.md for full design details.

// Type definitions for testing (package level)

type ProtoTypedTask[In, Out any] struct {
	name    string
	handler func(context.Context, In) (Out, error)
}

// Method does NOT introduce new type params (uses receiver's params)
func (t *ProtoTypedTask[In, Out]) Run(ctx context.Context, input In) (Out, error) {
	return t.handler(ctx, input)
}

func (t *ProtoTypedTask[In, Out]) Name() string {
	return t.name
}

type ProtoOptional[T any] struct {
	IsSet bool
	Value T
}

type ProtoStep interface {
	GetOutputType() reflect.Type
	GetName() string
}

type ProtoTypedStep[Out any] struct {
	name string
	out  Out
}

func (s *ProtoTypedStep[Out]) GetOutputType() reflect.Type {
	var zero Out
	return reflect.TypeOf(zero)
}

func (s *ProtoTypedStep[Out]) GetName() string {
	return s.name
}

type ProtoFlowBuilder[In, Out any] struct {
	name       string
	steps      []ProtoStep
	outputStep *string
}

func NewProtoFlowBuilder[In, Out any](name string) *ProtoFlowBuilder[In, Out] {
	return &ProtoFlowBuilder[In, Out]{name: name}
}

func (b *ProtoFlowBuilder[In, Out]) AddStep(step ProtoStep) *ProtoFlowBuilder[In, Out] {
	b.steps = append(b.steps, step)
	return b
}

func (b *ProtoFlowBuilder[In, Out]) SetOutputStep(name string) *ProtoFlowBuilder[In, Out] {
	b.outputStep = &name
	return b
}

// Tests

// Test 1: Generic types with non-generic methods (core pattern)
func TestGenericTypePattern(t *testing.T) {
	// Create instance with concrete types
	task := &ProtoTypedTask[string, int]{
		name: "convert",
		handler: func(ctx context.Context, s string) (int, error) {
			return len(s), nil
		},
	}

	result, err := task.Run(context.Background(), "hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
}

// Test 2: Optional[T] pattern
func TestOptionalPattern(t *testing.T) {
	// Set optional
	opt1 := ProtoOptional[int]{IsSet: true, Value: 42}
	if !opt1.IsSet || opt1.Value != 42 {
		t.Error("optional value mismatch")
	}

	// Unset optional
	opt2 := ProtoOptional[int]{IsSet: false}
	if opt2.IsSet {
		t.Error("optional should be unset")
	}

	// Zero value gives IsSet=false
	var opt3 ProtoOptional[int]
	if opt3.IsSet {
		t.Error("zero value optional should be unset")
	}
}

// Test 3: Reflection for type matching (build-time validation)
func TestReflectionTypeMatching(t *testing.T) {
	// Create steps with different types
	step1 := &ProtoTypedStep[int]{name: "step1"}
	step2 := &ProtoTypedStep[string]{name: "step2"}
	step3 := &ProtoTypedStep[int]{name: "step3"}

	type1 := step1.GetOutputType()
	type2 := step2.GetOutputType()
	type3 := step3.GetOutputType()

	// Verify type comparison works
	if type1 != type3 {
		t.Error("step1 and step3 should have same type")
	}
	if type1 == type2 {
		t.Error("step1 and step2 should have different types")
	}

	// Verify we can detect type mismatches
	if type1.String() != "int" {
		t.Errorf("expected 'int', got %s", type1.String())
	}
	if type2.String() != "string" {
		t.Errorf("expected 'string', got %s", type2.String())
	}
}

// Test 4: Flow builder with type parameters
func TestFlowBuilderPattern(t *testing.T) {
	step1 := &ProtoTypedStep[int]{name: "step1"}
	step2 := &ProtoTypedStep[string]{name: "step2"}

	// This pattern MUST compile and work
	flow := NewProtoFlowBuilder[string, string]("test-flow").
		AddStep(step1).
		AddStep(step2).
		SetOutputStep("step2")

	if flow.name != "test-flow" {
		t.Errorf("expected 'test-flow', got %s", flow.name)
	}
	if len(flow.steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(flow.steps))
	}
	if flow.outputStep == nil || *flow.outputStep != "step2" {
		t.Error("output step not set correctly")
	}
}

// Test 5: Handler with multiple dependency types
func TestMultipleDependencyTypes(t *testing.T) {
	// Handler with 2 dependencies of different types
	handler := func(ctx context.Context, input string, dep1 int, dep2 bool) (float64, error) {
		if dep2 {
			return float64(len(input) + dep1), nil
		}
		return float64(len(input)), nil
	}

	result, err := handler(context.Background(), "hello", 10, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 15.0 {
		t.Errorf("expected 15.0, got %f", result)
	}

	// Verify we can introspect the signature
	handlerType := reflect.TypeOf(handler)
	if handlerType.NumIn() != 4 {
		t.Errorf("expected 4 inputs, got %d", handlerType.NumIn())
	}
	if handlerType.In(2).Kind() != reflect.Int {
		t.Error("dep1 should be int")
	}
	if handlerType.In(3).Kind() != reflect.Bool {
		t.Error("dep2 should be bool")
	}
}

// Test 6: JSON round-trip with generics
func TestJSONWithGenerics(t *testing.T) {
	type Input struct {
		Name string
		Age  int
	}
	type Output struct {
		Message string
	}

	// Simulate task execution
	handler := func(ctx context.Context, in Input) (Output, error) {
		return Output{Message: fmt.Sprintf("%s is %d", in.Name, in.Age)}, nil
	}

	// Marshal input
	inputData := Input{Name: "Alice", Age: 30}
	inputJSON, err := json.Marshal(inputData)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	// Simulate DB storage (JSON bytes)
	// In real impl, this goes to PostgreSQL

	// Unmarshal input
	var receivedInput Input
	if err := json.Unmarshal(inputJSON, &receivedInput); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	// Execute handler
	output, err := handler(context.Background(), receivedInput)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}

	// Marshal output
	outputJSON, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal output error: %v", err)
	}

	// Unmarshal output
	var receivedOutput Output
	if err := json.Unmarshal(outputJSON, &receivedOutput); err != nil {
		t.Fatalf("unmarshal output error: %v", err)
	}

	if receivedOutput.Message != "Alice is 30" {
		t.Errorf("expected 'Alice is 30', got %s", receivedOutput.Message)
	}
}

// Test 7: Array type detection for MapStep
func TestArrayTypeDetection(t *testing.T) {
	// Function returning array
	arrayFunc := func(ctx context.Context, input string) ([]int, error) {
		return []int{1, 2, 3}, nil
	}

	// Function returning non-array
	nonArrayFunc := func(ctx context.Context, input string) (int, error) {
		return 42, nil
	}

	// Verify we can detect array types
	arrayType := reflect.TypeOf(arrayFunc).Out(0)
	nonArrayType := reflect.TypeOf(nonArrayFunc).Out(0)

	if arrayType.Kind() != reflect.Slice {
		t.Error("arrayFunc should return slice")
	}
	if nonArrayType.Kind() == reflect.Slice {
		t.Error("nonArrayFunc should not return slice")
	}

	// Extract element type
	if arrayType.Kind() == reflect.Slice {
		elemType := arrayType.Elem()
		if elemType.Kind() != reflect.Int {
			t.Errorf("expected int elements, got %v", elemType.Kind())
		}
	}
}

// Test 8: Conditional dependency with Optional
func TestConditionalDependencyPattern(t *testing.T) {
	// Convergence handler with optional dependencies
	convergence := func(
		ctx context.Context,
		input string,
		premium ProtoOptional[int],
		standard ProtoOptional[string],
	) (interface{}, error) {
		if premium.IsSet {
			return fmt.Sprintf("premium: %d", premium.Value), nil
		}
		if standard.IsSet {
			return fmt.Sprintf("standard: %s", standard.Value), nil
		}
		return "neither", nil
	}

	// Test premium path
	result1, _ := convergence(
		context.Background(),
		"input",
		ProtoOptional[int]{IsSet: true, Value: 100},
		ProtoOptional[string]{IsSet: false},
	)
	if result1 != "premium: 100" {
		t.Errorf("expected 'premium: 100', got %v", result1)
	}

	// Test standard path
	result2, _ := convergence(
		context.Background(),
		"input",
		ProtoOptional[int]{IsSet: false},
		ProtoOptional[string]{IsSet: true, Value: "basic"},
	)
	if result2 != "standard: basic" {
		t.Errorf("expected 'standard: basic', got %v", result2)
	}

	// Test neither path
	result3, _ := convergence(
		context.Background(),
		"input",
		ProtoOptional[int]{IsSet: false},
		ProtoOptional[string]{IsSet: false},
	)
	if result3 != "neither" {
		t.Errorf("expected 'neither', got %v", result3)
	}
}

// Test 9: Type parameter propagation through constructor
func TestTypeParameterPropagation(t *testing.T) {
	// Constructor propagates type params
	NewTask := func(
		name string,
		handler func(context.Context, string) (int, error),
	) *ProtoTypedTask[string, int] {
		return &ProtoTypedTask[string, int]{
			name:    name,
			handler: handler,
		}
	}

	// Create task
	task := NewTask("test", func(ctx context.Context, s string) (int, error) {
		return len(s), nil
	})

	// Verify types propagated correctly
	if task.Name() != "test" {
		t.Error("name not set")
	}

	// Call handler via Run method
	result, err := task.Run(context.Background(), "world")
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if result != 5 {
		t.Errorf("expected 5, got %d", result)
	}
}

// Test 10: Interface composition with generics
func TestInterfaceComposition(t *testing.T) {
	// Base interface (no generics)
	type Task interface {
		Name() string
	}

	// Verify interface implementation
	var _ Task = (*ProtoTypedTask[string, int])(nil)

	// Can store in slice with different type params
	tasks := []Task{
		&ProtoTypedTask[string, int]{name: "task1"},
		&ProtoTypedTask[int, bool]{name: "task2"},
		&ProtoTypedTask[bool, string]{name: "task3"},
	}

	names := []string{}
	for _, task := range tasks {
		names = append(names, task.Name())
	}

	if len(names) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(names))
	}
	if names[0] != "task1" || names[1] != "task2" || names[2] != "task3" {
		t.Errorf("task names incorrect: %v", names)
	}
}
