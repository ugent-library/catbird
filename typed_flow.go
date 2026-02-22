package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// isOptionalType checks if a type T is Optional[X] for some X
func isOptionalType[T any]() bool {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		return false
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	// Check if it's the Optional struct (has IsSet bool and Value field)
	if t.NumField() != 2 {
		return false
	}
	if t.Field(0).Name != "IsSet" || t.Field(0).Type.Kind() != reflect.Bool {
		return false
	}
	if t.Field(1).Name != "Value" {
		return false
	}
	return true
}

// StepInterface represents any flow step (typed or untyped)
type StepInterface interface {
	Name() string
	// dependencies returns the list of step names this step depends on
	dependencies() []string
	// optionalDependencies returns which dependencies are optional (may be skipped)
	optionalDependencies() []bool
	// hasSignal returns true if this step expects a signal input
	hasSignal() bool
	// condition returns the step condition expression (empty if none)
	condition() string
	// handlerOpts returns the handler execution options
	// Returns nil for definition-only steps
	handlerOpts() *handlerOpts
	// handle executes the step handler with flow input and dependency outputs
	handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) (outputJSON []byte, err error)
}

// TypedFlow is a flow with compile-time type safety
type TypedFlow[In, Out any] struct {
	name  string
	steps []StepInterface
}

// Name returns the flow name (implements FlowInterface)
func (f *TypedFlow[In, Out]) Name() string {
	return f.name
}

// Steps returns the flow's steps (implements FlowInterface)
func (f *TypedFlow[In, Out]) Steps() []StepInterface {
	return f.steps
}

// Run executes the flow with typed input, returns typed handle
func (f *TypedFlow[In, Out]) Run(ctx context.Context, client *Client, input In, opts *RunOpts) (*TypedHandle[Out], error) {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	if opts == nil {
		opts = &RunOpts{}
	}

	// Use the existing RunFlow SQL function
	q := `SELECT * FROM cb_run_flow(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4);`
	var id int64
	err = client.Conn.QueryRow(ctx, q, f.name, inputJSON, ptrOrNil(opts.ConcurrencyKey), ptrOrNil(opts.IdempotencyKey)).Scan(&id)
	if err != nil {
		return nil, err
	}

	return &TypedHandle[Out]{
		conn:  client.Conn,
		name:  f.name,
		id:    id,
		getFn: GetFlowRun,
	}, nil
}

// AddStep adds a step to the flow.
func (f *TypedFlow[In, Out]) AddStep(step StepInterface) *TypedFlow[In, Out] {
	f.steps = append(f.steps, step)
	return f
}

// NewFlow creates a new flow.
func NewFlow[In, Out any](name string) *TypedFlow[In, Out] {
	return &TypedFlow[In, Out]{
		name: name,
	}
}

// Step with 0 dependencies
type typedStep0[In, Out any] struct {
	name    string
	handler func(context.Context, In) (Out, error)
	opts    *handlerOpts
	config  *DefineStepOpts
}

func (s *typedStep0[In, Out]) Name() string {
	return s.name
}

func (s *typedStep0[In, Out]) dependencies() []string {
	return nil
}

func (s *typedStep0[In, Out]) optionalDependencies() []bool {
	return nil
}

func (s *typedStep0[In, Out]) hasSignal() bool {
	return false
}

func (s *typedStep0[In, Out]) condition() string {
	if s.config == nil {
		return ""
	}
	return s.config.Condition
}

func (s *typedStep0[In, Out]) handlerOpts() *handlerOpts {
	return s.opts
}

func (s *typedStep0[In, Out]) handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
	if s.handler == nil {
		return nil, fmt.Errorf("step %s has no handler (definition-only)", s.name)
	}

	// Unmarshal flow input
	var in In
	if err := json.Unmarshal(flowInputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal flow input: %w", err)
	}

	// Execute handler
	out, err := s.handler(ctx, in)
	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

// Step with 1 dependency
type typedStep1[In, D1, Out any] struct {
	name         string
	dep1         string
	dep1Optional bool
	handler      func(context.Context, In, D1) (Out, error)
	opts         *handlerOpts
	config       *DefineStepOpts
}

func (s *typedStep1[In, D1, Out]) Name() string {
	return s.name
}

func (s *typedStep1[In, D1, Out]) dependencies() []string {
	return []string{s.dep1}
}

func (s *typedStep1[In, D1, Out]) optionalDependencies() []bool {
	return []bool{s.dep1Optional}
}

func (s *typedStep1[In, D1, Out]) hasSignal() bool {
	return false
}

func (s *typedStep1[In, D1, Out]) condition() string {
	if s.config == nil {
		return ""
	}
	return s.config.Condition
}

func (s *typedStep1[In, D1, Out]) handlerOpts() *handlerOpts {
	return s.opts
}

func (s *typedStep1[In, D1, Out]) handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
	if s.handler == nil {
		return nil, fmt.Errorf("step %s has no handler (definition-only)", s.name)
	}

	// Unmarshal flow input
	var in In
	if err := json.Unmarshal(flowInputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal flow input: %w", err)
	}

	// Unmarshal dependency output
	var d1 D1
	dep1JSON, ok := depsJSON[s.dep1]
	if !ok {
		if !s.dep1Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep1)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d1 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep1Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep1JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep1, err)
			}
		} else {
			if err := json.Unmarshal(dep1JSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep1, err)
			}
		}
	}

	// Execute handler
	out, err := s.handler(ctx, in, d1)
	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

// Step with 2 dependencies
type typedStep2[In, D1, D2, Out any] struct {
	name         string
	dep1         string
	dep2         string
	dep1Optional bool
	dep2Optional bool
	handler      func(context.Context, In, D1, D2) (Out, error)
	opts         *handlerOpts
	config       *DefineStepOpts
}

func (s *typedStep2[In, D1, D2, Out]) Name() string {
	return s.name
}

func (s *typedStep2[In, D1, D2, Out]) dependencies() []string {
	return []string{s.dep1, s.dep2}
}

func (s *typedStep2[In, D1, D2, Out]) optionalDependencies() []bool {
	return []bool{s.dep1Optional, s.dep2Optional}
}

func (s *typedStep2[In, D1, D2, Out]) hasSignal() bool {
	return false
}

func (s *typedStep2[In, D1, D2, Out]) condition() string {
	if s.config == nil {
		return ""
	}
	return s.config.Condition
}

func (s *typedStep2[In, D1, D2, Out]) handlerOpts() *handlerOpts {
	return s.opts
}

func (s *typedStep2[In, D1, D2, Out]) handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
	if s.handler == nil {
		return nil, fmt.Errorf("step %s has no handler (definition-only)", s.name)
	}

	// Unmarshal flow input
	var in In
	if err := json.Unmarshal(flowInputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal flow input: %w", err)
	}

	// Unmarshal dependency outputs
	var d1 D1
	dep1JSON, ok := depsJSON[s.dep1]
	if !ok {
		if !s.dep1Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep1)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d1 is already zero-initialized
	} else {
		if err := json.Unmarshal(dep1JSON, &d1); err != nil {
			return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep1, err)
		}
	}

	var d2 D2
	dep2JSON, ok := depsJSON[s.dep2]
	if !ok {
		if !s.dep2Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep2)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d2 is already zero-initialized
	} else {
		if err := json.Unmarshal(dep2JSON, &d2); err != nil {
			return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep2, err)
		}
	}

	// Execute handler
	out, err := s.handler(ctx, in, d1, d2)
	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

// Step with 3 dependencies
type typedStep3[In, D1, D2, D3, Out any] struct {
	name         string
	dep1         string
	dep2         string
	dep3         string
	dep1Optional bool
	dep2Optional bool
	dep3Optional bool
	handler      func(context.Context, In, D1, D2, D3) (Out, error)
	opts         *handlerOpts
	config       *DefineStepOpts
}

func (s *typedStep3[In, D1, D2, D3, Out]) Name() string {
	return s.name
}

func (s *typedStep3[In, D1, D2, D3, Out]) dependencies() []string {
	return []string{s.dep1, s.dep2, s.dep3}
}

func (s *typedStep3[In, D1, D2, D3, Out]) optionalDependencies() []bool {
	return []bool{s.dep1Optional, s.dep2Optional, s.dep3Optional}
}

func (s *typedStep3[In, D1, D2, D3, Out]) hasSignal() bool {
	return false
}

func (s *typedStep3[In, D1, D2, D3, Out]) condition() string {
	if s.config == nil {
		return ""
	}
	return s.config.Condition
}

func (s *typedStep3[In, D1, D2, D3, Out]) handlerOpts() *handlerOpts {
	return s.opts
}

func (s *typedStep3[In, D1, D2, D3, Out]) handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
	if s.handler == nil {
		return nil, fmt.Errorf("step %s has no handler (definition-only)", s.name)
	}

	// Unmarshal flow input
	var in In
	if err := json.Unmarshal(flowInputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal flow input: %w", err)
	}

	// Unmarshal dependency outputs
	var d1 D1
	dep1JSON, ok := depsJSON[s.dep1]
	if !ok {
		if !s.dep1Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep1)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d1 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep1Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep1JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep1, err)
			}
		} else {
			if err := json.Unmarshal(dep1JSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep1, err)
			}
		}
	}

	var d2 D2
	dep2JSON, ok := depsJSON[s.dep2]
	if !ok {
		if !s.dep2Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep2)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d2 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep2Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep2JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d2); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep2, err)
			}
		} else {
			if err := json.Unmarshal(dep2JSON, &d2); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep2, err)
			}
		}
	}

	var d3 D3
	dep3JSON, ok := depsJSON[s.dep3]
	if !ok {
		if !s.dep3Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep3)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d3 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep3Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep3JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d3); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep3, err)
			}
		} else {
			if err := json.Unmarshal(dep3JSON, &d3); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep3, err)
			}
		}
	}

	// Execute handler
	out, err := s.handler(ctx, in, d1, d2, d3)
	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

// Signal-enabled step types

// Step with signal, no dependencies
type typedStepSignal0[In, Sig, Out any] struct {
	name    string
	handler func(context.Context, In, Sig) (Out, error)
	opts    *handlerOpts
	config  *DefineStepOpts
}

func (s *typedStepSignal0[In, Sig, Out]) Name() string {
	return s.name
}

func (s *typedStepSignal0[In, Sig, Out]) dependencies() []string {
	return nil
}

func (s *typedStepSignal0[In, Sig, Out]) optionalDependencies() []bool {
	return nil
}

func (s *typedStepSignal0[In, Sig, Out]) hasSignal() bool {
	return true
}

func (s *typedStepSignal0[In, Sig, Out]) condition() string {
	if s.config == nil {
		return ""
	}
	return s.config.Condition
}

func (s *typedStepSignal0[In, Sig, Out]) handlerOpts() *handlerOpts {
	return s.opts
}

func (s *typedStepSignal0[In, Sig, Out]) handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
	if s.handler == nil {
		return nil, fmt.Errorf("step %s has no handler (definition-only)", s.name)
	}

	// Unmarshal flow input
	var in In
	if err := json.Unmarshal(flowInputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal flow input: %w", err)
	}

	// Unmarshal signal input
	var sig Sig
	if err := json.Unmarshal(signalInputJSON, &sig); err != nil {
		return nil, fmt.Errorf("unmarshal signal input: %w", err)
	}

	// Execute handler
	out, err := s.handler(ctx, in, sig)
	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

// Step with signal and 1 dependency
type typedStepSignal1[In, Sig, D1, Out any] struct {
	name         string
	dep1         string
	dep1Optional bool
	handler      func(context.Context, In, Sig, D1) (Out, error)
	opts         *handlerOpts
	config       *DefineStepOpts
}

func (s *typedStepSignal1[In, Sig, D1, Out]) Name() string {
	return s.name
}

func (s *typedStepSignal1[In, Sig, D1, Out]) dependencies() []string {
	return []string{s.dep1}
}

func (s *typedStepSignal1[In, Sig, D1, Out]) optionalDependencies() []bool {
	return []bool{s.dep1Optional}
}

func (s *typedStepSignal1[In, Sig, D1, Out]) hasSignal() bool {
	return true
}

func (s *typedStepSignal1[In, Sig, D1, Out]) condition() string {
	if s.config == nil {
		return ""
	}
	return s.config.Condition
}

func (s *typedStepSignal1[In, Sig, D1, Out]) handlerOpts() *handlerOpts {
	return s.opts
}

func (s *typedStepSignal1[In, Sig, D1, Out]) handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
	if s.handler == nil {
		return nil, fmt.Errorf("step %s has no handler (definition-only)", s.name)
	}

	// Unmarshal flow input
	var in In
	if err := json.Unmarshal(flowInputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal flow input: %w", err)
	}

	// Unmarshal signal input
	var sig Sig
	if err := json.Unmarshal(signalInputJSON, &sig); err != nil {
		return nil, fmt.Errorf("unmarshal signal input: %w", err)
	}

	// Unmarshal dependency output
	var d1 D1
	dep1JSON, ok := depsJSON[s.dep1]
	if !ok {
		if !s.dep1Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep1)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d1 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep1Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep1JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep1, err)
			}
		} else {
			if err := json.Unmarshal(dep1JSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep1, err)
			}
		}
	}

	// Execute handler
	out, err := s.handler(ctx, in, sig, d1)
	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

// Step with signal and 2 dependencies
type typedStepSignal2[In, Sig, D1, D2, Out any] struct {
	name         string
	dep1         string
	dep2         string
	dep1Optional bool
	dep2Optional bool
	handler      func(context.Context, In, Sig, D1, D2) (Out, error)
	opts         *handlerOpts
	config       *DefineStepOpts
}

func (s *typedStepSignal2[In, Sig, D1, D2, Out]) Name() string {
	return s.name
}

func (s *typedStepSignal2[In, Sig, D1, D2, Out]) dependencies() []string {
	return []string{s.dep1, s.dep2}
}

func (s *typedStepSignal2[In, Sig, D1, D2, Out]) optionalDependencies() []bool {
	return []bool{s.dep1Optional, s.dep2Optional}
}

func (s *typedStepSignal2[In, Sig, D1, D2, Out]) hasSignal() bool {
	return true
}

func (s *typedStepSignal2[In, Sig, D1, D2, Out]) condition() string {
	if s.config == nil {
		return ""
	}
	return s.config.Condition
}

func (s *typedStepSignal2[In, Sig, D1, D2, Out]) handlerOpts() *handlerOpts {
	return s.opts
}

func (s *typedStepSignal2[In, Sig, D1, D2, Out]) handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
	if s.handler == nil {
		return nil, fmt.Errorf("step %s has no handler (definition-only)", s.name)
	}

	// Unmarshal flow input
	var in In
	if err := json.Unmarshal(flowInputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal flow input: %w", err)
	}

	// Unmarshal signal input
	var sig Sig
	if err := json.Unmarshal(signalInputJSON, &sig); err != nil {
		return nil, fmt.Errorf("unmarshal signal input: %w", err)
	}

	// Unmarshal dependency outputs
	var d1 D1
	dep1JSON, ok := depsJSON[s.dep1]
	if !ok {
		if !s.dep1Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep1)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d1 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep1Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep1JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep1, err)
			}
		} else {
			if err := json.Unmarshal(dep1JSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep1, err)
			}
		}
	}

	var d2 D2
	dep2JSON, ok := depsJSON[s.dep2]
	if !ok {
		if !s.dep2Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep2)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d2 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep2Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep2JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d2); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep2, err)
			}
		} else {
			if err := json.Unmarshal(dep2JSON, &d2); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep2, err)
			}
		}
	}

	// Execute handler
	out, err := s.handler(ctx, in, sig, d1, d2)
	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

// Step with signal and 3 dependencies
type typedStepSignal3[In, Sig, D1, D2, D3, Out any] struct {
	name         string
	dep1         string
	dep2         string
	dep3         string
	dep1Optional bool
	dep2Optional bool
	dep3Optional bool
	handler      func(context.Context, In, Sig, D1, D2, D3) (Out, error)
	opts         *handlerOpts
	config       *DefineStepOpts
}

func (s *typedStepSignal3[In, Sig, D1, D2, D3, Out]) Name() string {
	return s.name
}

func (s *typedStepSignal3[In, Sig, D1, D2, D3, Out]) dependencies() []string {
	return []string{s.dep1, s.dep2, s.dep3}
}

func (s *typedStepSignal3[In, Sig, D1, D2, D3, Out]) optionalDependencies() []bool {
	return []bool{s.dep1Optional, s.dep2Optional, s.dep3Optional}
}

func (s *typedStepSignal3[In, Sig, D1, D2, D3, Out]) hasSignal() bool {
	return true
}

func (s *typedStepSignal3[In, Sig, D1, D2, D3, Out]) condition() string {
	if s.config == nil {
		return ""
	}
	return s.config.Condition
}

func (s *typedStepSignal3[In, Sig, D1, D2, D3, Out]) handlerOpts() *handlerOpts {
	return s.opts
}

func (s *typedStepSignal3[In, Sig, D1, D2, D3, Out]) handle(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
	if s.handler == nil {
		return nil, fmt.Errorf("step %s has no handler (definition-only)", s.name)
	}

	// Unmarshal flow input
	var in In
	if err := json.Unmarshal(flowInputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal flow input: %w", err)
	}

	// Unmarshal signal input
	var sig Sig
	if err := json.Unmarshal(signalInputJSON, &sig); err != nil {
		return nil, fmt.Errorf("unmarshal signal input: %w", err)
	}

	// Unmarshal dependency outputs
	var d1 D1
	dep1JSON, ok := depsJSON[s.dep1]
	if !ok {
		if !s.dep1Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep1)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d1 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep1Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep1JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep1, err)
			}
		} else {
			if err := json.Unmarshal(dep1JSON, &d1); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep1, err)
			}
		}
	}

	var d2 D2
	dep2JSON, ok := depsJSON[s.dep2]
	if !ok {
		if !s.dep2Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep2)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d2 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep2Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep2JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d2); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep2, err)
			}
		} else {
			if err := json.Unmarshal(dep2JSON, &d2); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep2, err)
			}
		}
	}

	var d3 D3
	dep3JSON, ok := depsJSON[s.dep3]
	if !ok {
		if !s.dep3Optional {
			return nil, fmt.Errorf("missing dependency output: %s", s.dep3)
		}
		// Optional dependency is missing, use zero value (Optional[T]{IsSet: false})
		// d3 is already zero-initialized
	} else {
		// If dependency is optional, wrap the output in Optional structure
		if s.dep3Optional {
			// Construct JSON: {"IsSet": true, "Value": <original JSON>}
			wrappedJSON := []byte(`{"IsSet":true,"Value":` + string(dep3JSON) + `}`)
			if err := json.Unmarshal(wrappedJSON, &d3); err != nil {
				return nil, fmt.Errorf("unmarshal optional dependency %s output: %w", s.dep3, err)
			}
		} else {
			if err := json.Unmarshal(dep3JSON, &d3); err != nil {
				return nil, fmt.Errorf("unmarshal dependency %s output: %w", s.dep3, err)
			}
		}
	}

	// Execute handler
	out, err := s.handler(ctx, in, sig, d1, d2, d3)
	if err != nil {
		return nil, err
	}

	return json.Marshal(out)
}

// Step option structs (parallel to TaskOpts / TaskDefinitionOpts)

// StepOpts bundles all step and handler options together (for handler-based steps).
// Used with NewStep, NewStepDep, NewStep2Deps, etc.
type StepOpts struct {
	Condition      string
	Concurrency    int
	BatchSize      int
	Timeout        time.Duration
	MaxRetries     int
	MinDelay       time.Duration
	MaxDelay       time.Duration
	CircuitBreaker *CircuitBreaker
}

// DefineStepOpts contains step-only options (for definition-only steps).
// Used with DefineStep, DefineStepDep, DefineStep2Deps, etc.
type DefineStepOpts struct {
	Condition string
}

// Step constructors (handler-based)

// NewStep creates a flow step with no dependencies and a handler
func NewStep[In, Out any](
	name string,
	handler func(context.Context, In) (Out, error),
	opts *StepOpts,
) StepInterface {
	if opts == nil {
		opts = &StepOpts{Concurrency: 1, BatchSize: 10}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}

	handlerOpts := handlerOpts{
		concurrency:    opts.Concurrency,
		batchSize:      opts.BatchSize,
		timeout:        opts.Timeout,
		maxRetries:     opts.MaxRetries,
		minDelay:       opts.MinDelay,
		maxDelay:       opts.MaxDelay,
		circuitBreaker: opts.CircuitBreaker,
	}

	if err := handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return &typedStep0[In, Out]{
		name:    name,
		handler: handler,
		opts:    &handlerOpts,
		config:  &DefineStepOpts{Condition: opts.Condition},
	}
}

// NewStep1Dep creates a flow step with 1 dependency and a handler
func NewStep1Dep[In, D1, Out any](
	name string,
	dep1Name string,
	handler func(context.Context, In, D1) (Out, error),
	opts *StepOpts,
) StepInterface {
	if opts == nil {
		opts = &StepOpts{Concurrency: 1, BatchSize: 10}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}

	handlerOpts := handlerOpts{
		concurrency:    opts.Concurrency,
		batchSize:      opts.BatchSize,
		timeout:        opts.Timeout,
		maxRetries:     opts.MaxRetries,
		minDelay:       opts.MinDelay,
		maxDelay:       opts.MaxDelay,
		circuitBreaker: opts.CircuitBreaker,
	}

	if err := handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return &typedStep1[In, D1, Out]{
		name:         name,
		dep1:         dep1Name,
		dep1Optional: isOptionalType[D1](),
		handler:      handler,
		opts:         &handlerOpts,
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// NewStep2Deps creates a flow step with 2 dependencies and a handler
func NewStep2Deps[In, D1, D2, Out any](
	name string,
	dep1Name string,
	dep2Name string,
	handler func(context.Context, In, D1, D2) (Out, error),
	opts *StepOpts,
) StepInterface {
	if opts == nil {
		opts = &StepOpts{Concurrency: 1, BatchSize: 10}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}

	handlerOpts := handlerOpts{
		concurrency:    opts.Concurrency,
		batchSize:      opts.BatchSize,
		timeout:        opts.Timeout,
		maxRetries:     opts.MaxRetries,
		minDelay:       opts.MinDelay,
		maxDelay:       opts.MaxDelay,
		circuitBreaker: opts.CircuitBreaker,
	}

	if err := handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return &typedStep2[In, D1, D2, Out]{
		name:         name,
		dep1:         dep1Name,
		dep2:         dep2Name,
		dep1Optional: isOptionalType[D1](),
		dep2Optional: isOptionalType[D2](),
		handler:      handler,
		opts:         &handlerOpts,
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// NewStep3Deps creates a flow step with 3 dependencies and a handler
func NewStep3Deps[In, D1, D2, D3, Out any](
	name string,
	dep1Name string,
	dep2Name string,
	dep3Name string,
	handler func(context.Context, In, D1, D2, D3) (Out, error),
	opts *StepOpts,
) StepInterface {
	if opts == nil {
		opts = &StepOpts{Concurrency: 1, BatchSize: 10}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}

	handlerOpts := handlerOpts{
		concurrency:    opts.Concurrency,
		batchSize:      opts.BatchSize,
		timeout:        opts.Timeout,
		maxRetries:     opts.MaxRetries,
		minDelay:       opts.MinDelay,
		maxDelay:       opts.MaxDelay,
		circuitBreaker: opts.CircuitBreaker,
	}

	if err := handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return &typedStep3[In, D1, D2, D3, Out]{
		name:         name,
		dep1:         dep1Name,
		dep2:         dep2Name,
		dep3:         dep3Name,
		dep1Optional: isOptionalType[D1](),
		dep2Optional: isOptionalType[D2](),
		dep3Optional: isOptionalType[D3](),
		handler:      handler,
		opts:         &handlerOpts,
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// Signal-enabled step constructors (handler-based)

// NewStepSignal creates a flow step with a signal and no dependencies and a handler
func NewStepSignal[In, Sig, Out any](
	name string,
	handler func(context.Context, In, Sig) (Out, error),
	opts *StepOpts,
) StepInterface {
	if opts == nil {
		opts = &StepOpts{Concurrency: 1, BatchSize: 10}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}

	handlerOpts := handlerOpts{
		concurrency:    opts.Concurrency,
		batchSize:      opts.BatchSize,
		timeout:        opts.Timeout,
		maxRetries:     opts.MaxRetries,
		minDelay:       opts.MinDelay,
		maxDelay:       opts.MaxDelay,
		circuitBreaker: opts.CircuitBreaker,
	}

	if err := handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return &typedStepSignal0[In, Sig, Out]{
		name:    name,
		handler: handler,
		opts:    &handlerOpts,
		config:  &DefineStepOpts{Condition: opts.Condition},
	}
}

// NewStepSignal1Dep creates a flow step with a signal and 1 dependency and a handler
func NewStepSignal1Dep[In, Sig, D1, Out any](
	name string,
	dep1Name string,
	handler func(context.Context, In, Sig, D1) (Out, error),
	opts *StepOpts,
) StepInterface {
	if opts == nil {
		opts = &StepOpts{Concurrency: 1, BatchSize: 10}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}

	handlerOpts := handlerOpts{
		concurrency:    opts.Concurrency,
		batchSize:      opts.BatchSize,
		timeout:        opts.Timeout,
		maxRetries:     opts.MaxRetries,
		minDelay:       opts.MinDelay,
		maxDelay:       opts.MaxDelay,
		circuitBreaker: opts.CircuitBreaker,
	}

	if err := handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return &typedStepSignal1[In, Sig, D1, Out]{
		name:         name,
		dep1:         dep1Name,
		dep1Optional: isOptionalType[D1](),
		handler:      handler,
		opts:         &handlerOpts,
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// NewStepSignal2Deps creates a flow step with a signal and 2 dependencies and a handler
func NewStepSignal2Deps[In, Sig, D1, D2, Out any](
	name string,
	dep1Name string,
	dep2Name string,
	handler func(context.Context, In, Sig, D1, D2) (Out, error),
	opts *StepOpts,
) StepInterface {
	if opts == nil {
		opts = &StepOpts{Concurrency: 1, BatchSize: 10}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}

	handlerOpts := handlerOpts{
		concurrency:    opts.Concurrency,
		batchSize:      opts.BatchSize,
		timeout:        opts.Timeout,
		maxRetries:     opts.MaxRetries,
		minDelay:       opts.MinDelay,
		maxDelay:       opts.MaxDelay,
		circuitBreaker: opts.CircuitBreaker,
	}

	if err := handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return &typedStepSignal2[In, Sig, D1, D2, Out]{
		name:         name,
		dep1:         dep1Name,
		dep2:         dep2Name,
		dep1Optional: isOptionalType[D1](),
		dep2Optional: isOptionalType[D2](),
		handler:      handler,
		opts:         &handlerOpts,
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// NewStepSignal3Deps creates a flow step with a signal and 3 dependencies and a handler
func NewStepSignal3Deps[In, Sig, D1, D2, D3, Out any](
	name string,
	dep1Name string,
	dep2Name string,
	dep3Name string,
	handler func(context.Context, In, Sig, D1, D2, D3) (Out, error),
	opts *StepOpts,
) StepInterface {
	if opts == nil {
		opts = &StepOpts{Concurrency: 1, BatchSize: 10}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}

	handlerOpts := handlerOpts{
		concurrency:    opts.Concurrency,
		batchSize:      opts.BatchSize,
		timeout:        opts.Timeout,
		maxRetries:     opts.MaxRetries,
		minDelay:       opts.MinDelay,
		maxDelay:       opts.MaxDelay,
		circuitBreaker: opts.CircuitBreaker,
	}

	if err := handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return &typedStepSignal3[In, Sig, D1, D2, D3, Out]{
		name:         name,
		dep1:         dep1Name,
		dep2:         dep2Name,
		dep3:         dep3Name,
		dep1Optional: isOptionalType[D1](),
		dep2Optional: isOptionalType[D2](),
		dep3Optional: isOptionalType[D3](),
		handler:      handler,
		opts:         &handlerOpts,
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// Step definition-only constructors (no handler, for definition building)

// DefineStep creates a definition-only step with no dependencies (no handler)
func DefineStep[In, Out any](
	name string,
	opts *DefineStepOpts,
) StepInterface {
	if opts == nil {
		opts = &DefineStepOpts{}
	}

	return &typedStep0[In, Out]{
		name:   name,
		config: &DefineStepOpts{Condition: opts.Condition},
	}
}

// DefineStep1Dep creates a definition-only step with 1 dependency (no handler)
func DefineStep1Dep[In, D1, Out any](
	name string,
	dep1Name string,
	opts *DefineStepOpts,
) StepInterface {
	if opts == nil {
		opts = &DefineStepOpts{}
	}

	return &typedStep1[In, D1, Out]{
		name:         name,
		dep1:         dep1Name,
		dep1Optional: isOptionalType[D1](),
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// DefineStep2Deps creates a definition-only step with 2 dependencies (no handler)
func DefineStep2Deps[In, D1, D2, Out any](
	name string,
	dep1Name string,
	dep2Name string,
	opts *DefineStepOpts,
) StepInterface {
	if opts == nil {
		opts = &DefineStepOpts{}
	}

	return &typedStep2[In, D1, D2, Out]{
		name:         name,
		dep1:         dep1Name,
		dep2:         dep2Name,
		dep1Optional: isOptionalType[D1](),
		dep2Optional: isOptionalType[D2](),
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// DefineStep3Deps creates a definition-only step with 3 dependencies (no handler)
func DefineStep3Deps[In, D1, D2, D3, Out any](
	name string,
	dep1Name string,
	dep2Name string,
	dep3Name string,
	opts *DefineStepOpts,
) StepInterface {
	if opts == nil {
		opts = &DefineStepOpts{}
	}

	return &typedStep3[In, D1, D2, D3, Out]{
		name:         name,
		dep1:         dep1Name,
		dep2:         dep2Name,
		dep3:         dep3Name,
		dep1Optional: isOptionalType[D1](),
		dep2Optional: isOptionalType[D2](),
		dep3Optional: isOptionalType[D3](),
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// Signal-enabled definition-only constructors

// DefineStepSignal creates a definition-only step with a signal and no dependencies (no handler)
func DefineStepSignal[In, Sig, Out any](
	name string,
	opts *DefineStepOpts,
) StepInterface {
	if opts == nil {
		opts = &DefineStepOpts{}
	}

	return &typedStepSignal0[In, Sig, Out]{
		name:   name,
		config: &DefineStepOpts{Condition: opts.Condition},
	}
}

// DefineStepSignal1Dep creates a definition-only step with a signal and 1 dependency (no handler)
func DefineStepSignal1Dep[In, Sig, D1, Out any](
	name string,
	dep1Name string,
	opts *DefineStepOpts,
) StepInterface {
	if opts == nil {
		opts = &DefineStepOpts{}
	}

	return &typedStepSignal1[In, Sig, D1, Out]{
		name:         name,
		dep1:         dep1Name,
		dep1Optional: isOptionalType[D1](),
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// DefineStepSignal2Deps creates a definition-only step with a signal and 2 dependencies (no handler)
func DefineStepSignal2Deps[In, Sig, D1, D2, Out any](
	name string,
	dep1Name string,
	dep2Name string,
	opts *DefineStepOpts,
) StepInterface {
	if opts == nil {
		opts = &DefineStepOpts{}
	}

	return &typedStepSignal2[In, Sig, D1, D2, Out]{
		name:         name,
		dep1:         dep1Name,
		dep2:         dep2Name,
		dep1Optional: isOptionalType[D1](),
		dep2Optional: isOptionalType[D2](),
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}

// DefineStepSignal3Deps creates a definition-only step with a signal and 3 dependencies (no handler)
func DefineStepSignal3Deps[In, Sig, D1, D2, D3, Out any](
	name string,
	dep1Name string,
	dep2Name string,
	dep3Name string,
	opts *DefineStepOpts,
) StepInterface {
	if opts == nil {
		opts = &DefineStepOpts{}
	}

	return &typedStepSignal3[In, Sig, D1, D2, D3, Out]{
		name:         name,
		dep1:         dep1Name,
		dep2:         dep2Name,
		dep3:         dep3Name,
		dep1Optional: isOptionalType[D1](),
		dep2Optional: isOptionalType[D2](),
		dep3Optional: isOptionalType[D3](),
		config:       &DefineStepOpts{Condition: opts.Condition},
	}
}
