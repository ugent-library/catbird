package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type Flow struct {
	Name  string
	Steps []*Step `json:"steps"`
}

type Step struct {
	Name      string            `json:"name"`
	Condition string            `json:"condition,omitempty"`
	HasSignal bool              `json:"has_signal"`
	DependsOn []*StepDependency `json:"depends_on,omitempty"`
	handler   *stepHandler
}

// StepDependency represents a dependency on another step's output.
type StepDependency struct {
	Name     string `json:"name"`
	Optional bool   `json:"optional,omitempty"`
}

// Dependency creates a step dependency reference by name.
// Used when defining flow steps that depend on other steps.
//
// Example: Dependency("analyze")
func Dependency(name string) *StepDependency {
	return &StepDependency{
		Name: name,
	}
}

// OptionalDependency creates a dependency that may be skipped.
// When used, the corresponding handler argument must be Optional[T].
// Use this when depending on a step that has a condition and may be skipped.
func OptionalDependency(name string) *StepDependency {
	return &StepDependency{
		Name:     name,
		Optional: true,
	}
}

type FlowInfo struct {
	Name      string     `json:"name"`
	Steps     []StepInfo `json:"steps"`
	CreatedAt time.Time  `json:"created_at"`
}

type StepInfo struct {
	Name      string               `json:"name"`
	DependsOn []StepDependencyInfo `json:"depends_on,omitempty"`
}

type StepDependencyInfo struct {
	Name string `json:"name"`
}

// NewFlow creates a new flow with the given name and step options
// Each FlowOpt (e.g., InitialStep, StepWithDependency) adds a step to the flow
func NewFlow(name string, opts ...FlowOpt) *Flow {
	f := &Flow{
		Name: name,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

type FlowOpt func(*Flow)
type StepOpt func(*Step)

type stepMessage struct {
	ID          int64                      `json:"id"`
	Deliveries  int                        `json:"deliveries"`
	Input       json.RawMessage            `json:"input"`
	StepOutputs map[string]json.RawMessage `json:"step_outputs"`
	SignalInput json.RawMessage            `json:"signal_input"`
}

type stepHandler struct {
	handlerOpts
	fn func(context.Context, stepMessage) ([]byte, error)
}

// InitialStep creates a flow step with no dependencies
// The handler receives the flow input directly and produces output
// Input and output types are automatically marshaled to/from JSON
func InitialStep[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...HandlerOpt) FlowOpt {
	handler := newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
		var in In
		if err := json.Unmarshal(p.Input, &in); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	step := &Step{
		Name:      name,
		Condition: handler.condition,
		DependsOn: nil,
		handler:   handler,
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithDependency creates a flow step that depends on one previous step
// The handler receives the flow input and the output of the dependency step
func StepWithDependency[In, Dep1Out, Out any](name string, dep1 *StepDependency, fn func(context.Context, In, Dep1Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	var dep1Out Dep1Out
	if err := validateDependencyOutputs([]*StepDependency{dep1}, []any{&dep1Out}); err != nil {
		panic(err)
	}
	handler := newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
		var in In
		var dep1Out Dep1Out
		if err := unmarshalStepArgs(p, []string{dep1.Name}, &in, []any{&dep1Out}); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in, dep1Out)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	step := &Step{
		Name:      name,
		Condition: handler.condition,
		DependsOn: []*StepDependency{dep1},
		handler:   handler,
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithTwoDependencies creates a flow step that depends on two previous steps
// The handler receives the flow input and the outputs of both dependency steps
func StepWithTwoDependencies[In, Dep1Out, Dep2Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	var dep1Out Dep1Out
	var dep2Out Dep2Out
	if err := validateDependencyOutputs([]*StepDependency{dep1, dep2}, []any{&dep1Out, &dep2Out}); err != nil {
		panic(err)
	}
	handler := newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
		var in In
		var dep1Out Dep1Out
		var dep2Out Dep2Out
		if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name}, &in, []any{&dep1Out, &dep2Out}); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in, dep1Out, dep2Out)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	step := &Step{
		Name:      name,
		Condition: handler.condition,
		DependsOn: []*StepDependency{dep1, dep2},
		handler:   handler,
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithThreeDependencies creates a flow step that depends on three previous steps
// The handler receives the flow input and the outputs of all three dependency steps
func StepWithThreeDependencies[In, Dep1Out, Dep2Out, Dep3Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	var dep1Out Dep1Out
	var dep2Out Dep2Out
	var dep3Out Dep3Out
	if err := validateDependencyOutputs([]*StepDependency{dep1, dep2, dep3}, []any{&dep1Out, &dep2Out, &dep3Out}); err != nil {
		panic(err)
	}
	handler := newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
		var in In
		var dep1Out Dep1Out
		var dep2Out Dep2Out
		var dep3Out Dep3Out
		if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name, dep3.Name}, &in, []any{&dep1Out, &dep2Out, &dep3Out}); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in, dep1Out, dep2Out, dep3Out)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	step := &Step{
		Name:      name,
		Condition: handler.condition,
		DependsOn: []*StepDependency{dep1, dep2, dep3},
		handler:   handler,
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// InitialStepWithSignal creates a flow step with no dependencies that requires a signal
// The handler receives the flow input and signal input, then produces output
// Step waits for signal delivery via Signal() before executing
func InitialStepWithSignal[In, SigIn, Out any](name string, fn func(context.Context, In, SigIn) (Out, error), opts ...HandlerOpt) FlowOpt {
	handler := newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
		var in In
		var sigIn SigIn
		if err := json.Unmarshal(p.Input, &in); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(p.SignalInput, &sigIn); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in, sigIn)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	step := &Step{
		Name:      name,
		Condition: handler.condition,
		HasSignal: true,
		DependsOn: nil,
		handler:   handler,
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithSignalAndDependency creates a flow step with one dependency that requires a signal
// The handler receives the flow input, signal input, and the output of the dependency step
// Step waits for both dependency completion AND signal delivery before executing
func StepWithSignalAndDependency[In, SigIn, Dep1Out, Out any](name string, dep1 *StepDependency, fn func(context.Context, In, SigIn, Dep1Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	var dep1Out Dep1Out
	if err := validateDependencyOutputs([]*StepDependency{dep1}, []any{&dep1Out}); err != nil {
		panic(err)
	}
	handler := newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
		var in In
		var sigIn SigIn
		var dep1Out Dep1Out
		if err := json.Unmarshal(p.Input, &in); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(p.SignalInput, &sigIn); err != nil {
			return nil, err
		}
		if err := unmarshalStepArgs(p, []string{dep1.Name}, &in, []any{&dep1Out}); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in, sigIn, dep1Out)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	step := &Step{
		Name:      name,
		Condition: handler.condition,
		HasSignal: true,
		DependsOn: []*StepDependency{dep1},
		handler:   handler,
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithSignalAndTwoDependencies creates a flow step with two dependencies that requires a signal
// The handler receives the flow input, signal input, and the outputs of both dependency steps
// Step waits for both dependencies to complete AND signal delivery before executing
func StepWithSignalAndTwoDependencies[In, SigIn, Dep1Out, Dep2Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, fn func(context.Context, In, SigIn, Dep1Out, Dep2Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	var dep1Out Dep1Out
	var dep2Out Dep2Out
	if err := validateDependencyOutputs([]*StepDependency{dep1, dep2}, []any{&dep1Out, &dep2Out}); err != nil {
		panic(err)
	}
	handler := newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
		var in In
		var sigIn SigIn
		var dep1Out Dep1Out
		var dep2Out Dep2Out
		if err := json.Unmarshal(p.Input, &in); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(p.SignalInput, &sigIn); err != nil {
			return nil, err
		}
		if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name}, &in, []any{&dep1Out, &dep2Out}); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in, sigIn, dep1Out, dep2Out)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	step := &Step{
		Name:      name,
		Condition: handler.condition,
		HasSignal: true,
		DependsOn: []*StepDependency{dep1, dep2},
		handler:   handler,
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithSignalAndThreeDependencies creates a flow step with three dependencies that requires a signal
// The handler receives the flow input, signal input, and the outputs of all three dependency steps
// Step waits for all dependencies to complete AND signal delivery before executing
func StepWithSignalAndThreeDependencies[In, SigIn, Dep1Out, Dep2Out, Dep3Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, fn func(context.Context, In, SigIn, Dep1Out, Dep2Out, Dep3Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	var dep1Out Dep1Out
	var dep2Out Dep2Out
	var dep3Out Dep3Out
	if err := validateDependencyOutputs([]*StepDependency{dep1, dep2, dep3}, []any{&dep1Out, &dep2Out, &dep3Out}); err != nil {
		panic(err)
	}
	handler := newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
		var in In
		var sigIn SigIn
		var dep1Out Dep1Out
		var dep2Out Dep2Out
		var dep3Out Dep3Out
		if err := json.Unmarshal(p.Input, &in); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(p.SignalInput, &sigIn); err != nil {
			return nil, err
		}
		if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name, dep3.Name}, &in, []any{&dep1Out, &dep2Out, &dep3Out}); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in, sigIn, dep1Out, dep2Out, dep3Out)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}, opts...)
	step := &Step{
		Name:      name,
		Condition: handler.condition,
		HasSignal: true,
		DependsOn: []*StepDependency{dep1, dep2, dep3},
		handler:   handler,
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

func newStepHandler(name string, fn func(context.Context, stepMessage) ([]byte, error), opts ...HandlerOpt) *stepHandler {
	h := &stepHandler{
		handlerOpts: handlerOpts{
			concurrency: 1,
			batchSize:   10,
		},
		fn: fn,
	}

	for _, opt := range opts {
		opt(&h.handlerOpts)
	}

	if err := h.handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return h
}

func isOptionalOutput(outputPtr any) bool {
	_, ok := outputPtr.(optionalSetter)
	return ok
}

func validateDependencyOutputs(deps []*StepDependency, outputs []any) error {
	if len(deps) != len(outputs) {
		return fmt.Errorf("dependency output mismatch")
	}
	for i, dep := range deps {
		if dep == nil {
			return fmt.Errorf("nil dependency")
		}
		isOptional := isOptionalOutput(outputs[i])
		if dep.Optional && !isOptional {
			return fmt.Errorf("optional dependency %q requires Optional[T] handler arg", dep.Name)
		}
		if !dep.Optional && isOptional {
			return fmt.Errorf("required dependency %q cannot use Optional[T] handler arg", dep.Name)
		}
	}
	return nil
}

func unmarshalStepArgs(p stepMessage, stepNames []string, in any, stepOutputs []any) error {
	if err := json.Unmarshal(p.Input, in); err != nil {
		return err
	}

	for i, stepName := range stepNames {
		b, ok := p.StepOutputs[stepName]
		if !ok {
			if opt, ok := stepOutputs[i].(optionalSetter); ok {
				opt.setAbsent()
				continue
			}
			return fmt.Errorf("missing step output for step: %s", stepName)
		}
		if opt, ok := stepOutputs[i].(optionalSetter); ok {
			if err := opt.setFromJSON(b); err != nil {
				return err
			}
			continue
		}
		if err := json.Unmarshal(b, stepOutputs[i]); err != nil {
			return err
		}
	}

	return nil
}

func validateFlowDependencies(flow *Flow) error {
	// Build set of conditional steps (steps that have a condition and may be skipped)
	conditionalSteps := make(map[string]bool)
	for _, step := range flow.Steps {
		if strings.TrimSpace(step.Condition) != "" {
			conditionalSteps[step.Name] = true
		}
	}

	// Validate all dependencies
	for _, step := range flow.Steps {
		for _, dep := range step.DependsOn {
			if dep == nil {
				return fmt.Errorf("step %q has nil dependency", step.Name)
			}
			// If depending on a conditional step, must use OptionalDependency
			if conditionalSteps[dep.Name] && !dep.Optional {
				return fmt.Errorf("dependency %q on step %q must be OptionalDependency because %q has a condition and can be skipped", dep.Name, step.Name, dep.Name)
			}
		}
	}

	return nil
}

// CreateFlow creates a new flow definition.
func CreateFlow(ctx context.Context, conn Conn, flow *Flow) error {
	if err := validateFlowDependencies(flow); err != nil {
		return err
	}
	b, err := json.Marshal(flow.Steps)
	if err != nil {
		return err
	}
	q := `SELECT * FROM cb_create_flow(name => $1, steps => $2);`
	_, err = conn.Exec(ctx, q, flow.Name, b)
	if err != nil {
		return err
	}
	return nil
}

// GetFlow retrieves flow metadata by name.
func GetFlow(ctx context.Context, conn Conn, name string) (*FlowInfo, error) {
	q := `SELECT * FROM cb_flow_info WHERE name = $1;`
	return scanFlow(conn.QueryRow(ctx, q, name))
}

// ListFlows returns all flows
func ListFlows(ctx context.Context, conn Conn) ([]*FlowInfo, error) {
	q := `SELECT * FROM cb_flow_info ORDER BY name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleFlow)
}

type RunFlowOpts = RunOpts

// RunFlow enqueues a flow execution and returns a handle for monitoring.
func RunFlow(ctx context.Context, conn Conn, name string, input any, opts *RunFlowOpts) (*RunHandle, error) {
	if opts == nil {
		opts = &RunFlowOpts{}
	}

	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	q := `SELECT * FROM cb_run_flow(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4);`
	var id int64
	err = conn.QueryRow(ctx, q, name, b, ptrOrNil(opts.ConcurrencyKey), ptrOrNil(opts.IdempotencyKey)).Scan(&id)
	if err != nil {
		return nil, err
	}
	return &RunHandle{conn: conn, getFn: GetFlowRun, Name: name, ID: id}, nil
}

// GetFlowRun retrieves a specific flow run result by ID.
func GetFlowRun(ctx context.Context, conn Conn, name string, id int64) (*RunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at, NULL::timestamptz as skipped_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanRun(conn.QueryRow(ctx, query, id))
}

// ListFlowRuns returns recent flow runs for the specified flow.
func ListFlowRuns(ctx context.Context, conn Conn, name string) ([]*RunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at, NULL::timestamptz as skipped_at FROM %s ORDER BY started_at DESC LIMIT 20;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleRun)
}

// StepRunInfo represents the execution state of a single step within a flow run.
type StepRunInfo struct {
	ID           int64           `json:"id"`
	StepName     string          `json:"step_name"`
	Status       string          `json:"status"`
	Output       json.RawMessage `json:"output,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	StartedAt    time.Time       `json:"started_at,omitzero"`
	CompletedAt  time.Time       `json:"completed_at,omitzero"`
	FailedAt     time.Time       `json:"failed_at,omitzero"`
	SkippedAt    time.Time       `json:"skipped_at,omitzero"`
}

// GetFlowRunSteps retrieves all step runs for a specific flow run.
func GetFlowRunSteps(ctx context.Context, conn Conn, flowName string, flowRunID int64) ([]*StepRunInfo, error) {
	tableName := fmt.Sprintf("cb_s_%s", strings.ToLower(flowName))
	query := fmt.Sprintf(`
		SELECT id, step_name, status, output, error_message, started_at, completed_at, failed_at, skipped_at 
		FROM %s 
		WHERE flow_run_id = $1 
		ORDER BY id;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query, flowRunID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*StepRunInfo, error) {
		var s StepRunInfo
		var output *json.RawMessage
		var errorMessage *string
		var startedAt, completedAt, failedAt, skippedAt *time.Time

		err := row.Scan(
			&s.ID,
			&s.StepName,
			&s.Status,
			&output,
			&errorMessage,
			&startedAt,
			&completedAt,
			&failedAt,
			&skippedAt,
		)
		if err != nil {
			return nil, err
		}

		// Copy nullable fields
		if output != nil {
			s.Output = *output
		}
		if errorMessage != nil {
			s.ErrorMessage = *errorMessage
		}
		if startedAt != nil {
			s.StartedAt = *startedAt
		}
		if completedAt != nil {
			s.CompletedAt = *completedAt
		}
		if failedAt != nil {
			s.FailedAt = *failedAt
		}
		if skippedAt != nil {
			s.SkippedAt = *skippedAt
		}

		return &s, nil
	})
}

// SignalFlow delivers a signal to a waiting step in a flow run.
// The step must have been defined with a signal variant (e.g., InitialStepWithSignal, StepWithSignalAndDependency).
// Signals enable human-in-the-loop workflows where a step waits for external input before executing.
// Returns an error if the signal was already delivered or the step doesn't require a signal.
func SignalFlow(ctx context.Context, conn Conn, flowName string, flowRunID int64, stepName string, input any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}
	q := `SELECT cb_signal_flow($1, $2, $3, $4);`
	var delivered bool
	err = conn.QueryRow(ctx, q, flowName, flowRunID, stepName, b).Scan(&delivered)
	if err != nil {
		return err
	}
	if !delivered {
		return fmt.Errorf("signal not delivered: step may not require signal or signal already delivered")
	}
	return nil
}

func scanCollectibleFlow(row pgx.CollectableRow) (*FlowInfo, error) {
	return scanFlow(row)
}

func scanFlow(row pgx.Row) (*FlowInfo, error) {
	rec := FlowInfo{}

	var steps json.RawMessage

	if err := row.Scan(
		&rec.Name,
		&steps,
		&rec.CreatedAt,
	); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(steps, &rec.Steps); err != nil {
		return nil, err
	}

	return &rec, nil
}
