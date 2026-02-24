package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type Flow struct {
	name  string
	steps []Step
}

func NewFlow(name string) *Flow {
	return &Flow{name: name}
}

func (f *Flow) AddStep(step *Step) *Flow {
	f.steps = append(f.steps, *step)
	return f
}

type Step struct {
	name                 string
	dependencies         []string
	optionalDependencies map[string]bool // tracks which dependencies are Optional[T]
	condition            string
	signal               bool
	handler              func(context.Context, []byte, map[string][]byte, []byte) ([]byte, error)
	handlerOpts          *HandlerOpts
}

func NewStep(name string) *Step {
	return &Step{
		name:                 name,
		optionalDependencies: make(map[string]bool),
	}
}

func (s *Step) DependsOn(deps ...string) *Step {
	s.dependencies = append(s.dependencies, deps...)
	return s
}

func (s *Step) Condition(condition string) *Step {
	s.condition = condition
	return s
}

func (s *Step) Signal(signal bool) *Step {
	s.signal = signal
	return s
}

func (s *Step) Handler(fn any, opts *HandlerOpts) *Step {
	handler, optionalDeps, err := makeStepHandler(fn, s.name, s.dependencies, s.signal)
	if err != nil {
		panic(err)
	}
	s.handler = handler
	s.optionalDependencies = optionalDeps
	s.handlerOpts = applyDefaultHandlerOpts(opts)
	return s
}

// makeStepHandler uses reflection once to extract types and create cached wrapper for step handlers.
// Step handler signature: (context.Context, In, [Signal if signal], [Dep1, Dep2, ...]) (Out, error)
// Returns wrapper with signature: (context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error)
// Also returns a map indicating which dependencies are Optional[T]
func makeStepHandler(fn any, stepName string, dependencies []string, signal bool) (func(context.Context, []byte, map[string][]byte, []byte) ([]byte, error), map[string]bool, error) {
	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

	// Validate signature at build time
	if fnType.Kind() != reflect.Func {
		return nil, nil, fmt.Errorf("step %s: handler must be a function", stepName)
	}

	// Expected signature: (context.Context, In, [Signal if signal], [Dep1, Dep2, ...]) (Out, error)
	expectedInputs := 2 // ctx + flow input
	if signal {
		expectedInputs++ // signal input
	}
	expectedInputs += len(dependencies) // dependency outputs

	if fnType.NumIn() != expectedInputs {
		return nil, nil, fmt.Errorf("step %s: handler must have %d inputs (context.Context, In%s%s), got %d",
			stepName, expectedInputs,
			func() string {
				if signal {
					return ", Signal"
				}
				return ""
			}(),
			func() string {
				if len(dependencies) > 0 {
					return fmt.Sprintf(", %d dependencies", len(dependencies))
				}
				return ""
			}(),
			fnType.NumIn())
	}

	// Validate context.Context is first parameter
	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, nil, fmt.Errorf("step %s: first parameter must be context.Context, got %v", stepName, fnType.In(0))
	}

	// Validate error return
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if fnType.NumOut() != 2 || !fnType.Out(1).Implements(errorType) {
		return nil, nil, fmt.Errorf("step %s: handler must return (Out, error)", stepName)
	}

	// Determine parameter indices
	signalParamIdx := 2 // Signal comes after ctx and flow input (if present)
	depStartIdx := 2    // Dependencies start here
	if signal {
		depStartIdx = 3 // Dependencies start after signal
	}

	// CACHE: Extract all type info once - stored in closure
	flowInputType := fnType.In(1)
	var signalType reflect.Type
	if signal {
		signalType = fnType.In(signalParamIdx)
	}

	// Cache dependency types and check for Optional[T]
	depTypes := make([]reflect.Type, len(dependencies))
	depIsOptional := make([]bool, len(dependencies))
	optionalDepsMap := make(map[string]bool)
	for i := range dependencies {
		paramType := fnType.In(depStartIdx + i)
		depTypes[i] = paramType

		// Check if type is Optional[T] (struct with IsSet bool and Value T fields)
		isOptional := paramType.Kind() == reflect.Struct &&
			paramType.NumField() >= 2 &&
			paramType.Field(0).Name == "IsSet" &&
			paramType.Field(0).Type.Kind() == reflect.Bool &&
			paramType.Field(1).Name == "Value"
		depIsOptional[i] = isOptional
		optionalDepsMap[dependencies[i]] = isOptional
	}

	// Return wrapper with all type info cached in closure
	return func(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
		// Build arguments slice starting with context
		args := []reflect.Value{reflect.ValueOf(ctx)}

		// 1. Unmarshal flow input using cached type
		flowInputVal := reflect.New(flowInputType)
		if err := json.Unmarshal(flowInputJSON, flowInputVal.Interface()); err != nil {
			return nil, fmt.Errorf("unmarshal flow input: %w", err)
		}
		args = append(args, flowInputVal.Elem())

		// 2. Unmarshal signal if present (uses cached signalType)
		if signal {
			signalVal := reflect.New(signalType)
			if len(signalInputJSON) > 0 {
				if err := json.Unmarshal(signalInputJSON, signalVal.Interface()); err != nil {
					return nil, fmt.Errorf("unmarshal signal: %w", err)
				}
			}
			args = append(args, signalVal.Elem())
		}

		// 3. Unmarshal dependencies using cached depTypes
		for i, depName := range dependencies {
			if depIsOptional[i] {
				// Handle Optional[T] - construct directly
				optVal := reflect.New(depTypes[i]).Elem()
				if depOutput, exists := depsJSON[depName]; exists && len(depOutput) > 0 {
					// Set IsSet = true
					optVal.FieldByName("IsSet").SetBool(true)
					// Unmarshal into Value field
					valueField := optVal.FieldByName("Value")
					valuePtr := reflect.New(valueField.Type())
					if err := json.Unmarshal(depOutput, valuePtr.Interface()); err != nil {
						return nil, fmt.Errorf("unmarshal optional dependency %s: %w", depName, err)
					}
					valueField.Set(valuePtr.Elem())
				}
				// If not exists, optVal stays as Optional{IsSet: false}
				args = append(args, optVal)
			} else {
				// Handle required dependency
				depVal := reflect.New(depTypes[i])
				depJSON, ok := depsJSON[depName]
				if !ok {
					return nil, fmt.Errorf("missing required dependency: %s", depName)
				}
				if err := json.Unmarshal(depJSON, depVal.Interface()); err != nil {
					return nil, fmt.Errorf("unmarshal dependency %s: %w", depName, err)
				}
				args = append(args, depVal.Elem())
			}
		}

		// 4. Call handler using cached fnVal (~0.5-1μs overhead)
		results := fnVal.Call(args)

		// 5. Check error
		if !results[1].IsNil() {
			return nil, results[1].Interface().(error)
		}

		// 6. Marshal output (unavoidable cost)
		return json.Marshal(results[0].Interface())
	}, optionalDepsMap, nil
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

type stepClaim struct {
	ID          int64                      `json:"id"`
	Attempts    int                        `json:"attempts"`
	Input       json.RawMessage            `json:"input"`
	StepOutputs map[string]json.RawMessage `json:"step_outputs"`
	SignalInput json.RawMessage            `json:"signal_input"`
}

func validateFlowDependencies(flow *Flow) error {
	steps := flow.steps
	if len(steps) == 0 {
		return fmt.Errorf("flow %q must have at least one step", flow.name)
	}

	// Build set of conditional steps (steps that have a condition and may be skipped)
	conditionalSteps := make(map[string]bool)
	for _, step := range steps {
		if strings.TrimSpace(step.condition) != "" {
			conditionalSteps[step.name] = true
		}
	}

	// Build set of all steps that have dependents
	dependencySet := make(map[string]bool)
	for _, step := range steps {
		for _, depName := range step.dependencies {
			dependencySet[depName] = true
		}
	}

	// Find all steps with no dependents (final steps)
	var finalSteps []string
	for _, step := range steps {
		if !dependencySet[step.name] {
			finalSteps = append(finalSteps, step.name)
		}
	}

	// Enforce single final step constraint
	if len(finalSteps) == 0 {
		return fmt.Errorf("flow %q has no final step (circular dependency?)", flow.name)
	}

	if len(finalSteps) > 1 {
		return fmt.Errorf("flow %q must have exactly one final step, found %d: %v. When conditional branching creates multiple potential terminal steps, add an explicit reconvergence step that depends on all branches using OptionalDependency() and Optional[T] parameters", flow.name, len(finalSteps), finalSteps)
	}

	// Enforce that final step cannot have a condition (must always execute)
	finalStepName := finalSteps[0]
	for _, step := range steps {
		if step.name == finalStepName {
			if strings.TrimSpace(step.condition) != "" {
				return fmt.Errorf("flow %q: final step %q cannot have a condition - it must always execute to guarantee output availability", flow.name, finalStepName)
			}
			break
		}
	}

	// Validate that dependencies on conditional steps use Optional[T]
	for _, step := range steps {
		for _, depName := range step.dependencies {
			if conditionalSteps[depName] {
				// This is a dependency on a conditional step
				// Check if the dependency is marked as optional
				if !step.optionalDependencies[depName] {
					return fmt.Errorf("flow %q: step %q depends on conditional step %q but the handler does not use Optional[T] parameter - use Optional[%T] in the handler signature", flow.name, step.name, depName, depName)
				}
			}
		}
	}

	return nil
}

// CreateFlow creates one or more flow definitions.
func CreateFlow(ctx context.Context, conn Conn, flows ...*Flow) error {
	q := `SELECT * FROM cb_create_flow(name => $1, steps => $2);`
	for _, flow := range flows {
		if err := validateFlowDependencies(flow); err != nil {
			return err
		}

		// Need to marshal the steps with their public fields for JSON
		// Convert to a serializable structure
		type stepDependency struct {
			Name string `json:"name"`
		}
		type serializableStep struct {
			Name      string            `json:"name"`
			Condition string            `json:"condition,omitempty"`
			Signal    bool              `json:"signal"`
			DependsOn []*stepDependency `json:"depends_on,omitempty"`
		}

		steps := flow.steps
		serSteps := make([]serializableStep, len(steps))
		for i, s := range steps {
			// Convert dependency names to stepDependency objects
			depNames := s.dependencies
			deps := make([]*stepDependency, len(depNames))
			for j, depName := range depNames {
				deps[j] = &stepDependency{Name: depName}
			}

			serStep := serializableStep{
				Name:      s.name,
				Signal:    s.signal,
				DependsOn: deps,
			}
			serStep.Condition = s.condition
			serSteps[i] = serStep
		}

		b, err := json.Marshal(serSteps)
		if err != nil {
			return err
		}

		_, err = conn.Exec(ctx, q, flow.name, b)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetFlow retrieves flow metadata by name.
func GetFlow(ctx context.Context, conn Conn, flowName string) (*FlowInfo, error) {
	q := `SELECT * FROM cb_flow_info WHERE name = $1;`
	return scanFlow(conn.QueryRow(ctx, q, flowName))
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
func RunFlow(ctx context.Context, conn Conn, flowName string, input any, opts *RunFlowOpts) (*RunHandle, error) {
	q, args, err := RunFlowQuery(flowName, input, opts)
	if err != nil {
		return nil, err
	}

	var id int64
	err = conn.QueryRow(ctx, q, args...).Scan(&id)
	if err != nil {
		return nil, err
	}
	return &RunHandle{conn: conn, getFn: GetFlowRun, Name: flowName, ID: id}, nil
}

// RunFlowQuery builds the SQL query and args for a RunFlow operation.
// Pass nil opts to use defaults.
func RunFlowQuery(flowName string, input any, opts *RunFlowOpts) (string, []any, error) {
	if opts == nil {
		opts = &RunFlowOpts{}
	}

	b, err := json.Marshal(input)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT * FROM cb_run_flow(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4);`
	args := []any{flowName, b, ptrOrNil(opts.ConcurrencyKey), ptrOrNil(opts.IdempotencyKey)}

	return q, args, nil
}

// GetFlowRun retrieves a specific flow run result by ID.
func GetFlowRun(ctx context.Context, conn Conn, flowName string, flowRunID int64) (*RunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(flowName))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at, NULL::timestamptz as skipped_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanRun(conn.QueryRow(ctx, query, flowRunID))
}

// ListFlowRuns returns recent flow runs for the specified flow.
func ListFlowRuns(ctx context.Context, conn Conn, flowName string) ([]*RunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(flowName))
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
// The step must have been defined with a signal variant (e.g., NewStepWithSignal, NewStepWithSignalAndDependency).
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
