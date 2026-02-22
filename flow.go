package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type Flow interface {
	Name() string
	Steps() []StepInterface
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

type stepMessage struct {
	ID          int64                      `json:"id"`
	Deliveries  int                        `json:"deliveries"`
	Input       json.RawMessage            `json:"input"`
	StepOutputs map[string]json.RawMessage `json:"step_outputs"`
	SignalInput json.RawMessage            `json:"signal_input"`
}

func validateFlowDependencies(flow Flow) error {
	steps := flow.Steps()
	if len(steps) == 0 {
		return fmt.Errorf("flow %q must have at least one step", flow.Name())
	}

	// Build set of conditional steps (steps that have a condition and may be skipped)
	conditionalSteps := make(map[string]bool)
	for _, step := range steps {
		if strings.TrimSpace(step.condition()) != "" {
			conditionalSteps[step.Name()] = true
		}
	}

	// Build set of all steps that have dependents
	dependencySet := make(map[string]bool)
	for _, step := range steps {
		for _, depName := range step.dependencies() {
			dependencySet[depName] = true
		}
	}

	// Find all steps with no dependents (final steps)
	var finalSteps []string
	for _, step := range steps {
		if !dependencySet[step.Name()] {
			finalSteps = append(finalSteps, step.Name())
		}
	}

	// Enforce single final step constraint
	if len(finalSteps) == 0 {
		return fmt.Errorf("flow %q has no final step (circular dependency?)", flow.Name())
	}

	if len(finalSteps) > 1 {
		return fmt.Errorf("flow %q must have exactly one final step, found %d: %v. When conditional branching creates multiple potential terminal steps, add an explicit reconvergence step that depends on all branches using OptionalDependency() and Optional[T] parameters", flow.Name(), len(finalSteps), finalSteps)
	}

	// Enforce that final step cannot have a condition (must always execute)
	finalStepName := finalSteps[0]
	for _, step := range steps {
		if step.Name() == finalStepName {
			if strings.TrimSpace(step.condition()) != "" {
				return fmt.Errorf("flow %q: final step %q cannot have a condition - it must always execute to guarantee output availability", flow.Name(), finalStepName)
			}
			break
		}
	}

	// Validate that conditional dependencies are marked as optional
	for _, step := range steps {
		depNames := step.dependencies()
		optFlags := step.optionalDependencies()

		for i, depName := range depNames {
			isConditional := conditionalSteps[depName]
			isOptional := false
			if optFlags != nil && i < len(optFlags) {
				isOptional = optFlags[i]
			}

			// If depending on a conditional step, must be marked optional
			if isConditional && !isOptional {
				return fmt.Errorf("step %q depends on conditional step %q but does not use Optional[T] type for that parameter", step.Name(), depName)
			}
		}
	}

	return nil
}

// CreateFlow creates a new flow definition.
func CreateFlow(ctx context.Context, conn Conn, flow Flow) error {
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
		HasSignal bool              `json:"has_signal"`
		DependsOn []*stepDependency `json:"depends_on,omitempty"`
	}

	steps := flow.Steps()
	serSteps := make([]serializableStep, len(steps))
	for i, s := range steps {
		// Convert dependency names to stepDependency objects
		depNames := s.dependencies()
		deps := make([]*stepDependency, len(depNames))
		for j, depName := range depNames {
			deps[j] = &stepDependency{Name: depName}
		}

		serStep := serializableStep{
			Name:      s.Name(),
			HasSignal: s.hasSignal(),
			DependsOn: deps,
		}
		serStep.Condition = s.condition()
		serSteps[i] = serStep
	}

	b, err := json.Marshal(serSteps)
	if err != nil {
		return err
	}
	q := `SELECT * FROM cb_create_flow(name => $1, steps => $2);`
	_, err = conn.Exec(ctx, q, flow.Name(), b)
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
