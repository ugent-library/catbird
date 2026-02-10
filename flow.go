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
	Name  string  `json:"name"`
	Steps []*Step `json:"steps"`
}

type Step struct {
	Name      string            `json:"name"`
	DependsOn []*StepDependency `json:"depends_on,omitempty"`
	handler   *stepHandler
}

type StepDependency struct {
	Name string `json:"name"`
}

// Dependency creates a step dependency reference by name
// Used when defining flow steps that depend on other steps
func Dependency(name string) *StepDependency {
	return &StepDependency{Name: name}
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
// Each FlowOpt (e.g., InitialStep, StepWithOneDependency) adds a step to the flow
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
	FlowInput   json.RawMessage            `json:"flow_input"`
	StepOutputs map[string]json.RawMessage `json:"step_outputs"`
}

type stepHandler struct {
	handlerOpts
	fn func(context.Context, stepMessage) ([]byte, error)
}

// InitialStep creates a flow step with no dependencies
// The handler receives the flow input directly and produces output
// Input and output types are automatically marshaled to/from JSON
func InitialStep[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: nil,
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
			var in In
			if err := json.Unmarshal(p.FlowInput, &in); err != nil {
				return nil, err
			}
			out, err := fn(ctx, in)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, opts...),
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithOneDependency creates a flow step that depends on one previous step
// The handler receives the flow input and the output of the dependency step
func StepWithOneDependency[In, Dep1Out, Out any](name string, dep1 *StepDependency, fn func(context.Context, In, Dep1Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: []*StepDependency{dep1},
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		}, opts...),
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithTwoDependencies creates a flow step that depends on two previous steps
// The handler receives the flow input and the outputs of both dependency steps
func StepWithTwoDependencies[In, Dep1Out, Dep2Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: []*StepDependency{dep1, dep2},
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		}, opts...),
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithThreeDependencies creates a flow step that depends on three previous steps
// The handler receives the flow input and the outputs of all three dependency steps
func StepWithThreeDependencies[In, Dep1Out, Dep2Out, Dep3Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: []*StepDependency{dep1, dep2, dep3},
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		}, opts...),
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithFourDependencies creates a flow step that depends on four previous steps
// The handler receives the flow input and the outputs of all four dependency steps
func StepWithFourDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: []*StepDependency{dep1, dep2, dep3, dep4},
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name, dep3.Name, dep4.Name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out}); err != nil {
				return nil, err
			}
			out, err := fn(ctx, in, dep1Out, dep2Out, dep3Out, dep4Out)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, opts...),
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithFiveDependencies creates a flow step that depends on five previous steps
// The handler receives the flow input and the outputs of all five dependency steps
func StepWithFiveDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, dep5 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: []*StepDependency{dep1, dep2, dep3, dep4, dep5},
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			var dep5Out Dep5Out
			if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name, dep3.Name, dep4.Name, dep5.Name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out, &dep5Out}); err != nil {
				return nil, err
			}
			out, err := fn(ctx, in, dep1Out, dep2Out, dep3Out, dep4Out, dep5Out)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, opts...),
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithSixDependencies creates a flow step that depends on six previous steps
// The handler receives the flow input and the outputs of all six dependency steps
func StepWithSixDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, dep5 *StepDependency, dep6 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: []*StepDependency{dep1, dep2, dep3, dep4, dep5, dep6},
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			var dep5Out Dep5Out
			var dep6Out Dep6Out
			if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name, dep3.Name, dep4.Name, dep5.Name, dep6.Name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out, &dep5Out, &dep6Out}); err != nil {
				return nil, err
			}
			out, err := fn(ctx, in, dep1Out, dep2Out, dep3Out, dep4Out, dep5Out, dep6Out)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, opts...),
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithSevenDependencies creates a flow step that depends on seven previous steps
// The handler receives the flow input and the outputs of all seven dependency steps
func StepWithSevenDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, dep5 *StepDependency, dep6 *StepDependency, dep7 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: []*StepDependency{dep1, dep2, dep3, dep4, dep5, dep6, dep7},
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			var dep5Out Dep5Out
			var dep6Out Dep6Out
			var dep7Out Dep7Out
			if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name, dep3.Name, dep4.Name, dep5.Name, dep6.Name, dep7.Name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out, &dep5Out, &dep6Out, &dep7Out}); err != nil {
				return nil, err
			}
			out, err := fn(ctx, in, dep1Out, dep2Out, dep3Out, dep4Out, dep5Out, dep6Out, dep7Out)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, opts...),
	}
	return func(f *Flow) {
		f.Steps = append(f.Steps, step)
	}
}

// StepWithEightDependencies creates a flow step that depends on eight previous steps
// The handler receives the flow input and the outputs of all eight dependency steps
func StepWithEightDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Dep8Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, dep5 *StepDependency, dep6 *StepDependency, dep7 *StepDependency, dep8 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Dep8Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: []*StepDependency{dep1, dep2, dep3, dep4, dep5, dep6, dep7, dep8},
		handler: newStepHandler(name, func(ctx context.Context, p stepMessage) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			var dep5Out Dep5Out
			var dep6Out Dep6Out
			var dep7Out Dep7Out
			var dep8Out Dep8Out
			if err := unmarshalStepArgs(p, []string{dep1.Name, dep2.Name, dep3.Name, dep4.Name, dep5.Name, dep6.Name, dep7.Name, dep8.Name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out, &dep5Out, &dep6Out, &dep7Out, &dep8Out}); err != nil {
				return nil, err
			}
			out, err := fn(ctx, in, dep1Out, dep2Out, dep3Out, dep4Out, dep5Out, dep6Out, dep7Out, dep8Out)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out)
		}, opts...),
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

	if err := h.handlerOpts.Validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for step %s: %v", name, err))
	}

	return h
}

func unmarshalStepArgs(p stepMessage, stepNames []string, in any, stepOutputs []any) error {
	if err := json.Unmarshal(p.FlowInput, in); err != nil {
		return err
	}

	for i, stepName := range stepNames {
		b, ok := p.StepOutputs[stepName]
		if !ok {
			return fmt.Errorf("missing step output for step: %s", stepName)
		}
		if err := json.Unmarshal(b, stepOutputs[i]); err != nil {
			return err
		}
	}

	return nil
}

// CreateFlow creates a new flow definition.
func CreateFlow(ctx context.Context, conn Conn, flow *Flow) error {
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

type RunFlowOpts struct {
	DeduplicationID string
}

// RunFlow enqueues a flow execution and returns a handle for monitoring.
func RunFlow(ctx context.Context, conn Conn, name string, input any) (*RunHandle, error) {
	return RunFlowWithOpts(ctx, conn, name, input, RunFlowOpts{})
}

// RunFlowWithOpts enqueues a flow with options for deduplication and returns
// a handle for monitoring.
func RunFlowWithOpts(ctx context.Context, conn Conn, name string, input any, opts RunFlowOpts) (*RunHandle, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	q := `SELECT * FROM cb_run_flow(name => $1, input => $2, deduplication_id => $3);`
	var id int64
	err = conn.QueryRow(ctx, q, name, b, ptrOrNil(opts.DeduplicationID)).Scan(&id)
	if err != nil {
		return nil, err
	}
	return &RunHandle{conn: conn, getFn: GetFlowRun, Name: name, ID: id}, nil
}

// GetFlowRun retrieves a specific flow run result by ID.
func GetFlowRun(ctx context.Context, conn Conn, name string, id int64) (*RunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, deduplication_id, status, input, output, error_message, started_at, completed_at, failed_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanRun(conn.QueryRow(ctx, query, id))
}

// ListFlowRuns returns recent flow runs for the specified flow.
func ListFlowRuns(ctx context.Context, conn Conn, name string) ([]*RunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, deduplication_id, status, input, output, error_message, started_at, completed_at, failed_at FROM %s ORDER BY started_at DESC LIMIT 20;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleRun)
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
