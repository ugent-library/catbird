package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Status constants for task and flow runs
const (
	StatusCreated   = "created"
	StatusStarted   = "started"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

var (
	// ErrTaskFailed is returned when a task run fails
	ErrTaskFailed = fmt.Errorf("task failed")
	// ErrFlowFailed is returned when a flow run fails
	ErrFlowFailed = fmt.Errorf("flow failed")
)

type handlerOpts struct {
	concurrency int
	batchSize   int
	maxDuration time.Duration
	maxRetries  int
	minDelay    time.Duration
	maxDelay    time.Duration
}

// HandlerOpt is an option for configuring task and flow step handlers
type HandlerOpt func(*handlerOpts)

// WithConcurrency sets the number of concurrent handler executions
func WithConcurrency(n int) HandlerOpt {
	return func(h *handlerOpts) {
		h.concurrency = n
	}
}

// WithMaxDuration sets the maximum duration for handler execution
func WithMaxDuration(d time.Duration) HandlerOpt {
	return func(h *handlerOpts) {
		h.maxDuration = d
	}
}

// WithBatchSize sets the number of messages to read per batch
func WithBatchSize(n int) HandlerOpt {
	return func(h *handlerOpts) {
		h.batchSize = n
	}
}

// WithMaxRetries sets the number of retry attempts for failed handlers
func WithMaxRetries(n int) HandlerOpt {
	return func(h *handlerOpts) {
		h.maxRetries = n
	}
}

// WithBackoff sets the delay between retries, exponentially backing off from minDelay to maxDelay
func WithBackoff(minDelay, maxDelay time.Duration) HandlerOpt {
	return func(h *handlerOpts) {
		h.minDelay = minDelay
		h.maxDelay = maxDelay
	}
}

// Task represents a task definition with a generic typed handler
type Task struct {
	Name    string `json:"name"`
	handler *taskHandler
}

type taskHandler struct {
	handlerOpts
	fn func(context.Context, []byte) ([]byte, error)
}

// NewTask creates a new task with a generic handler function
// The handler receives typed input and returns typed output
// Input and output types are automatically marshaled to/from JSON
func NewTask[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...HandlerOpt) *Task {
	h := &taskHandler{
		handlerOpts: handlerOpts{
			concurrency: 1,
			batchSize:   10,
		},
		fn: func(ctx context.Context, b []byte) ([]byte, error) {
			var in In
			if err := json.Unmarshal(b, &in); err != nil {
				return nil, err
			}

			out, err := fn(ctx, in)
			if err != nil {
				return nil, err
			}

			return json.Marshal(out)
		},
	}

	for _, opt := range opts {
		opt(&h.handlerOpts)
	}

	return &Task{
		Name:    name,
		handler: h,
	}
}

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

type stepMessage struct {
	ID          int64                      `json:"id"`
	Deliveries  int                        `json:"deliveries"`
	FlowInput   json.RawMessage            `json:"flow_input"`
	StepOutputs map[string]json.RawMessage `json:"step_outputs"`
}

func (m stepMessage) getID() int64 { return m.ID }

type handlerMessage interface {
	getID() int64
}

type stepHandler struct {
	handlerOpts
	fn func(context.Context, stepMessage) ([]byte, error)
}

type StepOpt func(*Step)

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

// InitialStep creates a flow step with no dependencies
// The handler receives the flow input directly and produces output
// Input and output types are automatically marshaled to/from JSON
func InitialStep[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...HandlerOpt) FlowOpt {
	step := &Step{
		Name:      name,
		DependsOn: nil,
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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
		handler: newStepHandler(func(ctx context.Context, p stepMessage) ([]byte, error) {
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

func newStepHandler(fn func(context.Context, stepMessage) ([]byte, error), opts ...HandlerOpt) *stepHandler {
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

type TaskInfo struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type TaskRunInfo struct {
	ID              int64           `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Status          string          `json:"status"`
	Input           json.RawMessage `json:"input,omitempty"`
	Output          json.RawMessage `json:"output,omitempty"`
	ErrorMessage    string          `json:"error_message,omitempty"`
	StartedAt       time.Time       `json:"started_at,omitzero"`
	CompletedAt     time.Time       `json:"completed_at,omitzero"`
	FailedAt        time.Time       `json:"failed_at,omitzero"`
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

type FlowRunInfo struct {
	ID              int64           `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Status          string          `json:"status"`
	Input           json.RawMessage `json:"input,omitempty"`
	Output          json.RawMessage `json:"output,omitempty"`
	ErrorMessage    string          `json:"error_message,omitempty"`
	StartedAt       time.Time       `json:"started_at,omitzero"`
	CompletedAt     time.Time       `json:"completed_at,omitzero"`
	FailedAt        time.Time       `json:"failed_at,omitzero"`
}

type TaskHandlerInfo struct {
	TaskName string `json:"task_name"`
}

type StepHandlerInfo struct {
	FlowName string `json:"flow_name"`
	StepName string `json:"step_name"`
}

// CreateTask creates a new task definition.
func CreateTask(ctx context.Context, conn Conn, task *Task) error {
	q := `SELECT * FROM cb_create_task(name => $1);`
	_, err := conn.Exec(ctx, q, task.Name)
	if err != nil {
		return err
	}
	return nil
}

// GetTask retrieves task metadata by name.
func GetTask(ctx context.Context, conn Conn, name string) (*TaskInfo, error) {
	q := `SELECT name, created_at FROM cb_tasks WHERE name = $1;`
	return scanTask(conn.QueryRow(ctx, q, name))
}

// ListTasks returns all tasks
func ListTasks(ctx context.Context, conn Conn) ([]*TaskInfo, error) {
	q := `SELECT name, created_at FROM cb_tasks ORDER BY name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleTask)
}

type RunTaskOpts struct {
	DeduplicationID string
}

// RunTask enqueues a task execution and returns a handle for monitoring
// progress and retrieving output.
func RunTask(ctx context.Context, conn Conn, name string, input any) (*TaskHandle, error) {
	return RunTaskWithOpts(ctx, conn, name, input, RunTaskOpts{})
}

// RunTaskWithOpts enqueues a task with options for deduplication and returns
// a handle for monitoring.
func RunTaskWithOpts(ctx context.Context, conn Conn, name string, input any, opts RunTaskOpts) (*TaskHandle, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	q := `SELECT * FROM cb_run_task(name => $1, input => $2, deduplication_id => $3);`
	var id int64
	err = conn.QueryRow(ctx, q, name, b, ptrOrNil(opts.DeduplicationID)).Scan(&id)
	if err != nil {
		return nil, err
	}

	return &TaskHandle{conn: conn, name: name, id: id}, nil
}

// TaskHandle is a handle to a running or completed task execution
type TaskHandle struct {
	conn Conn
	name string
	id   int64
}

// Name returns the task name
func (h *TaskHandle) Name() string {
	return h.name
}

// ID returns the task run ID
func (h *TaskHandle) ID() int64 {
	return h.id
}

// WaitForOutput blocks until the task completes and unmarshals the output
func (h *TaskHandle) WaitForOutput(ctx context.Context, out any) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info, err := GetTaskRun(ctx, h.conn, h.name, h.id)
			if err != nil {
				return err
			}
			switch info.Status {
			case StatusCompleted:
				return json.Unmarshal(info.Output, out)
			case StatusFailed:
				return fmt.Errorf("%w: %s", ErrTaskFailed, info.ErrorMessage)
			}
		}
	}
}

// GetTaskRun retrieves a specific task run result by ID.
func GetTaskRun(ctx context.Context, conn Conn, name string, id int64) (*TaskRunInfo, error) {
	tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, deduplication_id, status, input, output, error_message, started_at, completed_at, failed_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanTaskRun(conn.QueryRow(ctx, query, id))
}

// ListTaskRuns returns recent task runs for the specified task.
func ListTaskRuns(ctx context.Context, conn Conn, name string) ([]*TaskRunInfo, error) {
	tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, deduplication_id, status, input, output, error_message, started_at, completed_at, failed_at FROM %s ORDER BY started_at DESC LIMIT 20;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleTaskRun)
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
func RunFlow(ctx context.Context, conn Conn, name string, input any) (*FlowHandle, error) {
	return RunFlowWithOpts(ctx, conn, name, input, RunFlowOpts{})
}

// RunFlowWithOpts enqueues a flow with options for deduplication and returns
// a handle for monitoring.
func RunFlowWithOpts(ctx context.Context, conn Conn, name string, input any, opts RunFlowOpts) (*FlowHandle, error) {
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
	return &FlowHandle{id: id, name: name, conn: conn}, nil
}

// FlowHandle is a handle to a running or completed flow execution
type FlowHandle struct {
	id   int64
	name string
	conn Conn
}

// ID returns the flow run ID
func (h *FlowHandle) ID() int64 {
	return h.id
}

// WaitForOutput blocks until the flow completes and unmarshals the output
// Flow output contains combined JSON object of all step outputs
func (h *FlowHandle) WaitForOutput(ctx context.Context, out any) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info, err := GetFlowRun(ctx, h.conn, h.name, h.id)
			if err != nil {
				return err
			}
			switch info.Status {
			case StatusCompleted:
				return json.Unmarshal(info.Output, out)
			case StatusFailed:
				return fmt.Errorf("%w: %s", ErrFlowFailed, info.ErrorMessage)
			}
		}
	}
}

// GetFlowRun retrieves a specific flow run result by ID.
func GetFlowRun(ctx context.Context, conn Conn, name string, id int64) (*FlowRunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, deduplication_id, status, input, output, error_message, started_at, completed_at, failed_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanFlowRun(conn.QueryRow(ctx, query, id))
}

// ListFlowRuns returns recent flow runs for the specified flow.
func ListFlowRuns(ctx context.Context, conn Conn, name string) ([]*FlowRunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, deduplication_id, status, input, output, error_message, started_at, completed_at, failed_at FROM %s ORDER BY started_at DESC LIMIT 20;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleFlowRun)
}

func scanCollectibleTask(row pgx.CollectableRow) (*TaskInfo, error) {
	return scanTask(row)
}

func scanTask(row pgx.Row) (*TaskInfo, error) {
	rec := TaskInfo{}

	if err := row.Scan(
		&rec.Name,
		&rec.CreatedAt,
	); err != nil {
		return nil, err
	}

	return &rec, nil
}

func scanCollectibleTaskRun(row pgx.CollectableRow) (*TaskRunInfo, error) {
	return scanTaskRun(row)
}

func scanTaskRun(row pgx.Row) (*TaskRunInfo, error) {
	rec := TaskRunInfo{}

	var deduplicationID *string
	var input *json.RawMessage
	var output *json.RawMessage
	var errorMessage *string
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&rec.Status,
		&input,
		&output,
		&errorMessage,
		&rec.StartedAt,
		&completedAt,
		&failedAt,
	); err != nil {
		return nil, err
	}

	if deduplicationID != nil {
		rec.DeduplicationID = *deduplicationID
	}
	if input != nil {
		rec.Input = *input
	}
	if output != nil {
		rec.Output = *output
	}
	if errorMessage != nil {
		rec.ErrorMessage = *errorMessage
	}
	if completedAt != nil {
		rec.CompletedAt = *completedAt
	}
	if failedAt != nil {
		rec.FailedAt = *failedAt
	}

	return &rec, nil
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

func scanCollectibleFlowRun(row pgx.CollectableRow) (*FlowRunInfo, error) {
	return scanFlowRun(row)
}

func scanFlowRun(row pgx.Row) (*FlowRunInfo, error) {
	rec := FlowRunInfo{}

	var deduplicationID *string
	var input *json.RawMessage
	var output *json.RawMessage
	var errorMessage *string
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&rec.Status,
		&input,
		&output,
		&errorMessage,
		&rec.StartedAt,
		&completedAt,
		&failedAt,
	); err != nil {
		return nil, err
	}

	if deduplicationID != nil {
		rec.DeduplicationID = *deduplicationID
	}
	if input != nil {
		rec.Input = *input
	}
	if output != nil {
		rec.Output = *output
	}
	if errorMessage != nil {
		rec.ErrorMessage = *errorMessage
	}
	if completedAt != nil {
		rec.CompletedAt = *completedAt
	}
	if failedAt != nil {
		rec.FailedAt = *failedAt
	}

	return &rec, nil
}
