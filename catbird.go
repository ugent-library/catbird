// Package catbird provides a PostgreSQL-based message queue
// with task and workflow execution engine.
package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// Conn is an interface for database connections compatible with pgx.Conn and pgx.Pool
type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Message represents a message in a queue
type Message struct {
	ID              int64           `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Topic           string          `json:"topic"`
	Payload         json.RawMessage `json:"payload"`
	Deliveries      int             `json:"deliveries"`
	CreatedAt       time.Time       `json:"created_at"`
	DeliverAt       time.Time       `json:"deliver_at"`
}

type handlerOpts struct {
	concurrency int
	batchSize   int
	timeout     time.Duration
	retries     int
	retryDelay  time.Duration
}

// HandlerOpt is an option for configuring task and flow step handlers
type HandlerOpt interface {
	apply(*handlerOpts)
}

type concurrencyOpt struct {
	concurrency int
}

func (o concurrencyOpt) apply(h *handlerOpts) {
	h.concurrency = o.concurrency
}

// WithConcurrency sets the number of concurrent handler executions
func WithConcurrency(n int) HandlerOpt {
	return concurrencyOpt{concurrency: n}
}

type timeoutOpt struct {
	timeout time.Duration
}

func (o timeoutOpt) apply(h *handlerOpts) {
	h.timeout = o.timeout
}

// WithTimeout sets the timeout for handler execution
func WithTimeout(d time.Duration) HandlerOpt {
	return timeoutOpt{timeout: d}
}

type batchSizeOpt struct {
	batchSize int
}

func (o batchSizeOpt) apply(h *handlerOpts) {
	h.batchSize = o.batchSize
}

// WithBatchSize sets the number of messages to read per batch
func WithBatchSize(n int) HandlerOpt {
	return batchSizeOpt{batchSize: n}
}

type retriesOpt struct {
	retries int
}

func (o retriesOpt) apply(h *handlerOpts) {
	h.retries = o.retries
}

// WithRetries sets the number of retry attempts for failed handlers
func WithRetries(n int) HandlerOpt {
	return retriesOpt{retries: n}
}

type retryDelayOpt struct {
	delay time.Duration
}

func (o retryDelayOpt) apply(h *handlerOpts) {
	h.retryDelay = o.delay
}

// WithRetryDelay sets the initial delay between retry attempts
func WithRetryDelay(d time.Duration) HandlerOpt {
	return retryDelayOpt{delay: d}
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
		opt.apply(&h.handlerOpts)
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
	FlowRunID   int64                      `json:"flow_run_id"`
	StepName    string                     `json:"step_name"`
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
		opt.apply(f)
	}
	return f
}

type FlowOpt interface {
	apply(*Flow)
}

type stepOpt struct {
	step *Step
}

func (o stepOpt) apply(f *Flow) {
	f.Steps = append(f.Steps, o.step)
}

// InitialStep creates a flow step with no dependencies
// The handler receives the flow input directly and produces output
// Input and output types are automatically marshaled to/from JSON
func InitialStep[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
}

// StepWithOneDependency creates a flow step that depends on one previous step
// The handler receives the flow input and the output of the dependency step
func StepWithOneDependency[In, Dep1Out, Out any](name string, dep1 *StepDependency, fn func(context.Context, In, Dep1Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
}

// StepWithTwoDependencies creates a flow step that depends on two previous steps
// The handler receives the flow input and the outputs of both dependency steps
func StepWithTwoDependencies[In, Dep1Out, Dep2Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
}

// StepWithThreeDependencies creates a flow step that depends on three previous steps
// The handler receives the flow input and the outputs of all three dependency steps
func StepWithThreeDependencies[In, Dep1Out, Dep2Out, Dep3Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
}

// StepWithFourDependencies creates a flow step that depends on four previous steps
// The handler receives the flow input and the outputs of all four dependency steps
func StepWithFourDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
}

// StepWithFiveDependencies creates a flow step that depends on five previous steps
// The handler receives the flow input and the outputs of all five dependency steps
func StepWithFiveDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, dep5 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
}

// StepWithSixDependencies creates a flow step that depends on six previous steps
// The handler receives the flow input and the outputs of all six dependency steps
func StepWithSixDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, dep5 *StepDependency, dep6 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
}

// StepWithSevenDependencies creates a flow step that depends on seven previous steps
// The handler receives the flow input and the outputs of all seven dependency steps
func StepWithSevenDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, dep5 *StepDependency, dep6 *StepDependency, dep7 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
}

// StepWithEightDependencies creates a flow step that depends on eight previous steps
// The handler receives the flow input and the outputs of all eight dependency steps
func StepWithEightDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Dep8Out, Out any](name string, dep1 *StepDependency, dep2 *StepDependency, dep3 *StepDependency, dep4 *StepDependency, dep5 *StepDependency, dep6 *StepDependency, dep7 *StepDependency, dep8 *StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Dep8Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
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
	}}
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
		opt.apply(&h.handlerOpts)
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

type QueueInfo struct {
	Name      string    `json:"name"`
	Unlogged  bool      `json:"unlogged"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
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

type WorkerInfo struct {
	ID              string             `json:"id"`
	TaskHandlers    []*TaskHandlerInfo `json:"task_handlers"`
	StepHandlers    []*StepHandlerInfo `json:"step_handlers"`
	StartedAt       time.Time          `json:"started_at"`
	LastHeartbeatAt time.Time          `json:"last_heartbeat_at"`
}

type TaskHandlerInfo struct {
	TaskName string `json:"task_name"`
}

type StepHandlerInfo struct {
	FlowName string `json:"flow_name"`
	StepName string `json:"step_name"`
}

type QueueOpts struct {
	ExpiresAt time.Time
	Unlogged  bool
}

// CreateQueue creates a new queue with the given name.
func CreateQueue(ctx context.Context, conn Conn, name string) error {
	return CreateQueueWithOpts(ctx, conn, name, QueueOpts{})
}

// CreateQueueWithOpts creates a queue with the specified options.
// Use Bind() separately to create topic bindings.
func CreateQueueWithOpts(ctx context.Context, conn Conn, name string, opts QueueOpts) error {
	q := `SELECT cb_create_queue(name => $1, expires_at => $2, unlogged => $3);`
	_, err := conn.Exec(ctx, q, name, ptrOrNil(opts.ExpiresAt), opts.Unlogged)
	return err
}

// GetQueue retrieves queue metadata by name.
func GetQueue(ctx context.Context, conn Conn, name string) (*QueueInfo, error) {
	q := `SELECT name, unlogged, created_at, expires_at FROM cb_queues WHERE name = $1;`
	var info QueueInfo
	var expiresAt *time.Time
	err := conn.QueryRow(ctx, q, name).Scan(&info.Name, &info.Unlogged, &info.CreatedAt, &expiresAt)
	if err != nil {
		return nil, err
	}
	if expiresAt != nil {
		info.ExpiresAt = *expiresAt
	}
	return &info, nil
}

// DeleteQueue deletes a queue and all its messages.
// Returns true if the queue existed.
func DeleteQueue(ctx context.Context, conn Conn, name string) (bool, error) {
	q := `SELECT * FROM cb_delete_queue(name => $1);`
	existed := false
	err := conn.QueryRow(ctx, q, name).Scan(&existed)
	return existed, err
}

// ListQueues returns all queues
func ListQueues(ctx context.Context, conn Conn) ([]*QueueInfo, error) {
	q := `SELECT name, unlogged, created_at, expires_at FROM cb_queues ORDER BY name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []*QueueInfo
	for rows.Next() {
		var info QueueInfo
		var expiresAt *time.Time
		if err := rows.Scan(&info.Name, &info.Unlogged, &info.CreatedAt, &expiresAt); err != nil {
			return nil, err
		}
		if expiresAt != nil {
			info.ExpiresAt = *expiresAt
		}
		queues = append(queues, &info)
	}

	return queues, rows.Err()
}

type SendOpts struct {
	Topic           string
	DeduplicationID string
	DeliverAt       time.Time
}

// Send enqueues a message to the specified queue.
// The payload is marshaled to JSON.
func Send(ctx context.Context, conn Conn, queue string, payload any) error {
	return SendWithOpts(ctx, conn, queue, payload, SendOpts{})
}

// SendWithOpts enqueues a message with options for topic, deduplication ID,
// and delivery time.
func SendWithOpts(ctx context.Context, conn Conn, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `SELECT cb_send(queue => $1, payload => $2, topic => $3, deduplication_id => $4, deliver_at => $5);`
	_, err = conn.Exec(ctx, q, queue, b, ptrOrNil(opts.Topic), ptrOrNil(opts.DeduplicationID), ptrOrNil(opts.DeliverAt))
	return err
}

// Bind subscribes a queue to a topic pattern.
// Pattern supports exact topics and wildcards: ? (single token), * (multi-token tail).
// Examples: "foo.bar", "foo.?.bar", "foo.bar.*"
func Bind(ctx context.Context, conn Conn, queue string, pattern string) error {
	q := `SELECT cb_bind(queue_name => $1, pattern => $2);`
	_, err := conn.Exec(ctx, q, queue, pattern)
	return err
}

// Unbind unsubscribes a queue from a topic pattern.
func Unbind(ctx context.Context, conn Conn, queue string, pattern string) error {
	q := `SELECT cb_unbind(queue_name => $1, pattern => $2);`
	_, err := conn.Exec(ctx, q, queue, pattern)
	return err
}

type DispatchOpts struct {
	DeduplicationID string
	DeliverAt       *time.Time
}

// Dispatch sends a message to all queues subscribed to the specified topic.
func Dispatch(ctx context.Context, conn Conn, topic string, payload any) error {
	return DispatchWithOpts(ctx, conn, topic, payload, DispatchOpts{})
}

// DispatchWithOpts sends a message to topic-subscribed queues with options
// for deduplication ID and delivery time.
func DispatchWithOpts(ctx context.Context, conn Conn, topic string, payload any, opts DispatchOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => $3, deliver_at => $4);`
	_, err = conn.Exec(ctx, q, topic, b, ptrOrNil(opts.DeduplicationID), ptrOrNil(opts.DeliverAt))
	return err
}

// Read reads up to quantity messages from the queue, hiding them from other
// readers for the specified duration.
func Read(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read(queue => $1, quantity => $2, hide_for => $3);`
	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Milliseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleMessage)
}

// ReadPoll reads messages from a queue with polling support.
// It polls repeatedly at the specified interval until messages are available
// or the pollFor timeout is reached.
func ReadPoll(ctx context.Context, conn Conn, queue string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read_poll(queue => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`

	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Milliseconds(), pollFor.Milliseconds(), pollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleMessage)
}

// Hide hides a single message from being read for the specified duration.
// Returns true if the message existed.
func Hide(ctx context.Context, conn Conn, queue string, id int64, hideFor time.Duration) (bool, error) {
	q := `SELECT * FROM cb_hide(queue => $1, id => $2, hide_for => $3);`
	exists := false
	err := conn.QueryRow(ctx, q, queue, id, hideFor.Milliseconds()).Scan(&exists)
	return exists, err
}

// HideMany hides multiple messages from being read for the specified duration.
func HideMany(ctx context.Context, conn Conn, queue string, ids []int64, hideFor time.Duration) error {
	q := `SELECT * FROM cb_hide(queue => $1, ids => $2, hide_for => $3);`
	_, err := conn.Exec(ctx, q, queue, ids, hideFor.Milliseconds())
	return err
}

// Delete deletes a single message from the queue.
// Returns true if the message existed.
func Delete(ctx context.Context, conn Conn, queue string, id int64) (bool, error) {
	q := `SELECT * FROM cb_delete(queue => $1, id => $2);`
	existed := false
	err := conn.QueryRow(ctx, q, queue, id).Scan(&existed)
	return existed, err
}

// DeleteMany deletes multiple messages from the queue.
func DeleteMany(ctx context.Context, conn Conn, queue string, ids []int64) error {
	q := `SELECT * FROM cb_delete(queue => $1, ids => $2);`
	_, err := conn.Exec(ctx, q, queue, ids)
	return err
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
	q := `SELECT name, created_at FROM cb_tasks;`
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
	q := `SELECT * FROM cb_flow_info;`
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

// ListWorkers returns all registered workers.
func ListWorkers(ctx context.Context, conn Conn) ([]*WorkerInfo, error) {
	q := `SELECT id, started_at, last_heartbeat_at, task_handlers, step_handlers FROM cb_worker_info;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleWorker)

}

// GC runs garbage collection to clean up expired and deleted entries.
func GC(ctx context.Context, conn Conn) error {
	q := `SELECT cb_gc();`
	_, err := conn.Exec(ctx, q)
	return err
}

// EnqueueSend adds a Send operation to a batch for efficient bulk message sending.
func EnqueueSend(batch *pgx.Batch, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	batch.Queue(
		`SELECT cb_send(queue => $1, payload => $2, topic => $3, deduplication_id => $4, deliver_at => $5);`,
		queue, b, ptrOrNil(opts.Topic), ptrOrNil(opts.DeduplicationID), ptrOrNil(opts.DeliverAt),
	)

	return nil
}

// EnqueueDispatch adds a Dispatch operation to a batch for efficient bulk message dispatching.
func EnqueueDispatch(batch *pgx.Batch, topic string, payload any, opts DispatchOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	batch.Queue(
		`SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => $3, deliver_at => $4);`,
		topic, b, ptrOrNil(opts.DeduplicationID), ptrOrNil(opts.DeliverAt),
	)

	return nil
}

func scanCollectibleMessage(row pgx.CollectableRow) (Message, error) {
	return scanMessage(row)
}

func scanMessage(row pgx.Row) (Message, error) {
	rec := Message{}

	var deduplicationID *string
	var topic *string

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&topic,
		&rec.Payload,
		&rec.Deliveries,
		&rec.CreatedAt,
		&rec.DeliverAt,
	); err != nil {
		return rec, err
	}

	if topic != nil {
		rec.Topic = *topic
	}
	if deduplicationID != nil {
		rec.DeduplicationID = *deduplicationID
	}

	return rec, nil
}

func scanCollectibleQueue(row pgx.CollectableRow) (*QueueInfo, error) {
	return scanQueue(row)
}

func scanQueue(row pgx.Row) (*QueueInfo, error) {
	rec := QueueInfo{}

	var expiresAt *time.Time

	if err := row.Scan(
		&rec.Name,
		&rec.Unlogged,
		&rec.CreatedAt,
		&expiresAt,
	); err != nil {
		return nil, err
	}

	if expiresAt != nil {
		rec.ExpiresAt = *expiresAt
	}

	return &rec, nil
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

func scanCollectibleWorker(row pgx.CollectableRow) (*WorkerInfo, error) {
	return scanWorker(row)
}

func scanWorker(row pgx.Row) (*WorkerInfo, error) {
	rec := WorkerInfo{}

	var taskHandlers json.RawMessage
	var stepHandlers json.RawMessage

	if err := row.Scan(
		&rec.ID,
		&rec.StartedAt,
		&rec.LastHeartbeatAt,
		&taskHandlers,
		&stepHandlers,
	); err != nil {
		return nil, err
	}

	if taskHandlers != nil {
		if err := json.Unmarshal(taskHandlers, &rec.TaskHandlers); err != nil {
			return nil, err
		}
	}
	if stepHandlers != nil {
		if err := json.Unmarshal(stepHandlers, &rec.StepHandlers); err != nil {
			return nil, err
		}
	}

	return &rec, nil
}

func ptrOrNil[T comparable](t T) *T {
	var tt T
	if t == tt {
		return nil
	}
	return &t
}
