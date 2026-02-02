package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	StatusCreated   = "created"
	StatusStarted   = "started"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

var (
	ErrTaskFailed = fmt.Errorf("task failed")
	ErrFlowFailed = fmt.Errorf("flow failed")
)

type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

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
	concurrency  int
	batchSize    int
	timeout      time.Duration
	retries      int
	retryDelay   time.Duration
	jitterFactor float64
}

type HandlerOpt interface {
	apply(*handlerOpts)
}

type concurrencyOpt struct {
	concurrency int
}

func (o concurrencyOpt) apply(h *handlerOpts) {
	h.concurrency = o.concurrency
}

func WithConcurrency(n int) HandlerOpt {
	return concurrencyOpt{concurrency: n}
}

type timeoutOpt struct {
	timeout time.Duration
}

func (o timeoutOpt) apply(h *handlerOpts) {
	h.timeout = o.timeout
}

func WithTimeout(d time.Duration) HandlerOpt {
	return timeoutOpt{timeout: d}
}

type batchSizeOpt struct {
	batchSize int
}

func (o batchSizeOpt) apply(h *handlerOpts) {
	h.batchSize = o.batchSize
}

func WithBatchSize(n int) HandlerOpt {
	return batchSizeOpt{batchSize: n}
}

type retriesOpt struct {
	retries int
}

func (o retriesOpt) apply(h *handlerOpts) {
	h.retries = o.retries
}

func WithRetries(n int) HandlerOpt {
	return retriesOpt{retries: n}
}

type retryDelayOpt struct {
	delay time.Duration
}

func (o retryDelayOpt) apply(h *handlerOpts) {
	h.retryDelay = o.delay
}

func WithRetryDelay(d time.Duration) HandlerOpt {
	return retryDelayOpt{delay: d}
}

type Task struct {
	Name    string `json:"name"`
	handler *taskHandler
}

type taskHandler struct {
	handlerOpts
	fn func(context.Context, taskPayload) ([]byte, error)
}

type taskPayload struct {
	ID    string          `json:"id"`
	Input json.RawMessage `json:"input"`
}

func NewTask[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...HandlerOpt) *Task {
	h := &taskHandler{
		handlerOpts: handlerOpts{
			concurrency:  1,
			batchSize:    10,
			jitterFactor: 0.1,
		},
		fn: func(ctx context.Context, p taskPayload) ([]byte, error) {
			var in In
			if err := json.Unmarshal(p.Input, &in); err != nil {
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
	Name      string   `json:"name"`
	DependsOn []string `json:"depends_on,omitempty"`
	handler   *stepHandler
}

type StepDependency struct {
	name string
}

func Dependency(name string) StepDependency {
	return StepDependency{name: name}
}

type stepPayload struct {
	ID          string                     `json:"id"`
	FlowInput   json.RawMessage            `json:"flow_input"`
	StepOutputs map[string]json.RawMessage `json:"step_outputs"`
}

type stepHandler struct {
	handlerOpts
	fn func(context.Context, stepPayload) ([]byte, error)
}

type StepOpt func(*Step)

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

func InitialStep[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: nil,
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
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

func StepWithOneDependency[In, Dep1Out, Out any](name string, dep1 StepDependency, fn func(context.Context, In, Dep1Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: []string{dep1.name},
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			if err := unmarshalStepArgs(p, []string{dep1.name}, &in, []any{&dep1Out}); err != nil {
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

func StepWithTwoDependencies[In, Dep1Out, Dep2Out, Out any](name string, dep1 StepDependency, dep2 StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: []string{dep1.name, dep2.name},
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			if err := unmarshalStepArgs(p, []string{dep1.name, dep2.name}, &in, []any{&dep1Out, &dep2Out}); err != nil {
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

func StepWithThreeDependencies[In, Dep1Out, Dep2Out, Dep3Out, Out any](name string, dep1 StepDependency, dep2 StepDependency, dep3 StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: []string{dep1.name, dep2.name, dep3.name},
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			if err := unmarshalStepArgs(p, []string{dep1.name, dep2.name, dep3.name}, &in, []any{&dep1Out, &dep2Out, &dep3Out}); err != nil {
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

func StepWithFourDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Out any](name string, dep1 StepDependency, dep2 StepDependency, dep3 StepDependency, dep4 StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: []string{dep1.name, dep2.name, dep3.name, dep4.name},
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			if err := unmarshalStepArgs(p, []string{dep1.name, dep2.name, dep3.name, dep4.name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out}); err != nil {
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

func StepWithFiveDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Out any](name string, dep1 StepDependency, dep2 StepDependency, dep3 StepDependency, dep4 StepDependency, dep5 StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: []string{dep1.name, dep2.name, dep3.name, dep4.name, dep5.name},
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			var dep5Out Dep5Out
			if err := unmarshalStepArgs(p, []string{dep1.name, dep2.name, dep3.name, dep4.name, dep5.name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out, &dep5Out}); err != nil {
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

func StepWithSixDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Out any](name string, dep1 StepDependency, dep2 StepDependency, dep3 StepDependency, dep4 StepDependency, dep5 StepDependency, dep6 StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: []string{dep1.name, dep2.name, dep3.name, dep4.name, dep5.name, dep6.name},
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			var dep5Out Dep5Out
			var dep6Out Dep6Out
			if err := unmarshalStepArgs(p, []string{dep1.name, dep2.name, dep3.name, dep4.name, dep5.name, dep6.name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out, &dep5Out, &dep6Out}); err != nil {
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

func StepWithSevenDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Out any](name string, dep1 StepDependency, dep2 StepDependency, dep3 StepDependency, dep4 StepDependency, dep5 StepDependency, dep6 StepDependency, dep7 StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: []string{dep1.name, dep2.name, dep3.name, dep4.name, dep5.name, dep6.name, dep7.name},
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			var dep5Out Dep5Out
			var dep6Out Dep6Out
			var dep7Out Dep7Out
			if err := unmarshalStepArgs(p, []string{dep1.name, dep2.name, dep3.name, dep4.name, dep5.name, dep6.name, dep7.name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out, &dep5Out, &dep6Out, &dep7Out}); err != nil {
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

func StepWithEightDependencies[In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Dep8Out, Out any](name string, dep1 StepDependency, dep2 StepDependency, dep3 StepDependency, dep4 StepDependency, dep5 StepDependency, dep6 StepDependency, dep7 StepDependency, dep8 StepDependency, fn func(context.Context, In, Dep1Out, Dep2Out, Dep3Out, Dep4Out, Dep5Out, Dep6Out, Dep7Out, Dep8Out) (Out, error), opts ...HandlerOpt) FlowOpt {
	return stepOpt{step: &Step{
		Name:      name,
		DependsOn: []string{dep1.name, dep2.name, dep3.name, dep4.name, dep5.name, dep6.name, dep7.name, dep8.name},
		handler: newStepHandler(func(ctx context.Context, p stepPayload) ([]byte, error) {
			var in In
			var dep1Out Dep1Out
			var dep2Out Dep2Out
			var dep3Out Dep3Out
			var dep4Out Dep4Out
			var dep5Out Dep5Out
			var dep6Out Dep6Out
			var dep7Out Dep7Out
			var dep8Out Dep8Out
			if err := unmarshalStepArgs(p, []string{dep1.name, dep2.name, dep3.name, dep4.name, dep5.name, dep6.name, dep7.name, dep8.name}, &in, []any{&dep1Out, &dep2Out, &dep3Out, &dep4Out, &dep5Out, &dep6Out, &dep7Out, &dep8Out}); err != nil {
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

func newStepHandler(fn func(context.Context, stepPayload) ([]byte, error), opts ...HandlerOpt) *stepHandler {
	h := &stepHandler{
		handlerOpts: handlerOpts{
			concurrency:  1,
			batchSize:    10,
			jitterFactor: 0.1,
		},
		fn: fn,
	}

	for _, opt := range opts {
		opt.apply(&h.handlerOpts)
	}

	return h
}

func unmarshalStepArgs(p stepPayload, stepNames []string, in any, stepOutputs []any) error {
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
	Topics    []string  `json:"topics,omitempty"`
	Unlogged  bool      `json:"unlogged"`
	CreatedAt time.Time `json:"created_at"`
	DeleteAt  time.Time `json:"delete_at,omitzero"`
}

type TaskInfo struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type TaskRunInfo struct {
	ID              string          `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Status          string          `json:"status"`
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
	Name      string   `json:"name"`
	DependsOn []string `json:"depends_on,omitempty"`
}

type FlowRunInfo struct {
	ID              string          `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Status          string          `json:"status"`
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
	Topics   []string
	DeleteAt time.Time
	Unlogged bool
}

func CreateQueue(ctx context.Context, conn Conn, name string) error {
	return CreateQueueWithOpts(ctx, conn, name, QueueOpts{})
}

func CreateQueueWithOpts(ctx context.Context, conn Conn, name string, opts QueueOpts) error {
	q := `SELECT cb_create_queue(name => $1, topics => $2, delete_at => $3, unlogged => $4);`
	_, err := conn.Exec(ctx, q, name, opts.Topics, ptrOrNil(opts.DeleteAt), opts.Unlogged)
	return err
}

func GetQueue(ctx context.Context, conn Conn, name string) (*QueueInfo, error) {
	q := `SELECT name, topics, unlogged, created_at, delete_at FROM cb_queues WHERE name = $1;`
	return scanQueue(conn.QueryRow(ctx, q, name))
}

func DeleteQueue(ctx context.Context, conn Conn, name string) (bool, error) {
	q := `SELECT * FROM cb_delete_queue(name => $1);`
	existed := false
	err := conn.QueryRow(ctx, q, name).Scan(&existed)
	return existed, err
}

func ListQueues(ctx context.Context, conn Conn) ([]*QueueInfo, error) {
	q := `SELECT name, topics, unlogged, created_at, delete_at FROM cb_queues;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleQueue)
}

type SendOpts struct {
	Topic           string
	DeduplicationID string
	DeliverAt       time.Time
}

func Send(ctx context.Context, conn Conn, queue string, payload any) error {
	return SendWithOpts(ctx, conn, queue, payload, SendOpts{})
}

func SendWithOpts(ctx context.Context, conn Conn, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `SELECT cb_send(queue => $1, payload => $2, topic => $3, deduplication_id => $4, deliver_at => $5);`
	_, err = conn.Exec(ctx, q, queue, b, ptrOrNil(opts.Topic), ptrOrNil(opts.DeduplicationID), ptrOrNil(opts.DeliverAt))
	return err
}

type DispatchOpts struct {
	DeduplicationID string
	DeliverAt       *time.Time
}

func Dispatch(ctx context.Context, conn Conn, topic string, payload any) error {
	return DispatchWithOpts(ctx, conn, topic, payload, DispatchOpts{})
}

func DispatchWithOpts(ctx context.Context, conn Conn, topic string, payload any, opts DispatchOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => $3, deliver_at => $4);`
	_, err = conn.Exec(ctx, q, topic, b, ptrOrNil(opts.DeduplicationID), ptrOrNil(opts.DeliverAt))
	return err
}

func Read(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read(queue => $1, quantity => $2, hide_for => $3);`
	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleMessage)
}

func ReadPoll(ctx context.Context, conn Conn, queue string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read_poll(queue => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`

	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Seconds(), pollFor.Seconds(), pollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleMessage)
}

func Hide(ctx context.Context, conn Conn, queue string, id int64, hideFor time.Duration) (bool, error) {
	q := `SELECT * FROM cb_hide(queue => $1, id => $2, hide_for => $3);`
	exists := false
	err := conn.QueryRow(ctx, q, queue, id, hideFor.Seconds()).Scan(&exists)
	return exists, err
}

func HideMany(ctx context.Context, conn Conn, queue string, ids []int64, hideFor time.Duration) error {
	q := `SELECT * FROM cb_hide_many(queue => $1, ids => $2, hide_for => $3);`
	_, err := conn.Exec(ctx, q, queue, ids, hideFor.Seconds())
	return err
}

func Delete(ctx context.Context, conn Conn, queue string, id int64) (bool, error) {
	q := `SELECT * FROM cb_delete(queue => $1, id => $2);`
	existed := false
	err := conn.QueryRow(ctx, q, queue, id).Scan(&existed)
	return existed, err
}

func DeleteMany(ctx context.Context, conn Conn, queue string, ids []int64) error {
	q := `SELECT * FROM cb_delete(queue => $1, ids => $2);`
	_, err := conn.Exec(ctx, q, queue, ids)
	return err
}

func CreateTask(ctx context.Context, conn Conn, task *Task) error {
	q := `SELECT * FROM cb_create_task(name => $1);`
	_, err := conn.Exec(ctx, q, task.Name)
	if err != nil {
		return err
	}
	return nil
}

func GetTask(ctx context.Context, conn Conn, name string) (*TaskInfo, error) {
	q := `SELECT name, created_at FROM cb_tasks WHERE name = $1;`
	return scanTask(conn.QueryRow(ctx, q, name))
}

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

func RunTask(ctx context.Context, conn Conn, name string, input any) (*TaskHandle, error) {
	return RunTaskWithOpts(ctx, conn, name, input, RunTaskOpts{})
}

func RunTaskWithOpts(ctx context.Context, conn Conn, name string, input any, opts RunTaskOpts) (*TaskHandle, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	q := `SELECT * FROM cb_run_task(name => $1, input => $2, deduplication_id => $3);`
	var id string
	err = conn.QueryRow(ctx, q, name, b, ptrOrNil(opts.DeduplicationID)).Scan(&id)
	if err != nil {
		return nil, err
	}

	return &TaskHandle{conn: conn, id: id}, nil
}

type TaskHandle struct {
	conn Conn
	id   string
}

func (h *TaskHandle) ID() string {
	return h.id
}

func (h *TaskHandle) WaitForOutput(ctx context.Context, out any) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info, err := GetTaskRun(ctx, h.conn, h.id)
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

func GetTaskRun(ctx context.Context, conn Conn, id string) (*TaskRunInfo, error) {
	q := `
		SELECT id, deduplication_id, status, output, error_message, started_at, completed_at, failed_at
		FROM cb_task_runs
		WHERE id = $1;`
	return scanTaskRun(conn.QueryRow(ctx, q, id))
}

func ListTaskRuns(ctx context.Context, conn Conn, name string) ([]*TaskRunInfo, error) {
	q := `
		SELECT id, deduplication_id, status, output, error_message, started_at, completed_at, failed_at
		FROM cb_task_runs
		WHERE task_name = $1
		ORDER BY started_at DESC
		LIMIT 20;`
	rows, err := conn.Query(ctx, q, name)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleTaskRun)
}

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

func GetFlow(ctx context.Context, conn Conn, name string) (*FlowInfo, error) {
	q := `
		SELECT f.name, s.steps AS steps, f.created_at
		FROM cb_flows f
		LEFT JOIN LATERAL (
			SELECT s.flow_name,
				jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
					'name', s.name,
					'depends_on', (
						SELECT jsonb_agg(s_d.dependency_name)
						FROM cb_step_dependencies AS s_d
						WHERE s_d.flow_name = s.flow_name
						AND s_d.step_name = s.name
					)
				)) ORDER BY s.idx) FILTER (WHERE s.idx IS NOT NULL) AS steps
			FROM cb_steps s
			WHERE s.flow_name = f.name
			GROUP BY flow_name
		) s ON s.flow_name = f.name
		WHERE f.name = $1;`
	return scanFlow(conn.QueryRow(ctx, q, name))
}

func ListFlows(ctx context.Context, conn Conn) ([]*FlowInfo, error) {
	q := `
		SELECT f.name, s.steps AS steps, f.created_at
		FROM cb_flows f
		LEFT JOIN LATERAL (
			SELECT s.flow_name,
				jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
					'name', s.name,
					'depends_on', (
						SELECT jsonb_agg(s_d.dependency_name)
						FROM cb_step_dependencies AS s_d
						WHERE s_d.flow_name = s.flow_name
						AND s_d.step_name = s.name
					)
				)) ORDER BY s.idx) FILTER (WHERE s.idx IS NOT NULL) AS steps
			FROM cb_steps s
			WHERE s.flow_name = f.name
			GROUP BY flow_name
		) s ON s.flow_name = f.name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleFlow)
}

type RunFlowOpts struct {
	DeduplicationID string
}

func RunFlow(ctx context.Context, conn Conn, name string, input any) (*FlowHandle, error) {
	return RunFlowWithOpts(ctx, conn, name, input, RunFlowOpts{})
}

func RunFlowWithOpts(ctx context.Context, conn Conn, name string, input any, opts RunFlowOpts) (*FlowHandle, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	q := `SELECT * FROM cb_run_flow(name => $1, input => $2, deduplication_id => $3);`
	var id string
	err = conn.QueryRow(ctx, q, name, b, ptrOrNil(opts.DeduplicationID)).Scan(&id)
	if err != nil {
		return nil, err
	}
	return &FlowHandle{id: id, conn: conn}, nil
}

type FlowHandle struct {
	id   string
	conn Conn
}

func (h *FlowHandle) ID() string {
	return h.id
}

func (h *FlowHandle) WaitForOutput(ctx context.Context, out any) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info, err := GetFlowRun(ctx, h.conn, h.id)
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

func GetFlowRun(ctx context.Context, conn Conn, id string) (*FlowRunInfo, error) {
	q := `
		SELECT id, deduplication_id, status, output, error_message, started_at, completed_at, failed_at
		FROM cb_flow_runs
		WHERE id = $1;`
	return scanFlowRun(conn.QueryRow(ctx, q, id))
}

func ListFlowRuns(ctx context.Context, conn Conn, name string) ([]*FlowRunInfo, error) {
	q := `
		SELECT id, deduplication_id, status, output, error_message, started_at, completed_at, failed_at
		FROM cb_flow_runs
		WHERE flow_name = $1
		ORDER BY started_at DESC
		LIMIT 20;`
	rows, err := conn.Query(ctx, q, name)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleFlowRun)
}

func ListWorkers(ctx context.Context, conn Conn) ([]*WorkerInfo, error) {
	q := `
		SELECT w.id, w.started_at, w.last_heartbeat_at, t.task_handlers AS task_handlers, s.step_handlers AS step_handlers
		FROM cb_workers w
		LEFT JOIN LATERAL (
			SELECT t.worker_id,
   			       json_agg(json_build_object('task_name', t.task_name) ORDER BY t.task_name) FILTER (WHERE t.worker_id IS NOT NULL) AS task_handlers
			FROM cb_task_handlers t
			WHERE t.worker_id = w.id
			GROUP BY worker_id
		) t ON t.worker_id = w.id
		LEFT JOIN LATERAL (
			SELECT s.worker_id,
   			       json_agg(json_build_object('flow_name', s.flow_name, 'step_name', s.step_name) ORDER BY s.flow_name, s.step_name) FILTER (WHERE s.worker_id IS NOT NULL) AS step_handlers
			FROM cb_step_handlers s
			WHERE s.worker_id = w.id
			GROUP BY worker_id
		) s ON s.worker_id = w.id
		ORDER BY w.started_at DESC;`

	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleWorker)

}

func GC(ctx context.Context, conn Conn) error {
	q := `SELECT cb_gc();`
	_, err := conn.Exec(ctx, q)
	return err
}

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

	var deleteAt *time.Time

	if err := row.Scan(
		&rec.Name,
		&rec.Topics,
		&rec.Unlogged,
		&rec.CreatedAt,
		&deleteAt,
	); err != nil {
		return nil, err
	}

	if deleteAt != nil {
		rec.DeleteAt = *deleteAt
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
	var output *json.RawMessage
	var errorMessage *string
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&rec.Status,
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
	var output *json.RawMessage
	var errorMessage *string
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&rec.Status,
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
