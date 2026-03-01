package catbird

import (
	"context"
	"time"
)

// Client is a facade for interacting with Catbird
type Client struct {
	Conn Conn
}

// New creates a new Client with the given database connection.
//
// The connection can be a *pgxpool.Pool, *pgx.Conn, or pgx.Tx.
func New(conn Conn) *Client {
	return &Client{Conn: conn}
}

// CreateQueue creates a queue with the given name and optional options.
func (c *Client) CreateQueue(ctx context.Context, queueName string, opts ...QueueOpts) error {
	return CreateQueue(ctx, c.Conn, queueName, opts...)
}

// GetQueue retrieves queue metadata by name.
func (c *Client) GetQueue(ctx context.Context, queueName string) (*QueueInfo, error) {
	return GetQueue(ctx, c.Conn, queueName)
}

// ListQueues returns all queues
func (c *Client) ListQueues(ctx context.Context) ([]*QueueInfo, error) {
	return ListQueues(ctx, c.Conn)
}

// DeleteQueue deletes a queue and all its messages.
// Returns true if the queue existed.
func (c *Client) DeleteQueue(ctx context.Context, queueName string) (bool, error) {
	return DeleteQueue(ctx, c.Conn, queueName)
}

// Send enqueues a message to the specified queue.
func (c *Client) Send(ctx context.Context, queueName string, payload any, opts ...SendOpts) error {
	return Send(ctx, c.Conn, queueName, payload, opts...)
}

// SendMany enqueues multiple messages to the specified queue.
func (c *Client) SendMany(ctx context.Context, queueName string, payloads []any, opts ...SendManyOpts) error {
	return SendMany(ctx, c.Conn, queueName, payloads, opts...)
}

// Bind subscribes a queue to a topic pattern.
// Pattern supports exact topics and wildcards: * (single token), # (multi-token tail).
// Examples: "foo.bar", "foo.*.bar", "foo.bar.#"
func (c *Client) Bind(ctx context.Context, queueName string, pattern string) error {
	return Bind(ctx, c.Conn, queueName, pattern)
}

// Unbind unsubscribes a queue from a topic pattern.
func (c *Client) Unbind(ctx context.Context, queueName string, pattern string) error {
	return Unbind(ctx, c.Conn, queueName, pattern)
}

// Publish sends a message to all queues subscribed to the specified topic.
func (c *Client) Publish(ctx context.Context, topic string, payload any, opts ...PublishOpts) error {
	return Publish(ctx, c.Conn, topic, payload, opts...)
}

// PublishMany sends multiple messages to all queues subscribed to the specified topic.
func (c *Client) PublishMany(ctx context.Context, topic string, payloads []any, opts ...PublishManyOpts) error {
	return PublishMany(ctx, c.Conn, topic, payloads, opts...)
}

// Read reads up to quantity messages from the queue, hiding them from other
// readers for the specified duration.
func (c *Client) Read(ctx context.Context, queueName string, quantity int, hideFor time.Duration) ([]Message, error) {
	return Read(ctx, c.Conn, queueName, quantity, hideFor)
}

// ReadPoll reads messages from a queue with polling support.
// It polls repeatedly at the specified interval until messages are available
// or the pollFor timeout is reached.
func (c *Client) ReadPoll(ctx context.Context, queueName string, quantity int, hideFor time.Duration, opts ...ReadPollOpts) ([]Message, error) {
	return ReadPoll(ctx, c.Conn, queueName, quantity, hideFor, opts...)
}

// Hide hides a single message from being read for the specified duration.
// Returns true if the message existed.
func (c *Client) Hide(ctx context.Context, queueName string, id int64, hideFor time.Duration) (bool, error) {
	return Hide(ctx, c.Conn, queueName, id, hideFor)
}

// HideMany hides multiple messages from being read for the specified duration.
func (c *Client) HideMany(ctx context.Context, queueName string, ids []int64, hideFor time.Duration) error {
	return HideMany(ctx, c.Conn, queueName, ids, hideFor)
}

// Delete deletes a single message from the queue.
// Returns true if the message existed.
func (c *Client) Delete(ctx context.Context, queueName string, id int64) (bool, error) {
	return Delete(ctx, c.Conn, queueName, id)
}

// DeleteMany deletes multiple messages from the queue.
func (c *Client) DeleteMany(ctx context.Context, queueName string, ids []int64) error {
	return DeleteMany(ctx, c.Conn, queueName, ids)
}

// CreateTask creates one or more task definitions.
func (c *Client) CreateTask(ctx context.Context, tasks ...*Task) error {
	return CreateTask(ctx, c.Conn, tasks...)
}

// GetTask retrieves task metadata by name.
func (c *Client) GetTask(ctx context.Context, taskName string) (*TaskInfo, error) {
	return GetTask(ctx, c.Conn, taskName)
}

// ListTasks returns all tasks
func (c *Client) ListTasks(ctx context.Context) ([]*TaskInfo, error) {
	return ListTasks(ctx, c.Conn)
}

// RunTask enqueues a task execution and returns a handle for monitoring
// progress and retrieving output.
func (c *Client) RunTask(ctx context.Context, taskName string, input any, opts ...RunTaskOpts) (*TaskHandle, error) {
	return RunTask(ctx, c.Conn, taskName, input, opts...)
}

// CancelTaskRun requests cancellation for a task run.
func (c *Client) CancelTaskRun(ctx context.Context, taskName string, runID int64, opts ...CancelOpts) error {
	return CancelTaskRun(ctx, c.Conn, taskName, runID, opts...)
}

// GetTaskRun retrieves a specific task run result by ID.
func (c *Client) GetTaskRun(ctx context.Context, taskName string, taskRunID int64) (*TaskRunInfo, error) {
	return GetTaskRun(ctx, c.Conn, taskName, taskRunID)
}

// ListTaskRuns returns recent task runs for the specified task.
func (c *Client) ListTaskRuns(ctx context.Context, taskName string) ([]*TaskRunInfo, error) {
	return ListTaskRuns(ctx, c.Conn, taskName)
}

// CreateFlow creates one or more flow definitions.
func (c *Client) CreateFlow(ctx context.Context, flows ...*Flow) error {
	return CreateFlow(ctx, c.Conn, flows...)
}

// GetFlow retrieves flow metadata by name.
func (c *Client) GetFlow(ctx context.Context, flowName string) (*FlowInfo, error) {
	return GetFlow(ctx, c.Conn, flowName)
}

// ListFlows returns all flows
func (c *Client) ListFlows(ctx context.Context) ([]*FlowInfo, error) {
	return ListFlows(ctx, c.Conn)
}

// RunFlow enqueues a flow execution and returns a handle for monitoring.
func (c *Client) RunFlow(ctx context.Context, flowName string, input any, opts ...RunFlowOpts) (*FlowHandle, error) {
	return RunFlow(ctx, c.Conn, flowName, input, opts...)
}

// CancelFlowRun requests cancellation for a flow run.
func (c *Client) CancelFlowRun(ctx context.Context, flowName string, runID int64, opts ...CancelOpts) error {
	return CancelFlowRun(ctx, c.Conn, flowName, runID, opts...)
}

// GetFlowRun retrieves a specific flow run result by ID.
func (c *Client) GetFlowRun(ctx context.Context, flowName string, flowRunID int64) (*FlowRunInfo, error) {
	return GetFlowRun(ctx, c.Conn, flowName, flowRunID)
}

// ListFlowRuns returns recent flow runs for the specified flow.
func (c *Client) ListFlowRuns(ctx context.Context, flowName string) ([]*FlowRunInfo, error) {
	return ListFlowRuns(ctx, c.Conn, flowName)
}

// GetFlowRunSteps retrieves all step runs for a specific flow run.
func (c *Client) GetFlowRunSteps(ctx context.Context, flowName string, flowRunID int64) ([]*StepRunInfo, error) {
	return GetFlowRunSteps(ctx, c.Conn, flowName, flowRunID)
}

// SignalFlow delivers a signal to a waiting step in a flow run.
// The step must have been defined with a signal variant (e.g., NewStepWithSignal).
// Returns an error if the signal was already delivered or the step doesn't require a signal.
func (c *Client) SignalFlow(ctx context.Context, flowName string, flowRunID int64, stepName string, input any) error {
	return SignalFlow(ctx, c.Conn, flowName, flowRunID, stepName, input)
}

// NewWorker creates a new worker that processes task and flow executions.
// Use the builder pattern methods (AddTask, AddFlow, etc.) to configure,
// then call Start(ctx) to begin processing.
func (c *Client) NewWorker(ctx context.Context) *Worker {
	return NewWorker(c.Conn)
}

// ListWorkers returns all registered workers.
func (c *Client) ListWorkers(ctx context.Context) ([]*WorkerInfo, error) {
	return ListWorkers(ctx, c.Conn)
}

// CreateTaskSchedule creates a cron-based schedule for a task.
// cronSpec should be in 5-field format (min hour day month dow) or descriptors like @hourly, @daily.
// opts are optional ScheduleOpt values configuring the schedule.
func (c *Client) CreateTaskSchedule(ctx context.Context, taskName, cronSpec string, opts ...ScheduleOpt) error {
	return CreateTaskSchedule(ctx, c.Conn, taskName, cronSpec, opts...)
}

// CreateFlowSchedule creates a cron-based schedule for a flow.
// cronSpec should be in 5-field format (min hour day month dow) or descriptors like @hourly, @daily.
// opts are optional ScheduleOpt values configuring the schedule.
func (c *Client) CreateFlowSchedule(ctx context.Context, flowName, cronSpec string, opts ...ScheduleOpt) error {
	return CreateFlowSchedule(ctx, c.Conn, flowName, cronSpec, opts...)
}

// ListTaskSchedules returns all task schedules ordered by next_run_at.
func (c *Client) ListTaskSchedules(ctx context.Context) ([]*TaskScheduleInfo, error) {
	return ListTaskSchedules(ctx, c.Conn)
}

// ListFlowSchedules returns all flow schedules ordered by next_run_at.
func (c *Client) ListFlowSchedules(ctx context.Context) ([]*FlowScheduleInfo, error) {
	return ListFlowSchedules(ctx, c.Conn)
}
