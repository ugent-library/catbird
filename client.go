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

// CreateQueue creates one or more queue definitions.
func (c *Client) CreateQueue(ctx context.Context, queues ...*Queue) error {
	return CreateQueue(ctx, c.Conn, queues...)
}

// GetQueue retrieves queue metadata by name.
func (c *Client) GetQueue(ctx context.Context, name string) (*QueueInfo, error) {
	return GetQueue(ctx, c.Conn, name)
}

// ListQueues returns all queues
func (c *Client) ListQueues(ctx context.Context) ([]*QueueInfo, error) {
	return ListQueues(ctx, c.Conn)
}

// DeleteQueue deletes a queue and all its messages.
// Returns true if the queue existed.
func (c *Client) DeleteQueue(ctx context.Context, name string) (bool, error) {
	return DeleteQueue(ctx, c.Conn, name)
}

// Send enqueues a message to the specified queue.
func (c *Client) Send(ctx context.Context, queue string, payload any, opts *SendOpts) error {
	return Send(ctx, c.Conn, queue, payload, opts)
}

// Bind subscribes a queue to a topic pattern.
// Pattern supports exact topics and wildcards: ? (single token), * (multi-token tail).
// Examples: "foo.bar", "foo.?.bar", "foo.bar.*"
func (c *Client) Bind(ctx context.Context, queue string, pattern string) error {
	return Bind(ctx, c.Conn, queue, pattern)
}

// Unbind unsubscribes a queue from a topic pattern.
func (c *Client) Unbind(ctx context.Context, queue string, pattern string) error {
	return Unbind(ctx, c.Conn, queue, pattern)
}

// Publish sends a message to all queues subscribed to the specified topic.
func (c *Client) Publish(ctx context.Context, topic string, payload any, opts *PublishOpts) error {
	return Publish(ctx, c.Conn, topic, payload, opts)
}

// Read reads up to quantity messages from the queue, hiding them from other
// readers for the specified duration.
func (c *Client) Read(ctx context.Context, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	return Read(ctx, c.Conn, queue, quantity, hideFor)
}

// ReadPoll reads messages from a queue with polling support.
// It polls repeatedly at the specified interval until messages are available
// or the pollFor timeout is reached.
func (c *Client) ReadPoll(ctx context.Context, queue string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]Message, error) {
	return ReadPoll(ctx, c.Conn, queue, quantity, hideFor, pollFor, pollInterval)
}

// Hide hides a single message from being read for the specified duration.
// Returns true if the message existed.
func (c *Client) Hide(ctx context.Context, queue string, id int64, hideFor time.Duration) (bool, error) {
	return Hide(ctx, c.Conn, queue, id, hideFor)
}

// HideMany hides multiple messages from being read for the specified duration.
func (c *Client) HideMany(ctx context.Context, queue string, ids []int64, hideFor time.Duration) error {
	return HideMany(ctx, c.Conn, queue, ids, hideFor)
}

// Delete deletes a single message from the queue.
// Returns true if the message existed.
func (c *Client) Delete(ctx context.Context, queue string, id int64) (bool, error) {
	return Delete(ctx, c.Conn, queue, id)
}

// DeleteMany deletes multiple messages from the queue.
func (c *Client) DeleteMany(ctx context.Context, queue string, ids []int64) error {
	return DeleteMany(ctx, c.Conn, queue, ids)
}

// CreateTask creates one or more task definitions.
func (c *Client) CreateTask(ctx context.Context, tasks ...*Task) error {
	return CreateTask(ctx, c.Conn, tasks...)
}

// GetTask retrieves task metadata by name.
func (c *Client) GetTask(ctx context.Context, name string) (*TaskInfo, error) {
	return GetTask(ctx, c.Conn, name)
}

// ListTasks returns all tasks
func (c *Client) ListTasks(ctx context.Context) ([]*TaskInfo, error) {
	return ListTasks(ctx, c.Conn)
}

// RunTask enqueues a task execution and returns a handle for monitoring
// progress and retrieving output.
func (c *Client) RunTask(ctx context.Context, name string, input any, opts *RunOpts) (*RunHandle, error) {
	return RunTask(ctx, c.Conn, name, input, opts)
}

// GetTaskRun retrieves a specific task run result by ID.
func (c *Client) GetTaskRun(ctx context.Context, name string, id int64) (*RunInfo, error) {
	return GetTaskRun(ctx, c.Conn, name, id)
}

// ListTaskRuns returns recent task runs for the specified task.
func (c *Client) ListTaskRuns(ctx context.Context, name string) ([]*RunInfo, error) {
	return ListTaskRuns(ctx, c.Conn, name)
}

// CreateFlow creates one or more flow definitions.
func (c *Client) CreateFlow(ctx context.Context, flows ...*Flow) error {
	return CreateFlow(ctx, c.Conn, flows...)
}

// GetFlow retrieves flow metadata by name.
func (c *Client) GetFlow(ctx context.Context, name string) (*FlowInfo, error) {
	return GetFlow(ctx, c.Conn, name)
}

// ListFlows returns all flows
func (c *Client) ListFlows(ctx context.Context) ([]*FlowInfo, error) {
	return ListFlows(ctx, c.Conn)
}

// RunFlow enqueues a flow execution and returns a handle for monitoring.
func (c *Client) RunFlow(ctx context.Context, name string, input any, opts *RunFlowOpts) (*RunHandle, error) {
	return RunFlow(ctx, c.Conn, name, input, opts)
}

// GetFlowRun retrieves a specific flow run result by ID.
func (c *Client) GetFlowRun(ctx context.Context, name string, id int64) (*RunInfo, error) {
	return GetFlowRun(ctx, c.Conn, name, id)
}

// ListFlowRuns returns recent flow runs for the specified flow.
func (c *Client) ListFlowRuns(ctx context.Context, name string) ([]*RunInfo, error) {
	return ListFlowRuns(ctx, c.Conn, name)
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
func (c *Client) NewWorker(ctx context.Context, opts *WorkerOpts) *Worker {
	return NewWorker(c.Conn, opts)
}

// ListWorkers returns all registered workers.
func (c *Client) ListWorkers(ctx context.Context) ([]*WorkerInfo, error) {
	return ListWorkers(ctx, c.Conn)
}

// GC runs garbage collection to clean up expired queues and stale workers.
// Note: Worker heartbeats automatically perform cleanup, so this is mainly
// useful for deployments without workers or for manual control.
func (c *Client) GC(ctx context.Context) error {
	return GC(ctx, c.Conn)
}
