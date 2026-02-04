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

// CreateQueue creates a new queue with the given name.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: unique queue identifier
func (c *Client) CreateQueue(ctx context.Context, name string) error {
	return CreateQueue(ctx, c.Conn, name)
}

// CreateQueueWithOpts creates a queue with options.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: unique queue identifier
// - opts: QueueOpts with Topics, DeleteAt, and Unlogged settings
func (c *Client) CreateQueueWithOpts(ctx context.Context, name string, opts QueueOpts) error {
	return CreateQueueWithOpts(ctx, c.Conn, name, opts)
}

// GetQueue retrieves queue metadata by name.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: unique queue identifier
func (c *Client) GetQueue(ctx context.Context, name string) (*QueueInfo, error) {
	return GetQueue(ctx, c.Conn, name)
}

// DeleteQueue deletes a queue and all its messages.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: unique queue identifier
//
// Returns: true if queue existed, false if not found
func (c *Client) DeleteQueue(ctx context.Context, name string) (bool, error) {
	return DeleteQueue(ctx, c.Conn, name)
}

// ListQueues returns all queues
func (c *Client) ListQueues(ctx context.Context) ([]*QueueInfo, error) {
	return ListQueues(ctx, c.Conn)
}

// Send enqueues a message to a queue.
//
// Parameters:
// - ctx: context for operation cancellation
// - queue: target queue name
// - payload: message payload (marshaled to JSON)
func (c *Client) Send(ctx context.Context, queue string, payload any) error {
	return Send(ctx, c.Conn, queue, payload)
}

// SendWithOpts enqueues a message with options.
//
// Parameters:
// - ctx: context for operation cancellation
// - queue: target queue name
// - payload: message payload (marshaled to JSON)
// - opts: SendOpts with Topic, DeduplicationID, and DeliverAt
func (c *Client) SendWithOpts(ctx context.Context, queue string, payload any, opts SendOpts) error {
	return SendWithOpts(ctx, c.Conn, queue, payload, opts)
}

// Dispatch sends a message to all queues subscribed to a topic.
//
// Parameters:
// - ctx: context for operation cancellation
// - topic: topic name
// - payload: message payload (marshaled to JSON)
func (c *Client) Dispatch(ctx context.Context, topic string, payload any) error {
	return Dispatch(ctx, c.Conn, topic, payload)
}

// DispatchWithOpts sends a message to topic-subscribed queues with options.
//
// Parameters:
// - ctx: context for operation cancellation
// - topic: topic name
// - payload: message payload (marshaled to JSON)
// - opts: DispatchOpts with DeduplicationID and DeliverAt
func (c *Client) DispatchWithOpts(ctx context.Context, topic string, payload any, opts DispatchOpts) error {
	return DispatchWithOpts(ctx, c.Conn, topic, payload, opts)
}

// Read reads messages from a queue with a specified hide duration.
//
// Parameters:
// - ctx: context for operation cancellation
// - queue: source queue name
// - quantity: maximum number of messages to read
// - hideFor: duration to hide messages from other readers
func (c *Client) Read(ctx context.Context, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	return Read(ctx, c.Conn, queue, quantity, hideFor)
}

// ReadPoll reads messages from a queue with polling support.
// Polls repeatedly until messages are available or pollFor timeout is reached.
//
// Parameters:
// - ctx: context for operation cancellation
// - queue: source queue name
// - quantity: maximum number of messages to read
// - hideFor: duration to hide messages from other readers
// - pollFor: total duration to poll before timing out
// - pollInterval: interval between poll attempts
func (c *Client) ReadPoll(ctx context.Context, queue string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]Message, error) {
	return ReadPoll(ctx, c.Conn, queue, quantity, hideFor, pollFor, pollInterval)
}

// Hide hides a single message from being read.
//
// Parameters:
// - ctx: context for operation cancellation
// - queue: queue name containing the message
// - id: message ID
// - hideFor: duration to hide message from readers
//
// Returns: true if message existed, false if not found
func (c *Client) Hide(ctx context.Context, queue string, id int64, hideFor time.Duration) (bool, error) {
	return Hide(ctx, c.Conn, queue, id, hideFor)
}

// HideMany hides multiple messages from being read.
//
// Parameters:
// - ctx: context for operation cancellation
// - queue: queue name containing the messages
// - ids: slice of message IDs
// - hideFor: duration to hide messages from readers
func (c *Client) HideMany(ctx context.Context, queue string, ids []int64, hideFor time.Duration) error {
	return HideMany(ctx, c.Conn, queue, ids, hideFor)
}

// Delete deletes a single message from a queue.
//
// Parameters:
// - ctx: context for operation cancellation
// - queue: queue name containing the message
// - id: message ID
//
// Returns: true if message existed, false if not found
func (c *Client) Delete(ctx context.Context, queue string, id int64) (bool, error) {
	return Delete(ctx, c.Conn, queue, id)
}

// DeleteMany deletes multiple messages from a queue.
//
// Parameters:
// - ctx: context for operation cancellation
// - queue: queue name containing the messages
// - ids: slice of message IDs
func (c *Client) DeleteMany(ctx context.Context, queue string, ids []int64) error {
	return DeleteMany(ctx, c.Conn, queue, ids)
}

// CreateTask creates a new task definition.
//
// Parameters:
// - ctx: context for operation cancellation
// - task: Task definition with name and handler
func (c *Client) CreateTask(ctx context.Context, task *Task) error {
	return CreateTask(ctx, c.Conn, task)
}

// GetTask retrieves task metadata by name.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: task name
func (c *Client) GetTask(ctx context.Context, name string) (*TaskInfo, error) {
	return GetTask(ctx, c.Conn, name)
}

// ListTasks returns all tasks
func (c *Client) ListTasks(ctx context.Context) ([]*TaskInfo, error) {
	return ListTasks(ctx, c.Conn)
}

// RunTask enqueues a task execution and returns a handle for monitoring.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: task name
// - input: task input (marshaled to JSON)
//
// Returns: handle to poll task status and retrieve output
func (c *Client) RunTask(ctx context.Context, name string, input any) (*TaskHandle, error) {
	return RunTask(ctx, c.Conn, name, input)
}

// RunTaskWithOpts enqueues a task with options.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: task name
// - input: task input (marshaled to JSON)
// - opts: RunTaskOpts with DeduplicationID for deduplication
//
// Returns: handle to poll task status and retrieve output
func (c *Client) RunTaskWithOpts(ctx context.Context, name string, input any, opts RunTaskOpts) (*TaskHandle, error) {
	return RunTaskWithOpts(ctx, c.Conn, name, input, opts)
}

// GetTaskRun retrieves a specific task run result.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: task name
// - id: task run ID
func (c *Client) GetTaskRun(ctx context.Context, name string, id int64) (*TaskRunInfo, error) {
	return GetTaskRun(ctx, c.Conn, name, id)
}

// ListTaskRuns returns recent task runs for a task.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: task name
func (c *Client) ListTaskRuns(ctx context.Context, name string) ([]*TaskRunInfo, error) {
	return ListTaskRuns(ctx, c.Conn, name)
}

// CreateFlow creates a new flow definition.
//
// Parameters:
// - ctx: context for operation cancellation
// - flow: Flow definition with name and steps
func (c *Client) CreateFlow(ctx context.Context, flow *Flow) error {
	return CreateFlow(ctx, c.Conn, flow)
}

// GetFlow retrieves flow metadata by name.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: flow name
func (c *Client) GetFlow(ctx context.Context, name string) (*FlowInfo, error) {
	return GetFlow(ctx, c.Conn, name)
}

// ListFlows returns all flows
func (c *Client) ListFlows(ctx context.Context) ([]*FlowInfo, error) {
	return ListFlows(ctx, c.Conn)
}

// RunFlow enqueues a flow execution and returns a handle for monitoring.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: flow name
// - input: flow input (marshaled to JSON)
//
// Returns: handle to poll flow status and retrieve combined step outputs
func (c *Client) RunFlow(ctx context.Context, name string, input any) (*FlowHandle, error) {
	return RunFlow(ctx, c.Conn, name, input)
}

// RunFlowWithOpts enqueues a flow with options.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: flow name
// - input: flow input (marshaled to JSON)
// - opts: RunFlowOpts with DeduplicationID for deduplication
//
// Returns: handle to poll flow status and retrieve combined step outputs
func (c *Client) RunFlowWithOpts(ctx context.Context, name string, input any, opts RunFlowOpts) (*FlowHandle, error) {
	return RunFlowWithOpts(ctx, c.Conn, name, input, opts)
}

// GetFlowRun retrieves a specific flow run result.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: flow name
// - id: flow run ID
func (c *Client) GetFlowRun(ctx context.Context, name string, id int64) (*FlowRunInfo, error) {
	return GetFlowRun(ctx, c.Conn, name, id)
}

// ListFlowRuns returns recent flow runs for a flow.
//
// Parameters:
// - ctx: context for operation cancellation
// - name: flow name
func (c *Client) ListFlowRuns(ctx context.Context, name string) ([]*FlowRunInfo, error) {
	return ListFlowRuns(ctx, c.Conn, name)
}

// ListWorkers returns all registered workers.
//
// Parameters:
// - ctx: context for operation cancellation
func (c *Client) ListWorkers(ctx context.Context) ([]*WorkerInfo, error) {
	return ListWorkers(ctx, c.Conn)
}

// NewWorker creates a new worker that processes task and flow executions.
//
// Parameters:
// - ctx: context for operation cancellation
// - opts: worker options (WithTask, WithFlow, WithScheduledTask, etc.)
//
// Returns: initialized worker ready to Start()
func (c *Client) NewWorker(ctx context.Context, opts ...WorkerOpt) (*Worker, error) {
	return NewWorker(ctx, c.Conn, opts...)
}

// GC runs garbage collection to clean up expired and deleted entries.
//
// Parameters:
// - ctx: context for operation cancellation
func (c *Client) GC(ctx context.Context) error {
	return GC(ctx, c.Conn)
}
