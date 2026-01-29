package catbird

import (
	"context"
	"time"
)

type Client struct {
	Conn Conn
}

func New(conn Conn) *Client {
	return &Client{Conn: conn}
}

func (c *Client) CreateQueue(ctx context.Context, name string) error {
	return CreateQueue(ctx, c.Conn, name)
}

func (c *Client) CreateQueueWithOpts(ctx context.Context, name string, opts QueueOpts) error {
	return CreateQueueWithOpts(ctx, c.Conn, name, opts)
}

func (c *Client) GetQueue(ctx context.Context, name string) (*QueueInfo, error) {
	return GetQueue(ctx, c.Conn, name)
}

func (c *Client) DeleteQueue(ctx context.Context, name string) (bool, error) {
	return DeleteQueue(ctx, c.Conn, name)
}

func (c *Client) ListQueues(ctx context.Context) ([]*QueueInfo, error) {
	return ListQueues(ctx, c.Conn)
}

func (c *Client) Send(ctx context.Context, queue string, payload any) error {
	return Send(ctx, c.Conn, queue, payload)
}

func (c *Client) SendWithOpts(ctx context.Context, queue string, payload any, opts SendOpts) error {
	return SendWithOpts(ctx, c.Conn, queue, payload, opts)
}

func (c *Client) Dispatch(ctx context.Context, topic string, payload any) error {
	return Dispatch(ctx, c.Conn, topic, payload)
}

func (c *Client) DispatchWithOpts(ctx context.Context, topic string, payload any, opts DispatchOpts) error {
	return DispatchWithOpts(ctx, c.Conn, topic, payload, opts)
}

func (c *Client) Read(ctx context.Context, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	return Read(ctx, c.Conn, queue, quantity, hideFor)
}

func (c *Client) ReadPoll(ctx context.Context, queue string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]Message, error) {
	return ReadPoll(ctx, c.Conn, queue, quantity, hideFor, pollFor, pollInterval)
}

func (c *Client) Hide(ctx context.Context, queue string, id int64, hideFor time.Duration) (bool, error) {
	return Hide(ctx, c.Conn, queue, id, hideFor)
}

func (c *Client) HideMany(ctx context.Context, queue string, ids []int64, hideFor time.Duration) error {
	return HideMany(ctx, c.Conn, queue, ids, hideFor)
}

func (c *Client) Delete(ctx context.Context, queue string, id int64) (bool, error) {
	return Delete(ctx, c.Conn, queue, id)
}

func (c *Client) DeleteMany(ctx context.Context, queue string, ids []int64) error {
	return DeleteMany(ctx, c.Conn, queue, ids)
}

func (c *Client) CreateTask(ctx context.Context, name string) error {
	return CreateTask(ctx, c.Conn, name)
}

func (c *Client) GetTask(ctx context.Context, name string) (*TaskInfo, error) {
	return GetTask(ctx, c.Conn, name)
}

func (c *Client) ListTasks(ctx context.Context) ([]*TaskInfo, error) {
	return ListTasks(ctx, c.Conn)
}

func (c *Client) RunTask(ctx context.Context, name string, input any) (*TaskHandle, error) {
	return RunTask(ctx, c.Conn, name, input)
}

func (c *Client) RunTaskWithOpts(ctx context.Context, name string, input any, opts RunTaskOpts) (*TaskHandle, error) {
	return RunTaskWithOpts(ctx, c.Conn, name, input, opts)
}

func (c *Client) GetTaskRun(ctx context.Context, id string) (*TaskRunInfo, error) {
	return GetTaskRun(ctx, c.Conn, id)
}

func (c *Client) ListTaskRuns(ctx context.Context, taskName string) ([]*TaskRunInfo, error) {
	return ListTaskRuns(ctx, c.Conn, taskName)
}

func (c *Client) CreateFlow(ctx context.Context, flow *Flow) error {
	return CreateFlow(ctx, c.Conn, flow)
}

func (c *Client) GetFlow(ctx context.Context, name string) (*FlowInfo, error) {
	return GetFlow(ctx, c.Conn, name)
}

func (c *Client) ListFlows(ctx context.Context) ([]*FlowInfo, error) {
	return ListFlows(ctx, c.Conn)
}

func (c *Client) RunFlow(ctx context.Context, name string, input any) (*FlowHandle, error) {
	return RunFlow(ctx, c.Conn, name, input)
}

func (c *Client) RunFlowWithOpts(ctx context.Context, name string, input any, opts RunFlowOpts) (*FlowHandle, error) {
	return RunFlowWithOpts(ctx, c.Conn, name, input, opts)
}

func (c *Client) GetFlowRun(ctx context.Context, id string) (*FlowRunInfo, error) {
	return GetFlowRun(ctx, c.Conn, id)
}

func (c *Client) ListFlowRuns(ctx context.Context, taskName string) ([]*FlowRunInfo, error) {
	return ListFlowRuns(ctx, c.Conn, taskName)
}

func (c *Client) NewWorker(tasks []*Handler, opts ...WorkerOpt) (*Worker, error) {
	return NewWorker(c.Conn, tasks, opts...)
}

func (c *Client) ListWorkers(ctx context.Context) ([]*WorkerInfo, error) {
	return ListWorkers(ctx, c.Conn)
}

func (c *Client) StartWorker(ctx context.Context, tasks []*Handler, opts ...WorkerOpt) error {
	worker, err := NewWorker(c.Conn, tasks, opts...)
	if err != nil {
		return err
	}
	return worker.Start(ctx)
}

func (c *Client) GC(ctx context.Context) error {
	return GC(ctx, c.Conn)
}
