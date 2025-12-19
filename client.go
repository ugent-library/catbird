package catbird

import (
	"context"
	"time"
)

type Client struct {
	conn Conn
}

func New(conn Conn) *Client {
	return &Client{conn: conn}
}

func (c *Client) CreateQueue(ctx context.Context, name string, opts QueueOpts) error {
	return CreateQueue(ctx, c.conn, name, opts)
}

func (c *Client) DeleteQueue(ctx context.Context, name string) (bool, error) {
	return DeleteQueue(ctx, c.conn, name)
}

func (c *Client) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	return ListQueues(ctx, c.conn)
}

func (c *Client) Dispatch(ctx context.Context, topic string, payload any, opts DispatchOpts) error {
	return Dispatch(ctx, c.conn, topic, payload, opts)
}

func (c *Client) Send(ctx context.Context, queue string, payload any, opts SendOpts) error {
	return Send(ctx, c.conn, queue, payload, opts)
}

func (c *Client) Read(ctx context.Context, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	return Read(ctx, c.conn, queue, quantity, hideFor)
}

func (c *Client) ReadPoll(ctx context.Context, queue string, quantity int, hideFor time.Duration, opts ReadPollOpts) ([]Message, error) {
	return ReadPoll(ctx, c.conn, queue, quantity, hideFor, opts)
}

func (c *Client) Hide(ctx context.Context, queue string, id int64, hideFor time.Duration) (bool, error) {
	return Hide(ctx, c.conn, queue, id, hideFor)
}

func (c *Client) HideMany(ctx context.Context, queue string, ids []int64, hideFor time.Duration) error {
	return HideMany(ctx, c.conn, queue, ids, hideFor)
}

func (c *Client) Delete(ctx context.Context, queue string, id int64) (bool, error) {
	return Delete(ctx, c.conn, queue, id)
}

func (c *Client) DeleteMany(ctx context.Context, queue string, ids []int64) error {
	return DeleteMany(ctx, c.conn, queue, ids)
}

func (c *Client) CreateTask(ctx context.Context, task *Task) error {
	return CreateTask(ctx, c.conn, task)
}

func (c *Client) RunTask(ctx context.Context, name string, input any, opts RunTaskOpts) (string, error) {
	return RunTask(ctx, c.conn, name, input, opts)
}

func (c *Client) GetTaskRun(ctx context.Context, id string) (*TaskRunInfo, error) {
	return GetTaskRun(ctx, c.conn, id)
}

func (c *Client) CreateFlow(ctx context.Context, flow *Flow) error {
	return CreateFlow(ctx, c.conn, flow)
}

func (c *Client) RunFlow(ctx context.Context, name string, input any) (string, error) {
	return RunFlow(ctx, c.conn, name, input)
}

func (c *Client) NewWorker(opts WorkerOpts) (*Worker, error) {
	return NewWorker(c.conn, opts)
}

func (c *Client) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	return ListWorkers(ctx, c.conn)
}

func (c *Client) GC(ctx context.Context) error {
	return GC(ctx, c.conn)
}

func (c *Client) GCTask() *Task {
	return GCTask(c.conn)
}
