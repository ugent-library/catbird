package catbird

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, optionsAndArgs ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, optionsAndArgs ...any) pgx.Row
}

type QueueOpts struct {
	DeleteAt time.Time
	Unlogged bool
}

type SendOpts struct {
	DeliverAt time.Time
}

type Message struct {
	ID        int64           `json:"id"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
	DeliverAt time.Time       `json:"updated_at"`
}

func CreateQueue(ctx context.Context, conn Conn, name string, topics []string, opts QueueOpts) error {
	if opts.DeleteAt.IsZero() {
		q := `SELECT cb_create_queue(name => $1, topics => $2, unlogged => $3);`
		_, err := conn.Exec(ctx, q, name, topics, opts.Unlogged)
		return err
	} else {
		q := `SELECT cb_create_queue(name => $1, topics => $2, delete_at => $3, unlogged => $4);`
		_, err := conn.Exec(ctx, q, name, topics, opts.DeleteAt, opts.Unlogged)
		return err
	}
}

func DeleteQueue(ctx context.Context, conn Conn, name string) (bool, error) {
	q := `SELECT * FROM cb_delete_queue(name => $1);`
	existed := false
	err := conn.QueryRow(ctx, q, name).Scan(&existed)
	return existed, err
}

func Send(ctx context.Context, conn Conn, topic string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		q := `SELECT cb_send(topic => $1, payload => $2);`
		_, err := conn.Exec(ctx, q, topic, b)
		return err
	} else {
		q := `SELECT cb_send(topic => $1, payload => $2, deliver_at => $3);`
		_, err := conn.Exec(ctx, q, topic, b, opts.DeliverAt)
		return err
	}
}

func Read(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read(queue => $1, quantity => $2, hide_for => $3);`
	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
}

func ReadPoll(ctx context.Context, conn Conn, queue string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read_poll(queue => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`
	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Seconds(), pollFor.Seconds(), pollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
}

func Hide(ctx context.Context, conn Conn, queue string, id int64, hideFor time.Duration) (bool, error) {
	q := `SELECT * FROM cb_hide(queue => $1, id => $2, hide_for => $3);`
	exists := false
	err := conn.QueryRow(ctx, q, queue, id, hideFor.Seconds()).Scan(&exists)
	return exists, err
}

func Delete(ctx context.Context, conn Conn, queue string, id int64) (bool, error) {
	q := `SELECT * FROM cb_delete(queue => $1, id => $2);`
	existed := false
	err := conn.QueryRow(ctx, q, queue, id).Scan(&existed)
	return existed, err
}

func GC(ctx context.Context, conn Conn) error {
	q := `SELECT cb_gc();`
	_, err := conn.Exec(ctx, q)
	return err
}

func EnqueueSend(batch *pgx.Batch, topic string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		batch.Queue(
			`SELECT cb_send(topic => $1, payload => $2);`,
			topic, b,
		)
	} else {
		batch.Queue(
			`SELECT cb_send(topic => $1, payload => $2, deliver_at => $3);`,
			topic, b, opts.DeliverAt,
		)
	}

	return nil
}

type Client struct {
	conn Conn
}

func New(conn Conn) *Client {
	return &Client{conn: conn}
}

func (c *Client) CreateQueue(ctx context.Context, name string, topics []string, opts QueueOpts) error {
	return CreateQueue(ctx, c.conn, name, topics, opts)
}

func (c *Client) DeleteQueue(ctx context.Context, name string) (bool, error) {
	return DeleteQueue(ctx, c.conn, name)
}

func (c *Client) Send(ctx context.Context, topic string, payload any, opts SendOpts) error {
	return Send(ctx, c.conn, topic, payload, opts)
}

func (c *Client) Read(ctx context.Context, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	return Read(ctx, c.conn, queue, quantity, hideFor)
}

func (c *Client) ReadPoll(ctx context.Context, conn Conn, queue string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]Message, error) {
	return ReadPoll(ctx, conn, queue, quantity, hideFor, pollFor, pollInterval)
}

func (c *Client) Hide(ctx context.Context, queue string, id int64, hideFor time.Duration) (bool, error) {
	return Hide(ctx, c.conn, queue, id, hideFor)
}

func (c *Client) Delete(ctx context.Context, queue string, id int64) (bool, error) {
	return Delete(ctx, c.conn, queue, id)
}

func (c *Client) GC(ctx context.Context) error {
	return GC(ctx, c.conn)
}
