package catbird

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	DefaultPollFor      = 5 * time.Second
	DefaultPollInterval = 100 * time.Millisecond
)

type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Queue struct {
	Name     string    `json:"name"`
	Topics   []string  `json:"topics,omitempty"`
	Unlogged bool      `json:"unlogged"`
	DeleteAt time.Time `json:"delete_at,omitzero"`
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

type QueueOpts struct {
	Topics   []string
	DeleteAt time.Time
	Unlogged bool
}

func CreateQueue(ctx context.Context, conn Conn, name string, opts QueueOpts) error {
	if opts.DeleteAt.IsZero() {
		q := `SELECT cb_create_queue(name => $1, topics => $2, unlogged => $3);`
		_, err := conn.Exec(ctx, q, name, opts.Topics, opts.Unlogged)
		return err
	} else {
		q := `SELECT cb_create_queue(name => $1, topics => $2, delete_at => $3, unlogged => $4);`
		_, err := conn.Exec(ctx, q, name, opts.Topics, opts.DeleteAt, opts.Unlogged)
		return err
	}
}

func DeleteQueue(ctx context.Context, conn Conn, name string) (bool, error) {
	q := `SELECT * FROM cb_delete_queue(name => $1);`
	existed := false
	err := conn.QueryRow(ctx, q, name).Scan(&existed)
	return existed, err
}

func ListQueues(ctx context.Context, conn Conn) ([]Queue, error) {
	q := `SELECT name, topics, unlogged, delete_at FROM cb_queues;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleQueue)

}

type DispatchOpts struct {
	DeduplicationID string
	DeliverAt       time.Time
}

func Dispatch(ctx context.Context, conn Conn, topic string, payload any, opts DispatchOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		q := `SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''));`
		_, err := conn.Exec(ctx, q, topic, b, opts.DeduplicationID)
		return err
	} else {
		q := `SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`
		_, err := conn.Exec(ctx, q, topic, b, opts.DeduplicationID, opts.DeliverAt)
		return err
	}
}

type SendOpts struct {
	DeduplicationID string
	Topic           string
	DeliverAt       time.Time
}

func Send(ctx context.Context, conn Conn, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		q := `SELECT cb_send(queue => $1, payload => $2, deduplication_id => nullif($3, ''));`
		_, err := conn.Exec(ctx, q, queue, b, opts.DeduplicationID)
		return err
	} else {
		q := `SELECT cb_send(queue => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`
		_, err := conn.Exec(ctx, q, queue, b, opts.DeduplicationID, opts.DeliverAt)
		return err
	}
}

func Read(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read(queue => $1, quantity => $2, hide_for => $3);`
	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleMessage)
}

type ReadPollOpts struct {
	PollFor      time.Duration
	PollInterval time.Duration
}

func ReadPoll(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration, opts ReadPollOpts) ([]Message, error) {
	if opts.PollFor == 0 {
		opts.PollFor = DefaultPollFor
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = DefaultPollInterval
	}

	q := `SELECT * FROM cb_read_poll(
			queue => $1,
			quantity => $2,
			hide_for => $3,
			poll_for => $4,
			poll_interval => $5);`

	rows, err := conn.Query(ctx, q,
		queue,
		quantity,
		hideFor.Seconds(),
		opts.PollFor.Seconds(),
		opts.PollInterval.Milliseconds(),
	)
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

func GC(ctx context.Context, conn Conn) error {
	q := `SELECT cb_gc();`
	_, err := conn.Exec(ctx, q)
	return err
}

func EnqueueDispatch(batch *pgx.Batch, topic string, payload any, opts DispatchOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		batch.Queue(
			`SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''));`,
			topic, b, opts.DeduplicationID,
		)
	} else {
		batch.Queue(
			`SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`,
			topic, b, opts.DeduplicationID, opts.DeliverAt,
		)
	}

	return nil
}

func EnqueueSend(batch *pgx.Batch, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		batch.Queue(
			`SELECT cb_send(topic => $1, payload => $2, deduplication_id => nullif($3, ''));`,
			queue, b, opts.DeduplicationID,
		)
	} else {
		batch.Queue(
			`SELECT cb_send(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`,
			queue, b, opts.DeduplicationID, opts.DeliverAt,
		)
	}

	return nil
}

func scanCollectibleQueue(row pgx.CollectableRow) (Queue, error) {
	return scanQueue(row)
}

func scanQueue(row pgx.Row) (Queue, error) {
	q := Queue{}

	var deleteAt *time.Time

	if err := row.Scan(
		&q.Name,
		&q.Topics,
		&q.Unlogged,
		&deleteAt,
	); err != nil {
		return q, err
	}

	if deleteAt != nil {
		q.DeleteAt = *deleteAt
	}

	return q, nil
}

func scanCollectibleMessage(row pgx.CollectableRow) (Message, error) {
	return scanMessage(row)
}

func scanMessage(row pgx.Row) (Message, error) {
	msg := Message{}

	var deduplicationID *string
	var topic *string

	if err := row.Scan(
		&msg.ID,
		&deduplicationID,
		&topic,
		&msg.Payload,
		&msg.Deliveries,
		&msg.CreatedAt,
		&msg.DeliverAt,
	); err != nil {
		return msg, err
	}

	if topic != nil {
		msg.Topic = *topic
	}
	if deduplicationID != nil {
		msg.DeduplicationID = *deduplicationID
	}

	return msg, nil
}
