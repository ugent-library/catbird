package catbird

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// Message represents a message in a queue
type Message struct {
	ID             int64           `json:"id"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Topic          string          `json:"topic"`
	Payload        json.RawMessage `json:"payload"`
	Deliveries     int             `json:"deliveries"`
	CreatedAt      time.Time       `json:"created_at"`
	DeliverAt      time.Time       `json:"deliver_at"`
}

type QueueInfo struct {
	Name      string    `json:"name"`
	Unlogged  bool      `json:"unlogged"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
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
	return scanQueue(conn.QueryRow(ctx, q, name))
}

// ListQueues returns all queues
func ListQueues(ctx context.Context, conn Conn) ([]*QueueInfo, error) {
	q := `SELECT name, unlogged, created_at, expires_at FROM cb_queues ORDER BY name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleQueue)
}

// DeleteQueue deletes a queue and all its messages.
// Returns true if the queue existed.
func DeleteQueue(ctx context.Context, conn Conn, name string) (bool, error) {
	q := `SELECT * FROM cb_delete_queue(name => $1);`
	existed := false
	err := conn.QueryRow(ctx, q, name).Scan(&existed)
	return existed, err
}

type SendOpts struct {
	Topic          string
	IdempotencyKey string
	DeliverAt      time.Time
}

// Send enqueues a message to the specified queue.
// The payload is marshaled to JSON.
func Send(ctx context.Context, conn Conn, queue string, payload any) error {
	return SendWithOpts(ctx, conn, queue, payload, SendOpts{})
}

// SendWithOpts enqueues a message with options for topic, idempotency key,
// and delivery time.
func SendWithOpts(ctx context.Context, conn Conn, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `SELECT cb_send(queue => $1, payload => $2, topic => $3, idempotency_key => $4, deliver_at => $5);`
	_, err = conn.Exec(ctx, q, queue, b, ptrOrNil(opts.Topic), ptrOrNil(opts.IdempotencyKey), ptrOrNil(opts.DeliverAt))
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
	IdempotencyKey string
	DeliverAt      *time.Time
}

// Dispatch sends a message to all queues subscribed to the specified topic.
func Dispatch(ctx context.Context, conn Conn, topic string, payload any) error {
	return DispatchWithOpts(ctx, conn, topic, payload, DispatchOpts{})
}

// DispatchWithOpts sends a message to topic-subscribed queues with options
// for idempotency key and delivery time.
func DispatchWithOpts(ctx context.Context, conn Conn, topic string, payload any, opts DispatchOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `SELECT cb_dispatch(topic => $1, payload => $2, idempotency_key => $3, deliver_at => $4);`
	_, err = conn.Exec(ctx, q, topic, b, ptrOrNil(opts.IdempotencyKey), ptrOrNil(opts.DeliverAt))
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

// EnqueueSend adds a Send operation to a batch for efficient bulk message sending.
func EnqueueSend(batch *pgx.Batch, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	batch.Queue(
		`SELECT cb_send(queue => $1, payload => $2, topic => $3, idempotency_key => $4, deliver_at => $5);`,
		queue, b, ptrOrNil(opts.Topic), ptrOrNil(opts.IdempotencyKey), ptrOrNil(opts.DeliverAt),
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
		`SELECT cb_dispatch(topic => $1, payload => $2, idempotency_key => $3, deliver_at => $4);`,
		topic, b, ptrOrNil(opts.IdempotencyKey), ptrOrNil(opts.DeliverAt),
	)

	return nil
}

func scanCollectibleMessage(row pgx.CollectableRow) (Message, error) {
	return scanMessage(row)
}

func scanMessage(row pgx.Row) (Message, error) {
	rec := Message{}

	var idempotencyKey *string
	var topic *string

	if err := row.Scan(
		&rec.ID,
		&idempotencyKey,
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
	if idempotencyKey != nil {
		rec.IdempotencyKey = *idempotencyKey
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
