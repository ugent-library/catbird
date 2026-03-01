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
	VisibleAt      time.Time       `json:"visible_at"`
}

type QueueInfo struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Unlogged    bool      `json:"unlogged"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at,omitzero"`
}

type QueueOpts struct {
	ExpiresAt   time.Time
	Unlogged    bool
	Description string
}

// CreateQueue creates a queue with the given name and optional options.
// Use Bind() separately to create topic bindings.
func CreateQueue(ctx context.Context, conn Conn, queueName string, opts ...QueueOpts) error {
	q := `SELECT cb_create_queue(name => $1, expires_at => $2, unlogged => $3, description => $4);`

	var resolved QueueOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	_, err := conn.Exec(ctx, q, queueName, ptrOrNil(resolved.ExpiresAt), resolved.Unlogged, ptrOrNil(resolved.Description))
	return err
}

// GetQueue retrieves queue metadata by name.
func GetQueue(ctx context.Context, conn Conn, queueName string) (*QueueInfo, error) {
	q := `SELECT name, description, unlogged, created_at, expires_at FROM cb_queues WHERE name = $1;`
	return scanQueue(conn.QueryRow(ctx, q, queueName))
}

// ListQueues returns all queues
func ListQueues(ctx context.Context, conn Conn) ([]*QueueInfo, error) {
	q := `SELECT name, description, unlogged, created_at, expires_at FROM cb_queues ORDER BY name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleQueue)
}

// DeleteQueue deletes a queue and all its messages.
// Returns true if the queue existed.
func DeleteQueue(ctx context.Context, conn Conn, queueName string) (bool, error) {
	q := `SELECT * FROM cb_delete_queue(name => $1);`
	existed := false
	err := conn.QueryRow(ctx, q, queueName).Scan(&existed)
	return existed, err
}

type SendOpts struct {
	Topic          string
	IdempotencyKey string
	VisibleAt      time.Time
}

// Send enqueues a message to the specified queue.
// Pass no opts to use defaults.
func Send(ctx context.Context, conn Conn, queueName string, payload any, opts ...SendOpts) error {
	q, args, err := SendQuery(queueName, payload, opts...)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, q, args...)
	return err
}

// SendQuery builds the SQL query and args for a Send operation.
// Pass no opts to use defaults.
func SendQuery(queueName string, payload any, opts ...SendOpts) (string, []any, error) {
	var resolved SendOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT cb_send(queue => $1, payload => $2, topic => $3, idempotency_key => $4, visible_at => $5);`
	args := []any{queueName, b, ptrOrNil(resolved.Topic), ptrOrNil(resolved.IdempotencyKey), ptrOrNil(resolved.VisibleAt)}

	return q, args, nil
}

type SendManyOpts struct {
	Topic           string
	IdempotencyKeys []string
	VisibleAt       time.Time
}

// SendMany enqueues multiple messages to the specified queue.
// Pass no opts to use defaults.
func SendMany(ctx context.Context, conn Conn, queueName string, payloads []any, opts ...SendManyOpts) error {
	q, args, err := SendManyQuery(queueName, payloads, opts...)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, q, args...)
	return err
}

// SendManyQuery builds the SQL query and args for a SendMany operation.
// Pass no opts to use defaults.
func SendManyQuery(queueName string, payloads []any, opts ...SendManyOpts) (string, []any, error) {
	var resolved SendManyOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	if payloads == nil {
		payloads = []any{}
	}

	encodedPayloads, err := marshalPayloads(payloads)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT cb_send(queue => $1, payloads => $2, topic => $3, idempotency_keys => $4, visible_at => $5);`
	args := []any{queueName, encodedPayloads, ptrOrNil(resolved.Topic), resolved.IdempotencyKeys, ptrOrNil(resolved.VisibleAt)}

	return q, args, nil
}

// Bind subscribes a queue to a topic pattern.
// Pattern supports exact topics and wildcards: * (single token), # (multi-token tail).
// Examples: "foo.bar", "foo.*.bar", "foo.bar.#"
func Bind(ctx context.Context, conn Conn, queueName string, pattern string) error {
	q := `SELECT cb_bind(queue_name => $1, pattern => $2);`
	_, err := conn.Exec(ctx, q, queueName, pattern)
	return err
}

// Unbind unsubscribes a queue from a topic pattern.
func Unbind(ctx context.Context, conn Conn, queueName string, pattern string) error {
	q := `SELECT cb_unbind(queue_name => $1, pattern => $2);`
	_, err := conn.Exec(ctx, q, queueName, pattern)
	return err
}

type PublishOpts struct {
	IdempotencyKey string
	VisibleAt      *time.Time
}

type PublishManyOpts struct {
	IdempotencyKeys []string
	VisibleAt       time.Time
}

// Publish sends a message to topic-subscribed queues with options.
// Pass no opts to use defaults.
func Publish(ctx context.Context, conn Conn, topic string, payload any, opts ...PublishOpts) error {
	q, args, err := PublishQuery(topic, payload, opts...)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, q, args...)
	return err
}

// PublishQuery builds the SQL query and args for a Publish operation.
// Pass no opts to use defaults.
func PublishQuery(topic string, payload any, opts ...PublishOpts) (string, []any, error) {
	var resolved PublishOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT cb_publish(topic => $1, payload => $2, idempotency_key => $3, visible_at => $4);`
	args := []any{topic, b, ptrOrNil(resolved.IdempotencyKey), ptrOrNil(resolved.VisibleAt)}

	return q, args, nil
}

// PublishMany sends multiple messages to topic-subscribed queues with options.
// Pass no opts to use defaults.
func PublishMany(ctx context.Context, conn Conn, topic string, payloads []any, opts ...PublishManyOpts) error {
	q, args, err := PublishManyQuery(topic, payloads, opts...)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, q, args...)
	return err
}

// PublishManyQuery builds the SQL query and args for a PublishMany operation.
// Pass no opts to use defaults.
func PublishManyQuery(topic string, payloads []any, opts ...PublishManyOpts) (string, []any, error) {
	var resolved PublishManyOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	if payloads == nil {
		payloads = []any{}
	}

	encodedPayloads, err := marshalPayloads(payloads)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT cb_publish(topic => $1, payloads => $2, idempotency_keys => $3, visible_at => $4);`
	args := []any{topic, encodedPayloads, resolved.IdempotencyKeys, ptrOrNil(resolved.VisibleAt)}

	return q, args, nil
}

// Read reads up to quantity messages from the queue, hiding them from other
// readers for the specified duration.
func Read(ctx context.Context, conn Conn, queueName string, quantity int, hideFor time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read(queue => $1, quantity => $2, hide_for => $3);`
	rows, err := conn.Query(ctx, q, queueName, quantity, hideFor.Milliseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleMessage)
}

// ReadPollOpts configures ReadPoll polling behavior.
// Zero values use defaults.
type ReadPollOpts struct {
	PollFor      time.Duration
	PollInterval time.Duration
}

// ReadPoll reads messages from a queue with polling support.
// It polls repeatedly at the specified interval until messages are available
// or the pollFor timeout is reached. Pass optional ReadPollOpts to configure
// polling behavior; defaults are used when omitted.
func ReadPoll(ctx context.Context, conn Conn, queueName string, quantity int, hideFor time.Duration, opts ...ReadPollOpts) ([]Message, error) {
	var pollFor time.Duration
	var pollInterval time.Duration

	if len(opts) > 0 {
		pollFor = opts[0].PollFor
		pollInterval = opts[0].PollInterval
	}

	pollForMs, pollIntervalMs := resolvePollDurations(defaultPollFor, defaultPollInterval, pollFor, pollInterval)

	q := `SELECT * FROM cb_read_poll(queue => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`

	rows, err := conn.Query(ctx, q, queueName, quantity, hideFor.Milliseconds(), pollForMs, pollIntervalMs)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleMessage)
}

// Hide hides a single message from being read for the specified duration.
// Returns true if the message existed.
func Hide(ctx context.Context, conn Conn, queueName string, id int64, hideFor time.Duration) (bool, error) {
	q := `SELECT * FROM cb_hide(queue => $1, id => $2, hide_for => $3);`
	exists := false
	err := conn.QueryRow(ctx, q, queueName, id, hideFor.Milliseconds()).Scan(&exists)
	return exists, err
}

// HideMany hides multiple messages from being read for the specified duration.
func HideMany(ctx context.Context, conn Conn, queueName string, ids []int64, hideFor time.Duration) error {
	q := `SELECT * FROM cb_hide(queue => $1, ids => $2, hide_for => $3);`
	_, err := conn.Exec(ctx, q, queueName, ids, hideFor.Milliseconds())
	return err
}

// Delete deletes a single message from the queue.
// Returns true if the message existed.
func Delete(ctx context.Context, conn Conn, queueName string, id int64) (bool, error) {
	q := `SELECT * FROM cb_delete(queue => $1, id => $2);`
	existed := false
	err := conn.QueryRow(ctx, q, queueName, id).Scan(&existed)
	return existed, err
}

// DeleteMany deletes multiple messages from the queue.
func DeleteMany(ctx context.Context, conn Conn, queueName string, ids []int64) error {
	q := `SELECT * FROM cb_delete(queue => $1, ids => $2);`
	_, err := conn.Exec(ctx, q, queueName, ids)
	return err
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
		&rec.VisibleAt,
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

	var description *string
	var expiresAt *time.Time

	if err := row.Scan(
		&rec.Name,
		&description,
		&rec.Unlogged,
		&rec.CreatedAt,
		&expiresAt,
	); err != nil {
		return nil, err
	}

	if description != nil {
		rec.Description = *description
	}

	if expiresAt != nil {
		rec.ExpiresAt = *expiresAt
	}

	return &rec, nil
}
