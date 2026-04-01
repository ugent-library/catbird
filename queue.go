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
	ConcurrencyKey string          `json:"concurrency_key,omitempty"`
	Topic          string          `json:"topic"`
	Body           json.RawMessage `json:"body"`
	Headers        json.RawMessage `json:"headers,omitempty"`
	Priority       int             `json:"priority"`
	Deliveries     int             `json:"deliveries"`
	CreatedAt      time.Time       `json:"created_at"`
	VisibleAt      time.Time       `json:"visible_at"`
	ExpiresAt      time.Time       `json:"expires_at,omitzero"`
}

type QueueInfo struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at,omitzero"`
}

type QueueOpts struct {
	ExpiresAt   time.Time
	Description string
}

// CreateQueue creates a queue with the given name and optional options.
// Use Bind() separately to create topic bindings.
func CreateQueue(ctx context.Context, conn Conn, queueName string, opts ...QueueOpts) error {
	q := `SELECT cb_create_queue(name => $1, expires_at => $2, description => $3);`

	var resolved QueueOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	_, err := conn.Exec(ctx, q, queueName, ptrOrNil(resolved.ExpiresAt), ptrOrNil(resolved.Description))
	return err
}

// GetQueue retrieves queue metadata by name.
func GetQueue(ctx context.Context, conn Conn, queueName string) (*QueueInfo, error) {
	q := `SELECT name, description, created_at, expires_at FROM cb_queues WHERE name = $1;`
	return scanQueue(conn.QueryRow(ctx, q, queueName))
}

// ListQueues returns all queues
func ListQueues(ctx context.Context, conn Conn) ([]*QueueInfo, error) {
	q := `SELECT name, description, created_at, expires_at FROM cb_queues ORDER BY name;`
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
	ConcurrencyKey string
	Headers        map[string]any
	VisibleAt      time.Time
	ExpiresAt      time.Time
	Priority       int
}

// Send enqueues a message to the specified queue.
// Pass no opts to use defaults.
func Send(ctx context.Context, conn Conn, queueName string, body any, opts ...SendOpts) error {
	q, args, err := SendQuery(queueName, body, opts...)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, q, args...)
	return wrapNotDefinedErr(err, "queue", queueName)
}

// SendQuery builds the SQL query and args for a Send operation.
// Pass no opts to use defaults.
func SendQuery(queueName string, body any, opts ...SendOpts) (string, []any, error) {
	var resolved SendOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	b, err := json.Marshal(body)
	if err != nil {
		return "", nil, err
	}
	headers, err := marshalOptionalHeaders(resolved.Headers)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT cb_send(queue => $1, body => $2, topic => $3, concurrency_key => $4, headers => $5::jsonb, visible_at => $6, priority => $7, expires_at => $8);`
	args := []any{queueName, b, ptrOrNil(resolved.Topic), ptrOrNil(resolved.ConcurrencyKey), headers, ptrOrNil(resolved.VisibleAt), resolved.Priority, ptrOrNil(resolved.ExpiresAt)}

	return q, args, nil
}

type SendManyOpts struct {
	Topic           string
	ConcurrencyKeys []string
	Headers         []map[string]any
	VisibleAt       time.Time
	ExpiresAt       time.Time
	Priority        int
}

// SendMany enqueues multiple messages to the specified queue.
// Pass no opts to use defaults.
func SendMany(ctx context.Context, conn Conn, queueName string, bodies []any, opts ...SendManyOpts) error {
	q, args, err := SendManyQuery(queueName, bodies, opts...)
	if err != nil {
		return err
	}

	_, err = conn.Exec(ctx, q, args...)
	return wrapNotDefinedErr(err, "queue", queueName)
}

// SendManyQuery builds the SQL query and args for a SendMany operation.
// Pass no opts to use defaults.
func SendManyQuery(queueName string, bodies []any, opts ...SendManyOpts) (string, []any, error) {
	var resolved SendManyOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	if bodies == nil {
		bodies = []any{}
	}

	encodedBodies, err := marshalBodies(bodies)
	if err != nil {
		return "", nil, err
	}
	headers, err := marshalOptionalHeadersArray(resolved.Headers)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT cb_send(queue => $1, bodies => $2, topic => $3, concurrency_keys => $4, headers => $5::jsonb[], visible_at => $6, priority => $7, expires_at => $8);`
	args := []any{queueName, encodedBodies, ptrOrNil(resolved.Topic), resolved.ConcurrencyKeys, headers, ptrOrNil(resolved.VisibleAt), resolved.Priority, ptrOrNil(resolved.ExpiresAt)}

	return q, args, nil
}

// Bind subscribes a queue to a topic pattern.
// Pattern supports exact topics and wildcards: * (single token), # (multi-token tail).
// Examples: "foo.bar", "foo.*.bar", "foo.bar.#"
func Bind(ctx context.Context, conn Conn, queueName string, pattern string) error {
	q := `SELECT cb_bind(queue => $1, pattern => $2);`
	_, err := conn.Exec(ctx, q, queueName, pattern)
	return wrapNotDefinedErr(err, "queue", queueName)
}

// Unbind unsubscribes a queue from a topic pattern.
// Returns true if a binding was removed, false if it was already absent.
func Unbind(ctx context.Context, conn Conn, queueName string, pattern string) (bool, error) {
	q := `SELECT cb_unbind(queue => $1, pattern => $2);`
	deleted := false
	err := conn.QueryRow(ctx, q, queueName, pattern).Scan(&deleted)
	if err != nil {
		return false, wrapNotDefinedErr(err, "queue", queueName)
	}
	return deleted, nil
}

type PublishOpts struct {
	ConcurrencyKey string
	Headers        map[string]any
	VisibleAt      *time.Time
	ExpiresAt      time.Time
	Priority       int
}

type PublishManyOpts struct {
	ConcurrencyKeys []string
	Headers         []map[string]any
	VisibleAt       time.Time
	ExpiresAt       time.Time
	Priority        int
}

// Publish sends a message to topic-subscribed queues with options.
// Pass no opts to use defaults.
func Publish(ctx context.Context, conn Conn, topic string, body any, opts ...PublishOpts) (int, error) {
	q, args, err := PublishQuery(topic, body, opts...)
	if err != nil {
		return 0, err
	}

	var matchedQueues int
	err = conn.QueryRow(ctx, q, args...).Scan(&matchedQueues)
	if err != nil {
		return 0, err
	}

	return matchedQueues, nil
}

// PublishQuery builds the SQL query and args for a Publish operation.
// Pass no opts to use defaults.
func PublishQuery(topic string, body any, opts ...PublishOpts) (string, []any, error) {
	var resolved PublishOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	b, err := json.Marshal(body)
	if err != nil {
		return "", nil, err
	}
	headers, err := marshalOptionalHeaders(resolved.Headers)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT cb_publish(topic => $1, body => $2, concurrency_key => $3, headers => $4::jsonb, visible_at => $5, priority => $6, expires_at => $7);`
	args := []any{topic, b, ptrOrNil(resolved.ConcurrencyKey), headers, ptrOrNil(resolved.VisibleAt), resolved.Priority, ptrOrNil(resolved.ExpiresAt)}

	return q, args, nil
}

// PublishMany sends multiple messages to topic-subscribed queues with options.
// Pass no opts to use defaults.
func PublishMany(ctx context.Context, conn Conn, topic string, bodies []any, opts ...PublishManyOpts) (int, error) {
	q, args, err := PublishManyQuery(topic, bodies, opts...)
	if err != nil {
		return 0, err
	}

	var matchedQueues int
	err = conn.QueryRow(ctx, q, args...).Scan(&matchedQueues)
	if err != nil {
		return 0, err
	}

	return matchedQueues, nil
}

// PublishManyQuery builds the SQL query and args for a PublishMany operation.
// Pass no opts to use defaults.
func PublishManyQuery(topic string, bodies []any, opts ...PublishManyOpts) (string, []any, error) {
	var resolved PublishManyOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	if bodies == nil {
		bodies = []any{}
	}

	encodedBodies, err := marshalBodies(bodies)
	if err != nil {
		return "", nil, err
	}
	headers, err := marshalOptionalHeadersArray(resolved.Headers)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT cb_publish(topic => $1, bodies => $2, concurrency_keys => $3, headers => $4::jsonb[], visible_at => $5, priority => $6, expires_at => $7);`
	args := []any{topic, encodedBodies, resolved.ConcurrencyKeys, headers, ptrOrNil(resolved.VisibleAt), resolved.Priority, ptrOrNil(resolved.ExpiresAt)}

	return q, args, nil
}

// Read reads up to quantity messages from the queue, hiding them from other
// readers for the specified duration.
func Read(ctx context.Context, conn Conn, queueName string, quantity int, hideFor time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read(queue => $1, quantity => $2, hide_for => $3);`
	rows, err := conn.Query(ctx, q, queueName, quantity, hideFor.Milliseconds())
	if err != nil {
		return nil, wrapNotDefinedErr(err, "queue", queueName)
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
		return nil, wrapNotDefinedErr(err, "queue", queueName)
	}

	return pgx.CollectRows(rows, scanCollectibleMessage)
}

// ReaderHandler processes a queue message. Return nil to ack (delete).
// Return an error to nack (message becomes visible again after hideFor).
type ReaderHandler = func(ctx context.Context, msg Message) error

// Reader continuously reads messages from a queue and processes them with
// the given handler. Blocks until ctx is cancelled. Mirrors the ReadPoll
// signature: quantity and hideFor are required, polling behavior is optional.
func Reader(ctx context.Context, conn Conn, queueName string, quantity int, hideFor time.Duration, handler ReaderHandler, opts ...ReadPollOpts) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		msgs, err := ReadPoll(ctx, conn, queueName, quantity, hideFor, opts...)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Brief pause before retry on transient errors.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}

		for _, msg := range msgs {
			if err := handler(ctx, msg); err != nil {
				// Nack: message stays hidden, will become visible after hideFor.
				continue
			}
			// Ack: delete the message.
			_, _ = Delete(ctx, conn, queueName, msg.ID)
		}
	}
}

// Hide hides a single message from being read for the specified duration.
// Returns true if the message existed.
func Hide(ctx context.Context, conn Conn, queueName string, id int64, hideFor time.Duration) (bool, error) {
	q := `SELECT * FROM cb_hide(queue => $1, id => $2, hide_for => $3);`
	exists := false
	err := conn.QueryRow(ctx, q, queueName, id, hideFor.Milliseconds()).Scan(&exists)
	return exists, wrapNotDefinedErr(err, "queue", queueName)
}

// HideMany hides multiple messages from being read for the specified duration.
// Returns the IDs that were actually hidden.
func HideMany(ctx context.Context, conn Conn, queueName string, ids []int64, hideFor time.Duration) ([]int64, error) {
	q := `SELECT * FROM cb_hide(queue => $1, ids => $2, hide_for => $3);`
	var hiddenIDs []int64
	err := conn.QueryRow(ctx, q, queueName, ids, hideFor.Milliseconds()).Scan(&hiddenIDs)
	if err != nil {
		return nil, wrapNotDefinedErr(err, "queue", queueName)
	}
	return hiddenIDs, nil
}

// Delete deletes a single message from the queue.
// Returns true if the message existed.
func Delete(ctx context.Context, conn Conn, queueName string, id int64) (bool, error) {
	q := `SELECT * FROM cb_delete(queue => $1, id => $2);`
	existed := false
	err := conn.QueryRow(ctx, q, queueName, id).Scan(&existed)
	return existed, wrapNotDefinedErr(err, "queue", queueName)
}

// DeleteMany deletes multiple messages from the queue.
// Returns the IDs that were actually deleted.
func DeleteMany(ctx context.Context, conn Conn, queueName string, ids []int64) ([]int64, error) {
	q := `SELECT * FROM cb_delete(queue => $1, ids => $2);`
	var deletedIDs []int64
	err := conn.QueryRow(ctx, q, queueName, ids).Scan(&deletedIDs)
	if err != nil {
		return nil, wrapNotDefinedErr(err, "queue", queueName)
	}
	return deletedIDs, nil
}

func scanCollectibleMessage(row pgx.CollectableRow) (Message, error) {
	return scanMessage(row)
}

func scanMessage(row pgx.Row) (Message, error) {
	rec := Message{}

	var concurrencyKey *string
	var topic *string
	var headers *json.RawMessage
	var expiresAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&concurrencyKey,
		&topic,
		&rec.Body,
		&headers,
		&rec.Priority,
		&rec.Deliveries,
		&rec.CreatedAt,
		&rec.VisibleAt,
		&expiresAt,
	); err != nil {
		return rec, err
	}

	if topic != nil {
		rec.Topic = *topic
	}
	if concurrencyKey != nil {
		rec.ConcurrencyKey = *concurrencyKey
	}
	if headers != nil {
		rec.Headers = *headers
	}
	if expiresAt != nil {
		rec.ExpiresAt = *expiresAt
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
