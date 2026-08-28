package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNotFound is returned when the addressed job does not exist or is not waiting
// on anything.
var ErrNotFound = errors.New("catbird: not found")

// Claim status values.
const (
	statusLive int16 = 0 // ready, waiting on dependencies, or claimed; visible_at tells which
	statusDead int16 = 1 // failed permanently or canceled; never claimed again
)

// Message is one row of cb_messages plus the job fields a worker needs.
type Message struct {
	ID            int64
	Position      int64 // place in the stream; 0 for job inputs
	Topic         string
	Payload       json.RawMessage
	Signals       map[string]json.RawMessage // payloads delivered with DeliverSignal; nil when none
	Attempts      int                        // 1 on the first run
	CorrelationID string
}

// Conn is the part of pgx.Tx and *pgxpool.Pool this package uses: a pool, a
// connection, or a transaction.
// Job handlers receive the worker's transaction through it, so the handler's
// own writes and the completion of the claim commit together.
type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Client struct{}

func NewClient() *Client {
	return &Client{}
}

// EnqueueOptions are the optional parts of Enqueue.
type EnqueueOptions struct {
	DedupKey      string        // when set, a second Enqueue with the same key does nothing
	CorrelationID string        // groups jobs so Cancel can stop them together
	Delay         time.Duration // earliest start, measured from now
	Dependencies  int           // number of ResolveDependency and DeliverSignal calls the job waits for
}

// Publish appends a message to the stream: consumers see it, no worker runs it.
// Returns the message id, or 0 when dedupKey already exists.
func (c *Client) Publish(ctx context.Context, db Conn, topic string, payload any, dedupKey string) (int64, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRow(ctx, `
		INSERT INTO cb_messages (topic, payload, stream, dedup_key)
		VALUES ($1, $2, true, $3)
		ON CONFLICT (dedup_key) DO NOTHING
		RETURNING id
	`, topic, body, nullString(dedupKey)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// Enqueue appends a message and a claim for it, and wakes the queue's workers
// unless the job still waits on dependencies. Returns the message id, or 0 when
// opts.DedupKey already exists.
//
// One statement does all three. The wake CTE calls pg_notify only when
// dependencies = 0; the final LEFT JOIN references it so that it runs (an
// unreferenced SELECT CTE is never executed). The notification is delivered
// when the caller's transaction commits, together with the row.
func (c *Client) Enqueue(ctx context.Context, db Conn, topic, queue string, payload any, opts EnqueueOptions) (int64, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRow(ctx, `
		WITH message AS (
			INSERT INTO cb_messages (topic, payload, dedup_key)
			VALUES ($1, $2, $3)
			ON CONFLICT (dedup_key) DO NOTHING
			RETURNING id
		),
		claim AS (
			INSERT INTO cb_claims (message_id, queue, visible_at, correlation_id, dependencies)
			SELECT id, $4, now() + $5::interval, $6, $7 FROM message
			RETURNING message_id, dependencies
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || $4, '') FROM claim WHERE dependencies = 0
		)
		SELECT message_id FROM claim LEFT JOIN wake ON true
	`, topic, body, nullString(opts.DedupKey), queue, opts.Delay, nullString(opts.CorrelationID), opts.Dependencies).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// Cancel marks every live job with this correlation id dead. A job that is
// already running finishes; cancel only stops jobs from starting.
func (c *Client) Cancel(ctx context.Context, db Conn, correlationID string) error {
	_, err := db.Exec(ctx, `
		UPDATE cb_claims SET status = $2
		WHERE correlation_id = $1 AND status = $3
	`, correlationID, statusDead, statusLive)
	return err
}

// GC deletes dead claims and messages older than retention. A message that
// still has a claim is kept, however old it is. Signals go with their message.
func (c *Client) GC(ctx context.Context, db Conn, retention time.Duration) error {
	_, err := db.Exec(ctx, `
		DELETE FROM cb_claims
		WHERE status = $2 AND visible_at < now() - $1::interval
	`, retention, statusDead)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		DELETE FROM cb_messages m
		WHERE created_at < now() - $1::interval
		  AND NOT EXISTS (SELECT 1 FROM cb_claims c WHERE c.message_id = m.id)
	`, retention)
	return err
}

// ResolveDependency counts one dependency of the job as done and wakes the queue
// when it was the last one. Returns ErrNotFound when the job is not waiting.
// Same shape as Enqueue: the wake CTE notifies only when the count hit 0.
func (c *Client) ResolveDependency(ctx context.Context, db Conn, messageID int64) error {
	var left int
	err := db.QueryRow(ctx, `
		WITH counted AS (
			UPDATE cb_claims SET dependencies = dependencies - 1
			WHERE message_id = $1 AND status = $2 AND dependencies > 0
			RETURNING queue, dependencies
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || queue, '') FROM counted WHERE dependencies = 0
		)
		SELECT dependencies FROM counted LEFT JOIN wake ON true
	`, messageID, statusLive).Scan(&left)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// DeliverSignal stores a payload for a job that waits on it and counts one
// dependency as done. Delivering the same name twice is a no-op. Returns
// ErrNotFound when the job is not waiting: signals must be counted in
// EnqueueOptions.Dependencies before they are delivered.
//
// The statement inserts the signal only if the job is waiting, and decrements
// only if the insert happened, so a duplicate can never decrement twice. When
// nothing was counted a second query tells a duplicate (fine) from a job that
// is not waiting (ErrNotFound).
func (c *Client) DeliverSignal(ctx context.Context, db Conn, messageID int64, name string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var left int
	err = db.QueryRow(ctx, `
		WITH waiting AS (
			SELECT message_id FROM cb_claims
			WHERE message_id = $1 AND status = $4 AND dependencies > 0
		),
		signal AS (
			INSERT INTO cb_signals (message_id, name, payload)
			SELECT message_id, $2, $3 FROM waiting
			ON CONFLICT DO NOTHING
			RETURNING message_id
		),
		counted AS (
			UPDATE cb_claims SET dependencies = dependencies - 1
			WHERE message_id IN (SELECT message_id FROM signal) AND dependencies > 0
			RETURNING queue, dependencies
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || queue, '') FROM counted WHERE dependencies = 0
		)
		SELECT dependencies FROM counted LEFT JOIN wake ON true
	`, messageID, name, body, statusLive).Scan(&left)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	// Nothing was counted: either this signal was delivered before, or the job
	// is not waiting.
	var delivered bool
	err = db.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM cb_signals WHERE message_id = $1 AND name = $2)
	`, messageID, name).Scan(&delivered)
	if err != nil {
		return err
	}
	if !delivered {
		return ErrNotFound
	}
	return nil
}

// SetOutput stores the job's result. Call it from the handler with the
// handler's tx so the result commits with the job's completion. A second call
// replaces the first.
func (c *Client) SetOutput(ctx context.Context, db Conn, messageID int64, output any) error {
	body, err := json.Marshal(output)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		INSERT INTO cb_outputs (message_id, output) VALUES ($1, $2)
		ON CONFLICT (message_id) DO UPDATE SET output = EXCLUDED.output
	`, messageID, body)
	return err
}

// Output returns the result a job stored with SetOutput, or ErrNotFound.
func (c *Client) Output(ctx context.Context, db Conn, messageID int64) (json.RawMessage, error) {
	var out json.RawMessage
	err := db.QueryRow(ctx, `SELECT output FROM cb_outputs WHERE message_id = $1`, messageID).Scan(&out)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return out, err
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
