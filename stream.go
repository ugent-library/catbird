package catbird

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StreamOptions are the optional parts of NewStreamConsumer and RegisterTrigger.
// Zero values take the defaults.
type StreamOptions struct {
	BatchSize    int           // default 50
	PollInterval time.Duration // wait when the stream is empty; default 2 seconds
	AssignEvery  time.Duration // how often the assigner runs; default 250 milliseconds
	Logger       *slog.Logger  // default slog.Default()
}

func (o StreamOptions) withDefaults() StreamOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = 50
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	if o.AssignEvery <= 0 {
		o.AssignEvery = 250 * time.Millisecond
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// StreamConsumer reads published messages in position order from a named cursor.
type StreamConsumer struct {
	pool   *pgxpool.Pool
	cursor string
	opts   StreamOptions
}

// NewStreamConsumer creates a consumer and starts the position assigner for as
// long as ctx lives. Every consumer starts one; a database lock makes sure only
// one of them does the work, so any number of processes may run consumers.
func NewStreamConsumer(ctx context.Context, pool *pgxpool.Pool, cursorName string, opts StreamOptions) *StreamConsumer {
	opts = opts.withDefaults()
	go assignPositions(ctx, pool, opts)
	return &StreamConsumer{pool: pool, cursor: cursorName, opts: opts}
}

// assignPositions moves published messages from cb_stream_pending to cb_stream,
// giving each the next position, every AssignEvery. A pending row is visible
// only once its transaction committed, so positions follow commit order: a
// message from a transaction that is still open is picked up on a later tick.
// Readers order by position and a batch of positions becomes readable when
// this statement commits, so no reader can pass a message that has no position yet.
//
// The advisory lock is taken for the statement only. When another assigner
// holds it, the DELETE matches nothing and the statement is a no-op.
func assignPositions(ctx context.Context, pool *pgxpool.Pool, opts StreamOptions) {
	ticker := time.NewTicker(opts.AssignEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		_, err := pool.Exec(ctx, `
			WITH lock AS (
				SELECT pg_try_advisory_xact_lock(hashtext('catbird'), 1) AS held
			),
			moved AS (
				DELETE FROM cb_stream_pending
				WHERE (SELECT held FROM lock)
				RETURNING message_id
			)
			INSERT INTO cb_stream (position, message_id, topic)
			SELECT nextval('cb_position_seq'), m.id, m.topic
			FROM moved
			JOIN cb_messages m ON m.id = moved.message_id
			ORDER BY m.id
		`)
		if err != nil && ctx.Err() == nil {
			opts.Logger.Error("catbird: assigning stream positions failed", "err", err)
		}
	}
}

// FetchBatch returns the next published messages after the cursor whose topic
// matches pattern (a LIKE pattern, e.g. 'order.%'). Job inputs written by
// Enqueue are not stream messages and are never returned.
func (s *StreamConsumer) FetchBatch(ctx context.Context, pattern string) ([]Message, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, s.position, m.topic, m.payload
		FROM cb_stream s
		JOIN cb_messages m ON m.id = s.message_id
		WHERE s.position > COALESCE((SELECT last_position FROM cb_cursors WHERE name = $1), 0)
		  AND s.topic LIKE $2
		ORDER BY s.position ASC
		LIMIT $3
	`, s.cursor, pattern, s.opts.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Position, &m.Topic, &m.Payload); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// Ack moves the cursor to position. The cursor never moves backwards, so
// several consumers on one cursor cannot undo each other's progress.
func (s *StreamConsumer) Ack(ctx context.Context, db Conn, position int64) error {
	_, err := db.Exec(ctx, `
		INSERT INTO cb_cursors (name, last_position) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET last_position = GREATEST(cb_cursors.last_position, EXCLUDED.last_position)
	`, s.cursor, position)
	return err
}

// RegisterTrigger enqueues a job on targetQueue for every message whose topic
// matches topicPattern, until ctx is canceled. The enqueues and the cursor
// advance commit in one transaction, and each job carries a dedup key derived
// from the message id, so a crash or a second process running the same trigger
// cannot produce a second job for the same message.
func (c *Client) RegisterTrigger(ctx context.Context, pool *pgxpool.Pool, name, topicPattern, targetQueue string, opts StreamOptions) {
	consumer := NewStreamConsumer(ctx, pool, "trigger:"+name, opts)
	go func() {
		for ctx.Err() == nil {
			n, err := c.enqueueNextBatch(ctx, pool, consumer, name, topicPattern, targetQueue)
			if err != nil && ctx.Err() == nil {
				consumer.opts.Logger.Error("catbird: trigger failed", "trigger", name, "err", err)
			}
			if err == nil && n == consumer.opts.BatchSize {
				continue // the stream may hold more
			}
			select {
			case <-ctx.Done():
			case <-time.After(consumer.opts.PollInterval):
			}
		}
	}()
}

// enqueueNextBatch reads the next batch of matching stream messages, enqueues a
// job for each, advances the cursor, and commits — all in one transaction.
// Returns how many messages it handled.
func (c *Client) enqueueNextBatch(ctx context.Context, pool *pgxpool.Pool, consumer *StreamConsumer, name, topicPattern, targetQueue string) (int, error) {
	msgs, err := consumer.FetchBatch(ctx, topicPattern)
	if err != nil || len(msgs) == 0 {
		return 0, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	for _, m := range msgs {
		dedup := "trigger:" + name + ":" + strconv.FormatInt(m.ID, 10)
		if _, err := c.Enqueue(ctx, tx, m.Topic, targetQueue, m.Payload, EnqueueOptions{DedupKey: dedup}); err != nil {
			return 0, err
		}
	}
	if err := consumer.Ack(ctx, tx, msgs[len(msgs)-1].Position); err != nil {
		return 0, err
	}
	return len(msgs), tx.Commit(ctx)
}
