package catbird

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
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

// assignPositions gives every published message that has none the next
// position, every AssignEvery. A message is visible to the assigner only once
// its transaction committed, so positions follow commit order: a message from a
// transaction that is still open is picked up on a later tick. Readers order by
// position and a batch of positions becomes readable when this statement
// commits, so a reader does not pass a message that has no position yet.
//
// The advisory lock is taken for the statement only. When another assigner
// holds it, the one-time filter on the UPDATE skips the scan.
//
// When it assigned anything it sends NOTIFY on channel cb_stream with the
// highest new position, so a LISTENing reader can fetch instead of polling.
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
			unassigned AS (
				SELECT id FROM cb_messages
				WHERE stream AND position IS NULL
				ORDER BY id
				LIMIT 5000
			),
			assigned AS (
				UPDATE cb_messages m
				SET position = nextval('cb_position_seq')
				FROM unassigned u
				WHERE m.id = u.id AND (SELECT held FROM lock)
				RETURNING position
			)
			SELECT pg_notify('cb_stream', max(position)::text) FROM assigned HAVING count(*) > 0
		`)
		if err != nil && ctx.Err() == nil {
			opts.Logger.Error("catbird: assigning stream positions failed", "err", err)
		}
	}
}

// FetchBatch returns the next published messages after the cursor on topic and
// on every topic under it: "order" matches "order", "order.paid" and
// "order.paid.refund"; "" matches everything. Topic names are literal; there is
// no pattern syntax. Job inputs written by Enqueue have no position and are
// not returned.
func (s *StreamConsumer) FetchBatch(ctx context.Context, topic string) ([]Message, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, position, topic, payload
		FROM cb_messages
		WHERE position > COALESCE((SELECT last_position FROM cb_cursors WHERE name = $1), 0)
		  AND ($2 = '' OR topic = $2 OR topic LIKE $3)
		ORDER BY position ASC
		LIMIT $4
	`, s.cursor, topic, subtopics(topic), s.opts.BatchSize)
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

// subtopics builds the LIKE pattern for everything under topic, with LIKE's
// own wildcard characters in the topic name escaped so they match literally.
func subtopics(topic string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(topic) + ".%"
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

// RegisterTrigger enqueues a job on targetQueue for every message on topic or
// a topic under it (see FetchBatch), until ctx is canceled. The enqueues and the cursor
// advance commit in one transaction, and each job carries a dedup key derived
// from the message id, so a crash or a second process running the same trigger
// cannot produce a second job for the same message.
func (c *Client) RegisterTrigger(ctx context.Context, pool *pgxpool.Pool, name, topic, targetQueue string, opts StreamOptions) {
	consumer := NewStreamConsumer(ctx, pool, "trigger:"+name, opts)
	go func() {
		for ctx.Err() == nil {
			n, err := c.enqueueNextBatch(ctx, pool, consumer, name, topic, targetQueue)
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
func (c *Client) enqueueNextBatch(ctx context.Context, pool *pgxpool.Pool, consumer *StreamConsumer, name, topic, targetQueue string) (int, error) {
	msgs, err := consumer.FetchBatch(ctx, topic)
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
