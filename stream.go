package catbird

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StreamOptions are the optional parts of NewConsumer and NewTrigger.
// Zero values take the defaults.
type StreamOptions struct {
	BatchSize    int           // default 50
	PollInterval time.Duration // wait when the stream is empty; default 2 seconds
}

func (o StreamOptions) withDefaults() StreamOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = 50
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	return o
}

// Consumer reads published messages in position order from a named cursor.
type Consumer struct {
	runtime *Runtime
	cursor  string
	opts    StreamOptions
}

// NewConsumer returns a consumer that reads from the named cursor. A cursor
// that does not exist yet starts at position 0. FetchBatch and Ack work whether
// or not the runtime is started; positions are only assigned while it runs.
func NewConsumer(r *Runtime, cursor string, opts StreamOptions) *Consumer {
	return &Consumer{runtime: r, cursor: cursor, opts: opts.withDefaults()}
}

// Consumer is NewConsumer(r, cursor, opts).
func (r *Runtime) Consumer(cursor string, opts StreamOptions) *Consumer {
	return NewConsumer(r, cursor, opts)
}

// assignPositions gives every published message that has none the next
// position, every AssignEvery. A message is visible to the assigner only once
// its transaction committed, so positions follow commit order: a message from a
// transaction that is still open is picked up on a later tick. Readers order by
// position and a batch of positions becomes readable when this statement
// commits, so a reader does not pass a message that has no position yet.
//
// The advisory lock is taken for the statement only. When another assigner
// holds it, the one-time filter on the UPDATE skips the scan. The UPDATE also
// requires position IS NULL, checked again on the committed row when it had to
// wait for a lock, so two assigners that run at the same time cannot move a
// position that is already set.
//
// When it assigned anything it sends NOTIFY on channel cb_stream with the
// highest new position, so a LISTENing reader can fetch instead of polling.
func assignPositions(ctx context.Context, pool *pgxpool.Pool, opts Options) {
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
				WHERE m.id = u.id AND m.position IS NULL AND (SELECT held FROM lock)
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
// no pattern syntax. The messages Enqueue writes get no position and are not
// returned: enqueuing a job does not publish it.
func (c *Consumer) FetchBatch(ctx context.Context, topic string) ([]Message, error) {
	rows, err := c.runtime.pool.Query(ctx, `
		SELECT id, position, topic, payload, created_at
		FROM cb_messages
		WHERE position > COALESCE((SELECT last_position FROM cb_cursors WHERE name = $1), 0)
		  AND ($2 = '' OR topic = $2 OR topic LIKE $3)
		ORDER BY position ASC
		LIMIT $4
	`, c.cursor, topic, subtopics(topic), c.opts.BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Position, &m.Topic, &m.Payload, &m.CreatedAt); err != nil {
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
func (c *Consumer) Ack(ctx context.Context, db Conn, position int64) error {
	_, err := db.Exec(ctx, `
		INSERT INTO cb_cursors (name, last_position) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET last_position = GREATEST(cb_cursors.last_position, EXCLUDED.last_position)
	`, c.cursor, position)
	return err
}

// Trigger turns stream messages into jobs.
type Trigger struct {
	runtime  *Runtime
	name     string
	topic    string
	queue    string
	consumer *Consumer
	opts     StreamOptions
}

// NewTrigger declares a trigger on the runtime: once the runtime is started,
// every message on topic or a topic under it (see FetchBatch) becomes a job on
// targetQueue. The enqueues and the cursor advance commit in one transaction,
// and each job carries a dedup key derived from the message id, so a crash or a
// second process running the same trigger cannot produce a second job for the
// same message.
func NewTrigger(r *Runtime, name, topic, targetQueue string, opts StreamOptions) *Trigger {
	t := &Trigger{
		runtime:  r,
		name:     name,
		topic:    topic,
		queue:    targetQueue,
		consumer: NewConsumer(r, "trigger:"+name, opts),
		opts:     opts.withDefaults(),
	}
	r.declare("cb_stream", t.start)
	return t
}

// Trigger is NewTrigger(r, name, topic, targetQueue, opts).
func (r *Runtime) Trigger(name, topic, targetQueue string, opts StreamOptions) *Trigger {
	return NewTrigger(r, name, topic, targetQueue, opts)
}

// start runs the trigger until ctx is canceled: a batch whenever the assigner
// announces new positions, and every PollInterval in case a notification was
// lost.
func (t *Trigger) start(ctx context.Context) {
	wake, unsubscribe := t.runtime.subscribe("cb_stream")
	defer unsubscribe()

	for ctx.Err() == nil {
		n, err := t.enqueueNextBatch(ctx)
		if err != nil && ctx.Err() == nil {
			t.runtime.opts.Logger.Error("catbird: trigger failed", "trigger", t.name, "err", err)
		}
		if err == nil && n == t.opts.BatchSize {
			continue // the stream may hold more
		}
		select {
		case <-ctx.Done():
		case <-wake:
		case <-time.After(t.opts.PollInterval):
		}
	}
}

// enqueueNextBatch reads the next batch of matching stream messages, enqueues a
// job for each, advances the cursor, and commits — all in one transaction. The
// whole batch is one EnqueueBatch statement, so a trigger costs one round trip
// per batch and wakes the target queue once. Returns how many messages it
// handled.
func (t *Trigger) enqueueNextBatch(ctx context.Context) (int, error) {
	msgs, err := t.consumer.FetchBatch(ctx, t.topic)
	if err != nil || len(msgs) == 0 {
		return 0, err
	}

	jobs := make([]BatchMessage, len(msgs))
	for i, m := range msgs {
		jobs[i] = BatchMessage{
			Topic:    m.Topic,
			Payload:  m.Payload,
			DedupKey: "trigger:" + t.name + ":" + strconv.FormatInt(m.ID, 10),
		}
	}

	tx, err := t.runtime.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	if _, err := EnqueueBatch(ctx, tx, t.queue, jobs, EnqueueOptions{}); err != nil {
		return 0, err
	}
	if err := t.consumer.Ack(ctx, tx, msgs[len(msgs)-1].Position); err != nil {
		return 0, err
	}
	return len(msgs), tx.Commit(ctx)
}
