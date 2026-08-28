package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// JobHandler runs one job. tx is the worker's transaction: writes made through it
// commit together with the completion of the job, or not at all.
type JobHandler func(ctx context.Context, tx Conn, msg Message) error

// WorkerOptions are the optional parts of NewWorker. Zero values take the defaults.
type WorkerOptions struct {
	Lease        time.Duration // how long one attempt may run; default 5 minutes
	MaxAttempts  int           // attempts before the job is dead; default 5
	Backoff      time.Duration // wait after a failed attempt; default 1 minute
	BatchSize    int           // jobs claimed per round; default 50
	PollInterval time.Duration // wake-up interval when no notification arrives; default 5 seconds
	OnDead       JobHandler    // runs once, outside the job transaction, after the last failed attempt
	Logger       *slog.Logger  // default: the runtime's logger
}

func (o WorkerOptions) withDefaults() WorkerOptions {
	if o.Lease <= 0 {
		o.Lease = 5 * time.Minute
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.Backoff <= 0 {
		o.Backoff = time.Minute
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 50
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 5 * time.Second
	}
	return o
}

// errLeaseExpired: another worker claimed the job after our lease ran out.
var errLeaseExpired = errors.New("catbird: lease expired before completion")

type Worker struct {
	runtime *Runtime
	queue   string
	handler JobHandler
	opts    WorkerOptions
}

// NewWorker declares a worker on the runtime: once the runtime is started, it
// claims jobs on queue in batches and runs handler for each.
func NewWorker(r *Runtime, queue string, handler JobHandler, opts WorkerOptions) *Worker {
	opts = opts.withDefaults()
	if opts.Logger == nil {
		opts.Logger = r.opts.Logger
	}
	w := &Worker{runtime: r, queue: queue, handler: handler, opts: opts}
	r.declare("cb_queue_"+queue, w.start)
	return w
}

// Worker is NewWorker(r, queue, handler, opts).
func (r *Runtime) Worker(queue string, handler JobHandler, opts WorkerOptions) *Worker {
	return NewWorker(r, queue, handler, opts)
}

// start claims batches of jobs and runs each batch concurrently, until ctx is
// canceled. Between batches it waits for a NOTIFY or for PollInterval.
func (w *Worker) start(ctx context.Context) {
	wake, unsubscribe := w.runtime.subscribe("cb_queue_" + w.queue)
	defer unsubscribe()

	for {
		msgs, err := w.claimBatch(ctx)
		if err != nil && ctx.Err() == nil {
			w.opts.Logger.Error("catbird: claim failed", "queue", w.queue, "err", err)
		}

		var wg sync.WaitGroup
		for _, m := range msgs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.run(ctx, m)
			}()
		}
		wg.Wait()

		if len(msgs) == w.opts.BatchSize {
			continue // the queue may hold more
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-time.After(w.opts.PollInterval):
		}
	}
}

// run executes one job: handler, then completion, in one transaction.
// Any error schedules a retry or marks the job dead.
func (w *Worker) run(ctx context.Context, m Message) {
	tx, err := w.runtime.pool.Begin(ctx)
	if err != nil {
		w.opts.Logger.Error("catbird: begin failed", "queue", w.queue, "message_id", m.ID, "err", err)
		return
	}
	defer tx.Rollback(ctx) // no-op after a successful commit

	err = w.handler(ctx, tx, m)
	if err == nil {
		err = w.complete(ctx, tx, m)
	}
	switch {
	case err == nil:
	case errors.Is(err, errLeaseExpired):
		w.opts.Logger.Warn("catbird: lease expired before completion, work discarded", "queue", w.queue, "message_id", m.ID, "attempt", m.Attempts)
	default:
		w.failed(ctx, m, err)
	}
}

// complete deletes the claim and its signals and commits, all in the handler's
// transaction. attempts is the lease token: if another worker claimed the job
// after our lease expired, attempts moved on and the delete finds nothing.
func (w *Worker) complete(ctx context.Context, tx pgx.Tx, m Message) error {
	tag, err := tx.Exec(ctx, `
		DELETE FROM cb_claims WHERE message_id = $1 AND attempts = $2
	`, m.ID, m.Attempts)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errLeaseExpired
	}
	if _, err := tx.Exec(ctx, `DELETE FROM cb_signals WHERE message_id = $1`, m.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// failed schedules a retry, or after the last attempt marks the job dead,
// cancels its correlation group, and runs OnDead.
func (w *Worker) failed(ctx context.Context, m Message, cause error) {
	log := w.opts.Logger.With("queue", w.queue, "message_id", m.ID, "attempt", m.Attempts)

	if m.Attempts < w.opts.MaxAttempts {
		log.Warn("catbird: job failed, will retry", "err", cause)
		_, err := w.runtime.pool.Exec(ctx, `
			UPDATE cb_claims SET visible_at = now() + $3::interval
			WHERE message_id = $1 AND attempts = $2 AND status = $4
		`, m.ID, m.Attempts, w.opts.Backoff, statusLive)
		if err != nil {
			log.Error("catbird: scheduling retry failed", "err", err)
		}
		return
	}

	log.Error("catbird: job dead", "err", cause)
	_, err := w.runtime.pool.Exec(ctx, `
		UPDATE cb_claims SET status = $3 WHERE message_id = $1 AND attempts = $2
	`, m.ID, m.Attempts, statusDead)
	if err != nil {
		log.Error("catbird: marking dead failed", "err", err)
	}
	if m.CorrelationID != "" {
		if err := NewClient().Cancel(ctx, w.runtime.pool, m.CorrelationID); err != nil {
			log.Error("catbird: cancel failed", "correlation_id", m.CorrelationID, "err", err)
		}
	}
	if w.opts.OnDead != nil {
		if err := w.opts.OnDead(ctx, w.runtime.pool, m); err != nil {
			log.Error("catbird: OnDead failed", "err", err)
		}
	}
}

// claimBatch leases up to BatchSize ready jobs by moving visible_at past the
// lease, and returns them with their payloads and delivered signals.
func (w *Worker) claimBatch(ctx context.Context) ([]Message, error) {
	rows, err := w.runtime.pool.Query(ctx, `
		WITH leased AS (
			UPDATE cb_claims
			SET visible_at = now() + $2::interval, attempts = attempts + 1
			WHERE message_id IN (
				SELECT message_id FROM cb_claims
				WHERE queue = $1 AND status = $4 AND dependencies = 0 AND visible_at <= now()
				ORDER BY visible_at ASC LIMIT $3
				FOR UPDATE SKIP LOCKED
			)
			RETURNING message_id, attempts, correlation_id
		)
		SELECT m.id, m.topic, m.payload, l.attempts, l.correlation_id,
		       (SELECT jsonb_object_agg(name, payload) FROM cb_signals s WHERE s.message_id = m.id)
		FROM leased l
		JOIN cb_messages m ON m.id = l.message_id
	`, w.queue, w.opts.Lease, w.opts.BatchSize, statusLive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var correlation *string
		var signals []byte
		if err := rows.Scan(&m.ID, &m.Topic, &m.Payload, &m.Attempts, &correlation, &signals); err != nil {
			return nil, err
		}
		if correlation != nil {
			m.CorrelationID = *correlation
		}
		if len(signals) > 0 {
			if err := json.Unmarshal(signals, &m.Signals); err != nil {
				return nil, err
			}
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}
