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
	BatchSize    int           // jobs running at once; default 50
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
// claims jobs on queue and runs handler for each, up to BatchSize at a time.
//
// BatchSize is bounded by the pool. Every running job holds a pool connection
// for its transaction, so a worker that runs more jobs than the pool has
// connections does not get more work done: the extra jobs sit in Begin with
// their leases already running, and a job that waits there long enough is
// claimed by another worker while this one still holds a slot for it. An
// unset BatchSize takes the smaller of the default and what the pool can carry;
// one the caller asked for is lowered with a warning, since it was asked for.
func NewWorker(r *Runtime, queue string, handler JobHandler, opts WorkerOptions) *Worker {
	// One connection stays free for the worker's own statements: the claim, and
	// the retry a failing job writes after its own transaction rolled back.
	poolMax := int(r.pool.Config().MaxConns)
	limit := max(poolMax-1, 1)
	asked := opts.BatchSize
	opts = opts.withDefaults()
	if opts.Logger == nil {
		opts.Logger = r.opts.Logger
	}
	if opts.BatchSize > limit {
		if asked > 0 {
			opts.Logger.Warn("catbird: BatchSize is above what the pool can carry, lowering it",
				"queue", queue, "batch_size", asked, "lowered_to", limit,
				"pool_max_conns", poolMax)
		}
		opts.BatchSize = limit
	}
	w := &Worker{runtime: r, queue: queue, handler: handler, opts: opts}
	r.reserve(opts.BatchSize)
	r.declare("cb_queue_"+queue, w.start)
	return w
}

// Worker is NewWorker(r, queue, handler, opts).
func (r *Runtime) Worker(queue string, handler JobHandler, opts WorkerOptions) *Worker {
	return NewWorker(r, queue, handler, opts)
}

// waitForSlots is how long the claim loop waits for more slots to free before
// it claims, while jobs are still waiting. Without the wait a worker running
// short jobs claims one or two of them per statement instead of a full batch.
const waitForSlots = 5 * time.Millisecond

// start keeps up to BatchSize jobs running at once: it claims as many jobs as it
// has free slots, hands each to a goroutine, and claims again as soon as a slot
// frees, so one long job does not hold up the jobs beside it. When a claim comes
// back short the queue is empty and it waits for a NOTIFY or for PollInterval.
func (w *Worker) start(ctx context.Context) {
	wake, unsubscribe := w.runtime.subscribe("cb_queue_" + w.queue)
	defer unsubscribe()

	// One token per job that may run at the same time: a claim takes a token per
	// job it claims, a finished job puts its token back.
	free := make(chan struct{}, w.opts.BatchSize)
	for range w.opts.BatchSize {
		free <- struct{}{}
	}
	var running sync.WaitGroup
	defer running.Wait() // on shutdown, return when the jobs in flight are done

	backlog := false // the last claim came back full, so jobs are still waiting
	for {
		select {
		case <-ctx.Done():
			return
		case <-free:
		}
		slots := 1 + w.takeFreeSlots(free, backlog)

		msgs, err := w.claimBatch(ctx, slots)
		if err != nil && ctx.Err() == nil {
			w.opts.Logger.Error("catbird: claim failed", "queue", w.queue, "err", err)
		}
		for _, m := range msgs {
			running.Go(func() {
				defer func() { free <- struct{}{} }()
				w.run(ctx, m)
			})
		}
		for range slots - len(msgs) { // the slots the queue could not fill
			free <- struct{}{}
		}

		backlog = len(msgs) == slots
		if backlog {
			continue // claim again; the loop above waits for the next free slot
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-time.After(w.opts.PollInterval):
		}
	}
}

// takeFreeSlots takes the slots that are free besides the one the caller holds,
// and returns how many. With jobs still waiting it gives slots waitForSlots to
// free, so a busy queue is claimed by one bigger statement instead of one
// statement per finished job; with an empty queue it takes what is free now and
// leaves the claim undelayed.
func (w *Worker) takeFreeSlots(free chan struct{}, backlog bool) int {
	taken := 0
	if !backlog {
		for taken < w.opts.BatchSize-1 {
			select {
			case <-free:
				taken++
			default:
				return taken
			}
		}
		return taken
	}
	wait := time.NewTimer(waitForSlots)
	defer wait.Stop()
	for taken < w.opts.BatchSize-1 {
		select {
		case <-free:
			taken++
		case <-wait.C:
			return taken
		}
	}
	return taken
}

// run executes one job: handler, then completion, in one transaction.
// Any error schedules a retry or marks the job dead.
func (w *Worker) run(ctx context.Context, m Message) {
	tx, err := w.runtime.pool.Begin(ctx)
	if err != nil {
		w.opts.Logger.Error("catbird: begin failed", "queue", w.queue, "message_id", m.ID, "err", err)
		return
	}
	defer tx.Rollback(ctx) // the panic path; the ordinary one rolls back below

	err = w.handler(ctx, tx, m)
	if err == nil {
		err = w.complete(ctx, tx, m)
	}
	// The transaction is over either way, and failed below asks the pool for a
	// connection of its own. Give this one back first: BatchSize leaves exactly
	// one connection free, so a worker whose jobs all fail at once would
	// otherwise run their bookkeeping one at a time through it, each with only
	// bookkeepingTimeout to get there. Rolling back on the job's context would
	// not give it back at shutdown -- that context is canceled, and pgx then
	// closes the connection instead of ending the transaction.
	rollbackCtx, cancelRollback := context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
	_ = tx.Rollback(rollbackCtx) // no-op after a successful commit
	cancelRollback()

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

// bookkeepingTimeout bounds the statements that record how an attempt ended.
// They run on a context of their own, so a job that failed because the process
// is shutting down still gets its retry written.
const bookkeepingTimeout = 5 * time.Second

// failed schedules a retry, or after the last attempt marks the job dead,
// cancels its correlation group, and runs OnDead.
//
// Its statements do not run on the job's context. That context is the worker's,
// so at shutdown it is already canceled by the time a handler returns, every
// statement here would fail at once, and the job would keep its claim until the
// lease expired -- five minutes by default -- instead of being retried. The
// attempt is over either way; recording how it ended is what has to survive.
func (w *Worker) failed(jobCtx context.Context, m Message, cause error) {
	log := w.opts.Logger.With("queue", w.queue, "message_id", m.ID, "attempt", m.Attempts)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(jobCtx), bookkeepingTimeout)
	defer cancel()

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
	// OnDead runs on the same context and shares the same budget. It is the
	// caller's code, so a longer one would be defensible -- a job dies once and
	// this is the only notification of it -- but shutdown waits for the jobs in
	// flight, and a budget of Lease would let one slow callback hold a stopping
	// process for five minutes. A callback with more to do than fits should
	// write a row here and do the rest from a job.
	if w.opts.OnDead != nil {
		if err := w.opts.OnDead(ctx, w.runtime.pool, m); err != nil {
			log.Error("catbird: OnDead failed", "err", err)
		}
	}
}

// claimBatch leases up to limit ready jobs by moving visible_at past the lease,
// and returns them with their payloads and delivered signals.
func (w *Worker) claimBatch(ctx context.Context, limit int) ([]Message, error) {
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
		SELECT m.id, m.topic, m.payload, m.created_at, l.attempts, l.correlation_id,
		       (SELECT jsonb_object_agg(name, payload) FROM cb_signals s WHERE s.message_id = m.id)
		FROM leased l
		JOIN cb_messages m ON m.id = l.message_id
	`, w.queue, w.opts.Lease, limit, statusLive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var correlation *string
		var signals []byte
		if err := rows.Scan(&m.ID, &m.Topic, &m.Payload, &m.CreatedAt, &m.Attempts, &correlation, &signals); err != nil {
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
