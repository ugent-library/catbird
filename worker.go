package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// JobHandler runs one job. It is given no connection: a handler that needs one
// opens it, and decides for itself how long to hold it. A handler that returns
// nil without calling Client.CompleteJob leaves the job for the worker to
// complete afterwards, which means its writes committed before the job ended
// and a crash in between runs them again. See CompleteJob.
type JobHandler func(ctx context.Context, job *Message) error

// WorkerOptions are the optional parts of NewWorker. Zero values take the defaults.
type WorkerOptions struct {
	Lease        time.Duration // how long one attempt may run; default 5 minutes
	MaxAttempts  int           // attempts before the job is dead; default 5
	Backoff      time.Duration // wait after a failed attempt; default 1 minute
	BatchSize    int           // jobs running at once; default 50
	PollInterval time.Duration // wake-up interval when no notification arrives; default 5 seconds
	OnDead       JobHandler    // runs once after the last failed attempt
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

type Worker struct {
	runtime *Runtime
	queue   string
	handler JobHandler
	opts    WorkerOptions
}

// NewWorker declares a worker on the runtime: once the runtime is started, it
// claims jobs on queue and runs handler for each, up to BatchSize at a time.
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

// waitForSlots is how long the claim loop waits for more slots to free before
// it claims, while jobs are still waiting. Without the wait a worker running
// short jobs claims one or two of them per statement instead of a full batch.
const waitForSlots = 5 * time.Millisecond

// afterHandlerTimeout limits the statements that run once the handler has
// returned: the completion, the retry, the give-back. They run on a context of
// their own, so nothing else stops them, and it is short because Start waits
// for the jobs in flight and a longer wait would hold a stopping process.
const afterHandlerTimeout = 5 * time.Second

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

		jobs, err := w.claimBatch(ctx, slots)
		if err != nil && ctx.Err() == nil {
			w.opts.Logger.Error("catbird: claim failed", "queue", w.queue, "err", err)
		}
		for _, job := range jobs {
			running.Go(func() {
				defer func() { free <- struct{}{} }()
				w.run(ctx, job)
			})
		}
		for range slots - len(jobs) { // the slots the queue could not fill
			free <- struct{}{}
		}

		backlog = len(jobs) == slots
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

// run executes one job. The worker holds nothing while the handler runs: no
// transaction, no connection. When the handler returns nil and did not complete
// the job in a transaction of its own, the worker completes it with one
// statement; an error schedules a retry or marks the job dead, and a shutdown
// hands the job back.
func (w *Worker) run(ctx context.Context, job *Message) {
	err := w.handler(ctx, job)

	// The statements after the handler run on a context detached from the
	// worker's, with a bound of their own. At shutdown the worker's context is
	// canceled by the time the handler returns and pgx would reject them
	// locally: the completion of finished work would be lost and the job would
	// run a second time, and a job that has to come back would keep the full
	// lease its claim set with the attempt already spent.
	after, cancel := context.WithTimeout(context.WithoutCancel(ctx), afterHandlerTimeout)
	defer cancel()

	if err == nil && !job.completed {
		err = NewClient().CompleteJob(after, w.runtime.pool, job)
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrLeaseExpired):
		w.opts.Logger.Warn("catbird: lease expired before completion, work discarded", "queue", w.queue, "message_id", job.ID, "attempt", job.Attempts)
	case ctx.Err() != nil:
		w.interrupted(after, job, err)
	default:
		w.failed(after, job, err)
	}
}

// interrupted hands a job back after shutdown stopped it: the attempt is given
// back and the job is visible again at once, because nothing about it failed.
// Without this, three rolling deploys spend three of five attempts and 15
// minutes of lease on a job that never ran wrong. attempts is the lease token,
// so if the lease had expired and another worker claimed the job, attempts has
// moved on and this writes nothing. A worker that crashes writes nothing at
// all, which is why the attempt is charged at claim time: it is the only thing
// that counts an attempt nobody saw end, and without it a job that kills its
// worker is retried forever. cause is what the handler returned: a real failure
// inside the shutdown window cannot be told from an interruption, so it is
// logged and the attempt is still given back.
func (w *Worker) interrupted(ctx context.Context, job *Message, cause error) {
	_, err := w.runtime.pool.Exec(ctx, `
		UPDATE cb_claims SET attempts = attempts - 1, visible_at = now()
		WHERE message_id = $1 AND attempts = $2 AND status = $3
	`, job.ID, job.Attempts, statusLive)
	if err != nil {
		w.opts.Logger.Error("catbird: returning an interrupted job failed", "queue", w.queue, "message_id", job.ID, "err", err, "cause", cause)
		return
	}
	w.opts.Logger.Info("catbird: job stopped by shutdown, returned to the queue", "queue", w.queue, "message_id", job.ID, "err", cause)
}

// failed schedules a retry, or after the last attempt marks the job dead,
// cancels its correlation group, and runs OnDead. Both writes carry the
// attempts lease token, and a write that finds no row means the claim is not
// this attempt's any more: the handler completed the job and then returned an
// error, or the lease expired and another worker has it. Neither is this
// attempt's failure to record, and the cascade must not run for a job that is
// finished or running elsewhere.
func (w *Worker) failed(ctx context.Context, job *Message, cause error) {
	log := w.opts.Logger.With("queue", w.queue, "message_id", job.ID, "attempt", job.Attempts)

	if job.Attempts < w.opts.MaxAttempts {
		tag, err := w.runtime.pool.Exec(ctx, `
			UPDATE cb_claims SET visible_at = now() + $3::interval
			WHERE message_id = $1 AND attempts = $2 AND status = $4
		`, job.ID, job.Attempts, w.opts.Backoff, statusLive)
		switch {
		case err != nil:
			log.Error("catbird: scheduling retry failed", "err", err, "cause", cause)
		case tag.RowsAffected() == 0:
			log.Warn("catbird: handler failed after the claim was gone, no retry scheduled", "err", cause)
		default:
			log.Warn("catbird: job failed, will retry", "err", cause)
		}
		return
	}

	tag, err := w.runtime.pool.Exec(ctx, `
		UPDATE cb_claims SET status = $3 WHERE message_id = $1 AND attempts = $2
	`, job.ID, job.Attempts, statusDead)
	if err != nil {
		log.Error("catbird: marking dead failed", "err", err, "cause", cause)
		return
	}
	if tag.RowsAffected() == 0 {
		log.Warn("catbird: handler failed after the claim was gone, not marking it dead", "err", cause)
		return
	}

	log.Error("catbird: job dead", "err", cause)
	if job.CorrelationID != "" {
		if err := NewClient().Cancel(ctx, w.runtime.pool, job.CorrelationID); err != nil {
			log.Error("catbird: cancel failed", "correlation_id", job.CorrelationID, "err", err)
		}
	}
	if w.opts.OnDead != nil {
		if err := w.opts.OnDead(ctx, job); err != nil {
			log.Error("catbird: OnDead failed", "err", err)
		}
	}
}

// claimBatch leases up to limit ready jobs by moving visible_at past the lease,
// and returns them with their payloads and delivered signals.
func (w *Worker) claimBatch(ctx context.Context, limit int) ([]*Message, error) {
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

	var jobs []*Message
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
		jobs = append(jobs, &m)
	}
	return jobs, rows.Err()
}
