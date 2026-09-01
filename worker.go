package catbird

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// worker claims one queue's jobs and runs the job types registered on it. A
// process runs one per queue however many types it handles, so the claim loop,
// the goroutine and the notification channel are per queue and not per kind of
// work.
type worker struct {
	runtime  *Runtime
	queue    *Queue
	handlers map[string]registration
	names    []string // what the claim filters on, so a process takes no job it cannot run
	logger   *slog.Logger
}

// registration is a job type and the function this process runs it with. The
// job type says how a run of it is retried; the function is this process's own.
type registration struct {
	jobType *JobType
	handle  Handler
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
func (w *worker) start(ctx context.Context) {
	wake, unsubscribe := w.runtime.subscribe("cb_queue_" + w.queue.name)
	defer unsubscribe()

	// One token per job that may run at the same time: a claim takes a token per
	// job it claims, a finished job puts its token back.
	free := make(chan struct{}, w.queue.opts.BatchSize)
	for range w.queue.opts.BatchSize {
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
			w.logger.Error("catbird: claim failed", "queue", w.queue.name, "err", err)
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
		case <-time.After(w.queue.opts.PollInterval):
		}
	}
}

// takeFreeSlots takes the slots that are free besides the one the caller holds,
// and returns how many. With jobs still waiting it gives slots waitForSlots to
// free, so a busy queue is claimed by one bigger statement instead of one
// statement per finished job; with an empty queue it takes what is free now and
// leaves the claim undelayed.
func (w *worker) takeFreeSlots(free chan struct{}, backlog bool) int {
	taken := 0
	batchSize := w.queue.opts.BatchSize
	if !backlog {
		for taken < batchSize-1 {
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
	for taken < batchSize-1 {
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
// hands the job back. An attempt that runs past the queue's Timeout is a failed
// attempt like any other.
func (w *worker) run(ctx context.Context, job *Job) {
	registered := w.handlers[job.Type]

	// The handler's context carries the queue's Timeout and is never a shadow of
	// ctx: the switch below tells a shutdown from a failure by the worker's
	// context, and a timeout that had cancelled that one would give the attempt
	// back instead of counting it, so a handler that always times out would be
	// claimed again at once, forever.
	handlerCtx, cancelHandler := context.WithTimeout(ctx, w.queue.opts.Timeout)
	defer cancelHandler()
	err := registered.handle(handlerCtx, job)

	// The statements after the handler run on a context detached from the
	// worker's, with a bound of their own. At shutdown the worker's context is
	// canceled by the time the handler returns and pgx would reject them
	// locally: the completion of finished work would be lost and the job would
	// run a second time, and a job that has to come back would keep the full
	// lease its claim set with the attempt already spent.
	after, cancel := context.WithTimeout(context.WithoutCancel(ctx), afterHandlerTimeout)
	defer cancel()

	if err == nil && !job.completed {
		err = Complete(after, w.runtime.pool, job)
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrLeaseExpired):
		w.logger.Warn("catbird: lease expired before completion, work discarded", "queue", w.queue.name, "job_type", job.Type, "job_id", job.ID, "attempt", job.Attempts)
	case ctx.Err() != nil:
		w.interrupted(after, job, err)
	default:
		w.failed(after, registered.jobType, job, err)
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
func (w *worker) interrupted(ctx context.Context, job *Job, cause error) {
	_, err := w.runtime.pool.Exec(ctx, `
		UPDATE cb_claims SET attempts = attempts - 1, visible_at = now()
		WHERE message_id = $1 AND attempts = $2 AND died_at IS NULL
	`, job.ID, job.Attempts)
	if err != nil {
		w.logger.Error("catbird: returning an interrupted job failed", "queue", w.queue.name, "job_id", job.ID, "err", err, "cause", cause)
		return
	}
	w.logger.Info("catbird: job stopped by shutdown, returned to the queue", "queue", w.queue.name, "job_id", job.ID, "err", cause)
}

// failed schedules a retry, or after the last attempt marks the job dead,
// cancels its workflow, and runs OnDead. MaxAttempts, MinBackoff, MaxBackoff and
// OnDead come from the job type, so two kinds of work sharing a queue are
// retried on their own terms.
//
// The retry waits at least MinBackoff and at most what doubling that per attempt
// has reached, up to MaxBackoff, and the wait itself is drawn at random between
// the two, so the jobs of an outage come back apart instead of in one second at
// a service that is still down. The exponent stops at 20 to keep the
// multiplication inside an interval.
//
// Both writes carry the attempts lease token, and a write that finds no row
// means the claim is not this attempt's any more: the handler completed the job
// and then returned an error, or the lease expired and another worker has it.
// Neither is this attempt's failure to record, and the cascade must not run for
// a job that is finished or running elsewhere. The error text rides on the same
// two writes, so an attempt that lost its lease records none.
func (w *worker) failed(ctx context.Context, t *JobType, job *Job, cause error) {
	log := w.logger.With("queue", w.queue.name, "job_type", job.Type, "job_id", job.ID, "attempt", job.Attempts)

	if job.Attempts < t.opts.MaxAttempts {
		tag, err := w.runtime.pool.Exec(ctx, `
			UPDATE cb_claims
			SET visible_at = now() + $3::interval
			    + (least($3::interval * 2 ^ least(attempts - 1, 20), $4::interval) - $3::interval) * random(),
			    last_error = left($5, 256)
			WHERE message_id = $1 AND attempts = $2 AND died_at IS NULL
		`, job.ID, job.Attempts, t.opts.MinBackoff, t.opts.MaxBackoff, cause.Error())
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
		UPDATE cb_claims SET died_at = now(), last_error = left($3, 256)
		WHERE message_id = $1 AND attempts = $2
	`, job.ID, job.Attempts, cause.Error())
	if err != nil {
		log.Error("catbird: marking dead failed", "err", err, "cause", cause)
		return
	}
	if tag.RowsAffected() == 0 {
		log.Warn("catbird: handler failed after the claim was gone, not marking it dead", "err", cause)
		return
	}

	log.Error("catbird: job dead", "err", cause)
	if err := Cancel(ctx, w.runtime.pool, job.GroupID); err != nil {
		log.Error("catbird: cancel failed", "group_id", job.GroupID, "err", err)
	}
	if t.opts.OnDead != nil {
		if err := t.opts.OnDead(ctx, job); err != nil {
			log.Error("catbird: OnDead failed", "err", err)
		}
	}
}

// claimBatch leases up to limit ready jobs by moving visible_at past the lease,
// and returns them with their payloads and delivered signals. The job_type
// filter is what keeps a process from taking work it has no handler for: a job
// of a type this process does not know is left for one that does, rather than
// failing here.
//
// The claim clears last_error, so text on a row always belongs to the attempt
// waiting to retry and never to the one running. That is what Status reads to
// tell them apart. Clearing a NULL column is free: 372 bytes of WAL per row with
// the clause and without it. On rows that do carry text it saves, 372 against
// 629, because the claim writes a 74-byte tuple instead of a 336-byte one.
func (w *worker) claimBatch(ctx context.Context, limit int) ([]*Job, error) {
	rows, err := w.runtime.pool.Query(ctx, `
		WITH leased AS (
			UPDATE cb_claims
			SET visible_at = now() + $2::interval, attempts = attempts + 1, last_error = NULL
			WHERE message_id IN (
				SELECT message_id FROM cb_claims
				WHERE queue = $1 AND died_at IS NULL AND dependencies = 0 AND visible_at <= now()
				  AND job_type = ANY($4)
				ORDER BY visible_at ASC LIMIT $3
				FOR UPDATE SKIP LOCKED
			)
			RETURNING message_id, job_type, attempts, coalesce(group_id, message_id) AS group_id,
			          signal, dependency_job_ids
		)
		SELECT m.id, m.topic, m.payload, m.created_at,
		       l.job_type, l.attempts, l.group_id, l.signal, l.dependency_job_ids
		FROM leased l
		JOIN cb_messages m ON m.id = l.message_id
	`, w.queue.name, w.queue.opts.Lease, limit, w.names)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Topic, &job.Payload, &job.CreatedAt,
			&job.Type, &job.Attempts, &job.GroupID, &job.Signal, &job.dependencyIDs); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}
	return jobs, rows.Err()
}
