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

	// The jobs whose claims the renewal loop keeps alive, by job id. Nil on a
	// queue whose HandlerTimeout fits inside its ClaimDuration: every attempt
	// there is over before its claim is, so there is nothing to renew, and
	// track and untrack do nothing. See renewClaims.
	mu       sync.Mutex
	inFlight map[int64]*inFlightJob
}

// inFlightJob is one running job as the renewal loop sees it: the attempts the
// claim was taken with, the handler's context, whose end stops renewal, and the
// cancel that ends the handler when the claim is not this attempt's any more.
type inFlightJob struct {
	id       int64
	attempts int
	ctx      context.Context
	cancel   context.CancelCauseFunc
}

// registration is a job type and the function this process runs it with. The
// job type says how a run of it is retried; the function is this process's own.
type registration struct {
	jobType *JobType
	handler Handler
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

	// One slot per job that may run at the same time: a claim takes a slot per
	// job it claims, a finished job puts its slot back.
	free := make(chan struct{}, w.queue.opts.BatchSize)
	for range w.queue.opts.BatchSize {
		free <- struct{}{}
	}
	var running sync.WaitGroup
	defer running.Wait() // on shutdown, return when the jobs in flight are done

	// A HandlerTimeout above ClaimDuration means an attempt may outlive its
	// claim, so this worker renews the claims of its running jobs; see
	// renewClaims.
	if w.queue.opts.HandlerTimeout > w.queue.opts.ClaimDuration {
		w.inFlight = map[int64]*inFlightJob{}
		running.Go(func() { w.renewClaims(ctx) })
	}

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
// hands the job back. An attempt that runs past the queue's HandlerTimeout is
// a failed attempt like any other.
func (w *worker) run(ctx context.Context, job *Job) {
	registered := w.handlers[job.Type]

	// The handler's context carries the queue's HandlerTimeout and is never a
	// shadow of ctx: the switch below tells a shutdown from a failure by the
	// worker's context, and a timeout that had cancelled that one would give
	// the attempt back instead of counting it, so a handler that always times
	// out would be claimed again at once, forever. The cause layer underneath
	// is for the renewal loop: it cancels through it with ErrClaimLost when
	// the claim is not this attempt's any more, and the switch reads the
	// cause, so the attempt is discarded rather than retried on a claim it
	// lost.
	lost, cancelLost := context.WithCancelCause(ctx)
	defer cancelLost(nil)
	handlerCtx, cancelHandler := context.WithTimeout(lost, w.queue.opts.HandlerTimeout)
	defer cancelHandler()

	tracked := &inFlightJob{id: job.ID, attempts: job.Attempts, ctx: handlerCtx, cancel: cancelLost}
	w.track(tracked)
	err := registered.handler.HandleJob(handlerCtx, job)
	w.untrack(tracked)

	// The statements after the handler run on a context detached from the
	// worker's, with a bound of their own. At shutdown the worker's context is
	// canceled by the time the handler returns and pgx would reject them
	// locally: the completion of finished work would be lost and the job would
	// run a second time, and a job that has to come back would keep the full
	// ClaimDuration its claim set with the attempt already spent.
	after, cancel := context.WithTimeout(context.WithoutCancel(ctx), afterHandlerTimeout)
	defer cancel()

	if err == nil && !job.completed {
		err = Complete(after, w.runtime.pool, job)
	}
	switch {
	case err == nil:
	case errors.Is(err, ErrClaimLost), errors.Is(context.Cause(handlerCtx), ErrClaimLost):
		w.logger.Warn("catbird: claim lost before completion, work discarded", "queue", w.queue.name, "job_type", job.Type, "job_id", job.ID, "attempt", job.Attempts)
	case ctx.Err() != nil:
		w.interrupted(after, job, err)
	default:
		w.failed(after, registered.jobType, job, err)
	}
}

// interrupted hands a job back after shutdown stopped it: the attempt is given
// back and the job is claimable again at once, because nothing about it failed.
// Without this, three rolling deploys spend three attempts and 15 minutes of
// claim time on a job that never ran wrong. The update matches on attempts:
// if the claim had expired and another worker took the job, attempts has
// moved on and this writes nothing. A worker that crashes writes nothing at
// all, which is why the attempt is charged at claim time: it is the only thing
// that counts an attempt nobody saw end, and without it a job that kills its
// worker is retried forever. cause is what the handler returned: a real failure
// inside the shutdown window cannot be told from an interruption, so it is
// logged and the attempt is still given back.
func (w *worker) interrupted(ctx context.Context, job *Job, cause error) {
	_, err := w.runtime.pool.Exec(ctx, `
		UPDATE cb_jobs SET attempts = attempts - 1, claimable_at = now()
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
// Both writes match on attempts, and a write that finds no row
// means the claim is not this attempt's any more: the handler completed the job
// and then returned an error, or the claim expired and another worker has it.
// Neither is this attempt's failure to record, and the cascade must not run for
// a job that is finished or running elsewhere. The error text rides on the same
// two writes, so an attempt that lost its claim records none.
func (w *worker) failed(ctx context.Context, t *JobType, job *Job, cause error) {
	log := w.logger.With("queue", w.queue.name, "job_type", job.Type, "job_id", job.ID, "attempt", job.Attempts)

	if job.Attempts < t.opts.MaxAttempts {
		tag, err := w.runtime.pool.Exec(ctx, `
			UPDATE cb_jobs
			SET claimable_at = now() + $3::interval
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
		UPDATE cb_jobs SET died_at = now(), last_error = left($3, 256)
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
		if err := t.opts.OnDead.HandleJob(ctx, job); err != nil {
			log.Error("catbird: OnDead failed", "err", err)
		}
	}
}

// claimBatch claims up to limit ready jobs by moving claimable_at a
// ClaimDuration ahead, and returns them with their payloads and delivered
// signals. The job_type
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
		WITH claimed AS (
			UPDATE cb_jobs
			SET claimable_at = now() + $2::interval, attempts = attempts + 1, last_error = NULL
			WHERE message_id IN (
				SELECT message_id FROM cb_jobs
				WHERE queue = $1 AND died_at IS NULL AND dependencies = 0 AND claimable_at <= now()
				  AND job_type = ANY($4)
				ORDER BY claimable_at ASC LIMIT $3
				FOR UPDATE SKIP LOCKED
			)
			RETURNING message_id, job_type, attempts, coalesce(group_id, message_id) AS group_id,
			          signal, dependency_job_ids
		)
		SELECT m.id, m.topic, m.payload, m.created_at,
		       c.job_type, c.attempts, c.group_id, c.signal, c.dependency_job_ids
		FROM claimed c
		JOIN cb_messages m ON m.id = c.message_id
	`, w.queue.name, w.queue.opts.ClaimDuration, limit, w.names)
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

// track and untrack keep the set the renewal loop reads; on a queue that does
// not renew they do nothing. untrack removes only its own entry: a handler
// that hangs past HandlerTimeout loses its claim, and this process may claim
// the job again while the hung handler still runs, so the id can be tracked
// twice and the first return must not stop the renewal of the live attempt.
func (w *worker) track(j *inFlightJob) {
	if w.inFlight == nil {
		return
	}
	w.mu.Lock()
	w.inFlight[j.id] = j
	w.mu.Unlock()
}

func (w *worker) untrack(j *inFlightJob) {
	if w.inFlight == nil {
		return
	}
	w.mu.Lock()
	if w.inFlight[j.id] == j {
		delete(w.inFlight, j.id)
	}
	w.mu.Unlock()
}

// running is the jobs the next renewal covers: the tracked jobs whose handler
// contexts are still live. A spent context is skipped, which is what stops the
// renewal of a handler that ran past HandlerTimeout.
func (w *worker) running() []*inFlightJob {
	w.mu.Lock()
	defer w.mu.Unlock()
	jobs := make([]*inFlightJob, 0, len(w.inFlight))
	for _, j := range w.inFlight {
		if j.ctx.Err() == nil {
			jobs = append(jobs, j)
		}
	}
	return jobs
}

// renewClaims keeps the claims of running handlers from expiring, on a queue
// whose HandlerTimeout lets an attempt outlive its ClaimDuration. Every half
// ClaimDuration it moves claimable_at a full ClaimDuration out for every job
// whose handler context is still live, all in one statement, so one missed
// tick — a network error, a slow statement — loses nothing. Renewal follows
// the handler's context rather than the handler: past HandlerTimeout the
// context is spent and the job is renewed no further, so a handler that hangs
// there holding its goroutine still loses the job to another worker about a
// ClaimDuration later. ClaimDuration is then how long a job stays stuck when
// the process running it crashes, and HandlerTimeout alone bounds an attempt.
//
// The renewal matches on attempts like every other write. One
// that matches no row means the claim is not this attempt's any more — the
// claim lapsed and another worker took it, or Cancel marked the job dead —
// so the handler is cancelled with ErrClaimLost to stop work nothing will
// commit. That is also what lets Cancel reach a running handler on a renewing
// queue, within about half a ClaimDuration.
func (w *worker) renewClaims(ctx context.Context) {
	tick := time.NewTicker(w.queue.opts.ClaimDuration / 2)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		running := w.running()
		if len(running) == 0 {
			continue
		}
		ids := make([]int64, len(running))
		attempts := make([]int32, len(running))
		for i, j := range running {
			ids[i], attempts[i] = j.id, int32(j.attempts)
		}
		renewed := map[int64]bool{}
		rows, err := w.runtime.pool.Query(ctx, `
			UPDATE cb_jobs job
			SET claimable_at = now() + $3::interval
			FROM unnest($1::bigint[], $2::int[]) AS running (message_id, attempts)
			WHERE job.message_id = running.message_id AND job.attempts = running.attempts
			  AND job.died_at IS NULL
			RETURNING job.message_id
		`, ids, attempts, w.queue.opts.ClaimDuration)
		if err == nil {
			for rows.Next() {
				var id int64
				if err = rows.Scan(&id); err != nil {
					break
				}
				renewed[id] = true
			}
			rows.Close()
			if err == nil {
				err = rows.Err()
			}
		}
		if err != nil {
			// Nothing is cancelled on an error: the claims may all still be
			// this worker's, and the next tick renews them a full
			// ClaimDuration deep.
			if ctx.Err() == nil {
				w.logger.Error("catbird: renewing claims failed", "queue", w.queue.name, "err", err)
			}
			continue
		}
		for _, j := range running {
			if !renewed[j.id] {
				w.logger.Warn("catbird: claim lost, cancelling the handler", "queue", w.queue.name, "job_id", j.id, "attempt", j.attempts)
				j.cancel(ErrClaimLost)
			}
		}
	}
}
