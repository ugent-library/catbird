package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/claimloop"
	"github.com/ugent-library/catbird/notify"
)

const (
	// how often to look for claimable steps when idle
	workerPollInterval = 2 * time.Second
	// hand-back pause for a step the worker has no handler for, so two
	// not-yet-updated workers do not pass it back and forth as fast as
	// they can during a rolling deploy
	noHandlerPause = 5 * time.Second
)

// WorkerOpts tunes a worker. Zero fields mean the defaults.
type WorkerOpts struct {
	PollInterval time.Duration    // 2s: how often to look for claimable steps when idle
	Notifier     *notify.Notifier // wakes the worker the moment a step becomes claimable; nil = wake by poll only
}

// Worker claims and executes steps: register a handler per job with
// Handle, then Start. Which queues the worker claims is read from cb_jobs
// at startup — the same routing authority that steps are stamped from.
type Worker struct {
	pool         *pgxpool.Pool
	name         string
	logger       *slog.Logger
	pollInterval time.Duration
	notifier     *notify.Notifier
	handlers     map[string]handlerFunc
	queues       []string
	definedJobs  map[string]bool
	err          error
}

func NewWorker(pool *pgxpool.Pool, opts ...WorkerOpts) *Worker {
	var o WorkerOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.PollInterval <= 0 {
		o.PollInterval = workerPollInterval
	}
	return &Worker{
		pool:         pool,
		name:         claimloop.Name("worker"),
		logger:       slog.Default(),
		pollInterval: o.PollInterval,
		notifier:     o.Notifier,
		handlers:     make(map[string]handlerFunc),
	}
}

// Handle registers the handler for a job. fn must have signature
// func(context.Context, In) error or (Out, error), with an optional
// *jobs.Plan parameter before In — four shapes; a wrong shape surfaces as
// an error from Start.
func (w *Worker) Handle(job string, fn any) {
	h, err := newHandler(fn)
	if err != nil {
		w.err = errors.Join(w.err, fmt.Errorf("catbird: job %s: %w", job, err))
		return
	}
	w.handlers[job] = h
}

// Start runs the worker until ctx ends: claim a batch across its queues,
// then per step start, run the handler and complete or fail — extending
// the leases on a cadence while a handler runs. On shutdown, unstarted
// steps are handed back and a running handler is canceled and reported as
// a failed attempt, so redelivery is backoff-paced instead of waiting out
// the lease.
func (w *Worker) Start(ctx context.Context) error {
	if w.err != nil {
		return w.err
	}
	if len(w.handlers) == 0 {
		return errors.New("catbird: worker has no handlers")
	}
	queues, err := w.readQueues(ctx)
	if err != nil {
		return err
	}
	w.queues = queues
	if err := w.readDefinedJobs(ctx); err != nil {
		return err
	}

	var wake <-chan struct{}
	if w.notifier != nil {
		var schema string
		if err := w.pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
			return err
		}
		// a queue-channel payload is a step's claimable_at: a time
		// already reached wakes the loop at once, a future one arms one
		// timer for the earliest pending wake — that is how a
		// backoff-paced retry is claimed on time without polling for it
		waker := notify.NewWaker()
		defer waker.Stop()
		for _, q := range w.queues {
			cancel := w.notifier.Subscribe(schema+".cbq_"+q, func(payload string) {
				waker.WakeAt(notify.ParseTime(payload))
			})
			defer cancel()
		}
		wake = waker.C
		w.logger.Info(fmt.Sprintf("catbird: worker waking on notify, poll safety net every %s", w.pollInterval))
	} else {
		w.logger.Info(fmt.Sprintf("catbird: worker waking by poll every %s", w.pollInterval))
	}

	return claimloop.Run(ctx, claimloop.Options{
		PollInterval: w.pollInterval,
		Wake:         wake,
		// misconfiguration, not a transient failure
		Fatal: func(err error) bool { return errors.Is(err, ErrNotDefined) },
	}, w.processClaim)
}

// readQueues reads which pools the handled jobs route to and checks
// coverage both ways: every handled job must be defined, and every job
// routed to a claimed queue must have a handler here — a claim is
// indiscriminate within its pool, so partial coverage would strand steps.
// Read at startup only — a job defined while the worker runs reaches the
// release-with-pause path in the claim loop instead.
func (w *Worker) readQueues(ctx context.Context) ([]string, error) {
	handled := slices.Sorted(maps.Keys(w.handlers))

	rows, err := w.pool.Query(ctx,
		`SELECT j.name, coalesce(j.queue, 'default') FROM cb_jobs j
		 WHERE j.name = ANY ($1)`, handled)
	if err != nil {
		return nil, err
	}
	routed := make(map[string]string, len(handled))
	var name, queue string
	if _, err := pgx.ForEachRow(rows, []any{&name, &queue}, func() error {
		routed[name] = queue
		return nil
	}); err != nil {
		return nil, err
	}

	var queues, missing []string
	for _, job := range handled {
		q, ok := routed[job]
		if !ok {
			missing = append(missing, job)
			continue
		}
		if !slices.Contains(queues, q) {
			queues = append(queues, q)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("catbird: worker handles jobs not defined: %s",
			strings.Join(missing, ", "))
	}
	slices.Sort(queues)

	rows, err = w.pool.Query(ctx,
		`SELECT j.name FROM cb_jobs j
		 WHERE coalesce(j.queue, 'default') = ANY ($1)
		 ORDER BY j.name`, queues)
	if err != nil {
		return nil, err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	var unhandled []string
	for _, name := range names {
		if _, ok := w.handlers[name]; !ok {
			unhandled = append(unhandled, name)
		}
	}
	if len(unhandled) > 0 {
		return nil, fmt.Errorf("catbird: worker claims queues %s but has no handler for %s",
			strings.Join(queues, ", "), strings.Join(unhandled, ", "))
	}
	return queues, nil
}

// readDefinedJobs snapshots the defined job names for Plan.Step's check.
// A job defined after this worker started is looked up on first use
// (jobDefined) instead.
func (w *Worker) readDefinedJobs(ctx context.Context) error {
	rows, err := w.pool.Query(ctx, `SELECT j.name FROM cb_jobs j`)
	if err != nil {
		return err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	w.definedJobs = make(map[string]bool, len(names))
	for _, n := range names {
		w.definedJobs[n] = true
	}
	return nil
}

// jobDefined is Plan.Step's check that a step names a declared job. A
// name outside the startup snapshot is asked of the database — the job
// may have been defined since. When that lookup itself fails the answer
// is yes: cb_job_complete enforces the same rule and is the authority.
// Only the running handler calls this, so the map needs no lock.
func (w *Worker) jobDefined(ctx context.Context, name string) bool {
	if w.definedJobs[name] {
		return true
	}
	var exists bool
	if err := w.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT FROM cb_jobs j WHERE j.name = $1)`, name).Scan(&exists); err != nil {
		return true
	}
	if exists {
		w.definedJobs[name] = true
	}
	return exists
}

func (w *Worker) newPlan(ctx context.Context, runID int64, signalInput json.RawMessage) *Plan {
	return &Plan{
		ctx:         ctx,
		conn:        w.pool,
		runID:       runID,
		signalInput: signalInput,
		defined:     w.jobDefined,
	}
}

type claimedStep struct {
	RunID   int64
	StepID  int64
	Name    string
	LeaseAt time.Time
}

type stepKey struct {
	runID, stepID int64
}

// processClaim runs one claim batch. It reports whether there was
// anything to claim.
func (w *Worker) processClaim(ctx context.Context) (bool, error) {
	claimedAt := time.Now()
	rows, err := w.pool.Query(ctx,
		`SELECT c.run_id, c.step_id, c.name, c.lease_at FROM cb_job_claim($1, $2) c`,
		w.queues, w.name)
	if err != nil {
		return false, wrapErr(err)
	}
	steps, err := pgx.CollectRows(rows, pgx.RowToStructByPos[claimedStep])
	if err != nil {
		return false, wrapErr(err)
	}
	if len(steps) == 0 {
		return false, nil
	}

	// the leases this worker holds right now; extendAll refreshes it
	held := make(map[stepKey]time.Time, len(steps))
	for _, s := range steps {
		held[stepKey{s.RunID, s.StepID}] = s.LeaseAt
	}

	// extendAll pushes every held lease forward and reports whether the
	// given step is still ours. A step missing from the result was taken
	// over, canceled or given up: its handler must stop, and a late
	// complete or fail would hit the fence and change nothing.
	extendAll := func(k stepKey) (bool, error) {
		rows, err := w.pool.Query(ctx,
			`SELECT e.run_id, e.step_id, e.lease_at FROM cb_job_extend($1, $2) e`,
			w.queues, w.name)
		if err != nil {
			return false, wrapErr(err)
		}
		exts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct {
			RunID   int64
			StepID  int64
			LeaseAt time.Time
		}])
		if err != nil {
			return false, wrapErr(err)
		}
		clear(held)
		for _, e := range exts {
			held[stepKey{e.RunID, e.StepID}] = e.LeaseAt
		}
		_, ok := held[k]
		return ok, nil
	}

	// Whatever ends this cycle early — shutdown or an infrastructure
	// error — must hand the unstarted steps back: the next cycle's extends
	// would otherwise keep leases alive on steps nobody is going to run.
	releaseFrom := func(i int) {
		rctx := context.WithoutCancel(ctx)
		for _, s := range steps[i:] {
			if _, ok := held[stepKey{s.RunID, s.StepID}]; !ok {
				continue
			}
			_, _ = w.pool.Exec(rctx, `SELECT cb_job_release($1, $2, $3)`,
				s.RunID, s.StepID, w.name)
		}
	}

	for i, s := range steps {
		k := stepKey{s.RunID, s.StepID}
		if ctx.Err() != nil {
			releaseFrom(i)
			return true, ctx.Err()
		}
		lease, ok := held[k]
		if !ok {
			continue // no longer ours; the new owner takes it from here
		}

		handle, ok := w.handlers[s.Name]
		if !ok {
			delete(held, k)
			if _, err := w.pool.Exec(ctx, `SELECT cb_job_release($1, $2, $3, $4)`,
				s.RunID, s.StepID, w.name, noHandlerPause); err != nil {
				releaseFrom(i + 1)
				return true, wrapErr(err)
			}
			continue
		}

		// earlier steps in the batch may have eaten into this lease
		ttl := s.LeaseAt.Sub(claimedAt)
		if time.Until(lease) < ttl/2 {
			still, err := extendAll(k)
			if err != nil {
				releaseFrom(i)
				return true, err
			}
			if !still {
				continue
			}
		}

		var name *string
		var input, signalInput json.RawMessage
		var attempt *int
		if err := w.pool.QueryRow(ctx,
			`SELECT s.name, s.input, s.signal_input, s.attempt FROM cb_job_start($1, $2, $3) s`,
			s.RunID, s.StepID, w.name).Scan(&name, &input, &signalInput, &attempt); err != nil {
			releaseFrom(i)
			return true, wrapErr(err)
		}
		if name == nil {
			// superseded, or the engine gave the step up instead of starting it
			delete(held, k)
			continue
		}

		var output json.RawMessage
		var plan *Plan
		verdict, err := claimloop.Handle(ctx, ttl/2,
			func() (bool, error) { return extendAll(k) },
			func(elapsed time.Duration) {
				w.logger.Info("catbird: handler still running",
					"job", s.Name, "run_id", s.RunID, "step_id", s.StepID,
					"elapsed", elapsed)
			},
			func(hctx context.Context) error {
				plan = w.newPlan(hctx, s.RunID, signalInput)
				var herr error
				output, herr = handle(hctx, plan, input)
				return herr
			})

		delete(held, k)
		switch {
		case errors.Is(err, claimloop.ErrLost):
			// taken over, canceled or given up; a late report would hit
			// the fence and change nothing, so none is sent
		case err != nil:
			releaseFrom(i + 1)
			return true, err // extends cannot be delivered; not the step's fault
		case ctx.Err() != nil:
			// graceful shutdown: the start is already spent, so report a
			// verdict — redelivery is backoff-paced instead of waiting out
			// the lease, and the attempt row records why
			rctx := context.WithoutCancel(ctx)
			if verdict == nil {
				_ = w.complete(rctx, s, *attempt, output, plan)
			} else {
				_ = w.fail(rctx, s, *attempt, shutdownMessage(verdict))
			}
			releaseFrom(i + 1)
			return true, ctx.Err()
		case verdict == nil:
			if err := w.complete(ctx, s, *attempt, output, plan); err != nil {
				releaseFrom(i + 1)
				return true, err
			}
		default:
			if err := w.fail(ctx, s, *attempt, verdict.Error()); err != nil {
				releaseFrom(i + 1)
				return true, err
			}
		}
	}
	return true, nil
}

// complete reports the successful attempt, with the steps and run output
// the Plan buffered. When the engine refuses a buffered step, the refusal
// is the handler's own bug: it is recorded through cb_job_fail like any
// handler error, so the normal attempt budget applies.
func (w *Worker) complete(ctx context.Context, s claimedStep, attempt int, output json.RawMessage, p *Plan) error {
	var steps, runOutput json.RawMessage
	if p != nil {
		if len(p.steps) > 0 {
			b, err := json.Marshal(p.steps)
			if err != nil {
				return err
			}
			steps = b
		}
		runOutput = p.runOutput
	}
	var applied bool
	err := wrapErr(w.pool.QueryRow(ctx, `SELECT cb_job_complete($1, $2, $3, $4, $5, $6)`,
		s.RunID, s.StepID, attempt, output, steps, runOutput).Scan(&applied))
	if msg, ok := invalidStep(err); ok {
		return w.fail(ctx, s, attempt, msg)
	}
	return err
}

// invalidStep reports whether the completion failed because a step the
// handler added was invalid — it names an undefined job, misses a wait
// key, or reuses the name of a still-unresolved signal-waiting step — and
// gives the message to record on the attempt.
func invalidStep(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if errors.Is(err, ErrInvalid) || errors.Is(err, ErrNotDefined) {
		return err.Error(), true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		pgErr.ConstraintName == "cb_job_steps_signal_name_idx" {
		msg := "catbird: a signal-waiting step with this name is already unresolved in this run"
		if pgErr.Detail != "" {
			msg += " (" + pgErr.Detail + ")"
		}
		return msg, true
	}
	return "", false
}

func (w *Worker) fail(ctx context.Context, s claimedStep, attempt int, errMsg string) error {
	var applied bool
	err := w.pool.QueryRow(ctx, `SELECT cb_job_fail($1, $2, $3, $4)`,
		s.RunID, s.StepID, attempt, errMsg).Scan(&applied)
	return wrapErr(err)
}

// shutdownMessage words the verdict of a handler that ended during
// shutdown: one that stopped because its context was canceled failed by
// the worker's hand, not its own.
func shutdownMessage(verdict error) string {
	if errors.Is(verdict, context.Canceled) {
		return "catbird: worker shutdown"
	}
	return verdict.Error()
}
