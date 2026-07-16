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
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/claimloop"
)

const (
	// fallback poll: the notify listener is the primary wake signal
	workerPoll = 2 * time.Second
	// hand-back pause for a step the worker has no handler for, so two
	// not-yet-updated workers do not pass it back and forth as fast as
	// they can during a rolling deploy
	noHandlerPause = 5 * time.Second
)

// Worker claims and executes steps: register a handler per job with
// Handle, then Start. Which queues the worker claims is read from cb_jobs
// at startup — the routing authority steps are stamped from.
type Worker struct {
	pool     *pgxpool.Pool
	name     string
	logger   *slog.Logger
	handlers map[string]handlerFunc
	queues   []string
	err      error
}

func NewWorker(pool *pgxpool.Pool) *Worker {
	return &Worker{
		pool:     pool,
		name:     claimloop.Name("worker"),
		logger:   slog.Default(),
		handlers: make(map[string]handlerFunc),
	}
}

// Handle registers the handler for a job. fn must have signature
// func(ctx context.Context, in In) (Out, error); a wrong shape surfaces as
// an error from Start.
func (w *Worker) Handle(job string, fn any) {
	h, err := newHandler(fn)
	if err != nil {
		if w.err == nil {
			w.err = fmt.Errorf("catbird: job %s: %w", job, err)
		}
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

	var schema string
	if err := w.pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return err
	}

	n := &notifier{
		pool:   w.pool,
		logger: w.logger,
		wake:   make(chan struct{}, 1),
	}
	for _, q := range w.queues {
		n.channels = append(n.channels, schema+".cbq_"+q)
	}

	nctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	wg.Go(func() {
		n.run(nctx)
	})

	return claimloop.Run(ctx, claimloop.Options{
		Poll: workerPoll,
		Wake: n.wake,
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
		var input, signal json.RawMessage
		var attempt *int
		if err := w.pool.QueryRow(ctx,
			`SELECT s.name, s.input, s.signal, s.attempt FROM cb_job_start($1, $2, $3) s`,
			s.RunID, s.StepID, w.name).Scan(&name, &input, &signal, &attempt); err != nil {
			releaseFrom(i)
			return true, wrapErr(err)
		}
		if name == nil {
			// superseded, or the engine gave the step up instead of starting it
			delete(held, k)
			continue
		}
		_ = signal // the payload of a signal-gated step; arrives with M4b

		var output json.RawMessage
		verdict, err := claimloop.Handle(ctx, ttl/2,
			func() (bool, error) { return extendAll(k) },
			func(elapsed time.Duration) {
				w.logger.Info("catbird: handler still running",
					"job", s.Name, "run_id", s.RunID, "step_id", s.StepID,
					"elapsed", elapsed)
			},
			func(hctx context.Context) error {
				var herr error
				output, herr = handle(hctx, input)
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
				_ = w.complete(rctx, s, *attempt, output)
			} else {
				_ = w.fail(rctx, s, *attempt, shutdownError(verdict))
			}
			releaseFrom(i + 1)
			return true, ctx.Err()
		case verdict == nil:
			if err := w.complete(ctx, s, *attempt, output); err != nil {
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

func (w *Worker) complete(ctx context.Context, s claimedStep, attempt int, output json.RawMessage) error {
	var applied bool
	err := w.pool.QueryRow(ctx, `SELECT cb_job_complete($1, $2, $3, $4)`,
		s.RunID, s.StepID, attempt, output).Scan(&applied)
	return wrapErr(err)
}

func (w *Worker) fail(ctx context.Context, s claimedStep, attempt int, errMsg string) error {
	var applied bool
	err := w.pool.QueryRow(ctx, `SELECT cb_job_fail($1, $2, $3, $4)`,
		s.RunID, s.StepID, attempt, errMsg).Scan(&applied)
	return wrapErr(err)
}

// shutdownError words the verdict of a handler that ended during shutdown:
// one that stopped because its context was canceled failed by the
// worker's hand, not its own.
func shutdownError(verdict error) string {
	if errors.Is(verdict, context.Canceled) {
		return "catbird: worker shutdown"
	}
	return verdict.Error()
}
