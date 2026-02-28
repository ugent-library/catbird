package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type WorkerInfo struct {
	ID              string             `json:"id"`
	TaskHandlers    []*TaskHandlerInfo `json:"task_handlers"`
	StepHandlers    []*StepHandlerInfo `json:"step_handlers"`
	StartedAt       time.Time          `json:"started_at"`
	LastHeartbeatAt time.Time          `json:"last_heartbeat_at"`
}

type TaskHandlerInfo struct {
	TaskName string `json:"task_name"`
}

type StepHandlerInfo struct {
	FlowName string `json:"flow_name"`
	StepName string `json:"step_name"`
}

type taskRunScope struct {
	conn   Conn
	name   string
	runID  int64
	cancel context.CancelFunc
}

type flowRunScope struct {
	conn   Conn
	name   string
	runID  int64
	cancel context.CancelFunc
}

type taskRunScopeContextKey struct{}
type flowRunScopeContextKey struct{}

func withTaskRunScope(ctx context.Context, conn Conn, taskName string, runID int64, cancel context.CancelFunc) context.Context {
	return context.WithValue(ctx, taskRunScopeContextKey{}, &taskRunScope{
		conn:   conn,
		name:   taskName,
		runID:  runID,
		cancel: cancel,
	})
}

func withFlowRunScope(ctx context.Context, conn Conn, flowName string, runID int64, cancel context.CancelFunc) context.Context {
	return context.WithValue(ctx, flowRunScopeContextKey{}, &flowRunScope{
		conn:   conn,
		name:   flowName,
		runID:  runID,
		cancel: cancel,
	})
}

// ListWorkers returns all registered workers.
func ListWorkers(ctx context.Context, conn Conn) ([]*WorkerInfo, error) {
	q := `SELECT id, started_at, last_heartbeat_at, task_handlers, step_handlers FROM cb_worker_info ORDER BY last_heartbeat_at DESC;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleWorker)

}

// Worker processes tasks and flows from the queue
type Worker struct {
	id              string
	conn            Conn
	logger          *slog.Logger
	tasks           []*Task
	flows           []*Flow
	shutdownTimeout time.Duration
}

// WorkerOpts is a configuration struct for creating workers
type WorkerOpts struct {
	Logger          *slog.Logger
	ShutdownTimeout time.Duration
}

// NewWorker creates a new worker with the given connection and configuration.
// Use builder methods (AddTask, AddFlow, etc.) to configure the worker.
// Call Start(ctx) to begin processing tasks and flows.
func NewWorker(conn Conn, opts ...WorkerOpts) *Worker {
	var resolved WorkerOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	logger := resolved.Logger
	if logger == nil {
		logger = slog.Default()
	}

	shutdownTimeout := resolved.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = 5 * time.Second
	}

	return &Worker{
		id:              uuid.NewString(),
		conn:            conn,
		logger:          logger,
		shutdownTimeout: shutdownTimeout,
	}
}

// AddTask registers a task with the worker.
func (w *Worker) AddTask(t *Task) *Worker {
	w.tasks = append(w.tasks, t)
	return w
}

// AddFlow registers a flow with the worker.
func (w *Worker) AddFlow(f *Flow) *Worker {
	w.flows = append(w.flows, f)
	return w
}

// Start begins processing tasks and flows.
//
// The worker will:
//   - poll for new work and execute task and flow step handlers while ctx is active
//   - run any configured cron-style task and flow schedules
//   - send periodic heartbeats while it is running
//   - register built-in garbage collection task running every 5 minutes
//
// Shutdown behaviour:
//   - when ctx is cancelled the worker immediately stops reading new work and
//     begins shutting down
//   - if ShutdownTimeout is set to a value > 0, that duration is used as a
//     grace period for in‑flight handlers after ctx is cancelled; once the
//     grace period expires the handler context is cancelled and remaining
//     handlers are asked to stop. The default graceful shutdown timeout is 5 seconds.
//   - if ShutdownTimeout is not set or set to 0, there is no grace period:
//     the handler context is cancelled immediately once ctx is cancelled and
//     Start returns after all goroutines finish
func (w *Worker) Start(ctx context.Context) error {
	// Validate HandlerOpts for all tasks
	for _, t := range w.tasks {
		if t.handlerOpts != nil {
			if err := t.handlerOpts.validate(); err != nil {
				return fmt.Errorf("task %q has invalid handler options: %w", t.name, err)
			}
		}
		if t.onFailOpts != nil {
			if err := t.onFailOpts.validate(); err != nil {
				return fmt.Errorf("task %q has invalid on-fail options: %w", t.name, err)
			}
		}
	}

	// Validate HandlerOpts for all flow steps
	for _, f := range w.flows {
		if f.onFailOpts != nil {
			if err := f.onFailOpts.validate(); err != nil {
				return fmt.Errorf("flow %q has invalid on-fail options: %w", f.name, err)
			}
		}
		for _, s := range f.steps {
			if s.handlerOpts != nil {
				if err := s.handlerOpts.validate(); err != nil {
					return fmt.Errorf("flow %q step %q has invalid handler options: %w", f.name, s.name, err)
				}
			}
		}
	}

	// Create tasks in database
	if err := CreateTask(ctx, w.conn, w.tasks...); err != nil {
		return err
	}

	// Create flows in database
	if err := CreateFlow(ctx, w.conn, w.flows...); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var taskHandlers = make([]*TaskHandlerInfo, 0)
	var stepHandlers = make([]*StepHandlerInfo, 0)

	// handlerCtx is used for in-flight handler execution so that when the
	// worker's context is cancelled we can stop reading new work while still
	// giving existing handlers a grace period to finish.
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	defer handlerCancel()

	wg.Go(func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := w.conn.Exec(ctx, `SELECT * FROM cb_worker_heartbeat(id => $1);`, w.id); err != nil {
					w.logger.ErrorContext(ctx, "worker: cannot send heartbeat", "error", err)
				}
			}
		}
	})

	// Start task workers
	for _, t := range w.tasks {
		if t.handlerOpts != nil {
			taskHandlers = append(taskHandlers, &TaskHandlerInfo{TaskName: t.name})
			worker := newTaskWorker(w.conn, w.logger, t)
			worker.start(ctx, handlerCtx, &wg)
		}
		if t.onFailOpts != nil {
			onFailWorker := newTaskOnFailWorker(w.conn, w.logger, t)
			onFailWorker.start(ctx, handlerCtx, &wg)
		}
	}

	// Start step workers
	for _, f := range w.flows {
		if f.onFailOpts != nil {
			onFailWorker := newFlowOnFailWorker(w.conn, w.logger, f)
			onFailWorker.start(ctx, handlerCtx, &wg)
		}
		for _, s := range f.steps {
			if s.handlerOpts != nil {
				stepHandlers = append(stepHandlers, &StepHandlerInfo{FlowName: f.name, StepName: s.name})

				if s.isGenerator {
					newStepWorker(w.conn, w.logger, f.name, &s).start(ctx, handlerCtx, &wg)
					newMapStepWorker(w.conn, w.logger, f.name, &s).start(ctx, handlerCtx, &wg)
					continue
				}

				if s.isMapStep && s.reducerFn != nil {
					newStepWorker(w.conn, w.logger, f.name, &s).start(ctx, handlerCtx, &wg)
					newMapStepWorker(w.conn, w.logger, f.name, &s).start(ctx, handlerCtx, &wg)
					continue
				}

				if s.isMapStep {
					newMapStepWorker(w.conn, w.logger, f.name, &s).start(ctx, handlerCtx, &wg)
				} else {
					newStepWorker(w.conn, w.logger, f.name, &s).start(ctx, handlerCtx, &wg)
				}
			}
		}
	}

	// Start schedule polling goroutine for all schedules (both tasks and flows)
	wg.Go(func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Poll for due task schedules and execute them
				var taskCount int
				if err := w.conn.QueryRow(ctx,
					`SELECT cb_execute_due_task_schedules(array(SELECT task_name FROM cb_task_schedules WHERE enabled), 32)`,
				).Scan(&taskCount); err != nil {
					// Ignore errors - likely no schedules exist yet
				}

				// Poll for due flow schedules and execute them
				var flowCount int
				if err := w.conn.QueryRow(ctx,
					`SELECT cb_execute_due_flow_schedules(array(SELECT flow_name FROM cb_flow_schedules WHERE enabled), 32)`,
				).Scan(&flowCount); err != nil {
					// Ignore errors - likely no schedules exist yet
				}
			}
		}
	})

	tb, err := json.Marshal(taskHandlers)
	if err != nil {
		return err
	}
	sb, err := json.Marshal(stepHandlers)
	if err != nil {
		return err
	}
	if _, err := w.conn.Exec(ctx, `SELECT * FROM cb_worker_started(id => $1, task_handlers => coalesce($2, '[]'::jsonb), step_handlers => coalesce($3, '[]'::jsonb));`, w.id, tb, sb); err != nil {
		return fmt.Errorf("worker: cannot mark as started: %w", err)
	}

	return w.waitForShutdown(ctx, handlerCancel, &wg)
}

// waitForShutdown blocks until all worker goroutines have finished.
// When ctx is cancelled it optionally gives handlers a grace period according
// to w.shutdownTimeout before cancelling the handler context.
func (w *Worker) waitForShutdown(ctx context.Context, handlerCancel context.CancelFunc, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// Shutdown requested. If a graceful shutdown timeout is configured, give
		// handlers a grace period to finish. Otherwise cancel them
		// immediately.
		if w.shutdownTimeout > 0 {
			select {
			case <-done:
				return nil
			case <-time.After(w.shutdownTimeout):
				handlerCancel()
				<-done
				return nil
			}
		}

		// No grace period configured: cancel handlers right away and wait
		// for all goroutines to finish.
		handlerCancel()
		<-done
		return nil
	}
}

func scanCollectibleWorker(row pgx.CollectableRow) (*WorkerInfo, error) {
	return scanWorker(row)
}

func scanWorker(row pgx.Row) (*WorkerInfo, error) {
	rec := WorkerInfo{}

	var taskHandlers json.RawMessage
	var stepHandlers json.RawMessage

	if err := row.Scan(
		&rec.ID,
		&rec.StartedAt,
		&rec.LastHeartbeatAt,
		&taskHandlers,
		&stepHandlers,
	); err != nil {
		return nil, err
	}

	if taskHandlers != nil {
		if err := json.Unmarshal(taskHandlers, &rec.TaskHandlers); err != nil {
			return nil, err
		}
	}
	if stepHandlers != nil {
		if err := json.Unmarshal(stepHandlers, &rec.StepHandlers); err != nil {
			return nil, err
		}
	}

	return &rec, nil
}

// taskWorker processes one task type
type taskWorker struct {
	conn   Conn
	logger *slog.Logger
	task   *Task

	tracker *inFlightTracker
}

func newTaskWorker(conn Conn, logger *slog.Logger, task *Task) *taskWorker {
	return &taskWorker{
		conn:    conn,
		logger:  logger,
		task:    task,
		tracker: newInFlightTracker(),
	}
}

func (w *taskWorker) start(shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup) {
	startInFlightHider(handlerCtx, wg, 30*time.Second, w.hideInFlight)

	// Start consumers
	h := w.task.handlerOpts
	if h == nil {
		w.logger.WarnContext(shutdownCtx, "task handler has no options (definition-only)", "task", w.task.name)
		return
	}

	runClaimLoop(shutdownCtx, handlerCtx, wg, claimLoopConfig[taskClaim]{
		concurrency: h.Concurrency,
		pollClaims:  w.pollClaims,
		handleClaim: w.handle,
		onPolled:    w.markInFlight,
		logPollError: func(ctx context.Context, err error) {
			w.logger.ErrorContext(ctx, "worker: cannot poll task claims", "task", w.task.name, "error", err)
		},
	})
}

func (w *taskWorker) markInFlight(msgs []taskClaim) {
	markClaimsInFlight(w.tracker, msgs, func(msg taskClaim) int64 { return msg.ID })
}

func (w *taskWorker) removeInFlight(id int64) {
	w.tracker.remove(id)
}

func (w *taskWorker) hideInFlight(ctx context.Context) {
	ids := w.tracker.list()
	if len(ids) == 0 {
		return
	}
	hideTaskRuns(ctx, w.conn, w.logger, w.task.name, ids, (10 * time.Minute).Milliseconds(), "worker: cannot hide in-flight tasks")
}

func (w *taskWorker) pollClaims(ctx context.Context) ([]taskClaim, error) {
	h := w.task.handlerOpts
	if h == nil {
		return nil, nil
	}

	q := `SELECT id, attempts, input FROM cb_poll_tasks(name => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.task.name, h.BatchSize, (10 * time.Minute).Milliseconds(), defaultPollFor.Milliseconds(), defaultPollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleTaskClaim)
}

func (w *taskWorker) handle(ctx context.Context, msg taskClaim) {
	defer w.removeInFlight(msg.ID)

	claimCtx, claimCancel := context.WithCancel(ctx)
	defer claimCancel()

	handlerCtx := withTaskRunScope(claimCtx, w.conn, w.task.name, msg.ID, claimCancel)

	isCanceled := func() bool {
		requested, err := isTaskCancelRequestedBestEffort(ctx, w.conn, w.task.name, msg.ID)
		if err == nil && requested {
			claimCancel()
			return true
		}
		return false
	}

	h := w.task.handlerOpts
	if h == nil {
		w.logger.ErrorContext(ctx, "worker: failed", "task", w.task.name, "error", "no handler options (definition-only)")
		return
	}

	if isCanceled() {
		return
	}

	if h.CircuitBreaker != nil {
		allowed, delay := h.CircuitBreaker.Allow(time.Now())
		if !allowed {
			if delay <= 0 {
				delay = time.Second
			}
			w.logger.WarnContext(ctx, "worker: circuit breaker open", "task", w.task.name, "retry_in", delay)
			delayMs := ensurePositiveDelayMs(delay)
			hideTaskRuns(ctx, w.conn, w.logger, w.task.name, []int64{msg.ID}, delayMs, "worker: cannot delay task due to open circuit")
			return
		}
	}

	w.logger.DebugContext(handlerCtx, "worker: handleTask",
		"task", w.task.name,
		"id", msg.ID,
		"attempts", msg.Attempts,
		"input", string(msg.Input),
	)

	out, err := runWithTimeout(handlerCtx, h.Timeout, func(fnCtx context.Context) ([]byte, error) {
		return runSafely("task handler panic", func() ([]byte, error) {
			inputJSON, marshalErr := json.Marshal(msg.Input)
			if marshalErr != nil {
				return nil, fmt.Errorf("marshal input: %w", marshalErr)
			}
			return w.task.handler(fnCtx, inputJSON)
		})
	})

	// Handle result
	if err != nil {
		if cancelRequested, cancelErr := isTaskCancelRequestedBestEffort(ctx, w.conn, w.task.name, msg.ID); cancelErr == nil && cancelRequested {
			return
		}

		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordFailure(time.Now())
		}
		w.logger.ErrorContext(ctx, "worker: failed", "task", w.task.name, "error", err)

		if msg.Attempts > h.MaxRetries {
			failTaskRun(ctx, w.conn, w.logger, w.task.name, msg.ID, err.Error())
		} else {
			delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
			hideTaskRuns(ctx, w.conn, w.logger, w.task.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay next task run")
		}
	} else {
		if isCanceled() {
			return
		}
		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordSuccess()
		}
		completeTaskRun(ctx, w.conn, w.logger, w.task.name, msg.ID, out)
	}
}

func scanCollectibleTaskClaim(row pgx.CollectableRow) (taskClaim, error) {
	return scanTaskClaim(row)
}

func scanTaskClaim(row pgx.Row) (taskClaim, error) {
	rec := taskClaim{}

	if err := row.Scan(
		&rec.ID,
		&rec.Attempts,
		&rec.Input,
	); err != nil {
		return rec, err
	}

	return rec, nil
}

// stepWorker processes one flow step
type stepWorker struct {
	conn     Conn
	logger   *slog.Logger
	flowName string
	step     *Step

	tracker *inFlightTracker
}

func newStepWorker(conn Conn, logger *slog.Logger, flowName string, step *Step) *stepWorker {
	return &stepWorker{
		conn:     conn,
		logger:   logger,
		flowName: flowName,
		step:     step,
		tracker:  newInFlightTracker(),
	}
}

func (w *stepWorker) start(shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup) {
	startInFlightHider(handlerCtx, wg, 30*time.Second, w.hideInFlight)

	// Start consumers
	h := w.step.handlerOpts

	runClaimLoop(shutdownCtx, handlerCtx, wg, claimLoopConfig[stepClaim]{
		concurrency: h.Concurrency,
		pollClaims:  w.pollClaims,
		handleClaim: w.handle,
		onPolled:    w.markInFlight,
		logPollError: func(ctx context.Context, err error) {
			w.logger.ErrorContext(ctx, "worker: cannot poll step claims", "flow", w.flowName, "step", w.step.name, "error", err)
		},
	})
}

func (w *stepWorker) markInFlight(msgs []stepClaim) {
	markClaimsInFlight(w.tracker, msgs, func(msg stepClaim) int64 { return msg.ID })
}

func (w *stepWorker) removeInFlight(id int64) {
	w.tracker.remove(id)
}

func (w *stepWorker) hideInFlight(ctx context.Context) {
	ids := w.tracker.list()
	if len(ids) == 0 {
		return
	}
	hideStepRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, ids, (10 * time.Minute).Milliseconds(), "worker: cannot hide in-flight steps")
}

func (w *stepWorker) pollClaims(ctx context.Context) ([]stepClaim, error) {
	h := w.step.handlerOpts

	q := `SELECT id, flow_run_id, attempts, input, step_outputs, signal_input FROM cb_poll_steps(flow_name => $1, step_name => $2, quantity => $3, hide_for => $4, poll_for => $5, poll_interval => $6);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.flowName, w.step.name, h.BatchSize, (10 * time.Minute).Milliseconds(), defaultPollFor.Milliseconds(), defaultPollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleStepClaim)
}

func (w *stepWorker) handle(ctx context.Context, msg stepClaim) {
	defer w.removeInFlight(msg.ID)

	h := w.step.handlerOpts
	flowRunID := msg.FlowRunID
	cancelRequested := false
	var stopRequested atomic.Bool

	claimCtx, claimCancel := context.WithCancel(ctx)
	defer claimCancel()

	handlerCtx := withFlowRunScope(claimCtx, w.conn, w.flowName, flowRunID, claimCancel)
	defer finalizeFlowCancelIfRequested(ctx, w.conn, w.logger, w.flowName, flowRunID, &cancelRequested)

	stopIfRequested := func() bool {
		if stopRequested.Load() {
			return true
		}

		if stopFlowIfRequested(ctx, w.conn, w.logger, w.flowName, flowRunID, claimCancel, &cancelRequested, func() {
			cancelStepRun(ctx, w.conn, w.logger, w.flowName, msg.ID)
		}) {
			stopRequested.Store(true)
			return true
		}

		return false
	}

	if stopIfRequested() {
		return
	}

	stopWatcher := watchFlowStop(claimCtx, 500*time.Millisecond, stopIfRequested)
	defer stopWatcher()

	if h.CircuitBreaker != nil {
		allowed, delay := h.CircuitBreaker.Allow(time.Now())
		if !allowed {
			if delay <= 0 {
				delay = time.Second
			}
			w.logger.WarnContext(ctx, "worker: circuit breaker open", "flow", w.flowName, "step", w.step.name, "retry_in", delay)
			hideStepRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay step due to open circuit")
			return
		}
	}

	if w.logger.Enabled(handlerCtx, slog.LevelDebug) {
		stepOutputsJSON, _ := json.Marshal(msg.StepOutputs)
		w.logger.DebugContext(handlerCtx, "worker: handleStep",
			"flow", w.flowName,
			"step", w.step.name,
			"id", msg.ID,
			"attempts", msg.Attempts,
			"input", string(msg.Input),
			"step_outputs", string(stepOutputsJSON),
		)
	}

	if w.step.isMapStep && w.step.reducerFn != nil {
		if stopIfRequested() {
			return
		}

		err := runWithTimeoutErr(handlerCtx, h.Timeout, func(fnCtx context.Context) error {
			_, reduceErr := runSafely("reducer panic", func() (struct{}, error) {
				return struct{}{}, finalizeStepReduction(fnCtx, w.conn, w.logger, w.flowName, w.step, msg.ID)
			})
			return reduceErr
		})

		if err != nil {
			if stopIfRequested() {
				return
			}

			if h.CircuitBreaker != nil {
				h.CircuitBreaker.RecordFailure(time.Now())
			}

			if msg.Attempts > h.MaxRetries {
				failStepRun(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, err.Error())
			} else {
				delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
				hideStepRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay reduced map step retry")
			}
			return
		}

		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordSuccess()
		}
		return
	}

	if w.step.isGenerator {
		if stopIfRequested() {
			return
		}

		if w.step.reducerFn != nil {
			reductionReady, reductionReadyErr := generatorReductionReady(ctx, w.conn, w.flowName, w.step.name, msg.ID)
			if reductionReadyErr != nil {
				if h.CircuitBreaker != nil {
					h.CircuitBreaker.RecordFailure(time.Now())
				}
				w.logger.ErrorContext(ctx, "worker: reducer readiness check failed", "flow", w.flowName, "step", w.step.name, "error", reductionReadyErr)
				if msg.Attempts > h.MaxRetries {
					failGeneratorStep(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, reductionReadyErr.Error())
				} else {
					delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
					hideStepRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay reducer readiness retry")
				}
				return
			}

			if reductionReady {
				err := runWithTimeoutErr(ctx, h.Timeout, func(fnCtx context.Context) error {
					_, reduceErr := runSafely("reducer panic", func() (struct{}, error) {
						return struct{}{}, finalizeStepReduction(fnCtx, w.conn, w.logger, w.flowName, w.step, msg.ID)
					})
					return reduceErr
				})

				if err != nil {
					if h.CircuitBreaker != nil {
						h.CircuitBreaker.RecordFailure(time.Now())
					}
					if msg.Attempts > h.MaxRetries {
						failGeneratorStep(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, err.Error())
					} else {
						delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
						hideStepRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay reduced generator step retry")
					}
					return
				}

				if h.CircuitBreaker != nil {
					h.CircuitBreaker.RecordSuccess()
				}
				return
			}
		}

		err := runWithTimeoutErr(handlerCtx, h.Timeout, func(fnCtx context.Context) error {
			_, genErr := runSafely("generator handler panic", func() (struct{}, error) {
				return struct{}{}, w.handleGeneratorClaim(fnCtx, msg)
			})
			return genErr
		})

		if err != nil {
			if stopIfRequested() {
				return
			}

			if h.CircuitBreaker != nil {
				h.CircuitBreaker.RecordFailure(time.Now())
			}
			w.logger.ErrorContext(ctx, "worker: generator failed", "flow", w.flowName, "step", w.step.name, "error", err)

			if msg.Attempts > h.MaxRetries {
				failGeneratorStep(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, err.Error())
			} else {
				delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
				hideStepRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay generator step retry")
			}
			return
		}

		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordSuccess()
		}
		return
	}

	out, err := runWithTimeout(handlerCtx, h.Timeout, func(fnCtx context.Context) ([]byte, error) {
		return runSafely("step handler panic", func() ([]byte, error) {
			inputJSON, marshalErr := json.Marshal(msg.Input)
			if marshalErr != nil {
				return nil, fmt.Errorf("marshal input: %w", marshalErr)
			}
			signalInputJSON, marshalErr := json.Marshal(msg.SignalInput)
			if marshalErr != nil {
				return nil, fmt.Errorf("marshal signal input: %w", marshalErr)
			}
			depsJSON := make(map[string][]byte)
			for name, depOut := range msg.StepOutputs {
				depJSON, marshalErr := json.Marshal(depOut)
				if marshalErr != nil {
					return nil, fmt.Errorf("marshal dependency %s output: %w", name, marshalErr)
				}
				depsJSON[name] = depJSON
			}
			if w.step.handler == nil {
				return nil, fmt.Errorf("step %s has no handler (definition-only)", w.step.name)
			}
			return w.step.handler(fnCtx, inputJSON, depsJSON, signalInputJSON)
		})
	})

	// Handle result
	if err != nil {
		if completion, ok := asEarlyCompletion(err); ok {
			if completion.flowName != w.flowName || completion.flowRunID != flowRunID {
				w.logger.ErrorContext(ctx, "worker: early completion scope mismatch", "flow", w.flowName, "step", w.step.name)
				return
			}

			changed, status, completeErr := completeFlowEarly(ctx, w.conn, w.flowName, flowRunID, w.step.name, completion.output, completion.reason)
			if completeErr != nil {
				w.logger.ErrorContext(ctx, "worker: cannot complete flow early", "flow", w.flowName, "step", w.step.name, "error", completeErr)
				if msg.Attempts > h.MaxRetries {
					failStepRun(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, completeErr.Error())
				} else {
					delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
					hideStepRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay step after early completion error")
				}
				return
			}

			if changed || status == "completed" {
				stopRequested.Store(true)
				claimCancel()
			}
			return
		}

		if stopIfRequested() {
			return
		}

		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordFailure(time.Now())
		}
		w.logger.ErrorContext(ctx, "worker: failed", "flow", w.flowName, "step", w.step.name, "error", err)

		if msg.Attempts > h.MaxRetries {
			failStepRun(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, err.Error())
		} else {
			delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
			hideStepRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay next step run")
		}
	} else {
		if stopIfRequested() {
			return
		}
		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordSuccess()
		}
		completeStepRun(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, out)
	}
}

func (w *stepWorker) handleGeneratorClaim(ctx context.Context, msg stepClaim) error {
	if w.step.generatorFn == nil {
		return fmt.Errorf("step %s has no generator function", w.step.name)
	}

	fnVal := reflect.ValueOf(w.step.generatorFn)
	fnType := fnVal.Type()

	args := make([]reflect.Value, 0, fnType.NumIn())
	args = append(args, reflect.ValueOf(ctx))

	flowInputPtr := reflect.New(fnType.In(1))
	if err := json.Unmarshal(msg.Input, flowInputPtr.Interface()); err != nil {
		return fmt.Errorf("unmarshal flow input: %w", err)
	}
	args = append(args, flowInputPtr.Elem())

	depStartIdx := 2
	if w.step.hasSignal {
		signalPtr := reflect.New(fnType.In(2))
		if len(msg.SignalInput) > 0 {
			if err := json.Unmarshal(msg.SignalInput, signalPtr.Interface()); err != nil {
				return fmt.Errorf("unmarshal signal input: %w", err)
			}
		}
		args = append(args, signalPtr.Elem())
		depStartIdx = 3
	}

	isOptionalType := func(t reflect.Type) bool {
		return t.Kind() == reflect.Struct &&
			t.NumField() >= 2 &&
			t.Field(0).Name == "IsSet" &&
			t.Field(0).Type.Kind() == reflect.Bool &&
			t.Field(1).Name == "Value"
	}

	for idx, depName := range w.step.dependencies {
		paramType := fnType.In(depStartIdx + idx)
		depRaw, ok := msg.StepOutputs[depName]

		if isOptionalType(paramType) {
			optVal := reflect.New(paramType).Elem()
			if ok && len(depRaw) > 0 {
				optVal.FieldByName("IsSet").SetBool(true)
				valueField := optVal.FieldByName("Value")
				valuePtr := reflect.New(valueField.Type())
				if err := json.Unmarshal(depRaw, valuePtr.Interface()); err != nil {
					return fmt.Errorf("unmarshal optional dependency %s: %w", depName, err)
				}
				valueField.Set(valuePtr.Elem())
			}
			args = append(args, optVal)
			continue
		}

		if !ok {
			return fmt.Errorf("step %s missing dependency output %q", w.step.name, depName)
		}

		depPtr := reflect.New(paramType)
		if err := json.Unmarshal(depRaw, depPtr.Interface()); err != nil {
			return fmt.Errorf("unmarshal dependency %s output: %w", depName, err)
		}
		args = append(args, depPtr.Elem())
	}

	batchSize := w.step.handlerOpts.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	zeroErr := reflect.Zero(errorType)
	batch := make([]json.RawMessage, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		items, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("marshal generator batch: %w", err)
		}

		if _, err := spawnGeneratorMapTasks(ctx, w.conn, w.flowName, w.step.name, msg.ID, items); err != nil {
			return err
		}

		batch = batch[:0]
		return nil
	}

	yieldParamType := fnType.In(fnType.NumIn() - 1)
	yieldItemType := yieldParamType.In(0)
	yieldType := reflect.FuncOf([]reflect.Type{yieldItemType}, []reflect.Type{errorType}, false)
	yieldFn := reflect.MakeFunc(yieldType, func(args []reflect.Value) []reflect.Value {
		if err := ctx.Err(); err != nil {
			return []reflect.Value{reflect.ValueOf(err)}
		}

		itemJSON, err := json.Marshal(args[0].Interface())
		if err != nil {
			return []reflect.Value{reflect.ValueOf(fmt.Errorf("marshal yielded item: %w", err))}
		}

		batch = append(batch, itemJSON)
		if len(batch) < batchSize {
			return []reflect.Value{zeroErr}
		}

		if err := flush(); err != nil {
			return []reflect.Value{reflect.ValueOf(err)}
		}

		return []reflect.Value{zeroErr}
	})
	args = append(args, yieldFn)

	results := fnVal.Call(args)

	if !results[0].IsNil() {
		return results[0].Interface().(error)
	}

	if err := flush(); err != nil {
		return err
	}

	if w.step.reducerFn == nil {
		return completeGeneratorStep(ctx, w.conn, w.flowName, w.step.name, msg.ID)
	}

	ready, err := completeGeneratorStepOrQueueReduce(ctx, w.conn, w.flowName, w.step.name, msg.ID)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}

	return finalizeStepReduction(ctx, w.conn, w.logger, w.flowName, w.step, msg.ID)
}

func scanCollectibleStepClaim(row pgx.CollectableRow) (stepClaim, error) {
	return scanStepClaim(row)
}

func scanStepClaim(row pgx.Row) (stepClaim, error) {
	rec := stepClaim{}

	var stepOutputs *map[string]json.RawMessage

	if err := row.Scan(
		&rec.ID,
		&rec.FlowRunID,
		&rec.Attempts,
		&rec.Input,
		&stepOutputs,
		&rec.SignalInput,
	); err != nil {
		return rec, err
	}

	if stepOutputs != nil {
		rec.StepOutputs = *stepOutputs
	}

	return rec, nil
}

type mapTaskClaim struct {
	ID          int64                      `json:"id"`
	FlowRunID   int64                      `json:"flow_run_id"`
	Attempts    int                        `json:"attempts"`
	Input       json.RawMessage            `json:"input"`
	StepOutputs map[string]json.RawMessage `json:"step_outputs"`
	SignalInput json.RawMessage            `json:"signal_input"`
	Item        json.RawMessage            `json:"item"`
}

type mapStepWorker struct {
	conn     Conn
	logger   *slog.Logger
	flowName string
	step     *Step

	tracker *inFlightTracker
}

func newMapStepWorker(conn Conn, logger *slog.Logger, flowName string, step *Step) *mapStepWorker {
	return &mapStepWorker{
		conn:     conn,
		logger:   logger,
		flowName: flowName,
		step:     step,
		tracker:  newInFlightTracker(),
	}
}

func (w *mapStepWorker) start(shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup) {
	startInFlightHider(handlerCtx, wg, 30*time.Second, w.hideInFlight)

	h := w.step.handlerOpts

	runClaimLoop(shutdownCtx, handlerCtx, wg, claimLoopConfig[mapTaskClaim]{
		concurrency: h.Concurrency,
		pollClaims:  w.pollClaims,
		handleClaim: w.handle,
		onPolled:    w.markInFlight,
		logPollError: func(ctx context.Context, err error) {
			w.logger.ErrorContext(ctx, "worker: cannot poll map task claims", "flow", w.flowName, "step", w.step.name, "error", err)
		},
	})
}

func (w *mapStepWorker) markInFlight(msgs []mapTaskClaim) {
	markClaimsInFlight(w.tracker, msgs, func(msg mapTaskClaim) int64 { return msg.ID })
}

func (w *mapStepWorker) removeInFlight(id int64) {
	w.tracker.remove(id)
}

func (w *mapStepWorker) hideInFlight(ctx context.Context) {
	ids := w.tracker.list()
	if len(ids) == 0 {
		return
	}
	hideMapTaskRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, ids, (10 * time.Minute).Milliseconds(), "worker: cannot hide in-flight map tasks")
}

func (w *mapStepWorker) pollClaims(ctx context.Context) ([]mapTaskClaim, error) {
	h := w.step.handlerOpts
	q := `SELECT id, flow_run_id, attempts, input, step_outputs, signal_input, item FROM cb_poll_map_tasks(flow_name => $1, step_name => $2, quantity => $3, hide_for => $4, poll_for => $5, poll_interval => $6);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.flowName, w.step.name, h.BatchSize, (10 * time.Minute).Milliseconds(), defaultPollFor.Milliseconds(), defaultPollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleMapTaskClaim)
}

func (w *mapStepWorker) handle(ctx context.Context, msg mapTaskClaim) {
	defer w.removeInFlight(msg.ID)

	h := w.step.handlerOpts
	flowRunID := msg.FlowRunID
	cancelRequested := false
	var stopRequested atomic.Bool

	claimCtx, claimCancel := context.WithCancel(ctx)
	defer claimCancel()

	handlerCtx := withFlowRunScope(claimCtx, w.conn, w.flowName, flowRunID, claimCancel)
	defer finalizeFlowCancelIfRequested(ctx, w.conn, w.logger, w.flowName, flowRunID, &cancelRequested)

	stopIfRequested := func() bool {
		if stopRequested.Load() {
			return true
		}

		if stopFlowIfRequested(ctx, w.conn, w.logger, w.flowName, flowRunID, claimCancel, &cancelRequested, func() {
			cancelMapTaskRun(ctx, w.conn, w.logger, w.flowName, msg.ID)
		}) {
			stopRequested.Store(true)
			return true
		}

		return false
	}

	if stopIfRequested() {
		return
	}

	stopWatcher := watchFlowStop(claimCtx, 500*time.Millisecond, stopIfRequested)
	defer stopWatcher()

	if h.CircuitBreaker != nil {
		allowed, delay := h.CircuitBreaker.Allow(time.Now())
		if !allowed {
			if delay <= 0 {
				delay = time.Second
			}
			hideMapTaskRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay map task due to open circuit")
			return
		}
	}

	itemOutput, err := runWithTimeout(handlerCtx, h.Timeout, func(fnCtx context.Context) (json.RawMessage, error) {
		return runSafely("step handler panic", func() (json.RawMessage, error) {
			if w.step.isGenerator {
				if w.step.generatorHandler == nil {
					return nil, fmt.Errorf("step %s has no generator handler", w.step.name)
				}

				itemPtr := reflect.New(w.step.generatorItemType)
				if err := json.Unmarshal(msg.Item, itemPtr.Interface()); err != nil {
					return nil, fmt.Errorf("unmarshal generator item: %w", err)
				}

				results := reflect.ValueOf(w.step.generatorHandler).Call([]reflect.Value{
					reflect.ValueOf(fnCtx),
					itemPtr.Elem(),
				})
				if !results[1].IsNil() {
					return nil, results[1].Interface().(error)
				}

				outJSON, err := json.Marshal(results[0].Interface())
				if err != nil {
					return nil, fmt.Errorf("marshal generator handler output: %w", err)
				}

				return outJSON, nil
			}

			flowInputJSON, marshalErr := json.Marshal(msg.Input)
			if marshalErr != nil {
				return nil, fmt.Errorf("marshal flow input: %w", marshalErr)
			}

			signalInputJSON, marshalErr := json.Marshal(msg.SignalInput)
			if marshalErr != nil {
				return nil, fmt.Errorf("marshal signal input: %w", marshalErr)
			}

			depsJSON := make(map[string][]byte)
			for name, depOut := range msg.StepOutputs {
				depJSON, marshalErr := json.Marshal(depOut)
				if marshalErr != nil {
					return nil, fmt.Errorf("marshal dependency %s output: %w", name, marshalErr)
				}
				depsJSON[name] = depJSON
			}

			singleItemArrayJSON, marshalErr := json.Marshal([]json.RawMessage{msg.Item})
			if marshalErr != nil {
				return nil, fmt.Errorf("marshal map item wrapper: %w", marshalErr)
			}

			if w.step.mapSource == "" {
				flowInputJSON = singleItemArrayJSON
			} else {
				depsJSON[w.step.mapSource] = singleItemArrayJSON
			}

			if w.step.handler == nil {
				return nil, fmt.Errorf("step %s has no handler", w.step.name)
			}

			out, handlerErr := w.step.handler(fnCtx, flowInputJSON, depsJSON, signalInputJSON)
			if handlerErr != nil {
				return nil, handlerErr
			}

			var arr []json.RawMessage
			if unmarshalErr := json.Unmarshal(out, &arr); unmarshalErr != nil {
				return nil, fmt.Errorf("unmarshal map step output array: %w", unmarshalErr)
			}
			if len(arr) != 1 {
				return nil, fmt.Errorf("map step handler must produce exactly one output item per map task, got %d", len(arr))
			}

			return arr[0], nil
		})
	})

	if err != nil {
		if completion, ok := asEarlyCompletion(err); ok {
			if completion.flowName != w.flowName || completion.flowRunID != flowRunID {
				w.logger.ErrorContext(ctx, "worker: early completion scope mismatch", "flow", w.flowName, "step", w.step.name)
				return
			}

			changed, status, completeErr := completeFlowEarly(ctx, w.conn, w.flowName, flowRunID, w.step.name, completion.output, completion.reason)
			if completeErr != nil {
				w.logger.ErrorContext(ctx, "worker: cannot complete flow early", "flow", w.flowName, "step", w.step.name, "error", completeErr)
				if msg.Attempts > h.MaxRetries {
					failMapTaskRun(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, completeErr.Error())
				} else {
					delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
					hideMapTaskRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay map task after early completion error")
				}
				return
			}

			if changed || status == "completed" {
				stopRequested.Store(true)
				claimCancel()
			}
			return
		}

		if stopIfRequested() {
			return
		}

		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordFailure(time.Now())
		}

		if msg.Attempts > h.MaxRetries {
			failMapTaskRun(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, err.Error())
		} else {
			delay := nextRetryDelay(msg.Attempts-1, h.Backoff, 0, 0)
			hideMapTaskRuns(ctx, w.conn, w.logger, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds(), "worker: cannot delay map task retry")
		}
		return
	}

	if h.CircuitBreaker != nil {
		h.CircuitBreaker.RecordSuccess()
	}

	if stopIfRequested() {
		return
	}

	if w.step.reducerFn != nil {
		_, err := completeMapTaskOrQueueReduce(ctx, w.conn, w.flowName, w.step.name, msg.ID, itemOutput)
		if err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark reduced map task as completed", "flow", w.flowName, "step", w.step.name, "error", err)
		}
		return
	}

	completeMapTaskRun(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, itemOutput)
}

func scanCollectibleMapTaskClaim(row pgx.CollectableRow) (mapTaskClaim, error) {
	return scanMapTaskClaim(row)
}

func scanMapTaskClaim(row pgx.Row) (mapTaskClaim, error) {
	rec := mapTaskClaim{}
	var stepOutputs *map[string]json.RawMessage

	if err := row.Scan(
		&rec.ID,
		&rec.FlowRunID,
		&rec.Attempts,
		&rec.Input,
		&stepOutputs,
		&rec.SignalInput,
		&rec.Item,
	); err != nil {
		return rec, err
	}

	if stepOutputs != nil {
		rec.StepOutputs = *stepOutputs
	}

	return rec, nil
}

type claimLoopConfig[C any] struct {
	concurrency    int
	pollClaims     func(context.Context) ([]C, error)
	handleClaim    func(context.Context, C)
	onPolled       func([]C)
	emptyPollDelay time.Duration
	logPollError   func(context.Context, error)
}

func runClaimLoop[C any](shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup, cfg claimLoopConfig[C]) {
	claims := make(chan C)

	wg.Go(func() {
		defer close(claims)

		retryAttempt := 0
		for {
			select {
			case <-shutdownCtx.Done():
				return
			default:
			}

			msgs, err := cfg.pollClaims(shutdownCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				if cfg.logPollError != nil {
					cfg.logPollError(shutdownCtx, err)
				}

				delay := backoffWithFullJitter(retryAttempt, 250*time.Millisecond, 5*time.Second)
				retryAttempt++
				timer := time.NewTimer(delay)
				select {
				case <-shutdownCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				continue
			}

			retryAttempt = 0
			if len(msgs) == 0 {
				if cfg.emptyPollDelay > 0 {
					timer := time.NewTimer(cfg.emptyPollDelay)
					select {
					case <-shutdownCtx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
				continue
			}

			if cfg.onPolled != nil {
				cfg.onPolled(msgs)
			}

			for _, msg := range msgs {
				select {
				case <-shutdownCtx.Done():
					return
				case claims <- msg:
				}
			}
		}
	})

	for i := 0; i < cfg.concurrency; i++ {
		wg.Go(func() {
			for msg := range claims {
				cfg.handleClaim(handlerCtx, msg)
			}
		})
	}
}

func hideTaskRuns(ctx context.Context, conn Conn, logger *slog.Logger, taskName string, ids []int64, hideForMs int64, errorMsg string) {
	q := `SELECT * FROM cb_hide_tasks(name => $1, ids => $2, hide_for => $3);`
	_, err := execWithRetry(ctx, conn, q, taskName, ids, hideForMs)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.ErrorContext(ctx, errorMsg, "task", taskName, "error", err)
	}
}

func completeTaskRun(ctx context.Context, conn Conn, logger *slog.Logger, taskName string, runID int64, output []byte) {
	q := `SELECT * FROM cb_complete_task(name => $1, id => $2, output => $3);`
	if _, err := execWithRetry(ctx, conn, q, taskName, runID, output); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark task as completed", "task", taskName, "error", err)
	}
}

func failTaskRun(ctx context.Context, conn Conn, logger *slog.Logger, taskName string, runID int64, errorMessage string) {
	q := `SELECT * FROM cb_fail_task(name => $1, id => $2, error_message => $3);`
	if _, err := execWithRetry(ctx, conn, q, taskName, runID, errorMessage); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark task as failed", "task", taskName, "error", err)
	}
}

func hideStepRuns(ctx context.Context, conn Conn, logger *slog.Logger, flowName, stepName string, ids []int64, hideForMs int64, errorMsg string) {
	q := `SELECT * FROM cb_hide_steps(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
	_, err := execWithRetry(ctx, conn, q, flowName, stepName, ids, hideForMs)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.ErrorContext(ctx, errorMsg, "flow", flowName, "step", stepName, "error", err)
	}
}

func completeStepRun(ctx context.Context, conn Conn, logger *slog.Logger, flowName, stepName string, stepID int64, output []byte) {
	q := `SELECT * FROM cb_complete_step(flow_name => $1, step_name => $2, step_id => $3, output => $4);`
	if _, err := execWithRetry(ctx, conn, q, flowName, stepName, stepID, output); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark step as completed", "flow", flowName, "step", stepName, "error", err)
	}
}

func failStepRun(ctx context.Context, conn Conn, logger *slog.Logger, flowName, stepName string, stepID int64, errorMessage string) {
	q := `SELECT * FROM cb_fail_step(flow_name => $1, step_name => $2, step_id => $3, error_message => $4);`
	if _, err := execWithRetry(ctx, conn, q, flowName, stepName, stepID, errorMessage); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark step as failed", "flow", flowName, "step", stepName, "error", err)
	}
}

func hideMapTaskRuns(ctx context.Context, conn Conn, logger *slog.Logger, flowName, stepName string, ids []int64, hideForMs int64, errorMsg string) {
	q := `SELECT * FROM cb_hide_map_tasks(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
	_, err := execWithRetry(ctx, conn, q, flowName, stepName, ids, hideForMs)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.ErrorContext(ctx, errorMsg, "flow", flowName, "step", stepName, "error", err)
	}
}

func completeMapTaskRun(ctx context.Context, conn Conn, logger *slog.Logger, flowName, stepName string, mapTaskID int64, output []byte) {
	q := `SELECT * FROM cb_complete_map_task(flow_name => $1, step_name => $2, map_task_id => $3, output => $4);`
	if _, err := execWithRetry(ctx, conn, q, flowName, stepName, mapTaskID, output); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark map task as completed", "flow", flowName, "step", stepName, "error", err)
	}
}

func failMapTaskRun(ctx context.Context, conn Conn, logger *slog.Logger, flowName, stepName string, mapTaskID int64, errorMessage string) {
	q := `SELECT * FROM cb_fail_map_task(flow_name => $1, step_name => $2, map_task_id => $3, error_message => $4);`
	if _, err := execWithRetry(ctx, conn, q, flowName, stepName, mapTaskID, errorMessage); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark map task as failed", "flow", flowName, "step", stepName, "error", err)
	}
}

func spawnGeneratorMapTasks(ctx context.Context, conn Conn, flowName, stepName string, stepID int64, items []byte) (int, error) {
	q := `SELECT * FROM cb_spawn_generator_map_tasks(flow_name => $1, step_name => $2, step_id => $3, items => $4);`
	var spawned int
	if err := conn.QueryRow(ctx, q, flowName, stepName, stepID, items).Scan(&spawned); err != nil {
		return 0, fmt.Errorf("spawn generator map tasks: %w", err)
	}
	return spawned, nil
}

func completeGeneratorStep(ctx context.Context, conn Conn, flowName, stepName string, stepID int64) error {
	q := `SELECT * FROM cb_complete_generator_step(flow_name => $1, step_name => $2, step_id => $3);`
	if _, err := execWithRetry(ctx, conn, q, flowName, stepName, stepID); err != nil {
		return fmt.Errorf("mark generator complete: %w", err)
	}
	return nil
}

func completeGeneratorStepOrQueueReduce(ctx context.Context, conn Conn, flowName, stepName string, stepID int64) (bool, error) {
	q := `SELECT cb_complete_generator_step_or_queue_reduce(flow_name => $1, step_name => $2, step_id => $3);`
	var ready bool
	if err := conn.QueryRow(ctx, q, flowName, stepName, stepID).Scan(&ready); err != nil {
		return false, fmt.Errorf("complete generator step or queue reduce: %w", err)
	}
	return ready, nil
}

func completeMapTaskOrQueueReduce(ctx context.Context, conn Conn, flowName, stepName string, mapTaskID int64, output []byte) (int64, error) {
	q := `SELECT cb_complete_map_task_or_queue_reduce(flow_name => $1, step_name => $2, map_task_id => $3, output => $4);`
	var stepID *int64
	if err := conn.QueryRow(ctx, q, flowName, stepName, mapTaskID, output).Scan(&stepID); err != nil {
		return 0, fmt.Errorf("complete map task or queue reduce: %w", err)
	}
	if stepID == nil {
		return 0, nil
	}
	return *stepID, nil
}

func finalizeStepReduction(ctx context.Context, conn Conn, logger *slog.Logger, flowName string, step *Step, stepID int64) error {
	if step.reducerFn == nil {
		return fmt.Errorf("step %s has no reducer", step.name)
	}

	flowRunID, err := getStepFlowRunIDForReduction(ctx, conn, flowName, step, stepID)
	if err != nil {
		return err
	}
	if flowRunID == 0 {
		return nil
	}

	accPtr := reflect.New(step.reducerAcc)
	if err := json.Unmarshal(step.reducerInit, accPtr.Interface()); err != nil {
		return fmt.Errorf("unmarshal reducer initial accumulator: %w", err)
	}
	acc := accPtr.Elem()

	mapTable := fmt.Sprintf("cb_m_%s", strings.ToLower(flowName))
	q := fmt.Sprintf(`SELECT output FROM %s WHERE flow_run_id = $1 AND step_name = $2 AND status = 'completed' ORDER BY item_idx`, mapTable)
	rows, err := queryWithRetry(ctx, conn, q, flowRunID, step.name)
	if err != nil {
		return fmt.Errorf("query reducer map outputs: %w", err)
	}
	defer rows.Close()

	reducerFn := reflect.ValueOf(step.reducerFn)
	for rows.Next() {
		var outJSON json.RawMessage
		if scanErr := rows.Scan(&outJSON); scanErr != nil {
			return fmt.Errorf("scan reducer map output: %w", scanErr)
		}

		itemPtr := reflect.New(step.reducerItem)
		if unmarshalErr := json.Unmarshal(outJSON, itemPtr.Interface()); unmarshalErr != nil {
			return fmt.Errorf("unmarshal reducer item output: %w", unmarshalErr)
		}

		results := reducerFn.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			acc,
			itemPtr.Elem(),
		})

		if !results[1].IsNil() {
			return results[1].Interface().(error)
		}

		acc = results[0]
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate reducer map outputs: %w", err)
	}

	output, err := json.Marshal(acc.Interface())
	if err != nil {
		return fmt.Errorf("marshal reducer output: %w", err)
	}

	completeStepRun(ctx, conn, logger, flowName, step.name, stepID, output)
	return nil
}

func getStepFlowRunIDForReduction(ctx context.Context, conn Conn, flowName string, step *Step, stepID int64) (int64, error) {
	tableName := fmt.Sprintf("cb_s_%s", strings.ToLower(flowName))
	q := fmt.Sprintf(`SELECT flow_run_id FROM %s WHERE id = $1 AND step_name = $2 AND status IN ('queued', 'started')`, tableName)
	if step.isGenerator {
		q += ` AND generator_status = 'complete'`
	}
	var flowRunID int64
	if err := conn.QueryRow(ctx, q, stepID, step.name).Scan(&flowRunID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("get flow run for reducer: %w", err)
	}
	return flowRunID, nil
}

func generatorReductionReady(ctx context.Context, conn Conn, flowName, stepName string, stepID int64) (bool, error) {
	tableName := fmt.Sprintf("cb_s_%s", strings.ToLower(flowName))
	q := fmt.Sprintf(`SELECT generator_status = 'complete' FROM %s WHERE id = $1 AND step_name = $2 AND status IN ('queued', 'started')`, tableName)
	var ready bool
	if err := conn.QueryRow(ctx, q, stepID, stepName).Scan(&ready); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("check generator reducer readiness: %w", err)
	}
	return ready, nil
}

func failGeneratorStep(ctx context.Context, conn Conn, logger *slog.Logger, flowName, stepName string, stepID int64, errorMessage string) {
	q := `SELECT * FROM cb_fail_generator_step(flow_name => $1, step_name => $2, step_id => $3, error_message => $4);`
	if _, err := execWithRetry(ctx, conn, q, flowName, stepName, stepID, errorMessage); err != nil {
		logger.ErrorContext(ctx, "worker: cannot fail generator step", "flow", flowName, "step", stepName, "id", stepID, "error", err)
	}
}

func completeTaskOnFail(ctx context.Context, conn Conn, logger *slog.Logger, taskName string, runID int64) {
	if _, err := execWithRetry(ctx, conn, `SELECT cb_complete_task_on_fail($1, $2);`, taskName, runID); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark task on-fail as completed", "task", taskName, "id", runID, "error", err)
	}
}

func failTaskOnFail(ctx context.Context, conn Conn, logger *slog.Logger, taskName string, runID int64, errorMessage string, exhausted bool, delayMs int64) {
	if _, err := execWithRetry(ctx, conn, `SELECT cb_fail_task_on_fail($1, $2, $3, $4, $5);`, taskName, runID, errorMessage, exhausted, delayMs); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark task on-fail as failed", "task", taskName, "id", runID, "error", err)
	}
}

func completeFlowOnFail(ctx context.Context, conn Conn, logger *slog.Logger, flowName string, runID int64) {
	if _, err := execWithRetry(ctx, conn, `SELECT cb_complete_flow_on_fail($1, $2);`, flowName, runID); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark flow on-fail as completed", "flow", flowName, "id", runID, "error", err)
	}
}

func failFlowOnFail(ctx context.Context, conn Conn, logger *slog.Logger, flowName string, runID int64, errorMessage string, exhausted bool, delayMs int64) {
	if _, err := execWithRetry(ctx, conn, `SELECT cb_fail_flow_on_fail($1, $2, $3, $4, $5);`, flowName, runID, errorMessage, exhausted, delayMs); err != nil {
		logger.ErrorContext(ctx, "worker: cannot mark flow on-fail as failed", "flow", flowName, "id", runID, "error", err)
	}
}

func completeFlowEarly(ctx context.Context, conn Conn, flowName string, runID int64, stepName string, output any, reason string) (bool, string, error) {
	payload, err := json.Marshal(output)
	if err != nil {
		return false, "", fmt.Errorf("marshal early completion output: %w", err)
	}

	q := `SELECT changed, status FROM cb_early_exit_flow(flow_name => $1, flow_run_id => $2, step_name => $3, output => $4, reason => $5);`
	var changed bool
	var status string
	err = conn.QueryRow(ctx, q, flowName, runID, stepName, payload, reason).Scan(&changed, &status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, "", nil
		}
		return false, "", err
	}

	return changed, status, nil
}

func isTaskCancelRequested(ctx context.Context, conn Conn, taskName string, runID int64) (bool, error) {
	var requested bool
	err := conn.QueryRow(ctx, `SELECT cb_task_cancel_requested(name => $1, run_id => $2);`, taskName, runID).Scan(&requested)
	if err != nil {
		return false, err
	}
	return requested, nil
}

func isTaskCancelRequestedBestEffort(ctx context.Context, conn Conn, taskName string, runID int64) (bool, error) {
	requested, err := isTaskCancelRequested(ctx, conn, taskName, runID)
	if err == nil || ctx == nil || ctx.Err() == nil {
		return requested, err
	}

	retryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return isTaskCancelRequested(retryCtx, conn, taskName, runID)
}

func getFlowStatus(ctx context.Context, conn Conn, flowName string, runID int64) (string, error) {
	if runID == 0 {
		return "", nil
	}

	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(flowName))
	query := fmt.Sprintf("SELECT status FROM %s WHERE id = $1", tableName)

	var status string
	err := conn.QueryRow(ctx, query, runID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}

	return status, nil
}

func getFlowStatusBestEffort(ctx context.Context, conn Conn, flowName string, runID int64) (string, error) {
	status, err := getFlowStatus(ctx, conn, flowName, runID)
	if err == nil || ctx == nil || ctx.Err() == nil {
		return status, err
	}

	retryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return getFlowStatus(retryCtx, conn, flowName, runID)
}

func shouldStopFlow(status string) bool {
	switch status {
	case "canceling", "canceled", "failed", "completed":
		return true
	default:
		return false
	}
}

func stopFlowIfRequested(
	ctx context.Context,
	conn Conn,
	logger *slog.Logger,
	flowName string,
	flowRunID int64,
	claimCancel context.CancelFunc,
	cancelRequested *bool,
	onCancel func(),
) bool {
	status, err := getFlowStatusBestEffort(ctx, conn, flowName, flowRunID)
	if err != nil || !shouldStopFlow(status) {
		return false
	}

	if cancelRequested != nil && (status == "canceling" || status == "canceled") {
		*cancelRequested = true
	}

	if claimCancel != nil {
		claimCancel()
	}
	if onCancel != nil && (status == "canceling" || status == "canceled") {
		onCancel()
	}
	if logger != nil {
		logger.DebugContext(ctx, "worker: flow stop observed", "flow", flowName, "flow_run_id", flowRunID, "status", status)
	}

	return true
}

func watchFlowStop(ctx context.Context, interval time.Duration, stopIfRequested func() bool) func() {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if stopIfRequested() {
					return
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

func finalizeFlowCancel(ctx context.Context, conn Conn, logger *slog.Logger, flowName string, runID int64) {
	if runID == 0 {
		return
	}
	if _, err := execWithRetry(ctx, conn, `SELECT cb_finalize_flow_cancellation(name => $1, run_id => $2);`, flowName, runID); err != nil {
		logger.DebugContext(ctx, "worker: finalize flow canceled skipped", "flow", flowName, "id", runID, "error", err)
	}
}

func finalizeFlowCancelIfRequested(ctx context.Context, conn Conn, logger *slog.Logger, flowName string, runID int64, cancelRequested *bool) {
	if cancelRequested == nil || !*cancelRequested {
		return
	}

	finalizeCtx := ctx
	if ctx == nil || ctx.Err() != nil {
		retryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		finalizeCtx = retryCtx
	}

	finalizeFlowCancel(finalizeCtx, conn, logger, flowName, runID)
}

func cancelStepRun(ctx context.Context, conn Conn, logger *slog.Logger, flowName string, stepID int64) {
	if _, err := execWithRetry(ctx, conn, `SELECT cb_cancel_step_run(flow_name => $1, step_id => $2);`, flowName, stepID); err != nil {
		logger.DebugContext(ctx, "worker: cannot cancel step run", "flow", flowName, "step_id", stepID, "error", err)
	}
}

func cancelMapTaskRun(ctx context.Context, conn Conn, logger *slog.Logger, flowName string, mapTaskID int64) {
	if _, err := execWithRetry(ctx, conn, `SELECT cb_cancel_map_task_run(flow_name => $1, map_task_id => $2);`, flowName, mapTaskID); err != nil {
		logger.DebugContext(ctx, "worker: cannot cancel map task run", "flow", flowName, "map_task_id", mapTaskID, "error", err)
	}
}

func runWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	if timeout <= 0 {
		return fn(ctx)
	}

	fnCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(fnCtx)
}

func runWithTimeoutErr(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	_, err := runWithTimeout(ctx, timeout, func(fnCtx context.Context) (struct{}, error) {
		return struct{}{}, fn(fnCtx)
	})
	return err
}

func runSafely[T any](panicPrefix string, fn func() (T, error)) (out T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s: %v", panicPrefix, r)
		}
	}()
	return fn()
}

func nextRetryDelay(attempt int, backoff BackoffStrategy, defaultMin, defaultMax time.Duration) time.Duration {
	if backoff != nil {
		return backoff.NextDelay(attempt)
	}
	return backoffWithFullJitter(attempt, defaultMin, defaultMax)
}

func ensurePositiveDelayMs(delay time.Duration) int64 {
	ms := delay.Milliseconds()
	if ms <= 0 {
		return 1
	}
	return ms
}

type inFlightTracker struct {
	mu  sync.Mutex
	ids map[int64]struct{}
}

func newInFlightTracker() *inFlightTracker {
	return &inFlightTracker{ids: make(map[int64]struct{})}
}

func (t *inFlightTracker) mark(id int64) {
	t.mu.Lock()
	t.ids[id] = struct{}{}
	t.mu.Unlock()
}

func (t *inFlightTracker) remove(id int64) {
	t.mu.Lock()
	delete(t.ids, id)
	t.mu.Unlock()
}

func (t *inFlightTracker) list() []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	ids := make([]int64, 0, len(t.ids))
	for id := range t.ids {
		ids = append(ids, id)
	}

	return ids
}

func markClaimsInFlight[T any](tracker *inFlightTracker, claims []T, idFn func(T) int64) {
	for _, claim := range claims {
		tracker.mark(idFn(claim))
	}
}

func startInFlightHider(handlerCtx context.Context, wg *sync.WaitGroup, interval time.Duration, hideFn func(context.Context)) {
	wg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-handlerCtx.Done():
				return
			case <-ticker.C:
				hideFn(handlerCtx)
			}
		}
	})
}

type taskOnFailClaim struct {
	ID             int64           `json:"id"`
	Input          json.RawMessage `json:"input"`
	ErrorMessage   string          `json:"error_message"`
	Attempts       int             `json:"attempts"`
	OnFailAttempts int             `json:"on_fail_attempts"`
	StartedAt      time.Time       `json:"started_at"`
	FailedAt       time.Time       `json:"failed_at"`
	ConcurrencyKey string          `json:"concurrency_key,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type taskOnFailWorker struct {
	conn   Conn
	logger *slog.Logger
	task   *Task
}

func newTaskOnFailWorker(conn Conn, logger *slog.Logger, task *Task) *taskOnFailWorker {
	return &taskOnFailWorker{conn: conn, logger: logger, task: task}
}

func (w *taskOnFailWorker) start(shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup) {
	h := w.task.onFailOpts

	runClaimLoop(shutdownCtx, handlerCtx, wg, claimLoopConfig[taskOnFailClaim]{
		concurrency:    h.Concurrency,
		pollClaims:     w.pollClaims,
		handleClaim:    w.handle,
		emptyPollDelay: 500 * time.Millisecond,
		logPollError: func(ctx context.Context, err error) {
			w.logger.ErrorContext(ctx, "worker: cannot poll task on-fail claims", "task", w.task.name, "error", err)
		},
	})
}

func (w *taskOnFailWorker) pollClaims(ctx context.Context) ([]taskOnFailClaim, error) {
	h := w.task.onFailOpts
	q := `SELECT * FROM cb_poll_task_on_fail($1, $2);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.task.name, h.BatchSize)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleTaskOnFailClaim)
}

func (w *taskOnFailWorker) handle(ctx context.Context, claim taskOnFailClaim) {
	h := w.task.onFailOpts

	failure := TaskFailure{
		TaskName:       w.task.name,
		TaskRunID:      claim.ID,
		ErrorMessage:   claim.ErrorMessage,
		Attempts:       claim.Attempts,
		OnFailAttempts: claim.OnFailAttempts,
		StartedAt:      claim.StartedAt,
		FailedAt:       claim.FailedAt,
		ConcurrencyKey: claim.ConcurrencyKey,
		IdempotencyKey: claim.IdempotencyKey,
	}

	err := runWithTimeoutErr(ctx, h.Timeout, func(fnCtx context.Context) error {
		return w.task.onFail(fnCtx, claim.Input, failure)
	})

	if err == nil {
		completeTaskOnFail(ctx, w.conn, w.logger, w.task.name, claim.ID)
		return
	}

	exhausted := claim.OnFailAttempts > h.MaxRetries
	delay := nextRetryDelay(claim.OnFailAttempts-1, h.Backoff, 250*time.Millisecond, 5*time.Second)
	failTaskOnFail(ctx, w.conn, w.logger, w.task.name, claim.ID, err.Error(), exhausted, delay.Milliseconds())
}

func scanCollectibleTaskOnFailClaim(row pgx.CollectableRow) (taskOnFailClaim, error) {
	return scanTaskOnFailClaim(row)
}

func scanTaskOnFailClaim(row pgx.Row) (taskOnFailClaim, error) {
	rec := taskOnFailClaim{}
	var concurrencyKey *string
	var idempotencyKey *string
	var startedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&rec.Input,
		&rec.ErrorMessage,
		&rec.Attempts,
		&rec.OnFailAttempts,
		&startedAt,
		&failedAt,
		&concurrencyKey,
		&idempotencyKey,
	); err != nil {
		return rec, err
	}

	if startedAt != nil {
		rec.StartedAt = *startedAt
	}
	if failedAt != nil {
		rec.FailedAt = *failedAt
	}
	if concurrencyKey != nil {
		rec.ConcurrencyKey = *concurrencyKey
	}
	if idempotencyKey != nil {
		rec.IdempotencyKey = *idempotencyKey
	}

	return rec, nil
}

type flowOnFailClaim struct {
	ID                    int64           `json:"id"`
	Input                 json.RawMessage `json:"input"`
	ErrorMessage          string          `json:"error_message"`
	OnFailAttempts        int             `json:"on_fail_attempts"`
	StartedAt             time.Time       `json:"started_at"`
	FailedAt              time.Time       `json:"failed_at"`
	ConcurrencyKey        string          `json:"concurrency_key,omitempty"`
	IdempotencyKey        string          `json:"idempotency_key,omitempty"`
	FailedStepName        string          `json:"failed_step_name,omitempty"`
	FailedStepInput       json.RawMessage `json:"failed_step_input,omitempty"`
	FailedStepSignalInput json.RawMessage `json:"failed_step_signal_input,omitempty"`
	FailedStepAttempts    int             `json:"failed_step_attempts"`
	CompletedStepOutputs  json.RawMessage `json:"completed_step_outputs,omitempty"`
}

type flowOnFailWorker struct {
	conn   Conn
	logger *slog.Logger
	flow   *Flow
}

func newFlowOnFailWorker(conn Conn, logger *slog.Logger, flow *Flow) *flowOnFailWorker {
	return &flowOnFailWorker{conn: conn, logger: logger, flow: flow}
}

func (w *flowOnFailWorker) start(shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup) {
	h := w.flow.onFailOpts

	runClaimLoop(shutdownCtx, handlerCtx, wg, claimLoopConfig[flowOnFailClaim]{
		concurrency:    h.Concurrency,
		pollClaims:     w.pollClaims,
		handleClaim:    w.handle,
		emptyPollDelay: 500 * time.Millisecond,
		logPollError: func(ctx context.Context, err error) {
			w.logger.ErrorContext(ctx, "worker: cannot poll flow on-fail claims", "flow", w.flow.name, "error", err)
		},
	})
}

func (w *flowOnFailWorker) pollClaims(ctx context.Context) ([]flowOnFailClaim, error) {
	h := w.flow.onFailOpts
	q := `SELECT * FROM cb_poll_flow_on_fail($1, $2);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.flow.name, h.BatchSize)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleFlowOnFailClaim)
}

func (w *flowOnFailWorker) handle(ctx context.Context, claim flowOnFailClaim) {
	h := w.flow.onFailOpts

	failure := FlowFailure{
		FlowName:              w.flow.name,
		FlowRunID:             claim.ID,
		FailedStepName:        claim.FailedStepName,
		ErrorMessage:          claim.ErrorMessage,
		Attempts:              claim.FailedStepAttempts,
		OnFailAttempts:        claim.OnFailAttempts,
		StartedAt:             claim.StartedAt,
		FailedAt:              claim.FailedAt,
		ConcurrencyKey:        claim.ConcurrencyKey,
		IdempotencyKey:        claim.IdempotencyKey,
		FailedStepInput:       claim.FailedStepInput,
		FailedStepSignalInput: claim.FailedStepSignalInput,
	}

	if len(claim.CompletedStepOutputs) > 0 {
		outputs := make(map[string]json.RawMessage)
		if err := json.Unmarshal(claim.CompletedStepOutputs, &outputs); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot decode completed step outputs", "flow", w.flow.name, "id", claim.ID, "error", err)
		} else {
			failure.CompletedStepOutputs = outputs
		}
	}

	err := runWithTimeoutErr(ctx, h.Timeout, func(fnCtx context.Context) error {
		return w.flow.onFail(fnCtx, claim.Input, failure)
	})

	if err == nil {
		completeFlowOnFail(ctx, w.conn, w.logger, w.flow.name, claim.ID)
		return
	}

	exhausted := claim.OnFailAttempts > h.MaxRetries
	delay := nextRetryDelay(claim.OnFailAttempts-1, h.Backoff, 250*time.Millisecond, 5*time.Second)
	failFlowOnFail(ctx, w.conn, w.logger, w.flow.name, claim.ID, err.Error(), exhausted, delay.Milliseconds())
}

func scanCollectibleFlowOnFailClaim(row pgx.CollectableRow) (flowOnFailClaim, error) {
	return scanFlowOnFailClaim(row)
}

func scanFlowOnFailClaim(row pgx.Row) (flowOnFailClaim, error) {
	rec := flowOnFailClaim{}
	var startedAt *time.Time
	var failedAt *time.Time
	var concurrencyKey *string
	var idempotencyKey *string
	var failedStepName *string
	var failedStepInput *json.RawMessage
	var failedStepSignalInput *json.RawMessage
	var failedStepAttempts *int
	var completedStepOutputs *json.RawMessage

	if err := row.Scan(
		&rec.ID,
		&rec.Input,
		&rec.ErrorMessage,
		&rec.OnFailAttempts,
		&startedAt,
		&failedAt,
		&concurrencyKey,
		&idempotencyKey,
		&failedStepName,
		&failedStepInput,
		&failedStepSignalInput,
		&failedStepAttempts,
		&completedStepOutputs,
	); err != nil {
		return rec, err
	}

	if startedAt != nil {
		rec.StartedAt = *startedAt
	}
	if failedAt != nil {
		rec.FailedAt = *failedAt
	}
	if concurrencyKey != nil {
		rec.ConcurrencyKey = *concurrencyKey
	}
	if idempotencyKey != nil {
		rec.IdempotencyKey = *idempotencyKey
	}
	if failedStepName != nil {
		rec.FailedStepName = *failedStepName
	}
	if failedStepInput != nil {
		rec.FailedStepInput = *failedStepInput
	}
	if failedStepSignalInput != nil {
		rec.FailedStepSignalInput = *failedStepSignalInput
	}
	if failedStepAttempts != nil {
		rec.FailedStepAttempts = *failedStepAttempts
	}
	if completedStepOutputs != nil {
		rec.CompletedStepOutputs = *completedStepOutputs
	}

	return rec, nil
}
