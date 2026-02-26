package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
				var worker interface {
					start(context.Context, context.Context, *sync.WaitGroup)
				}
				if s.isMapStep {
					worker = newMapStepWorker(w.conn, w.logger, f.name, &s)
				} else {
					worker = newStepWorker(w.conn, w.logger, f.name, &s)
				}
				worker.start(ctx, handlerCtx, &wg)
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

	h := w.task.handlerOpts
	if h == nil {
		w.logger.ErrorContext(ctx, "worker: failed", "task", w.task.name, "error", "no handler options (definition-only)")
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

	w.logger.DebugContext(ctx, "worker: handleTask",
		"task", w.task.name,
		"id", msg.ID,
		"attempts", msg.Attempts,
		"input", string(msg.Input),
	)

	out, err := runWithTimeout(ctx, h.Timeout, func(fnCtx context.Context) ([]byte, error) {
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

	q := `SELECT id, attempts, input, step_outputs, signal_input FROM cb_poll_steps(flow_name => $1, step_name => $2, quantity => $3, hide_for => $4, poll_for => $5, poll_interval => $6);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.flowName, w.step.name, h.BatchSize, (10 * time.Minute).Milliseconds(), defaultPollFor.Milliseconds(), defaultPollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleStepClaim)
}

func (w *stepWorker) handle(ctx context.Context, msg stepClaim) {
	defer w.removeInFlight(msg.ID)

	h := w.step.handlerOpts

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

	if w.logger.Enabled(ctx, slog.LevelDebug) {
		stepOutputsJSON, _ := json.Marshal(msg.StepOutputs)
		w.logger.DebugContext(ctx, "worker: handleStep",
			"flow", w.flowName,
			"step", w.step.name,
			"id", msg.ID,
			"attempts", msg.Attempts,
			"input", string(msg.Input),
			"step_outputs", string(stepOutputsJSON),
		)
	}

	out, err := runWithTimeout(ctx, h.Timeout, func(fnCtx context.Context) ([]byte, error) {
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
		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordSuccess()
		}
		completeStepRun(ctx, w.conn, w.logger, w.flowName, w.step.name, msg.ID, out)
	}
}

func scanCollectibleStepClaim(row pgx.CollectableRow) (stepClaim, error) {
	return scanStepClaim(row)
}

func scanStepClaim(row pgx.Row) (stepClaim, error) {
	rec := stepClaim{}

	var stepOutputs *map[string]json.RawMessage

	if err := row.Scan(
		&rec.ID,
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
	q := `SELECT id, attempts, input, step_outputs, signal_input, item FROM cb_poll_map_tasks(flow_name => $1, step_name => $2, quantity => $3, hide_for => $4, poll_for => $5, poll_interval => $6);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.flowName, w.step.name, h.BatchSize, (10 * time.Minute).Milliseconds(), defaultPollFor.Milliseconds(), defaultPollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleMapTaskClaim)
}

func (w *mapStepWorker) handle(ctx context.Context, msg mapTaskClaim) {
	defer w.removeInFlight(msg.ID)

	h := w.step.handlerOpts
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

	itemOutput, err := runWithTimeout(ctx, h.Timeout, func(fnCtx context.Context) (json.RawMessage, error) {
		return runSafely("step handler panic", func() (json.RawMessage, error) {
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
	concurrency  int
	pollClaims   func(context.Context) ([]C, error)
	handleClaim  func(context.Context, C)
	onPolled     func([]C)
	logPollError func(context.Context, error)
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

func runWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	if timeout <= 0 {
		return fn(ctx)
	}

	fnCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(fnCtx)
}

func runWithTimeoutErr(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	_, err := runWithTimeout[struct{}](ctx, timeout, func(fnCtx context.Context) (struct{}, error) {
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
		concurrency: h.Concurrency,
		pollClaims:  w.pollClaims,
		handleClaim: w.handle,
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
		concurrency: h.Concurrency,
		pollClaims:  w.pollClaims,
		handleClaim: w.handle,
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
