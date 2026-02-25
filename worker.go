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
	}

	// Validate HandlerOpts for all flow steps
	for _, f := range w.flows {
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
	}

	// Start step workers
	for _, f := range w.flows {
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

	// In-flight message tracking (replaces messageHider)
	inFlight   map[int64]struct{}
	inFlightMu sync.Mutex
}

func newTaskWorker(conn Conn, logger *slog.Logger, task *Task) *taskWorker {
	return &taskWorker{
		conn:     conn,
		logger:   logger,
		task:     task,
		inFlight: make(map[int64]struct{}),
	}
}

func (w *taskWorker) start(shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup) {
	claims := make(chan taskClaim)

	// Start periodic hiding of in-flight tasks
	wg.Go(func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-handlerCtx.Done():
				return
			case <-ticker.C:
				w.hideInFlight(handlerCtx)
			}
		}
	})

	// Start producer
	wg.Go(func() {
		defer close(claims)

		retryAttempt := 0

		for {
			select {
			case <-shutdownCtx.Done():
				return
			default:
			}

			msgs, err := w.pollClaims(shutdownCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				w.logger.ErrorContext(shutdownCtx, "worker: cannot poll task claims", "task", w.task.name, "error", err)

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

			// Success: reset backoff
			retryAttempt = 0

			if len(msgs) == 0 {
				continue
			}

			w.markInFlight(msgs)

			for _, msg := range msgs {
				select {
				case <-shutdownCtx.Done():
					return
				case claims <- msg:
				}
			}
		}
	})

	// Start consumers
	h := w.task.handlerOpts
	if h == nil {
		w.logger.WarnContext(shutdownCtx, "task handler has no options (definition-only)", "task", w.task.name)
		return
	}
	for i := 0; i < h.Concurrency; i++ {
		wg.Go(func() {
			for msg := range claims {
				w.handle(handlerCtx, msg)
			}
		})
	}
}

func (w *taskWorker) markInFlight(msgs []taskClaim) {
	w.inFlightMu.Lock()
	for _, msg := range msgs {
		w.inFlight[msg.ID] = struct{}{}
	}
	w.inFlightMu.Unlock()
}

func (w *taskWorker) removeInFlight(id int64) {
	w.inFlightMu.Lock()
	delete(w.inFlight, id)
	w.inFlightMu.Unlock()
}

func (w *taskWorker) getInFlight() []int64 {
	w.inFlightMu.Lock()
	defer w.inFlightMu.Unlock()

	ids := make([]int64, 0, len(w.inFlight))
	for id := range w.inFlight {
		ids = append(ids, id)
	}
	return ids
}

func (w *taskWorker) hideInFlight(ctx context.Context) {
	ids := w.getInFlight()
	if len(ids) == 0 {
		return
	}

	q := `SELECT * FROM cb_hide_tasks(name => $1, ids => $2, hide_for => $3);`
	_, err := execWithRetry(ctx, w.conn, q, w.task.name, ids, (10 * time.Minute).Milliseconds())

	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.logger.ErrorContext(ctx, "worker: cannot hide in-flight tasks", "task", w.task.name, "error", err)
	}
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
			delayMs := delay.Milliseconds()
			if delayMs <= 0 {
				delayMs = 1
			}
			q := `SELECT * FROM cb_hide_tasks(name => $1, ids => $2, hide_for => $3);`
			if _, err := execWithRetry(ctx, w.conn, q, w.task.name, []int64{msg.ID}, delayMs); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay task due to open circuit", "task", w.task.name, "error", err)
			}
			return
		}
	}

	w.logger.DebugContext(ctx, "worker: handleTask",
		"task", w.task.name,
		"id", msg.ID,
		"attempts", msg.Attempts,
		"input", string(msg.Input),
	)

	// Execute with timeout if configured
	fnCtx := ctx
	if h.Timeout > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, h.Timeout)
		defer cancel()
	}

	var out []byte
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("task handler panic: %v", r)
			}
		}()
		// Call the interface method to execute the handler
		inputJSON, marshalErr := json.Marshal(msg.Input)
		if marshalErr != nil {
			err = fmt.Errorf("marshal input: %w", marshalErr)
			return
		}
		out, err = w.task.handler(fnCtx, inputJSON)
	}()

	// Handle result
	if err != nil {
		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordFailure(time.Now())
		}
		w.logger.ErrorContext(ctx, "worker: failed", "task", w.task.name, "error", err)

		if msg.Attempts > h.MaxRetries {
			q := `SELECT * FROM cb_fail_task(name => $1, id => $2, error_message => $3);`
			if _, err := execWithRetry(ctx, w.conn, q, w.task.name, msg.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark task as failed", "task", w.task.name, "error", err)
			}
		} else {
			delay := backoffWithFullJitter(msg.Attempts-1, 0, 0)
			if h.Backoff != nil {
				delay = h.Backoff.NextDelay(msg.Attempts - 1)
			}
			q := `SELECT * FROM cb_hide_tasks(name => $1, ids => $2, hide_for => $3);`
			if _, err := execWithRetry(ctx, w.conn, q, w.task.name, []int64{msg.ID}, delay.Milliseconds()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next task run", "task", w.task.name, "error", err)
			}
		}
	} else {
		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordSuccess()
		}
		q := `SELECT * FROM cb_complete_task(name => $1, id => $2, output => $3);`
		if _, err := execWithRetry(ctx, w.conn, q, w.task.name, msg.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark task as completed", "task", w.task.name, "error", err)
		}
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

	// In-flight message tracking
	inFlight   map[int64]struct{}
	inFlightMu sync.Mutex
}

func newStepWorker(conn Conn, logger *slog.Logger, flowName string, step *Step) *stepWorker {
	return &stepWorker{
		conn:     conn,
		logger:   logger,
		flowName: flowName,
		step:     step,
		inFlight: make(map[int64]struct{}),
	}
}

func (w *stepWorker) start(shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup) {
	claims := make(chan stepClaim)

	// Start periodic hiding of in-flight steps
	wg.Go(func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-handlerCtx.Done():
				return
			case <-ticker.C:
				w.hideInFlight(handlerCtx)
			}
		}
	})

	// Start producer
	// Start producer
	wg.Go(func() {
		defer close(claims)

		retryAttempt := 0

		for {
			select {
			case <-shutdownCtx.Done():
				return
			default:
			}

			msgs, err := w.pollClaims(shutdownCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				w.logger.ErrorContext(shutdownCtx, "worker: cannot poll step claims", "flow", w.flowName, "step", w.step.name, "error", err)

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

			// Success: reset backoff
			retryAttempt = 0

			if len(msgs) == 0 {
				continue
			}

			w.markInFlight(msgs)

			for _, msg := range msgs {
				select {
				case <-shutdownCtx.Done():
					return
				case claims <- msg:
				}
			}
		}
	})

	// Start consumers
	h := w.step.handlerOpts
	for i := 0; i < h.Concurrency; i++ {
		wg.Go(func() {
			for msg := range claims {
				w.handle(handlerCtx, msg)
			}
		})
	}
}

func (w *stepWorker) getInFlight() []int64 {
	w.inFlightMu.Lock()
	defer w.inFlightMu.Unlock()

	ids := make([]int64, 0, len(w.inFlight))
	for id := range w.inFlight {
		ids = append(ids, id)
	}
	return ids
}

func (w *stepWorker) markInFlight(msgs []stepClaim) {
	w.inFlightMu.Lock()
	for _, msg := range msgs {
		w.inFlight[msg.ID] = struct{}{}
	}
	w.inFlightMu.Unlock()
}

func (w *stepWorker) removeInFlight(id int64) {
	w.inFlightMu.Lock()
	delete(w.inFlight, id)
	w.inFlightMu.Unlock()
}

func (w *stepWorker) hideInFlight(ctx context.Context) {
	ids := w.getInFlight()
	if len(ids) == 0 {
		return
	}

	q := `SELECT * FROM cb_hide_steps(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
	_, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, w.getInFlight(), (10 * time.Minute).Milliseconds())

	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.logger.ErrorContext(ctx, "worker: cannot hide in-flight steps", "flow", w.flowName, "step", w.step.name, "error", err)
	}
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
			q := `SELECT * FROM cb_hide_steps(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
			if _, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay step due to open circuit", "flow", w.flowName, "step", w.step.name, "error", err)
			}
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

	// Execute with timeout if configured
	fnCtx := ctx
	if h.Timeout > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, h.Timeout)
		defer cancel()
	}

	var out []byte
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("step handler panic: %v", r)
			}
		}()
		// Call the interface method to execute the handler
		inputJSON, marshalErr := json.Marshal(msg.Input)
		if marshalErr != nil {
			err = fmt.Errorf("marshal input: %w", marshalErr)
			return
		}
		signalInputJSON, marshalErr := json.Marshal(msg.SignalInput)
		if marshalErr != nil {
			err = fmt.Errorf("marshal signal input: %w", marshalErr)
			return
		}
		// Convert step_outputs map to []byte map
		depsJSON := make(map[string][]byte)
		for name, depOut := range msg.StepOutputs {
			depJSON, marshalErr := json.Marshal(depOut)
			if marshalErr != nil {
				err = fmt.Errorf("marshal dependency %s output: %w", name, marshalErr)
				return
			}
			depsJSON[name] = depJSON
		}
		if w.step.handler == nil {
			err = fmt.Errorf("step %s has no handler (definition-only)", w.step.name)
			return
		}
		out, err = w.step.handler(fnCtx, inputJSON, depsJSON, signalInputJSON)
	}()

	// Handle result
	if err != nil {
		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordFailure(time.Now())
		}
		w.logger.ErrorContext(ctx, "worker: failed", "flow", w.flowName, "step", w.step.name, "error", err)

		if msg.Attempts > h.MaxRetries {
			q := `SELECT * FROM cb_fail_step(flow_name => $1, step_name => $2, step_id => $3, error_message => $4);`
			if _, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, msg.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark step as failed", "flow", w.flowName, "step", w.step.name, "error", err)
			}
		} else {
			delay := backoffWithFullJitter(msg.Attempts-1, 0, 0)
			if h.Backoff != nil {
				delay = h.Backoff.NextDelay(msg.Attempts - 1)
			}
			q := `SELECT * FROM cb_hide_steps(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
			if _, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next step run", "flow", w.flowName, "step", w.step.name, "error", err)
			}
		}
	} else {
		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordSuccess()
		}
		q := `SELECT * FROM cb_complete_step(flow_name => $1, step_name => $2, step_id => $3, output => $4);`
		if _, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, msg.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark step as completed", "flow", w.flowName, "step", w.step.name, "error", err)
		}
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

	inFlight   map[int64]struct{}
	inFlightMu sync.Mutex
}

func newMapStepWorker(conn Conn, logger *slog.Logger, flowName string, step *Step) *mapStepWorker {
	return &mapStepWorker{
		conn:     conn,
		logger:   logger,
		flowName: flowName,
		step:     step,
		inFlight: make(map[int64]struct{}),
	}
}

func (w *mapStepWorker) start(shutdownCtx, handlerCtx context.Context, wg *sync.WaitGroup) {
	claims := make(chan mapTaskClaim)

	wg.Go(func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-handlerCtx.Done():
				return
			case <-ticker.C:
				w.hideInFlight(handlerCtx)
			}
		}
	})

	wg.Go(func() {
		defer close(claims)

		retryAttempt := 0
		for {
			select {
			case <-shutdownCtx.Done():
				return
			default:
			}

			msgs, err := w.pollClaims(shutdownCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				w.logger.ErrorContext(shutdownCtx, "worker: cannot poll map task claims", "flow", w.flowName, "step", w.step.name, "error", err)

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

			w.markInFlight(msgs)
			for _, msg := range msgs {
				select {
				case <-shutdownCtx.Done():
					return
				case claims <- msg:
				}
			}
		}
	})

	h := w.step.handlerOpts
	for i := 0; i < h.Concurrency; i++ {
		wg.Go(func() {
			for msg := range claims {
				w.handle(handlerCtx, msg)
			}
		})
	}
}

func (w *mapStepWorker) getInFlight() []int64 {
	w.inFlightMu.Lock()
	defer w.inFlightMu.Unlock()

	ids := make([]int64, 0, len(w.inFlight))
	for id := range w.inFlight {
		ids = append(ids, id)
	}
	return ids
}

func (w *mapStepWorker) markInFlight(msgs []mapTaskClaim) {
	w.inFlightMu.Lock()
	for _, msg := range msgs {
		w.inFlight[msg.ID] = struct{}{}
	}
	w.inFlightMu.Unlock()
}

func (w *mapStepWorker) removeInFlight(id int64) {
	w.inFlightMu.Lock()
	delete(w.inFlight, id)
	w.inFlightMu.Unlock()
}

func (w *mapStepWorker) hideInFlight(ctx context.Context) {
	ids := w.getInFlight()
	if len(ids) == 0 {
		return
	}

	q := `SELECT * FROM cb_hide_map_tasks(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
	_, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, ids, (10 * time.Minute).Milliseconds())
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.logger.ErrorContext(ctx, "worker: cannot hide in-flight map tasks", "flow", w.flowName, "step", w.step.name, "error", err)
	}
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
			q := `SELECT * FROM cb_hide_map_tasks(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
			if _, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay map task due to open circuit", "flow", w.flowName, "step", w.step.name, "error", err)
			}
			return
		}
	}

	fnCtx := ctx
	if h.Timeout > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, h.Timeout)
		defer cancel()
	}

	var itemOutput json.RawMessage
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("step handler panic: %v", r)
			}
		}()

		flowInputJSON, marshalErr := json.Marshal(msg.Input)
		if marshalErr != nil {
			err = fmt.Errorf("marshal flow input: %w", marshalErr)
			return
		}

		signalInputJSON, marshalErr := json.Marshal(msg.SignalInput)
		if marshalErr != nil {
			err = fmt.Errorf("marshal signal input: %w", marshalErr)
			return
		}

		depsJSON := make(map[string][]byte)
		for name, depOut := range msg.StepOutputs {
			depJSON, marshalErr := json.Marshal(depOut)
			if marshalErr != nil {
				err = fmt.Errorf("marshal dependency %s output: %w", name, marshalErr)
				return
			}
			depsJSON[name] = depJSON
		}

		singleItemArrayJSON, marshalErr := json.Marshal([]json.RawMessage{msg.Item})
		if marshalErr != nil {
			err = fmt.Errorf("marshal map item wrapper: %w", marshalErr)
			return
		}

		if w.step.mapSource == "" {
			flowInputJSON = singleItemArrayJSON
		} else {
			depsJSON[w.step.mapSource] = singleItemArrayJSON
		}

		if w.step.handler == nil {
			err = fmt.Errorf("step %s has no handler", w.step.name)
			return
		}

		out, handlerErr := w.step.handler(fnCtx, flowInputJSON, depsJSON, signalInputJSON)
		if handlerErr != nil {
			err = handlerErr
			return
		}

		var arr []json.RawMessage
		if unmarshalErr := json.Unmarshal(out, &arr); unmarshalErr != nil {
			err = fmt.Errorf("unmarshal map step output array: %w", unmarshalErr)
			return
		}
		if len(arr) != 1 {
			err = fmt.Errorf("map step handler must produce exactly one output item per map task, got %d", len(arr))
			return
		}
		itemOutput = arr[0]
	}()

	if err != nil {
		if h.CircuitBreaker != nil {
			h.CircuitBreaker.RecordFailure(time.Now())
		}

		if msg.Attempts > h.MaxRetries {
			q := `SELECT * FROM cb_fail_map_task(flow_name => $1, step_name => $2, map_task_id => $3, error_message => $4);`
			if _, qErr := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, msg.ID, err.Error()); qErr != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark map task as failed", "flow", w.flowName, "step", w.step.name, "error", qErr)
			}
		} else {
			delay := backoffWithFullJitter(msg.Attempts-1, 0, 0)
			if h.Backoff != nil {
				delay = h.Backoff.NextDelay(msg.Attempts - 1)
			}
			q := `SELECT * FROM cb_hide_map_tasks(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
			if _, qErr := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, []int64{msg.ID}, delay.Milliseconds()); qErr != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay map task retry", "flow", w.flowName, "step", w.step.name, "error", qErr)
			}
		}
		return
	}

	if h.CircuitBreaker != nil {
		h.CircuitBreaker.RecordSuccess()
	}

	q := `SELECT * FROM cb_complete_map_task(flow_name => $1, step_name => $2, map_task_id => $3, output => $4);`
	if _, qErr := execWithRetry(ctx, w.conn, q, w.flowName, w.step.name, msg.ID, itemOutput); qErr != nil {
		w.logger.ErrorContext(ctx, "worker: cannot mark map task as completed", "flow", w.flowName, "step", w.step.name, "error", qErr)
	}
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
