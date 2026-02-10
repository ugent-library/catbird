package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"
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
	flowSchedules   []flowSchedule
	taskSchedules   []taskSchedule
	shutdownTimeout time.Duration
}

type taskSchedule struct {
	taskName string
	schedule string
	inputFn  func() (any, error)
}

type flowSchedule struct {
	flowName string
	schedule string
	inputFn  func() (any, error)
}

// WorkerOpt is an option for configuring a worker
type WorkerOpt func(*Worker)

// WithTask registers a task with the worker
func WithTask(t *Task) WorkerOpt {
	return func(w *Worker) {
		w.tasks = append(w.tasks, t)
	}
}

// WithFlow registers a flow with the worker
func WithFlow(f *Flow) WorkerOpt {
	return func(w *Worker) {
		w.flows = append(w.flows, f)
	}
}

// WithLogger sets a custom logger for the worker. Default is slog.Default().
func WithLogger(l *slog.Logger) WorkerOpt {
	return func(w *Worker) {
		w.logger = l
	}
}

// WithShutdownTimeout sets the shutdown timeout for the worker. Default is 5 seconds.
func WithShutdownTimeout(d time.Duration) WorkerOpt {
	return func(w *Worker) {
		w.shutdownTimeout = d
	}
}

// WithScheduledTask registers a scheduled task execution using cron syntax.
// The input function can be used to provide dynamic input at execution time.
// If the input function is nil, an empty JSON object will be used as input to the task.
func WithScheduledTask(taskName string, schedule string, inputFn func() (any, error)) WorkerOpt {
	return func(w *Worker) {
		if inputFn == nil {
			inputFn = func() (any, error) { return struct{}{}, nil }
		}
		w.taskSchedules = append(w.taskSchedules, taskSchedule{
			taskName: taskName,
			schedule: schedule,
			inputFn:  inputFn,
		})
	}
}

// WithScheduledFlow registers a scheduled flow execution using cron syntax.
// The input function can be used to provide dynamic input at execution time.
// If the input function is nil, an empty JSON object will be used as input to the flow.
func WithScheduledFlow(flowName string, schedule string, inputFn func() (any, error)) WorkerOpt {
	return func(w *Worker) {
		if inputFn == nil {
			inputFn = func() (any, error) { return struct{}{}, nil }
		}
		w.flowSchedules = append(w.flowSchedules, flowSchedule{
			flowName: flowName,
			schedule: schedule,
			inputFn:  inputFn,
		})
	}
}

// NewWorker creates a new worker with the given options
// The worker will register all tasks and flows it has been configured with.
// Garbage collection is automatically enabled and runs every 5 minutes to clean up
// expired queues and stale worker heartbeats.
func NewWorker(ctx context.Context, conn Conn, opts ...WorkerOpt) (*Worker, error) {
	w := &Worker{
		id:              uuid.NewString(),
		conn:            conn,
		logger:          slog.Default(),
		shutdownTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(w)
	}

	// Register built-in garbage collection task (automatic, runs every 5 minutes)
	gcTask := NewTask("gc", func(ctx context.Context, in struct{}) (struct{}, error) {
		return struct{}{}, GC(ctx, w.conn)
	})
	w.tasks = append(w.tasks, gcTask)
	w.taskSchedules = append(w.taskSchedules, taskSchedule{
		taskName: "gc",
		schedule: "@every 5m",
	})

	for _, t := range w.tasks {
		if err := CreateTask(ctx, w.conn, t); err != nil {
			return nil, err
		}
	}

	for _, f := range w.flows {
		if err := CreateFlow(ctx, w.conn, f); err != nil {
			return nil, err
		}
	}

	return w, nil
}

// Start begins processing tasks and flows.
//
// The worker will:
//   - poll for new work and execute task and flow step handlers while ctx is active
//   - run any configured cron-style task and flow schedules
//   - send periodic heartbeats while it is running
//
// Shutdown behaviour:
//   - when ctx is cancelled the worker immediately stops reading new work and
//     begins shutting down
//   - if WithShutdownTimeout is set to a value > 0, that duration is used as a
//     grace period for in‑flight handlers after ctx is cancelled; once the
//     grace period expires the handler context is cancelled and remaining
//     handlers are asked to stop. The default shutdown timeout is 5 seconds.
//   - if WithShutdownTimeout is not set or set to 0, there is no grace period:
//     the handler context is cancelled immediately once ctx is cancelled and
//     Start returns after all goroutines finish
func (w *Worker) Start(ctx context.Context) error {
	var scheduler *cron.Cron
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

	for _, t := range w.tasks {
		if h := t.handler; h != nil {
			taskHandlers = append(taskHandlers, &TaskHandlerInfo{TaskName: t.Name})

			msgHider := newTaskHider(w.conn, w.logger, t.Name)

			startHandlerWorkers(
				ctx,
				handlerCtx,
				&wg,
				func(readCtx context.Context) ([]taskMessage, error) {
					msgs, err := readTasks(readCtx, w.conn, t.Name, h.batchSize, 10*time.Minute, 10*time.Second, 100*time.Millisecond)
					if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						w.logger.ErrorContext(ctx, "worker: cannot read messages", "handler", t.Name, "error", err)
					}
					return msgs, err
				},
				func(handleCtx context.Context, msg taskMessage) {
					w.handleTask(handleCtx, t, msg, msgHider.stopHiding)
				},
				msgHider,
				h.concurrency,
			)
		}
	}

	for _, f := range w.flows {
		for _, s := range f.Steps {
			if h := s.handler; h != nil {
				stepHandlers = append(stepHandlers, &StepHandlerInfo{FlowName: f.Name, StepName: s.Name})

				msgHider := newStepHider(w.conn, w.logger, f.Name, s.Name)

				startHandlerWorkers(
					ctx,
					handlerCtx,
					&wg,
					func(readCtx context.Context) ([]stepMessage, error) {
						msgs, err := readSteps(readCtx, w.conn, f.Name, s.Name, h.batchSize, 10*time.Minute, 10*time.Second, 100*time.Millisecond)
						if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
							w.logger.ErrorContext(ctx, "worker: cannot read steps", "flow", f.Name, "step", s.Name, "error", err)
						}
						return msgs, err
					},
					func(handleCtx context.Context, msg stepMessage) {
						w.handleStep(handleCtx, f.Name, s, msg, msgHider.stopHiding)
					},
					msgHider,
					h.concurrency,
				)
			}
		}
	}

	// TODO ensure tasks ands flows exist
	if len(w.taskSchedules) > 0 || len(w.flowSchedules) > 0 {
		scheduler = cron.New()

		for _, ts := range w.taskSchedules {
			var entryID cron.EntryID
			entryID, err := scheduler.AddFunc(ts.schedule, func() {
				entry := scheduler.Entry(entryID)
				scheduledTime := entry.Prev
				if scheduledTime.IsZero() {
					// use Next if Prev is not set, which will only happen for the first run
					scheduledTime = entry.Next
				}

				var inputJSON json.RawMessage
				if ts.inputFn != nil {
					input, err := ts.inputFn()
					if err != nil {
						w.logger.ErrorContext(ctx, "worker: failed to get scheduled task input", "task", ts.taskName, "error", err)
						return
					}
					inputJSON, err = json.Marshal(input)
					if err != nil {
						w.logger.ErrorContext(ctx, "worker: failed to marshal scheduled task input", "task", ts.taskName, "error", err)
						return
					}
				} else {
					inputJSON = json.RawMessage("{}")
				}

				_, err := RunTaskWithOpts(ctx, w.conn, ts.taskName, inputJSON, RunTaskOpts{
					DeduplicationID: fmt.Sprint(scheduledTime),
				})
				if err != nil {
					w.logger.ErrorContext(ctx, "worker: failed to schedule task", "task", ts.taskName, "error", err)
				}
			})
			if err != nil {
				return fmt.Errorf("worker: failed to register schedule for task %s: %w", ts.taskName, err)
			}
		}

		for _, fs := range w.flowSchedules {
			var entryID cron.EntryID
			entryID, err := scheduler.AddFunc(fs.schedule, func() {
				entry := scheduler.Entry(entryID)
				scheduledTime := entry.Prev
				if scheduledTime.IsZero() {
					// use Next if Prev is not set, which will only happen for the first run
					scheduledTime = entry.Next
				}

				var inputJSON json.RawMessage
				if fs.inputFn != nil {
					input, err := fs.inputFn()
					if err != nil {
						w.logger.ErrorContext(ctx, "worker: failed to get scheduled flow input", "flow", fs.flowName, "error", err)
						return
					}
					inputJSON, err = json.Marshal(input)
					if err != nil {
						w.logger.ErrorContext(ctx, "worker: failed to marshal scheduled flow input", "flow", fs.flowName, "error", err)
						return
					}
				} else {
					inputJSON = json.RawMessage("{}")
				}

				_, err := RunFlowWithOpts(ctx, w.conn, fs.flowName, inputJSON, RunFlowOpts{
					DeduplicationID: fmt.Sprint(scheduledTime),
				})
				if err != nil {
					w.logger.ErrorContext(ctx, "worker: failed to schedule flow", "flow", fs.flowName, "error", err)
				}
			})
			if err != nil {
				return fmt.Errorf("worker: failed to register schedule for flow %s: %w", fs.flowName, err)
			}
		}

		scheduler.Start()

		wg.Go(func() {
			<-ctx.Done()

			stopCtx := scheduler.Stop()
			select {
			case <-stopCtx.Done():
			case <-time.After(w.shutdownTimeout):
			}
		})
	}

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
		defer close(done)
		wg.Wait()
	}()

	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			// Shutdown requested. If a shutdown timeout is configured, give
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
}

func (w *Worker) handleTask(ctx context.Context, t *Task, msg taskMessage, stopHiding func(id int64)) {
	h := t.handler

	w.logger.DebugContext(ctx, "worker: handleTask",
		"task", t.Name,
		"id", msg.ID,
		"deliveries", msg.Deliveries,
		"input", string(msg.Input),
	)

	fnCtx := ctx
	if h.maxDuration > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, time.Duration(h.maxDuration))
		defer cancel()
	}

	var out json.RawMessage
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("task handler panic: %v", r)
			}
		}()
		out, err = h.fn(fnCtx, msg)
	}()

	stopHiding(msg.ID)

	if err != nil {
		w.logger.ErrorContext(ctx, "worker: failed", "task", t.Name, "error", err)

		if msg.Deliveries > h.maxRetries {
			q := `SELECT * FROM cb_fail_task(name => $1, id => $2, error_message => $3);`
			if _, err := w.conn.Exec(ctx, q, t.Name, msg.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark task as failed", "task", t.Name, "error", err)
			}
		} else {
			if err := hideTasks(ctx, w.conn, t.Name, []int64{msg.ID}, backoffWithFullJitter(msg.Deliveries-1, h.minDelay, h.maxDelay)); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next run", "task", t.Name, "error", err)
			}
		}
	} else {
		q := `SELECT * FROM cb_complete_task(name => $1, id => $2, output => $3);`
		if _, err := w.conn.Exec(ctx, q, t.Name, msg.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark task as completed", "task", t.Name, "error", err)
		}
	}
}

func (w *Worker) handleStep(ctx context.Context, flowName string, s *Step, msg stepMessage, stopHiding func(id int64)) {
	h := s.handler

	if w.logger.Enabled(ctx, slog.LevelDebug) {
		stepOutputsJSON, _ := json.Marshal(msg.StepOutputs)
		w.logger.DebugContext(ctx, "worker: handleStep",
			"flow", flowName,
			"step", s.Name,
			"id", msg.ID,
			"deliveries", msg.Deliveries,
			"flow_input", string(msg.FlowInput),
			"step_outputs", string(stepOutputsJSON),
		)
	}

	fnCtx := ctx
	if h.maxDuration > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, time.Duration(h.maxDuration))
		defer cancel()
	}

	var out json.RawMessage
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("step handler panic: %v", r)
			}
		}()
		out, err = h.fn(fnCtx, msg)
	}()

	stopHiding(msg.ID)

	if err != nil {
		w.logger.ErrorContext(ctx, "worker: failed", "flow", flowName, "step", s.Name, "error", err)

		if msg.Deliveries > h.maxRetries {
			q := `SELECT * FROM cb_fail_step(flow_name => $1, step_name => $2, step_id => $3, error_message => $4);`
			if _, err := w.conn.Exec(ctx, q, flowName, s.Name, msg.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark step as failed", "flow", flowName, "step", s.Name, "error", err)
			}
		} else {
			if err := hideSteps(ctx, w.conn, flowName, s.Name, []int64{msg.ID}, backoffWithFullJitter(msg.Deliveries-1, h.minDelay, h.maxDelay)); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next run", "flow", flowName, "step", s.Name, "error", err)
			}
		}
	} else {
		q := `SELECT * FROM cb_complete_step(flow_name => $1, step_name => $2, step_id => $3, output => $4);`
		if _, err := w.conn.Exec(ctx, q, flowName, s.Name, msg.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark step as completed", "flow", flowName, "step", s.Name, "error", err)
		}
	}
}

func startHandlerWorkers[T handlerMessage](
	shutdownCtx context.Context,
	handlerCtx context.Context,
	wg *sync.WaitGroup,
	readFn func(context.Context) ([]T, error),
	handleFn func(context.Context, T),
	msgHider *messageHider[T],
	concurrency int,
) {
	// hider
	wg.Go(func() {
		msgHider.start(handlerCtx)
	})

	// producer
	msgChan := make(chan T)
	wg.Go(func() {
		defer close(msgChan)
		for {
			// Check for cancellation before reading
			select {
			case <-shutdownCtx.Done():
				return
			default:
			}

			msgs, err := readFn(shutdownCtx)
			if err != nil {
				return
			}
			if len(msgs) == 0 {
				continue
			}

			msgHider.hideMessages(msgs)

			for _, msg := range msgs {
				select {
				case <-shutdownCtx.Done():
					return
				case msgChan <- msg:
				}
			}
		}
	})

	// consumers
	for i := 0; i < concurrency; i++ {
		wg.Go(func() {
			for msg := range msgChan {
				handleFn(handlerCtx, msg)
			}
		})
	}
}

type taskMessage struct {
	ID         int64           `json:"id"`
	Deliveries int             `json:"deliveries"`
	Input      json.RawMessage `json:"input"`
}

func (m taskMessage) getID() int64 { return m.ID }

func readTasks(ctx context.Context, conn Conn, name string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]taskMessage, error) {
	q := `SELECT id, deliveries, input FROM cb_read_tasks(name => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`

	rows, err := conn.Query(ctx, q, name, quantity, hideFor.Milliseconds(), pollFor.Milliseconds(), pollInterval.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("worker: read tasks: %w", err)
	}

	return pgx.CollectRows(rows, scanCollectibleTaskMessage)
}

func readSteps(ctx context.Context, conn Conn, flowName, stepName string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]stepMessage, error) {
	q := `SELECT id, deliveries, flow_input, step_outputs FROM cb_read_steps(flow_name => $1, step_name => $2, quantity => $3, hide_for => $4, poll_for => $5, poll_interval => $6);`

	rows, err := conn.Query(ctx, q, flowName, stepName, quantity, hideFor.Milliseconds(), pollFor.Milliseconds(), pollInterval.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("worker: read steps: %w", err)
	}

	return pgx.CollectRows(rows, scanCollectibleStepMessage)
}

func hideTasks(ctx context.Context, conn Conn, name string, ids []int64, hideFor time.Duration) error {
	q := `SELECT * FROM cb_hide_tasks(name => $1, ids => $2, hide_for => $3);`
	_, err := conn.Exec(ctx, q, name, ids, hideFor.Milliseconds())
	return fmt.Errorf("worker: hide tasks: %w", err)
}

func hideSteps(ctx context.Context, conn Conn, flowName, stepName string, ids []int64, hideFor time.Duration) error {
	q := `SELECT * FROM cb_hide_steps(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
	_, err := conn.Exec(ctx, q, flowName, stepName, ids, hideFor.Milliseconds())
	return fmt.Errorf("worker: hide steps: %w", err)
}

type messageHider[T handlerMessage] struct {
	conn     Conn
	logger   *slog.Logger
	ids      map[int64]struct{}
	mu       sync.Mutex
	hideFn   func(context.Context, Conn, []int64, time.Duration) error
	logAttrs []any
}

func newTaskHider(conn Conn, logger *slog.Logger, name string) *messageHider[taskMessage] {
	return &messageHider[taskMessage]{
		conn:   conn,
		logger: logger,
		ids:    make(map[int64]struct{}),
		hideFn: func(ctx context.Context, conn Conn, ids []int64, hideFor time.Duration) error {
			return hideTasks(ctx, conn, name, ids, hideFor)
		},
		logAttrs: []any{"name", name},
	}
}

func newStepHider(conn Conn, logger *slog.Logger, flowName, stepName string) *messageHider[stepMessage] {
	return &messageHider[stepMessage]{
		conn:   conn,
		logger: logger,
		ids:    make(map[int64]struct{}),
		hideFn: func(ctx context.Context, conn Conn, ids []int64, hideFor time.Duration) error {
			return hideSteps(ctx, conn, flowName, stepName, ids, hideFor)
		},
		logAttrs: []any{"flow", flowName, "step", stepName},
	}
}

func (mh *messageHider[T]) start(ctx context.Context) {
	hideFor := 10 * time.Minute
	hideTicker := time.NewTicker(30 * time.Second)
	defer hideTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hideTicker.C:
			mh.mu.Lock()
			numIDs := len(mh.ids)
			if numIDs == 0 {
				mh.mu.Unlock()
				continue
			}
			ids := make([]int64, 0, numIDs)
			for id := range mh.ids {
				ids = append(ids, id)
			}
			mh.mu.Unlock()

			if err := mh.hideFn(ctx, mh.conn, ids, hideFor); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					attrs := append(mh.logAttrs, "error", err)
					mh.logger.ErrorContext(ctx, "worker: cannot hide messages", attrs...)
				}
			}
		}
	}
}

func (mh *messageHider[T]) hideMessages(msgs []T) {
	mh.mu.Lock()
	for _, msg := range msgs {
		mh.ids[msg.getID()] = struct{}{}
	}
	mh.mu.Unlock()
}

func (mh *messageHider[T]) stopHiding(id int64) {
	mh.mu.Lock()
	delete(mh.ids, id)
	mh.mu.Unlock()
}

// backoffWithFullJitter calculates the next backoff duration using full jitter strategy.
// The first retryAttempt should be 0.
func backoffWithFullJitter(retryAttempt int, minDelay, maxDelay time.Duration) time.Duration {
	delay := time.Duration(float64(minDelay) * math.Pow(2, float64(retryAttempt)))
	if delay > maxDelay {
		delay = maxDelay
	}
	return time.Duration(rand.Int63n(int64(delay)))
}

func scanCollectibleTaskMessage(row pgx.CollectableRow) (taskMessage, error) {
	return scanTaskMessage(row)
}

func scanTaskMessage(row pgx.Row) (taskMessage, error) {
	rec := taskMessage{}

	if err := row.Scan(
		&rec.ID,
		&rec.Deliveries,
		&rec.Input,
	); err != nil {
		return rec, err
	}

	return rec, nil
}

func scanCollectibleStepMessage(row pgx.CollectableRow) (stepMessage, error) {
	return scanStepMessage(row)
}

func scanStepMessage(row pgx.Row) (stepMessage, error) {
	rec := stepMessage{}

	var stepOutputs *map[string]json.RawMessage

	if err := row.Scan(
		&rec.ID,
		&rec.Deliveries,
		&rec.FlowInput,
		&stepOutputs,
	); err != nil {
		return rec, err
	}

	if stepOutputs != nil {
		rec.StepOutputs = *stepOutputs
	}

	return rec, nil
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
