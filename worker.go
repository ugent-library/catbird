package catbird

import (
	"context"
	"encoding/json"
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
	tasks           []Task
	flows           []Flow
	scheduler       *Scheduler
	shutdownTimeout time.Duration
}

// WorkerOpt is an option for configuring a worker
type WorkerOpt func(*Worker)

// WithTask registers a task with the worker
func WithTask(t Task) WorkerOpt {
	return func(w *Worker) {
		w.tasks = append(w.tasks, t)
	}
}

// WithFlow registers a flow with the worker
func WithFlow(f Flow) WorkerOpt {
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

// WithGracefulShutdown sets the graceful shutdown timeout for the worker. Default is 5 seconds.
func WithGracefulShutdown(d time.Duration) WorkerOpt {
	return func(w *Worker) {
		w.shutdownTimeout = d
	}
}

// WithScheduledTask registers a scheduled task execution using cron syntax.
// The WithInput option can be used to provide dynamic input at execution time.
// Otherwise an empty JSON object will be used as input to the task.
func WithScheduledTask(taskName string, schedule string, opts ...ScheduleOpt) WorkerOpt {
	return func(w *Worker) {
		if w.scheduler == nil {
			w.scheduler = NewScheduler(w.conn, w.logger)
		}
		w.scheduler.AddTask(taskName, schedule, opts...)
	}
}

// WithScheduledFlow registers a scheduled flow execution using cron syntax.
// The WithInput option can be used to provide dynamic input at execution time.
// Otherwise an empty JSON object will be used as input to the flow.
func WithScheduledFlow(flowName string, schedule string, opts ...ScheduleOpt) WorkerOpt {
	return func(w *Worker) {
		if w.scheduler == nil {
			w.scheduler = NewScheduler(w.conn, w.logger)
		}
		w.scheduler.AddFlow(flowName, schedule, opts...)
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
	}, nil)
	w.tasks = append(w.tasks, gcTask)
	if w.scheduler == nil {
		w.scheduler = NewScheduler(w.conn, w.logger)
	}
	w.scheduler.AddTask("gc", "@every 5m")

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
//   - if WithGracefulShutdown is set to a value > 0, that duration is used as a
//     grace period for in‑flight handlers after ctx is cancelled; once the
//     grace period expires the handler context is cancelled and remaining
//     handlers are asked to stop. The default graceful shutdown timeout is 5 seconds.
//   - if WithGracefulShutdown is not set or set to 0, there is no grace period:
//     the handler context is cancelled immediately once ctx is cancelled and
//     Start returns after all goroutines finish
func (w *Worker) Start(ctx context.Context) error {
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
		if t.handlerOpts() != nil {
			taskHandlers = append(taskHandlers, &TaskHandlerInfo{TaskName: t.Name()})
			worker := newTaskWorker(w.conn, w.logger, t)
			worker.start(ctx, handlerCtx, &wg)
		}
	}

	// Start step workers
	for _, f := range w.flows {
		for _, s := range f.Steps() {
			if s.handlerOpts() != nil {
				stepHandlers = append(stepHandlers, &StepHandlerInfo{FlowName: f.Name(), StepName: s.Name()})
				worker := newStepWorker(w.conn, w.logger, f.Name(), s)
				worker.start(ctx, handlerCtx, &wg)
			}
		}
	}

	// Start scheduler if configured
	if w.scheduler != nil {
		if err := w.scheduler.Start(ctx); err != nil {
			return err
		}

		wg.Go(func() {
			<-ctx.Done()
			w.scheduler.Stop(handlerCtx)
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
