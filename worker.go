package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"
)

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
}

type flowSchedule struct {
	flowName string
	schedule string
}

type WorkerOpt interface {
	apply(*Worker)
}

type taskOpt struct {
	task *Task
}

func (o taskOpt) apply(w *Worker) {
	w.tasks = append(w.tasks, o.task)
}

func WithTask(t *Task) WorkerOpt {
	return &taskOpt{task: t}
}

type flowOpt struct {
	flow *Flow
}

func (o flowOpt) apply(w *Worker) {
	w.flows = append(w.flows, o.flow)
}

func WithFlow(f *Flow) WorkerOpt {
	return &flowOpt{flow: f}
}

type loggerOpt struct {
	logger *slog.Logger
}

func (o loggerOpt) apply(w *Worker) {
	w.logger = o.logger
}

func WithLogger(l *slog.Logger) WorkerOpt {
	return &loggerOpt{logger: l}
}

type shutdownTimeoutOpt struct {
	shutdownTimeout time.Duration
}

func WithShutdownTimeout(d time.Duration) WorkerOpt {
	return &shutdownTimeoutOpt{shutdownTimeout: d}
}

func (o shutdownTimeoutOpt) apply(w *Worker) {
	w.shutdownTimeout = o.shutdownTimeout
}

type scheduledTaskOpt struct {
	taskName string
	schedule string
}

func (o scheduledTaskOpt) apply(w *Worker) {
	w.taskSchedules = append(w.taskSchedules, taskSchedule{
		taskName: o.taskName,
		schedule: o.schedule,
	})
}

func WithScheduledTask(taskName, schedule string) WorkerOpt {
	return &scheduledTaskOpt{taskName: taskName, schedule: schedule}
}

type scheduledFlowOpt struct {
	flowName string
	schedule string
}

func (o scheduledFlowOpt) apply(w *Worker) {
	w.flowSchedules = append(w.flowSchedules, flowSchedule{
		flowName: o.flowName,
		schedule: o.schedule,
	})
}

func WithScheduledFlow(flowName, schedule string) WorkerOpt {
	return &scheduledFlowOpt{flowName: flowName, schedule: schedule}
}

type gcOpt struct {
	schedule string
}

func WithGC() WorkerOpt {
	return &gcOpt{schedule: "@every 10m"}
}

func (o *gcOpt) Schedule(schedule string) *gcOpt {
	o.schedule = schedule
	return o
}

func (o gcOpt) apply(w *Worker) {
	w.tasks = append(w.tasks, NewTask("gc", func(ctx context.Context, input struct{}) (struct{}, error) {
		return struct{}{}, GC(ctx, w.conn)
	}))
	w.taskSchedules = append(w.taskSchedules, taskSchedule{
		taskName: "gc",
		schedule: o.schedule,
	})
}

func NewWorker(ctx context.Context, conn Conn, opts ...WorkerOpt) (*Worker, error) {
	w := &Worker{
		id:     uuid.NewString(),
		conn:   conn,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt.apply(w)
	}

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

func (w *Worker) Start(ctx context.Context) error {
	var scheduler *cron.Cron
	var wg sync.WaitGroup
	var taskHandlers []TaskHandlerInfo
	var stepHandlers []StepHandlerInfo

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
			taskHandlers = append(taskHandlers, TaskHandlerInfo{TaskName: t.Name})

			msgChan := make(chan taskMessage)
			msgHider := newTaskHider(w.conn, w.logger, t.Name, h.jitterFactor)

			wg.Go(func() {
				msgHider.start(ctx)
			})

			// producer
			wg.Go(func() {
				defer close(msgChan)

				for {
					msgs, err := readTasks(ctx, w.conn, t.Name, h.batchSize, withJitter(10*time.Minute, h.jitterFactor), 10*time.Second, 100*time.Millisecond)
					if err != nil {
						if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
							w.logger.ErrorContext(ctx, "worker: cannot read messages", "handler", t.Name, "error", err)
						}
						return
					}
					if len(msgs) == 0 {
						continue
					}

					msgHider.hideMessages(msgs)

					for _, msg := range msgs {
						select {
						case <-ctx.Done():
							return
						default:
							msgChan <- msg
						}
					}
				}
			})

			// // consumers
			for i := 0; i < h.concurrency; i++ {
				wg.Go(func() {
					for msg := range msgChan {
						w.handleTask(ctx, t, msg, msgHider.stopHiding)
					}
				})
			}
		}
	}

	for _, f := range w.flows {
		for _, s := range f.Steps {
			if h := s.handler; h != nil {
				stepHandlers = append(stepHandlers, StepHandlerInfo{FlowName: f.Name, StepName: s.Name})

				queueName := fmt.Sprintf("f_%s_%s", f.Name, s.Name)
				msgChan := make(chan Message)
				msgHider := newMessageHider(w.conn, w.logger, s.Name, queueName, h.jitterFactor)

				wg.Go(func() {
					msgHider.start(ctx)
				})

				// producer
				wg.Go(func() {
					defer close(msgChan)

					for {
						msgs, err := ReadPoll(ctx, w.conn, queueName, h.batchSize, withJitter(10*time.Minute, h.jitterFactor), 10*time.Second, 100*time.Millisecond)
						if err != nil {
							if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
								w.logger.ErrorContext(ctx, "worker: cannot read messages", "handler", s.Name, "error", err)
							}
							return
						}
						if len(msgs) == 0 {
							continue
						}

						msgHider.hideMessages(msgs)

						for _, msg := range msgs {
							select {
							case <-ctx.Done():
								return
							default:
								msgChan <- msg
							}
						}
					}
				})

				// consumers
				for i := 0; i < h.concurrency; i++ {
					wg.Go(func() {
						for msg := range msgChan {
							w.handleStep(ctx, s, queueName, msg, msgHider.stopHiding)
						}
					})
				}
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

				_, err := RunTaskWithOpts(ctx, w.conn, ts.taskName, struct{}{}, RunTaskOpts{
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

				_, err := RunFlowWithOpts(ctx, w.conn, fs.flowName, struct{}{}, RunFlowOpts{
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
		return err
	}

	wg.Wait()

	return nil
}

func (w *Worker) handleTask(ctx context.Context, t *Task, msg taskMessage, stopHiding func(id int64)) {
	h := t.handler

	w.logger.DebugContext(ctx, "worker: handleTask",
		"task", t.Name,
		"id", msg.ID,
		"deduplication_id", msg.DeduplicationID,
		"input", string(msg.Input),
	)

	fnCtx := ctx
	if h.timeout > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, time.Duration(h.timeout))
		defer cancel()
	}

	out, err := h.fn(fnCtx, msg.Input)

	stopHiding(msg.ID)

	if err != nil {
		w.logger.ErrorContext(ctx, "worker: failed", "task", t.Name, "error", err)

		if msg.Deliveries > h.retries {
			q := `SELECT * FROM cb_fail_task(name => $1, id => $2, error_message => $3);`
			if _, err := w.conn.Exec(ctx, q, msg.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark task as failed", "task", t.Name, "error", err)
			}
		} else if h.retryDelay > 0 {
			if err := hideTasks(ctx, w.conn, t.Name, []int64{msg.ID}, withJitter(h.retryDelay, h.jitterFactor)); err != nil {
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

func (w *Worker) handleStep(ctx context.Context, s *Step, queueName string, msg Message, stopHiding func(id int64)) {
	h := s.handler

	var p stepPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		w.logger.ErrorContext(ctx, "worker: cannot decode payload", "handler", s.Name, "error", err)
		return // TODO
	}

	w.logger.DebugContext(ctx, "worker: handleStep",
		"name", s.Name,
		"id", p.ID,
		"deduplication_id", msg.DeduplicationID,
		"message_id", msg.ID,
		"payload", string(msg.Payload),
	)

	fnCtx := ctx
	if h.timeout > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, time.Duration(h.timeout))
		defer cancel()
	}

	out, err := h.fn(fnCtx, p)

	stopHiding(msg.ID)

	if err != nil {
		w.logger.ErrorContext(ctx, "worker: failed", "handler", s.Name, "error", err)

		if msg.Deliveries > h.retries {
			q := `SELECT * FROM cb_fail_step(id => $1, error_message => $2);`
			if _, err := w.conn.Exec(ctx, q, p.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark step as failed", "handler", s.Name, "error", err)
			}
		} else if h.retryDelay > 0 {
			if _, err := Hide(ctx, w.conn, queueName, msg.ID, withJitter(h.retryDelay, h.jitterFactor)); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next run", "handler", s.Name, "error", err)
			}
		}
	} else {
		q := `SELECT * FROM cb_complete_step(id => $1, output => $2);`
		if _, err := w.conn.Exec(ctx, q, p.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark step as completed", "handler", s.Name, "error", err)
		}
	}
}

type messageHider struct {
	conn         Conn
	logger       *slog.Logger
	ids          map[int64]struct{}
	mu           sync.Mutex
	name         string
	queue        string
	jitterFactor float64
}

func newMessageHider(conn Conn, logger *slog.Logger, name, queue string, jitterFactor float64) *messageHider {
	return &messageHider{
		conn:         conn,
		logger:       logger,
		ids:          make(map[int64]struct{}),
		name:         name,
		queue:        queue,
		jitterFactor: jitterFactor,
	}
}

func (mh *messageHider) start(ctx context.Context) {
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

			if err := HideMany(ctx, mh.conn, mh.queue, ids, withJitter(hideFor, mh.jitterFactor)); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					mh.logger.ErrorContext(ctx, "worker: cannot hide messages", "handler", mh.name, "error", err)
				}
			}
		}
	}
}

func (mh *messageHider) hideMessages(msgs []Message) {
	mh.mu.Lock()
	for _, msg := range msgs {
		mh.ids[msg.ID] = struct{}{}
	}
	mh.mu.Unlock()
}

func (mh *messageHider) stopHiding(id int64) {
	mh.mu.Lock()
	delete(mh.ids, id)
	mh.mu.Unlock()
}

func withJitter(delay time.Duration, jitterFactor float64) time.Duration {
	if jitterFactor == 0 {
		return delay
	}
	randomFactor := 1 + (1-rand.Float64()*2)*jitterFactor
	return time.Duration(float64(delay) * randomFactor)
}

type taskMessage struct {
	ID              int64           `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Input           json.RawMessage `json:"input"`
	Deliveries      int             `json:"deliveries"`
}

func readTasks(ctx context.Context, conn Conn, name string, quantity int, hideFor, pollFor, pollInterval time.Duration) ([]taskMessage, error) {
	q := `SELECT * FROM cb_read_tasks(name => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`

	rows, err := conn.Query(ctx, q, name, quantity, hideFor.Seconds(), pollFor.Seconds(), pollInterval.Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleTaskMessage)
}

func hideTasks(ctx context.Context, conn Conn, name string, ids []int64, hideFor time.Duration) error {
	q := `SELECT * FROM cb_hide_tasks(name => $1, ids => $2, hide_for => $3);`
	_, err := conn.Exec(ctx, q, name, ids, hideFor.Seconds())
	return err
}

type taskHider struct {
	conn         Conn
	logger       *slog.Logger
	ids          map[int64]struct{}
	mu           sync.Mutex
	name         string
	jitterFactor float64
}

func newTaskHider(conn Conn, logger *slog.Logger, name string, jitterFactor float64) *taskHider {
	return &taskHider{
		conn:         conn,
		logger:       logger,
		ids:          make(map[int64]struct{}),
		name:         name,
		jitterFactor: jitterFactor,
	}
}

func (mh *taskHider) start(ctx context.Context) {
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

			if err := hideTasks(ctx, mh.conn, mh.name, ids, withJitter(hideFor, mh.jitterFactor)); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					mh.logger.ErrorContext(ctx, "worker: cannot hide tasks", "name", mh.name, "error", err)
				}
			}
		}
	}
}

func (mh *taskHider) hideMessages(msgs []taskMessage) {
	mh.mu.Lock()
	for _, msg := range msgs {
		mh.ids[msg.ID] = struct{}{}
	}
	mh.mu.Unlock()
}

func (mh *taskHider) stopHiding(id int64) {
	mh.mu.Lock()
	delete(mh.ids, id)
	mh.mu.Unlock()
}

func scanCollectibleTaskMessage(row pgx.CollectableRow) (taskMessage, error) {
	return scanTaskMessage(row)
}

func scanTaskMessage(row pgx.Row) (taskMessage, error) {
	rec := taskMessage{}

	var deduplicationID *string

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&rec.Input,
		&rec.Deliveries,
	); err != nil {
		return rec, err
	}

	if deduplicationID != nil {
		rec.DeduplicationID = *deduplicationID
	}

	return rec, nil
}
