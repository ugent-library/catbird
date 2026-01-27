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
	"github.com/robfig/cron/v3"
)

const (
	handlerTypeTask = iota
	handlerTypeStep
)

type Handler struct {
	handlerType int
	name        string
	taskName    string
	flowName    string
	stepName    string
	queue       string
	batchSize   int
	hideFor     time.Duration
	concurrency int
	retries     int
	delay       time.Duration
	jitter      time.Duration
	timeout     time.Duration
	schedule    string
	fn          func(context.Context, []byte) ([]byte, error)
}

type TaskHandlerOpts struct {
	BatchSize   int
	HideFor     time.Duration
	Concurrency int
	Retries     int
	Delay       time.Duration
	Jitter      time.Duration
	Timeout     time.Duration
	Schedule    string
}

func NewTaskHandler[Input, Output any](taskName string, fn func(context.Context, Input) (Output, error), opts TaskHandlerOpts) *Handler {
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}

	return &Handler{
		handlerType: handlerTypeTask,
		name:        taskName,
		taskName:    taskName,
		queue:       "t_" + taskName,
		batchSize:   opts.BatchSize,
		hideFor:     opts.HideFor,
		concurrency: opts.Concurrency,
		retries:     opts.Retries,
		delay:       opts.Delay,
		jitter:      opts.Jitter,
		timeout:     opts.Timeout,
		schedule:    opts.Schedule,
		fn: func(ctx context.Context, b []byte) ([]byte, error) {
			var in Input
			if err := json.Unmarshal(b, &in); err != nil {
				return nil, err
			}

			out, err := fn(ctx, in)
			if err != nil {
				return nil, err
			}

			return json.Marshal(out)
		},
	}
}

type StepHandlerOpts struct {
	BatchSize   int
	HideFor     time.Duration
	Concurrency int
	Retries     int
	Delay       time.Duration
	Jitter      time.Duration
	Timeout     time.Duration
}

func NewStepHandler[Input, Output any](flowName, stepName string, fn func(context.Context, Input) (Output, error), opts StepHandlerOpts) *Handler {
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}

	return &Handler{
		handlerType: handlerTypeStep,
		name:        flowName + "/" + stepName,
		flowName:    flowName,
		stepName:    stepName,
		queue:       "f_" + flowName + "_" + stepName,
		batchSize:   opts.BatchSize,
		hideFor:     opts.HideFor,
		concurrency: opts.Concurrency,
		retries:     opts.Retries,
		delay:       opts.Delay,
		jitter:      opts.Jitter,
		timeout:     opts.Timeout,
		fn: func(ctx context.Context, b []byte) ([]byte, error) {
			var in Input
			if err := json.Unmarshal(b, &in); err != nil {
				return nil, err
			}

			out, err := fn(ctx, in)
			if err != nil {
				return nil, err
			}

			return json.Marshal(out)
		},
	}
}

type Worker struct {
	id       string
	conn     Conn
	handlers []*Handler
	logger   *slog.Logger
	timeout  time.Duration
}

func NewWorker(conn Conn, handlers []*Handler, opts ...WorkerOpt) (*Worker, error) {
	w := &Worker{
		id:       uuid.NewString(),
		conn:     conn,
		handlers: handlers,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt.applyToWorker(w)
	}

	return w, nil
}

func (w *Worker) Start(ctx context.Context) error {
	var scheduler *cron.Cron
	var handlerNames []string
	var wg sync.WaitGroup

	for _, h := range w.handlers {
		handlerNames = append(handlerNames, h.name)

		if h.handlerType == handlerTypeTask {
			if err := CreateTask(ctx, w.conn, h.taskName); err != nil {
				return err
			}

			if h.schedule != "" {
				if scheduler == nil {
					scheduler = cron.New()
				}

				var entryID cron.EntryID
				entryID, err := scheduler.AddFunc(h.schedule, func() {
					entry := scheduler.Entry(entryID)
					scheduledTime := entry.Prev
					if scheduledTime.IsZero() {
						// use Next if Prev is not set, which will only happen for the first run
						scheduledTime = entry.Next
					}
					dedupID := fmt.Sprintf("%s-cron-%s", h.name, scheduledTime)
					_, err := RunTask(ctx, w.conn, h.name, struct{}{}, RunTaskOpts{dedupID})
					if err != nil {
						w.logger.ErrorContext(ctx, "worker: failed to schedule task", "task", h.name, "error", err)
					}
				})
				if err != nil {
					return fmt.Errorf("worker: failed to register schedule for task %s: %w", h.name, err)
				}
			}
		}

	}

	if _, err := w.conn.Exec(ctx, `SELECT * FROM cb_worker_started(id => $1, handlers => $2);`, w.id, handlerNames); err != nil {
		return err
	}

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

	if scheduler != nil {
		scheduler.Start()

		wg.Go(func() {
			<-ctx.Done()

			stopCtx := scheduler.Stop()
			select {
			case <-stopCtx.Done():
			case <-time.After(w.timeout):
			}
		})
	}

	for _, h := range w.handlers {
		msgChan := make(chan Message, h.concurrency)

		// producer
		wg.Go(func() {
			defer func() {
				close(msgChan)
			}()

			for {
				msgs, err := ReadPoll(ctx, w.conn, h.queue, h.batchSize, h.hideFor, ReadPollOpts{})
				if err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						w.logger.ErrorContext(ctx, "worker: cannot read messages", "handler", h.name, "error", err)
					}
					return
				}

				for _, msg := range msgs {
					select {
					case <-ctx.Done():
						return
					default:
						msgChan <- msg
					}
				}

				if len(msgs) < h.batchSize {
					time.Sleep(1 * time.Second) // TODO
				}
			}
		})

		// consumers
		for i := 0; i < h.concurrency; i++ {
			wg.Go(func() {
				for msg := range msgChan {
					w.handle(ctx, h, msg)
				}
			})
		}
	}

	wg.Wait()

	return nil
}

type handlerPayload struct {
	ID    string          `json:"id,omitempty"`
	Input json.RawMessage `json:"input"`
}

func (w *Worker) handle(ctx context.Context, h *Handler, msg Message) {
	var p handlerPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		w.logger.ErrorContext(ctx, "worker: cannot decode payload", "handler", h.name, "error", err)
		return // TODO
	}

	w.logger.DebugContext(ctx, "worker: handle",
		"handler", h.name,
		"id", p.ID,
		"deduplication_id", msg.DeduplicationID,
		"message_id", msg.ID,
		"input", string(p.Input),
	)

	fnCtx := ctx
	if h.timeout > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, time.Duration(h.timeout))
		defer cancel()
	}

	out, err := h.fn(fnCtx, p.Input)

	if err != nil {
		w.logger.ErrorContext(ctx, "worker: failed", "handler", h.name, "error", err)

		if msg.Deliveries > h.retries {
			if h.handlerType == handlerTypeTask {
				q := `SELECT * FROM cb_fail_task(id => $1, error_message => $2);`
				if _, err := w.conn.Exec(ctx, q, p.ID, err.Error()); err != nil {
					w.logger.ErrorContext(ctx, "worker: cannot mark task as failed", "handler", h.name, "error", err)
				}
			} else {
				q := `SELECT * FROM cb_fail_step(id => $1, error_message => $2);`
				if _, err := w.conn.Exec(ctx, q, p.ID, err.Error()); err != nil {
					w.logger.ErrorContext(ctx, "worker: cannot mark step as failed", "handler", h.name, "error", err)
				}
			}
		} else if h.delay > 0 {
			delay := h.delay
			if h.jitter > 0 {
				delay += time.Duration((1 - rand.Float64()*2) * float64(h.jitter))
			}

			if _, err := Hide(ctx, w.conn, h.queue, msg.ID, delay); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next run", "handler", h.name, "error", err)
			}
		}
	} else if h.handlerType == handlerTypeTask {
		q := `SELECT * FROM cb_complete_task(id => $1, output => $2);`
		if _, err := w.conn.Exec(ctx, q, p.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark task as completed", "handler", h.name, "error", err)
		}
	} else {
		q := `SELECT * FROM cb_complete_step(id => $1, output => $2);`
		if _, err := w.conn.Exec(ctx, q, p.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark step as completed", "handler", h.name, "error", err)
		}
	}
}
