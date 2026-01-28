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
	handlerType  int
	name         string
	taskName     string
	flowName     string
	stepName     string
	queue        string
	batchSize    int
	concurrency  int
	retries      int
	delay        time.Duration
	jitterFactor float64
	timeout      time.Duration
	schedule     string
	fn           func(context.Context, []byte) ([]byte, error)
}

func NewTaskHandler[Input, Output any](taskName string, fn func(context.Context, Input) (Output, error), opts ...HandlerOpt) *Handler {
	h := &Handler{
		handlerType:  handlerTypeTask,
		name:         taskName,
		taskName:     taskName,
		queue:        "t_" + taskName,
		batchSize:    10,
		concurrency:  1,
		jitterFactor: 0.1, // 10% jitter hardcoded for now
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
	for _, opt := range opts {
		opt.applyToHandler(h)
	}

	return h
}

func NewPeriodicTaskHandler[Input, Output any](taskName, schedule string, fn func(context.Context, Input) (Output, error), opts ...HandlerOpt) *Handler {
	t := NewTaskHandler(taskName, fn, opts...)
	t.schedule = schedule
	return t
}

func NewStepHandler[Input, Output any](flowName, stepName string, fn func(context.Context, Input) (Output, error), opts ...HandlerOpt) *Handler {
	h := &Handler{
		handlerType:  handlerTypeStep,
		name:         flowName + "/" + stepName,
		flowName:     flowName,
		stepName:     stepName,
		queue:        "f_" + flowName + "_" + stepName,
		batchSize:    10,
		concurrency:  1,
		jitterFactor: 0.1, // 10% jitter hardcoded for now
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
	for _, opt := range opts {
		opt.applyToHandler(h)
	}

	return h
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
		msgHider := newMessageHider(w.conn, h, w.logger)

		wg.Go(func() {
			msgHider.start(ctx)
		})

		// producer
		wg.Go(func() {
			defer close(msgChan)

			for {
				msgs, err := ReadPoll(ctx, w.conn, h.queue, h.batchSize, delayWithJitter(10*time.Minute, h.jitterFactor), ReadPollOpts{})
				if err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						w.logger.ErrorContext(ctx, "worker: cannot read messages", "handler", h.name, "error", err)
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
					w.handle(ctx, h, msg, msgHider.stopHiding)
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

func (w *Worker) handle(ctx context.Context, h *Handler, msg Message, stopHiding func(id int64)) {
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

	stopHiding(msg.ID)

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
			if _, err := Hide(ctx, w.conn, h.queue, msg.ID, delayWithJitter(h.delay, h.jitterFactor)); err != nil {
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

type messageHider struct {
	conn   Conn
	h      *Handler
	logger *slog.Logger
	ids    map[int64]struct{}
	mu     sync.Mutex
}

func newMessageHider(conn Conn, h *Handler, logger *slog.Logger) *messageHider {
	return &messageHider{
		conn:   conn,
		h:      h,
		logger: logger,
		ids:    make(map[int64]struct{}),
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

			if err := HideMany(ctx, mh.conn, mh.h.queue, ids, delayWithJitter(hideFor, mh.h.jitterFactor)); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					mh.logger.ErrorContext(ctx, "worker: cannot hide messages", "handler", mh.h.name, "error", err)
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

func delayWithJitter(delay time.Duration, jitterFactor float64) time.Duration {
	if jitterFactor == 0 {
		return delay
	}
	randomFactor := 1 + (1-rand.Float64()*2)*jitterFactor
	return time.Duration(float64(delay) * randomFactor)
}
