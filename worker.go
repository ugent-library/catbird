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

type taskSchedule struct {
	taskName string
	schedule string
}

type flowSchedule struct {
	flowName string
	schedule string
}

type Worker struct {
	id            string
	conn          Conn
	logger        *slog.Logger
	tasks         []*Task
	flows         []*Flow
	flowSchedules []flowSchedule
	taskSchedules []taskSchedule
	timeout       time.Duration
}

func NewWorker(conn Conn, opts ...WorkerOpt) (*Worker, error) {
	w := &Worker{
		id:     uuid.NewString(),
		conn:   conn,
		logger: slog.Default(),
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
		if err := CreateTask(ctx, w.conn, t); err != nil {
			return err
		}

		if h := t.handler; h != nil {
			handlerNames = append(handlerNames, t.Name)

			queueName := fmt.Sprintf("t_%s", t.Name)
			msgChan := make(chan Message)
			msgHider := newMessageHider(w.conn, w.logger, t.Name, queueName, h.jitterFactor)

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
						w.handleTask(ctx, t, queueName, msg, msgHider.stopHiding)
					}
				})
			}
		}
	}

	for _, f := range w.flows {
		if err := CreateFlow(ctx, w.conn, f); err != nil {
			return err
		}

		for _, s := range f.Steps {
			if h := s.handler; h != nil {
				handlerNames = append(handlerNames, fmt.Sprintf("%s/%s", f.Name, s.Name))

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
			case <-time.After(w.timeout):
			}
		})
	}

	if _, err := w.conn.Exec(ctx, `SELECT * FROM cb_worker_started(id => $1, handlers => $2);`, w.id, handlerNames); err != nil {
		return err
	}

	wg.Wait()

	return nil
}

type handlerPayload struct {
	ID    string          `json:"id,omitempty"`
	Input json.RawMessage `json:"input"`
}

func (w *Worker) handleTask(ctx context.Context, t *Task, queueName string, msg Message, stopHiding func(id int64)) {
	h := t.handler

	var p handlerPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		w.logger.ErrorContext(ctx, "worker: cannot decode payload", "handler", t.Name, "error", err)
		return // TODO
	}

	w.logger.DebugContext(ctx, "worker: handleTask",
		"handler", t.Name,
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
		w.logger.ErrorContext(ctx, "worker: failed", "handler", t.Name, "error", err)

		if msg.Deliveries > h.retries {
			q := `SELECT * FROM cb_fail_task(id => $1, error_message => $2);`
			if _, err := w.conn.Exec(ctx, q, p.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark task as failed", "handler", t.Name, "error", err)
			}
		} else if h.retryDelay > 0 {
			if _, err := Hide(ctx, w.conn, queueName, msg.ID, withJitter(h.retryDelay, h.jitterFactor)); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next run", "handler", t.Name, "error", err)
			}
		}
	} else {
		q := `SELECT * FROM cb_complete_task(id => $1, output => $2);`
		if _, err := w.conn.Exec(ctx, q, p.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark task as completed", "handler", t.Name, "error", err)
		}
	}
}

func (w *Worker) handleStep(ctx context.Context, s *Step, queueName string, msg Message, stopHiding func(id int64)) {
	h := s.handler

	var p handlerPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		w.logger.ErrorContext(ctx, "worker: cannot decode payload", "handler", s.Name, "error", err)
		return // TODO
	}

	w.logger.DebugContext(ctx, "worker: handleStep",
		"name", s.Name,
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
