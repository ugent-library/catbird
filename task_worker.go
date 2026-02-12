package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

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
	messages := make(chan taskMessage)

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
		defer close(messages)

		retryAttempt := 0

		for {
			select {
			case <-shutdownCtx.Done():
				return
			default:
			}

			msgs, err := w.readMessages(shutdownCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				w.logger.ErrorContext(shutdownCtx, "worker: cannot read task messages", "task", w.task.Name, "error", err)

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
				case messages <- msg:
				}
			}
		}
	})

	// Start consumers
	for i := 0; i < w.task.handler.concurrency; i++ {
		wg.Go(func() {
			for msg := range messages {
				w.handle(handlerCtx, msg)
			}
		})
	}
}

func (w *taskWorker) markInFlight(msgs []taskMessage) {
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
	_, err := execWithRetry(ctx, w.conn, q, w.task.Name, ids, (10 * time.Minute).Milliseconds())

	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.logger.ErrorContext(ctx, "worker: cannot hide in-flight tasks", "task", w.task.Name, "error", err)
	}
}

func (w *taskWorker) readMessages(ctx context.Context) ([]taskMessage, error) {
	h := w.task.handler

	q := `SELECT id, deliveries, input FROM cb_read_tasks(name => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.task.Name, h.batchSize, (10 * time.Minute).Milliseconds(), (10 * time.Second).Milliseconds(), (100 * time.Millisecond).Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleTaskMessage)
}

func (w *taskWorker) handle(ctx context.Context, msg taskMessage) {
	defer w.removeInFlight(msg.ID)

	h := w.task.handler

	if h.circuitBreaker != nil {
		allowed, delay := h.circuitBreaker.allow(time.Now())
		if !allowed {
			if delay <= 0 {
				delay = time.Second
			}
			w.logger.WarnContext(ctx, "worker: circuit breaker open", "task", w.task.Name, "retry_in", delay)
			delayMs := delay.Milliseconds()
			if delayMs <= 0 {
				delayMs = 1
			}
			q := `SELECT * FROM cb_hide_tasks(name => $1, ids => $2, hide_for => $3);`
			if _, err := execWithRetry(ctx, w.conn, q, w.task.Name, []int64{msg.ID}, delayMs); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay task due to open circuit", "task", w.task.Name, "error", err)
			}
			return
		}
	}

	w.logger.DebugContext(ctx, "worker: handleTask",
		"task", w.task.Name,
		"id", msg.ID,
		"deliveries", msg.Deliveries,
		"input", string(msg.Input),
	)

	// Execute with timeout if configured
	fnCtx := ctx
	if h.maxDuration > 0 {
		var cancel context.CancelFunc
		fnCtx, cancel = context.WithTimeout(ctx, h.maxDuration)
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

	// Handle result
	if err != nil {
		if h.circuitBreaker != nil {
			h.circuitBreaker.recordFailure(time.Now())
		}
		w.logger.ErrorContext(ctx, "worker: failed", "task", w.task.Name, "error", err)

		if msg.Deliveries > h.maxRetries {
			q := `SELECT * FROM cb_fail_task(name => $1, id => $2, error_message => $3);`
			if _, err := execWithRetry(ctx, w.conn, q, w.task.Name, msg.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark task as failed", "task", w.task.Name, "error", err)
			}
		} else {
			delay := backoffWithFullJitter(msg.Deliveries-1, h.minDelay, h.maxDelay)
			q := `SELECT * FROM cb_hide_tasks(name => $1, ids => $2, hide_for => $3);`
			if _, err := execWithRetry(ctx, w.conn, q, w.task.Name, []int64{msg.ID}, delay.Milliseconds()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next task run", "task", w.task.Name, "error", err)
			}
		}
	} else {
		if h.circuitBreaker != nil {
			h.circuitBreaker.recordSuccess()
		}
		q := `SELECT * FROM cb_complete_task(name => $1, id => $2, output => $3);`
		if _, err := execWithRetry(ctx, w.conn, q, w.task.Name, msg.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark task as completed", "task", w.task.Name, "error", err)
		}
	}
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
