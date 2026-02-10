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
	messages := make(chan stepMessage, 100)

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

				w.logger.ErrorContext(shutdownCtx, "worker: cannot read step messages", "flow", w.flowName, "step", w.step.Name, "error", err)

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
	for i := 0; i < w.step.handler.concurrency; i++ {
		wg.Go(func() {
			for msg := range messages {
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

func (w *stepWorker) markInFlight(msgs []stepMessage) {
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
	_, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.Name, w.getInFlight(), (10 * time.Minute).Milliseconds())

	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.logger.ErrorContext(ctx, "worker: cannot hide in-flight steps", "flow", w.flowName, "step", w.step.Name, "error", err)
	}
}

func (w *stepWorker) readMessages(ctx context.Context) ([]stepMessage, error) {
	h := w.step.handler

	q := `SELECT id, deliveries, flow_input, step_outputs FROM cb_read_steps(flow_name => $1, step_name => $2, quantity => $3, hide_for => $4, poll_for => $5, poll_interval => $6);`

	rows, err := queryWithRetry(ctx, w.conn, q, w.flowName, w.step.Name, h.batchSize, (10 * time.Minute).Milliseconds(), (10 * time.Second).Milliseconds(), (100 * time.Millisecond).Milliseconds())
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleStepMessage)
}

func (w *stepWorker) handle(ctx context.Context, msg stepMessage) {
	defer w.removeInFlight(msg.ID)

	h := w.step.handler

	if w.logger.Enabled(ctx, slog.LevelDebug) {
		stepOutputsJSON, _ := json.Marshal(msg.StepOutputs)
		w.logger.DebugContext(ctx, "worker: handleStep",
			"flow", w.flowName,
			"step", w.step.Name,
			"id", msg.ID,
			"deliveries", msg.Deliveries,
			"flow_input", string(msg.FlowInput),
			"step_outputs", string(stepOutputsJSON),
		)
	}

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
				err = fmt.Errorf("step handler panic: %v", r)
			}
		}()
		out, err = h.fn(fnCtx, msg)
	}()

	// Handle result
	if err != nil {
		w.logger.ErrorContext(ctx, "worker: failed", "flow", w.flowName, "step", w.step.Name, "error", err)

		if msg.Deliveries > h.maxRetries {
			q := `SELECT * FROM cb_fail_step(flow_name => $1, step_name => $2, step_id => $3, error_message => $4);`
			if _, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.Name, msg.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark step as failed", "flow", w.flowName, "step", w.step.Name, "error", err)
			}
		} else {
			delay := backoffWithFullJitter(msg.Deliveries-1, h.minDelay, h.maxDelay)
			q := `SELECT * FROM cb_hide_steps(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
			if _, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.Name, []int64{msg.ID}, delay.Milliseconds()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next step run", "flow", w.flowName, "step", w.step.Name, "error", err)
			}
		}
	} else {
		q := `SELECT * FROM cb_complete_step(flow_name => $1, step_name => $2, step_id => $3, output => $4);`
		if _, err := execWithRetry(ctx, w.conn, q, w.flowName, w.step.Name, msg.ID, out); err != nil {
			w.logger.ErrorContext(ctx, "worker: cannot mark step as completed", "flow", w.flowName, "step", w.step.Name, "error", err)
		}
	}
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
