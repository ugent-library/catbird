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
	inFlightIDs   map[int64]struct{}
	inFlightIDsMu sync.Mutex
}

func newStepWorker(conn Conn, logger *slog.Logger, flowName string, step *Step) *stepWorker {
	return &stepWorker{
		conn:        conn,
		logger:      logger,
		flowName:    flowName,
		step:        step,
		inFlightIDs: make(map[int64]struct{}),
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
	wg.Go(func() {
		defer close(messages)

		for {
			select {
			case <-shutdownCtx.Done():
				return
			default:
			}

			msgs, err := w.readMessages(shutdownCtx)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.ErrorContext(shutdownCtx, "worker: cannot read steps", "flow", w.flowName, "step", w.step.Name, "error", err)
				}
				return // TODO: consider pausing and retrying on transient errors instead of exiting
			}
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

func (w *stepWorker) getInFlightIDs() []int64 {
	w.inFlightIDsMu.Lock()
	defer w.inFlightIDsMu.Unlock()

	ids := make([]int64, 0, len(w.inFlightIDs))
	for id := range w.inFlightIDs {
		ids = append(ids, id)
	}
	return ids
}

func (w *stepWorker) markInFlight(msgs []stepMessage) {
	w.inFlightIDsMu.Lock()
	for _, msg := range msgs {
		w.inFlightIDs[msg.ID] = struct{}{}
	}
	w.inFlightIDsMu.Unlock()
}

func (w *stepWorker) removeInFlight(id int64) {
	w.inFlightIDsMu.Lock()
	delete(w.inFlightIDs, id)
	w.inFlightIDsMu.Unlock()
}

func (w *stepWorker) hideInFlight(ctx context.Context) {
	ids := w.getInFlightIDs()
	if len(ids) == 0 {
		return
	}

	q := `SELECT * FROM cb_hide_steps(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
	_, err := w.conn.Exec(ctx, q, w.flowName, w.step.Name, w.getInFlightIDs(), (10 * time.Minute).Milliseconds())

	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.logger.ErrorContext(ctx, "worker: cannot hide in-flight steps", "flow", w.flowName, "step", w.step.Name, "error", err)
	}
}

func (w *stepWorker) readMessages(ctx context.Context) ([]stepMessage, error) {
	h := w.step.handler

	q := `SELECT id, deliveries, flow_input, step_outputs FROM cb_read_steps(flow_name => $1, step_name => $2, quantity => $3, hide_for => $4, poll_for => $5, poll_interval => $6);`

	rows, err := w.conn.Query(ctx, q, w.flowName, w.step.Name, h.batchSize, (10 * time.Minute).Milliseconds(), (10 * time.Second).Milliseconds(), (100 * time.Millisecond).Milliseconds())
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
			if _, err := w.conn.Exec(ctx, q, w.flowName, w.step.Name, msg.ID, err.Error()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot mark step as failed", "flow", w.flowName, "step", w.step.Name, "error", err)
			}
		} else {
			delay := backoffWithFullJitter(msg.Deliveries-1, h.minDelay, h.maxDelay)
			q := `SELECT * FROM cb_hide_steps(flow_name => $1, step_name => $2, ids => $3, hide_for => $4);`
			if _, err := w.conn.Exec(ctx, q, w.flowName, w.step.Name, []int64{msg.ID}, delay.Milliseconds()); err != nil {
				w.logger.ErrorContext(ctx, "worker: cannot delay next step run", "flow", w.flowName, "step", w.step.Name, "error", err)
			}
		}
	} else {
		q := `SELECT * FROM cb_complete_step(flow_name => $1, step_name => $2, step_id => $3, output => $4);`
		if _, err := w.conn.Exec(ctx, q, w.flowName, w.step.Name, msg.ID, out); err != nil {
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
