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

	rows, err := queryWithRetry(ctx, w.conn, q, w.flowName, w.step.name, h.BatchSize, (10 * time.Minute).Milliseconds(), (10 * time.Second).Milliseconds(), (100 * time.Millisecond).Milliseconds())
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
