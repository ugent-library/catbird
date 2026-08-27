package catbird

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// JobHandler is the function signature users implement.
// By accepting DBRunner (a transaction), all application side-effects and the framework's claim cleanup
// are perfectly fenced into a single atomic PostgreSQL commit.
type JobHandler func(ctx context.Context, tx DBRunner, msg Message) error

type Worker struct {
	pool     *pgxpool.Pool
	queue    string
	handler  JobHandler
	cleanup  JobHandler // Optional fallback execution
	lease    time.Duration
	wakeChan chan struct{}
}

func NewWorker(pool *pgxpool.Pool, queue string, handler JobHandler) *Worker {
	return &Worker{
		pool:     pool,
		queue:    queue,
		handler:  handler,
		lease:    5 * time.Minute,
		wakeChan: make(chan struct{}, 1),
	}
}

// WithCleanup defines an explicit failure handler
func (w *Worker) WithCleanup(fn JobHandler) *Worker {
	w.cleanup = fn
	return w
}

func (w *Worker) Start(ctx context.Context) {
	// Sub-millisecond Latency Lowering via LISTEN/NOTIFY bridging to a Go channel
	go func() {
		conn, err := w.pool.Acquire(ctx)
		if err != nil {
			return
		}
		defer conn.Release()

		conn.Exec(ctx, "LISTEN cb_queue_"+w.queue)
		for {
			_, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				return
			}
			select {
			case w.wakeChan <- struct{}{}:
			default:
				// Channel already has a wake signal, safe to ignore
			}
		}
	}()

	for {
		// Claim up to 50 jobs at once using standard SKIP LOCKED
		msgs, err := w.claimBatch(ctx, 50)

		if err == nil && len(msgs) > 0 {
			// Process the batch concurrently
			var wg sync.WaitGroup
			for _, msg := range msgs {
				wg.Add(1)
				go func(m Message) {
					defer wg.Done()

					// FENCING: Wrap both the user's handler and the cleanup in a single transaction
					tx, err := w.pool.Begin(ctx)
					if err != nil {
						return // Cannot process without a transaction
					}

					err = w.handler(ctx, WrapDBRunner(tx), m)

					if err == nil {
						// The Bloat Killer: Deleting the claim inside the SAME context
						tx.Exec(ctx, `DELETE FROM cb_claims WHERE message_id = $1`, m.ID)
						tx.Exec(ctx, `DELETE FROM cb_signals WHERE message_id = $1`, m.ID)

						// Commit perfectly fences both the user's DB updates and our claim deletions
						err = tx.Commit(ctx)
						if err != nil {
							// If commit fails, the connection dropped. Postgres drops everything safely.
							return
						}
					} else {
						tx.Rollback(ctx)

						if m.Attempts >= 5 {
							w.dead(ctx, m.ID)
							if m.CorrelationID != nil {
								client := NewClient()
								client.CancelCascade(ctx, w.pool, *m.CorrelationID)
							}
							if w.cleanup != nil {
								// Note: Cleanup runs outside the primary fencing TX boundary since it initiates fallback chains
								w.cleanup(ctx, WrapDBRunner(w.pool), m)
							}
						} else {
							w.fail(ctx, m.ID, 1*time.Minute)
						}
					}
				}(msg)
			}
			wg.Wait() // Wait for batch to finish before claiming next batch
		}

		// Nothing to do. Sleep or Wait for INSTANT wake.
		select {
		case <-ctx.Done():
			return
		case <-w.wakeChan:
			// Instantly wakes up upon enqueue
		case <-time.After(5 * time.Second):
			// Fallback poll unblocks delayed executions that just hit their `visible_at` window
		}
	}
}

func (w *Worker) claimBatch(ctx context.Context, limit int) ([]Message, error) {
	rows, err := w.pool.Query(ctx, `
		WITH leased AS (
			UPDATE cb_claims
			SET status = 1, visible_at = now() + $2, attempts = attempts + 1
			WHERE message_id IN (
				SELECT message_id FROM cb_claims
				WHERE queue = $1 AND status = 0 AND visible_at <= now() AND dependencies = 0
				ORDER BY visible_at ASC LIMIT $3 FOR UPDATE SKIP LOCKED
			) RETURNING message_id, attempts, correlation_id
		)
		SELECT m.id, m.topic, m.payload, l.attempts, l.correlation_id,
		       (SELECT jsonb_object_agg(name, payload) FROM cb_signals s WHERE s.message_id = m.id) as signals
		FROM cb_messages m
		JOIN leased l ON m.id = l.message_id;
	`, w.queue, w.lease, limit)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var sigs []byte
		if err := rows.Scan(&m.ID, &m.Topic, &m.Payload, &m.Attempts, &m.CorrelationID, &sigs); err == nil {
			if len(sigs) > 0 {
				json.Unmarshal(sigs, &m.Signals)
			}
			msgs = append(msgs, m)
		}
	}
	return msgs, rows.Err()
}

func (w *Worker) fail(ctx context.Context, id int64, backoff time.Duration) {
	w.pool.Exec(ctx, `UPDATE cb_claims SET status = 0, visible_at = now() + $2 WHERE message_id = $1`, id, backoff)
}

func (w *Worker) dead(ctx context.Context, id int64) {
	w.pool.Exec(ctx, `UPDATE cb_claims SET status = 3 WHERE message_id = $1`, id)
}
