package streams

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConsumeOpts tunes the consume loop. Zero fields mean the defaults.
type ConsumeOpts struct {
	BatchSize    int           // 100
	PollInterval time.Duration // 250ms: how often to look for new messages when caught up
}

const maxConsumeBackoff = 30 * time.Second

// Consume processes the stream in order: at-least-once, so handlers must be
// idempotent. A failing batch retries in place with backoff; a batch is
// redelivered whole when the process dies before finishing it. For
// exactly-once processing, run Read inside your own transaction.
func Consume(ctx context.Context, pool *pgxpool.Pool, stream, cursor string,
	handler func(ctx context.Context, batch []Message) error,
	opts ...ConsumeOpts,
) error {
	var o ConsumeOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	batchSize := o.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	tick := o.PollInterval
	if tick <= 0 {
		tick = 250 * time.Millisecond
	}

	timer := time.NewTimer(tick)
	defer timer.Stop()
	wait := func(d time.Duration) error {
		timer.Reset(d)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}

	backoff := tick
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var n int
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			batch, err := Read(ctx, tx, stream, cursor, batchSize)
			n = len(batch)
			if err != nil || n == 0 {
				return err
			}
			// an error rolls back the cursor with the transaction: the
			// same batch is read again on the next pass
			return handler(ctx, batch)
		})
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(err, ErrNotDefined):
			return err // misconfiguration, not a transient failure
		case err != nil:
			if err := wait(backoff); err != nil {
				return err
			}
			backoff = min(backoff*2, maxConsumeBackoff)
		case n > 0:
			backoff = tick // a full pass succeeded; drain the backlog
		default:
			backoff = tick
			if err := wait(tick); err != nil {
				return err
			}
		}
	}
}
