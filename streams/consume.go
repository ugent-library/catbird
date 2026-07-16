package streams

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/claimloop"
)

// ConsumeOpts tunes the consume loop. Zero fields mean the defaults.
type ConsumeOpts struct {
	BatchSize    int           // 100
	PollInterval time.Duration // 250ms: how often to look for new messages when caught up
}

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
	poll := o.PollInterval
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}

	return claimloop.Run(ctx, claimloop.Options{
		Poll: poll,
		// misconfiguration, not a transient failure
		Fatal: func(err error) bool { return errors.Is(err, ErrNotDefined) },
	}, func(ctx context.Context) (bool, error) {
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
		return n > 0, err
	})
}
