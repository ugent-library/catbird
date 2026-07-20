package streams

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/claimloop"
	"github.com/ugent-library/catbird/notify"
)

// ConsumeOpts tunes the consume loop. Zero fields mean the defaults.
type ConsumeOpts struct {
	BatchSize    int              // 100
	PollInterval time.Duration    // 250ms: how often to look for new messages when caught up
	Notifier     *notify.Notifier // wakes the consumer the moment new messages are readable; nil = wake by poll only
}

// notifyChannel prefixes a channel name with the connection's schema —
// the SQL notifies on '<schema>.<name>'.
func notifyChannel(ctx context.Context, pool *pgxpool.Pool, name string) (string, error) {
	var schema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return "", err
	}
	return schema + "." + name, nil
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
	pollInterval := o.PollInterval
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}

	var wake <-chan struct{}
	if o.Notifier != nil {
		channel, err := notifyChannel(ctx, pool, "cbs_"+stream)
		if err != nil {
			return err
		}
		waker := notify.NewWaker()
		defer waker.Stop()
		cancel := o.Notifier.Subscribe(channel,
			func(string) { waker.Wake() },
			func() { waker.Wake() })
		defer cancel()
		wake = waker.C
		slog.Info(fmt.Sprintf("catbird: consumer waking on notify, poll safety net every %s", pollInterval),
			"stream", stream, "cursor", cursor)
	} else {
		slog.Info(fmt.Sprintf("catbird: consumer waking by poll every %s", pollInterval),
			"stream", stream, "cursor", cursor)
	}

	return claimloop.Run(ctx, claimloop.Options{
		PollInterval: pollInterval,
		Wake:         wake,
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
