package streams

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/claimloop"
)

// ConsumeSubscriptionOpts tunes the subscription consume loop. Zero fields mean the defaults.
type ConsumeSubscriptionOpts struct {
	PollInterval time.Duration // 250ms: how often to look for new messages when caught up
}

// ConsumeSubscription processes a subscription's messages: unordered, at-least-once,
// parallel across consumers. The claim is kept alive for as long as
// a handler runs. Handler errors are retried with the subscription's backoff policy
// and kept or deleted when attempts run out. Failed and crashed
// messages come back as due retry rows, served by the same claim call.
func ConsumeSubscription(ctx context.Context, pool *pgxpool.Pool, stream, subscription string,
	handler func(ctx context.Context, msg Message) error,
	opts ...ConsumeSubscriptionOpts,
) error {
	var o ConsumeSubscriptionOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	poll := o.PollInterval
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}

	consumer := claimloop.Name("consumer")

	return claimloop.Run(ctx, claimloop.Options{
		Poll: poll,
		// misconfiguration, not a transient failure
		Fatal: func(err error) bool { return errors.Is(err, ErrNotDefined) },
	}, func(ctx context.Context) (bool, error) {
		return consumeClaim(ctx, pool, stream, subscription, consumer, handler)
	})
}

// consumeClaim runs one claim cycle: claim a range, handle its messages,
// close it. It reports whether there was anything to claim.
func consumeClaim(ctx context.Context, pool *pgxpool.Pool, stream, subscription, consumer string,
	handler func(ctx context.Context, msg Message) error,
) (bool, error) {
	var fromPos, toPos *int64
	var expiresAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT c.from_pos, c.to_pos, c.expires_at FROM cb_stream_claim($1, $2, $3) c`,
		stream, subscription, consumer,
	).Scan(&fromPos, &toPos, &expiresAt); err != nil {
		return false, wrapErr(err)
	}
	if fromPos == nil {
		return false, nil
	}

	// hand the rest of the range back on shutdown so another consumer can
	// pick it up now instead of at the claim's expiry
	release := func() {
		_, _ = pool.Exec(context.WithoutCancel(ctx),
			`SELECT cb_stream_release_claim($1, $2, $3, $4)`,
			stream, subscription, consumer, *fromPos)
	}

	rows, err := pool.Query(ctx, `
		SELECT m.id, m.stream, m.pos, coalesce(m.topic, ''), m.payload, m.headers, m.created_at
		FROM cb_stream_read_claim($1, $2, $3, $4) m`,
		stream, subscription, *fromPos, *toPos)
	if err != nil {
		return true, err
	}
	msgs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
	if err != nil {
		return true, wrapErr(err)
	}

	// keep the claim alive: extend between messages when the deadline nears,
	// and on a steady cadence while a handler runs. An empty extend should
	// never happen while this loop runs on schedule — it means the process
	// was frozen past the ttl and another consumer took the claim: stop.
	ttl := time.Until(*expiresAt)
	exp := *expiresAt
	extend := func() (bool, error) {
		var newExp *time.Time
		if err := pool.QueryRow(ctx,
			`SELECT cb_stream_extend_claim($1, $2, $3, $4)`,
			stream, subscription, consumer, *fromPos).Scan(&newExp); err != nil {
			return false, wrapErr(err)
		}
		if newExp == nil {
			return false, nil
		}
		exp = *newExp
		return true, nil
	}

	for _, m := range msgs {
		if ctx.Err() != nil {
			release()
			return true, ctx.Err()
		}

		if time.Until(exp) < ttl/2 {
			ok, err := extend()
			if err != nil {
				return true, err
			}
			if !ok {
				return true, nil // adopted away; the new owner takes it from here
			}
		}

		verdict, err := claimloop.Handle(ctx, ttl/2, extend,
			func(elapsed time.Duration) {
				slog.Info("catbird: handler still running",
					"stream", stream, "subscription", subscription, "pos", m.Pos,
					"elapsed", elapsed)
			},
			func(hctx context.Context) error { return handler(hctx, m) })
		switch {
		case errors.Is(err, claimloop.ErrLost):
			return true, nil
		case err != nil:
			return true, err // extends cannot be delivered; not the message's fault
		case verdict == nil:
		case ctx.Err() != nil:
			release() // shutdown, not a failure: no attempt is spent
			return true, ctx.Err()
		default:
			if _, err := pool.Exec(ctx,
				`SELECT cb_stream_fail($1, $2, $3, $4, $5)`,
				stream, subscription, consumer, m.Pos, verdict.Error()); err != nil {
				return true, wrapErr(err)
			}
		}
	}

	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_close_claim($1, $2, $3, $4)`,
		stream, subscription, consumer, *fromPos); err != nil {
		return true, wrapErr(err)
	}
	return true, nil
}
