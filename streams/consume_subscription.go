package streams

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConsumeSubscriptionOpts tunes the subscription consume loop. Zero fields mean the defaults.
type ConsumeSubscriptionOpts struct {
	PollInterval time.Duration // 250ms: how often to look for new messages when caught up
}

// errClaimLost reports that a claim expired and another consumer adopted it.
var errClaimLost = errors.New("catbird: claim lost")

// newConsumerName generates a unique name for a consumer. It consists
// of the hostname, pid and a random suffix.
func newConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "consumer"
	}
	host = strings.ReplaceAll(host, ".", "_") // keep the generated consumer name dot-free
	var r [8]byte
	rand.Read(r[:])
	return fmt.Sprintf("%s_%d_%x", host, os.Getpid(), r)
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

	consumer := newConsumerName()

	timer := time.NewTimer(poll)
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

	backoff := poll
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		worked, loopErr := consumeClaim(ctx, pool, stream, subscription, consumer, handler)

		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.Is(loopErr, ErrNotDefined):
			return loopErr // misconfiguration, not a transient failure
		case loopErr != nil:
			if err := wait(backoff); err != nil {
				return err
			}
			backoff = min(backoff*2, maxConsumeBackoff)
		case worked:
			backoff = poll
		default:
			backoff = poll
			if err := wait(poll); err != nil {
				return err
			}
		}
	}
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

		verdict, err := runHandler(ctx, stream, subscription, ttl, m, extend, handler)
		switch {
		case errors.Is(err, errClaimLost):
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

// runHandler runs one handler call while the extend cadence keeps the claim
// alive. The verdict is the handler's own outcome — its error, or its panic
// reported as one. err is the loop's outcome: errClaimLost when the claim
// expired anyway and another consumer took it, or the failed extend. On a
// non-nil err the handler is canceled and awaited first, so one consumer
// never runs two handlers at once, and the verdict is meaningless.
func runHandler(ctx context.Context, stream, subscription string, ttl time.Duration, m Message,
	extend func() (bool, error),
	handler func(ctx context.Context, msg Message) error,
) (verdict error, err error) {
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("catbird: handler panic: %v", r)
			}
		}()
		done <- handler(hctx, m)
	}()

	interval := ttl / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	started := time.Now()
	for {
		select {
		case verdict := <-done:
			return verdict, nil
		case <-ticker.C:
			ok, err := extend()
			if err == nil && ok {
				slog.Info("catbird: handler still running",
					"stream", stream, "subscription", subscription, "pos", m.Pos,
					"elapsed", time.Since(started))
				continue
			}
			// the claim is no longer ours, or extends cannot be delivered:
			// stop the handler and wait for it before handing off
			cancel()
			<-done
			if err != nil {
				return nil, err
			}
			return nil, errClaimLost
		}
	}
}
