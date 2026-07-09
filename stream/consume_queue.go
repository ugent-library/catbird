package stream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConsumeQueueOpts tunes the queue consume loop. Zero fields mean the defaults.
type ConsumeQueueOpts struct {
	PollInterval time.Duration // 250ms: how often to look for new messages when caught up
}

const consumeQueueBatchSize = 100

// newConsumerName generates a unique name for a consumer. It consists
// of the hostname, pid and a random suffix.
func newConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "consumer"
	}
	host = strings.ReplaceAll(host, ".", "_") // internal stream names use dots
	var r [8]byte
	rand.Read(r[:])
	return fmt.Sprintf("%s_%d_%x", host, os.Getpid(), r)
}

// ConsumeQueue processes a queue's messages: unordered, at-least-once,
// parallel across consumers. Failures are retried with the queue's backoff
// policy and dead-lettered when attempts run out. A message slower than the
// queue's claim_ttl is indistinguishable from a crash — size claim_ttl for
// the slowest message the queue can see.
func ConsumeQueue(ctx context.Context, pool *pgxpool.Pool, stream, queue string,
	handler func(ctx context.Context, msg Message) error,
	opts ...ConsumeQueueOpts,
) error {
	var o ConsumeQueueOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	poll := o.PollInterval
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}

	consumer := newConsumerName()
	// The queue's failed messages are republished to its retry stream, which
	// exists once something has failed; probe until it does.
	retryStream := "sr." + stream + "." + queue
	retryReady := false

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

		var loopErr error
		worked := false

		if !retryReady {
			loopErr = pool.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM cb_stream_queues q WHERE q.stream = $1 AND q.name = $2)`,
				retryStream, queue).Scan(&retryReady)
		}
		if loopErr == nil {
			streams := []string{stream}
			if retryReady {
				streams = append(streams, retryStream)
			}
			for _, s := range streams {
				n, err := consumeClaim(ctx, pool, s, queue, consumer, handler)
				if err != nil {
					loopErr = err
					break
				}
				worked = worked || n
			}
		}

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
func consumeClaim(ctx context.Context, pool *pgxpool.Pool, stream, queue, consumer string,
	handler func(ctx context.Context, msg Message) error,
) (bool, error) {
	var fromPos, toPos *int64
	var expiresAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT c.from_pos, c.to_pos, c.expires_at FROM cb_stream_claim($1, $2, $3, $4) c`,
		stream, queue, consumer, consumeQueueBatchSize,
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
			stream, queue, consumer, *fromPos)
	}

	rows, err := pool.Query(ctx, `
		SELECT m.id, m.stream, m.pos, coalesce(m.topic, ''), m.payload, m.headers, m.created_at
		FROM cb_stream_messages m
		WHERE m.stream = $1 AND m.pos BETWEEN $2 AND $3
		ORDER BY m.pos`,
		stream, *fromPos, *toPos)
	if err != nil {
		return true, err
	}
	msgs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
	if err != nil {
		return true, wrapErr(err)
	}

	ttl := time.Until(*expiresAt)
	exp := *expiresAt
	for _, m := range msgs {
		if ctx.Err() != nil {
			release()
			return true, ctx.Err()
		}

		// extend between messages when the claim nears its deadline; a NULL
		// return means it expired and another consumer adopted it: stop
		if time.Until(exp) < ttl/2 {
			var newExp *time.Time
			if err := pool.QueryRow(ctx,
				`SELECT cb_stream_extend_claim($1, $2, $3, $4)`,
				stream, queue, consumer, *fromPos).Scan(&newExp); err != nil {
				return true, wrapErr(err)
			}
			if newExp == nil {
				return true, nil
			}
			exp = *newExp
		}

		if err := handler(ctx, m); err != nil {
			if ctx.Err() != nil {
				release() // shutdown, not a failure: no attempt is spent
				return true, ctx.Err()
			}
			if _, err := pool.Exec(ctx,
				`SELECT cb_stream_fail($1, $2, $3, $4)`,
				stream, queue, m.Pos, err.Error()); err != nil {
				return true, wrapErr(err)
			}
		}
	}

	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_close_claim($1, $2, $3, $4)`,
		stream, queue, consumer, *fromPos); err != nil {
		return true, wrapErr(err)
	}
	return true, nil
}
