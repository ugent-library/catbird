package catbird

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ConsumeOptions are the optional parts of Consume. Zero values take the
// defaults. ClaimDuration and HandlerTimeout follow the queue's rule, see
// QueueOptions: each one left unset defaults from the other so that the claim
// covers the handler, and a HandlerTimeout above ClaimDuration makes the
// consumer renew its claim while a batch runs.
type ConsumeOptions struct {
	BatchSize    int           // messages per handler call; default 50
	PollInterval time.Duration // wait when the stream is empty, another process holds the cursor, or a batch failed; default 2 seconds

	// ClaimDuration is how long the cursor stays with a process that stopped
	// renewing it, and so how long it stays stuck when that process crashes.
	// Unset, it is HandlerTimeout plus the few seconds the ack needs; five
	// minutes when neither is set.
	ClaimDuration time.Duration

	// HandlerTimeout bounds one batch: the handler's context ends when it
	// passes, and the batch counts as failed. Above ClaimDuration it makes the
	// consumer renew the claim every half ClaimDuration while the handler's
	// context lives, so a batch may take minutes while a crashed process's
	// cursor is taken over in seconds. Without renewal a batch that outlived
	// its claim would be taken over and run once more in the other process,
	// so renewal is what lets ClaimDuration be short. A handler that ignores
	// its context past HandlerTimeout is renewed no further and loses the
	// cursor about a ClaimDuration later, like any other overrun.
	HandlerTimeout time.Duration
}

func (o ConsumeOptions) withDefaults() ConsumeOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = 50
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	o.ClaimDuration, o.HandlerTimeout = defaultDurations(o.ClaimDuration, o.HandlerTimeout)
	return o
}

// consumer hands stream messages to a handler, a batch at a time.
type consumer struct {
	runtime *Runtime
	name    string
	cursor  Cursor
	handle  func(ctx context.Context, msgs []Message) error
	opts    ConsumeOptions
}

// Consume registers a stream consumer on the runtime: once it is started, the
// messages matching patterns are handed to handle in batches of up to
// BatchSize, in position order, and the cursor moves past a batch when handle
// returns nil. The patterns are the ones the stream reads take — a topic, a
// prefix followed by ".#", or "#" — and a pattern that does not compile panics
// here rather than failing on every round once the runtime is started. The
// cursor row is consumer:<name>.
//
// One process runs a cursor at a time. Every process that registered the
// consumer claims the cursor before it reads; the others find it claimed and
// wait for the next wake-up, and one of them takes it over when the claim
// lapses — the process holding it crashed, or its handler ran past
// HandlerTimeout. So one process drains a burst alone and in order, which is
// what lets a handler reduce a batch of five hundred messages to the distinct
// records they concern and index each once. More processes give failover, not
// throughput: work that should spread across processes, one message at a time,
// is a Trigger and a job type.
//
// A handler error leaves the cursor where it was and the same batch comes back
// after PollInterval, for as long as it fails. A consumer has no attempts and
// no failed state: a message row is written once and a cursor is one row, so
// there is nowhere to mark one message of a stream as given up on, and skipping
// it would leave a projection missing a record with nobody told. The handler
// decides what it can pass over — it logs the message and returns nil — and
// per-message retries are a job type behind a trigger. Delivery is at least
// once: a batch whose process crashed after the handler and before the ack
// runs again.
func (r *Runtime) Consume(name string, patterns []string, handle func(ctx context.Context, msgs []Message) error, opts ConsumeOptions) {
	if handle == nil {
		panic("catbird: consumer " + name + " registered with no handler")
	}
	if _, _, err := compilePatterns(patterns, 1); err != nil {
		panic(err)
	}
	c := &consumer{
		runtime: r,
		name:    name,
		cursor:  Cursor{Name: "consumer:" + name, Patterns: patterns},
		handle:  handle,
		opts:    opts.withDefaults(),
	}
	r.declare("cb_stream", c.start)
}

// start runs the consumer until ctx is canceled: a batch whenever the assigner
// announces new positions, and every PollInterval in case a notification was
// lost, another process held the cursor, or the last batch failed.
func (c *consumer) start(ctx context.Context) {
	wake, unsubscribe := c.runtime.subscribe("cb_stream")
	defer unsubscribe()

	for ctx.Err() == nil {
		n, err := c.handleNextBatch(ctx)
		if err != nil && ctx.Err() == nil {
			c.runtime.opts.Logger.Error("catbird: consumer failed", "consumer", c.name, "err", err)
		}
		if err == nil && n == c.opts.BatchSize {
			continue // the stream may hold more
		}
		select {
		case <-ctx.Done():
		case <-wake:
		case <-time.After(c.opts.PollInterval):
		}
	}
}

// handleNextBatch claims the cursor, reads the batch after it, runs the handler
// on the batch, and acks: the last position read when the handler returned
// nil, the position it claimed at otherwise, so an empty or failed batch
// releases the cursor without moving it and the three outcomes are one path.
// Returns how many messages the handler took, and 0 with no error when another
// process holds the cursor or took it over during the handler; a batch whose
// claim was lost is not acked, because the ack would match no row and the
// process that holds the cursor now runs the batch itself.
//
// The ack runs on a context the shutdown cannot cancel, like the worker's
// completion: a handler stopped by shutdown returns with ctx already canceled,
// and a claim left standing would keep every process off the cursor for the
// rest of ClaimDuration.
func (c *consumer) handleNextBatch(ctx context.Context) (int, error) {
	position, claimableAt, claimed, err := c.claim(ctx)
	if err != nil || !claimed {
		return 0, err
	}
	msgs, err := ReadAfter(ctx, c.runtime.pool, c.cursor.Patterns, position, c.opts.BatchSize)
	if err == nil && len(msgs) > 0 {
		var lost bool
		claimableAt, lost, err = c.runHandler(ctx, msgs, claimableAt)
		if lost {
			c.runtime.opts.Logger.Warn("catbird: cursor claim lost during the handler, work discarded",
				"consumer", c.name, "messages", len(msgs), "err", err)
			return 0, nil
		}
		if err == nil {
			position = msgs[len(msgs)-1].Position
		}
	}

	after, cancel := context.WithTimeout(context.WithoutCancel(ctx), afterHandlerTimeout)
	defer cancel()
	switch ackErr := c.ack(after, position, claimableAt); {
	case errors.Is(ackErr, ErrClaimLost):
		c.runtime.opts.Logger.Warn("catbird: cursor claim lost before the ack, the batch runs again in another process",
			"consumer", c.name, "messages", len(msgs))
		return 0, err
	case ackErr != nil:
		return 0, errors.Join(err, ackErr)
	case err != nil:
		return 0, err
	}
	return len(msgs), nil
}

// runHandler runs the handler on a context bounded by HandlerTimeout and, on a
// consumer whose HandlerTimeout exceeds ClaimDuration, renews the claim while
// that context lives. It returns the deadline the claim holds once the handler
// is done — the last renewal's, or the one the claim was taken with — and
// whether the claim was lost on the way.
//
// The handler's context is never a shadow of ctx: a timeout on the loop's own
// context would stop the loop, and the cause layer underneath is for the
// renewal, which cancels through it with ErrClaimLost when the claim is not
// this process's any more, so the batch is discarded rather than acked on a
// cursor another process holds.
func (c *consumer) runHandler(ctx context.Context, msgs []Message, claimableAt time.Time) (time.Time, bool, error) {
	lost, cancelLost := context.WithCancelCause(ctx)
	defer cancelLost(nil)
	handlerCtx, cancelHandler := context.WithTimeout(lost, c.opts.HandlerTimeout)
	defer cancelHandler()

	renewed := make(chan time.Time, 1)
	if c.opts.HandlerTimeout > c.opts.ClaimDuration {
		go func() { renewed <- c.renewClaim(ctx, handlerCtx, claimableAt, cancelLost) }()
	} else {
		renewed <- claimableAt
	}
	err := c.handle(handlerCtx, msgs)
	cancelHandler() // ends the renewal, which then reports the deadline it last set
	claimableAt = <-renewed
	return claimableAt, errors.Is(context.Cause(handlerCtx), ErrClaimLost), err
}

// renewClaim moves the claim's deadline a full ClaimDuration out every half
// ClaimDuration for as long as handlerCtx lives, so one missed renewal — a
// network error, a slow statement — loses nothing, and returns the deadline it
// last set. Renewal follows the handler's context rather than the handler:
// past HandlerTimeout the context is spent and the cursor is renewed no
// further, so a handler that hangs there loses the cursor to another process
// about a ClaimDuration later. ClaimDuration is then how long the cursor stays
// stuck when the process holding it crashes, and HandlerTimeout alone bounds a
// batch.
//
// The renewal matches on the deadline like the ack. One that matches no row
// means the claim is not this process's any more — it lapsed and another
// process took the cursor — so the handler is cancelled with ErrClaimLost to
// stop work nothing will ack.
func (c *consumer) renewClaim(ctx, handlerCtx context.Context, claimableAt time.Time, cancel context.CancelCauseFunc) time.Time {
	tick := time.NewTicker(c.opts.ClaimDuration / 2)
	defer tick.Stop()
	for {
		select {
		case <-handlerCtx.Done():
			return claimableAt
		case <-tick.C:
		}
		var renewed time.Time
		err := c.runtime.pool.QueryRow(ctx, `
			UPDATE cb_cursors
			SET claimable_at = now() + $2::interval
			WHERE name = $1 AND claimable_at = $3
			RETURNING claimable_at
		`, c.cursor.Name, c.opts.ClaimDuration, claimableAt).Scan(&renewed)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			cancel(ErrClaimLost)
			return claimableAt
		case err != nil:
			// Nothing is cancelled on an error: the claim may still be this
			// process's, and the next tick renews it a full ClaimDuration deep.
			if ctx.Err() == nil {
				c.runtime.opts.Logger.Error("catbird: renewing the cursor claim failed", "consumer", c.name, "err", err)
			}
		default:
			claimableAt = renewed
		}
	}
}

// claim takes the cursor for ClaimDuration and returns the position to read
// after and the deadline the claim set, or false when another process holds
// the cursor. One statement covers the three cases: it creates the row on a
// cursor's first claim, takes an existing row whose claimable_at has passed,
// and writes nothing when it has not — ON CONFLICT DO UPDATE with a WHERE
// updates no row when the condition fails, and RETURNING returns only rows
// written. A bare UPDATE would take nothing on a cursor that has never been
// acked, and every process would read that as another one holding it.
func (c *consumer) claim(ctx context.Context) (position int64, claimableAt time.Time, claimed bool, err error) {
	err = c.runtime.pool.QueryRow(ctx, `
		INSERT INTO cb_cursors (name, last_position, claimable_at)
		VALUES ($1, 0, now() + $2::interval)
		ON CONFLICT (name) DO UPDATE SET claimable_at = now() + $2::interval
		WHERE cb_cursors.claimable_at <= now()
		RETURNING last_position, claimable_at
	`, c.cursor.Name, c.opts.ClaimDuration).Scan(&position, &claimableAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, false, nil
	}
	if err != nil {
		return 0, time.Time{}, false, err
	}
	return position, claimableAt, true, nil
}

// ack moves the cursor to position and releases the claim. It matches on the
// deadline the claim or the last renewal set, as every write on a job matches
// on attempts: when this process's claim lapsed and another process took the
// cursor over, the ack writes nothing and returns ErrClaimLost. Without the
// match, a late ack would release the other process's claim while its batch is
// still running, and a third process would start the next batch beside it.
// GREATEST keeps the cursor from moving backwards, as Cursor.Ack does.
func (c *consumer) ack(ctx context.Context, position int64, claimableAt time.Time) error {
	tag, err := c.runtime.pool.Exec(ctx, `
		UPDATE cb_cursors
		SET last_position = GREATEST(last_position, $2), claimable_at = '-infinity'
		WHERE name = $1 AND claimable_at = $3
	`, c.cursor.Name, position, claimableAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrClaimLost
	}
	return nil
}
