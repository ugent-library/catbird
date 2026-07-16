package jobs

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// idle wait per notification read; also the ping deadline that tells a
// quiet connection from a dead one
const notifyPingTimeout = 30 * time.Second

// notifier holds one LISTEN connection on the worker's queue channels and
// turns notifications into wake signals for the claim loop. A payload is a
// step's claimable_at: a time already reached wakes the loop at once, a
// future one arms a single timer for the earliest pending wake — that is
// how a backoff-paced retry is claimed on time without polling for it.
type notifier struct {
	pool     *pgxpool.Pool
	logger   *slog.Logger
	channels []string
	wake     chan struct{}

	mu    sync.Mutex
	timer *time.Timer
	next  time.Time
}

// run listens until ctx ends, reconnecting on connection loss.
func (n *notifier) run(ctx context.Context) {
	defer n.stopTimer()
	for {
		err := n.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		n.logger.Warn("catbird: worker notifier reconnecting", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (n *notifier) listenOnce(ctx context.Context) error {
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// take the connection out of the pool for good: a connection with
	// LISTEN state must not be handed to other callers
	pgConn := conn.Hijack()
	defer pgConn.Close(context.WithoutCancel(ctx))

	for _, ch := range n.channels {
		if _, err := pgConn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return err
		}
	}

	// notifications sent while the listener was down are gone: claim once
	// right away instead of assuming nothing happened
	n.signal()

	for {
		waitCtx, cancel := context.WithTimeout(ctx, notifyPingTimeout)
		notification, err := pgConn.WaitForNotification(waitCtx)
		cancel()
		if err != nil {
			// A clean timeout just means no notification arrived in this
			// window — the connection may still be healthy. Ping to tell a
			// quiet connection from one that has silently died: a healthy
			// one keeps waiting, a dead one reconnects instead of blocking
			// forever.
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				pingCtx, pingCancel := context.WithTimeout(ctx, notifyPingTimeout)
				pingErr := pgConn.Ping(pingCtx)
				pingCancel()
				if pingErr == nil {
					continue
				}
				return pingErr
			}
			return err
		}
		n.signalAt(parseClaimableAt(notification.Payload))
	}
}

// signalAt wakes the claim loop when the notified claimable_at arrives: at
// once when it has passed, otherwise through one timer kept at the
// earliest pending time.
func (n *notifier) signalAt(at time.Time) {
	if at.IsZero() || !at.After(time.Now()) {
		n.signal()
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.timer != nil && !at.Before(n.next) {
		return // an earlier wake is already set
	}
	if n.timer != nil {
		n.timer.Stop()
	}
	n.next = at
	n.timer = time.AfterFunc(time.Until(at), func() {
		n.signal()
		n.mu.Lock()
		n.timer = nil
		n.next = time.Time{}
		n.mu.Unlock()
	})
}

// signal wakes the claim loop without blocking: a wake already pending is
// enough.
func (n *notifier) signal() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *notifier) stopTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.timer != nil {
		n.timer.Stop()
		n.timer = nil
	}
}

// parseClaimableAt parses a notify payload as an RFC 3339 timestamp. An
// empty or unparseable payload wakes at once: a spurious claim is cheap,
// a missed one is not.
func parseClaimableAt(payload string) time.Time {
	if payload == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, payload)
	if err != nil {
		return time.Time{}
	}
	return t
}
