package catbird

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// workerNotifyTarget wraps a signal channel with a single pending timer.
// At most one timer is active per target — when a NOTIFY arrives with
// a future visible_at, the timer is set (or reset if the new time is
// earlier). Immediate signals bypass the timer entirely.
type workerNotifyTarget struct {
	ch    chan struct{}
	mu    sync.Mutex
	timer *time.Timer
	next  time.Time // earliest pending visible_at
}

func newWorkerNotifyTarget(ch chan struct{}) *workerNotifyTarget {
	return &workerNotifyTarget{ch: ch}
}

// signalAt signals the target at the appropriate time based on visibleAt.
// If visibleAt is in the past or zero, signals immediately.
// If visibleAt is in the future, schedules (or reschedules) a single timer
// to the earliest pending time.
func (t *workerNotifyTarget) signalAt(visibleAt time.Time) {
	if visibleAt.IsZero() || !visibleAt.After(time.Now()) {
		signal(t.ch)
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// If there's already a timer for an earlier or equal time, keep it.
	if t.timer != nil && !visibleAt.Before(t.next) {
		return
	}

	// Cancel existing timer if any.
	if t.timer != nil {
		t.timer.Stop()
	}

	t.next = visibleAt
	t.timer = time.AfterFunc(time.Until(visibleAt), func() {
		signal(t.ch)
		t.mu.Lock()
		t.timer = nil
		t.next = time.Time{}
		t.mu.Unlock()
	})
}

// workerNotifier manages a dedicated LISTEN connection and fans out
// PostgreSQL notifications to Go channels. It is internal to the worker.
//
// NOTIFY payloads carry a timestamp (visible_at). The workerNotifier parses it
// and either signals targets immediately or schedules a single timer per
// target for the earliest future visible_at.
type workerNotifier struct {
	pool   *pgxpool.Pool
	logger *slog.Logger

	mu   sync.Mutex
	subs map[string][]*workerNotifyTarget // PG channel → list of targets
}

func newWorkerNotifier(pool *pgxpool.Pool, logger *slog.Logger) *workerNotifier {
	return &workerNotifier{
		pool:   pool,
		logger: logger,
		subs:   make(map[string][]*workerNotifyTarget),
	}
}

// subscribe registers a target channel to receive signals when a
// PostgreSQL NOTIFY arrives on the given channel name. The caller
// owns the signal channel; the workerNotifier manages timing internally.
func (n *workerNotifier) subscribe(channel string, target chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.subs[channel] = append(n.subs[channel], newWorkerNotifyTarget(target))
}

// listen acquires a dedicated connection and issues LISTEN for all
// subscribed channels. Returns a function that runs the notification
// read loop with automatic reconnection. Call listen() before starting
// claim loops so LISTEN is established before any NOTIFY fires.
func (n *workerNotifier) listen(ctx context.Context) (run func(), err error) {
	n.mu.Lock()
	channels := make([]string, 0, len(n.subs))
	for ch := range n.subs {
		channels = append(channels, ch)
	}
	n.mu.Unlock()

	if len(channels) == 0 {
		return func() {}, nil
	}

	n.logger.InfoContext(ctx, "worker notifier: listening", "channels", len(channels))

	return func() {
		for {
			if err := n.listenOnce(ctx, channels); err != nil {
				if ctx.Err() != nil {
					return
				}
				n.logger.WarnContext(ctx, "worker notifier: connection lost, reconnecting", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		}
	}, nil
}

func (n *workerNotifier) listenOnce(ctx context.Context, channels []string) error {
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	pgConn := conn.Conn()

	for _, ch := range channels {
		if _, listenErr := pgConn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); listenErr != nil {
			return listenErr
		}
	}

	for {
		notification, waitErr := pgConn.WaitForNotification(ctx)
		if waitErr != nil {
			return waitErr
		}

		n.fanOut(notification.Channel, notification.Payload)
	}
}

// fanOut parses the payload as a timestamp and signals all subscribers.
// Empty payload or unparseable timestamp triggers an immediate signal.
func (n *workerNotifier) fanOut(channel, payload string) {
	visibleAt := parseVisibleAt(payload)

	n.mu.Lock()
	subs := n.subs[channel]
	n.mu.Unlock()

	for _, target := range subs {
		target.signalAt(visibleAt)
	}
}

// parseVisibleAt parses a NOTIFY payload as an RFC 3339 timestamp.
// Returns zero time (immediate signal) if the payload is empty or unparseable.
func parseVisibleAt(payload string) time.Time {
	if payload == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, payload)
	if err != nil {
		return time.Time{}
	}
	return t
}

// signal sends a non-blocking notification to ch.
// Drains any existing signal first to avoid blocking.
func signal(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// channelName builds a schema-qualified NOTIFY channel name.
// Matches the SQL pattern: current_schema || '.cb_...'
func channelName(schema string, parts ...string) string {
	return strings.ToLower(schema + "." + strings.Join(parts, ""))
}
