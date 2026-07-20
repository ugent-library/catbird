// Package notify carries PostgreSQL notifications to in-process
// subscribers. One Notifier serves a whole process: every consume loop,
// ticker and wire subscriber shares its single LISTEN connection, so a
// notifying commit signals one backend per process instead of one per
// loop.
//
// A notification means "look now" — it carries no work of its own and is
// gone when the connection is down, so every subscriber keeps its poll
// interval as the layer that guarantees delivery. A consumer without a
// notifier looks on its poll interval alone; that is the right
// configuration for transaction-pooled connections, where LISTEN cannot
// work, and for workloads that do not care about delivery latency.
package notify

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
const pingTimeout = 30 * time.Second

// Opts tunes a Notifier. Zero fields mean the defaults.
type Opts struct {
	Logger *slog.Logger // slog.Default()
}

// Notifier holds one LISTEN connection and hands every notification to
// the subscribers of its channel. Construct one per process with New,
// run it with Start, and pass it to each consume loop and ticker.
type Notifier struct {
	pool        *pgxpool.Pool
	logger      *slog.Logger
	mu          sync.Mutex
	subscribers map[string][]*subscriber
	listened    map[string]bool    // channels the live connection listens on
	cancelWait  context.CancelFunc // ends the current wait so LISTEN changes apply now
}

type subscriber struct {
	channel     string
	onNotify    func(payload string)
	onReconnect func() // may be nil
}

func New(pool *pgxpool.Pool, opts ...Opts) *Notifier {
	var o Opts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Notifier{
		pool:        pool,
		logger:      o.Logger,
		subscribers: make(map[string][]*subscriber),
	}
}

// Subscribe registers callbacks for a channel's notifications, before or
// while the notifier runs. They run on the notifier's connection
// goroutine and must not block.
//
// onNotify is called once per notification, with the payload the SQL
// sent — always a real payload, never a synthetic one. Each subscriber
// parses its own channel's payload format.
//
// onReconnect, when not nil, is called once after every (re)connect.
// Notifications sent while the connection was down are gone, so a
// subscriber that needs to catch up looks for itself here. Pass nil when
// there is nothing to recover — an at-most-once channel whose missed
// events are simply missed. Reconnect is its own callback, not an
// onNotify call with an empty payload, so onNotify never has to tell a
// real notification from a reconnect.
//
// The returned function ends the subscription.
func (n *Notifier) Subscribe(channel string, onNotify func(payload string), onReconnect func()) func() {
	s := &subscriber{channel: channel, onNotify: onNotify, onReconnect: onReconnect}
	n.mu.Lock()
	n.subscribers[channel] = append(n.subscribers[channel], s)
	cancelWait := n.cancelWait
	n.mu.Unlock()
	if cancelWait != nil {
		cancelWait()
	}
	return func() {
		n.mu.Lock()
		subs := n.subscribers[channel]
		for i, x := range subs {
			if x == s {
				n.subscribers[channel] = append(subs[:i:i], subs[i+1:]...)
				break
			}
		}
		if len(n.subscribers[channel]) == 0 {
			delete(n.subscribers, channel)
		}
		cancelWait := n.cancelWait
		n.mu.Unlock()
		if cancelWait != nil {
			cancelWait()
		}
	}
}

// Start runs the notifier until ctx ends, reconnecting on connection
// loss.
func (n *Notifier) Start(ctx context.Context) error {
	for {
		err := n.listenOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n.logger.Warn("catbird: notifier reconnecting", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (n *Notifier) listenOnce(ctx context.Context) error {
	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// take the connection out of the pool for good: a connection with
	// LISTEN state must not be handed to other callers
	pgConn := conn.Hijack()
	defer pgConn.Close(context.WithoutCancel(ctx))

	n.mu.Lock()
	n.listened = make(map[string]bool)
	n.mu.Unlock()
	if err := n.syncChannels(ctx, pgConn); err != nil {
		return err
	}

	// notifications sent while the connection was down are gone: tell
	// every subscriber that has something to recover to look for itself
	n.mu.Lock()
	var all []*subscriber
	for _, subs := range n.subscribers {
		all = append(all, subs...)
	}
	n.mu.Unlock()
	for _, s := range all {
		if s.onReconnect != nil {
			s.onReconnect()
		}
	}

	for {
		// register the wait's cancel before reading the subscriptions:
		// a Subscribe that lands after the read pre-cancels the wait, so
		// its LISTEN is applied on the retry instead of waiting out the
		// ping window
		waitCtx, cancel := context.WithTimeout(ctx, pingTimeout)
		n.mu.Lock()
		n.cancelWait = cancel
		n.mu.Unlock()
		if err := n.syncChannels(ctx, pgConn); err != nil {
			cancel()
			return err
		}
		notification, err := pgConn.WaitForNotification(waitCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// a subscription change ended the wait; apply it now
			if errors.Is(err, context.Canceled) {
				continue
			}
			// A timeout just means no notification arrived in this
			// window — the connection may still be healthy. Ping to tell
			// a quiet connection from one that has silently died: a
			// healthy one keeps waiting, a dead one reconnects instead
			// of blocking forever.
			if errors.Is(err, context.DeadlineExceeded) {
				pingCtx, pingCancel := context.WithTimeout(ctx, pingTimeout)
				pingErr := pgConn.Ping(pingCtx)
				pingCancel()
				if pingErr == nil {
					continue
				}
				return pingErr
			}
			return err
		}

		n.mu.Lock()
		subs := append([]*subscriber(nil), n.subscribers[notification.Channel]...)
		n.mu.Unlock()
		for _, s := range subs {
			s.onNotify(notification.Payload)
		}
	}
}

// syncChannels brings the connection's LISTEN state in step with the
// subscriptions.
func (n *Notifier) syncChannels(ctx context.Context, conn *pgx.Conn) error {
	n.mu.Lock()
	var listen, unlisten []string
	for ch := range n.subscribers {
		if !n.listened[ch] {
			listen = append(listen, ch)
		}
	}
	for ch := range n.listened {
		if _, ok := n.subscribers[ch]; !ok {
			unlisten = append(unlisten, ch)
		}
	}
	n.mu.Unlock()

	for _, ch := range listen {
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return err
		}
		n.mu.Lock()
		n.listened[ch] = true
		n.mu.Unlock()
	}
	for _, ch := range unlisten {
		if _, err := conn.Exec(ctx, "UNLISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return err
		}
		n.mu.Lock()
		delete(n.listened, ch)
		n.mu.Unlock()
	}
	return nil
}
