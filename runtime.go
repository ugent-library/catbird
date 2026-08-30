package catbird

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Options are the optional parts of New. Zero values take the defaults.
type Options struct {
	AssignEvery    time.Duration // how often the position assigner runs; default 250 milliseconds
	ReconnectAfter time.Duration // wait before reconnecting a dropped LISTEN connection; default 5 seconds
	Logger         *slog.Logger  // default slog.Default()
}

func (o Options) withDefaults() Options {
	if o.AssignEvery <= 0 {
		o.AssignEvery = 250 * time.Millisecond
	}
	if o.ReconnectAfter <= 0 {
		o.ReconnectAfter = 5 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Runtime is a process's catbird: the pool, one LISTEN connection, the position
// assigner, and every worker and trigger declared on it. Declare them with
// NewWorker and NewTrigger (or the methods of the same names), then call Start.
// The statements a caller runs — Enqueue, Publish, Complete, the stream reads
// and the rest — are package functions and need no runtime: they work on any
// connection or transaction.
type Runtime struct {
	pool *pgxpool.Pool
	opts Options

	mu       sync.Mutex
	started  bool
	channels []string                              // what the connection LISTENs on; fixed at Start
	loops    []func(ctx context.Context)           // what Start runs, one goroutine each
	wakes    map[string]map[chan struct{}]struct{} // per channel, the loops waiting for a notification on it
}

func New(pool *pgxpool.Pool, opts Options) *Runtime {
	return &Runtime{
		pool:     pool,
		opts:     opts.withDefaults(),
		channels: []string{"cb_stream"},
		wakes:    map[string]map[chan struct{}]struct{}{},
	}
}

// Start runs everything declared on the runtime until ctx is canceled, then
// waits for all of it to stop: the LISTEN connection, the position assigner,
// and one goroutine per worker and trigger.
func (r *Runtime) Start(ctx context.Context) {
	r.mu.Lock()
	r.started = true
	channels, loops := r.channels, r.loops
	r.mu.Unlock()

	var wg sync.WaitGroup
	wg.Go(func() { assignPositions(ctx, r.pool, r.opts) })
	wg.Go(func() { r.listen(ctx, channels) })
	for _, loop := range loops {
		wg.Go(func() { loop(ctx) })
	}
	wg.Wait()
}

// assignPositions gives every published message that has none the next
// position, every AssignEvery. A message is visible to the assigner only once
// its transaction committed, so positions follow commit order: a message from a
// transaction that is still open is picked up on a later tick. Readers order by
// position and a batch of positions becomes readable when this statement
// commits, so a reader does not pass a message that has no position yet.
//
// The advisory lock is taken for the statement only. When another assigner
// holds it, the one-time filter on the UPDATE skips the scan. The UPDATE also
// requires position IS NULL, checked again on the committed row when it had to
// wait for a lock, so two assigners that run at the same time cannot move a
// position that is already set.
//
// When it assigned anything it sends NOTIFY on channel cb_stream with the
// highest new position, so a LISTENing reader can fetch instead of polling.
func assignPositions(ctx context.Context, pool *pgxpool.Pool, opts Options) {
	ticker := time.NewTicker(opts.AssignEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		_, err := pool.Exec(ctx, `
			WITH lock AS (
				SELECT pg_try_advisory_xact_lock(hashtext('catbird'), 1) AS held
			),
			unassigned AS (
				SELECT id FROM cb_messages
				WHERE stream AND position IS NULL
				ORDER BY id
				LIMIT 5000
			),
			assigned AS (
				UPDATE cb_messages m
				SET position = nextval('cb_position_seq')
				FROM unassigned u
				WHERE m.id = u.id AND m.position IS NULL AND (SELECT held FROM lock)
				RETURNING position
			)
			SELECT pg_notify('cb_stream', max(position)::text) FROM assigned HAVING count(*) > 0
		`)
		if err != nil && ctx.Err() == nil {
			opts.Logger.Error("catbird: assigning stream positions failed", "err", err)
		}
	}
}

// declare registers a loop for Start to run, and the channel its wake-ups come
// from. Declaring after Start is a programming error: the connection's channel
// set is fixed when it connects.
func (r *Runtime) declare(channel string, loop func(ctx context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		panic("catbird: declared after Start")
	}
	if !slices.Contains(r.channels, channel) {
		r.channels = append(r.channels, channel)
	}
	r.loops = append(r.loops, loop)
}

// subscribe returns a channel that receives a value whenever a notification
// arrives on channel, and a function that ends the subscription. A value
// already pending is not doubled: one wake-up covers everything that arrived
// before the receiver looked.
func (r *Runtime) subscribe(channel string) (wake <-chan struct{}, unsubscribe func()) {
	ch := make(chan struct{}, 1)
	r.mu.Lock()
	if r.wakes[channel] == nil {
		r.wakes[channel] = map[chan struct{}]struct{}{}
	}
	r.wakes[channel][ch] = struct{}{}
	r.mu.Unlock()
	return ch, func() {
		r.mu.Lock()
		delete(r.wakes[channel], ch)
		r.mu.Unlock()
	}
}

// wake nudges every loop subscribed to channel; "" nudges all of them.
func (r *Runtime) wake(channel string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, subscribers := range r.wakes {
		if channel != "" && name != channel {
			continue
		}
		for ch := range subscribers {
			select {
			case ch <- struct{}{}:
			default: // a wake-up is already pending
			}
		}
	}
}

// listen holds the one LISTEN connection and turns each notification into a
// wake-up for the loops subscribed to its channel. When the connection drops it
// logs, waits ReconnectAfter, and connects again; in between the loops run on
// their poll intervals. Notifications sent while there was no connection are
// gone, so every loop is woken once after each connect.
func (r *Runtime) listen(ctx context.Context, channels []string) {
	for ctx.Err() == nil {
		err := func() error {
			conn, err := r.pool.Acquire(ctx)
			if err != nil {
				return err
			}
			defer conn.Release()
			for _, channel := range channels {
				if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
					return err
				}
			}
			r.wake("")
			for {
				n, err := conn.Conn().WaitForNotification(ctx)
				if err != nil {
					return err
				}
				r.wake(n.Channel)
			}
		}()
		if ctx.Err() == nil {
			r.opts.Logger.Error("catbird: listen failed, reconnecting", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(r.opts.ReconnectAfter):
			}
		}
	}
}
