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
// assigner, and every worker, trigger and consumer declared on it. Declare them
// with NewWorker, NewTrigger and NewConsumer (or the methods of the same names),
// then call Start. Client is not created from the runtime: it is a plain helper
// that works on any connection or transaction.
type Runtime struct {
	pool *pgxpool.Pool
	opts Options

	mu       sync.Mutex
	started  bool
	channels []string                              // what the connection LISTENs on; fixed at Start
	loops    []func(ctx context.Context)           // what Start runs, one goroutine each
	wakes    map[string]map[chan struct{}]struct{} // per channel, the loops waiting for a notification on it
	demand   int                                   // pool connections the declared loops hold for as long as a job runs
}

func New(pool *pgxpool.Pool, opts Options) *Runtime {
	return &Runtime{
		pool:     pool,
		opts:     opts.withDefaults(),
		channels: []string{"cb_stream"},
		wakes:    map[string]map[chan struct{}]struct{}{},
	}
}

// reserve records how many pool connections a declared loop holds for as long
// as the work in them runs, so Start can say whether the pool is big enough for
// what was declared. Statements that come and go -- the claim, the assigner,
// the retry a failed job writes -- are not counted; they share what is left.
func (r *Runtime) reserve(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.demand += n
}

// Start runs everything declared on the runtime until ctx is canceled, then
// waits for all of it to stop: the LISTEN connection, the position assigner,
// and one goroutine per worker and trigger.
func (r *Runtime) Start(ctx context.Context) {
	r.mu.Lock()
	r.started = true
	channels, loops, demand := r.channels, r.loops, r.demand
	r.mu.Unlock()

	// What was declared can hold more connections at once than the pool has.
	// Nothing deadlocks -- every holder finishes and gives its connection back
	// -- but a job blocked in Begin has already been claimed, so its lease is
	// running while it waits for a connection it may not get before another
	// worker takes the job back. NewWorker keeps one worker under the limit;
	// this is the only place that sees all of them together. One connection is
	// left out of the limit for the statements that come and go.
	if poolMax := int(r.pool.Config().MaxConns); demand > poolMax-1 {
		r.opts.Logger.Warn("catbird: declared loops hold more connections than the pool has",
			"connections_held", demand, "pool_max_conns", poolMax,
			"hint", "raise pool_max_conns, or lower BatchSize on the workers")
	}

	var wg sync.WaitGroup
	wg.Go(func() { assignPositions(ctx, r.pool, r.opts) })
	wg.Go(func() { r.listen(ctx, channels) })
	for _, loop := range loops {
		wg.Go(func() { loop(ctx) })
	}
	wg.Wait()
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
//
// The connection is its own, opened from the pool's configuration rather than
// taken from the pool. It is held for the life of the process, so from the pool
// it would be a connection the workers never get back -- and worse, when the
// workers hold every connection there is, this could not open one at all and
// the whole process would fall back to polling with a reconnect error every
// ReconnectAfter.
func (r *Runtime) listen(ctx context.Context, channels []string) {
	for ctx.Err() == nil {
		err := func() error {
			// The pool's own connect hooks still apply: this connection is
			// outside the pool, not outside the caller's configuration.
			cfg := r.pool.Config()
			if cfg.BeforeConnect != nil {
				if err := cfg.BeforeConnect(ctx, cfg.ConnConfig); err != nil {
					return err
				}
			}
			conn, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
			if err != nil {
				return err
			}
			if cfg.AfterConnect != nil {
				if err := cfg.AfterConnect(ctx, conn); err != nil {
					conn.Close(ctx)
					return err
				}
			}
			// Closing needs a context of its own: at shutdown ctx is already
			// canceled, and a close on a canceled context abandons the socket
			// instead of ending the session.
			defer func() {
				closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
				defer cancel()
				conn.Close(closeCtx)
			}()
			for _, channel := range channels {
				if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
					return err
				}
			}
			r.wake("")
			for {
				n, err := conn.WaitForNotification(ctx)
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
