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
	// How long a job's result, and the messages no job refers to any more, are
	// kept after the job ended. Set, Start runs GC with it: once at start and
	// then every hour, in every process, with one process at a time doing the
	// deleting. Zero, the default, runs no GC; the application calls GC on its
	// own schedule. Not stored: two processes that disagree give the database
	// the shorter one.
	Retention time.Duration
	Logger    *slog.Logger // default slog.Default()
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
// assigner, the GC loop when Retention is set, and every job type and trigger
// registered on it. Register them with
// Handle and Trigger, then call Start. The statements a caller runs — Enqueue,
// Publish, Complete, the stream reads and the rest — are package functions and
// need no runtime: they work on any connection or transaction.
type Runtime struct {
	pool *pgxpool.Pool
	opts Options

	mu       sync.Mutex
	started  bool
	channels []string                              // what the connection LISTENs on; fixed at Start
	loops    []func(ctx context.Context)           // what Start runs, one goroutine each
	wakes    map[string]map[chan struct{}]struct{} // per channel, the loops waiting for a notification on it
	workers  map[string]*worker                    // one per queue a registered job type runs on
}

func New(pool *pgxpool.Pool, opts Options) *Runtime {
	return &Runtime{
		pool:     pool,
		opts:     opts.withDefaults(),
		channels: []string{"cb_stream"},
		wakes:    map[string]map[chan struct{}]struct{}{},
		workers:  map[string]*worker{},
	}
}

// Handle registers a job type on the runtime together with the function that
// runs it: once the runtime is started, this process claims that type's jobs and
// runs handle for each. Types sharing a queue share one claim loop, one
// goroutine and one notification channel, so a process handling thirty kinds of
// work does not run thirty of each.
//
// A process claims only the types registered on it. A job of a type it does not
// know is left for a process that does, which is what makes a deploy that adds a
// type safe in either order.
func (r *Runtime) Handle(t *JobType, handle Handler) {
	if handle == nil {
		panic("catbird: job type " + t.name + " registered with no handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		panic("catbird: registered after Start")
	}
	w := r.workers[t.queue.name]
	if w == nil {
		logger := t.queue.opts.Logger
		if logger == nil {
			logger = r.opts.Logger
		}
		w = &worker{runtime: r, queue: t.queue, handlers: map[string]registration{}, logger: logger}
		r.workers[t.queue.name] = w
		r.declareLocked("cb_queue_"+t.queue.name, w.start)
	}
	if _, taken := w.handlers[t.name]; taken {
		panic("catbird: job type " + t.name + " is already registered on queue " + t.queue.name)
	}
	w.handlers[t.name] = registration{jobType: t, handler: handle}
	w.names = append(w.names, t.name)

	// A type declared with Schedule is ticked by the processes that handle it,
	// so a scheduled job is only ever enqueued where something can run it and
	// an enqueue-only process never ticks. Every handling process ticks; the
	// tick statement's guards keep the result single, so there is no leader.
	if t.schedule != nil {
		p := &periodic{runtime: r, jobType: t, logger: w.logger}
		r.declareLocked("", p.start)
	}
}

// HandleFunc registers a plain function the way Handle registers a Handler,
// like http.ServeMux's HandleFunc.
func (r *Runtime) HandleFunc(t *JobType, handle func(ctx context.Context, job *Job) error) {
	if handle == nil {
		panic("catbird: job type " + t.name + " registered with no handler")
	}
	r.Handle(t, HandlerFunc(handle))
}

// Start runs everything declared on the runtime until ctx is canceled, then
// waits for all of it to stop: the LISTEN connection, the position assigner,
// the GC loop when Retention is set, and one goroutine per worker and trigger.
func (r *Runtime) Start(ctx context.Context) {
	r.mu.Lock()
	r.started = true
	channels, loops := r.channels, r.loops
	r.mu.Unlock()

	var wg sync.WaitGroup
	wg.Go(func() { assignPositions(ctx, r.pool, r.opts) })
	wg.Go(func() { r.listen(ctx, channels) })
	if r.opts.Retention > 0 {
		wg.Go(func() { collectGarbage(ctx, r.pool, r.opts) })
	}
	for _, loop := range loops {
		wg.Go(func() { loop(ctx) })
	}
	wg.Wait()
}

// assignPositions gives every published message that has none the next
// position, every AssignEvery. A message is visible to the assigner only once
// its transaction committed, so positions follow commit order: a message from a
// transaction that is still open is picked up on a later tick. Readers order by
// position and a batch of positions becomes readable when the statement
// commits, so a reader does not pass a message that has no position yet.
//
// A tick drains: the statement takes assignBatchSize messages at a time and
// runs again while its batch came back full, up to assignRoundsPerTick. One
// statement per tick would cap the whole database at 5000 positions per tick,
// 20k a second at the defaults, and a single PublishBatch of more than that
// would wait ticks it does not need to. The round bound keeps one tick from
// running without end; when the last round it was allowed was still full, the
// backlog is growing faster than the assigner drains it, which is logged rather
// than left to show up as stream latency.
func assignPositions(ctx context.Context, pool *pgxpool.Pool, opts Options) {
	ticker := time.NewTicker(opts.AssignEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for round := 1; ; round++ {
			assigned, err := assignPositionBatch(ctx, pool)
			if err != nil {
				if ctx.Err() == nil {
					opts.Logger.Error("catbird: assigning stream positions failed", "err", err)
				}
				break
			}
			if assigned < assignBatchSize {
				break
			}
			if round == assignRoundsPerTick {
				opts.Logger.Warn("catbird: more stream messages published than positions assigned",
					"assigned", round*assignBatchSize, "every", opts.AssignEvery)
				break
			}
		}
	}
}

const (
	assignBatchSize     = 5000 // positions one statement assigns
	assignRoundsPerTick = 20   // statements one tick runs before it reports a backlog
)

// assignPositionBatch assigns the next assignBatchSize positions and returns
// how many it set. When it set any it sends NOTIFY on channel cb_stream with
// the highest of them, so a LISTENing reader can fetch instead of polling.
//
// The advisory lock is taken for the statement only. When another assigner
// holds it, the one-time filter on the UPDATE skips the scan and the batch
// comes back empty. The UPDATE also requires position IS NULL, checked again on
// the committed row when it had to wait for a lock, so two assigners that run
// at the same time cannot move a position that is already set.
func assignPositionBatch(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var assigned int
	err := pool.QueryRow(ctx, `
		WITH lock AS (
			SELECT pg_try_advisory_xact_lock(hashtext('catbird'), 1) AS held
		),
		unassigned AS (
			SELECT id FROM cb_messages
			WHERE stream AND position IS NULL
			ORDER BY id
			LIMIT $1
		),
		assigned AS (
			UPDATE cb_messages m
			SET position = nextval('cb_position_seq')
			FROM unassigned u
			WHERE m.id = u.id AND m.position IS NULL AND (SELECT held FROM lock)
			RETURNING position
		),
		announcement AS (
			SELECT count(*)::int AS positions, pg_notify('cb_stream', max(position)::text)
			FROM assigned
			HAVING count(*) > 0
		)
		SELECT coalesce((SELECT positions FROM announcement), 0)
	`, assignBatchSize).Scan(&assigned)
	return assigned, err
}

// gcEvery is how often a process with Retention set runs GC. Both deletes are
// index range scans from the old end of their table, so a run that finds
// nothing reads a few pages, and the interval decides how promptly rows go
// rather than what a run costs.
const gcEvery = time.Hour

// collectGarbage runs GC once at start and then every gcEvery until ctx is
// canceled. Every process runs the loop, and one run at a time does the work:
// a run takes advisory lock 4 under catbird's namespace for its transaction
// and skips when another run holds it. A skipped run is not a lost one, since
// whatever it would have deleted is still there for the next. GC deletes
// nothing wrong when two runs overlap, but two runs deleting the same rows can
// lock them in a different order and one is aborted with a deadlock; the lock
// keeps that from happening.
func collectGarbage(ctx context.Context, pool *pgxpool.Pool, opts Options) {
	ticker := time.NewTicker(gcEvery)
	defer ticker.Stop()
	for {
		if err := collectGarbageOnce(ctx, pool, opts.Retention); err != nil && ctx.Err() == nil {
			opts.Logger.Error("catbird: GC failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// collectGarbageOnce runs GC in one transaction behind the advisory lock, and
// does nothing when another process holds it.
func collectGarbageOnce(ctx context.Context, pool *pgxpool.Pool, retention time.Duration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var held bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('catbird'), 4)`).Scan(&held); err != nil {
		return err
	}
	if !held {
		return nil
	}
	if err := GC(ctx, tx, retention); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// declare registers a loop for Start to run, and the channel its wake-ups come
// from — empty for a loop that runs on time alone, like a periodic's. Declaring
// after Start is a programming error: the connection's channel set is fixed
// when it connects.
func (r *Runtime) declare(channel string, loop func(ctx context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.declareLocked(channel, loop)
}

func (r *Runtime) declareLocked(channel string, loop func(ctx context.Context)) {
	if r.started {
		panic("catbird: declared after Start")
	}
	if channel != "" && !slices.Contains(r.channels, channel) {
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
// The connection is taken from the pool and then hijacked out of it. Hijack
// gives the pool its slot back, so a process does not run one connection short
// of MaxConns for as long as it listens, and it takes the connection out of the
// pool's set, so a session carrying LISTEN state is never handed to another
// caller. The process therefore holds MaxConns + 1 connections.
func (r *Runtime) listen(ctx context.Context, channels []string) {
	for ctx.Err() == nil {
		err := func() error {
			pooled, err := r.pool.Acquire(ctx)
			if err != nil {
				return err
			}
			conn := pooled.Hijack()
			// ctx is canceled on shutdown, and closing under a canceled
			// context sets an immediate deadline, so the Terminate never
			// reaches the server and the backend has to notice the dropped
			// socket instead.
			defer conn.Close(context.WithoutCancel(ctx))
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
