package streams

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/ticker"
)

// TickerOpts tunes the ticker. Zero fields mean the defaults.
type TickerOpts struct {
	AssignPositionsInterval time.Duration // 100ms: publish consume latency
	DeliverInterval         time.Duration // 500ms: delayed message and schedule accuracy
	PruneInterval           time.Duration // 60s
	Logger                  *slog.Logger  // slog.Default()
}

// StartTicker runs the stream engine's background work: assigning positions,
// delivering due pending messages and schedules, and pruning expired
// messages and keys.
// Running this from multiple processes is safe, the SQL locks decide who
// does the work. If none are running, delivery pauses but publishing keeps
// working.
func StartTicker(ctx context.Context, pool *pgxpool.Pool, opts ...TickerOpts) error {
	var o TickerOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.AssignPositionsInterval <= 0 {
		o.AssignPositionsInterval = 100 * time.Millisecond
	}
	if o.DeliverInterval <= 0 {
		o.DeliverInterval = 500 * time.Millisecond
	}
	if o.PruneInterval <= 0 {
		o.PruneInterval = time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}

	t := ticker.New(o.Logger)
	t.Add(ticker.Tick{Name: "stream.assign", Every: o.AssignPositionsInterval,
		Run: func(ctx context.Context) (int, error) { return assignPositions(ctx, pool) }})
	t.Add(ticker.Tick{Name: "stream.deliver", Every: o.DeliverInterval,
		Run: func(ctx context.Context) (int, error) { return deliver(ctx, pool) }})
	t.Add(ticker.Tick{Name: "stream.prune", Every: o.PruneInterval,
		Run: func(ctx context.Context) (int, error) { return prune(ctx, pool) }})
	return t.Start(ctx)
}

func assignPositions(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	rows, err := pool.Query(ctx,
		`SELECT DISTINCT stream FROM cb_stream_messages WHERE pos IS NULL`)
	if err != nil {
		return 0, err
	}
	streams, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}

	n := 0
	for _, s := range streams {
		var assigned int
		if err := pool.QueryRow(ctx,
			`SELECT _cb_stream_assign_positions($1)`, s).Scan(&assigned); err != nil {
			return n, err
		}
		n += assigned
	}
	return n, nil
}

func deliver(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var pending, schedules int
	if err := pool.QueryRow(ctx,
		`SELECT _cb_stream_deliver_pending()`).Scan(&pending); err != nil {
		return 0, err
	}
	if err := pool.QueryRow(ctx,
		`SELECT _cb_stream_deliver_schedules()`).Scan(&schedules); err != nil {
		return pending, err
	}
	return pending + schedules, nil
}

func prune(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	rows, err := pool.Query(ctx, `SELECT name FROM cb_streams`)
	if err != nil {
		return 0, err
	}
	streams, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}

	n := 0
	for _, s := range streams {
		var messages, keys int
		if err := pool.QueryRow(ctx,
			`SELECT _cb_stream_prune_messages($1)`, s).Scan(&messages); err != nil {
			return n, err
		}
		if err := pool.QueryRow(ctx,
			`SELECT _cb_stream_prune_keys($1)`, s).Scan(&keys); err != nil {
			return n + messages, err
		}
		n += messages + keys
	}
	return n, nil
}
