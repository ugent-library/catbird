package wire

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/ticker"
)

// TickerOpts tunes the ticker. Zero fields mean the defaults. Wire has
// no definitions table, so the retention windows are configuration, not
// declarations: set them here per app.
type TickerOpts struct {
	PruneInterval time.Duration // 60s
	ReadRetention time.Duration // 30d: a row read longer ago is deleted
	SeenRetention time.Duration // 90d: a row seen longer ago is deleted
	MaxAge        time.Duration // 365d: any row older is deleted, seen or not
	Logger        *slog.Logger  // slog.Default()
}

// StartTicker runs the wire module's background work: pruning inbox rows
// the identities are done with. A row leaves when its explicit expiry has
// passed — that always wins, seen or not — or by the retention tiers:
// read longer ago than ReadRetention, seen longer ago than SeenRetention,
// or older than MaxAge outright. A row that was never seen and has no
// expiry lives the full MaxAge — it waits to be seen.
// Running this from multiple processes is safe; the deletes are
// independent. Nothing wakes retention, so there is no Notifier here.
func StartTicker(ctx context.Context, pool *pgxpool.Pool, opts ...TickerOpts) error {
	var o TickerOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.PruneInterval <= 0 {
		o.PruneInterval = time.Minute
	}
	if o.ReadRetention <= 0 {
		o.ReadRetention = 30 * 24 * time.Hour
	}
	if o.SeenRetention <= 0 {
		o.SeenRetention = 90 * 24 * time.Hour
	}
	if o.MaxAge <= 0 {
		o.MaxAge = 365 * 24 * time.Hour
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}

	t := ticker.New(o.Logger)
	t.Add(ticker.Tick{Name: "wire.prune", Every: o.PruneInterval,
		Run: func(ctx context.Context) (int, error) {
			var n int64
			err := pool.QueryRow(ctx,
				`SELECT _cb_wire_prune_inbox($1, $2, $3)`,
				interval(o.ReadRetention), interval(o.SeenRetention), interval(o.MaxAge)).Scan(&n)
			return int(n), err
		}})
	return t.Start(ctx)
}

func interval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}
