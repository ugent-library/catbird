package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/ticker"
)

// TickerOpts tunes the ticker. Zero fields mean the defaults.
type TickerOpts struct {
	ScheduleInterval time.Duration // 500ms: scheduled-run accuracy
	PruneInterval    time.Duration // 60s
	Logger           *slog.Logger  // slog.Default()
}

// StartTicker runs the job engine's background work: firing scheduled runs
// and pruning finished runs past their job's retention.
// Running this from multiple processes is safe, the SQL locks decide who
// does the work. If none are running, scheduled runs and pruning pause but
// on-demand runs keep working.
func StartTicker(ctx context.Context, pool *pgxpool.Pool, opts ...TickerOpts) error {
	var o TickerOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.ScheduleInterval <= 0 {
		o.ScheduleInterval = 500 * time.Millisecond
	}
	if o.PruneInterval <= 0 {
		o.PruneInterval = time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}

	t := ticker.New(o.Logger)
	t.Add(ticker.Tick{Name: "job.run_scheduled", Every: o.ScheduleInterval,
		Run: func(ctx context.Context) (int, error) {
			var n int
			err := pool.QueryRow(ctx, `SELECT _cb_job_run_scheduled()`).Scan(&n)
			return n, err
		}})
	t.Add(ticker.Tick{Name: "job.prune_runs", Every: o.PruneInterval,
		Run: func(ctx context.Context) (int, error) {
			var n int64
			err := pool.QueryRow(ctx, `SELECT _cb_job_prune_runs()`).Scan(&n)
			return int(n), err
		}})
	return t.Start(ctx)
}
