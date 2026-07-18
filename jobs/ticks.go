package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/ticker"
	"github.com/ugent-library/catbird/notify"
)

// TickerOpts tunes the ticker. Zero fields mean the defaults.
type TickerOpts struct {
	ScheduleInterval time.Duration    // 500ms: scheduled-run accuracy
	TriggerInterval  time.Duration    // 500ms: trigger delivery accuracy
	PruneInterval    time.Duration    // 60s
	Logger           *slog.Logger     // slog.Default()
	Notifier         *notify.Notifier // wakes the trigger tick the moment a source stream publishes; nil = wake by poll only
}

// StartTicker runs the job engine's background work: firing scheduled runs,
// delivering triggers and pruning finished runs past their job's retention.
// Running this from multiple processes is safe, the SQL locks decide who
// does the work. If none are running, scheduled runs, triggers and pruning
// pause but on-demand runs keep working.
func StartTicker(ctx context.Context, pool *pgxpool.Pool, opts ...TickerOpts) error {
	var o TickerOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.ScheduleInterval <= 0 {
		o.ScheduleInterval = 500 * time.Millisecond
	}
	if o.TriggerInterval <= 0 {
		o.TriggerInterval = 500 * time.Millisecond
	}
	if o.PruneInterval <= 0 {
		o.PruneInterval = time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}

	// The trigger tick wakes when one of its source streams publishes.
	// Which streams that is, is read once at start, like the worker reads
	// its queues: a trigger defined while the ticker runs is served by
	// poll until the process restarts.
	var triggerWake <-chan struct{}
	if o.Notifier != nil {
		var schema string
		if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
			return err
		}
		rows, err := pool.Query(ctx, `SELECT DISTINCT stream FROM cb_triggers`)
		if err != nil {
			return err
		}
		streams, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		waker := notify.NewWaker()
		defer waker.Stop()
		for _, s := range streams {
			cancel := o.Notifier.Subscribe(schema+".cbs_"+s, func(string) { waker.Wake() })
			defer cancel()
		}
		triggerWake = waker.C
		o.Logger.Info(fmt.Sprintf("catbird: trigger delivery waking on notify, poll safety net every %s", o.TriggerInterval))
	} else {
		o.Logger.Info(fmt.Sprintf("catbird: trigger delivery waking by poll every %s", o.TriggerInterval))
	}

	t := ticker.New(o.Logger)
	t.Add(ticker.Tick{Name: "job.run_scheduled", Every: o.ScheduleInterval,
		Run: func(ctx context.Context) (int, error) {
			var n int
			err := pool.QueryRow(ctx, `SELECT _cb_job_run_scheduled()`).Scan(&n)
			return n, err
		}})
	t.Add(ticker.Tick{Name: "job.run_triggered", Every: o.TriggerInterval, Wake: triggerWake,
		Run: func(ctx context.Context) (int, error) { return runTriggered(ctx, pool) }})
	t.Add(ticker.Tick{Name: "job.prune_runs", Every: o.PruneInterval,
		Run: func(ctx context.Context) (int, error) {
			var n int64
			err := pool.QueryRow(ctx, `SELECT _cb_job_prune_runs()`).Scan(&n)
			return int(n), err
		}})
	return t.Start(ctx)
}

// runTriggered delivers every trigger's next batch, one call per trigger
// so each batch is its own transaction. A stalled trigger is reported and
// skipped, never blocking the others; the tick logs the collected errors
// every interval until a define or a deploy fixes the cause.
func runTriggered(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	rows, err := pool.Query(ctx, `SELECT name FROM cb_triggers ORDER BY name`)
	if err != nil {
		return 0, err
	}
	triggers, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}

	n := 0
	var errs []error
	for _, name := range triggers {
		var delivered int
		if err := pool.QueryRow(ctx,
			`SELECT _cb_job_run_triggered($1)`, name).Scan(&delivered); err != nil {
			errs = append(errs, fmt.Errorf("trigger %s: %w", name, wrapErr(err)))
			continue
		}
		n += delivered
	}
	return n, errors.Join(errs...)
}
