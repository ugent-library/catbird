package wire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/internal/ticker"
	"github.com/ugent-library/catbird/notify"
)

// TickerOpts tunes the ticker. Zero fields mean the defaults. Wire has
// no retention declarations, so the inbox windows are configuration, not
// rows: set them here per app.
type TickerOpts struct {
	// Notifier wakes relay delivery when a source stream publishes. Nil
	// delivers by poll on RelayInterval alone.
	Notifier      *notify.Notifier
	RelayInterval time.Duration // 2s
	PruneInterval time.Duration // 60s
	ReadRetention time.Duration // 30d: an inbox row read longer ago is deleted
	SeenRetention time.Duration // 90d: an inbox row seen longer ago is deleted
	MaxAge        time.Duration // 365d: any inbox row older is deleted, seen or not
	Logger        *slog.Logger  // slog.Default()
}

// StartTicker runs the wire module's background work: delivering relays
// and pruning the rows their readers are done with.
//
// Relay delivery runs every RelayInterval and on every publish to a
// relay's source stream when a Notifier is given. Which streams that is,
// is read once at start: a relay defined while the ticker runs is served
// by poll until the process restarts.
//
// The prune removes lapsed watches, expired presence rows (nudging their
// topics so watching pages drop them), and the inbox rows the recipients
// are done with: explicit expiry always wins, seen or not; otherwise
// read longer ago than ReadRetention, seen longer ago than
// SeenRetention, or older than MaxAge outright. A row that was never
// seen and has no expiry lives the full MaxAge — it waits to be seen.
//
// Running this from multiple processes is safe: relay delivery locks per
// relay, and the deletes are independent.
func StartTicker(ctx context.Context, pool *pgxpool.Pool, opts ...TickerOpts) error {
	var o TickerOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.RelayInterval <= 0 {
		o.RelayInterval = 2 * time.Second
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

	// The relay tick wakes when one of its source streams publishes,
	// like the job module's trigger tick.
	var relayWake <-chan struct{}
	if o.Notifier != nil {
		var schema string
		if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
			return err
		}
		rows, err := pool.Query(ctx, `SELECT DISTINCT stream FROM cb_wire_relays`)
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
			cancel := o.Notifier.Subscribe(schema+".cbs_"+s,
				func(string) { waker.Wake() },
				func() { waker.Wake() })
			defer cancel()
		}
		relayWake = waker.C
		o.Logger.Info(fmt.Sprintf("catbird: relay delivery waking on notify, poll safety net every %s", o.RelayInterval))
	} else {
		o.Logger.Info(fmt.Sprintf("catbird: relay delivery waking by poll every %s", o.RelayInterval))
	}

	t := ticker.New(o.Logger)
	t.Add(ticker.Tick{Name: "wire.relay_deliver", Every: o.RelayInterval, Wake: relayWake,
		Run: func(ctx context.Context) (int, error) { return deliverRelays(ctx, pool) }})
	t.Add(ticker.Tick{Name: "wire.prune", Every: o.PruneInterval,
		Run: func(ctx context.Context) (int, error) {
			var inbox, watches, presence int64
			err := pool.QueryRow(ctx,
				`SELECT cb_wire_prune_inbox($1, $2, $3), cb_wire_prune_subscriptions(), cb_wire_prune_presence()`,
				interval(o.ReadRetention), interval(o.SeenRetention), interval(o.MaxAge)).
				Scan(&inbox, &watches, &presence)
			return int(inbox + watches + presence), err
		}})
	return t.Start(ctx)
}

// deliverRelays delivers every relay's next batch, one call per relay so
// each batch is its own transaction. A stalled relay is reported and
// skipped, never blocking the others; the tick logs the collected errors
// every interval until a define or a deploy fixes the cause.
func deliverRelays(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	rows, err := pool.Query(ctx, `SELECT name FROM cb_wire_relays ORDER BY name`)
	if err != nil {
		return 0, err
	}
	relays, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}

	n := 0
	var errs []error
	for _, name := range relays {
		var delivered int
		if err := pool.QueryRow(ctx,
			`SELECT cb_wire_relay_deliver($1)`, name).Scan(&delivered); err != nil {
			errs = append(errs, fmt.Errorf("relay %s: %w", name, wrapErr(err)))
			continue
		}
		n += delivered
	}
	return n, errors.Join(errs...)
}

func interval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}
