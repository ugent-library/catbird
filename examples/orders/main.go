// The vision's §4 example, running: one Publish inside the caller's
// transaction fans out to every declared consumer — a worker pool, a job
// chain, a live browser push and a durable inbox row — and none of it
// fires when the transaction rolls back.
//
// Run it with a local PostgreSQL (docker compose up -d, or set CB_DSN),
// then open http://localhost:8080 and place an order. The README walks
// through what happens and where to look.
package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ugent-library/catbird/jobs"
	"github.com/ugent-library/catbird/notify"
	"github.com/ugent-library/catbird/streams"
	"github.com/ugent-library/catbird/wire"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable"
	// one hardcoded browser user — a real app would put its session's
	// user id here
	identity = "demo-user"
	addr     = ":8080"
)

// demo only — a real secret comes from configuration
var secret = []byte("an-example-32-byte-demo-secret!!")

//go:embed index.html
var indexFS embed.FS

// Order is the message payload: the stream, the trigger-born job run and
// the browser all read this same shape.
type Order struct {
	ID    int64    `json:"id"`
	Items []string `json:"items"`
}

// Pick is one item of an order, the fan-out step's input.
type Pick struct {
	OrderID int64  `json:"order_id"`
	Item    string `json:"item"`
}

var catalog = []string{"field guide", "binoculars", "bird seed", "nest box", "feather brush", "spotting scope"}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\ndemo failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dsn := os.Getenv("CB_DSN")
	if dsn == "" {
		dsn = defaultDSN
	}
	slog.Info("connecting to PostgreSQL", "url", redacted(dsn))

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	// prove the database is reachable before anything else, and give up
	// fast: a dial that hangs would otherwise sit silent for a minute
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	err = db.PingContext(pingCtx)
	cancelPing()
	if err != nil {
		return fmt.Errorf("cannot reach PostgreSQL at %s — start it with `docker compose up -d`, or point CB_DSN at your server: %w", redacted(dsn), err)
	}

	// migrations: each module manages its own schema
	if err := streams.MigrateUpTo(ctx, db, streams.SchemaVersion); err != nil {
		return err
	}
	if err := jobs.MigrateUpTo(ctx, db, jobs.SchemaVersion); err != nil {
		return err
	}
	if err := wire.MigrateUpTo(ctx, db, wire.SchemaVersion); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	// the demo's own table — the row whose commit the publish rides
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS demo_orders (
		    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		    items jsonb NOT NULL,
		    created_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return err
	}

	// declare the catalog: one stream, two readers on it, three jobs and
	// the trigger that connects stream to engine. All idempotent — every
	// boot declares the same things.
	if err := streams.Ensure(ctx, pool, "orders"); err != nil {
		return err
	}
	if err := streams.EnsureSubscription(ctx, pool, "orders", "fulfilment"); err != nil {
		return err
	}
	if err := streams.EnsureCursor(ctx, pool, "orders", "relay"); err != nil {
		return err
	}
	// the demo's own queue: a worker must handle every job on the queues
	// it claims, so sharing a queue with other apps' jobs won't start
	if err := jobs.DefineQueue(ctx, pool, "orders_engine"); err != nil {
		return err
	}
	for _, job := range []string{"pick_item", "confirm_order", "process_order"} {
		if err := jobs.Define(ctx, pool, job, jobs.JobOpts{Queue: "orders_engine"}); err != nil {
			return err
		}
	}
	if err := jobs.DefineTrigger(ctx, pool, "order_intake", "orders", "process_order",
		jobs.TriggerOpts{Topic: "order.placed"}); err != nil {
		return err
	}

	// one notifier per process; every consumer below shares it the way
	// they share the pool
	n := notify.New(pool)

	w := wire.New(pool, secret, wire.Opts{Notifier: n})
	wire.Render(w, "order.placed", func(_ *http.Request, _ string, o Order) (wire.Fragment, error) {
		return fragment("placed", "order #%d placed: %s", o.ID, html.EscapeString(strings.Join(o.Items, ", "))), nil
	})
	wire.Render(w, "order.item_picked", func(_ *http.Request, _ string, p Pick) (wire.Fragment, error) {
		return fragment("picked", "order #%d: picked the %s", p.OrderID, html.EscapeString(p.Item)), nil
	})
	wire.Render(w, "order.processed", func(_ *http.Request, _ string, o Order) (wire.Fragment, error) {
		return fragment("processed", "order #%d processed — all %d items picked", o.ID, len(o.Items)), nil
	})
	w.Render("inbox.order", func(_ *http.Request, _, message string) (wire.Fragment, error) {
		return wire.Fragment{Data: "<li>" + html.EscapeString(message) + "</li>"}, nil
	})

	// the job chain: process_order fans out one pick_item per item, and
	// confirm_order runs after all of them — the barrier
	worker := jobs.NewWorker(pool, jobs.WorkerOpts{Notifier: n})
	worker.Handle("process_order", func(_ context.Context, p *jobs.Plan, order Order) error {
		for _, item := range order.Items {
			p.Step("pick_item", Pick{OrderID: order.ID, Item: item})
		}
		p.After().Step("confirm_order", order)
		return nil
	})
	worker.Handle("pick_item", func(ctx context.Context, pick Pick) error {
		time.Sleep(300 * time.Millisecond) // the work
		slog.Info("picked an item", "order", pick.OrderID, "item", pick.Item)
		return w.Notify(ctx, "order.item_picked", mustJSON(pick))
	})
	worker.Handle("confirm_order", func(ctx context.Context, order Order) error {
		slog.Info("order processed", "order", order.ID)
		return w.Notify(ctx, "order.processed", mustJSON(order))
	})

	// every long-runner reports here; the first real error ends the demo
	fail := make(chan error, 1)
	start := func(name string, fn func(context.Context) error) {
		go func() {
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				select {
				case fail <- fmt.Errorf("%s: %w", name, err):
				default:
				}
			}
		}()
	}

	start("notifier", n.Start)
	start("stream ticker", func(ctx context.Context) error {
		return streams.StartTicker(ctx, pool, streams.TickerOpts{Notifier: n})
	})
	start("job ticker", func(ctx context.Context) error {
		return jobs.StartTicker(ctx, pool, jobs.TickerOpts{Notifier: n})
	})
	start("wire ticker", func(ctx context.Context) error {
		return wire.StartTicker(ctx, pool)
	})
	start("wire", w.Start)
	start("job worker", worker.Start)

	// the worker-pool leg: competing consumers would each run this loop
	start("fulfilment", func(ctx context.Context) error {
		return streams.ConsumeSubscription(ctx, pool, "orders", "fulfilment",
			func(_ context.Context, m streams.Message) error {
				var order Order
				if err := json.Unmarshal(m.Payload, &order); err != nil {
					return err
				}
				slog.Info("fulfilment picked up an order", "order", order.ID, "items", len(order.Items))
				return nil
			}, streams.ConsumeSubscriptionOpts{Notifier: n})
	})

	// the browser leg: a cursor holding the full message calls wire —
	// live push and durable inbox row, 02 §5's consumer-callback relay
	start("relay", func(ctx context.Context) error {
		return streams.Consume(ctx, pool, "orders", "relay",
			func(ctx context.Context, batch []streams.Message) error {
				for _, m := range batch {
					if err := w.Notify(ctx, m.Topic, string(m.Payload)); err != nil {
						return err
					}
					var order Order
					if err := json.Unmarshal(m.Payload, &order); err != nil {
						return err
					}
					if _, err := wire.NotifyDurable(ctx, pool, identity, "inbox.order",
						fmt.Sprintf("Order #%d placed — %d items", order.ID, len(order.Items))); err != nil {
						return err
					}
				}
				return nil
			}, streams.ConsumeOpts{Notifier: n})
	})

	page, err := template.ParseFS(indexFS, "index.html")
	if err != nil {
		return err
	}
	token := w.Token([]string{"order.#", "inbox.#"}, wire.TokenOpts{Identity: identity})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(rw http.ResponseWriter, r *http.Request) {
		if err := page.Execute(rw, map[string]string{"Token": token}); err != nil {
			slog.Warn("page render failed", "error", err)
		}
	})
	mux.HandleFunc("GET /events", func(rw http.ResponseWriter, r *http.Request) {
		w.ServeSSE(rw, r, r.URL.Query().Get("token"))
	})
	mux.HandleFunc("GET /inbox", func(rw http.ResponseWriter, r *http.Request) {
		w.ServePoll(rw, r, r.URL.Query().Get("token"))
	})
	mux.HandleFunc("POST /inbox/read", func(rw http.ResponseWriter, r *http.Request) {
		until, err := strconv.ParseInt(r.URL.Query().Get("until"), 10, 64)
		if err != nil {
			http.Error(rw, "until must be an inbox cursor", http.StatusBadRequest)
			return
		}
		if _, err := wire.MarkReadUntil(r.Context(), pool, identity, until); err != nil {
			http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("POST /order", func(rw http.ResponseWriter, r *http.Request) {
		order, err := placeOrder(r.Context(), pool, r.URL.Query().Get("fail") == "1")
		if err != nil {
			slog.Warn("placing an order failed", "error", err)
			http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		if r.URL.Query().Get("fail") == "1" {
			fmt.Fprintf(rw, "order #%d rolled back — nothing will happen", order.ID)
			return
		}
		fmt.Fprintf(rw, "order #%d placed", order.ID)
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	start("http", func(context.Context) error {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("demo running — open http://localhost" + addr)

	select {
	case <-ctx.Done():
		return nil
	case err := <-fail:
		return err
	}
}

// placeOrder writes the order row and publishes its event in one
// transaction. With rollBack it aborts instead of committing: the row,
// the message and everything downstream of it never happen.
func placeOrder(ctx context.Context, pool *pgxpool.Pool, rollBack bool) (Order, error) {
	items := make([]string, 2+rand.IntN(2))
	for i, j := range rand.Perm(len(catalog))[:len(items)] {
		items[i] = catalog[j]
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return Order{}, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))

	var order Order
	order.Items = items
	if err := tx.QueryRow(ctx,
		`INSERT INTO demo_orders (items) VALUES ($1::jsonb) RETURNING id`,
		mustJSON(items)).Scan(&order.ID); err != nil {
		return Order{}, err
	}
	if _, err := streams.Publish(ctx, tx, "orders", "order.placed", order); err != nil {
		return Order{}, err
	}
	if rollBack {
		return order, tx.Rollback(ctx)
	}
	return order, tx.Commit(ctx)
}

// fragment builds one live-feed entry.
func fragment(kind, format string, args ...any) wire.Fragment {
	return wire.Fragment{
		Data: fmt.Sprintf(`<li class="%s">`, kind) + fmt.Sprintf(format, args...) + "</li>",
	}
}

// redacted is the connection URL with its password hidden, for log lines.
func redacted(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return dsn
	}
	if _, has := u.User.Password(); has {
		u.User = url.UserPassword(u.User.Username(), "xxx")
	}
	return u.String()
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
