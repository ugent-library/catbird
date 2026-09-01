package wire_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
	"github.com/ugent-library/catbird/wire"
)

const testDSN = "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable"

// TestMain holds an advisory lock for the life of the test binary. The root
// package's tests use the same database, and go test runs the two packages'
// binaries at once — without the lock each drops the tables under the other
// mid-test. Session lock 2 under catbird's namespace; the assigner's
// transaction lock is 1.
func TestMain(m *testing.M) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, testDSN)
	if err != nil {
		panic(err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('catbird'), 2)`); err != nil {
		panic(err)
	}
	code := m.Run()
	conn.Close(ctx)
	os.Exit(code)
}

// setupTestDB mirrors the root package's: drop the four tables and apply the
// migration, so tests share no state.
func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	b, err := os.ReadFile("../migrations/00001_lite.sql")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	up, _, _ := strings.Cut(string(b), "-- +goose down")

	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS cb_outputs, cb_claims, cb_cursors, cb_messages CASCADE;
		DROP SEQUENCE IF EXISTS cb_position_seq;
	`)
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err = pool.Exec(ctx, up); err != nil {
		t.Fatalf("schema: %v", err)
	}
	return pool
}

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// startAssigner runs a Runtime so published messages get positions.
func startAssigner(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond, Logger: discardLogger()})
	go rt.Start(ctx)
}

// publish publishes the topics in order and waits until every message in the
// stream has its position, so a poll that follows sees them.
func publish(t *testing.T, pool *pgxpool.Pool, topics ...string) {
	t.Helper()
	ctx := context.Background()
	for _, topic := range topics {
		if _, err := catbird.Publish(ctx, pool, topic, nil, ""); err != nil {
			t.Fatal(err)
		}
	}
	var total int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cb_messages WHERE stream`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		last, err := catbird.LastPosition(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		if last >= total {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d messages got a position", last, total)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func poll(w *wire.Wire, token string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	w.ServePoll(rec, httptest.NewRequest("GET", "/events", nil), token)
	return rec
}

func cursorPosition(t *testing.T, pool *pgxpool.Pool, name string) int64 {
	t.Helper()
	var position int64
	err := pool.QueryRow(context.Background(),
		`SELECT last_position FROM cb_cursors WHERE name = $1`, name).Scan(&position)
	if err != nil {
		t.Fatal(err)
	}
	return position
}

func TestServePollDeliversOnceAcrossPolls(t *testing.T) {
	pool := setupTestDB(t)
	startAssigner(t, pool)

	rd := wire.NewRenderer()
	rd.HandleFunc("record.work.{id}.#", func(r *http.Request, m wire.Match, f *wire.Fragment) error {
		fmt.Fprintf(f, "[record-%s:%d]", m.Var("id"), len(m.Messages))
		return nil
	})
	w := wire.New(pool, rd, wire.Options{Secret: []byte("secret"), Logger: discardLogger()})
	token := w.Token("tray:1", "record.work.#")

	// Two edits of one record and one of another render two fragments, the
	// batched record once.
	publish(t, pool, "record.work.7.updated", "record.work.7.updated", "record.work.9.updated")
	rec := poll(w, token)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if got, want := rec.Body.String(), "[record-7:2][record-9:1]"; got != want {
		t.Fatalf("body %q, want %q", got, want)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("Content-Type %q, want text/html", ct)
	}

	// Sent is seen: the next poll finds nothing.
	if rec := poll(w, token); rec.Code != http.StatusNoContent {
		t.Fatalf("second poll status %d, want 204", rec.Code)
	}

	// A later message arrives alone, not with what was already shown.
	publish(t, pool, "record.work.9.updated")
	if got, want := poll(w, token).Body.String(), "[record-9:1]"; got != want {
		t.Fatalf("third poll body %q, want %q", got, want)
	}
}

func TestThePollReadsOnlyTheTokensTopics(t *testing.T) {
	pool := setupTestDB(t)
	startAssigner(t, pool)

	// The rule is broader than the token; it must still see nothing extra,
	// because the read never returns topics the token does not name.
	rd := wire.NewRenderer()
	rd.HandleFunc("#", func(r *http.Request, m wire.Match, f *wire.Fragment) error {
		for _, msg := range m.Messages {
			fmt.Fprintf(f, "[%s]", msg.Topic)
		}
		return nil
	})
	w := wire.New(pool, rd, wire.Options{Secret: []byte("secret"), Logger: discardLogger()})

	publish(t, pool, "public.news", "secret.plans")
	got := poll(w, w.Token("tray:1", "public.#")).Body.String()
	if got != "[public.news]" {
		t.Fatalf("body %q, want only the token's topic", got)
	}
}

func TestAnEmptyPollWritesNothing(t *testing.T) {
	pool := setupTestDB(t)
	w := wire.New(pool, wire.NewRenderer(), wire.Options{Secret: []byte("secret"), Logger: discardLogger()})

	if rec := poll(w, w.Token("tray:1", "user.1.#")); rec.Code != http.StatusNoContent {
		t.Fatalf("status %d, want 204", rec.Code)
	}
	var rows int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM cb_cursors`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("an empty poll created %d cursor rows, want none", rows)
	}
}

func TestUnrenderedMessagesStillAck(t *testing.T) {
	pool := setupTestDB(t)
	startAssigner(t, pool)

	// No rule matches, so nothing goes out — but the message was read and
	// allowed, so the cursor moves past it.
	w := wire.New(pool, wire.NewRenderer(), wire.Options{Secret: []byte("secret"), Logger: discardLogger()})
	publish(t, pool, "user.1.ping")

	if rec := poll(w, w.Token("tray:1", "user.1.#")); rec.Code != http.StatusNoContent {
		t.Fatalf("status %d, want 204", rec.Code)
	}
	if position := cursorPosition(t, pool, "tray:1"); position != 1 {
		t.Fatalf("cursor at %d, want 1", position)
	}
}

func TestAFailingHandlerDropsItsRegionAndStillAcks(t *testing.T) {
	pool := setupTestDB(t)
	startAssigner(t, pool)

	rd := wire.NewRenderer()
	rd.HandleFunc("user.{id}.#", func(r *http.Request, m wire.Match, f *wire.Fragment) error {
		return fmt.Errorf("template broke")
	})
	rd.HandleFunc("#", func(r *http.Request, m wire.Match, f *wire.Fragment) error {
		fmt.Fprint(f, "[badge]")
		return nil
	})
	w := wire.New(pool, rd, wire.Options{Secret: []byte("secret"), Logger: discardLogger()})
	token := w.Token("tray:1", "user.1.#")

	publish(t, pool, "user.1.ping")
	if got := poll(w, token).Body.String(); got != "[badge]" {
		t.Fatalf("body %q, want the surviving rule's fragment only", got)
	}
	// The dropped call does not hold the page: the poll does not repeat it.
	if rec := poll(w, token); rec.Code != http.StatusNoContent {
		t.Fatalf("second poll status %d, want 204", rec.Code)
	}
}

func TestServePollAnswers401ToAnInvalidToken(t *testing.T) {
	w := wire.New(nil, wire.NewRenderer(), wire.Options{Secret: []byte("secret"), Logger: discardLogger()})
	if rec := poll(w, "not a token"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", rec.Code)
	}
	other := wire.New(nil, wire.NewRenderer(), wire.Options{Secret: []byte("other"), Logger: discardLogger()})
	if rec := poll(w, other.Token("tray:1", "user.1.#")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401 for another secret's token", rec.Code)
	}
}

func TestServeAnswers500WhenTheTokenNamesNoCursor(t *testing.T) {
	// The form where the page holds the position is not built yet, and a
	// token without a cursor must fail loudly rather than read and lose.
	w := wire.New(nil, wire.NewRenderer(), wire.Options{Secret: []byte("secret"), Logger: discardLogger()})
	if rec := poll(w, w.Token("", "user.1.#")); rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", rec.Code)
	}
}
