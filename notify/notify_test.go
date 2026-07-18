package notify

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testDSN = "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable"

var (
	testPool *pgxpool.Pool
	testOnce sync.Once
)

func setupTest(t *testing.T) *pgxpool.Pool {
	t.Helper()
	testOnce.Do(func() {
		var err error
		testPool, err = pgxpool.New(context.Background(), testDSN)
		if err != nil {
			panic(err)
		}
	})
	return testPool
}

// startNotifier runs a notifier for the test's lifetime.
func startNotifier(t *testing.T, pool *pgxpool.Pool) *Notifier {
	t.Helper()
	n := New(pool)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = n.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return n
}

// sendUntil sends payload on the channel until got reports it arrived:
// a NOTIFY sent before the notifier's LISTEN is active is simply gone,
// so a single send would race the subscription.
func sendUntil(t *testing.T, pool *pgxpool.Pool, channel, payload string, got <-chan string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if _, err := pool.Exec(context.Background(),
			`SELECT pg_notify($1, $2)`, channel, payload); err != nil {
			t.Fatal(err)
		}
		select {
		case p := <-got:
			if p == payload {
				return
			}
			// an empty payload is the (re)connect signal, anything else
			// a leftover of an earlier retried send; keep waiting
		case <-deadline:
			t.Fatalf("no notification arrived on %s", channel)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestNotifier(t *testing.T) {
	pool := setupTest(t)
	n := startNotifier(t, pool)

	a1 := make(chan string, 16)
	a2 := make(chan string, 16)
	b := make(chan string, 16)
	cancelA1 := n.Subscribe("nt_a", func(p string) { a1 <- p })
	n.Subscribe("nt_a", func(p string) { a2 <- p })
	n.Subscribe("nt_b", func(p string) { b <- p })

	// both subscribers of a channel get the payload as sent
	sendUntil(t, pool, "nt_a", "hello", a1)
	select {
	case p := <-a2:
		if p != "" && p != "hello" {
			t.Fatalf("second subscriber got %q, want %q", p, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second subscriber got nothing")
	}

	// channels are isolated
	sendUntil(t, pool, "nt_b", "other", b)
	select {
	case p := <-a1:
		if p != "" && p != "hello" {
			t.Fatalf("subscriber of nt_a got nt_b's payload %q", p)
		}
	default:
	}

	// an ended subscription stops delivering while others keep going
	cancelA1()
	drain(a1)
	sendUntil(t, pool, "nt_a", "after", a2)
	select {
	case p := <-a1:
		if p != "" {
			t.Fatalf("ended subscription still delivered %q", p)
		}
	default:
	}
}

func TestNotifierSubscribeWhileRunning(t *testing.T) {
	pool := setupTest(t)
	n := startNotifier(t, pool)

	// let the notifier reach its notification wait first, so this
	// subscription must interrupt a blocked wait to apply its LISTEN
	warm := make(chan string, 16)
	n.Subscribe("nt_warm", func(p string) { warm <- p })
	sendUntil(t, pool, "nt_warm", "up", warm)

	got := make(chan string, 16)
	n.Subscribe("nt_late", func(p string) { got <- p })
	start := time.Now()
	sendUntil(t, pool, "nt_late", "prompt", got)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("late subscription took %s to deliver", elapsed)
	}
}

func TestNotifierReconnect(t *testing.T) {
	appName := fmt.Sprintf("cb_notify_test_%d", time.Now().UnixNano())
	pool, err := pgxpool.New(context.Background(),
		testDSN+"&application_name="+appName)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	n := startNotifier(t, pool)

	got := make(chan string, 16)
	n.Subscribe("nt_re", func(p string) { got <- p })

	// the connect signal: one empty payload telling the subscriber to
	// look for itself
	waitPayload(t, got, "")
	sendUntil(t, setupTest(t), "nt_re", "before", got)

	// kill the notifier's connection; it must reconnect, signal the
	// subscriber again, and keep delivering
	if _, err := setupTest(t).Exec(context.Background(),
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		 WHERE application_name = $1`, appName); err != nil {
		t.Fatal(err)
	}
	waitPayload(t, got, "")
	sendUntil(t, setupTest(t), "nt_re", "after", got)
}

func TestWaker(t *testing.T) {
	w := NewWaker()
	defer w.Stop()

	// a zero or passed time signals at once
	w.WakeAt(time.Time{})
	waitSignal(t, w.C, 100*time.Millisecond)
	w.WakeAt(time.Now().Add(-time.Second))
	waitSignal(t, w.C, 100*time.Millisecond)

	// a future time signals when it arrives, not before
	w.WakeAt(time.Now().Add(200 * time.Millisecond))
	select {
	case <-w.C:
		t.Fatal("future wake signaled at once")
	case <-time.After(50 * time.Millisecond):
	}
	waitSignal(t, w.C, time.Second)

	// of two pending times the earliest fires
	start := time.Now()
	w.WakeAt(start.Add(2 * time.Second))
	w.WakeAt(start.Add(100 * time.Millisecond))
	waitSignal(t, w.C, time.Second)
	if time.Since(start) > time.Second {
		t.Fatal("earlier wake did not replace the later one")
	}
	w.Stop()

	// a stopped waker drops its pending wake
	w.WakeAt(time.Now().Add(50 * time.Millisecond))
	w.Stop()
	select {
	case <-w.C:
		t.Fatal("stopped waker signaled")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestParseTime(t *testing.T) {
	if !ParseTime("").IsZero() {
		t.Fatal("empty payload must be a zero time")
	}
	if !ParseTime("not a time").IsZero() {
		t.Fatal("unparseable payload must be a zero time")
	}
	want := time.Date(2026, 7, 18, 12, 30, 0, 123456000, time.UTC)
	got := ParseTime("2026-07-18T12:30:00.123456Z")
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func waitPayload(t *testing.T, got <-chan string, want string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case p := <-got:
			if p == want {
				return
			}
		case <-deadline:
			t.Fatalf("payload %q did not arrive", want)
		}
	}
}

func waitSignal(t *testing.T, c <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(timeout):
		t.Fatal("no signal")
	}
}

func drain(c chan string) {
	for {
		select {
		case <-c:
		default:
			return
		}
	}
}
