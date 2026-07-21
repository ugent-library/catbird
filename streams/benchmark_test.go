package streams

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/ugent-library/catbird/notify"
)

// The M5 exit benchmark. BenchmarkPublishConsumeLatency measures end-to-end
// publish→consume latency — publish round trip, position assignment, and
// delivery to a live consume loop's handler: ns/op is the latency of one
// message. The notify variant runs with polls too slow to deliver, so only
// the NOTIFY wake path delivers; the poll variant runs with a nil
// notifier at the default intervals, the tick math M1's decision gate
// measured (assigner 100ms + consume poll 250ms).
func BenchmarkPublishConsumeLatency(b *testing.B) {
	b.Run("notify", func(b *testing.B) {
		benchmarkPublishConsume(b, "go_bench_nfy", testNotifier(b))
	})
	b.Run("poll", func(b *testing.B) {
		benchmarkPublishConsume(b, "go_bench_poll", nil)
	})
}

func benchmarkPublishConsume(b *testing.B, stream string, n *notify.Notifier) {
	pool := setupTest(b)
	ctx := context.Background()

	if err := Ensure(ctx, pool, stream); err != nil {
		b.Fatal(err)
	}
	if err := EnsureCursor(ctx, pool, stream, "w", CursorOpts{StartPos: At(0)}); err != nil {
		b.Fatal(err)
	}

	// with a notifier, push the polls out of the measurement window; with
	// none, zero values keep the defaults the poll variant is measuring
	var slow time.Duration
	if n != nil {
		slow = 10 * time.Second
	}

	cctx, cancel := context.WithCancel(ctx)
	ticksDone := make(chan error, 1)
	go func() {
		ticksDone <- StartTicker(cctx, pool, TickerOpts{
			AssignPositionsInterval: slow,
			DeliverInterval:         slow,
			Notifier:                n,
		})
	}()

	got := make(chan string, 16)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- Consume(cctx, pool, stream, "w", func(_ context.Context, batch []Message) error {
			for _, m := range batch {
				got <- string(m.Payload)
			}
			return nil
		}, ConsumeOpts{PollInterval: slow, Notifier: n})
	}()

	// one message outside the timer covers loop startup, and — on the
	// notify variant — the window before the subscriptions reach the
	// LISTEN connection (its wait is generous: a lost notification there
	// costs one slow poll, not a failure)
	if n != nil {
		time.Sleep(500 * time.Millisecond)
	}
	roundTrip := func(i int, timeout time.Duration) {
		if _, err := Publish(ctx, pool, stream, "t", i); err != nil {
			b.Fatal(err)
		}
		select {
		case payload := <-got:
			if want := strconv.Itoa(i); payload != want {
				b.Fatalf("payload = %s, want %s", payload, want)
			}
		case <-time.After(timeout):
			b.Fatalf("message %d did not arrive within %s", i, timeout)
		}
	}
	roundTrip(-1, 15*time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roundTrip(i, 15*time.Second)
	}
	b.StopTimer()

	cancel()
	for _, done := range []chan error{ticksDone, consumeDone} {
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			b.Fatal(err)
		}
	}
}
