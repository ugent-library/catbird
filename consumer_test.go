package catbird_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ugent-library/catbird"
)

// Two processes run one consumer: every message is handled once and in
// position order, because a process claims the cursor before it reads and the
// other finds it claimed. Full batches are followed up at once, so 120
// messages in batches of 50 are three handler calls, and the cursor ends at
// the last position, released.
func TestConsumeHandlesEachMessageOnceInOrder(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const total = 120
	msgs := make([]catbird.BatchMessage, total)
	for i := range msgs {
		msgs[i] = catbird.BatchMessage{Topic: "record.work", Payload: i}
	}
	if n, err := catbird.PublishBatch(ctx, pool, msgs); err != nil || n != total {
		t.Fatalf("published %d messages (%v)", n, err)
	}

	var mu sync.Mutex
	var positions []int64
	batches := 0
	handle := func(ctx context.Context, batch []catbird.Message) error {
		mu.Lock()
		defer mu.Unlock()
		batches++
		for _, m := range batch {
			positions = append(positions, m.Position)
		}
		return nil
	}
	for range 2 {
		rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
		rt.Consume("indexer", []string{"record.#"}, handle, catbird.ConsumeOptions{BatchSize: 50, PollInterval: 50 * time.Millisecond})
		go rt.Start(ctx)
	}

	waitFor(t, 5*time.Second, "not every message was handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(positions) >= total
	})
	time.Sleep(300 * time.Millisecond) // let both processes run again on the cursor

	mu.Lock()
	defer mu.Unlock()
	if len(positions) != total {
		t.Fatalf("%d messages handled for %d published", len(positions), total)
	}
	for i := 1; i < len(positions); i++ {
		if positions[i] <= positions[i-1] {
			t.Fatalf("position %d handled after %d", positions[i], positions[i-1])
		}
	}
	if batches != 3 {
		t.Errorf("%d handler calls for %d messages in batches of 50", batches, total)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_cursors WHERE name = 'consumer:indexer' AND last_position = $1 AND claimable_at = '-infinity'", positions[total-1]); n != 1 {
		t.Errorf("the cursor is not at the last position and released")
	}
}

// A handler error leaves the cursor where it was: the next round hands the
// same batch to the handler again, and the cursor moves once it passes.
func TestConsumeRetriesAFailedBatch(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := range 3 {
		if _, err := catbird.Publish(ctx, pool, "record.work", i, ""); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var calls [][]int64
	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	rt.Consume("indexer", []string{"record.#"}, func(ctx context.Context, batch []catbird.Message) error {
		mu.Lock()
		defer mu.Unlock()
		var positions []int64
		for _, m := range batch {
			positions = append(positions, m.Position)
		}
		calls = append(calls, positions)
		if len(calls) == 1 {
			return errors.New("index unavailable")
		}
		return nil
	}, catbird.ConsumeOptions{PollInterval: 50 * time.Millisecond})
	go rt.Start(ctx)

	waitFor(t, 5*time.Second, "the failed batch was not handed out again", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) >= 2
	})
	time.Sleep(200 * time.Millisecond) // let the ack land

	mu.Lock()
	defer mu.Unlock()
	if len(calls[0]) != 3 || !slices.Equal(calls[0], calls[1]) {
		t.Fatalf("the retried batch %v is not the failed one %v", calls[1], calls[0])
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_cursors WHERE name = 'consumer:indexer' AND last_position = $1", calls[1][2]); n != 1 {
		t.Errorf("the cursor did not move past the batch that passed")
	}
}

// The handler's context ends at HandlerTimeout. The batch counts as failed,
// the cursor is released without moving, and the batch comes back.
func TestConsumeHandlerTimeoutEndsTheHandler(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := catbird.Publish(ctx, pool, "record.work", 1, ""); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	waited := make(chan time.Duration, 1)
	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	rt.Consume("indexer", []string{"record.#"}, func(ctx context.Context, batch []catbird.Message) error {
		if calls.Add(1) > 1 {
			return nil
		}
		start := time.Now()
		<-ctx.Done()
		waited <- time.Since(start)
		return ctx.Err()
	}, catbird.ConsumeOptions{HandlerTimeout: 100 * time.Millisecond, PollInterval: 20 * time.Millisecond})
	go rt.Start(ctx)

	select {
	case d := <-waited:
		if d < 100*time.Millisecond || d > 2*time.Second {
			t.Errorf("the handler's context ended after %s, want about 100ms", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler's context never ended")
	}
	waitFor(t, 5*time.Second, "the batch did not come back", func() bool { return calls.Load() >= 2 })
	time.Sleep(200 * time.Millisecond) // let the ack land
	if n := count(t, pool, "SELECT count(*) FROM cb_cursors WHERE name = 'consumer:indexer' AND last_position = (SELECT max(position) FROM cb_messages)"); n != 1 {
		t.Errorf("the cursor did not move past the batch that passed")
	}
}

// With HandlerTimeout above ClaimDuration the consumer renews its claim while
// the handler runs, so a batch that takes longer than ClaimDuration is not
// taken over: the other process finds the cursor claimed the whole time, the
// batch is handled once, and the ack is accepted.
func TestConsumeRenewsTheClaimWhileTheHandlerRuns(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := catbird.Publish(ctx, pool, "record.work", 1, ""); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	handled := map[string]int{} // handler calls, by process
	handledBy := func(process string) int {
		mu.Lock()
		defer mu.Unlock()
		return handled[process]
	}
	var logs lockedBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	opts := catbird.ConsumeOptions{ClaimDuration: 300 * time.Millisecond, HandlerTimeout: 5 * time.Second, PollInterval: 20 * time.Millisecond}
	slow := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond, Logger: logger})
	slow.Consume("indexer", []string{"record.#"}, func(ctx context.Context, batch []catbird.Message) error {
		mu.Lock()
		handled["slow"]++
		mu.Unlock()
		time.Sleep(900 * time.Millisecond) // three claims long
		return nil
	}, opts)
	go slow.Start(ctx)
	waitFor(t, 5*time.Second, "the slow process never claimed the cursor", func() bool { return handledBy("slow") == 1 })

	fast := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond, Logger: logger})
	fast.Consume("indexer", []string{"record.#"}, func(ctx context.Context, batch []catbird.Message) error {
		mu.Lock()
		handled["fast"]++
		mu.Unlock()
		return nil
	}, opts)
	go fast.Start(ctx)

	time.Sleep(1500 * time.Millisecond) // past the slow handler and several claims
	if n := handledBy("fast"); n != 0 {
		t.Errorf("the other process handled %d batches while the claim was being renewed", n)
	}
	if n := handledBy("slow"); n != 1 {
		t.Errorf("the slow process handled the batch %d times", n)
	}
	if strings.Contains(logs.String(), "claim lost") {
		t.Errorf("a claim was lost:\n%s", logs.String())
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_cursors WHERE name = 'consumer:indexer' AND last_position = (SELECT max(position) FROM cb_messages) AND claimable_at = '-infinity'"); n != 1 {
		t.Errorf("the cursor is not past the batch and released")
	}
}

// A renewal that matches no row means the claim is not this process's any
// more: the handler is cancelled with ErrClaimLost as its cause, nothing is
// acked, and the batch is handled again under a new claim.
func TestConsumeCancelsTheHandlerWhenTheClaimIsLost(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := catbird.Publish(ctx, pool, "record.work", 1, ""); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var causes []error // one per handler call: why its context ended, nil when it did not wait
	started := make(chan struct{}, 1)
	var logs lockedBuffer
	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	rt.Consume("indexer", []string{"record.#"}, func(ctx context.Context, batch []catbird.Message) error {
		mu.Lock()
		call := len(causes)
		causes = append(causes, nil)
		mu.Unlock()
		if call > 0 {
			return nil
		}
		started <- struct{}{}
		<-ctx.Done()
		mu.Lock()
		causes[0] = context.Cause(ctx)
		mu.Unlock()
		return ctx.Err()
	}, catbird.ConsumeOptions{ClaimDuration: 300 * time.Millisecond, HandlerTimeout: 5 * time.Second, PollInterval: 20 * time.Millisecond})
	go rt.Start(ctx)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never started")
	}
	// Take the cursor away under the running handler, as a takeover would.
	if _, err := pool.Exec(ctx, "UPDATE cb_cursors SET claimable_at = '-infinity' WHERE name = 'consumer:indexer'"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the batch was not handled again", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(causes) >= 2
	})
	time.Sleep(200 * time.Millisecond) // let the second call's ack land

	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(causes[0], catbird.ErrClaimLost) {
		t.Errorf("the first handler was cancelled with %v, want ErrClaimLost", causes[0])
	}
	if !strings.Contains(logs.String(), "claim lost during the handler") {
		t.Errorf("the lost claim was not logged:\n%s", logs.String())
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_cursors WHERE name = 'consumer:indexer' AND last_position = (SELECT max(position) FROM cb_messages) AND claimable_at = '-infinity'"); n != 1 {
		t.Errorf("the cursor is not past the batch and released")
	}
}

// A handler that ignores its context and runs past HandlerTimeout is renewed
// no further, so its claim lapses: another process takes the cursor over and
// handles the same batch, and the slow process's ack then matches nothing and
// is refused, rather than releasing the claim of the process that holds the
// cursor now.
func TestConsumeTakesOverAnExpiredClaim(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := catbird.Publish(ctx, pool, "record.work", 1, ""); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	handled := map[string][]int64{} // positions handled, by process
	record := func(process string, batch []catbird.Message) {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range batch {
			handled[process] = append(handled[process], m.Position)
		}
	}
	handledBy := func(process string) int {
		mu.Lock()
		defer mu.Unlock()
		return len(handled[process])
	}

	var logs lockedBuffer
	// ClaimDuration alone: HandlerTimeout defaults inside it, so nothing renews.
	opts := catbird.ConsumeOptions{ClaimDuration: 200 * time.Millisecond, PollInterval: 50 * time.Millisecond}
	slow := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	slow.Consume("indexer", []string{"record.#"}, func(ctx context.Context, batch []catbird.Message) error {
		record("slow", batch)
		time.Sleep(600 * time.Millisecond) // past HandlerTimeout and ClaimDuration, ignoring ctx
		return nil
	}, opts)
	go slow.Start(ctx)
	waitFor(t, 5*time.Second, "the slow process never claimed the cursor", func() bool { return handledBy("slow") == 1 })

	fast := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	fast.Consume("indexer", []string{"record.#"}, func(ctx context.Context, batch []catbird.Message) error {
		record("fast", batch)
		return nil
	}, opts)
	go fast.Start(ctx)
	waitFor(t, 5*time.Second, "the other process did not take the cursor over", func() bool { return handledBy("fast") == 1 })
	waitFor(t, 5*time.Second, "the slow process's late ack was not refused", func() bool {
		return strings.Contains(logs.String(), "cursor claim lost before the ack")
	})

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(handled["slow"], handled["fast"]) {
		t.Errorf("the two processes handled different batches: %v and %v", handled["slow"], handled["fast"])
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_cursors WHERE name = 'consumer:indexer' AND last_position = $1 AND claimable_at = '-infinity'", handled["fast"][0]); n != 1 {
		t.Errorf("the cursor is not past the batch and released")
	}
}

// A pattern that does not compile, or no handler, is refused at registration.
func TestConsumeRefusesBadRegistration(t *testing.T) {
	handle := func(context.Context, []catbird.Message) error { return nil }
	for name, register := range map[string]func(*catbird.Runtime){
		"a pattern with a wildcard": func(rt *catbird.Runtime) {
			rt.Consume("bad", []string{"record.*"}, handle, catbird.ConsumeOptions{})
		},
		"no handler": func(rt *catbird.Runtime) {
			rt.Consume("bad", []string{"record.#"}, nil, catbird.ConsumeOptions{})
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: registered without a panic", name)
				}
			}()
			register(catbird.New(nil, catbird.Options{}))
		}()
	}
}

// lockedBuffer is a bytes.Buffer safe to write from the runtime's goroutines
// and read from the test's.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
