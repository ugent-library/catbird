package catbird

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerShutdownImmediateWhenNoTimeout(t *testing.T) {
	t.Helper()

	w := &Worker{
		shutdownTimeout: 0,
	}

	var wg sync.WaitGroup

	// Simulate a handler goroutine that runs until handlerCtx is cancelled.
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	wg.Add(1)

	started := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer wg.Done()
		close(started)
		<-handlerCtx.Done()
		close(stopped)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.waitForShutdown(ctx, handlerCancel, &wg)
	}()

	<-started

	// Trigger shutdown and ensure we don't wait for a grace period when
	// gracefulShutdown is zero.
	start := time.Now()
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("waitForShutdown returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitForShutdown did not return promptly with zero gracefulShutdown")
	}

	if time.Since(start) > 400*time.Millisecond {
		t.Fatalf("waitForShutdown took too long with zero gracefulShutdown: %s", time.Since(start))
	}

	select {
	case <-stopped:
	default:
		t.Fatal("handler was not cancelled immediately when gracefulShutdown is zero")
	}
}

func TestWorkerShutdownWithGracePeriod(t *testing.T) {
	t.Helper()

	grace := 200 * time.Millisecond
	w := &Worker{
		shutdownTimeout: grace,
	}

	var wg sync.WaitGroup

	// Simulate a handler goroutine that only stops when handlerCtx is
	// cancelled. This lets us observe the grace period behaviour.
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	wg.Add(1)

	started := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer wg.Done()
		close(started)
		<-handlerCtx.Done()
		close(stopped)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.waitForShutdown(ctx, handlerCancel, &wg)
	}()

	<-started

	// Trigger shutdown and measure how long waitForShutdown takes to return.
	cancel()
	start := time.Now()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("waitForShutdown returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForShutdown did not return within expected time")
	}

	elapsed := time.Since(start)
	if elapsed < grace {
		t.Fatalf("waitForShutdown returned before grace period elapsed: got %s, want at least %s", elapsed, grace)
	}

	select {
	case <-stopped:
	default:
		t.Fatal("handler was not cancelled after grace period elapsed")
	}
}

func TestHandlerOpts_WithRetriesAndBackoff(t *testing.T) {
	min := 100 * time.Millisecond
	max := 2 * time.Second

	// Just verify that task can be created with these options
	// Internal fields are not exposed via interface
	task := NewTask("test").Handler(func(_ context.Context, _ any) (any, error) { return nil, nil }, &HandlerOpts{
		Concurrency: 1,
		BatchSize:   10,
		MaxRetries:  3,
		Backoff:     NewFullJitterBackoff(min, max),
	})

	if task == nil {
		t.Fatal("expected task to be created")
	}

	// Options are validated during New().Handler() - if we got here, they were accepted
}

func TestBackoffWithFullJitter(t *testing.T) {
	// make randomness deterministic for bounds checking
	rand.Seed(42)

	min := 100 * time.Millisecond
	max := 1 * time.Second

	// attempt 0: delay = min
	d0 := backoffWithFullJitter(0, min, max)
	if d0 < 0 || d0 >= min {
		t.Fatalf("attempt 0: expected 0 <= d < %v, got %v", min, d0)
	}

	// attempt 1: delay = min*2
	d1 := backoffWithFullJitter(1, min, max)
	if d1 < 0 || d1 >= min*2 {
		t.Fatalf("attempt 1: expected 0 <= d < %v, got %v", min*2, d1)
	}

	// attempt where exponential exceeds max -> capped to max
	// choose retryAttempt large enough
	dLarge := backoffWithFullJitter(10, min, max)
	if dLarge < 0 || dLarge >= max {
		t.Fatalf("large attempt: expected 0 <= d < %v, got %v", max, dLarge)
	}
}

// Integration test: run a worker with a task that fails a couple of times
// and then succeeds. Verify the handler was invoked the expected number
// of times (retries + final success) when configured with MaxRetries
// and Backoff.
func TestTaskRetriesIntegration(t *testing.T) {
	client := getTestClient(t)

	var calls int32
	failTimes := int32(2) // fail first 2 attempts, succeed on 3rd

	minDelay := 200 * time.Millisecond
	maxDelay := 2 * time.Second

	var mu sync.Mutex
	var times []time.Time

	// seed global RNG for deterministic jitter
	const seed int64 = 42
	rand.Seed(seed)

	task := NewTask("retry_task").Handler(func(ctx context.Context, in string) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		if n <= failTimes {
			return "", fmt.Errorf("intentional failure %d", n)
		}
		return fmt.Sprintf("success at %d", n), nil
	}, &HandlerOpts{
		Concurrency: 1,
		BatchSize:   10,
		MaxRetries:  3,
		Backoff:     NewFullJitterBackoff(minDelay, maxDelay),
	})

	worker := client.NewWorker(t.Context(), nil).AddTask(task, nil)

	startTestWorker(t, worker)

	// give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "retry_task", "input", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	var out string
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatalf("wait for output failed: %v", err)
	}

	expected := failTimes + 1
	if atomic.LoadInt32(&calls) != expected {
		t.Fatalf("expected %d handler calls, got %d", expected, calls)
	}

	// compute expected jitter values using a local RNG seeded the same way
	localRand := rand.New(rand.NewSource(seed))

	mu.Lock()
	if len(times) != int(expected) {
		mu.Unlock()
		t.Fatalf("expected %d timestamps, got %d", expected, len(times))
	}
	observed := make([]time.Duration, 0, len(times)-1)
	for i := 0; i < len(times)-1; i++ {
		observed = append(observed, times[i+1].Sub(times[i]))
	}
	mu.Unlock()

	// slack to account for processing time
	slack := 150 * time.Millisecond

	for i := 0; i < int(failTimes); i++ {
		retryAttempt := i // deliveries-1 corresponds to 0,1,...
		delay := time.Duration(float64(minDelay) * math.Pow(2, float64(retryAttempt)))
		if delay > maxDelay {
			delay = maxDelay
		}
		// expected jitter value from same RNG
		jitter := time.Duration(localRand.Int63n(int64(delay)))

		obs := observed[i]
		if obs+slack < jitter {
			t.Fatalf("attempt %d: observed delay %v is less than expected jitter %v (with slack %v)", i, obs, jitter, slack)
		}
	}
}
