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

func TestHandlerOpts_WithRetriesAndBackoff(t *testing.T) {
	min := 100 * time.Millisecond
	max := 2 * time.Second

	task := NewTask("test", func(_ context.Context, _ any) (any, error) { return nil, nil }, WithMaxRetries(3), WithBackoff(min, max))

	if task.handler.maxRetries != 3 {
		t.Fatalf("expected maxRetries=3, got %d", task.handler.maxRetries)
	}
	if task.handler.minDelay != min {
		t.Fatalf("expected minDelay=%v, got %v", min, task.handler.minDelay)
	}
	if task.handler.maxDelay != max {
		t.Fatalf("expected maxDelay=%v, got %v", max, task.handler.maxDelay)
	}
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
// of times (retries + final success) when configured with WithMaxRetries
// and WithBackoff.
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

	task := NewTask("retry_task", func(ctx context.Context, in string) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		if n <= failTimes {
			return "", fmt.Errorf("intentional failure %d", n)
		}
		return fmt.Sprintf("success at %d", n), nil
	}, WithMaxRetries(3), WithBackoff(minDelay, maxDelay))

	worker, err := client.NewWorker(t.Context(), WithTask(task))
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := worker.Start(t.Context()); err != nil {
			t.Logf("worker error: %v", err)
		}
	}()

	// give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "retry_task", "input")
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
