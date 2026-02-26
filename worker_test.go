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

type fixedBackoff struct {
	delay time.Duration
}

func (b fixedBackoff) Validate() error {
	if b.delay < 0 {
		return fmt.Errorf("backoff delay cannot be negative")
	}
	return nil
}

func (b fixedBackoff) NextDelay(_ int) time.Duration {
	return b.delay
}

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
	task := NewTask("test").Handler(func(_ context.Context, _ any) (any, error) { return nil, nil }, HandlerOpts{
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
	}, HandlerOpts{
		Concurrency: 1,
		BatchSize:   10,
		MaxRetries:  3,
		Backoff:     NewFullJitterBackoff(minDelay, maxDelay),
	})

	worker := client.NewWorker(t.Context()).AddTask(task)

	startTestWorker(t, worker)

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
		retryAttempt := i // attempts-1 corresponds to 0,1,...
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

func TestTaskOnFailIntegration(t *testing.T) {
	client := getTestClient(t)

	type Input struct {
		OrderID string `json:"order_id"`
	}

	onFailCalled := make(chan TaskFailure, 1)

	task := NewTask("task_on_fail").
		Handler(func(_ context.Context, _ Input) (string, error) {
			return "", fmt.Errorf("boom")
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  0,
		}).
		OnFail(func(_ context.Context, in Input, failure TaskFailure) error {
			if in.OrderID != "ord-1" {
				return fmt.Errorf("unexpected input in on-fail: %+v", in)
			}
			onFailCalled <- failure
			return nil
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  0,
		})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "task_on_fail", Input{OrderID: "ord-1"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var out string
	err = h.WaitForOutput(ctx, &out)
	if err == nil {
		t.Fatal("expected task to fail")
	}

	select {
	case f := <-onFailCalled:
		if f.TaskRunID <= 0 {
			t.Fatalf("expected TaskRunID > 0, got %d", f.TaskRunID)
		}
		if f.TaskName != "task_on_fail" {
			t.Fatalf("unexpected task name: %s", f.TaskName)
		}
		if f.ErrorMessage == "" {
			t.Fatal("expected error message in task failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for task on-fail handler")
	}
}

func TestTaskOnFailRetries(t *testing.T) {
	client := getTestClient(t)

	type Input struct {
		OrderID string `json:"order_id"`
	}

	onFailCalled := make(chan TaskFailure, 1)
	var attempts int32
	var mu sync.Mutex
	var seenAttempts []int

	task := NewTask("task_on_fail_retry").
		Handler(func(_ context.Context, _ Input) (string, error) {
			return "", fmt.Errorf("boom")
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  0,
		}).
		OnFail(func(_ context.Context, _ Input, failure TaskFailure) error {
			attempt := int(atomic.AddInt32(&attempts, 1))
			mu.Lock()
			seenAttempts = append(seenAttempts, failure.OnFailAttempts)
			mu.Unlock()
			if attempt == 1 {
				return fmt.Errorf("retry me")
			}
			onFailCalled <- failure
			return nil
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  1,
			Backoff:     fixedBackoff{delay: 10 * time.Millisecond},
		})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "task_on_fail_retry", Input{OrderID: "ord-retry"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var out string
	err = h.WaitForOutput(ctx, &out)
	if err == nil {
		t.Fatal("expected task to fail")
	}

	select {
	case <-onFailCalled:
		if atomic.LoadInt32(&attempts) != 2 {
			t.Fatalf("expected 2 on-fail attempts, got %d", attempts)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(seenAttempts) != 2 {
			t.Fatalf("expected 2 attempt values, got %d", len(seenAttempts))
		}
		if seenAttempts[0] != 1 || seenAttempts[1] != 2 {
			t.Fatalf("unexpected attempt sequence: %v", seenAttempts)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for task on-fail retry")
	}
}

func TestTaskOnFailMaxRetriesExhausted(t *testing.T) {
	client := getTestClient(t)

	type Input struct {
		OrderID string `json:"order_id"`
	}

	onFailCalled := make(chan TaskFailure, 2)
	var attempts int32

	task := NewTask("task_on_fail_exhausted").
		Handler(func(_ context.Context, _ Input) (string, error) {
			return "", fmt.Errorf("boom")
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  0,
		}).
		OnFail(func(_ context.Context, _ Input, _ TaskFailure) error {
			atomic.AddInt32(&attempts, 1)
			onFailCalled <- TaskFailure{}
			return fmt.Errorf("still failing")
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  0,
		})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "task_on_fail_exhausted", Input{OrderID: "ord-exhaust"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var out string
	err = h.WaitForOutput(ctx, &out)
	if err == nil {
		t.Fatal("expected task to fail")
	}

	select {
	case <-onFailCalled:
		// First on-fail call captured.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for task on-fail handler")
	}

	select {
	case <-onFailCalled:
		t.Fatal("unexpected on-fail retry after max retries exhausted")
	case <-time.After(300 * time.Millisecond):
	}

	if atomic.LoadInt32(&attempts) != 1 {
		t.Fatalf("expected 1 on-fail attempt, got %d", attempts)
	}
}

func TestFlowOnFailIntegration(t *testing.T) {
	client := getTestClient(t)

	type Input struct {
		OrderID string `json:"order_id"`
	}

	onFailCalled := make(chan FlowFailure, 1)

	flow := NewFlow("flow_on_fail").
		AddStep(NewStep("step1").
			Handler(func(_ context.Context, _ Input) (string, error) {
				return "", fmt.Errorf("step failed")
			}, HandlerOpts{
				Concurrency: 1,
				BatchSize:   10,
				MaxRetries:  0,
			})).
		OnFail(func(_ context.Context, in Input, failure FlowFailure) error {
			if in.OrderID != "ord-2" {
				return fmt.Errorf("unexpected flow input in on-fail: %+v", in)
			}
			onFailCalled <- failure
			return nil
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  0,
		})

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "flow_on_fail", Input{OrderID: "ord-2"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var out string
	err = h.WaitForOutput(ctx, &out)
	if err == nil {
		t.Fatal("expected flow to fail")
	}

	select {
	case f := <-onFailCalled:
		if f.FlowRunID <= 0 {
			t.Fatalf("expected FlowRunID > 0, got %d", f.FlowRunID)
		}
		if f.FlowName != "flow_on_fail" {
			t.Fatalf("unexpected flow name: %s", f.FlowName)
		}
		if f.FailedStepName != "step1" {
			t.Fatalf("unexpected failed step name: %s", f.FailedStepName)
		}
		if f.ErrorMessage == "" {
			t.Fatal("expected error message in flow failure")
		}
		var decoded Input
		if err := f.FailedStepInputAs(&decoded); err != nil {
			t.Fatalf("expected failed step input to decode: %v", err)
		}
		if decoded.OrderID != "ord-2" {
			t.Fatalf("unexpected failed step input: %+v", decoded)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flow on-fail handler")
	}
}

func TestFlowOnFailMapStepInputIntegration(t *testing.T) {
	client := getTestClient(t)

	type Item struct {
		ID int `json:"id"`
	}

	onFailCalled := make(chan FlowFailure, 1)

	flow := NewFlow("flow_on_fail_map_input").
		AddStep(NewStep("map_fail").
			MapInput().
			Handler(func(_ context.Context, in Item) (string, error) {
				return "", fmt.Errorf("map item failed: %d", in.ID)
			}, HandlerOpts{
				Concurrency: 1,
				BatchSize:   10,
				MaxRetries:  0,
			})).
		OnFail(func(_ context.Context, in []Item, failure FlowFailure) error {
			onFailCalled <- failure
			if len(in) != 2 {
				return fmt.Errorf("unexpected flow input size: %d", len(in))
			}
			return nil
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  0,
		})

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "flow_on_fail_map_input", []Item{{ID: 10}, {ID: 20}})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var out string
	err = h.WaitForOutput(ctx, &out)
	if err == nil {
		t.Fatal("expected flow to fail")
	}

	select {
	case f := <-onFailCalled:
		if f.FailedStepName != "map_fail" {
			t.Fatalf("unexpected failed step name: %s", f.FailedStepName)
		}
		var item Item
		if err := f.FailedStepInputAs(&item); err != nil {
			t.Fatalf("expected failed map item input to decode: %v", err)
		}
		if item.ID != 10 {
			t.Fatalf("unexpected failed map item input: %+v", item)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flow on-fail handler")
	}
}

func TestWorkerValidatesTaskHandlerOpts(t *testing.T) {
	client := getTestClient(t)

	// Test invalid concurrency
	t.Run("negative_concurrency", func(t *testing.T) {
		task := NewTask("invalid_task").Handler(func(_ context.Context, _ any) (any, error) {
			return nil, nil
		}, HandlerOpts{
			Concurrency: -1,
			BatchSize:   10,
		})

		worker := client.NewWorker(t.Context()).AddTask(task)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for negative concurrency")
		}
		if err.Error() != `task "invalid_task" has invalid handler options: concurrency must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Test invalid batch size
	t.Run("negative_batch_size", func(t *testing.T) {
		task := NewTask("invalid_task2").Handler(func(_ context.Context, _ any) (any, error) {
			return nil, nil
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   -1,
		})

		worker := client.NewWorker(t.Context()).AddTask(task)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for negative batch size")
		}
		if err.Error() != `task "invalid_task2" has invalid handler options: batch size must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Test invalid backoff config
	t.Run("invalid_backoff", func(t *testing.T) {
		task := NewTask("invalid_task3").Handler(func(_ context.Context, _ any) (any, error) {
			return nil, nil
		}, HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  3,
			Backoff:     NewFullJitterBackoff(time.Second, 500*time.Millisecond), // MaxDelay < MinDelay
		})

		worker := client.NewWorker(t.Context()).AddTask(task)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for invalid backoff")
		}
		if err.Error() != `task "invalid_task3" has invalid handler options: backoff maximum delay must be greater than minimum delay` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestWorkerValidatesFlowStepHandlerOpts(t *testing.T) {
	client := getTestClient(t)

	// Test invalid concurrency in flow step
	t.Run("negative_concurrency", func(t *testing.T) {
		flow := NewFlow("invalid_flow").
			AddStep(NewStep("step1").Handler(func(_ context.Context, _ any) (any, error) {
				return nil, nil
			}, HandlerOpts{
				Concurrency: -1,
				BatchSize:   10,
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for negative concurrency")
		}
		if err.Error() != `flow "invalid_flow" step "step1" has invalid handler options: concurrency must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Test invalid batch size in flow step
	t.Run("negative_batch_size", func(t *testing.T) {
		flow := NewFlow("invalid_flow2").
			AddStep(NewStep("step1").Handler(func(_ context.Context, _ any) (any, error) {
				return nil, nil
			}, HandlerOpts{
				Concurrency: 1,
				BatchSize:   -5, // Negative value
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for negative batch size")
		}
		if err.Error() != `flow "invalid_flow2" step "step1" has invalid handler options: batch size must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Test invalid circuit breaker config in flow step
	t.Run("invalid_circuit_breaker", func(t *testing.T) {
		flow := NewFlow("invalid_flow3").
			AddStep(NewStep("step1").Handler(func(_ context.Context, _ any) (any, error) {
				return nil, nil
			}, HandlerOpts{
				Concurrency:    1,
				BatchSize:      10,
				CircuitBreaker: &CircuitBreaker{failureThreshold: 0}, // Invalid threshold
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for invalid circuit breaker")
		}
		// Should contain validation error for circuit breaker
		if err.Error() != `flow "invalid_flow3" step "step1" has invalid handler options: circuit breaker failure threshold must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
