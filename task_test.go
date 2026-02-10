package catbird

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskCreate(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("test_task", func(ctx context.Context, in string) (string, error) {
		return in + " processed", nil
	})

	err := client.CreateTask(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}

	info, err := client.GetTask(t.Context(), "test_task")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "test_task" {
		t.Fatalf("unexpected task name: %s", info.Name)
	}
}

func TestTaskRunAndWait(t *testing.T) {
	client := getTestClient(t)

	type TaskInput struct {
		Value int `json:"value"`
	}

	task := NewTask("math_task", func(ctx context.Context, in TaskInput) (int, error) {
		return in.Value * 2, nil
	})

	worker, err := client.NewWorker(t.Context(),
		WithTask(task),
	)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		err := worker.Start(t.Context())
		if err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "math_task", TaskInput{Value: 21})
	if err != nil {
		t.Fatal(err)
	}

	var result int
	if err := h.WaitForOutput(t.Context(), &result); err != nil {
		t.Fatal(err)
	}

	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestTaskPanicRecovery(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("panic_task", func(ctx context.Context, in string) (string, error) {
		panic("intentional panic in task")
	})

	worker, err := client.NewWorker(t.Context(),
		WithTask(task),
	)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		err := worker.Start(t.Context())
		if err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	if _, err := client.RunTask(t.Context(), "panic_task", "test input"); err != nil {
		t.Fatal(err)
	}

	// Wait for task to fail with panic
	time.Sleep(1 * time.Second)

	// Get task run to verify it was marked as failed
	taskRuns, err := client.ListTaskRuns(t.Context(), "panic_task")
	if err != nil {
		t.Fatal(err)
	}

	if len(taskRuns) == 0 {
		t.Fatal("expected at least one task run")
	}

	// Check that the task run shows failure due to panic
	if taskRuns[0].Status != "failed" {
		t.Fatalf("expected status failed, got %s", taskRuns[0].Status)
	}

	if taskRuns[0].ErrorMessage == "" || !strings.Contains(taskRuns[0].ErrorMessage, "panic") {
		t.Fatalf("expected panic error message, got %q", taskRuns[0].ErrorMessage)
	}
}

func TestTaskCircuitBreaker(t *testing.T) {
	client := getTestClient(t)

	openTimeout := 300 * time.Millisecond
	minBackoff := 10 * time.Millisecond
	maxBackoff := 20 * time.Millisecond

	var calls int32
	var mu sync.Mutex
	var times []time.Time

	task := NewTask("circuit_task", func(ctx context.Context, in string) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		if n == 1 {
			return "", fmt.Errorf("intentional failure")
		}
		return "ok", nil
	}, WithMaxRetries(2), WithBackoff(minBackoff, maxBackoff), WithCircuitBreaker(1, openTimeout))

	worker, err := client.NewWorker(t.Context(), WithTask(task))
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := worker.Start(t.Context()); err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "circuit_task", "input")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result string
	if err := h.WaitForOutput(ctx, &result); err != nil {
		t.Fatalf("wait for output failed: %v", err)
	}
	if result != "ok" {
		t.Fatalf("unexpected result: %s", result)
	}

	// Circuit breaker should limit handler invocations.
	// Due to a race condition where multiple workers may pick up the same task
	// before the circuit opens, we may see up to 3 calls (failure threshold + 2).
	callCount := atomic.LoadInt32(&calls)
	if callCount < 2 || callCount > 3 {
		t.Fatalf("expected 2-3 handler calls due to circuit breaker, got %d", callCount)
	}

	mu.Lock()
	if len(times) < 2 {
		mu.Unlock()
		t.Fatalf("expected at least 2 handler timestamps, got %d", len(times))
	}
	delta := times[1].Sub(times[0])
	mu.Unlock()

	if delta < openTimeout-50*time.Millisecond {
		t.Fatalf("expected circuit breaker delay at least %s, got %s", openTimeout-50*time.Millisecond, delta)
	}
}
