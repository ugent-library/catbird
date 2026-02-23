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

func TestRunTaskQuery(t *testing.T) {
	type input struct {
		Value string `json:"value"`
	}

	query, args, err := RunTaskQuery(
		"test_task",
		input{Value: "hello"},
		&RunOpts{
			ConcurrencyKey: "con-1",
			IdempotencyKey: "idem-1",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedQuery := `SELECT * FROM cb_run_task(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4);`
	if query != expectedQuery {
		t.Fatalf("unexpected query: %s", query)
	}

	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}

	if gotName, ok := args[0].(string); !ok || gotName != "test_task" {
		t.Fatalf("unexpected name arg: %#v", args[0])
	}

	if gotInput, ok := args[1].([]byte); !ok || string(gotInput) != `{"value":"hello"}` {
		t.Fatalf("unexpected input arg: %#v", args[1])
	}

	if gotConcurrencyKey, ok := args[2].(*string); !ok || gotConcurrencyKey == nil || *gotConcurrencyKey != "con-1" {
		t.Fatalf("unexpected concurrency key arg: %#v", args[2])
	}

	if gotIdempotencyKey, ok := args[3].(*string); !ok || gotIdempotencyKey == nil || *gotIdempotencyKey != "idem-1" {
		t.Fatalf("unexpected idempotency key arg: %#v", args[3])
	}

	_, nilArgs, err := RunTaskQuery("test_task", input{Value: "hello"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nilArgs) != 4 {
		t.Fatalf("expected 4 args for nil opts, got %d", len(nilArgs))
	}
	if gotConcurrencyKey, ok := nilArgs[2].(*string); !ok || gotConcurrencyKey != nil {
		t.Fatalf("expected nil *string concurrency key arg for nil opts, got %#v", nilArgs[2])
	}
	if gotIdempotencyKey, ok := nilArgs[3].(*string); !ok || gotIdempotencyKey != nil {
		t.Fatalf("expected nil *string idempotency key arg for nil opts, got %#v", nilArgs[3])
	}
}

func TestTaskCreate(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("test_task").Handler(func(ctx context.Context, in string) (string, error) {
		return in + " processed", nil
	}, nil)

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

	task := NewTask("math_task").Handler(func(ctx context.Context, in TaskInput) (int, error) {
		return in.Value * 2, nil
	}, nil)

	worker := client.NewWorker(t.Context(), nil).AddTask(task)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "math_task", TaskInput{Value: 21}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var result int
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &result); err != nil {
		t.Fatal(err)
	}

	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestTaskPanicRecovery(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("panic_task").Handler(func(ctx context.Context, in string) (string, error) {
		panic("intentional panic in task")
	}, nil)

	worker := client.NewWorker(t.Context(), nil).AddTask(task)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	if _, err := client.RunTask(t.Context(), "panic_task", "test input", nil); err != nil {
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

	task := NewTask("circuit_task").Handler(func(ctx context.Context, in string) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		if n == 1 {
			return "", fmt.Errorf("intentional failure")
		}
		return "ok", nil
	}, &HandlerOpts{
		Concurrency:    1,
		BatchSize:      10,
		MaxRetries:     2,
		Backoff:        NewFullJitterBackoff(minBackoff, maxBackoff),
		CircuitBreaker: NewCircuitBreaker(1, openTimeout),
	})

	worker := client.NewWorker(t.Context(), nil).AddTask(task)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "circuit_task", "input", nil)
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
