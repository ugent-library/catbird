package catbird

import (
	"context"
	"errors"
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
		RunTaskOpts{
			ConcurrencyKey: "con-1",
			IdempotencyKey: "idem-1",
			VisibleAt:      time.Unix(1700000200, 0).UTC(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedQuery := `SELECT * FROM cb_run_task(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4, visible_at => $5);`
	if query != expectedQuery {
		t.Fatalf("unexpected query: %s", query)
	}

	if len(args) != 5 {
		t.Fatalf("expected 5 args, got %d", len(args))
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

	if gotVisibleAt, ok := args[4].(*time.Time); !ok || gotVisibleAt == nil || !gotVisibleAt.Equal(time.Unix(1700000200, 0).UTC()) {
		t.Fatalf("unexpected visible_at arg: %#v", args[4])
	}

	_, nilArgs, err := RunTaskQuery("test_task", input{Value: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nilArgs) != 5 {
		t.Fatalf("expected 5 args for nil opts, got %d", len(nilArgs))
	}
	if gotConcurrencyKey, ok := nilArgs[2].(*string); !ok || gotConcurrencyKey != nil {
		t.Fatalf("expected nil *string concurrency key arg for nil opts, got %#v", nilArgs[2])
	}
	if gotIdempotencyKey, ok := nilArgs[3].(*string); !ok || gotIdempotencyKey != nil {
		t.Fatalf("expected nil *string idempotency key arg for nil opts, got %#v", nilArgs[3])
	}
	if gotVisibleAt, ok := nilArgs[4].(*time.Time); !ok || gotVisibleAt != nil {
		t.Fatalf("expected nil *time.Time visible_at arg for nil opts, got %#v", nilArgs[4])
	}
}

func TestTaskCreate(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("test_task").Handler(func(ctx context.Context, in string) (string, error) {
		return in + " processed", nil
	}).Description("Task description")

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
	if info.Description != "Task description" {
		t.Fatalf("unexpected task description: %s", info.Description)
	}
}

func TestTaskRunAndWait(t *testing.T) {
	client := getTestClient(t)

	type TaskInput struct {
		Value int `json:"value"`
	}

	task := NewTask("math_task").Handler(func(ctx context.Context, in TaskInput) (int, error) {
		return in.Value * 2, nil
	})

	worker := client.NewWorker(t.Context()).AddTask(task)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "math_task", TaskInput{Value: 21})
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

func TestTaskRunDelayedVisibleAt(t *testing.T) {
	client := getTestClient(t)

	taskName := fmt.Sprintf("delayed_task_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in string) (string, error) {
		return in + " processed", nil
	})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)

	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), taskName, "input", RunTaskOpts{
		VisibleAt: time.Now().Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)
	runInfo, err := client.GetTaskRun(t.Context(), taskName, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if runInfo.Status != "queued" {
		t.Fatalf("expected delayed task to remain queued before visible_at, got %s", runInfo.Status)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	var out string
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}
	if out != "input processed" {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestTaskPanicRecovery(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("panic_task").Handler(func(ctx context.Context, in string) (string, error) {
		panic("intentional panic in task")
	})

	worker := client.NewWorker(t.Context()).AddTask(task)

	startTestWorker(t, worker)

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

	task := NewTask("circuit_task").Handler(func(ctx context.Context, in string) (string, error) {
		n := atomic.AddInt32(&calls, 1)
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		if n == 1 {
			return "", fmt.Errorf("intentional failure")
		}
		return "ok", nil
	}, HandlerOpts{
		Concurrency:    1,
		BatchSize:      10,
		MaxRetries:     2,
		Backoff:        NewFullJitterBackoff(minBackoff, maxBackoff),
		CircuitBreaker: NewCircuitBreaker(1, openTimeout),
	})

	worker := client.NewWorker(t.Context()).AddTask(task)

	startTestWorker(t, worker)

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

func waitForTaskRunStatus(t *testing.T, client *Client, taskName string, runID int64, status string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		run, err := client.GetTaskRun(t.Context(), taskName, runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == status {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected task run to reach status %s, last status: %s", status, run.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestTaskConcurrencyKey(t *testing.T) {
	client := getTestClient(t)
	taskName := fmt.Sprintf("concurrent_task_%d", time.Now().UnixNano())

	task := NewTask(taskName).Handler(func(ctx context.Context, in int) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return in * 2, nil
	})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h1, err := client.RunTask(t.Context(), taskName, 10, RunTaskOpts{ConcurrencyKey: "run-123"})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	h2, err := client.RunTask(t.Context(), taskName, 20, RunTaskOpts{ConcurrencyKey: "run-123"})
	if err != nil {
		t.Fatal(err)
	}

	if h2.ID != h1.ID {
		t.Fatalf("expected duplicate run to return existing ID %d, got %d", h1.ID, h2.ID)
	}

	var result1 int
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &result1); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if result1 != 20 {
		t.Fatalf("expected 20, got %d", result1)
	}

	h3, err := client.RunTask(t.Context(), taskName, 30, RunTaskOpts{ConcurrencyKey: "run-123"})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID == h1.ID {
		t.Fatal("expected new run to be created after completion (different ID)")
	}

	var result3 int
	ctx3, cancel3 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel3()
	if err := h3.WaitForOutput(ctx3, &result3); err != nil {
		t.Fatalf("third run failed: %v", err)
	}
	if result3 != 60 {
		t.Fatalf("expected 60, got %d", result3)
	}
}

func TestTaskIdempotencyKey(t *testing.T) {
	client := getTestClient(t)
	taskName := fmt.Sprintf("idempotent_task_%d", time.Now().UnixNano())

	task := NewTask(taskName).Handler(func(ctx context.Context, in int) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return in * 3, nil
	})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h1, err := client.RunTask(t.Context(), taskName, 7, RunTaskOpts{IdempotencyKey: "payment-456"})
	if err != nil {
		t.Fatal(err)
	}
	if h1.ID == 0 {
		t.Fatal("expected first run to be created")
	}

	h2, err := client.RunTask(t.Context(), taskName, 8, RunTaskOpts{IdempotencyKey: "payment-456"})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID != h1.ID {
		t.Fatalf("expected duplicate run to return existing ID %d, got %d", h1.ID, h2.ID)
	}

	var result1 int
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &result1); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if result1 != 21 {
		t.Fatalf("expected 21, got %d", result1)
	}

	h3, err := client.RunTask(t.Context(), taskName, 9, RunTaskOpts{IdempotencyKey: "payment-456"})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID != h1.ID {
		t.Fatalf("expected duplicate run to return existing ID %d after completion, got %d", h1.ID, h3.ID)
	}

	h4, err := client.RunTask(t.Context(), taskName, 10, RunTaskOpts{IdempotencyKey: "payment-789"})
	if err != nil {
		t.Fatal(err)
	}
	if h4.ID == h1.ID {
		t.Fatal("expected new run with different IdempotencyKey to have different ID")
	}

	var result4 int
	ctx4, cancel4 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel4()
	if err := h4.WaitForOutput(ctx4, &result4); err != nil {
		t.Fatalf("fourth run failed: %v", err)
	}
	if result4 != 30 {
		t.Fatalf("expected 30, got %d", result4)
	}
}

func TestTaskDeduplicationRetryOnFailure(t *testing.T) {
	client := getTestClient(t)

	type RetryInput struct {
		Fail bool `json:"fail"`
	}

	taskName := fmt.Sprintf("retry_task_dedupe_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in RetryInput) (string, error) {
		if in.Fail {
			return "", fmt.Errorf("task failed")
		}
		return "success", nil
	})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h1, err := client.RunTask(t.Context(), taskName, RetryInput{Fail: true}, RunTaskOpts{ConcurrencyKey: "retry-test-1"})
	if err != nil {
		t.Fatal(err)
	}
	if h1.ID == 0 {
		t.Fatal("expected first run to be created")
	}

	waitForTaskRunStatus(t, client, taskName, h1.ID, "failed", 5*time.Second)

	h2, err := client.RunTask(t.Context(), taskName, RetryInput{Fail: false}, RunTaskOpts{ConcurrencyKey: "retry-test-1"})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID == h1.ID {
		t.Fatal("expected retry to create new run after failure (different ID for ConcurrencyKey)")
	}

	h3, err := client.RunTask(t.Context(), taskName, RetryInput{Fail: true}, RunTaskOpts{IdempotencyKey: "idempotent-retry-1"})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID == 0 {
		t.Fatal("expected first run to be created")
	}

	waitForTaskRunStatus(t, client, taskName, h3.ID, "failed", 5*time.Second)

	h4, err := client.RunTask(t.Context(), taskName, RetryInput{Fail: false}, RunTaskOpts{IdempotencyKey: "idempotent-retry-1"})
	if err != nil {
		t.Fatal(err)
	}
	if h4.ID == h3.ID {
		t.Fatal("expected retry to create new run after failure (different ID for IdempotencyKey)")
	}
}

func TestFlowConcurrencyKey(t *testing.T) {
	client := getTestClient(t)

	flowName := fmt.Sprintf("concurrent_flow_%d", time.Now().UnixNano())
	concurrencyKey := fmt.Sprintf("flow-run-%d", time.Now().UnixNano())
	input1 := "input"
	input2 := "input2"

	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			time.Sleep(200 * time.Millisecond)
			return in + " processed", nil
		}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h1, err := client.RunFlow(t.Context(), flowName, input1, RunFlowOpts{ConcurrencyKey: concurrencyKey})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	h2, err := client.RunFlow(t.Context(), flowName, input1, RunFlowOpts{ConcurrencyKey: concurrencyKey})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID != h1.ID {
		t.Fatalf("expected duplicate flow run to return existing ID %d, got %d", h1.ID, h2.ID)
	}

	var out1 string
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &out1); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if out1 != input1+" processed" {
		t.Fatalf("unexpected output: %s", out1)
	}

	h3, err := client.RunFlow(t.Context(), flowName, input2, RunFlowOpts{ConcurrencyKey: concurrencyKey})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID == h1.ID {
		t.Fatal("expected new flow run to be created after completion (different ID)")
	}
}

func TestFlowIdempotencyKey(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow("idempotent_flow").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in int) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return in * 5, nil
		}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	uniqueKey1 := fmt.Sprintf("order-999-%d", time.Now().UnixNano())
	uniqueKey2 := fmt.Sprintf("order-888-%d", time.Now().UnixNano())

	h1, err := client.RunFlow(t.Context(), "idempotent_flow", 10, RunFlowOpts{IdempotencyKey: uniqueKey1})
	if err != nil {
		t.Fatal(err)
	}
	if h1.ID == 0 {
		t.Fatal("expected first flow run to be created")
	}

	h2, err := client.RunFlow(t.Context(), "idempotent_flow", 20, RunFlowOpts{IdempotencyKey: uniqueKey1})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID != h1.ID {
		t.Fatalf("expected duplicate flow run to return existing ID %d, got %d", h1.ID, h2.ID)
	}

	var out1 int
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &out1); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if out1 != 50 {
		t.Fatalf("expected 50, got %d", out1)
	}

	h3, err := client.RunFlow(t.Context(), "idempotent_flow", 30, RunFlowOpts{IdempotencyKey: uniqueKey1})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID != h1.ID {
		t.Fatalf("expected duplicate flow run to return existing ID %d after completion, got %d", h1.ID, h3.ID)
	}

	h4, err := client.RunFlow(t.Context(), "idempotent_flow", 40, RunFlowOpts{IdempotencyKey: uniqueKey2})
	if err != nil {
		t.Fatal(err)
	}
	if h4.ID == h1.ID {
		t.Fatal("expected new flow run with different IdempotencyKey to have different ID")
	}
}

func TestTaskBothKeysRejected(t *testing.T) {
	client := getTestClient(t)
	var err error
	taskName := fmt.Sprintf("both_keys_task_%d", time.Now().UnixNano())

	task := NewTask(taskName).Handler(func(ctx context.Context, in string) (string, error) {
		return in, nil
	})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	_, err = client.RunTask(t.Context(), taskName, "input", RunTaskOpts{
		ConcurrencyKey: "key1",
		IdempotencyKey: "key2",
	})
	if err == nil {
		t.Fatal("expected error when both ConcurrencyKey and IdempotencyKey are provided")
	}
}

func TestFlowBothKeysRejected(t *testing.T) {
	client := getTestClient(t)
	var err error

	flow := NewFlow("both_keys_flow").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return in, nil
		}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	_, err = client.RunFlow(t.Context(), "both_keys_flow", "input", RunFlowOpts{
		ConcurrencyKey: "key1",
		IdempotencyKey: "key2",
	})
	if err == nil {
		t.Fatal("expected error when both ConcurrencyKey and IdempotencyKey are provided")
	}
}

func TestTaskCancelQueuedRun(t *testing.T) {
	client := getTestClient(t)

	taskName := fmt.Sprintf("cancel_queued_task_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in string) (string, error) {
		return in + " done", nil
	})

	if err := client.CreateTask(t.Context(), task); err != nil {
		t.Fatal(err)
	}

	h, err := client.RunTask(t.Context(), taskName, "input")
	if err != nil {
		t.Fatal(err)
	}

	if err := client.CancelTaskRun(t.Context(), taskName, h.ID, CancelOpts{Reason: "test cancel queued"}); err != nil {
		t.Fatal(err)
	}

	run, err := client.GetTaskRun(t.Context(), taskName, h.ID)
	if err != nil {
		t.Fatal(err)
	}

	if run.Status != "canceled" {
		t.Fatalf("expected canceled, got %s", run.Status)
	}
	if run.CancelReason == "" {
		t.Fatal("expected cancel reason to be populated")
	}
}

func TestTaskCancelStartedRun(t *testing.T) {
	client := getTestClient(t)

	taskName := fmt.Sprintf("cancel_started_task_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), taskName, "input")
	if err != nil {
		t.Fatal(err)
	}

	waitForTaskRunStatus(t, client, taskName, h.ID, "started", 5*time.Second)

	if err := client.CancelTaskRun(t.Context(), taskName, h.ID, CancelOpts{Reason: "test cancel started"}); err != nil {
		t.Fatal(err)
	}

	waitForTaskRunStatus(t, client, taskName, h.ID, "canceled", 5*time.Second)
}

func TestTaskInternalCancelCurrentRun(t *testing.T) {
	client := getTestClient(t)

	taskName := fmt.Sprintf("cancel_internal_task_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in string) (string, error) {
		if err := Cancel(ctx, CancelOpts{Reason: "business early exit"}); err != nil {
			return "", err
		}
		<-ctx.Done()
		return "", ctx.Err()
	})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), taskName, "input")
	if err != nil {
		t.Fatal(err)
	}

	var out string
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err = h.WaitForOutput(ctx, &out)
	if !errors.Is(err, ErrRunCanceled) {
		t.Fatalf("expected ErrRunCanceled, got %v", err)
	}

	run, err := client.GetTaskRun(t.Context(), taskName, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "canceled" {
		t.Fatalf("expected canceled, got %s", run.Status)
	}
}

func TestTaskHandleWaitForOutput(t *testing.T) {
	client := getTestClient(t)

	taskName := fmt.Sprintf("task_handle_finished_%d", time.Now().UnixNano())
	task := NewTask(taskName).Handler(func(ctx context.Context, in string) (string, error) {
		time.Sleep(150 * time.Millisecond)
		return in + ":ok", nil
	})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), taskName, "input")
	if err != nil {
		t.Fatal(err)
	}

	var out string
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}
	if out != "input:ok" {
		t.Fatalf("unexpected output: %s", out)
	}

	run, err := client.GetTaskRun(t.Context(), taskName, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if !run.IsDone() {
		t.Fatalf("expected task run IsDone() to be true in terminal state")
	}
	if !run.IsCompleted() {
		t.Fatalf("expected task run IsCompleted() to be true in completed state")
	}
}

func TestTaskRunInfoOutputAsSkipped(t *testing.T) {
	run := &TaskRunInfo{Status: "skipped"}

	var out string
	err := run.OutputAs(&out)
	if err == nil {
		t.Fatal("expected error for skipped task run")
	}
	if !strings.Contains(err.Error(), "run skipped") {
		t.Fatalf("expected skipped error message, got %v", err)
	}
}
