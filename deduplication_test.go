package catbird

import (
	"context"
	"fmt"
	"testing"
	"time"
)

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

// TestTaskConcurrencyKey verifies that ConcurrencyKey prevents concurrent runs
// but allows new runs after completion.
func TestTaskConcurrencyKey(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("concurrent_task", func(ctx context.Context, in int) (int, error) {
		time.Sleep(200 * time.Millisecond) // Simulate work
		return in * 2, nil
	}, nil)

	worker, err := client.NewWorker(t.Context(), WithTask(task))
	if err != nil {
		t.Fatal(err)
	}
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Run 1: Start first execution with ConcurrencyKey
	h1, err := client.RunTask(t.Context(), "concurrent_task", 10, &RunOpts{
		ConcurrencyKey: "run-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Give first run time to start
	time.Sleep(50 * time.Millisecond)

	// Run 2: Attempt duplicate while first is running (should be rejected)
	h2, err := client.RunTask(t.Context(), "concurrent_task", 20, &RunOpts{
		ConcurrencyKey: "run-123",
	})
	if err != nil {
		t.Fatal(err)
	}

	// h2 should have same ID as h1 (duplicate was rejected, existing row returned)
	if h2.ID != h1.ID {
		t.Fatalf("expected duplicate run to return existing ID %d, got %d", h1.ID, h2.ID)
	}

	// Wait for first run to complete
	var result1 int
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &result1); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if result1 != 20 {
		t.Fatalf("expected 20, got %d", result1)
	}

	// Run 3: Now that first run completed, same ConcurrencyKey should create new run
	h3, err := client.RunTask(t.Context(), "concurrent_task", 30, &RunOpts{
		ConcurrencyKey: "run-123",
	})
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

// TestTaskIdempotencyKey verifies that IdempotencyKey prevents both concurrent
// and completed runs, ensuring exactly-once semantics.
func TestTaskIdempotencyKey(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("idempotent_task", func(ctx context.Context, in int) (int, error) {
		time.Sleep(100 * time.Millisecond) // Simulate work
		return in * 3, nil
	}, nil)

	worker, err := client.NewWorker(t.Context(), WithTask(task))
	if err != nil {
		t.Fatal(err)
	}
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Run 1: First execution with IdempotencyKey
	h1, err := client.RunTask(t.Context(), "idempotent_task", 7, &RunOpts{
		IdempotencyKey: "payment-456",
	})
	if err != nil {
		t.Fatal(err)
	}
	if h1.ID == 0 {
		t.Fatal("expected first run to be created")
	}

	// Run 2: Duplicate while running (should return existing ID)
	h2, err := client.RunTask(t.Context(), "idempotent_task", 8, &RunOpts{
		IdempotencyKey: "payment-456",
	})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID != h1.ID {
		t.Fatalf("expected duplicate run to return existing ID %d, got %d", h1.ID, h2.ID)
	}

	// Wait for completion
	var result1 int
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &result1); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if result1 != 21 {
		t.Fatalf("expected 21, got %d", result1)
	}

	// Run 3: After completion, IdempotencyKey should still return existing ID (exactly-once)
	h3, err := client.RunTask(t.Context(), "idempotent_task", 9, &RunOpts{
		IdempotencyKey: "payment-456",
	})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID != h1.ID {
		t.Fatalf("expected duplicate run to return existing ID %d after completion, got %d", h1.ID, h3.ID)
	}

	// Run 4: Different IdempotencyKey should create new run
	h4, err := client.RunTask(t.Context(), "idempotent_task", 10, &RunOpts{
		IdempotencyKey: "payment-789",
	})
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

// TestTaskDeduplicationRetryOnFailure verifies that both ConcurrencyKey and
// IdempotencyKey allow retries when a task fails.
func TestTaskDeduplicationRetryOnFailure(t *testing.T) {
	client := getTestClient(t)

	type RetryInput struct {
		Fail bool `json:"fail"`
	}

	taskName := fmt.Sprintf("retry_task_dedupe_%d", time.Now().UnixNano())
	task := NewTask(taskName, func(ctx context.Context, in RetryInput) (string, error) {
		if in.Fail {
			return "", fmt.Errorf("task failed")
		}
		return "success", nil
	}, nil)

	worker, err := client.NewWorker(t.Context(), WithTask(task))
	if err != nil {
		t.Fatal(err)
	}
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Test ConcurrencyKey allows retry on failure
	h1, err := client.RunTask(t.Context(), taskName, RetryInput{Fail: true}, &RunOpts{
		ConcurrencyKey: "retry-test-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if h1.ID == 0 {
		t.Fatal("expected first run to be created")
	}

	// Wait for task to actually fail
	waitForTaskRunStatus(t, client, taskName, h1.ID, "failed", 5*time.Second)

	// Retry with same ConcurrencyKey (should work because first failed)
	h2, err := client.RunTask(t.Context(), taskName, RetryInput{Fail: false}, &RunOpts{
		ConcurrencyKey: "retry-test-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID == h1.ID {
		t.Fatal("expected retry to create new run after failure (different ID for ConcurrencyKey)")
	}

	// Test IdempotencyKey allows retry on failure
	h3, err := client.RunTask(t.Context(), taskName, RetryInput{Fail: true}, &RunOpts{
		IdempotencyKey: "idempotent-retry-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID == 0 {
		t.Fatal("expected first run to be created")
	}

	// Wait for task to actually fail
	waitForTaskRunStatus(t, client, taskName, h3.ID, "failed", 5*time.Second)

	// Retry with same IdempotencyKey (should work because first failed)
	h4, err := client.RunTask(t.Context(), taskName, RetryInput{Fail: false}, &RunOpts{
		IdempotencyKey: "idempotent-retry-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if h4.ID == h3.ID {
		t.Fatal("expected retry to create new run after failure (different ID for IdempotencyKey)")
	}
}

// TestFlowConcurrencyKey verifies ConcurrencyKey for flows.
func TestFlowConcurrencyKey(t *testing.T) {
	client := getTestClient(t)

	flowName := fmt.Sprintf("concurrent_flow_%d", time.Now().UnixNano())
	concurrencyKey := fmt.Sprintf("flow-run-%d", time.Now().UnixNano())
	input1 := "input"
	input2 := "input2"

	flow := NewFlow[string, map[string]any](flowName).
		AddStep(NewStep("step1", func(ctx context.Context, in string) (string, error) {
			time.Sleep(200 * time.Millisecond) // Simulate work
			return in + " processed", nil
		}, nil))

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
	if err != nil {
		t.Fatal(err)
	}
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Run 1: Start first execution
	h1, err := client.RunFlow(t.Context(), flowName, input1, &RunFlowOpts{
		ConcurrencyKey: concurrencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Run 2: Duplicate while running (should return existing ID)
	h2, err := client.RunFlow(t.Context(), flowName, input1, &RunFlowOpts{
		ConcurrencyKey: concurrencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID != h1.ID {
		t.Fatalf("expected duplicate flow run to return existing ID %d, got %d", h1.ID, h2.ID)
	}

	// Wait for completion
	var out1 string
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &out1); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if out1 != input1+" processed" {
		t.Fatalf("unexpected output: %s", out1)
	}

	// Run 3: After completion, same ConcurrencyKey should create new run
	h3, err := client.RunFlow(t.Context(), flowName, input2, &RunFlowOpts{
		ConcurrencyKey: concurrencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID == h1.ID {
		t.Fatal("expected new flow run to be created after completion (different ID)")
	}
}

// TestFlowIdempotencyKey verifies IdempotencyKey for flows.
func TestFlowIdempotencyKey(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow[int, int]("idempotent_flow").
		AddStep(NewStep("step1", func(ctx context.Context, in int) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return in * 5, nil
		}, nil))

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
	if err != nil {
		t.Fatal(err)
	}
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Use unique idempotency keys per test run to avoid conflicts with old data
	uniqueKey1 := fmt.Sprintf("order-999-%d", time.Now().UnixNano())
	uniqueKey2 := fmt.Sprintf("order-888-%d", time.Now().UnixNano())

	// Run 1: First execution
	h1, err := client.RunFlow(t.Context(), "idempotent_flow", 10, &RunFlowOpts{
		IdempotencyKey: uniqueKey1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if h1.ID == 0 {
		t.Fatal("expected first flow run to be created")
	}

	// Run 2: Duplicate (should return existing ID)
	h2, err := client.RunFlow(t.Context(), "idempotent_flow", 20, &RunFlowOpts{
		IdempotencyKey: uniqueKey1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if h2.ID != h1.ID {
		t.Fatalf("expected duplicate flow run to return existing ID %d, got %d", h1.ID, h2.ID)
	}

	// Wait for completion
	var out1 int
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &out1); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if out1 != 50 {
		t.Fatalf("expected 50, got %d", out1)
	}

	// Run 3: After completion, still return existing ID (exactly-once)
	h3, err := client.RunFlow(t.Context(), "idempotent_flow", 30, &RunFlowOpts{
		IdempotencyKey: uniqueKey1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if h3.ID != h1.ID {
		t.Fatalf("expected duplicate flow run to return existing ID %d after completion, got %d", h1.ID, h3.ID)
	}

	// Run 4: Different IdempotencyKey should create new run
	h4, err := client.RunFlow(t.Context(), "idempotent_flow", 40, &RunFlowOpts{
		IdempotencyKey: uniqueKey2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if h4.ID == h1.ID {
		t.Fatal("expected new flow run with different IdempotencyKey to have different ID")
	}
}

// TestTaskBothKeysRejected verifies that providing both ConcurrencyKey and
// IdempotencyKey is rejected.
func TestTaskBothKeysRejected(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("both_keys_task", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, nil)

	worker, err := client.NewWorker(t.Context(), WithTask(task))
	if err != nil {
		t.Fatal(err)
	}
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Attempt to run with both keys (should fail)
	_, err = client.RunTask(t.Context(), "both_keys_task", "input", &RunOpts{
		ConcurrencyKey: "key1",
		IdempotencyKey: "key2",
	})
	if err == nil {
		t.Fatal("expected error when both ConcurrencyKey and IdempotencyKey are provided")
	}
}

// TestFlowBothKeysRejected verifies that providing both keys for flows is rejected.
func TestFlowBothKeysRejected(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow[string, map[string]any]("both_keys_flow").
		AddStep(NewStep("step1", func(ctx context.Context, in string) (string, error) {
			return in, nil
		}, nil))

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
	if err != nil {
		t.Fatal(err)
	}
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Attempt to run with both keys (should fail)
	_, err = client.RunFlow(t.Context(), "both_keys_flow", "input", &RunFlowOpts{
		ConcurrencyKey: "key1",
		IdempotencyKey: "key2",
	})
	if err == nil {
		t.Fatal("expected error when both ConcurrencyKey and IdempotencyKey are provided")
	}
}
