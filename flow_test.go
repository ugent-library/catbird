package catbird

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlowCreate(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow[string, map[string]any]("test_flow").
		AddStep(NewStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed", nil
		}, nil))

	err := client.CreateFlow(t.Context(), flow)
	if err != nil {
		t.Fatal(err)
	}

	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)

	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "test_flow", "input", nil)
	if err != nil {
		t.Fatal(err)
	}

	var out string
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	if out != "input processed" {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestFlowSingleStep(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow[string, map[string]any]("single_step_flow").
		AddStep(NewStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed by step 1", nil
		}, nil))

	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "single_step_flow", "input", nil)
	if err != nil {
		t.Fatal(err)
	}

	var out string
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	if out != "input processed by step 1" {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestFlowWithDependencies(t *testing.T) {
	client := getTestClient(t)

	// Flow structure: step1 -> step2 -> step3 (linear chain)
	// Final step is step3, which returns the full chain

	flow := NewFlow[string, map[string]any]("dependency_flow").
		AddStep(NewStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed by step 1", nil
		}, nil)).
		AddStep(NewStep1Dep("step2",
			"step1",
			func(ctx context.Context, in string, step1Out string) (string, error) {
				return step1Out + " and by step 2", nil
			}, nil)).
		AddStep(NewStep1Dep("step3",
			"step2",
			func(ctx context.Context, in string, step2Out string) (string, error) {
				return step2Out + " and by step 3", nil
			}, nil))

	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "dependency_flow", "input", nil)
	if err != nil {
		t.Fatal(err)
	}

	var out string
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	// Final step (step3) returns the full chain
	if out != "input processed by step 1 and by step 2 and by step 3" {
		t.Fatalf("unexpected flow output for step3: %s", out)
	}
}

func TestFlowListFlows(t *testing.T) {
	client := getTestClient(t)

	// Create multiple flows
	flows := make([]Flow, 2)

	flow1 := NewFlow[string, map[string]any]("list_flow_1").
		AddStep(NewStep("step1", func(ctx context.Context, in string) (string, error) {
			return in, nil
		}, nil))

	flow2 := NewFlow[string, map[string]any]("list_flow_2").
		AddStep(NewStep("step1", func(ctx context.Context, in string) (string, error) {
			return in, nil
		}, nil))

	flows[0] = flow1
	flows[1] = flow2

	err := client.CreateFlow(t.Context(), flow1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateFlow(t.Context(), flow2)
	if err != nil {
		t.Fatal(err)
	}

	// Start worker to execute flows
	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow1),
		WithFlow(flow2),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)

	time.Sleep(100 * time.Millisecond)

	// Verify both flows execute successfully
	h1, err := client.RunFlow(t.Context(), "list_flow_1", "input_1", nil)
	if err != nil {
		t.Fatal(err)
	}

	var out1 string
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &out1); err != nil {
		t.Fatal(err)
	}

	h2, err := client.RunFlow(t.Context(), "list_flow_2", "input_2", nil)
	if err != nil {
		t.Fatal(err)
	}

	var out2 string
	ctx2, cancel2 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel2()
	if err := h2.WaitForOutput(ctx2, &out2); err != nil {
		t.Fatal(err)
	}

	if out1 != "input_1" || out2 != "input_2" {
		t.Fatalf("unexpected flow outputs")
	}
}

func TestTaskListTasks(t *testing.T) {
	client := getTestClient(t)

	task1 := NewTask("list_task_1", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, nil)

	task2 := NewTask("list_task_2", func(ctx context.Context, in string) (string, error) {
		return in, nil
	}, nil)

	err := client.CreateTask(t.Context(), task1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateTask(t.Context(), task2)
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := client.ListTasks(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	var found1, found2 bool
	for _, task := range tasks {
		if task.Name == "list_task_1" {
			found1 = true
		}
		if task.Name == "list_task_2" {
			found2 = true
		}
	}

	if !found1 || !found2 {
		t.Fatalf("not all tasks found in list")
	}
}
func TestFlowComplexDependencies(t *testing.T) {
	client := getTestClient(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Flow structure:
	//        step1
	//       /     \
	//    step2    step3
	//       \     /
	//        step4
	// Create a complex flow with multiple dependencies:
	flow := NewFlow[string, map[string]any]("complex_flow").
		AddStep(NewStep("step1", func(ctx context.Context, in string) (int, error) {
			return 10, nil
		}, nil)).
		AddStep(NewStep1Dep("step2",
			"step1",
			func(ctx context.Context, in string, step1Out int) (int, error) {
				return step1Out * 2, nil // 20
			}, nil)).
		AddStep(NewStep1Dep("step3",
			"step1",
			func(ctx context.Context, in string, step1Out int) (int, error) {
				return step1Out * 3, nil // 30
			}, nil)).
		AddStep(NewStep2Deps("step4",
			"step2",
			"step3",
			func(ctx context.Context, in string, step2Out, step3Out int) (int, error) {
				return step2Out + step3Out, nil // 20 + 30 = 50
			}, nil))

	err := client.CreateFlow(t.Context(), flow)
	if err != nil {
		t.Fatal(err)
	}

	// Start worker to execute flow
	worker, err := client.NewWorker(t.Context(),
		WithLogger(logger),
		WithFlow(flow),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)

	time.Sleep(100 * time.Millisecond)

	// Run the flow
	h, err := client.RunFlow(t.Context(), "complex_flow", "input", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Get results - final step (step4) returns the aggregated result
	var out int
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	// Verify the final step (step4) computed correctly (step2 + step3 = 20 + 30 = 50)
	if out != 50 {
		t.Fatalf("expected step4=50, got %d", out)
	}
}

func TestFlowStepPanicRecovery(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow[string, map[string]any]("panic_flow").
		AddStep(NewStep("step1", func(ctx context.Context, in string) (string, error) {
			return "success", nil
		}, nil)).
		AddStep(NewStep1Dep("step2",
			"step1",
			func(ctx context.Context, in string, step1Out string) (string, error) {
				panic("intentional panic in flow step")
			}, nil))

	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	if _, err := client.RunFlow(t.Context(), "panic_flow", "test input", nil); err != nil {
		t.Fatal(err)
	}

	// Wait for flow to execute
	time.Sleep(1 * time.Second)

	// Get flow runs to verify step failure
	flowRuns, err := client.ListFlowRuns(t.Context(), "panic_flow")
	if err != nil {
		t.Fatal(err)
	}

	if len(flowRuns) == 0 {
		t.Fatal("expected at least one flow run")
	}

	// Flow should have failed due to step panic
	if flowRuns[0].Status != "failed" {
		t.Fatalf("expected flow status failed, got %s", flowRuns[0].Status)
	}
}

func TestStepCircuitBreaker(t *testing.T) {
	client := getTestClient(t)

	openTimeout := 300 * time.Millisecond
	minBackoff := 10 * time.Millisecond
	maxBackoff := 20 * time.Millisecond

	var calls int32
	var mu sync.Mutex
	var times []time.Time

	flow := NewFlow[string, map[string]any]("circuit_flow").
		AddStep(NewStep("step1",
			func(ctx context.Context, in string) (string, error) {
				n := atomic.AddInt32(&calls, 1)
				mu.Lock()
				times = append(times, time.Now())
				mu.Unlock()
				if n == 1 {
					return "", fmt.Errorf("intentional failure")
				}
				return "ok", nil
			}, &StepOpts{
				MaxRetries:     2,
				MinDelay:       minBackoff,
				MaxDelay:       maxBackoff,
				CircuitBreaker: &CircuitBreaker{failureThreshold: 1, openTimeout: openTimeout},
			}))

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "circuit_flow", "input", nil)
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

	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected 2 handler calls, got %d", calls)
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

func TestFlowWithSignal(t *testing.T) {
	client := getTestClient(t)

	type ApprovalInput struct {
		ApproverID string `json:"approver_id"`
		Approved   bool   `json:"approved"`
	}

	flowName := testFlowName(t, "signal_approval_flow")
	flow := NewFlow[string, map[string]any](flowName).
		AddStep(NewStep("submit", func(ctx context.Context, doc string) (string, error) {
			return "submitted: " + doc, nil
		}, nil)).
		AddStep(NewStepSignal1Dep("approve", "submit",
			func(ctx context.Context, doc string, approval ApprovalInput, submitResult string) (string, error) {
				if !approval.Approved {
					return "", fmt.Errorf("approval denied by %s", approval.ApproverID)
				}
				return fmt.Sprintf("approved by %s: %s", approval.ApproverID, submitResult), nil
			}, nil)).
		AddStep(NewStep1Dep("publish", "approve",
			func(ctx context.Context, doc string, approveResult string) (string, error) {
				return "published: " + approveResult, nil
			}, nil))

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "my_document", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Wait a bit for submit step to complete
	time.Sleep(200 * time.Millisecond)

	// Deliver approval signal
	err = client.SignalFlow(t.Context(), flowName, h.ID, "approve", ApprovalInput{
		ApproverID: "user123",
		Approved:   true,
	})
	if err != nil {
		t.Fatalf("signal delivery failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result string
	if err := h.WaitForOutput(ctx, &result); err != nil {
		t.Fatalf("wait for output failed: %v", err)
	}

	expectedResult := "published: approved by user123: submitted: my_document"
	if result != expectedResult {
		t.Errorf("unexpected publish result: %v, expected: %s", result, expectedResult)
	}
}

func TestFlowWithInitialSignal(t *testing.T) {
	t.Skip("TODO: Implement signal support in typed API")
}

func TestFlowSignalAlreadyDelivered(t *testing.T) {
	t.Skip("TODO: Implement signal support in typed API")
}
