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

	flow := NewFlow("test_flow",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed", nil
		}),
	)

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

	go func() {
		err := worker.Start(t.Context())
		if err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	type FlowOutput struct {
		Step1 string `json:"step1"`
	}

	h, err := client.RunFlow(t.Context(), "test_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	var out FlowOutput
	if err := h.WaitForOutput(t.Context(), &out); err != nil {
		t.Fatal(err)
	}

	if out.Step1 != "input processed" {
		t.Fatalf("unexpected output: %s", out.Step1)
	}
}

func TestFlowSingleStep(t *testing.T) {
	client := getTestClient(t)

	type FlowOutput struct {
		Step1 string `json:"step1"`
	}

	flow := NewFlow("single_step_flow",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed by step 1", nil
		}),
	)

	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow),
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

	h, err := client.RunFlow(t.Context(), "single_step_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	var out FlowOutput
	if err := h.WaitForOutput(t.Context(), &out); err != nil {
		t.Fatal(err)
	}

	if out.Step1 != "input processed by step 1" {
		t.Fatalf("unexpected output: %s", out.Step1)
	}
}

func TestFlowWithDependencies(t *testing.T) {
	client := getTestClient(t)

	// Flow structure: step1 -> step2 -> step3 (linear chain)
	type FlowOutput struct {
		Step1 string `json:"step1"`
		Step2 string `json:"step2"`
		Step3 string `json:"step3"`
	}

	flow := NewFlow("dependency_flow",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed by step 1", nil
		}),
		StepWithOneDependency("step2",
			Dependency("step1"),
			func(ctx context.Context, in string, step1Out string) (string, error) {
				return step1Out + " and by step 2", nil
			}),
		StepWithOneDependency("step3",
			Dependency("step2"),
			func(ctx context.Context, in string, step2Out string) (string, error) {
				return step2Out + " and by step 3", nil
			}),
	)

	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow),
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

	h, err := client.RunFlow(t.Context(), "dependency_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	var out FlowOutput
	if err := h.WaitForOutput(t.Context(), &out); err != nil {
		t.Fatal(err)
	}

	if out.Step1 != "input processed by step 1" {
		t.Fatalf("unexpected flow output for step1: %s", out.Step1)
	}
	if out.Step2 != "input processed by step 1 and by step 2" {
		t.Fatalf("unexpected flow output for step2: %s", out.Step2)
	}
	if out.Step3 != "input processed by step 1 and by step 2 and by step 3" {
		t.Fatalf("unexpected flow output for step3: %s", out.Step3)
	}
}

func TestFlowListFlows(t *testing.T) {
	client := getTestClient(t)

	// Create multiple flows
	flows := make([]*Flow, 2)

	flow1 := NewFlow("list_flow_1",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in, nil
		}),
	)

	flow2 := NewFlow("list_flow_2",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in, nil
		}),
	)

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

	go func() {
		if err := worker.Start(t.Context()); err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Verify both flows execute successfully
	type FlowOutput struct {
		Step1 string `json:"step1"`
	}

	h1, err := client.RunFlow(t.Context(), "list_flow_1", "input_1")
	if err != nil {
		t.Fatal(err)
	}

	var out1 FlowOutput
	if err := h1.WaitForOutput(t.Context(), &out1); err != nil {
		t.Fatal(err)
	}

	h2, err := client.RunFlow(t.Context(), "list_flow_2", "input_2")
	if err != nil {
		t.Fatal(err)
	}

	var out2 FlowOutput
	if err := h2.WaitForOutput(t.Context(), &out2); err != nil {
		t.Fatal(err)
	}

	if out1.Step1 != "input_1" || out2.Step1 != "input_2" {
		t.Fatalf("unexpected flow outputs")
	}
}

func TestTaskListTasks(t *testing.T) {
	client := getTestClient(t)

	task1 := NewTask("list_task_1", func(ctx context.Context, in string) (string, error) {
		return in, nil
	})

	task2 := NewTask("list_task_2", func(ctx context.Context, in string) (string, error) {
		return in, nil
	})

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

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Flow structure:
	//        step1
	//       /     \
	//    step2    step3
	//       \     /
	//        step4
	// Create a complex flow with multiple dependencies:
	flow := NewFlow("complex_flow",
		InitialStep("step1", func(ctx context.Context, in string) (int, error) {
			return 10, nil
		}),
		StepWithOneDependency("step2",
			Dependency("step1"),
			func(ctx context.Context, in string, step1Out int) (int, error) {
				return step1Out * 2, nil // 20
			}),
		StepWithOneDependency("step3",
			Dependency("step1"),
			func(ctx context.Context, in string, step1Out int) (int, error) {
				return step1Out * 3, nil // 30
			}),
		StepWithTwoDependencies("step4",
			Dependency("step2"),
			Dependency("step3"),
			func(ctx context.Context, in string, step2Out, step3Out int) (int, error) {
				return step2Out + step3Out, nil // 20 + 30 = 50
			}),
	)

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

	go func() {
		if err := worker.Start(t.Context()); err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Run the flow
	h, err := client.RunFlow(t.Context(), "complex_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	// Get results
	type ComplexOutput struct {
		Step1 int `json:"step1"`
		Step2 int `json:"step2"`
		Step3 int `json:"step3"`
		Step4 int `json:"step4"`
	}

	var out ComplexOutput
	if err := h.WaitForOutput(t.Context(), &out); err != nil {
		t.Fatal(err)
	}

	// Verify the computation graph was executed correctly
	if out.Step1 != 10 {
		t.Fatalf("expected step1=10, got %d", out.Step1)
	}
	if out.Step2 != 20 {
		t.Fatalf("expected step2=20, got %d", out.Step2)
	}
	if out.Step3 != 30 {
		t.Fatalf("expected step3=30, got %d", out.Step3)
	}
	if out.Step4 != 50 {
		t.Fatalf("expected step4=50, got %d", out.Step4)
	}
}

func TestFlowStepPanicRecovery(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow("panic_flow",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return "success", nil
		}),
		StepWithOneDependency("step2",
			Dependency("step1"),
			func(ctx context.Context, in string, step1Out string) (string, error) {
				panic("intentional panic in flow step")
			}),
	)

	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow),
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

	if _, err := client.RunFlow(t.Context(), "panic_flow", "test input"); err != nil {
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

	flow := NewFlow("circuit_flow",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			n := atomic.AddInt32(&calls, 1)
			mu.Lock()
			times = append(times, time.Now())
			mu.Unlock()
			if n == 1 {
				return "", fmt.Errorf("intentional failure")
			}
			return "ok", nil
		}, WithMaxRetries(2), WithBackoff(minBackoff, maxBackoff), WithCircuitBreaker(1, openTimeout)),
	)

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
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

	h, err := client.RunFlow(t.Context(), "circuit_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result map[string]string
	if err := h.WaitForOutput(ctx, &result); err != nil {
		t.Fatalf("wait for output failed: %v", err)
	}
	if result["step1"] != "ok" {
		t.Fatalf("unexpected result: %s", result["step1"])
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

	type FlowOutput struct {
		Submit  string `json:"submit"`
		Approve string `json:"approve"`
		Publish string `json:"publish"`
	}

	flow := NewFlow("signal_approval_flow",
		InitialStep("submit", func(ctx context.Context, doc string) (string, error) {
			return "submitted: " + doc, nil
		}),
		StepWithOneDependencyAndSignal("approve",
			Dependency("submit"),
			func(ctx context.Context, doc string, approval ApprovalInput, submitResult string) (string, error) {
				if !approval.Approved {
					return "", fmt.Errorf("approval denied by %s", approval.ApproverID)
				}
				return fmt.Sprintf("approved by %s: %s", approval.ApproverID, submitResult), nil
			}),
		StepWithOneDependency("publish",
			Dependency("approve"),
			func(ctx context.Context, doc string, approveResult string) (string, error) {
				return "published: " + approveResult, nil
			}),
	)

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
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

	h, err := client.RunFlow(t.Context(), "signal_approval_flow", "my_document")
	if err != nil {
		t.Fatal(err)
	}

	// Wait a bit for submit step to complete
	time.Sleep(200 * time.Millisecond)

	// Deliver approval signal
	err = client.SignalFlow(t.Context(), "signal_approval_flow", h.ID, "approve", ApprovalInput{
		ApproverID: "user123",
		Approved:   true,
	})
	if err != nil {
		t.Fatalf("signal delivery failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result FlowOutput
	if err := h.WaitForOutput(ctx, &result); err != nil {
		t.Fatalf("wait for output failed: %v", err)
	}

	if result.Submit != "submitted: my_document" {
		t.Fatalf("unexpected submit result: %s", result.Submit)
	}
	if result.Approve != "approved by user123: submitted: my_document" {
		t.Fatalf("unexpected approve result: %s", result.Approve)
	}
	if result.Publish != "published: approved by user123: submitted: my_document" {
		t.Fatalf("unexpected publish result: %s", result.Publish)
	}
}

func TestFlowWithInitialSignal(t *testing.T) {
	client := getTestClient(t)

	type TriggerInput struct {
		Action string `json:"action"`
	}

	type FlowOutput struct {
		Start  string `json:"start"`
		Finish string `json:"finish"`
	}

	flow := NewFlow("signal_trigger_flow",
		InitialStepWithSignal("start",
			func(ctx context.Context, data string, trigger TriggerInput) (string, error) {
				return fmt.Sprintf("started with %s from %s", trigger.Action, data), nil
			}),
		StepWithOneDependency("finish",
			Dependency("start"),
			func(ctx context.Context, data string, startResult string) (string, error) {
				return "completed: " + startResult, nil
			}),
	)

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
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

	h, err := client.RunFlow(t.Context(), "signal_trigger_flow", "workflow_data")
	if err != nil {
		t.Fatal(err)
	}

	// Deliver signal to initial step
	err = client.SignalFlow(t.Context(), "signal_trigger_flow", h.ID, "start", TriggerInput{
		Action: "manual_trigger",
	})
	if err != nil {
		t.Fatalf("signal delivery failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result FlowOutput
	if err := h.WaitForOutput(ctx, &result); err != nil {
		t.Fatalf("wait for output failed: %v", err)
	}

	if result.Start != "started with manual_trigger from workflow_data" {
		t.Fatalf("unexpected start result: %s", result.Start)
	}
	if result.Finish != "completed: started with manual_trigger from workflow_data" {
		t.Fatalf("unexpected finish result: %s", result.Finish)
	}
}

func TestFlowSignalAlreadyDelivered(t *testing.T) {
	client := getTestClient(t)

	type SignalData struct {
		Value int `json:"value"`
	}

	flow := NewFlow("signal_duplicate_flow",
		InitialStepWithSignal("step1",
			func(ctx context.Context, in string, sig SignalData) (string, error) {
				return fmt.Sprintf("%s: %d", in, sig.Value), nil
			}),
	)

	worker, err := client.NewWorker(t.Context(), WithFlow(flow))
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := worker.Start(t.Context()); err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "signal_duplicate_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	// Deliver signal first time (should succeed)
	err = client.SignalFlow(t.Context(), "signal_duplicate_flow", h.ID, "step1", SignalData{Value: 42})
	if err != nil {
		t.Fatalf("first signal delivery failed: %v", err)
	}

	// Try to deliver signal second time (should fail)
	err = client.SignalFlow(t.Context(), "signal_duplicate_flow", h.ID, "step1", SignalData{Value: 99})
	if err == nil {
		t.Fatal("expected error when delivering signal twice, got nil")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result map[string]string
	if err := h.WaitForOutput(ctx, &result); err != nil {
		t.Fatalf("wait for output failed: %v", err)
	}

	// Verify first signal was used, not second
	if result["step1"] != "input: 42" {
		t.Fatalf("unexpected result (should use first signal): %s", result["step1"])
	}
}
