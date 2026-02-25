package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunFlowQuery(t *testing.T) {
	type input struct {
		Value string `json:"value"`
	}

	query, args, err := RunFlowQuery(
		"test_flow",
		input{Value: "hello"},
		RunFlowOpts{
			ConcurrencyKey: "con-1",
			IdempotencyKey: "idem-1",
			VisibleAt:      time.Unix(1700000300, 0).UTC(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedQuery := `SELECT * FROM cb_run_flow(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4, visible_at => $5);`
	if query != expectedQuery {
		t.Fatalf("unexpected query: %s", query)
	}

	if len(args) != 5 {
		t.Fatalf("expected 5 args, got %d", len(args))
	}

	if gotName, ok := args[0].(string); !ok || gotName != "test_flow" {
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

	if gotVisibleAt, ok := args[4].(*time.Time); !ok || gotVisibleAt == nil || !gotVisibleAt.Equal(time.Unix(1700000300, 0).UTC()) {
		t.Fatalf("unexpected visible_at arg: %#v", args[4])
	}

	_, nilArgs, err := RunFlowQuery("test_flow", input{Value: "hello"})
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

func TestFlowCreate(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow("test_flow").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return in + " processed", nil
		}))

	err := client.CreateFlow(t.Context(), flow)
	if err != nil {
		t.Fatal(err)
	}

	worker := client.NewWorker(t.Context()).AddFlow(flow)

	startTestWorker(t, worker)

	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "test_flow", "input")
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

	flow := NewFlow("single_step_flow").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return in + " processed by step 1", nil
		}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "single_step_flow", "input")
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

func TestFlowRunDelayedVisibleAt(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "delayed_flow")
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return in + " processed", nil
		}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)

	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "input", RunFlowOpts{
		VisibleAt: time.Now().Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)
	runInfo, err := client.GetFlowRun(t.Context(), flowName, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if runInfo.Status != StatusStarted {
		t.Fatalf("expected delayed flow to remain started before visible_at, got %s", runInfo.Status)
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

func TestFlowWithDependencies(t *testing.T) {
	client := getTestClient(t)

	// Flow structure: step1 -> step2 -> step3 (linear chain)
	// Final step is step3, which returns the full chain

	flow := NewFlow("dependency_flow").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return in + " processed by step 1", nil
		})).
		AddStep(NewStep("step2").
			DependsOn("step1").
			Handler(func(ctx context.Context, in string, step1Out string) (string, error) {
				return step1Out + " and by step 2", nil
			})).
		AddStep(NewStep("step3").
			DependsOn("step2").
			Handler(func(ctx context.Context, in string, step2Out string) (string, error) {
				return step2Out + " and by step 3", nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "dependency_flow", "input")
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

func TestFlowMapInput(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "map_input_flow")

	flow := NewFlow(flowName).
		AddStep(NewStep("double").
			MapInput().
			Handler(func(ctx context.Context, n int) (int, error) {
				return n * 2, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, []int{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	var out []int
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	if len(out) != 3 || out[0] != 2 || out[1] != 4 || out[2] != 6 {
		t.Fatalf("unexpected output: %#v", out)
	}
}

func TestFlowMapStepOutput(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "map_dep_flow")

	flow := NewFlow(flowName).
		AddStep(NewStep("numbers").Handler(func(ctx context.Context, in string) ([]int, error) {
			return []int{1, 2, 3, 4}, nil
		})).
		AddStep(NewStep("double").
			Map("numbers").
			Handler(func(ctx context.Context, in string, n int) (int, error) {
				return n * 2, nil
			})).
		AddStep(NewStep("sum").
			DependsOn("double").
			Handler(func(ctx context.Context, in string, doubled []int) (int, error) {
				sum := 0
				for _, n := range doubled {
					sum += n
				}
				return sum, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "input")
	if err != nil {
		t.Fatal(err)
	}

	var out int
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	if out != 20 {
		t.Fatalf("unexpected output: %d", out)
	}
}

func TestFlowMapMetadataInInfo(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "map_info_flow")

	flow := NewFlow(flowName).
		AddStep(NewStep("numbers").Handler(func(ctx context.Context, in string) ([]int, error) {
			return []int{1, 2, 3}, nil
		})).
		AddStep(NewStep("double").
			Map("numbers").
			Handler(func(ctx context.Context, in string, n int) (int, error) {
				return n * 2, nil
			}))

	if err := client.CreateFlow(t.Context(), flow); err != nil {
		t.Fatal(err)
	}

	info, err := client.GetFlow(t.Context(), flowName)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(info.Steps))
	}

	var foundMapped bool
	for _, s := range info.Steps {
		if s.Name == "double" {
			foundMapped = true
			if !s.IsMapStep {
				t.Fatalf("expected step %q to be marked as map step", s.Name)
			}
			if s.MapSource != "numbers" {
				t.Fatalf("expected map source 'numbers', got %q", s.MapSource)
			}
		}
	}

	if !foundMapped {
		t.Fatalf("mapped step not found in flow info")
	}
}

func TestFlowMapSourceMustExist(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "map_missing_source")

	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return in, nil
		})).
		AddStep(NewStep("mapped").
			Map("does_not_exist").
			Handler(func(ctx context.Context, in string, v int) (int, error) {
				return v, nil
			}))

	err := client.CreateFlow(t.Context(), flow)
	if err == nil {
		t.Fatalf("expected CreateFlow to fail for missing map source")
	}
}

func TestFlowMapSQLValidationRejectsMissingDependency(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "map_sql_validation")

	steps := []map[string]any{
		{
			"name": "numbers",
		},
		{
			"name":       "mapped",
			"is_map_step": true,
			"map_source":  "numbers",
			"depends_on":  []map[string]any{},
		},
	}

	stepsJSON, err := json.Marshal(steps)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Conn.Exec(t.Context(), `SELECT * FROM cb_create_flow(name => $1, steps => $2);`, flowName, stepsJSON)
	if err == nil {
		t.Fatalf("expected SQL map validation to reject step without map_source dependency")
	}
}

func TestFlowMapParentCompletesAfterAllMapTasks(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "map_parent_waits")

	flow := NewFlow(flowName).
		AddStep(NewStep("numbers").Handler(func(ctx context.Context, in int) ([]int, error) {
			return []int{0, 1, 2, 3, 4}, nil
		})).
		AddStep(NewStep("work").
			Map("numbers").
			Handler(func(ctx context.Context, in int, item int) (int, error) {
				if item == 4 {
					time.Sleep(300 * time.Millisecond)
				}
				return item + 10, nil
			})).
		AddStep(NewStep("collect").
			DependsOn("work").
			Handler(func(ctx context.Context, in int, items []int) ([]int, error) {
				return items, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(120 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, 5)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(120 * time.Millisecond)

	steps, err := client.GetFlowRunSteps(t.Context(), flowName, h.ID)
	if err != nil {
		t.Fatal(err)
	}

	var workStep *StepRunInfo
	for _, step := range steps {
		if step.StepName == "work" {
			workStep = step
			break
		}
	}
	if workStep == nil {
		t.Fatalf("work step not found")
	}
	if workStep.Status == StatusCompleted {
		t.Fatalf("expected map parent step not to be completed while tasks are still in-flight")
	}

	mapTable := fmt.Sprintf("cb_m_%s", flowName)
	earlyQ := fmt.Sprintf(`
		SELECT
			count(*) AS total,
			count(*) FILTER (WHERE status = 'completed') AS completed,
			count(*) FILTER (WHERE status IN ('created', 'started')) AS pending
		FROM %s
		WHERE flow_run_id = $1 AND step_name = $2;`, mapTable)

	var total int
	var completed int
	var pending int
	if err := client.Conn.QueryRow(t.Context(), earlyQ, h.ID, "work").Scan(&total, &completed, &pending); err != nil {
		t.Fatal(err)
	}
	if workStep.Status == StatusStarted && total == 0 {
		t.Fatalf("expected spawned map tasks while parent step is started")
	}

	var out []int
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	if len(out) != 5 {
		t.Fatalf("unexpected output length: got=%d want=5", len(out))
	}
	for i := 0; i < 5; i++ {
		want := i + 10
		if out[i] != want {
			t.Fatalf("unexpected output ordering at index %d: got=%d want=%d", i, out[i], want)
		}
	}

	finalQ := fmt.Sprintf(`
		SELECT
			count(*) AS total,
			count(*) FILTER (WHERE status = 'completed') AS completed,
			count(*) FILTER (WHERE status = 'failed') AS failed
		FROM %s
		WHERE flow_run_id = $1 AND step_name = $2;`, mapTable)

	var failed int
	if err := client.Conn.QueryRow(t.Context(), finalQ, h.ID, "work").Scan(&total, &completed, &failed); err != nil {
		t.Fatal(err)
	}
	if total != 5 || completed != 5 || failed != 0 {
		t.Fatalf("unexpected map task terminal counts: total=%d completed=%d failed=%d", total, completed, failed)
	}
}

func TestFlowMapTaskFailureFailsParentAndFlow(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "map_failure_propagation")

	flow := NewFlow(flowName).
		AddStep(NewStep("numbers").Handler(func(ctx context.Context, in int) ([]int, error) {
			return []int{0, 1, 2, 3}, nil
		})).
		AddStep(NewStep("work").
			Map("numbers").
			Handler(func(ctx context.Context, in int, item int) (int, error) {
				if item == 2 {
					return 0, fmt.Errorf("intentional map task failure")
				}
				return item + 10, nil
			})).
		AddStep(NewStep("collect").
			DependsOn("work").
			Handler(func(ctx context.Context, in int, items []int) ([]int, error) {
				return items, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(120 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, 4)
	if err != nil {
		t.Fatal(err)
	}

	var out []int
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	err = h.WaitForOutput(ctx, &out)
	if err == nil {
		t.Fatalf("expected flow to fail due to map task failure")
	}

	runInfo, err := client.GetFlowRun(t.Context(), flowName, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if runInfo.Status != StatusFailed {
		t.Fatalf("expected flow status failed, got %s", runInfo.Status)
	}

	steps, err := client.GetFlowRunSteps(t.Context(), flowName, h.ID)
	if err != nil {
		t.Fatal(err)
	}

	var workStatus string
	for _, step := range steps {
		if step.StepName == "work" {
			workStatus = step.Status
			break
		}
	}
	if workStatus != StatusFailed {
		t.Fatalf("expected map parent step to be failed, got %s", workStatus)
	}

	mapTable := fmt.Sprintf("cb_m_%s", flowName)
	q := fmt.Sprintf(`
		SELECT
			count(*) AS total,
			count(*) FILTER (WHERE status = 'failed') AS failed
		FROM %s
		WHERE flow_run_id = $1 AND step_name = $2;`, mapTable)

	var total int
	var failed int
	if err := client.Conn.QueryRow(t.Context(), q, h.ID, "work").Scan(&total, &failed); err != nil {
		t.Fatal(err)
	}
	if total == 0 || failed == 0 {
		t.Fatalf("expected at least one failed map task, got total=%d failed=%d", total, failed)
	}
}

func TestFlowMapStepConcurrentWorkersSlow(t *testing.T) {
	requireSlowTests(t)

	client := getTestClient(t)
	flowName := testFlowName(t, "map_concurrency")

	var processed atomic.Int64

	flow := NewFlow(flowName).
		AddStep(NewStep("numbers").Handler(func(ctx context.Context, n int) ([]int, error) {
			items := make([]int, n)
			for i := 0; i < n; i++ {
				items[i] = i
			}
			return items, nil
		})).
		AddStep(NewStep("work").
			Map("numbers").
			Handler(func(ctx context.Context, n int, item int) (int, error) {
				processed.Add(1)
				// tiny delay to increase overlap opportunity across workers
				time.Sleep(1 * time.Millisecond)
				return item * 2, nil
			})).
		AddStep(NewStep("sum").
			DependsOn("work").
			Handler(func(ctx context.Context, n int, doubled []int) (int, error) {
				sum := 0
				for _, v := range doubled {
					sum += v
				}
				return sum, nil
			}))

	// Start multiple workers for same flow to stress DB claim concurrency.
	for i := 0; i < 4; i++ {
		worker := client.NewWorker(t.Context(), WorkerOpts{Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))}).AddFlow(flow)
		startTestWorker(t, worker)
	}

	time.Sleep(150 * time.Millisecond)

	const itemCount = 200
	h, err := client.RunFlow(t.Context(), flowName, itemCount)
	if err != nil {
		t.Fatal(err)
	}

	var out int
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	// sum(2*i, i=0..n-1) = n*(n-1)
	wantSum := itemCount * (itemCount - 1)
	if out != wantSum {
		t.Fatalf("unexpected output sum: got=%d want=%d", out, wantSum)
	}

	if got := processed.Load(); got != itemCount {
		t.Fatalf("map handler processed wrong item count: got=%d want=%d", got, itemCount)
	}
}

func TestStepMapModeConflictPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when setting conflicting map source")
		}
	}()

	_ = NewStep("conflict").MapInput().Map("numbers")
}

func TestFlowListFlows(t *testing.T) {
	client := getTestClient(t)

	// Create multiple flows
	flows := make([]*Flow, 2)

	flow1 := NewFlow("list_flow_1").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return in, nil
		}))

	flow2 := NewFlow("list_flow_2").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return in, nil
		}))

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
	worker := client.NewWorker(t.Context()).
		AddFlow(flow1).
		AddFlow(flow2)

	startTestWorker(t, worker)

	time.Sleep(100 * time.Millisecond)

	// Verify both flows execute successfully
	h1, err := client.RunFlow(t.Context(), "list_flow_1", "input_1")
	if err != nil {
		t.Fatal(err)
	}

	var out1 string
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &out1); err != nil {
		t.Fatal(err)
	}

	h2, err := client.RunFlow(t.Context(), "list_flow_2", "input_2")
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

	task1 := NewTask("list_task_1").Handler(func(ctx context.Context, in string) (string, error) {
		return in, nil
	})

	task2 := NewTask("list_task_2").Handler(func(ctx context.Context, in string) (string, error) {
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Flow structure:
	//        step1
	//       /     \
	//    step2    step3
	//       \     /
	//        step4
	// Create a complex flow with multiple dependencies:
	flow := NewFlow("complex_flow").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (int, error) {
			return 10, nil
		})).
		AddStep(NewStep("step2").
			DependsOn("step1").
			Handler(func(ctx context.Context, in string, step1Out int) (int, error) {
				return step1Out * 2, nil // 20
			})).
		AddStep(NewStep("step3").
			DependsOn("step1").
			Handler(func(ctx context.Context, in string, step1Out int) (int, error) {
				return step1Out * 3, nil // 30
			})).
		AddStep(NewStep("step4").
			DependsOn("step2", "step3").
			Handler(func(ctx context.Context, in string, step2Out, step3Out int) (int, error) {
				return step2Out + step3Out, nil // 20 + 30 = 50
			}))

	err := client.CreateFlow(t.Context(), flow)
	if err != nil {
		t.Fatal(err)
	}

	// Start worker to execute flow
	worker := client.NewWorker(t.Context(), WorkerOpts{Logger: logger}).
		AddFlow(flow)

	startTestWorker(t, worker)

	time.Sleep(100 * time.Millisecond)

	// Run the flow
	h, err := client.RunFlow(t.Context(), "complex_flow", "input")
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

	flow := NewFlow("panic_flow").
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			return "success", nil
		})).
		AddStep(NewStep("step2").
			DependsOn("step1").
			Handler(func(ctx context.Context, in string, step1Out string) (string, error) {
				panic("intentional panic in flow step")
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)

	startTestWorker(t, worker)

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
	flowName := testFlowName(t, "circuit_flow")

	openTimeout := 300 * time.Millisecond
	minBackoff := 10 * time.Millisecond
	maxBackoff := 20 * time.Millisecond

	var calls int32
	var mu sync.Mutex
	var times []time.Time

	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(
			func(ctx context.Context, in string) (string, error) {
				n := atomic.AddInt32(&calls, 1)
				mu.Lock()
				times = append(times, time.Now())
				mu.Unlock()
				if n == 1 {
					return "", fmt.Errorf("intentional failure")
				}
				return "ok", nil
			}, HandlerOpts{
				MaxRetries:     2,
				Backoff:        NewFullJitterBackoff(minBackoff, maxBackoff),
				CircuitBreaker: &CircuitBreaker{failureThreshold: 1, openTimeout: openTimeout},
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "input")
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
	flow := NewFlow(flowName).
		AddStep(NewStep("submit").Handler(func(ctx context.Context, doc string) (string, error) {
			return "submitted: " + doc, nil
		})).
		AddStep(NewStep("approve").
			DependsOn("submit").
			Signal(true).
			Handler(func(ctx context.Context, doc string, approval ApprovalInput, submitResult string) (string, error) {
				if !approval.Approved {
					return "", fmt.Errorf("approval denied by %s", approval.ApproverID)
				}
				return fmt.Sprintf("approved by %s: %s", approval.ApproverID, submitResult), nil
			})).
		AddStep(NewStep("publish").
			DependsOn("approve").
			Handler(func(ctx context.Context, doc string, approveResult string) (string, error) {
				return "published: " + approveResult, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "my_document")
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
	client := getTestClient(t)

	type StartInput struct {
		InitiatorID string `json:"initiator_id"`
	}

	flowName := testFlowName(t, "initial_signal_flow")
	flow := NewFlow(flowName).
		AddStep(NewStep("start").
			Signal(true).
			Handler(func(ctx context.Context, flowInput string, signal StartInput) (string, error) {
				return fmt.Sprintf("started by %s", signal.InitiatorID), nil
			})).
		AddStep(NewStep("process").
			DependsOn("start").
			Handler(func(ctx context.Context, flowInput string, startResult string) (string, error) {
				return startResult + " - processed", nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "test_flow")
	if err != nil {
		t.Fatal(err)
	}

	// Deliver signal to initial step immediately
	err = client.SignalFlow(t.Context(), flowName, h.ID, "start", StartInput{
		InitiatorID: "user_alpha",
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

	expectedResult := "started by user_alpha - processed"
	if result != expectedResult {
		t.Errorf("unexpected result: got %q, want %q", result, expectedResult)
	}
}

func TestFlowSignalAlreadyDelivered(t *testing.T) {
	client := getTestClient(t)

	type ApprovalInput struct {
		ApproverID string `json:"approver_id"`
		Response   string `json:"response"`
	}

	flowName := testFlowName(t, "signal_already_delivered_flow")
	flow := NewFlow(flowName).
		AddStep(NewStep("request").Handler(func(ctx context.Context, doc string) (string, error) {
			return "approval requested for: " + doc, nil
		})).
		AddStep(NewStep("approve").
			DependsOn("request").
			Signal(true).
			Handler(func(ctx context.Context, doc string, approval ApprovalInput, reqResult string) (string, error) {
				if approval.Response != "approved" {
					return "", fmt.Errorf("approval denied by %s: %s", approval.ApproverID, approval.Response)
				}
				return fmt.Sprintf("approved by %s: %s", approval.ApproverID, reqResult), nil
			})).
		AddStep(NewStep("complete").
			DependsOn("approve").
			Handler(func(ctx context.Context, doc string, approveResult string) (string, error) {
				return approveResult + " - completed", nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)

	startTestWorker(t, worker)

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "my_doc")
	if err != nil {
		t.Fatal(err)
	}

	// Wait for step to be ready for signal
	time.Sleep(200 * time.Millisecond)

	// Deliver approval signal first time
	err = client.SignalFlow(t.Context(), flowName, h.ID, "approve", ApprovalInput{
		ApproverID: "reviewer1",
		Response:   "approved",
	})
	if err != nil {
		t.Fatalf("first signal delivery failed: %v", err)
	}

	// Try to deliver signal again (should be a no-op or silently ignored)
	// This tests idempotency of signal delivery
	err = client.SignalFlow(t.Context(), flowName, h.ID, "approve", ApprovalInput{
		ApproverID: "reviewer2",
		Response:   "rejected",
	})
	// The second signal delivery might fail or be ignored depending on implementation
	// For now, we just check the flow completes with the first signal's data
	_ = err

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result string
	if err := h.WaitForOutput(ctx, &result); err != nil {
		t.Fatalf("wait for output failed: %v", err)
	}

	// Result should reflect the first signal (reviewer1 approved)
	expectedResult := "approved by reviewer1: approval requested for: my_doc - completed"
	if result != expectedResult {
		t.Errorf("unexpected result: got %q, want %q", result, expectedResult)
	}
}
