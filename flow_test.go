package catbird

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
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
	if runInfo.Status != "started" {
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

func TestMapStepReducerTypeMismatchPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for map reducer item type mismatch")
		}
	}()

	_ = NewStep("mapped").
		MapInput().
		Handler(func(ctx context.Context, n int) (int, error) {
			return n * 2, nil
		}).
		Reduce(0, func(ctx context.Context, acc int, item string) (int, error) {
			return acc, nil
		})
}

func TestFlowMapStepReducerRuntime(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "map_reduce_runtime")

	flow := NewFlow(flowName).
		AddStep(NewStep("double").
			MapInput().
			Handler(func(ctx context.Context, n int) (int, error) {
				return n * 2, nil
			}).
			Reduce(0, func(ctx context.Context, acc int, out int) (int, error) {
				return acc + out, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(120 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, []int{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}

	var out int
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatalf("wait for map reduced flow output failed: %v", err)
	}

	if out != 20 {
		t.Fatalf("unexpected map reduced flow output: got %d, want %d", out, 20)
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
			"name":        "mapped",
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
	if workStep.Status == "completed" {
		t.Fatalf("expected map parent step not to be completed while tasks are still in-flight")
	}

	mapTable := fmt.Sprintf("cb_m_%s", flowName)
	earlyQ := fmt.Sprintf(`
		SELECT
			count(*) AS total,
			count(*) FILTER (WHERE status = 'completed') AS completed,
			count(*) FILTER (WHERE status IN ('queued', 'started')) AS active
		FROM %s
		WHERE flow_run_id = $1 AND step_name = $2;`, mapTable)

	var total int
	var completed int
	var active int
	if err := client.Conn.QueryRow(t.Context(), earlyQ, h.ID, "work").Scan(&total, &completed, &active); err != nil {
		t.Fatal(err)
	}
	if workStep.Status == "started" && total == 0 {
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
	if runInfo.Status != "failed" {
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
	if workStatus != "failed" {
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

func TestGeneratorStepYieldFunctionAccepted(t *testing.T) {
	step := NewGeneratorStep("discover").
		DependsOn("seed").
		Generator(func(ctx context.Context, in string, seed string, yield func(int) error) error {
			return yield(len(seed))
		}).
		Handler(func(ctx context.Context, item int) (string, error) {
			return fmt.Sprintf("%d", item), nil
		})

	if !step.isGenerator {
		t.Fatalf("expected step to be generator")
	}
	if step.generatorFn == nil {
		t.Fatalf("expected generator function to be set")
	}
	if step.generatorHandler == nil {
		t.Fatalf("expected generator handler to be set")
	}

	flow := NewFlow("generator_validate_ok").
		AddStep(NewStep("seed").Handler(func(ctx context.Context, in string) (string, error) { return in, nil })).
		AddStep(step)

	if err := validateFlowDependencies(flow); err != nil {
		t.Fatalf("expected valid generator flow, got error: %v", err)
	}
}

func TestGeneratorStepChannelYieldRejected(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for channel-based generator yield")
		}
	}()

	_ = NewGeneratorStep("discover").
		DependsOn("seed").
		Generator(func(ctx context.Context, seed string, yield chan<- int) error {
			return nil
		})
}

func TestGeneratorStepReducerTypeMismatchPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for reducer item type mismatch")
		}
	}()

	_ = NewGeneratorStep("discover").
		Generator(func(ctx context.Context, in int, yield func(int) error) error {
			return nil
		}).
		Handler(func(ctx context.Context, item int) (int, error) {
			return item, nil
		}).
		Reduce(0, func(ctx context.Context, acc int, item string) (int, error) {
			return acc, nil
		})
}

func TestFlowGeneratorStepRuntime(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "generator_runtime")

	flow := NewFlow(flowName).
		AddStep(NewStep("seed").Handler(func(ctx context.Context, in int) (int, error) {
			return in, nil
		})).
		AddStep(NewGeneratorStep("generate").
			DependsOn("seed").
			Generator(func(ctx context.Context, in int, seed int, yield func(int) error) error {
				for i := 0; i < seed; i++ {
					if err := yield(i); err != nil {
						return err
					}
				}
				return nil
			}).
			Handler(func(ctx context.Context, item int) (int, error) {
				return item * 2, nil
			})).
		AddStep(NewStep("sum").
			DependsOn("generate").
			Handler(func(ctx context.Context, in int, generated []int) (int, error) {
				total := 0
				for _, v := range generated {
					total += v
				}
				return total, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(120 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, 5)
	if err != nil {
		t.Fatal(err)
	}

	var out int
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatalf("wait for generator flow output failed: %v", err)
	}

	if out != 20 {
		t.Fatalf("unexpected generator flow output: got %d, want %d", out, 20)
	}
}

func TestFlowGeneratorStepFailure(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "generator_failure")

	flow := NewFlow(flowName).
		AddStep(NewStep("seed").Handler(func(ctx context.Context, in int) (int, error) {
			return in, nil
		})).
		AddStep(NewGeneratorStep("generate").
			DependsOn("seed").
			Generator(func(ctx context.Context, in int, seed int, yield func(int) error) error {
				for i := 0; i < seed; i++ {
					if err := yield(i); err != nil {
						return err
					}
					if i == 1 {
						return fmt.Errorf("generator stopped at %d", i)
					}
				}
				return nil
			}).
			Handler(func(ctx context.Context, item int) (int, error) {
				return item * 2, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(120 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, 5)
	if err != nil {
		t.Fatal(err)
	}

	var out []int
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	err = h.WaitForOutput(ctx, &out)
	if err == nil {
		t.Fatalf("expected generator flow to fail")
	}

	runInfo, err := client.GetFlowRun(t.Context(), flowName, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if runInfo.Status != "failed" {
		t.Fatalf("expected failed flow status, got %s", runInfo.Status)
	}
	if !strings.Contains(runInfo.ErrorMessage, "generator stopped at 1") {
		t.Fatalf("unexpected error message: %s", runInfo.ErrorMessage)
	}
}

func TestFlowGeneratorStepNoDependencies(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "generator_no_deps")

	flow := NewFlow(flowName).
		AddStep(NewGeneratorStep("generate").
			Generator(func(ctx context.Context, input int, yield func(int) error) error {
				for i := 0; i < input; i++ {
					if err := yield(i); err != nil {
						return err
					}
				}
				return nil
			}).
			Handler(func(ctx context.Context, item int) (int, error) {
				return item + 1, nil
			})).
		AddStep(NewStep("sum").
			DependsOn("generate").
			Handler(func(ctx context.Context, in int, generated []int) (int, error) {
				total := 0
				for _, v := range generated {
					total += v
				}
				return total, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(120 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, 5)
	if err != nil {
		t.Fatal(err)
	}

	var out int
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatalf("wait for generator no-deps flow output failed: %v", err)
	}

	if out != 15 {
		t.Fatalf("unexpected generator no-deps flow output: got %d, want %d", out, 15)
	}
}

func TestFlowGeneratorStepWithSignalAndDependency(t *testing.T) {
	type approval struct {
		Offset int `json:"offset"`
	}

	client := getTestClient(t)
	flowName := testFlowName(t, "generator_signal_dep")

	flow := NewFlow(flowName).
		AddStep(NewStep("seed").Handler(func(ctx context.Context, in int) (int, error) {
			return 3, nil
		})).
		AddStep(NewGeneratorStep("generate").
			DependsOn("seed").
			Signal().
			Generator(func(ctx context.Context, in int, signal approval, seed int, yield func(int) error) error {
				for i := 0; i < seed; i++ {
					if err := yield(in + signal.Offset + i); err != nil {
						return err
					}
				}
				return nil
			}).
			Handler(func(ctx context.Context, item int) (int, error) {
				return item, nil
			})).
		AddStep(NewStep("sum").
			DependsOn("generate").
			Handler(func(ctx context.Context, in int, generated []int) (int, error) {
				total := 0
				for _, v := range generated {
					total += v
				}
				return total, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(120 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, 10)
	if err != nil {
		t.Fatal(err)
	}

	err = client.SignalFlow(t.Context(), flowName, h.ID, "generate", approval{Offset: 1})
	if err != nil {
		t.Fatalf("signal flow failed: %v", err)
	}

	var out int
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatalf("wait for generator signal/dep flow output failed: %v", err)
	}

	if out != 36 {
		t.Fatalf("unexpected generator signal/dep flow output: got %d, want %d", out, 36)
	}
}

func TestFlowGeneratorStepReducerRuntime(t *testing.T) {
	client := getTestClient(t)
	flowName := testFlowName(t, "generator_reduce_runtime")

	flow := NewFlow(flowName).
		AddStep(NewGeneratorStep("generate").
			Generator(func(ctx context.Context, input int, yield func(int) error) error {
				for i := 0; i < input; i++ {
					if err := yield(i); err != nil {
						return err
					}
				}
				return nil
			}).
			Handler(func(ctx context.Context, item int) (int, error) {
				return item * 2, nil
			}).
			Reduce(0, func(ctx context.Context, acc int, out int) (int, error) {
				return acc + out, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(120 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, 5)
	if err != nil {
		t.Fatal(err)
	}

	var out int
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatalf("wait for generator reduced flow output failed: %v", err)
	}

	if out != 20 {
		t.Fatalf("unexpected generator reduced flow output: got %d, want %d", out, 20)
	}
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
			Signal().
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
			Signal().
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
			Signal().
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

var testRunID = fmt.Sprintf("%x", sha1.Sum([]byte(strconv.FormatInt(time.Now().UnixNano(), 10))))[:6]

func testFlowName(t *testing.T, base string) string {
	sanitized := strings.ToLower(t.Name())
	sanitized = strings.NewReplacer("/", "_").Replace(sanitized)
	sanitized = regexp.MustCompile(`[^a-z0-9_]+`).ReplaceAllString(sanitized, "_")
	suffix := fmt.Sprintf("%x", sha1.Sum([]byte(testRunID+"_"+sanitized)))[:8]
	maxBaseLen := 58 - len(suffix) - 1
	if maxBaseLen < 1 {
		maxBaseLen = 1
	}
	if len(base) > maxBaseLen {
		base = base[:maxBaseLen]
	}
	return fmt.Sprintf("%s_%s", base, suffix)
}

// TestConditionEvaluation tests the cb_evaluate_condition() PostgreSQL function directly
func TestConditionEvaluation(t *testing.T) {
	client := getTestClient(t)

	tests := []struct {
		name       string
		condition  string
		payload    string
		namedConds map[string]string
		want       bool
		wantErr    bool
	}{
		// Basic equality
		{
			name:      "eq_number_true",
			condition: "score eq 80",
			payload:   `{"score": 80}`,
			want:      true,
		},
		{
			name:      "eq_number_false",
			condition: "score eq 80",
			payload:   `{"score": 90}`,
			want:      false,
		},
		{
			name:      "eq_string_true",
			condition: "status eq approved",
			payload:   `{"status": "approved"}`,
			want:      true,
		},
		{
			name:      "eq_string_false",
			condition: "status eq approved",
			payload:   `{"status": "rejected"}`,
			want:      false,
		},

		// Not equal
		{
			name:      "ne_true",
			condition: "status ne approved",
			payload:   `{"status": "pending"}`,
			want:      true,
		},
		{
			name:      "ne_false",
			condition: "status ne approved",
			payload:   `{"status": "approved"}`,
			want:      false,
		},

		// Greater than / less than
		{
			name:      "gt_true",
			condition: "score gt 80",
			payload:   `{"score": 90}`,
			want:      true,
		},
		{
			name:      "gt_false",
			condition: "score gt 80",
			payload:   `{"score": 70}`,
			want:      false,
		},
		{
			name:      "gte_equal",
			condition: "score gte 80",
			payload:   `{"score": 80}`,
			want:      true,
		},
		{
			name:      "gte_greater",
			condition: "score gte 80",
			payload:   `{"score": 90}`,
			want:      true,
		},
		{
			name:      "lt_true",
			condition: "score lt 80",
			payload:   `{"score": 70}`,
			want:      true,
		},
		{
			name:      "lte_equal",
			condition: "score lte 80",
			payload:   `{"score": 80}`,
			want:      true,
		},

		// Exists
		{
			name:      "exists_true",
			condition: "optional_field exists",
			payload:   `{"optional_field": "value"}`,
			want:      true,
		},
		{
			name:      "exists_false",
			condition: "optional_field exists",
			payload:   `{"other_field": "value"}`,
			want:      false,
		},
		{
			name:      "not_exists_true",
			condition: "not optional_field exists",
			payload:   `{"other_field": "value"}`,
			want:      true,
		},
		{
			name:      "not_exists_false",
			condition: "not optional_field exists",
			payload:   `{"optional_field": "value"}`,
			want:      false,
		},

		// Contains
		{
			name:      "contains_string_true",
			condition: "message contains error",
			payload:   `{"message": "an error occurred"}`,
			want:      true,
		},
		{
			name:      "contains_string_false",
			condition: "message contains error",
			payload:   `{"message": "success"}`,
			want:      false,
		},
		{
			name:      "not_contains_true",
			condition: "not message contains error",
			payload:   `{"message": "success"}`,
			want:      true,
		},
		{
			name:      "not_contains_false",
			condition: "not message contains error",
			payload:   `{"message": "an error occurred"}`,
			want:      false,
		},

		// Nested fields
		{
			name:      "nested_field_true",
			condition: "user.age gte 18",
			payload:   `{"user": {"age": 25}}`,
			want:      true,
		},
		{
			name:      "nested_field_false",
			condition: "user.age gte 18",
			payload:   `{"user": {"age": 15}}`,
			want:      false,
		},
		{
			name:      "deep_nested_field",
			condition: "data.metrics.score gt 50",
			payload:   `{"data": {"metrics": {"score": 75}}}`,
			want:      true,
		},

		// Edge cases
		{
			name:      "missing_field_eq",
			condition: "missing eq value",
			payload:   `{"other": "data"}`,
			want:      false,
		},
		{
			name:      "null_value_exists",
			condition: "nullable exists",
			payload:   `{"nullable": null}`,
			want:      true, // Field exists even if null
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool

			query := `SELECT cb_evaluate_condition_expr($1, $2::jsonb)`
			err := client.Conn.QueryRow(t.Context(), query, tt.condition, tt.payload).Scan(&result)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result != tt.want {
				t.Errorf("cb_evaluate_condition_expr(%q, %s) = %v, want %v",
					tt.condition, tt.payload, result, tt.want)
			}
		})
	}
}

// TestFlowCondition tests positive condition logic in flows
func TestFlowCondition(t *testing.T) {
	client := getTestClient(t)

	t.Run("skip_step_when_condition_false", func(t *testing.T) {
		flowName := testFlowName(t, "condition_test_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
				return score, nil
			})).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1 lt 90").
				Handler(func(ctx context.Context, score int, step1Score int) (string, error) {
					return "step2_executed", nil
				})).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, score int, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				result["step1"] = score
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, 95)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		if _, exists := result["step2"]; exists {
			t.Errorf("step2 should have been skipped, but found in output: %v", result)
		}

		var state string
		stepTableName := "cb_s_" + handle.Name
		query := fmt.Sprintf(`SELECT status FROM %s WHERE flow_run_id = $1 AND step_name = 'step2'`, stepTableName)
		err = client.Conn.QueryRow(t.Context(), query, handle.ID).Scan(&state)
		if err != nil {
			t.Fatalf("failed to query step state: %v", err)
		}
		if state != "skipped" {
			t.Errorf("step2 state = %q, want 'skipped'", state)
		}
	})

	t.Run("execute_step_when_condition_true", func(t *testing.T) {
		flowName := testFlowName(t, "condition_true_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
				return score, nil
			})).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1 lt 90").
				Handler(func(ctx context.Context, score int, step1Score int) (string, error) {
					return "step2_executed", nil
				})).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, score int, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				result["step1"] = score
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, 75)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		if step2Val, exists := result["step2"]; !exists {
			t.Errorf("step2 should have executed, but not found in output")
		} else if step2Val != "step2_executed" {
			t.Errorf("step2 output = %v, want 'step2_executed'", step2Val)
		}
	})

	t.Run("skip_with_condition", func(t *testing.T) {
		flowName := testFlowName(t, "condition_named_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
				return score, nil
			})).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1 lt 90").
				Handler(func(ctx context.Context, score int, step1Score int) (string, error) {
					return "step2_executed", nil
				})).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, score int, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				result["step1"] = score
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, 95)
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		var state string
		tableName := "cb_s_" + handle.Name
		query := fmt.Sprintf(`SELECT status FROM %s WHERE flow_run_id = $1 AND step_name = 'step2'`, tableName)
		err = client.Conn.QueryRow(t.Context(), query, handle.ID).Scan(&state)
		if err != nil {
			t.Fatalf("failed to query step state: %v", err)
		}
		if state != "skipped" {
			t.Errorf("step2 state = %q, want 'skipped'", state)
		}
	})
}

func TestFlowConditionEdgeCases(t *testing.T) {
	client := getTestClient(t)

	t.Run("missing_field_condition", func(t *testing.T) {
		flowName := testFlowName(t, "missing_field_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return input, nil
			})).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1.optional_field ne value").
				Handler(func(ctx context.Context, input map[string]interface{}, step1Out map[string]interface{}) (string, error) {
					return "executed", nil
				})).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, input map[string]interface{}, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, map[string]interface{}{"other": "data"})
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		if _, exists := result["step2"]; !exists {
			t.Errorf("step2 should have executed when field is missing")
		}
	})

	t.Run("null_value_handling", func(t *testing.T) {
		flowName := testFlowName(t, "null_value_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return input, nil
			})).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1.field exists").
				Handler(func(ctx context.Context, input map[string]interface{}, step1Out map[string]interface{}) (string, error) {
					return "executed", nil
				})).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, input map[string]interface{}, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, map[string]interface{}{"field": nil})
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		step2Val, exists := result["step2"]
		if !exists {
			t.Errorf("step2 should have executed when field exists (even if null)")
		} else if step2Val != "executed" {
			t.Errorf("step2 output = %v, want 'executed'", step2Val)
		}
	})

	t.Run("complex_nested_path", func(t *testing.T) {
		flowName := testFlowName(t, "nested_path_flow")
		flow := NewFlow(flowName).
			AddStep(NewStep("step1").Handler(func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
				return input, nil
			})).
			AddStep(NewStep("step2").
				DependsOn("step1").
				Condition("step1.data.user.age gte 18").
				Handler(func(ctx context.Context, input map[string]interface{}, step1Out map[string]interface{}) (string, error) {
					return "executed", nil
				})).
			AddStep(NewStep("finalize").DependsOn("step2").Handler(func(ctx context.Context, input map[string]interface{}, step2Result Optional[string]) (map[string]any, error) {
				result := make(map[string]any)
				if step2Result.IsSet {
					result["step2"] = step2Result.Value
				}
				return result, nil
			}))

		worker := client.NewWorker(t.Context()).AddFlow(flow)
		startTestWorker(t, worker)

		handle, err := client.RunFlow(t.Context(), flowName, map[string]interface{}{
			"data": map[string]interface{}{
				"user": map[string]interface{}{
					"age": 15,
				},
			},
		})
		if err != nil {
			t.Fatalf("RunFlow failed: %v", err)
		}

		ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var result map[string]interface{}
		err = handle.WaitForOutput(ctxTimeout, &result)
		if err != nil {
			t.Fatalf("WaitForOutput failed: %v", err)
		}

		var state string
		tableName := "cb_s_" + handle.Name
		query := fmt.Sprintf(`SELECT status FROM %s WHERE flow_run_id = $1 AND step_name = 'step2'`, tableName)
		err = client.Conn.QueryRow(t.Context(), query, handle.ID).Scan(&state)
		if err != nil {
			t.Fatalf("failed to query step state: %v", err)
		}
		if state != "skipped" {
			t.Errorf("step2 state = %q, want 'skipped'", state)
		}
	})
}

func TestFlowOptionalDependency(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "optional_dep_flow")
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
			return score, nil
		})).
		AddStep(NewStep("step2").
			DependsOn("step1").
			Condition("step1 gte 50").
			Handler(func(ctx context.Context, score int, step1Score int) (int, error) {
				return step1Score * 2, nil
			})).
		AddStep(NewStep("step3").
			DependsOn("step2").
			Handler(func(ctx context.Context, score int, step2Out Optional[int]) (int, error) {
				if step2Out.IsSet {
					return step2Out.Value, nil
				}
				return 0, nil
			}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)

	handle, err := client.RunFlow(t.Context(), flowName, 40)
	if err != nil {
		t.Fatalf("RunFlow failed: %v", err)
	}

	ctxTimeout, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var result int
	if err := handle.WaitForOutput(ctxTimeout, &result); err != nil {
		t.Fatalf("WaitForOutput failed: %v", err)
	}

	if result != 0 {
		t.Errorf("step3 output = %d, want 0", result)
	}

	handle, err = client.RunFlow(t.Context(), flowName, 60)
	if err != nil {
		t.Fatalf("RunFlow failed: %v", err)
	}

	ctxTimeout, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	result = 0
	if err := handle.WaitForOutput(ctxTimeout, &result); err != nil {
		t.Fatalf("WaitForOutput failed: %v", err)
	}

	if result != 120 {
		t.Errorf("step3 output = %d, want 120", result)
	}
}

func TestFlowConditionalDependencyRequiresOptional(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "missing_optional_dep")
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, score int) (int, error) {
			return score, nil
		})).
		AddStep(NewStep("step2").
			DependsOn("step1").
			Condition("step1 gte 50").
			Handler(func(ctx context.Context, score int, step1Score int) (int, error) {
				return step1Score * 2, nil
			})).
		AddStep(NewStep("step3").
			DependsOn("step2").
			Handler(func(ctx context.Context, score int, step2Out int) (int, error) {
				return step2Out, nil
			}))

	if err := client.CreateFlow(t.Context(), flow); err == nil {
		t.Fatalf("expected CreateFlow to fail when depending on conditional step without OptionalDependency")
	}
}

func waitForFlowRunStatus(t *testing.T, client *Client, flowName string, runID int64, status string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		run, err := client.GetFlowRun(t.Context(), flowName, runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == status {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected flow run to reach status %s, last status: %s", status, run.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestFlowCancelStartedRun(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "cancel_started")
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "input")
	if err != nil {
		t.Fatal(err)
	}

	waitForFlowRunStatus(t, client, flowName, h.ID, "started", 5*time.Second)

	if err := client.CancelFlowRun(t.Context(), flowName, h.ID, CancelOpts{Reason: "test flow cancel"}); err != nil {
		t.Fatal(err)
	}

	waitForFlowRunStatus(t, client, flowName, h.ID, "canceled", 5*time.Second)
}

func TestFlowInternalCancelCurrentRun(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "cancel_internal")
	flow := NewFlow(flowName).
		AddStep(NewStep("step1").Handler(func(ctx context.Context, in string) (string, error) {
			if err := CancelCurrentFlowRun(ctx, CancelOpts{Reason: "business early exit"}); err != nil {
				return "", err
			}
			<-ctx.Done()
			return "", ctx.Err()
		}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "input")
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

	run, err := client.GetFlowRun(t.Context(), flowName, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "canceled" {
		t.Fatalf("expected canceled, got %s", run.Status)
	}
}
