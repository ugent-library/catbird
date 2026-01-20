package catbird

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestFlows(t *testing.T) {
	dsn := os.Getenv("CB_TEST_CONN")

	func() {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		MigrateDownTo(t.Context(), db, 0)
		MigrateUpTo(t.Context(), db, SchemaVersion)
	}()

	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}

	client := New(pool)

	task1 := NewTask("task1", func(ctx context.Context, in string) (string, error) {
		return "processed by task 1", nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	flow1 := &Flow{
		Name: "flow1",
		Steps: []Step{
			{Name: "step1"},
			{Name: "step2", DependsOn: []string{"step1"}},
		},
	}

	type flow1Output struct {
		Step1 string `json:"step1"`
		Step2 string `json:"step2"`
	}

	type step1Input struct {
		FlowInput string `json:"flow_input"`
	}

	step1 := NewTask("step1", func(ctx context.Context, in step1Input) (string, error) {
		return in.FlowInput + " processed by step 1", nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	type step2Input struct {
		FlowInput string `json:"flow_input"`
		Step1     string `json:"step1"`
	}

	step2 := NewTask("step2", func(ctx context.Context, in step2Input) (string, error) {
		return in.Step1 + " and by step 2", nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	flow2 := &Flow{
		Name: "flow2",
		Steps: []Step{
			{Name: "flow2step1"},
			{Name: "flow2step2", Map: "flow2step1"},
			{Name: "flow2step3", DependsOn: []string{"flow2step2"}},
		},
	}

	type flow2Output struct {
		Step1 []string `json:"flow2step1"`
		Step2 []string `json:"flow2step2"`
		Step3 string   `json:"flow2step3"`
	}

	type flow2step1Input struct {
		FlowInput string `json:"flow_input"`
	}

	flow2step1 := NewTask("flow2step1", func(ctx context.Context, in flow2step1Input) ([]string, error) {
		return strings.Split(in.FlowInput, ","), nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	type flow2step2Input struct {
		FlowInput string `json:"flow_input"`
		Step1     string `json:"flow2step1"`
	}

	flow2step2 := NewTask("flow2step2", func(ctx context.Context, in flow2step2Input) (string, error) {
		return strings.ToUpper(in.Step1), nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	type flow2step3Input struct {
		FlowInput string   `json:"flow_input"`
		Step2     []string `json:"flow2step2"`
	}

	flow2step3 := NewTask("flow2step3", func(ctx context.Context, in flow2step3Input) (string, error) {
		return strings.Join(in.Step2, "|"), nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	// the worker will also create the tasks, but we create them here explicitly to test CreateTask
	if err := client.CreateTask(t.Context(), task1); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTask(t.Context(), step1); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTask(t.Context(), step2); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTask(t.Context(), flow2step1); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTask(t.Context(), flow2step2); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTask(t.Context(), flow2step3); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateFlow(t.Context(), flow1); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateFlow(t.Context(), flow2); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	worker, err := client.NewWorker(WorkerOpts{
		Log:   logger,
		Tasks: []*Task{task1, step1, step2, flow2step1, flow2step2, flow2step3},
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := worker.Start(t.Context()); err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	func() {
		var out string
		info, err := client.RunTaskWait(t.Context(), "task1", "", RunTaskOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if err := info.OutputAs(&out); err != nil {
			t.Fatal(err)
		}
		if out != "processed by task 1" {
			t.Fatalf("unexpected task output: %s", info.Output)
		}
	}()

	func() {
		var out flow1Output
		info, err := client.RunFlowWait(t.Context(), "flow1", "input")
		if err != nil {
			t.Fatal(err)
		}
		if err := info.OutputAs(&out); err != nil {
			t.Fatal(err)
		}
		if out.Step1 != "input processed by step 1" {
			t.Fatalf("unexpected flow output: %s", info.Output)
		}
		if out.Step2 != "input processed by step 1 and by step 2" {
			t.Fatalf("unexpected flow output: %s", info.Output)
		}
	}()

	func() {
		var out flow2Output
		info, err := client.RunFlowWait(t.Context(), "flow2", "a,b,c")
		if err != nil {
			t.Fatal(err)
		}
		if err := info.OutputAs(&out); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(out.Step1, []string{"a", "b", "c"}) {
			t.Fatalf("unexpected flow output: %s", info.Output)
		}
		if !slices.Equal(out.Step2, []string{"A", "B", "C"}) {
			t.Fatalf("unexpected flow output: %s", info.Output)
		}
		if out.Step3 != "A|B|C" {
			t.Fatalf("unexpected flow output: %s", info.Output)
		}
	}()
}
