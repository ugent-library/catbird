package catbird

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"

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

	type Task1Input struct {
		Str string `json:"str"`
	}

	task1 := NewTaskHandler("task1", func(ctx context.Context, in Task1Input) (string, error) {
		return in.Str + " processed by task 1", nil
	}, TaskHandlerOpts{})

	err = client.CreateTask(t.Context(), "task1")
	if err != nil {
		t.Fatal(err)
	}

	err = client.CreateFlow(t.Context(), NewFlow("flow1",
		WithStep("step1"),
		WithStep("step2").DependsOn("step1"),
		WithStep("step3").DependsOn("step2"),
	))
	if err != nil {
		t.Fatal(err)
	}

	type flow1Output struct {
		Step1 string `json:"step1"`
		Step2 string `json:"step2"`
		Step3 string `json:"step3"`
	}

	type step1Input struct {
		FlowInput string `json:"flow_input"`
	}

	step1 := NewStepHandler("flow1", "step1", func(ctx context.Context, in step1Input) (string, error) {
		return in.FlowInput + " processed by step 1", nil
	}, StepHandlerOpts{})

	type step2Input struct {
		FlowInput string `json:"flow_input"`
		Step1     string `json:"step1"`
	}

	step2 := NewStepHandler("flow1", "step2", func(ctx context.Context, in step2Input) (string, error) {
		return in.Step1 + " and by step 2", nil
	}, StepHandlerOpts{})

	type step3Input struct {
		FlowInput string `json:"flow_input"`
		Step2     string `json:"step2"`
	}

	step3 := NewStepHandler("flow1", "step3", func(ctx context.Context, in step3Input) (string, error) {
		return in.Step2 + " and by step 3", nil
	}, StepHandlerOpts{})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	go func() {
		err := client.StartWorker(t.Context(), []*Handler{task1, step1, step2, step3},
			WithLogger(logger),
		)
		if err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	func() {
		var out string
		info, err := client.RunTaskWait(t.Context(), "task1", Task1Input{Str: "input"}, RunTaskOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if err := info.OutputAs(&out); err != nil {
			t.Fatal(err)
		}
		if out != "input processed by task 1" {
			t.Fatalf("unexpected task output: %s", info.Output)
		}
	}()

	func() {
		var out flow1Output
		info, err := client.RunFlowWait(t.Context(), "flow1", "input", RunFlowOpts{})
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
		if out.Step3 != "input processed by step 1 and by step 2 and by step 3" {
			t.Fatalf("unexpected flow output: %s", info.Output)
		}
	}()
}
