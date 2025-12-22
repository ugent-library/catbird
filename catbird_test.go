package catbird

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFlows(t *testing.T) {
	pool, err := pgxpool.New(t.Context(), os.Getenv("TEST_PG_CONN"))
	if err != nil {
		t.Fatal(err)
	}

	client := New(pool)

	task1 := NewTask("task1", func(ctx context.Context, in string) (string, error) {
		return "processed by task 1", nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	type step1Input struct {
		Flow string `json:"flow"`
	}
	type step2Input struct {
		Flow  string `json:"flow"`
		Steps struct {
			Step1 string `json:"step1"`
		} `json:"steps"`
	}

	step1 := NewTask("step1", func(ctx context.Context, in step1Input) (string, error) {
		return in.Flow + " processed by step 1", nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	step2 := NewTask("step2", func(ctx context.Context, in step2Input) (string, error) {
		return in.Steps.Step1 + " and by step 2", nil
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

	// the worker will also create the tasks, but we create them here explicitly to test CreateTask
	if err := client.CreateTask(t.Context(), step1); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTask(t.Context(), step2); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateFlow(t.Context(), flow1); err != nil {
		t.Fatal(err)
	}

	type flow2step1Input struct {
		Flow string `json:"flow"`
	}
	type flow2step2Input struct {
		// Flow  string `json:"flow"`
		Idx   int `json:"idx"`
		Steps struct {
			Step1 []string `json:"flow2step1"`
		} `json:"steps"`
	}
	type flow2step3Input struct {
		// Flow  string `json:"flow"`
		Steps struct {
			// Step1 []string `json:"flow2step1"`
			Step2 []string `json:"flow2step2"`
		} `json:"steps"`
	}

	flow2step1 := NewTask("flow2step1", func(ctx context.Context, in flow2step1Input) ([]string, error) {
		return strings.Split(in.Flow, ","), nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	flow2step2 := NewTask("flow2step2", func(ctx context.Context, in flow2step2Input) (string, error) {
		return strings.ToUpper(in.Steps.Step1[in.Idx]), nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	flow2step3 := NewTask("flow2step3", func(ctx context.Context, in flow2step3Input) (string, error) {
		return strings.Join(in.Steps.Step2, "|"), nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	flow2 := &Flow{
		Name: "flow2",
		Steps: []Step{
			{Name: "flow2step1"},
			{Name: "flow2step2", DependsOn: []string{"flow2step1"}, Map: "flow2step1"},
			{Name: "flow2step3", DependsOn: []string{"flow2step2"}},
		},
	}
	// the worker will also create the tasks, but we create them here explicitly to test CreateTask
	if err := client.CreateTask(t.Context(), flow2step1); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTask(t.Context(), flow2step2); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTask(t.Context(), flow2step3); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateFlow(t.Context(), flow2); err != nil {
		t.Fatal(err)
	}

	worker, err := client.NewWorker(WorkerOpts{
		Log:   slog.Default(),
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

	time.Sleep(1 * time.Second)

	id, err := client.RunTask(t.Context(), "task1", "", RunTaskOpts{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("task run id: %s", id)

	id, err = client.RunFlow(t.Context(), "flow1", "input")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("flow run id: %s", id)

	id, err = client.RunFlow(t.Context(), "flow2", "a,b,c")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("flow run id: %s", id)

	time.Sleep(10 * time.Second)
}
