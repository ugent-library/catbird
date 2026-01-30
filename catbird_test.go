package catbird

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var testClient *Client
var testOnce sync.Once

func getTestClient(t *testing.T) *Client {
	testOnce.Do(func() {
		dsn := os.Getenv("CB_CONN")

		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		MigrateDownTo(t.Context(), db, 0)
		MigrateUpTo(t.Context(), db, SchemaVersion)

		pool, err := pgxpool.New(t.Context(), dsn)
		if err != nil {
			t.Fatal(err)
		}

		testClient = New(pool)
	})

	return testClient
}

func TestQueues(t *testing.T) {
	client := getTestClient(t)

	err := client.CreateQueueWithOpts(t.Context(), "test_queue", QueueOpts{
		Topics: []string{"topic1", "topic2"},
	})
	if err != nil {
		t.Fatal(err)
	}

	info, err := client.GetQueue(t.Context(), "test_queue")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "test_queue" {
		t.Fatalf("unexpected queue name: %s", info.Name)
	}
	if len(info.Topics) != 2 || info.Topics[0] != "topic1" || info.Topics[1] != "topic2" {
		t.Fatalf("unexpected queue topics: %v", info.Topics)
	}
}

func TestFlows(t *testing.T) {
	client := getTestClient(t)

	type Task1Input struct {
		Str string `json:"str"`
	}

	task1 := NewTask("task1", TaskHandler(func(ctx context.Context, in Task1Input) (string, error) {
		return in.Str + " processed by task 1", nil
	}))

	type flow1Output struct {
		Step1 string `json:"step1"`
		Step2 string `json:"step2"`
		Step3 string `json:"step3"`
	}

	type step1Input struct {
		FlowInput string `json:"flow_input"`
	}

	type step2Input struct {
		FlowInput string `json:"flow_input"`
		Step1     string `json:"step1"`
	}

	type step3Input struct {
		FlowInput string `json:"flow_input"`
		Step2     string `json:"step2"`
	}

	flow1 := NewFlow("flow1")
	flow1.AddStep("step1", StepHandler(func(ctx context.Context, in step1Input) (string, error) {
		return in.FlowInput + " processed by step 1", nil
	}))
	flow1.AddStep("step2", DependsOn("step1"), StepHandler(func(ctx context.Context, in step2Input) (string, error) {
		return in.Step1 + " and by step 2", nil
	}))
	flow1.AddStep("step3", DependsOn("step2"), StepHandler(func(ctx context.Context, in step3Input) (string, error) {
		return in.Step2 + " and by step 3", nil
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	go func() {
		err := client.StartWorker(t.Context(),
			WithLogger(logger),
			WithTask(task1),
			WithFlow(flow1),
		)
		if err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	func() {
		h, err := client.RunTask(t.Context(), "task1", Task1Input{Str: "input"})
		if err != nil {
			t.Fatal(err)
		}
		var out string
		if err := h.WaitForOutput(t.Context(), &out); err != nil {
			t.Fatal(err)
		}
		if out != "input processed by task 1" {
			t.Fatalf("unexpected task output: %s", out)
		}
	}()

	func() {
		h, err := client.RunFlow(t.Context(), "flow1", "input")
		if err != nil {
			t.Fatal(err)
		}
		var out flow1Output
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
	}()
}
