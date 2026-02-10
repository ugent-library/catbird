package catbird

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestTaskCreate(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("test_task", func(ctx context.Context, in string) (string, error) {
		return in + " processed", nil
	})

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
}

func TestTaskRunAndWait(t *testing.T) {
	client := getTestClient(t)

	type TaskInput struct {
		Value int `json:"value"`
	}

	task := NewTask("math_task", func(ctx context.Context, in TaskInput) (int, error) {
		return in.Value * 2, nil
	})

	worker, err := client.NewWorker(t.Context(),
		WithTask(task),
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

	h, err := client.RunTask(t.Context(), "math_task", TaskInput{Value: 21})
	if err != nil {
		t.Fatal(err)
	}

	var result int
	if err := h.WaitForOutput(t.Context(), &result); err != nil {
		t.Fatal(err)
	}

	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestTaskPanicRecovery(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("panic_task", func(ctx context.Context, in string) (string, error) {
		panic("intentional panic in task")
	})

	worker, err := client.NewWorker(t.Context(),
		WithTask(task),
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
