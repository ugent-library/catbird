package catbird

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestTaskRetentionPeriodRoundTrip verifies that RetentionPeriod is persisted
// to cb_tasks and read back correctly via GetTask.
func TestTaskRetentionPeriodRoundTrip(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("retention_task_with_period").
		RetentionPeriod(7 * 24 * time.Hour).
		Do(func(ctx context.Context, in string) (string, error) {
			return in, nil
		})

	if err := client.CreateTask(t.Context(), task); err != nil {
		t.Fatal(err)
	}

	info, err := client.GetTask(t.Context(), "retention_task_with_period")
	if err != nil {
		t.Fatal(err)
	}
	if info.RetentionPeriod != 7*24*time.Hour {
		t.Fatalf("expected retention period %v, got %v", 7*24*time.Hour, info.RetentionPeriod)
	}
}

// TestTaskRetentionPeriodDefaultsToZero verifies that tasks without a
// RetentionPeriod report zero (no cleanup).
func TestTaskRetentionPeriodDefaultsToZero(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("retention_task_no_period").
		Do(func(ctx context.Context, in string) (string, error) {
			return in, nil
		})

	if err := client.CreateTask(t.Context(), task); err != nil {
		t.Fatal(err)
	}

	info, err := client.GetTask(t.Context(), "retention_task_no_period")
	if err != nil {
		t.Fatal(err)
	}
	if info.RetentionPeriod != 0 {
		t.Fatalf("expected zero retention period, got %v", info.RetentionPeriod)
	}
}

// TestPurgeTaskRuns verifies that PurgeTaskRuns deletes completed task runs
// older than the given duration. Passing 0 deletes all terminal runs.
func TestPurgeTaskRuns(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("retention_purge_task").
		Do(func(ctx context.Context, in string) (string, error) {
			return in + " done", nil
		})

	worker := client.NewWorker(t.Context()).AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "retention_purge_task", "input")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out string
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	// Confirm the run exists before purging.
	if _, err := client.GetTaskRun(t.Context(), "retention_purge_task", h.ID); err != nil {
		t.Fatalf("run should exist before purge: %v", err)
	}

	// Purge with duration=0: deletes all terminal runs finished before now().
	if _, err := client.PurgeTaskRuns(t.Context(), "retention_purge_task", 0); err != nil {
		t.Fatal(err)
	}

	// Run should no longer exist.
	_, err = client.GetTaskRun(t.Context(), "retention_purge_task", h.ID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after purge, got %v", err)
	}
}

// TestFlowRetentionPeriodRoundTrip verifies that RetentionPeriod is persisted
// to cb_flows and read back correctly via GetFlow.
func TestFlowRetentionPeriodRoundTrip(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "retention_flow_with_period")
	flow := NewFlow(flowName)
	flow.RetentionPeriod(30 * 24 * time.Hour)
	flow.AddStep(NewStep("step1").Do(func(ctx context.Context, in string) (string, error) {
		return in, nil
	}))

	if err := client.CreateFlow(t.Context(), flow); err != nil {
		t.Fatal(err)
	}

	info, err := client.GetFlow(t.Context(), flowName)
	if err != nil {
		t.Fatal(err)
	}
	if info.RetentionPeriod != 30*24*time.Hour {
		t.Fatalf("expected retention period %v, got %v", 30*24*time.Hour, info.RetentionPeriod)
	}
}

// TestFlowRetentionPeriodDefaultsToZero verifies that flows without a
// RetentionPeriod report zero (no cleanup).
func TestFlowRetentionPeriodDefaultsToZero(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "retention_flow_no_period")
	flow := NewFlow(flowName)
	flow.AddStep(NewStep("step1").Do(func(ctx context.Context, in string) (string, error) {
		return in, nil
	}))

	if err := client.CreateFlow(t.Context(), flow); err != nil {
		t.Fatal(err)
	}

	info, err := client.GetFlow(t.Context(), flowName)
	if err != nil {
		t.Fatal(err)
	}
	if info.RetentionPeriod != 0 {
		t.Fatalf("expected zero retention period, got %v", info.RetentionPeriod)
	}
}

// TestPurgeFlowRuns verifies that PurgeFlowRuns deletes completed flow runs
// older than the given duration. Passing 0 deletes all terminal runs.
func TestPurgeFlowRuns(t *testing.T) {
	client := getTestClient(t)

	flowName := testFlowName(t, "retention_purge_flow")
	flow := NewFlow(flowName)
	flow.AddStep(NewStep("step1").Do(func(ctx context.Context, in string) (string, error) {
		return in + " done", nil
	}))

	worker := client.NewWorker(t.Context()).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), flowName, "input")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out string
	if err := h.WaitForOutput(ctx, &out); err != nil {
		t.Fatal(err)
	}

	// Confirm the run exists before purging.
	if _, err := client.GetFlowRun(t.Context(), flowName, h.ID); err != nil {
		t.Fatalf("run should exist before purge: %v", err)
	}

	// Purge with duration=0: deletes all terminal runs finished before now().
	if _, err := client.PurgeFlowRuns(t.Context(), flowName, 0); err != nil {
		t.Fatal(err)
	}

	// Run should no longer exist.
	_, err = client.GetFlowRun(t.Context(), flowName, h.ID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after purge, got %v", err)
	}
}

// TestGCPurgesRetentionRuns verifies the end-to-end path: a task/flow configured
// with a 1ns retention period has its terminal runs deleted by GC.
func TestGCPurgesRetentionRuns(t *testing.T) {
	client := getTestClient(t)

	taskName := "retention_gc_task"
	task := NewTask(taskName).
		RetentionPeriod(time.Nanosecond). // effectively purge immediately
		Do(func(ctx context.Context, in string) (string, error) {
			return in, nil
		})

	flowName := testFlowName(t, "retention_gc_flow")
	flow := NewFlow(flowName)
	flow.RetentionPeriod(time.Nanosecond)
	flow.AddStep(NewStep("step1").Do(func(ctx context.Context, in string) (string, error) {
		return in, nil
	}))

	worker := client.NewWorker(t.Context()).AddTask(task).AddFlow(flow)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	th, err := client.RunTask(ctx, taskName, "input")
	if err != nil {
		t.Fatal(err)
	}
	fh, err := client.RunFlow(ctx, flowName, "input")
	if err != nil {
		t.Fatal(err)
	}

	var s string
	if err := th.WaitForOutput(ctx, &s); err != nil {
		t.Fatal(err)
	}
	if err := fh.WaitForOutput(ctx, &s); err != nil {
		t.Fatal(err)
	}

	// Small sleep to ensure finished_at is strictly before now().
	time.Sleep(10 * time.Millisecond)

	if _, err := GC(t.Context(), client.Conn); err != nil {
		t.Fatal(err)
	}

	if _, err := client.GetTaskRun(t.Context(), taskName, th.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected task run to be purged by GC, got %v", err)
	}
	if _, err := client.GetFlowRun(t.Context(), flowName, fh.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected flow run to be purged by GC, got %v", err)
	}
}
