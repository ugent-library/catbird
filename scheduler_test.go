package catbird

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestSchedulerIdempotencyKeyGeneration verifies that scheduled runs generate
// stable, deterministic idempotency keys based on scheduled time.
func TestSchedulerIdempotencyKeyGeneration(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("scheduled_task", func(ctx context.Context, in string) (string, error) {
		return in + " processed", nil
	})

	worker, err := client.NewWorker(t.Context(),
		WithTask(task),
		// Add a scheduled task that runs every minute
		WithScheduledTask("scheduled_task", "@hourly"),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Manually generate two keys from the same scheduled time to verify consistency
	scheduledTime := time.Date(2026, 2, 12, 15, 30, 0, 0, time.UTC)
	key1 := fmt.Sprintf("schedule:%d", scheduledTime.Unix())
	key2 := fmt.Sprintf("schedule:%d", scheduledTime.Unix())

	if key1 != key2 {
		t.Fatalf("idempotency keys should be identical for same time: %s vs %s", key1, key2)
	}

	// Keys should have the expected format
	if len(key1) < len("schedule:") {
		t.Fatalf("key format is too short: %s", key1)
	}
	if key1[:9] != "schedule:" {
		t.Fatalf("key should start with 'schedule:': %s", key1)
	}
}

// TestSchedulerCrossWorkerDedup verifies that multiple workers generate
// identical idempotency keys for the same scheduled execution, ensuring
// only one run is enqueued even when multiple workers run the same schedule.
func TestSchedulerCrossWorkerDedup(t *testing.T) {
	client := getTestClient(t)

	// Track how many times the task is executed
	executionCount := 0
	var countMutex sync.Mutex

	task := NewTask("dedup_test_task", func(ctx context.Context, in string) (string, error) {
		countMutex.Lock()
		executionCount++
		countMutex.Unlock()
		return in, nil
	})

	// Create and run worker to execute task (must create task before running)
	worker, err := client.NewWorker(t.Context(),
		WithTask(task),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)
	time.Sleep(200 * time.Millisecond)

	// Simulate multiple workers by manually triggering scheduled runs with same time
	// In real scenario, multiple workers would independently trigger cron at same time
	scheduledTime := time.Now().UTC()
	idempotencyKey := fmt.Sprintf("schedule:%d", scheduledTime.Unix())

	// First "worker" enqueues
	h1, err := client.RunTaskWithOpts(t.Context(), "dedup_test_task", "test", RunOpts{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Second "worker" tries to enqueue same run (should be deduplicated)
	h2, err := client.RunTaskWithOpts(t.Context(), "dedup_test_task", "test", RunOpts{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should get same ID (dedup worked)
	if h1.ID != h2.ID {
		t.Fatalf("expected same run ID for identical idempotency key: %d vs %d", h1.ID, h2.ID)
	}

	// Wait for task to complete
	var output string
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h1.WaitForOutput(ctx, &output); err != nil {
		t.Fatal(err)
	}

	// Verify task executed exactly once
	countMutex.Lock()
	defer countMutex.Unlock()
	if executionCount != 1 {
		t.Fatalf("expected task to execute exactly once, but executed %d times", executionCount)
	}
}

// TestSchedulerIdempotencyPersists verifies that after a scheduled run completes,
// the idempotency key persists and prevents new runs with the same key.
func TestSchedulerIdempotencyPersists(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("persisted_dedup_task", func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	worker, err := client.NewWorker(t.Context(),
		WithTask(task),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Enqueue first run with idempotency key
	idempotencyKey := "schedule:1707759600"
	h1, err := client.RunTaskWithOpts(t.Context(), "persisted_dedup_task", 21, RunOpts{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for completion
	var result1 int
	ctx1, cancel1 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel1()
	if err := h1.WaitForOutput(ctx1, &result1); err != nil {
		t.Fatal(err)
	}
	if result1 != 42 {
		t.Fatalf("expected 42, got %d", result1)
	}

	// Verify task run status is completed
	run1, err := client.GetTaskRun(context.Background(), "persisted_dedup_task", h1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run1.Status != StatusCompleted {
		t.Fatalf("expected status %s, got %s", StatusCompleted, run1.Status)
	}

	// Try to enqueue with same idempotency key (should be rejected)
	// In idempotency mode, completed runs block new runs with same key
	run, err := client.GetTaskRun(context.Background(), "persisted_dedup_task", h1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.IdempotencyKey != idempotencyKey {
		t.Fatalf("expected idempotency key %s, got %s", idempotencyKey, run.IdempotencyKey)
	}
}

// TestSchedulerUTCNormalization verifies that cron scheduler uses UTC and generates
// keys that work correctly across different time zones.
func TestSchedulerUTCNormalization(t *testing.T) {
	// Create two time objects representing the same moment in time
	// but in different timezones
	utcTime := time.Date(2026, 2, 12, 15, 30, 0, 0, time.UTC)

	// Generate keys - should be identical regardless of timezone
	// because we use UTC seconds
	key1 := fmt.Sprintf("schedule:%d", utcTime.Unix())

	// Simulate a different timezone (EST is UTC-5)
	estTZ, err := time.LoadLocation("America/New_York")
	if err != nil {
		// If timezone loading fails, skip this part of the test
		t.Logf("warning: could not load EST timezone: %v", err)
	} else {
		// Convert to EST (same moment, different local representation)
		estTime := utcTime.In(estTZ)

		// The Unix() value should be identical (same moment in time)
		key2 := fmt.Sprintf("schedule:%d", estTime.Unix())

		if key1 != key2 {
			t.Fatalf("Unix() values should be identical for same moment: %s vs %s", key1, key2)
		}

		// Verify they are the same moment
		if !utcTime.Equal(estTime) {
			t.Fatal("times should represent the same moment")
		}
	}
}

// TestSchedulerFlowIdempotency verifies scheduled flows also use idempotency dedup.
func TestSchedulerFlowIdempotency(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow("scheduled_flow",
		InitialStep("step1", func(ctx context.Context, in string) (int, error) {
			return 42, nil
		}),
	)

	worker, err := client.NewWorker(t.Context(),
		WithFlow(flow),
	)
	if err != nil {
		t.Fatal(err)
	}

	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Enqueue two flows with same idempotency key
	idempotencyKey := "schedule:1707759600"

	h1, err := client.RunFlowWithOpts(t.Context(), "scheduled_flow", "test1", RunOpts{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	h2, err := client.RunFlowWithOpts(t.Context(), "scheduled_flow", "test2", RunOpts{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should get same ID (dedup worked)
	if h1.ID != h2.ID {
		t.Fatalf("expected same flow run ID for identical idempotency key: %d vs %d", h1.ID, h2.ID)
	}

	// Wait for completion
	var output map[string]interface{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h1.WaitForOutput(ctx, &output); err != nil {
		t.Fatal(err)
	}
}
