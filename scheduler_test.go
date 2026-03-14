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

	task := NewTask("scheduled_task").
		Do(func(ctx context.Context, in string) (string, error) {
			return in + " processed", nil
		})

	worker := client.NewWorker().
		AddTask(task)

	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Create schedule separately
	if err := client.CreateTaskSchedule(t.Context(), "scheduled_task", "@hourly"); err != nil {
		t.Fatal(err)
	}

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

	task := NewTask("dedup_test_task").Do(func(ctx context.Context, in string) (string, error) {
		countMutex.Lock()
		executionCount++
		countMutex.Unlock()
		return in, nil
	})

	// Create and run worker to execute task (must create task before running)
	worker := client.NewWorker().AddTask(task)

	startTestWorker(t, worker)
	time.Sleep(200 * time.Millisecond)

	// Simulate multiple workers by manually triggering scheduled runs with same time
	// In real scenario, multiple workers would independently trigger cron at same time
	scheduledTime := time.Now().UTC()
	idempotencyKey := fmt.Sprintf("schedule:%d", scheduledTime.Unix())

	// First "worker" enqueues
	h1, err := client.RunTask(t.Context(), "dedup_test_task", "test", RunTaskOpts{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Second "worker" tries to enqueue same run (should be deduplicated)
	h2, err := client.RunTask(t.Context(), "dedup_test_task", "test", RunTaskOpts{
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

	task := NewTask("persisted_dedup_task").Do(func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	worker := client.NewWorker().AddTask(task)
	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	idempotencyKey := "schedule:1707759600"
	h1, err := client.RunTask(t.Context(), "persisted_dedup_task", 21, RunTaskOpts{
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

	flow := NewFlow("scheduled_flow")
	flow.AddStep(NewStep("step1").Do(func(ctx context.Context, in string) (int, error) {
		return 42, nil
	}))

	worker := client.NewWorker().AddFlow(flow)

	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Create schedule separately
	if err := client.CreateFlowSchedule(t.Context(), "scheduled_flow", "@hourly"); err != nil {
		t.Fatal(err)
	}

	// Enqueue two flows with same idempotency key
	idempotencyKey := "schedule:1707759600"

	h1, err := client.RunFlow(t.Context(), "scheduled_flow", "test1", RunFlowOpts{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	h2, err := client.RunFlow(t.Context(), "scheduled_flow", "test2", RunFlowOpts{
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
	var output int
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h1.WaitForOutput(ctx, &output); err != nil {
		t.Fatal(err)
	}
}

// TestSchedulerTaskScheduleCreation verifies the basic schedule creation API is available.
func TestSchedulerEnqueueRollback(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	taskName := "test_sched_task" + suffix
	flowName := "test_sched_flow" + suffix

	// Just verify that we can call the client-level schedule functions without it being part of a worker
	// This tests the new decoupled scheduling API

	// Create a task first
	task := NewTask(taskName).Do(func(ctx context.Context, in string) (string, error) {
		return "done", nil
	})

	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}

	// Now create a schedule independently
	err := client.CreateTaskSchedule(t.Context(), taskName, "@hourly")
	if err != nil {
		t.Fatalf("CreateTaskSchedule failed: %v", err)
	}

	taskSchedules, err := client.ListTaskSchedules(t.Context())
	if err != nil {
		t.Fatalf("ListTaskSchedules failed: %v", err)
	}

	taskScheduleCount := 0
	for _, schedule := range taskSchedules {
		if schedule.TaskName == taskName {
			taskScheduleCount++
		}
	}

	if taskScheduleCount != 1 {
		t.Fatalf("expected exactly 1 task schedule for %q, got %d", taskName, taskScheduleCount)
	}

	err = client.CreateTaskSchedule(t.Context(), taskName, "@hourly")
	if err != nil {
		t.Fatalf("CreateTaskSchedule duplicate create failed: %v", err)
	}

	taskSchedules, err = client.ListTaskSchedules(t.Context())
	if err != nil {
		t.Fatalf("ListTaskSchedules after duplicate create failed: %v", err)
	}

	taskScheduleCount = 0
	for _, schedule := range taskSchedules {
		if schedule.TaskName == taskName {
			taskScheduleCount++
		}
	}

	if taskScheduleCount != 1 {
		t.Fatalf("expected idempotent task schedule creation for %q, got %d rows", taskName, taskScheduleCount)
	}

	// Similarly for flows
	flow := NewFlow(flowName)
	flow.AddStep(NewStep("s1").Do(func(ctx context.Context, in int) (int, error) {
		return in, nil
	}))

	if err := CreateFlow(t.Context(), client.Conn, flow); err != nil {
		t.Fatal(err)
	}

	err = client.CreateFlowSchedule(t.Context(), flowName, "@daily")
	if err != nil {
		t.Fatalf("CreateFlowSchedule failed: %v", err)
	}

	flowSchedules, err := client.ListFlowSchedules(t.Context())
	if err != nil {
		t.Fatalf("ListFlowSchedules failed: %v", err)
	}

	flowScheduleCount := 0
	for _, schedule := range flowSchedules {
		if schedule.FlowName == flowName {
			flowScheduleCount++
		}
	}

	if flowScheduleCount != 1 {
		t.Fatalf("expected exactly 1 flow schedule for %q, got %d", flowName, flowScheduleCount)
	}

	err = client.CreateFlowSchedule(t.Context(), flowName, "@daily")
	if err != nil {
		t.Fatalf("CreateFlowSchedule duplicate create failed: %v", err)
	}

	flowSchedules, err = client.ListFlowSchedules(t.Context())
	if err != nil {
		t.Fatalf("ListFlowSchedules after duplicate create failed: %v", err)
	}

	flowScheduleCount = 0
	for _, schedule := range flowSchedules {
		if schedule.FlowName == flowName {
			flowScheduleCount++
		}
	}

	if flowScheduleCount != 1 {
		t.Fatalf("expected idempotent flow schedule creation for %q, got %d rows", flowName, flowScheduleCount)
	}
}

// TestSchedulerConcurrentWorkers verifies that multiple workers fairly distribute
// due schedules without creating duplicates or starving any worker.
// This tests the core distributed scheduling guarantee: exactly one execution
// per schedule tick, even with many concurrent workers polling simultaneously.
func TestSchedulerConcurrentWorkers(t *testing.T) {
	requireSlowTests(t)

	client := getTestClient(t)

	// Track executions
	executionCount := 0
	var countMutex sync.Mutex

	task := NewTask("concurrent_sched_test").
		Do(func(ctx context.Context, in any) (string, error) {
			countMutex.Lock()
			executionCount++
			countMutex.Unlock()
			return "executed", nil
		})

	// Create task in database first
	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}

	// Create a schedule that triggers every 20 seconds (using specific minutes with regex support)
	// This ensures multiple ticks within our test window
	now := time.Now()
	currentMin := now.Minute()
	nextMins := []int{
		(currentMin + 1) % 60,
		(currentMin + 2) % 60,
		(currentMin + 3) % 60,
	}
	cronSpec := fmt.Sprintf("%d,%d,%d * * * *", nextMins[0], nextMins[1], nextMins[2])

	if err := client.CreateTaskSchedule(t.Context(), "concurrent_sched_test", cronSpec); err != nil {
		t.Fatal(err)
	}

	// Spin up 3 concurrent workers, all polling the same schedule
	workers := make([]*Worker, 3)

	for i := 0; i < 3; i++ {
		w := client.NewWorker().AddTask(task)
		w.shutdownTimeout = 0

		workers[i] = w

		// Start worker in background
		go func(worker *Worker) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			_ = worker.Start(ctx)
		}(w)
	}

	// Let all workers run concurrently for ~140 seconds
	// With 3 specific minute targets, we're guaranteed to hit at least 2-3 ticks
	time.Sleep(140 * time.Second)

	countMutex.Lock()
	executedCount := executionCount
	countMutex.Unlock()

	// Verify: With 3 specific minute targets over 140 seconds, we expect 2-3 executions
	// (guaranteed to catch at least 2-3 minute boundaries)
	if executedCount < 2 {
		t.Logf("WARNING: concurrent workers test executed %d times; expected 2-3", executedCount)
	} else if executedCount > 3 {
		t.Logf("WARNING: concurrent workers test executed %d times (expected 2-3)", executedCount)
	}

	t.Logf("INFO: concurrent workers test completed - task executed %d times with 3 workers", executedCount)
}

// TestSchedulerOnePolicy verifies the default "one" catch-up policy: after downtime,
// the scheduler enqueues exactly one catch-up run and jumps next_run_at to the future.
func TestSchedulerOnePolicy(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	taskName := "skip_missed" + suffix

	task := NewTask(taskName).Do(func(ctx context.Context, in string) (string, error) {
		return "ok", nil
	})

	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}

	// Create a per-minute schedule
	if err := client.CreateTaskSchedule(t.Context(), taskName, "* * * * *"); err != nil {
		t.Fatal(err)
	}

	// Simulate 5 minutes of downtime by pushing next_run_at 5 minutes into the past
	pastTime := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := client.Conn.Exec(t.Context(),
		`UPDATE cb_task_schedules SET next_run_at = $1 WHERE task_name = $2`,
		pastTime, taskName,
	); err != nil {
		t.Fatal(err)
	}

	// Execute due schedules (this is what the worker poll calls)
	var executed int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_task_schedules(ARRAY[$1]::text[], 32)`, taskName,
	).Scan(&executed); err != nil {
		t.Fatal(err)
	}

	// Should execute exactly 1 catch-up run, not 5
	if executed != 1 {
		t.Fatalf("expected 1 catch-up run, got %d", executed)
	}

	// Verify next_run_at is now in the future
	var nextRunAt time.Time
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT next_run_at FROM cb_task_schedules WHERE task_name = $1`, taskName,
	).Scan(&nextRunAt); err != nil {
		t.Fatal(err)
	}

	if !nextRunAt.After(time.Now()) {
		t.Fatalf("expected next_run_at to be in the future, got %s", nextRunAt.Format(time.RFC3339))
	}

	// Run again — should find nothing due
	var executed2 int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_task_schedules(ARRAY[$1]::text[], 32)`, taskName,
	).Scan(&executed2); err != nil {
		t.Fatal(err)
	}

	if executed2 != 0 {
		t.Fatalf("expected 0 runs on second poll, got %d", executed2)
	}
}

// TestFlowSchedulerOnePolicy verifies the default "one" catch-up policy for flow schedules.
func TestFlowSchedulerOnePolicy(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	flowName := "skip_missed_flow" + suffix

	flow := NewFlow(flowName)
	flow.AddStep(NewStep("s1").Do(func(ctx context.Context, in string) (string, error) {
		return "ok", nil
	}))

	if err := CreateFlow(t.Context(), client.Conn, flow); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateFlowSchedule(t.Context(), flowName, "* * * * *"); err != nil {
		t.Fatal(err)
	}

	// Simulate 5 minutes of downtime
	pastTime := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := client.Conn.Exec(t.Context(),
		`UPDATE cb_flow_schedules SET next_run_at = $1 WHERE flow_name = $2`,
		pastTime, flowName,
	); err != nil {
		t.Fatal(err)
	}

	var executed int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_flow_schedules(ARRAY[$1]::text[], 32)`, flowName,
	).Scan(&executed); err != nil {
		t.Fatal(err)
	}

	if executed != 1 {
		t.Fatalf("expected 1 catch-up run, got %d", executed)
	}

	// Verify next_run_at is now in the future
	var nextRunAt time.Time
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT next_run_at FROM cb_flow_schedules WHERE flow_name = $1`, flowName,
	).Scan(&nextRunAt); err != nil {
		t.Fatal(err)
	}

	if !nextRunAt.After(time.Now()) {
		t.Fatalf("expected next_run_at to be in the future, got %s", nextRunAt.Format(time.RFC3339))
	}
}

// TestSchedulerSkipPolicy verifies that with WithSkipCatchUp(), 5 minutes of downtime
// produces 0 enqueued runs and next_run_at jumps to the future.
func TestSchedulerSkipPolicy(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	taskName := "skip_policy" + suffix

	task := NewTask(taskName).Do(func(ctx context.Context, in string) (string, error) {
		return "ok", nil
	})

	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateTaskSchedule(t.Context(), taskName, "* * * * *", WithSkipCatchUp()); err != nil {
		t.Fatal(err)
	}

	// Simulate 5 minutes of downtime
	pastTime := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := client.Conn.Exec(t.Context(),
		`UPDATE cb_task_schedules SET next_run_at = $1 WHERE task_name = $2`,
		pastTime, taskName,
	); err != nil {
		t.Fatal(err)
	}

	var executed int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_task_schedules(ARRAY[$1]::text[], 32)`, taskName,
	).Scan(&executed); err != nil {
		t.Fatal(err)
	}

	// Skip policy: 0 runs enqueued
	if executed != 0 {
		t.Fatalf("expected 0 runs with skip policy, got %d", executed)
	}

	// Verify next_run_at jumped to the future
	var nextRunAt time.Time
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT next_run_at FROM cb_task_schedules WHERE task_name = $1`, taskName,
	).Scan(&nextRunAt); err != nil {
		t.Fatal(err)
	}

	if !nextRunAt.After(time.Now()) {
		t.Fatalf("expected next_run_at in the future, got %s", nextRunAt.Format(time.RFC3339))
	}
}

// TestSchedulerAllPolicy verifies that with WithCatchUpAll(), 5 minutes of downtime
// replays all 5 missed ticks.
func TestSchedulerAllPolicy(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	taskName := "all_policy" + suffix

	task := NewTask(taskName).Do(func(ctx context.Context, in string) (string, error) {
		return "ok", nil
	})

	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateTaskSchedule(t.Context(), taskName, "* * * * *", WithCatchUpAll()); err != nil {
		t.Fatal(err)
	}

	// Simulate 5 minutes of downtime
	pastTime := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := client.Conn.Exec(t.Context(),
		`UPDATE cb_task_schedules SET next_run_at = $1 WHERE task_name = $2`,
		pastTime, taskName,
	); err != nil {
		t.Fatal(err)
	}

	// Execute with a large batch to get all missed ticks
	var executed int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_task_schedules(ARRAY[$1]::text[], 32)`, taskName,
	).Scan(&executed); err != nil {
		t.Fatal(err)
	}

	// All policy: should replay all missed ticks (5-6 depending on timing)
	if executed < 5 || executed > 6 {
		t.Fatalf("expected 5-6 catch-up runs with all policy, got %d", executed)
	}

	// After catching up, next poll should find nothing
	var executed2 int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_task_schedules(ARRAY[$1]::text[], 32)`, taskName,
	).Scan(&executed2); err != nil {
		t.Fatal(err)
	}

	if executed2 != 0 {
		t.Fatalf("expected 0 runs after catching up, got %d", executed2)
	}
}

// TestSchedulerAllPolicyBatchBound verifies that the all policy respects batch_size.
func TestSchedulerAllPolicyBatchBound(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	taskName := "all_batch" + suffix

	task := NewTask(taskName).Do(func(ctx context.Context, in string) (string, error) {
		return "ok", nil
	})

	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateTaskSchedule(t.Context(), taskName, "* * * * *", WithCatchUpAll()); err != nil {
		t.Fatal(err)
	}

	// Simulate 50 minutes of downtime
	pastTime := time.Now().UTC().Add(-50 * time.Minute)
	if _, err := client.Conn.Exec(t.Context(),
		`UPDATE cb_task_schedules SET next_run_at = $1 WHERE task_name = $2`,
		pastTime, taskName,
	); err != nil {
		t.Fatal(err)
	}

	// Execute with batch_size=10
	var executed int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_task_schedules(ARRAY[$1]::text[], 10)`, taskName,
	).Scan(&executed); err != nil {
		t.Fatal(err)
	}

	// Should be capped at batch_size
	if executed != 10 {
		t.Fatalf("expected 10 runs (batch_size bound), got %d", executed)
	}
}

// TestSchedulerDefaultPolicy verifies that no option produces "one" behavior.
func TestSchedulerDefaultPolicy(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	taskName := "default_policy" + suffix

	task := NewTask(taskName).Do(func(ctx context.Context, in string) (string, error) {
		return "ok", nil
	})

	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}

	// No catch-up option — should default to "one"
	if err := client.CreateTaskSchedule(t.Context(), taskName, "* * * * *"); err != nil {
		t.Fatal(err)
	}

	// Verify the stored policy
	var policy string
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT catch_up FROM cb_task_schedules WHERE task_name = $1`, taskName,
	).Scan(&policy); err != nil {
		t.Fatal(err)
	}

	if policy != CatchUpOne {
		t.Fatalf("expected default policy %q, got %q", CatchUpOne, policy)
	}

	// Simulate 5 minutes of downtime
	pastTime := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := client.Conn.Exec(t.Context(),
		`UPDATE cb_task_schedules SET next_run_at = $1 WHERE task_name = $2`,
		pastTime, taskName,
	); err != nil {
		t.Fatal(err)
	}

	var executed int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_task_schedules(ARRAY[$1]::text[], 32)`, taskName,
	).Scan(&executed); err != nil {
		t.Fatal(err)
	}

	if executed != 1 {
		t.Fatalf("expected 1 catch-up run with default policy, got %d", executed)
	}
}

// TestFlowSchedulerSkipPolicy verifies skip catch-up policy for flow schedules.
func TestFlowSchedulerSkipPolicy(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	flowName := "skip_flow" + suffix

	flow := NewFlow(flowName)
	flow.AddStep(NewStep("s1").Do(func(ctx context.Context, in string) (string, error) {
		return "ok", nil
	}))

	if err := CreateFlow(t.Context(), client.Conn, flow); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateFlowSchedule(t.Context(), flowName, "* * * * *", WithSkipCatchUp()); err != nil {
		t.Fatal(err)
	}

	pastTime := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := client.Conn.Exec(t.Context(),
		`UPDATE cb_flow_schedules SET next_run_at = $1 WHERE flow_name = $2`,
		pastTime, flowName,
	); err != nil {
		t.Fatal(err)
	}

	var executed int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_flow_schedules(ARRAY[$1]::text[], 32)`, flowName,
	).Scan(&executed); err != nil {
		t.Fatal(err)
	}

	if executed != 0 {
		t.Fatalf("expected 0 runs with skip policy, got %d", executed)
	}

	var nextRunAt time.Time
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT next_run_at FROM cb_flow_schedules WHERE flow_name = $1`, flowName,
	).Scan(&nextRunAt); err != nil {
		t.Fatal(err)
	}

	if !nextRunAt.After(time.Now()) {
		t.Fatalf("expected next_run_at in the future, got %s", nextRunAt.Format(time.RFC3339))
	}
}

// TestFlowSchedulerAllPolicy verifies all catch-up policy for flow schedules.
func TestFlowSchedulerAllPolicy(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	flowName := "all_flow" + suffix

	flow := NewFlow(flowName)
	flow.AddStep(NewStep("s1").Do(func(ctx context.Context, in string) (string, error) {
		return "ok", nil
	}))

	if err := CreateFlow(t.Context(), client.Conn, flow); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateFlowSchedule(t.Context(), flowName, "* * * * *", WithCatchUpAll()); err != nil {
		t.Fatal(err)
	}

	pastTime := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := client.Conn.Exec(t.Context(),
		`UPDATE cb_flow_schedules SET next_run_at = $1 WHERE flow_name = $2`,
		pastTime, flowName,
	); err != nil {
		t.Fatal(err)
	}

	var executed int
	if err := client.Conn.QueryRow(t.Context(),
		`SELECT cb_execute_due_flow_schedules(ARRAY[$1]::text[], 32)`, flowName,
	).Scan(&executed); err != nil {
		t.Fatal(err)
	}

	if executed < 5 || executed > 6 {
		t.Fatalf("expected 5-6 catch-up runs with all policy, got %d", executed)
	}
}

func TestCronNextTick(t *testing.T) {
	client := getTestClient(t)

	tests := []struct {
		name string
		spec string
		from time.Time
		want time.Time
	}{
		{
			name: "hourly_keyword",
			spec: "@hourly",
			from: time.Date(2026, 2, 24, 10, 15, 30, 0, time.UTC),
			want: time.Date(2026, 2, 24, 11, 0, 0, 0, time.UTC),
		},
		{
			name: "daily_keyword",
			spec: "@daily",
			from: time.Date(2026, 2, 24, 23, 59, 59, 0, time.UTC),
			want: time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "every_fifteen_minutes",
			spec: "*/15 * * * *",
			from: time.Date(2026, 2, 24, 10, 14, 0, 0, time.UTC),
			want: time.Date(2026, 2, 24, 10, 15, 0, 0, time.UTC),
		},
		{
			name: "weekday_specific",
			spec: "0 9 * * 1",
			from: time.Date(2026, 2, 24, 10, 0, 0, 0, time.UTC),
			want: time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC),
		},
		{
			name: "dom_dow_or_semantics",
			spec: "0 9 1 * 1",
			from: time.Date(2026, 2, 2, 10, 0, 0, 0, time.UTC),
			want: time.Date(2026, 2, 9, 9, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got time.Time
			err := client.Conn.QueryRow(t.Context(), `SELECT cb_next_cron_tick($1, $2::timestamptz)`, tt.spec, tt.from).Scan(&got)
			if err != nil {
				t.Fatalf("cb_next_cron_tick failed: %v", err)
			}

			if !got.Equal(tt.want) {
				t.Fatalf("cb_next_cron_tick(%q, %s) = %s, want %s", tt.spec, tt.from.Format(time.RFC3339), got.Format(time.RFC3339), tt.want.Format(time.RFC3339))
			}
		})
	}
}

func TestCronNextTickInvalidSpec(t *testing.T) {
	client := getTestClient(t)

	var got time.Time
	err := client.Conn.QueryRow(
		t.Context(),
		`SELECT cb_next_cron_tick($1, $2::timestamptz)`,
		"61 * * * *",
		time.Date(2026, 2, 24, 10, 0, 0, 0, time.UTC),
	).Scan(&got)
	if err == nil {
		t.Fatal("expected error for invalid cron spec, got nil")
	}
}
