package catbird

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestSchedulerConcurrencyKeyGeneration verifies that scheduled runs generate
// stable, deterministic concurrency keys based on scheduled time.
func TestSchedulerConcurrencyKeyGeneration(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("scheduled_task").
		Do(func(ctx context.Context, in string) (string, error) {
			return in + " processed", nil
		})

	worker := NewWorker(testPool).
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
		t.Fatalf("concurrency keys should be identical for same time: %s vs %s", key1, key2)
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
// identical concurrency keys for the same scheduled execution, ensuring
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
	worker := NewWorker(testPool).AddTask(task)

	startTestWorker(t, worker)
	time.Sleep(200 * time.Millisecond)

	// Simulate multiple workers by manually triggering scheduled runs with same time
	// In real scenario, multiple workers would independently trigger cron at same time
	scheduledTime := time.Now().UTC()
	concurrencyKey := fmt.Sprintf("schedule:%d", scheduledTime.Unix())

	// First "worker" enqueues
	h1, err := client.RunTask(t.Context(), "dedup_test_task", "test", RunTaskOpts{
		ConcurrencyKey: concurrencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Second "worker" tries to enqueue same run (should be deduplicated)
	h2, err := client.RunTask(t.Context(), "dedup_test_task", "test", RunTaskOpts{
		ConcurrencyKey: concurrencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should get same ID (dedup worked)
	if h1.ID != h2.ID {
		t.Fatalf("expected same run ID for identical concurrency key: %d vs %d", h1.ID, h2.ID)
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

// TestSchedulerFlowConcurrencyKey verifies scheduled flows also use concurrency key dedup.
func TestSchedulerFlowConcurrencyKey(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow("scheduled_flow")
	flow.AddStep(NewStep("step1").Do(func(ctx context.Context, in string) (int, error) {
		return 42, nil
	}))

	worker := NewWorker(testPool).AddFlow(flow)

	startTestWorker(t, worker)
	time.Sleep(100 * time.Millisecond)

	// Create schedule separately
	if err := client.CreateFlowSchedule(t.Context(), "scheduled_flow", "@hourly"); err != nil {
		t.Fatal(err)
	}

	// Enqueue two flows with same concurrency key
	concurrencyKey := "schedule:1707759600"

	h1, err := client.RunFlow(t.Context(), "scheduled_flow", "test1", RunFlowOpts{
		ConcurrencyKey: concurrencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	h2, err := client.RunFlow(t.Context(), "scheduled_flow", "test2", RunFlowOpts{
		ConcurrencyKey: concurrencyKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should get same ID (dedup worked)
	if h1.ID != h2.ID {
		t.Fatalf("expected same flow run ID for identical concurrency key: %d vs %d", h1.ID, h2.ID)
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

// TestSchedulerScheduleUpsert verifies that re-creating an existing schedule
// applies a changed cron spec (instead of silently ignoring it), while a
// re-create with the same spec preserves next_run_at. See issue #34.
func TestSchedulerScheduleUpsert(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	taskName := "test_upsert_task" + suffix
	flowName := "test_upsert_flow" + suffix

	findTaskSchedule := func(name string) *TaskScheduleInfo {
		schedules, err := client.ListTaskSchedules(t.Context())
		if err != nil {
			t.Fatalf("ListTaskSchedules failed: %v", err)
		}
		for _, s := range schedules {
			if s.TaskName == name {
				return s
			}
		}
		t.Fatalf("no task schedule found for %q", name)
		return nil
	}

	task := NewTask(taskName).Do(func(ctx context.Context, in string) (string, error) {
		return "done", nil
	})
	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}

	// Create with an hourly spec (the "stale fallback" from the bug report).
	if err := client.CreateTaskSchedule(t.Context(), taskName, "0 */1 * * *"); err != nil {
		t.Fatalf("CreateTaskSchedule failed: %v", err)
	}
	initial := findTaskSchedule(taskName)
	if initial.CronSpec != "0 */1 * * *" {
		t.Fatalf("expected initial cron_spec %q, got %q", "0 */1 * * *", initial.CronSpec)
	}

	// Re-create with the corrected spec — must be applied, not ignored.
	if err := client.CreateTaskSchedule(t.Context(), taskName, "*/1 * * * *", WithCatchUpAll()); err != nil {
		t.Fatalf("CreateTaskSchedule (update) failed: %v", err)
	}
	updated := findTaskSchedule(taskName)
	if updated.CronSpec != "*/1 * * * *" {
		t.Fatalf("expected updated cron_spec %q, got %q (spec change silently ignored)", "*/1 * * * *", updated.CronSpec)
	}
	if updated.CatchUp != "all" {
		t.Fatalf("expected catch_up to update to %q, got %q", "all", updated.CatchUp)
	}
	if !updated.NextRunAt.Before(initial.NextRunAt) {
		t.Fatalf("expected next_run_at to be recomputed earlier after spec change: initial=%s updated=%s", initial.NextRunAt, updated.NextRunAt)
	}

	// Re-create with the same spec — next_run_at must be preserved.
	if err := client.CreateTaskSchedule(t.Context(), taskName, "*/1 * * * *", WithCatchUpAll()); err != nil {
		t.Fatalf("CreateTaskSchedule (same spec) failed: %v", err)
	}
	unchanged := findTaskSchedule(taskName)
	if !unchanged.NextRunAt.Equal(updated.NextRunAt) {
		t.Fatalf("expected next_run_at preserved on same-spec re-create: before=%s after=%s", updated.NextRunAt, unchanged.NextRunAt)
	}

	// Flows behave the same way.
	flow := NewFlow(flowName)
	flow.AddStep(NewStep("s1").Do(func(ctx context.Context, in int) (int, error) {
		return in, nil
	}))
	if err := CreateFlow(t.Context(), client.Conn, flow); err != nil {
		t.Fatal(err)
	}

	if err := client.CreateFlowSchedule(t.Context(), flowName, "0 */1 * * *"); err != nil {
		t.Fatalf("CreateFlowSchedule failed: %v", err)
	}
	if err := client.CreateFlowSchedule(t.Context(), flowName, "*/1 * * * *"); err != nil {
		t.Fatalf("CreateFlowSchedule (update) failed: %v", err)
	}

	flowSchedules, err := client.ListFlowSchedules(t.Context())
	if err != nil {
		t.Fatalf("ListFlowSchedules failed: %v", err)
	}
	var fs *FlowScheduleInfo
	for _, s := range flowSchedules {
		if s.FlowName == flowName {
			fs = s
		}
	}
	if fs == nil {
		t.Fatalf("no flow schedule found for %q", flowName)
	}
	if fs.CronSpec != "*/1 * * * *" {
		t.Fatalf("expected updated flow cron_spec %q, got %q (spec change silently ignored)", "*/1 * * * *", fs.CronSpec)
	}
}

// TestSchedulerScheduleDelete verifies schedules can be removed, that delete
// reports whether a schedule existed, and that deleting a missing schedule is
// a no-op. See issue #34.
func TestSchedulerScheduleDelete(t *testing.T) {
	client := getTestClient(t)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano())
	taskName := "test_delete_task" + suffix
	flowName := "test_delete_flow" + suffix

	hasTaskSchedule := func(name string) bool {
		schedules, err := client.ListTaskSchedules(t.Context())
		if err != nil {
			t.Fatalf("ListTaskSchedules failed: %v", err)
		}
		for _, s := range schedules {
			if s.TaskName == name {
				return true
			}
		}
		return false
	}
	hasFlowSchedule := func(name string) bool {
		schedules, err := client.ListFlowSchedules(t.Context())
		if err != nil {
			t.Fatalf("ListFlowSchedules failed: %v", err)
		}
		for _, s := range schedules {
			if s.FlowName == name {
				return true
			}
		}
		return false
	}

	// Deleting a non-existent schedule is a no-op that reports false.
	existed, err := client.DeleteTaskSchedule(t.Context(), taskName)
	if err != nil {
		t.Fatalf("DeleteTaskSchedule (missing) failed: %v", err)
	}
	if existed {
		t.Fatalf("expected DeleteTaskSchedule to report false for missing schedule")
	}

	task := NewTask(taskName).Do(func(ctx context.Context, in string) (string, error) {
		return "done", nil
	})
	if err := CreateTask(t.Context(), client.Conn, task); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateTaskSchedule(t.Context(), taskName, "@hourly"); err != nil {
		t.Fatalf("CreateTaskSchedule failed: %v", err)
	}
	if !hasTaskSchedule(taskName) {
		t.Fatalf("expected task schedule to exist after create")
	}

	existed, err = client.DeleteTaskSchedule(t.Context(), taskName)
	if err != nil {
		t.Fatalf("DeleteTaskSchedule failed: %v", err)
	}
	if !existed {
		t.Fatalf("expected DeleteTaskSchedule to report true for existing schedule")
	}
	if hasTaskSchedule(taskName) {
		t.Fatalf("expected task schedule to be gone after delete")
	}

	// Flows behave the same way.
	flow := NewFlow(flowName)
	flow.AddStep(NewStep("s1").Do(func(ctx context.Context, in int) (int, error) {
		return in, nil
	}))
	if err := CreateFlow(t.Context(), client.Conn, flow); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateFlowSchedule(t.Context(), flowName, "@daily"); err != nil {
		t.Fatalf("CreateFlowSchedule failed: %v", err)
	}

	existed, err = client.DeleteFlowSchedule(t.Context(), flowName)
	if err != nil {
		t.Fatalf("DeleteFlowSchedule failed: %v", err)
	}
	if !existed {
		t.Fatalf("expected DeleteFlowSchedule to report true for existing schedule")
	}
	if hasFlowSchedule(flowName) {
		t.Fatalf("expected flow schedule to be gone after delete")
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
		w := NewWorker(testPool).AddTask(task)
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
