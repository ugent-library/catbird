package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"
)

// Scheduler manages scheduled task and flow executions using cron syntax.
type Scheduler struct {
	conn          Conn
	logger        *slog.Logger
	cron          *cron.Cron
	taskSchedules []scheduleEntry
	flowSchedules []scheduleEntry
}

type scheduleEntry struct {
	name     string
	schedule string
	inputFn  func(context.Context) (any, error)
}

type ScheduleOpt func(*scheduleEntry)

func WithInput[T any](inputFn func(context.Context) (T, error)) ScheduleOpt {
	return func(o *scheduleEntry) {
		o.inputFn = func(ctx context.Context) (any, error) {
			return inputFn(ctx)
		}
	}
}

// NewScheduler creates a new scheduler instance.
func NewScheduler(conn Conn, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		conn:   conn,
		logger: logger,
	}
}

// AddTask registers a scheduled task execution using cron syntax.
// The WithInput option can be used to provide dynamic input at execution time.
// Otherwise an empty JSON object will be used as input to the task.
//
// All scheduled task executions use idempotency deduplication keyed on the
// scheduled execution time. This means exactly one execution per cron tick will
// occur even when running multiple workers concurrently.
func (s *Scheduler) AddTask(taskName string, schedule string, opts ...ScheduleOpt) {
	entry := scheduleEntry{
		name:     taskName,
		schedule: schedule,
		inputFn:  func(_ context.Context) (any, error) { return struct{}{}, nil },
	}
	for _, opt := range opts {
		opt(&entry)
	}

	s.taskSchedules = append(s.taskSchedules, entry)
}

// AddFlow registers a scheduled flow execution using cron syntax.
// The WithInput option can be used to provide dynamic input at execution time.
// Otherwise an empty JSON object will be used as input to the flow.
//
// All scheduled flow executions use idempotency deduplication keyed on the
// scheduled execution time. This means exactly one execution per cron tick will
// occur even when running multiple workers concurrently.
func (s *Scheduler) AddFlow(flowName string, schedule string, opts ...ScheduleOpt) {
	entry := scheduleEntry{
		name:     flowName,
		schedule: schedule,
		inputFn:  func(_ context.Context) (any, error) { return struct{}{}, nil },
	}
	for _, opt := range opts {
		opt(&entry)
	}

	s.flowSchedules = append(s.flowSchedules, entry)
}

// Start begins executing scheduled tasks and flows.
// Returns an error if any schedule fails to register.
// The scheduler will continue until ctx is cancelled or Stop is called.
//
// Deduplication strategy: All scheduled runs use IdempotencyKey derived from
// the scheduled execution time in UTC (format: "schedule:<unix_seconds>").
// This ensures:
// - Multiple workers/machines generate identical keys for the same cron tick
// - One execution per scheduled tick, even after completion
// - Retries allowed on failed runs (idempotency persists across completion)
// - No clock skew or timezone issues (all times normalized to UTC)
func (s *Scheduler) Start(ctx context.Context) error {
	if len(s.taskSchedules) == 0 && len(s.flowSchedules) == 0 {
		return nil
	}

	// Initialize cron with UTC location to ensure consistent scheduling across all workers
	s.cron = cron.New(cron.WithLocation(time.UTC))

	for _, ts := range s.taskSchedules {
		var entryID cron.EntryID
		entryID, err := s.cron.AddFunc(ts.schedule, func() {
			entry := s.cron.Entry(entryID)
			scheduledTime := entry.Prev
			if scheduledTime.IsZero() {
				// use Next if Prev is not set, which will only happen for the first run
				scheduledTime = entry.Next
			}

			input, err := ts.inputFn(ctx)
			if err != nil {
				s.logger.ErrorContext(ctx, "scheduler: failed to get scheduled task input", "task", ts.name, "error", err)
				return
			}
			inputJSON, err := json.Marshal(input)
			if err != nil {
				s.logger.ErrorContext(ctx, "scheduler: failed to marshal scheduled task input", "task", ts.name, "error", err)
				return
			}

			_, err = RunTask(ctx, s.conn, ts.name, inputJSON, &RunOpts{
				// Generate stable idempotency key from scheduled time (UTC seconds)
				// Format: "schedule:<unix_seconds>" ensures consistent dedup across workers
				IdempotencyKey: fmt.Sprintf("schedule:%d", scheduledTime.Unix()),
			})
			if err != nil {
				s.logger.ErrorContext(ctx, "scheduler: failed to schedule task", "task", ts.name, "error", err)
			}
		})
		if err != nil {
			return fmt.Errorf("scheduler: failed to register schedule for task %s: %w", ts.name, err)
		}
	}

	for _, fs := range s.flowSchedules {
		var entryID cron.EntryID
		entryID, err := s.cron.AddFunc(fs.schedule, func() {
			entry := s.cron.Entry(entryID)
			scheduledTime := entry.Prev
			if scheduledTime.IsZero() {
				// use Next if Prev is not set, which will only happen for the first run
				scheduledTime = entry.Next
			}

			input, err := fs.inputFn(ctx)
			if err != nil {
				s.logger.ErrorContext(ctx, "scheduler: failed to get scheduled flow input", "flow", fs.name, "error", err)
				return
			}
			inputJSON, err := json.Marshal(input)
			if err != nil {
				s.logger.ErrorContext(ctx, "scheduler: failed to marshal scheduled flow input", "flow", fs.name, "error", err)
				return
			}

			_, err = RunFlow(ctx, s.conn, fs.name, inputJSON, &RunOpts{
				// Generate stable idempotency key from scheduled time (UTC seconds)
				// Format: "schedule:<unix_seconds>" ensures consistent dedup across workers
				IdempotencyKey: fmt.Sprintf("schedule:%d", scheduledTime.Unix()),
			})
			if err != nil {
				s.logger.ErrorContext(ctx, "scheduler: failed to schedule flow", "flow", fs.name, "error", err)
			}
		})
		if err != nil {
			return fmt.Errorf("scheduler: failed to register schedule for flow %s: %w", fs.name, err)
		}
	}

	s.cron.Start()

	return nil
}

func (s *Scheduler) Stop(ctx context.Context) {
	if s.cron == nil {
		return
	}

	stopCtx := s.cron.Stop()
	select {
	case <-stopCtx.Done():
	case <-ctx.Done():
	}
}
