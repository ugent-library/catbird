package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ScheduleOpts configures scheduled task/flow behavior.
type ScheduleOpts struct {
	// Input is the static input value for each scheduled execution.
	// It will be marshaled to JSON and stored in the database.
	// If nil, an empty object {} is used.
	Input any
}

// TaskScheduleInfo contains metadata about a scheduled task.
type TaskScheduleInfo struct {
	TaskName       string     `json:"task_name"`
	CronSpec       string     `json:"cron_spec"`
	NextRunAt      time.Time  `json:"next_run_at"`
	LastRunAt      *time.Time `json:"last_run_at,omitempty"`
	LastEnqueuedAt *time.Time `json:"last_enqueued_at,omitempty"`
	Enabled        bool       `json:"enabled"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// FlowScheduleInfo contains metadata about a scheduled flow.
type FlowScheduleInfo struct {
	FlowName       string     `json:"flow_name"`
	CronSpec       string     `json:"cron_spec"`
	NextRunAt      time.Time  `json:"next_run_at"`
	LastRunAt      *time.Time `json:"last_run_at,omitempty"`
	LastEnqueuedAt *time.Time `json:"last_enqueued_at,omitempty"`
	Enabled        bool       `json:"enabled"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// CreateTaskSchedule creates a cron-based schedule for a task.
//
// Parameters:
//   - ctx: context for database operations
//   - conn: database connection (can be *pgxpool.Pool, *pgx.Conn, or pgx.Tx)
//   - taskName: name of the task to schedule (must already exist via CreateTask)
//   - cronSpec: cron schedule in 5-field format (min hour day month dow) or descriptors
//     Descriptors: @hourly, @daily, @midnight, @weekly, @monthly, @yearly
//     Examples: "0 * * * *" (hourly), "0 0 * * *" (daily at midnight)
//   - opts: optional ScheduleOpts configuring the schedule (Input field for static input)
//     If nil or opts.Input is nil, empty JSON object {} is used
//
// The schedule will execute the task with the provided static input at each cron tick.
// Returns an error if the task does not exist or if the cron spec is invalid.
func CreateTaskSchedule(ctx context.Context, conn Conn, taskName, cronSpec string, opts *ScheduleOpts) error {
	var inputJSON []byte
	var err error

	if opts == nil || opts.Input == nil {
		inputJSON = []byte("{}")
	} else {
		inputJSON, err = json.Marshal(opts.Input)
		if err != nil {
			return fmt.Errorf("failed to marshal task schedule input: %w", err)
		}
	}

	_, err = conn.Exec(ctx, `SELECT cb_create_task_schedule($1, $2, $3);`, taskName, cronSpec, inputJSON)
	if err != nil {
		return fmt.Errorf("failed to create task schedule %q: %w", taskName, err)
	}
	return nil
}

// CreateFlowSchedule creates a cron-based schedule for a flow.
//
// Parameters:
//   - ctx: context for database operations
//   - conn: database connection (can be *pgxpool.Pool, *pgx.Conn, or pgx.Tx)
//   - flowName: name of the flow to schedule (must already exist via CreateFlow)
//   - cronSpec: cron schedule in 5-field format (min hour day month dow) or descriptors
//     Descriptors: @hourly, @daily, @midnight, @weekly, @monthly, @yearly
//     Examples: "0 * * * *" (hourly), "0 0 * * *" (daily at midnight)
//   - opts: optional ScheduleOpts configuring the schedule (Input field for static input)
//     If nil or opts.Input is nil, empty JSON object {} is used
//
// The schedule will execute the flow with the provided static input at each cron tick.
// Returns an error if the flow does not exist or if the cron spec is invalid.
func CreateFlowSchedule(ctx context.Context, conn Conn, flowName, cronSpec string, opts *ScheduleOpts) error {
	var inputJSON []byte
	var err error

	if opts == nil || opts.Input == nil {
		inputJSON = []byte("{}")
	} else {
		inputJSON, err = json.Marshal(opts.Input)
		if err != nil {
			return fmt.Errorf("failed to marshal flow schedule input: %w", err)
		}
	}

	_, err = conn.Exec(ctx, `SELECT cb_create_flow_schedule($1, $2, $3);`, flowName, cronSpec, inputJSON)
	if err != nil {
		return fmt.Errorf("failed to create flow schedule %q: %w", flowName, err)
	}
	return nil
}

// ListTaskSchedules returns all task schedules ordered by next_run_at.
func ListTaskSchedules(ctx context.Context, conn Conn) ([]*TaskScheduleInfo, error) {
	q := `SELECT task_name, cron_spec, next_run_at, last_run_at, last_enqueued_at, enabled, created_at, updated_at
		FROM cb_task_schedules
		ORDER BY next_run_at ASC;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*TaskScheduleInfo, error) {
		var s TaskScheduleInfo
		err := row.Scan(&s.TaskName, &s.CronSpec, &s.NextRunAt, &s.LastRunAt, &s.LastEnqueuedAt, &s.Enabled, &s.CreatedAt, &s.UpdatedAt)
		return &s, err
	})
}

// ListFlowSchedules returns all flow schedules ordered by next_run_at.
func ListFlowSchedules(ctx context.Context, conn Conn) ([]*FlowScheduleInfo, error) {
	q := `SELECT flow_name, cron_spec, next_run_at, last_run_at, last_enqueued_at, enabled, created_at, updated_at
		FROM cb_flow_schedules
		ORDER BY next_run_at ASC;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*FlowScheduleInfo, error) {
		var s FlowScheduleInfo
		err := row.Scan(&s.FlowName, &s.CronSpec, &s.NextRunAt, &s.LastRunAt, &s.LastEnqueuedAt, &s.Enabled, &s.CreatedAt, &s.UpdatedAt)
		return &s, err
	})
}
