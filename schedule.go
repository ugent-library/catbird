package catbird

import (
	"context"
	"encoding/json"
	"fmt"
)

// ScheduleOpts configures scheduled task/flow behavior.
type ScheduleOpts struct {
	// Input is the static input value for each scheduled execution.
	// It will be marshaled to JSON and stored in the database.
	// If nil, an empty object {} is used.
	Input any
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
