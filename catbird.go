// Package catbird provides a PostgreSQL-based message queue
// with task and workflow execution engine.
package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	StatusWaitingForDependencies = "waiting_for_dependencies"
	StatusWaitingForSignal       = "waiting_for_signal"
	StatusWaitingForMapTasks     = "waiting_for_map_tasks"
	StatusQueued                 = "queued"
	StatusStarted                = "started"
	StatusCanceling              = "canceling"
	StatusCompleted              = "completed"
	StatusFailed                 = "failed"
	StatusSkipped                = "skipped"
	StatusCanceled               = "canceled"
	StatusExpired                = "expired"
)

var (
	// ErrRunFailed is returned when you try to unmarshal the output of a failed task or flow run
	ErrRunFailed = fmt.Errorf("catbird: run failed")
	// ErrRunCanceled is returned when you try to wait for output from a canceled task or flow run
	ErrRunCanceled = fmt.Errorf("catbird: run canceled")
	// ErrNotFound is returned when a requested run or resource cannot be found
	ErrNotFound = fmt.Errorf("catbird: not found")
	// ErrNoRunContext is returned when cancellation helpers are called outside handler run context
	ErrNoRunContext = fmt.Errorf("catbird: no run context")
	// ErrUnknownStepOutput is returned when a requested step output is not present in completed outputs.
	ErrUnknownStepOutput = fmt.Errorf("catbird: unknown step output")
	// ErrNoFailedStepInput is returned when failed step input is not available.
	ErrNoFailedStepInput = fmt.Errorf("catbird: failed step input not available")
	// ErrNoFailedStepSignal is returned when failed step signal input is not available.
	ErrNoFailedStepSignal = fmt.Errorf("catbird: failed step signal input not available")
	// ErrNoOutputCandidate is returned when a flow completes without any configured output candidate producing output.
	ErrNoOutputCandidate = fmt.Errorf("catbird: no output candidate produced output")
	// ErrNotDefined is returned when an operation references a queue, task, or flow that has not been created.
	ErrNotDefined = fmt.Errorf("catbird: not defined")
	// ErrRunSkipped is returned when waiting for output from a skipped task or flow run.
	ErrRunSkipped = fmt.Errorf("catbird: run skipped")
	// ErrRunNotCompleted is returned when waiting for output from a run that is still in progress.
	ErrRunNotCompleted = fmt.Errorf("catbird: run not completed")
	// ErrSignalNotDelivered is returned when a signal could not be delivered to a flow step.
	ErrSignalNotDelivered = fmt.Errorf("catbird: signal not delivered")
)

// Conn is an interface for database connections compatible with pgx.Conn and pgx.Pool
type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// GCInfo is the garbage collection report returned by cb_gc().
type GCInfo struct {
	ExpiredQueuesDeleted   int `json:"expired_queues_deleted"`
	ExpiredMessagesDeleted int `json:"expired_messages_deleted"`
	ExpiredTaskRuns        int `json:"expired_task_runs"`
	ExpiredFlowRuns        int `json:"expired_flow_runs"`
	StaleWorkersDeleted    int `json:"stale_workers_deleted"`
	StaleWireNodesDeleted  int `json:"stale_wire_nodes_deleted"`
	TaskRunsPurged         int `json:"task_runs_purged"`
	FlowRunsPurged         int `json:"flow_runs_purged"`
}

// GC runs garbage collection to clean up expired queues and stale workers.
// Note: Worker heartbeats automatically perform cleanup, so this is mainly
// useful for deployments without workers or for manual control.
func GC(ctx context.Context, conn Conn) (*GCInfo, error) {
	var raw []byte
	err := conn.QueryRow(ctx, `SELECT cb_gc();`).Scan(&raw)
	if err != nil {
		return nil, err
	}

	var info GCInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// PurgeTaskRuns deletes terminal task runs (completed, failed, skipped, canceled)
// older than the given duration. Useful for manual cleanup or targeted removal
// independent of the configured retention period.
func PurgeTaskRuns(ctx context.Context, conn Conn, taskName string, olderThan time.Duration) (int, error) {
	var deletedCount int
	err := conn.QueryRow(ctx, `SELECT cb_purge_task_runs($1, $2);`, taskName, olderThan).Scan(&deletedCount)
	if err != nil {
		return 0, wrapNotDefinedErr(err, "task", taskName)
	}
	return deletedCount, nil
}

// PurgeFlowRuns deletes terminal flow runs (completed, failed, canceled) older than
// the given duration. Step runs and map tasks are deleted via cascade.
// Useful for manual cleanup or targeted removal independent of the configured retention period.
func PurgeFlowRuns(ctx context.Context, conn Conn, flowName string, olderThan time.Duration) (int, error) {
	var deletedCount int
	err := conn.QueryRow(ctx, `SELECT cb_purge_flow_runs($1, $2);`, flowName, olderThan).Scan(&deletedCount)
	if err != nil {
		return 0, wrapNotDefinedErr(err, "flow", flowName)
	}
	return deletedCount, nil
}

// ClearTaskRuns deletes all runs for the given task regardless of status,
// including in-progress runs. Use with caution — in-flight work will be lost.
func ClearTaskRuns(ctx context.Context, conn Conn, taskName string) (int, error) {
	var deletedCount int
	err := conn.QueryRow(ctx, `SELECT cb_clear_task_runs($1);`, taskName).Scan(&deletedCount)
	if err != nil {
		return 0, wrapNotDefinedErr(err, "task", taskName)
	}
	return deletedCount, nil
}

// ClearFlowRuns deletes all runs for the given flow regardless of status,
// including in-progress runs. Step runs and map tasks are deleted via cascade.
// Use with caution — in-flight work will be lost.
func ClearFlowRuns(ctx context.Context, conn Conn, flowName string) (int, error) {
	var deletedCount int
	err := conn.QueryRow(ctx, `SELECT cb_clear_flow_runs($1);`, flowName).Scan(&deletedCount)
	if err != nil {
		return 0, wrapNotDefinedErr(err, "flow", flowName)
	}
	return deletedCount, nil
}
