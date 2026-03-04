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
)

var (
	// ErrRunFailed is returned when you try to unmarshal the output of a failed task or flow run
	ErrRunFailed = fmt.Errorf("run failed")
	// ErrRunCanceled is returned when you try to wait for output from a canceled task or flow run
	ErrRunCanceled = fmt.Errorf("run canceled")
	// ErrNotFound is returned when a requested run or resource cannot be found
	ErrNotFound = fmt.Errorf("not found")
	// ErrNoRunContext is returned when cancellation helpers are called outside handler run context
	ErrNoRunContext = fmt.Errorf("no run context")
	// ErrUnknownStepOutput is returned when a requested step output is not present in completed outputs.
	ErrUnknownStepOutput = fmt.Errorf("unknown step output")
	// ErrNoFailedStepInput is returned when failed step input is not available.
	ErrNoFailedStepInput = fmt.Errorf("failed step input not available")
	// ErrNoFailedStepSignal is returned when failed step signal input is not available.
	ErrNoFailedStepSignal = fmt.Errorf("failed step signal input not available")
	// ErrNoOutputCandidate is returned when a flow completes without any configured output candidate producing output.
	ErrNoOutputCandidate = fmt.Errorf("no output candidate produced output")
)

// Conn is an interface for database connections compatible with pgx.Conn and pgx.Pool
type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// GCInfo is the garbage collection report returned by cb_gc().
type GCInfo struct {
	ExpiredQueuesDeleted int `json:"expired_queues_deleted"`
	StaleWorkersDeleted  int `json:"stale_workers_deleted"`
	TaskRunsPurged       int `json:"task_runs_purged"`
	FlowRunsPurged       int `json:"flow_runs_purged"`
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
		return 0, err
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
		return 0, err
	}
	return deletedCount, nil
}
