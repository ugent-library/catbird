// Package catbird provides a PostgreSQL-based message queue
// with task and workflow execution engine.
package catbird

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	// ErrRunFailed is returned when you try to unmarshal the output of a failed task or flow run
	ErrRunFailed = fmt.Errorf("run failed")
	// ErrUnknownStepOutput is returned when a requested step output is not present in completed outputs.
	ErrUnknownStepOutput = fmt.Errorf("unknown step output")
	// ErrNoFailedStepInput is returned when failed step input is not available.
	ErrNoFailedStepInput = fmt.Errorf("failed step input not available")
	// ErrNoFailedStepSignal is returned when failed step signal input is not available.
	ErrNoFailedStepSignal = fmt.Errorf("failed step signal input not available")
	// ErrInvalidDecodeTarget is returned when decode target is nil or not a pointer.
	ErrInvalidDecodeTarget = fmt.Errorf("invalid decode target")
)

// Conn is an interface for database connections compatible with pgx.Conn and pgx.Pool
type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// GC runs garbage collection to clean up expired queues and stale workers.
// Note: Worker heartbeats automatically perform cleanup, so this is mainly
// useful for deployments without workers or for manual control.
func GC(ctx context.Context, conn Conn) error {
	q := `SELECT cb_gc();`
	_, err := conn.Exec(ctx, q)
	return err
}
