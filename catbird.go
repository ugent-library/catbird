// Package catbird provides a PostgreSQL-based message queue
// with task and workflow execution engine.
//
// # Scheduling for Distributed Environments
//
// Catbird includes built-in support for scheduled task and flow execution using cron syntax.
// When multiple workers run concurrently (on different machines or processes), Catbird
// guarantees that each scheduled execution runs **exactly once per cron tick**, even across
// clock skew and timezone differences.
//
// This is achieved through:
//   - UTC-normalized cron scheduling: all workers use UTC, eliminating timezone confusion
//   - Idempotency key deduplication: each cron tick generates a deterministic key
//     (format: "schedule:{unix_nanos_utc}") that persists across completion
//   - PostgreSQL as the single source of truth: the database enforces the unique constraint
//     on idempotency keys, preventing duplicates at the atomic level
//
// See scheduler.go for implementation details and SCHEDULING_ADVANCED.md for the
// roadmap toward a future DB-driven scheduling system with even stronger guarantees.
package catbird

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Status constants for task and flow runs
const (
	StatusCreated   = "created"
	StatusStarted   = "started"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusSkipped   = "skipped" // Step skipped due to condition
)

var (
	// ErrRunFailed is returned when you try to unmarshal the output of a failed task or flow run
	ErrRunFailed = fmt.Errorf("run failed")
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
