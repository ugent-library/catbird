// Package catbird provides a PostgreSQL-based message queue
// with task and workflow execution engine.
package catbird

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Conn is an interface for database connections compatible with pgx.Conn and pgx.Pool
type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// GC runs garbage collection to clean up expired and deleted entries.
func GC(ctx context.Context, conn Conn) error {
	q := `SELECT cb_gc();`
	_, err := conn.Exec(ctx, q)
	return err
}

func ptrOrNil[T comparable](t T) *T {
	var tt T
	if t == tt {
		return nil
	}
	return &t
}
