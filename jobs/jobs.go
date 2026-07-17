// Package jobs is the job engine: declared jobs, runs with durable
// handles, steps claimed and executed by workers, retries and give-up
// paced per pool. All engine logic lives in the SQL functions of
// migrations/00001_job.sql; this package declares, enqueues and runs
// handlers against that contract.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Conn is an interface for database connections compatible with pgx.Conn,
// pgxpool.Pool and pgx.Tx.
type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

var (
	ErrInvalid     = errors.New("catbird: invalid argument")
	ErrNotDefined  = errors.New("catbird: not defined")
	ErrNotFound    = errors.New("catbird: not found")
	ErrRunFailed   = errors.New("catbird: run failed")
	ErrRunCanceled = errors.New("catbird: run canceled")
	// ErrStreamsRequired: the call needs the stream module's schema and it
	// is not installed in this database. Defining nothing fixes it; run the
	// stream migrations.
	ErrStreamsRequired = errors.New("catbird: stream schema required")
)

// Forever mirrors cb_forever(): a retention with no limit.
const Forever = -time.Second

// Run and step statuses. A run is 'running', 'failing' (the outcome is
// already failed, only the on_fail chain still executes) or terminal. A
// step is waiting (its status says what it waits for), 'queued',
// 'started' or terminal.
const (
	StatusWaitingForSteps  = "waiting_for_steps"
	StatusWaitingForSignal = "waiting_for_signal"

	StatusQueued    = "queued"
	StatusStarted   = "started"
	StatusRunning   = "running"
	StatusFailing   = "failing"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCanceled  = "canceled"
)

type BackoffKind string

const (
	BackoffNone       BackoffKind = "none"
	BackoffFixed      BackoffKind = "fixed"
	BackoffFullJitter BackoffKind = "full_jitter"
)

// Backoff paces retries: the delay before a failed or crashed step becomes
// claimable again, growing with the attempt count. The zero value means
// the stock policy: full_jitter, 1s to 1m.
type Backoff struct {
	Kind BackoffKind
	Base time.Duration
	Max  time.Duration
}

// FullJitterBackoff grows the delay exponentially from base to max, with
// each delay drawn at random below that cap so retries spread out.
func FullJitterBackoff(base, max time.Duration) Backoff {
	return Backoff{Kind: BackoffFullJitter, Base: base, Max: max}
}

// FixedBackoff waits the same delay before every retry.
func FixedBackoff(d time.Duration) Backoff {
	return Backoff{Kind: BackoffFixed, Base: d, Max: d}
}

// NoBackoff retries immediately.
func NoBackoff() Backoff {
	return Backoff{Kind: BackoffNone}
}

type CatchUpPolicy string

const (
	CatchUpSkip CatchUpPolicy = "skip"
	CatchUpAll  CatchUpPolicy = "all"
)

func nullInterval(d time.Duration) pgtype.Interval {
	if d == 0 {
		return pgtype.Interval{}
	}
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

func nullText(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullInt(n int) any {
	if n == 0 {
		return nil
	}
	return int32(n)
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// wrapErr translates the SQL surface's raised errors into the package
// sentinels so callers can use errors.Is. The SQLSTATE codes are declared
// in jobs/migrations/00001_job.sql.
func wrapErr(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	var sentinel error
	switch pgErr.Code {
	case "IRD01":
		sentinel = ErrInvalid
	case "IRD02":
		sentinel = ErrNotDefined
	case "IRD03":
		sentinel = ErrStreamsRequired
	default:
		return err
	}
	return fmt.Errorf("%w: %s", sentinel,
		strings.TrimPrefix(pgErr.Message, "catbird: "))
}
