package streams

import (
	"context"
	"encoding/json"
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
	ErrInvalid    = errors.New("catbird: invalid argument")
	ErrNotDefined = errors.New("catbird: not defined")
	ErrNotFound   = errors.New("catbird: not found")
)

// Forever mirrors cb_forever(): a retention with no limit.
const Forever = -time.Second

type RefKind string

const (
	RefMessage RefKind = "message"
	RefPending RefKind = "pending"
)

// Ref reports where a publish ended up.
type Ref struct {
	Kind     RefKind
	ID       int64
	Existing bool // the key was already taken; nothing was stored
}

type Message struct {
	ID         int64
	Stream     string
	Pos        int64
	Topic      string
	Payload    json.RawMessage
	Headers    map[string]any
	Recipients []string // who the publisher named; matched by $.recipients
	CreatedAt  time.Time
}

// collectMessages scans message rows (id, stream, pos, topic, payload,
// headers, recipients, created_at).
func collectMessages(rows pgx.Rows) ([]Message, error) {
	msgs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
	return msgs, wrapErr(err)
}

// Fetch returns one message by address. The log is position-addressable:
// a live wire frame carries {stream, pos}, and the receiving node fetches
// the row it was told about. Returns ErrNotFound when the position holds
// nothing — pruned by retention, or never assigned.
func Fetch(ctx context.Context, conn Conn, stream string, pos int64) (Message, error) {
	rows, err := conn.Query(ctx, `
		SELECT m.id, m.stream, m.pos, coalesce(m.topic, ''), m.payload, m.headers, m.recipients, m.created_at
		FROM cb_stream_fetch($1, $2) m`, stream, pos)
	if err != nil {
		return Message{}, wrapErr(err)
	}
	msgs, err := collectMessages(rows)
	if err != nil {
		return Message{}, err
	}
	if len(msgs) == 0 {
		return Message{}, fmt.Errorf("%w: no message at %s pos %d", ErrNotFound, stream, pos)
	}
	return msgs[0], nil
}

// EnsureOpts are initial values, applied only when this call creates the
// stream: an existing stream is never modified. Zero fields mean the defaults.
type EnsureOpts struct {
	Retention time.Duration // how long messages are kept; forever by default
}

// Ensure creates the stream when it does not exist yet; an existing
// stream is never modified. Call it at boot, in its own transaction:
// creating a stream briefly locks the whole message table, so an ensure
// that runs after reads or writes in the same transaction can deadlock
// with another process's ensure.
func Ensure(ctx context.Context, conn Conn, stream string, opts ...EnsureOpts) error {
	var o EnsureOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx, `SELECT cb_stream_ensure($1, $2)`,
		stream, nullInterval(o.Retention))
	return wrapErr(err)
}

// At is sugar for the StartPos option fields.
func At(pos int64) *int64 { return &pos }

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
// at the top of stream/migrations/00001_stream.sql.
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
		sentinel = ErrNotFound
	default:
		return err
	}
	return fmt.Errorf("%w: %s", sentinel,
		strings.TrimPrefix(pgErr.Message, "catbird: "))
}
