package stream

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type CursorOpts struct {
	// StartPos: where a new cursor begins; everything at or below it counts
	// as processed. nil starts at the stream's tail (skip history); At(0)
	// reads from the beginning. Ignored when the cursor already exists.
	StartPos *int64
	// Topic: which topics this cursor reads, applied server-side. '*'
	// matches one segment, '#' zero or more trailing segments. "" reads
	// every topic. The cursor advances over everything it scans, so a
	// filtered read can return fewer messages than the batch size, or
	// none.
	Topic string
	// Condition: AND-only expression over headers and payload, parsed once
	// at creation and applied server-side after the topic pattern. MVP
	// forms: exists($.payload.a.b), $.headers.a.b == <scalar>. Slower than
	// topic matching: costs a per-row jsonb evaluation, never
	// index-assisted.
	Condition string
}

func EnsureCursor(ctx context.Context, conn Conn, stream, cursor string, opts ...CursorOpts) error {
	var o CursorOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx, `SELECT cb_stream_ensure_cursor($1, $2, $3, $4, $5)`,
		stream, cursor, o.StartPos, nullText(o.Topic), nullText(o.Condition))
	return wrapErr(err)
}

// Read returns the next batch after the cursor and advances it, in the
// caller's transaction when conn is one: handle the batch and commit for
// exactly-once processing.
func Read(ctx context.Context, conn Conn, stream, cursor string, batchSize int) ([]Message, error) {
	rows, err := conn.Query(ctx, `
		SELECT m.id, m.stream, m.pos, coalesce(m.topic, ''), m.payload, m.headers, m.created_at
		FROM cb_stream_read($1, $2, $3) m`,
		stream, cursor, batchSize)
	if err != nil {
		return nil, wrapErr(err)
	}
	msgs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
	return msgs, wrapErr(err)
}
