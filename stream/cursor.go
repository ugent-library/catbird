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
}

func EnsureCursor(ctx context.Context, conn Conn, stream, cursor string, opts ...CursorOpts) error {
	var o CursorOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx, `SELECT cb_stream_ensure_cursor($1, $2, $3)`,
		stream, cursor, o.StartPos)
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
	msgs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Message, error) {
		var m Message
		err := row.Scan(&m.ID, &m.Stream, &m.Pos, &m.Topic, &m.Payload, &m.Headers, &m.CreatedAt)
		return m, err
	})
	return msgs, wrapErr(err)
}
