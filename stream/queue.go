package stream

import (
	"context"
	"time"
)

type BackoffKind string

const (
	BackoffNone       BackoffKind = "none"
	BackoffFixed      BackoffKind = "fixed"
	BackoffFullJitter BackoffKind = "full_jitter"
)

type FailPolicy string

const (
	FailDeadLetter FailPolicy = "dead_letter"
	FailDrop       FailPolicy = "drop"
)

// QueueOpts are initial values, applied only when this call creates the
// queue: an existing queue is never modified. Zero fields mean the defaults.
type QueueOpts struct {
	StartPos    *int64        // claim from here; nil starts at the tail
	ClaimTTL    time.Duration // 30s
	MaxAttempts int           // 3
	MaxCrashes  int           // 3
	BackoffKind BackoffKind   // full_jitter
	BackoffBase time.Duration // 5s
	BackoffMax  time.Duration // 5m
	OnFail      FailPolicy    // dead_letter
}

func EnsureQueue(ctx context.Context, conn Conn, stream, queue string, opts ...QueueOpts) error {
	var o QueueOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_stream_ensure_queue($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		stream, queue, o.StartPos,
		nullInterval(o.ClaimTTL), nullInt(o.MaxAttempts),
		nullText(string(o.BackoffKind)), nullInterval(o.BackoffBase), nullInterval(o.BackoffMax),
		nullText(string(o.OnFail)), nullInt(o.MaxCrashes))
	return wrapErr(err)
}
