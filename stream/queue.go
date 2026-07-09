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
// The fields are workload policy: where to start, how many at a time, how
// to retry, when to give up. The engine's failure-detection mechanics
// (claim_ttl, max_crashes) have defaults on the queue row and are tuned
// there, not here.
type QueueOpts struct {
	StartPos       *int64        // claim from here; nil starts at the tail
	ClaimBatchSize int           // 100: messages per claim
	MaxAttempts    int           // 3
	BackoffKind    BackoffKind   // full_jitter
	BackoffBase    time.Duration // 5s
	BackoffMax     time.Duration // 5m
	OnFail         FailPolicy    // dead_letter
}

func EnsureQueue(ctx context.Context, conn Conn, stream, queue string, opts ...QueueOpts) error {
	var o QueueOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_stream_ensure_queue($1, $2,
			start_pos        => $3,
			max_attempts     => $4,
			backoff_kind     => $5,
			backoff_base     => $6,
			backoff_max      => $7,
			on_fail          => $8,
			claim_batch_size => $9)`,
		stream, queue, o.StartPos,
		nullInt(o.MaxAttempts),
		nullText(string(o.BackoffKind)), nullInterval(o.BackoffBase), nullInterval(o.BackoffMax),
		nullText(string(o.OnFail)), nullInt(o.ClaimBatchSize))
	return wrapErr(err)
}
