package streams

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
	FailKeep   FailPolicy = "keep"
	FailDelete FailPolicy = "delete"
)

// SubscriptionOpts are initial values, applied only when this call creates the
// subscription: an existing subscription is never modified. Zero fields mean the defaults.
// The fields are workload policy: where to start, how many at a time, how
// to retry, when to give up. The crash-detection window (claim_ttl) has a
// default on the subscription row and is tuned there, not here.
type SubscriptionOpts struct {
	StartPos       *int64        // claim from here; nil starts at the tail
	ClaimBatchSize int           // 100: messages per claim
	MaxAttempts    int           // 3
	BackoffKind    BackoffKind   // full_jitter
	BackoffBase    time.Duration // 5s
	BackoffMax     time.Duration // 5m
	OnFail         FailPolicy    // keep
	// Topic: which topics this subscription reads, applied server-side. '*'
	// matches one segment, '#' zero or more trailing segments. "" reads
	// every topic. A claim covers every position in its range, matching
	// or not, so it can hold fewer messages than claim_batch_size, or
	// none.
	Topic string
	// Condition: AND-only expression over headers and payload, parsed once
	// at creation and applied server-side after the topic pattern. MVP
	// forms: exists($.payload.a.b), $.headers.a.b == <scalar>. Slower than
	// topic matching: costs a per-row jsonb evaluation, never
	// index-assisted.
	Condition string
}

func EnsureSubscription(ctx context.Context, conn Conn, stream, subscription string, opts ...SubscriptionOpts) error {
	var o SubscriptionOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_stream_ensure_subscription($1, $2,
			start_pos        => $3,
			max_attempts     => $4,
			backoff_kind     => $5,
			backoff_base     => $6,
			backoff_max      => $7,
			on_fail          => $8,
			claim_batch_size => $9,
			topic            => $10,
			condition        => $11)`,
		stream, subscription,
		o.StartPos,
		nullInt(o.MaxAttempts),
		nullText(string(o.BackoffKind)),
		nullInterval(o.BackoffBase),
		nullInterval(o.BackoffMax),
		nullText(string(o.OnFail)),
		nullInt(o.ClaimBatchSize),
		nullText(o.Topic),
		nullText(o.Condition),
	)
	return wrapErr(err)
}

// RetryFailed resets every failed row of a subscription to a fresh, due retry with
// its full attempt budget, and reports how many were revived. The messages
// go back to this subscription only; cursors never saw them.
func RetryFailed(ctx context.Context, conn Conn, stream, subscription string) (int64, error) {
	var n int64
	err := conn.QueryRow(ctx,
		`SELECT cb_stream_retry_failed($1, $2)`, stream, subscription).Scan(&n)
	return n, wrapErr(err)
}
