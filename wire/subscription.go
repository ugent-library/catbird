package wire

import (
	"context"
	"time"
)

// SubscribeOpts configures a watch. Zero fields mean the defaults.
type SubscribeOpts struct {
	// ExpiresAt: how long the watch lasts; the prune removes it after.
	// Zero watches until Unsubscribe.
	ExpiresAt time.Time
}

// Subscribe declares a watch: relayed messages matching the pattern land
// in the recipient's inbox. The pattern is prefix-only — an exact topic,
// or a prefix followed by '.#' ('order.1042.#' covers the order and
// everything under it); there is no '*'. Creating and updating are the
// same call; an identical declaration writes nothing. An invalid pattern
// or empty recipient returns ErrInvalid.
func Subscribe(ctx context.Context, conn Conn, recipient, pattern string, opts ...SubscribeOpts) error {
	var o SubscribeOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_wire_subscribe($1, $2, $3)`,
		recipient, pattern, nullTime(o.ExpiresAt))
	return wrapErr(err)
}

// Unsubscribe removes a watch. It reports whether one existed; removing
// a missing watch is a no-op.
func Unsubscribe(ctx context.Context, conn Conn, recipient, pattern string) (bool, error) {
	var deleted bool
	err := conn.QueryRow(ctx, `SELECT cb_wire_unsubscribe($1, $2)`, recipient, pattern).Scan(&deleted)
	return deleted, wrapErr(err)
}
