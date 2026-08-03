package wire

import (
	"context"
	"time"
)

// RelayOpts is the relay's whole declaration beyond its name and stream:
// omitted fields mean the defaults, never "keep what is there".
type RelayOpts struct {
	// Topic: which topics this relay forwards, applied server-side on the
	// relay's cursor. '*' matches one segment, '#' zero or more trailing
	// segments. "" forwards every topic.
	Topic string
	// Condition: AND-only expression over headers and payload, the same
	// language cursors use — including $.recipients == "name".
	Condition string
	// StartPos: where a new relay begins; everything at or below it is
	// skipped. nil starts at the stream's tail, At(0) forwards the stream
	// from the beginning. On an existing relay nil keeps the position
	// and a value repositions it.
	StartPos *int64
	// ExpiresAfter: the inbox relevance window, anchored at each
	// message's created_at — an inbox row from this relay expires that
	// long after the event happened, and a message already past its
	// window writes no rows. Zero means no window; the retention tiers
	// still apply.
	ExpiresAfter time.Duration
}

// At names a stream position for RelayOpts.StartPos.
func At(pos int64) *int64 { return &pos }

// DefineRelay declares that every matching message on a stream is
// forwarded to the web: one live frame to the connections whose token
// topics match, and one inbox row per addressed recipient — the
// recipients the publisher named plus everyone subscribed to a covering
// pattern. Creating and updating are the same call, and an identical
// declaration writes nothing. The stream must be defined first, and the
// module's tick (StartTicker) delivers. The relay owns the cursor named
// after it on its stream. Without the stream schema installed this
// returns ErrStreamsRequired.
func DefineRelay(ctx context.Context, conn Conn, name, stream string, opts ...RelayOpts) error {
	var o RelayOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	var expiresAfter any
	if o.ExpiresAfter > 0 {
		expiresAfter = o.ExpiresAfter
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_wire_define_relay($1, $2, $3, $4, $5, $6)`,
		name, stream, nullText(o.Topic), nullText(o.Condition), o.StartPos, expiresAfter)
	return wrapErr(err)
}

// DeleteRelay removes a relay and its cursor. It reports whether one
// existed; deleting a missing relay is a no-op.
func DeleteRelay(ctx context.Context, conn Conn, name string) (bool, error) {
	var deleted bool
	err := conn.QueryRow(ctx, `SELECT cb_wire_delete_relay($1)`, name).Scan(&deleted)
	return deleted, wrapErr(err)
}
