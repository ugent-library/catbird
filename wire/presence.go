package wire

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// Presence is one row of "who is here": a recipient at a topic, with a
// payload for the detail (which field, what state) and the moment their
// heartbeat lapses. Presence is not a message — nothing is kept, nothing
// is addressed — so wire never renders it: a change nudges the topic
// (an empty frame named after it) and the app's own handler refetches
// and renders the current rows.
type Presence struct {
	Topic     string          `json:"topic"`
	Recipient string          `json:"recipient"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	ExpiresAt time.Time       `json:"expires_at"`
}

// Appear records that a recipient is at a topic and re-arms the row's
// ttl: call it on arrival, on every heartbeat, and on detail changes
// (the payload — which field they moved to). Watchers are nudged only
// when something visible changed; a bare heartbeat re-arm is silent.
// Presence topics are ordinary topics the app names by convention
// ("record.123.presence"), so token grants and SSE event names work
// unchanged.
func Appear(ctx context.Context, conn Conn, topic, recipient string, payload any, ttl time.Duration) error {
	var body any
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = json.RawMessage(b)
	}
	var ttlArg any
	if ttl > 0 {
		ttlArg = ttl
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_wire_appear($1, $2, $3, $4)`,
		topic, recipient, body, ttlArg)
	return wrapErr(err)
}

// Disappear removes the row at once — the polite leave on navigation.
// Silence works too: the row expires on its own ttl. Reports whether a
// row existed.
func Disappear(ctx context.Context, conn Conn, topic, recipient string) (bool, error) {
	var deleted bool
	err := conn.QueryRow(ctx, `SELECT cb_wire_disappear($1, $2)`, topic, recipient).Scan(&deleted)
	return deleted, wrapErr(err)
}

// PresenceAt returns who is at the topic right now: the live rows, in
// recipient order. Expired rows never appear, pruned or not.
func PresenceAt(ctx context.Context, conn Conn, topic string) ([]Presence, error) {
	rows, err := conn.Query(ctx, `
		SELECT p.topic, p.recipient, p.payload, p.expires_at
		FROM cb_wire_presence p
		WHERE p.topic = $1 AND p.expires_at > now()
		ORDER BY p.recipient`, topic)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Presence])
}
