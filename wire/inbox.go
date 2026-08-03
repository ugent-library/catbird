package wire

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// Notification is a single durable notification in a recipient's inbox.
// It carries the event itself — topic and payload exactly as published —
// and is rendered at read time; the fact it points at lives permanently
// elsewhere (a run row, a record), so the row may go stale.
type Notification struct {
	ID        int64     `json:"id"` // monotonic cursor
	Recipient string    `json:"recipient"`
	Topic     string    `json:"topic"`
	Payload   string    `json:"payload,omitempty"` // the event's payload, JSON
	CreatedAt time.Time `json:"created_at"`
	SeenAt    time.Time `json:"seen_at,omitzero"`    // zero if unseen
	ReadAt    time.Time `json:"read_at,omitzero"`    // zero if unread
	ExpiresAt time.Time `json:"expires_at,omitzero"` // zero if none
}

// SendOpts configures a sent notification.
//
// ExpiresAt is the relevance window, and it always wins over the
// retention tiers: past it the row stops being returned and the prune
// deletes it, seen or not. When zero, the row has no time expiry — an
// unseen row then survives until the ticker's MaxAge.
type SendOpts struct {
	ExpiresAt time.Time
}

// Send appends an event to the recipient's inbox — wire's one write —
// nudges the recipient's connected clients to re-poll, and returns the
// row's id (the cursor value). The payload is marshaled to JSON and
// rendered at read time by the same renderers that serve the live feed.
//
// Callable inside the caller's transaction: the row commits atomically
// with the app's writes, and the nudge fires only on commit — a
// rollback delivers neither. The caller holds the recipient as a value;
// nothing is extracted from topics. An empty recipient or topic returns
// ErrInvalid: the inbox is recipient-keyed and rendered by topic.
func Send(ctx context.Context, conn Conn, recipient, topic string, payload any, opts ...SendOpts) (int64, error) {
	var resolved SendOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	var body any
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return 0, err
		}
		body = json.RawMessage(b)
	}

	q := `SELECT cb_wire_send(recipient => $1, topic => $2, payload => $3, expires_at => $4)`

	var id int64
	err := conn.QueryRow(ctx, q,
		recipient, topic, body, nullTime(resolved.ExpiresAt),
	).Scan(&id)
	if err != nil {
		return 0, wrapErr(err)
	}
	return id, nil
}

// ReadUnseen returns a recipient's unseen and still-relevant
// notifications with id greater than afterID, ordered by id (the cursor),
// at most batchSize rows. Pass afterID 0 to start from the beginning.
//
// Stale notifications (past their expires_at) are filtered out: the read never
// returns "everything since the cursor", so a client that was offline for a
// while is not flooded with obsolete prompts.
//
// Ids are assigned at insert, not at commit, so a row from a
// still-uncommitted transaction can surface with an id below a cursor a
// reader already advanced past. A fresh poll (afterID 0) repairs that,
// and the badge count never uses the cursor.
func ReadUnseen(ctx context.Context, conn Conn, recipient string, afterID int64, batchSize int) ([]Notification, error) {
	q := `
		SELECT id, recipient, topic, payload::text, created_at, seen_at, read_at, expires_at
		FROM cb_wire_inbox
		WHERE recipient = $1
		  AND id > $2
		  AND seen_at IS NULL
		  AND (expires_at IS NULL OR expires_at > now())
		ORDER BY id
		LIMIT $3`

	rows, err := conn.Query(ctx, q, recipient, afterID, batchSize)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanNotification)
}

// MarkSeenUntil acks the cursor as a bounded watermark: it marks all of the
// recipient's unseen notifications with id less than or equal to id as seen,
// and returns the number of rows marked. The id bound is load-bearing — it
// must not mark rows that arrived between a reader's fetch and its ack.
//
// This is whole-inbox scope only: a by-id range is unsafe across interleaved
// subsets (a subset's ids interleave with others' in one inbox), so
// subset-scoped acks use MarkSeen instead. Seen-tracking always flows through
// these acks, regardless of transport.
func MarkSeenUntil(ctx context.Context, conn Conn, recipient string, id int64) (int64, error) {
	q := `SELECT cb_wire_mark_seen_until(recipient => $1, id => $2)`

	var marked int64
	err := conn.QueryRow(ctx, q, recipient, id).Scan(&marked)
	if err != nil {
		return 0, wrapErr(err)
	}
	return marked, nil
}

// MarkSeen acks precisely: it marks the recipient's unseen notifications
// whose id is in ids as seen, and returns the number of rows marked. Use this
// for subset-scoped acks — a transport matches a subset's unseen rows and
// acks exactly those ids, since the watermark MarkSeenUntil would clobber
// interleaved sibling subsets.
func MarkSeen(ctx context.Context, conn Conn, recipient string, ids []int64) (int64, error) {
	q := `SELECT cb_wire_mark_seen(recipient => $1, ids => $2)`

	var marked int64
	err := conn.QueryRow(ctx, q, recipient, ids).Scan(&marked)
	if err != nil {
		return 0, wrapErr(err)
	}
	return marked, nil
}

// MarkRead marks one notification as read — the recipient opened or acted
// on it. Reading implies seeing: an unseen row gets its seen timestamp
// stamped too, so an opened row leaves the badge count. Each timestamp
// keeps its first value. Returns whether the row exists; marking an
// already-read row changes nothing and still returns true.
func MarkRead(ctx context.Context, conn Conn, recipient string, id int64) (bool, error) {
	q := `SELECT cb_wire_mark_read(recipient => $1, id => $2)`

	var found bool
	err := conn.QueryRow(ctx, q, recipient, id).Scan(&found)
	if err != nil {
		return false, wrapErr(err)
	}
	return found, nil
}

// MarkReadUntil marks the recipient's unread notifications with id less
// than or equal to id as read ("mark all as read"), stamping the seen
// timestamp on the way, and returns the number of rows marked.
func MarkReadUntil(ctx context.Context, conn Conn, recipient string, id int64) (int64, error) {
	q := `SELECT cb_wire_mark_read_until(recipient => $1, id => $2)`

	var marked int64
	err := conn.QueryRow(ctx, q, recipient, id).Scan(&marked)
	if err != nil {
		return 0, wrapErr(err)
	}
	return marked, nil
}

func scanNotification(row pgx.CollectableRow) (Notification, error) {
	rec := Notification{}

	var payload *string
	var seenAt, readAt, expiresAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&rec.Recipient,
		&rec.Topic,
		&payload,
		&rec.CreatedAt,
		&seenAt,
		&readAt,
		&expiresAt,
	); err != nil {
		return rec, err
	}

	if payload != nil {
		rec.Payload = *payload
	}
	if seenAt != nil {
		rec.SeenAt = *seenAt
	}
	if readAt != nil {
		rec.ReadAt = *readAt
	}
	if expiresAt != nil {
		rec.ExpiresAt = *expiresAt
	}

	return rec, nil
}
