package wire

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Notification is a single durable notification in an identity's inbox.
// It is a perishable pointer to a durable fact: the underlying result lives
// permanently elsewhere (e.g. a run row); the notification is only the prompt
// to look, so it may go stale.
type Notification struct {
	ID        int64     `json:"id"` // monotonic cursor
	Identity  string    `json:"identity"`
	Topic     string    `json:"topic"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	SeenAt    time.Time `json:"seen_at,omitzero"`    // zero if unseen
	ReadAt    time.Time `json:"read_at,omitzero"`    // zero if unread
	ExpiresAt time.Time `json:"expires_at,omitzero"` // zero if none
}

// NotifyDurableOpts configures a durable notification.
//
// ExpiresAt is the relevance window, and it always wins over the
// retention tiers: past it the row stops being returned and the prune
// deletes it, seen or not. When zero, the row has no time expiry — an
// unseen row then survives until the ticker's MaxAge.
type NotifyDurableOpts struct {
	ExpiresAt time.Time
}

// NotifyDurable appends a durable notification to identity's inbox,
// nudges the identity's connected clients to re-poll, and returns the
// row's id (the cursor value). Unlike the ephemeral Notify, this is
// stored so the client can catch up against it on its own schedule.
//
// Callable inside the caller's transaction: the row commits atomically
// with the app's writes, and the nudge fires only on commit — a
// rollback delivers neither. The handler writing the row holds the
// identity as a value; nothing is extracted from topics. An empty
// identity is an error: the inbox is identity-keyed, and a row no
// identity can address is meaningless.
func NotifyDurable(ctx context.Context, conn Conn, identity, topic, message string, opts ...NotifyDurableOpts) (int64, error) {
	var resolved NotifyDurableOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	q := `SELECT cb_wire_notify_durable(identity => $1, topic => $2, message => $3, expires_at => $4)`

	var id int64
	err := conn.QueryRow(ctx, q,
		identity, topic, nullText(message), nullTime(resolved.ExpiresAt),
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// ReadUnseen returns an identity's unseen and still-relevant
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
func ReadUnseen(ctx context.Context, conn Conn, identity string, afterID int64, batchSize int) ([]Notification, error) {
	q := `
		SELECT id, identity, topic, message, created_at, seen_at, read_at, expires_at
		FROM cb_wire_inbox
		WHERE identity = $1
		  AND id > $2
		  AND seen_at IS NULL
		  AND (expires_at IS NULL OR expires_at > now())
		ORDER BY id
		LIMIT $3`

	rows, err := conn.Query(ctx, q, identity, afterID, batchSize)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanNotification)
}

// MarkSeenUntil acks the cursor as a bounded watermark: it marks all of identity's
// unseen notifications with id less than or equal to id as seen, and returns the
// number of rows marked. The id bound is load-bearing — it must not mark rows that
// arrived between a reader's fetch and its ack.
//
// This is whole-inbox scope only: a by-id range is unsafe across interleaved subsets
// (a subset's ids interleave with others' in one inbox), so subset-scoped acks use
// MarkSeen instead. Seen-tracking always flows through these acks, regardless of
// transport.
func MarkSeenUntil(ctx context.Context, conn Conn, identity string, id int64) (int64, error) {
	q := `SELECT cb_wire_mark_seen_until(identity => $1, id => $2)`

	var marked int64
	err := conn.QueryRow(ctx, q, identity, id).Scan(&marked)
	if err != nil {
		return 0, err
	}
	return marked, nil
}

// MarkSeen acks precisely: it marks the identity's unseen notifications whose id is
// in ids as seen, and returns the number of rows marked. Use this for subset-scoped
// acks — a transport matches a subset's unseen rows and acks exactly those ids, since
// the watermark MarkSeenUntil would clobber interleaved sibling subsets.
func MarkSeen(ctx context.Context, conn Conn, identity string, ids []int64) (int64, error) {
	q := `SELECT cb_wire_mark_seen(identity => $1, ids => $2)`

	var marked int64
	err := conn.QueryRow(ctx, q, identity, ids).Scan(&marked)
	if err != nil {
		return 0, err
	}
	return marked, nil
}

// MarkRead marks one notification as read — the identity opened or acted
// on it. Reading implies seeing: an unseen row gets its seen timestamp
// stamped too, so an opened row leaves the badge count. Each timestamp
// keeps its first value. Returns whether the row exists; marking an
// already-read row changes nothing and still returns true.
func MarkRead(ctx context.Context, conn Conn, identity string, id int64) (bool, error) {
	q := `SELECT cb_wire_mark_read(identity => $1, id => $2)`

	var found bool
	err := conn.QueryRow(ctx, q, identity, id).Scan(&found)
	if err != nil {
		return false, err
	}
	return found, nil
}

// MarkReadUntil marks the identity's unread notifications with id less
// than or equal to id as read ("mark all as read"), stamping the seen
// timestamp on the way, and returns the number of rows marked.
func MarkReadUntil(ctx context.Context, conn Conn, identity string, id int64) (int64, error) {
	q := `SELECT cb_wire_mark_read_until(identity => $1, id => $2)`

	var marked int64
	err := conn.QueryRow(ctx, q, identity, id).Scan(&marked)
	if err != nil {
		return 0, err
	}
	return marked, nil
}

func scanNotification(row pgx.CollectableRow) (Notification, error) {
	rec := Notification{}

	var message *string
	var seenAt, readAt, expiresAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&rec.Identity,
		&rec.Topic,
		&message,
		&rec.CreatedAt,
		&seenAt,
		&readAt,
		&expiresAt,
	); err != nil {
		return rec, err
	}

	if message != nil {
		rec.Message = *message
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
