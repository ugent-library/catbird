package catbird

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Notification is a single durable notification in an identity's inbox.
// It is a perishable pointer to a durable fact: the underlying result lives
// permanently elsewhere (e.g. a task row); the notification is only the prompt
// to look, so it may go stale.
type Notification struct {
	ID          int64     `json:"id"` // monotonic cursor
	Identity    string    `json:"identity"`
	Topic       string    `json:"topic"`
	CollapseKey string    `json:"collapse_key,omitempty"` // empty if none
	Message     string    `json:"message,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	SeenAt      time.Time `json:"seen_at,omitzero"`    // zero if unseen
	ExpiresAt   time.Time `json:"expires_at,omitzero"` // zero if none
}

// NotifyDurableOpts configures a durable notification.
//
// ExpiresAt is the relevance window (and the GC drop trigger). When zero, the
// notification has no time expiry: it waits in the inbox until seen (or until
// collapsed by a newer notification with the same CollapseKey).
//
// CollapseKey, when set, collapses prior unseen notifications with the same
// (identity, collapse_key) — the FCM collapse-key semantics (keep newest).
type NotifyDurableOpts struct {
	ExpiresAt   time.Time
	CollapseKey string
}

// NotifyDurable appends a durable notification to identity's inbox and returns
// its id (the cursor value). Unlike the ephemeral Notify, this is stored so the
// client can catch up against it on its own schedule.
//
// When opts.ExpiresAt is set, it is the relevance window: the notification stops
// being returned by UnseenNotifications once it passes, and GC drops the row. When
// zero, the notification waits until seen.
//
// When opts.CollapseKey is set, prior unseen notifications with the same
// (identity, collapse_key) are marked seen first (keep-newest collapse), so the
// new row is the only live one for that subject.
func NotifyDurable(ctx context.Context, conn Conn, identity, topic, message string, opts ...NotifyDurableOpts) (int64, error) {
	var resolved NotifyDurableOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	q := `SELECT cb_notify_durable(identity => $1, topic => $2, message => $3, collapse_key => $4, expires_at => $5);`

	var id int64
	err := conn.QueryRow(ctx, q,
		identity, topic, ptrOrNil(message), ptrOrNil(resolved.CollapseKey), ptrOrNil(resolved.ExpiresAt),
	).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// UnseenNotifications returns an identity's unseen and still-relevant
// notifications with id greater than afterID, ordered by id (the cursor),
// limited to limit rows. Pass afterID 0 to start from the beginning.
//
// Stale notifications (past their expires_at) are filtered out: the read never
// returns "everything since the cursor", so a client that was offline for a
// while is not flooded with obsolete prompts.
func UnseenNotifications(ctx context.Context, conn Conn, identity string, afterID int64, limit int) ([]Notification, error) {
	q := `
		SELECT id, identity, topic, collapse_key, message, created_at, seen_at, expires_at
		FROM cb_notifications
		WHERE identity = $1
		  AND id > $2
		  AND seen_at IS NULL
		  AND (expires_at IS NULL OR expires_at > now())
		ORDER BY id
		LIMIT $3;`

	rows, err := conn.Query(ctx, q, identity, afterID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleNotification)
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
	q := `SELECT cb_mark_seen_until(identity => $1, id => $2);`

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
	q := `SELECT cb_mark_seen(identity => $1, ids => $2);`

	var marked int64
	err := conn.QueryRow(ctx, q, identity, ids).Scan(&marked)
	if err != nil {
		return 0, err
	}
	return marked, nil
}

func scanCollectibleNotification(row pgx.CollectableRow) (Notification, error) {
	return scanNotification(row)
}

func scanNotification(row pgx.Row) (Notification, error) {
	rec := Notification{}

	var collapseKey *string
	var message *string
	var seenAt *time.Time
	var expiresAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&rec.Identity,
		&rec.Topic,
		&collapseKey,
		&message,
		&rec.CreatedAt,
		&seenAt,
		&expiresAt,
	); err != nil {
		return rec, err
	}

	if collapseKey != nil {
		rec.CollapseKey = *collapseKey
	}
	if message != nil {
		rec.Message = *message
	}
	if seenAt != nil {
		rec.SeenAt = *seenAt
	}
	if expiresAt != nil {
		rec.ExpiresAt = *expiresAt
	}

	return rec, nil
}
