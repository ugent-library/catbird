package catbird

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestNotifyDurableAndUnseen verifies a basic append → unseen read round-trip:
// fields are persisted, rows come back in cursor order, and inboxes are isolated
// per identity.
func TestNotifyDurableAndUnseen(t *testing.T) {
	client := getTestClient(t)
	ctx := t.Context()
	conn := client.Conn

	alice := "TestNotifyDurableAndUnseen-alice"
	bob := "TestNotifyDurableAndUnseen-bob"

	id1, err := NotifyDurable(ctx, conn, alice, "import.started", "started")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := NotifyDurable(ctx, conn, alice, "import.done", "done")
	if err != nil {
		t.Fatal(err)
	}
	if id2 <= id1 {
		t.Fatalf("expected monotonic ids, got %d then %d", id1, id2)
	}

	// Bob's notification must not leak into Alice's inbox.
	if _, err := NotifyDurable(ctx, conn, bob, "other", "nope"); err != nil {
		t.Fatal(err)
	}

	got, err := UnseenNotifications(ctx, conn, alice, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 unseen for alice, got %d", len(got))
	}
	if got[0].ID != id1 || got[1].ID != id2 {
		t.Fatalf("expected cursor order [%d %d], got [%d %d]", id1, id2, got[0].ID, got[1].ID)
	}
	if got[0].Topic != "import.started" || got[0].Message != "started" {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
	if got[0].Identity != alice {
		t.Fatalf("expected identity %q, got %q", alice, got[0].Identity)
	}
	if !got[0].SeenAt.IsZero() {
		t.Fatalf("expected unseen row to have zero SeenAt, got %v", got[0].SeenAt)
	}
}

// TestUnseenNotificationsPaging verifies cursor paging: afterID advances through
// the inbox and limit bounds the page size.
func TestUnseenNotificationsPaging(t *testing.T) {
	client := getTestClient(t)
	ctx := t.Context()
	conn := client.Conn

	identity := "TestUnseenNotificationsPaging"
	var ids []int64
	for i := 0; i < 5; i++ {
		id, err := NotifyDurable(ctx, conn, identity, "evt", fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// First page of 2.
	page1, err := UnseenNotifications(ctx, conn, identity, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || page1[0].ID != ids[0] || page1[1].ID != ids[1] {
		t.Fatalf("unexpected first page: %+v", page1)
	}

	// Second page resumes after the last id of the first page.
	cursor := page1[len(page1)-1].ID
	page2, err := UnseenNotifications(ctx, conn, identity, cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].ID != ids[2] || page2[1].ID != ids[3] {
		t.Fatalf("unexpected second page: %+v", page2)
	}

	// Final page has the remaining single row.
	cursor = page2[len(page2)-1].ID
	page3, err := UnseenNotifications(ctx, conn, identity, cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page3) != 1 || page3[0].ID != ids[4] {
		t.Fatalf("unexpected third page: %+v", page3)
	}
}

// TestMarkSeen verifies the cursor ack: marking through a mid-inbox id leaves
// only later rows unseen and reports the number of rows marked.
func TestMarkSeen(t *testing.T) {
	client := getTestClient(t)
	ctx := t.Context()
	conn := client.Conn

	identity := "TestMarkSeen"
	var ids []int64
	for i := 0; i < 4; i++ {
		id, err := NotifyDurable(ctx, conn, identity, "evt", fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Mark seen through the second notification.
	marked, err := MarkSeen(ctx, conn, identity, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if marked != 2 {
		t.Fatalf("expected 2 rows marked, got %d", marked)
	}

	got, err := UnseenNotifications(ctx, conn, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != ids[2] || got[1].ID != ids[3] {
		t.Fatalf("expected only ids[2..3] unseen, got %+v", got)
	}

	// Re-acking the same cursor is a no-op (rows already seen).
	marked, err = MarkSeen(ctx, conn, identity, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if marked != 0 {
		t.Fatalf("expected 0 rows marked on re-ack, got %d", marked)
	}
}

// TestNotifyDurableCollapse verifies write-time keep-newest collapse: a newer
// notification with the same (identity, collapse_key) supersedes prior unseen
// ones, leaving a single live row carrying the freshest message.
func TestNotifyDurableCollapse(t *testing.T) {
	client := getTestClient(t)
	ctx := t.Context()
	conn := client.Conn

	identity := "TestNotifyDurableCollapse"
	key := "import-42"

	if _, err := NotifyDurable(ctx, conn, identity, "import.progress", "50%", NotifyDurableOpts{CollapseKey: key}); err != nil {
		t.Fatal(err)
	}
	if _, err := NotifyDurable(ctx, conn, identity, "import.progress", "80%", NotifyDurableOpts{CollapseKey: key}); err != nil {
		t.Fatal(err)
	}
	latest, err := NotifyDurable(ctx, conn, identity, "import.progress", "100%", NotifyDurableOpts{CollapseKey: key})
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnseenNotifications(ctx, conn, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 live row after collapse, got %d: %+v", len(got), got)
	}
	if got[0].ID != latest || got[0].Message != "100%" {
		t.Fatalf("expected newest row (id=%d, msg=100%%), got %+v", latest, got[0])
	}
	if got[0].CollapseKey != key {
		t.Fatalf("expected collapse key %q, got %q", key, got[0].CollapseKey)
	}

	// A different collapse key is an independent subject and is not collapsed.
	if _, err := NotifyDurable(ctx, conn, identity, "export.progress", "10%", NotifyDurableOpts{CollapseKey: "export-1"}); err != nil {
		t.Fatal(err)
	}
	got, err = UnseenNotifications(ctx, conn, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 live rows across two collapse keys, got %d", len(got))
	}
}

// TestUnseenFiltersStale verifies that notifications past their relevance window
// are filtered from the unseen read even though they still physically exist.
// The expires_at > created_at constraint forbids inserting an already-stale row
// through cb_notify_durable, so the stale row is seeded directly with a past
// created_at.
func TestUnseenFiltersStale(t *testing.T) {
	client := getTestClient(t)
	ctx := t.Context()
	conn := client.Conn

	identity := "TestUnseenFiltersStale"

	seedStaleNotification(t, ctx, conn, identity, "stale")

	// A fresh, still-relevant notification alongside it.
	fresh, err := NotifyDurable(ctx, conn, identity, "fresh", "live", NotifyDurableOpts{ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnseenNotifications(ctx, conn, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != fresh {
		t.Fatalf("expected only the fresh row, got %+v", got)
	}

	// The stale row is still physically present until GC.
	if n := countNotifications(t, ctx, conn, identity); n != 2 {
		t.Fatalf("expected 2 physical rows before GC, got %d", n)
	}
}

// TestGCDeletesExpiredNotifications verifies that cb_gc physically deletes
// notifications past their relevance window and reports the count. A notification
// with no expiry (waits until seen) must survive GC.
func TestGCDeletesExpiredNotifications(t *testing.T) {
	client := getTestClient(t)
	ctx := t.Context()
	conn := client.Conn

	identity := "TestGCDeletesExpiredNotifications"

	seedStaleNotification(t, ctx, conn, identity, "stale")
	// A notification with no expiry waits until seen and must survive GC.
	keep, err := NotifyDurable(ctx, conn, identity, "keep", "waits until seen")
	if err != nil {
		t.Fatal(err)
	}

	info, err := GC(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if info.ExpiredNotificationsDeleted < 1 {
		t.Fatalf("expected at least 1 expired notification deleted, got %d", info.ExpiredNotificationsDeleted)
	}

	if n := countNotifications(t, ctx, conn, identity); n != 1 {
		t.Fatalf("expected 1 row remaining after GC, got %d", n)
	}
	got, err := UnseenNotifications(ctx, conn, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != keep {
		t.Fatalf("expected the no-expiry row to survive, got %+v", got)
	}
}

// TestNotifyDurableConcurrentCursorOrdering verifies that concurrent inserts get
// distinct, monotonically increasing cursor ids and read back in id order.
func TestNotifyDurableConcurrentCursorOrdering(t *testing.T) {
	client := getTestClient(t)
	ctx := t.Context()
	conn := client.Conn

	identity := "TestNotifyDurableConcurrentCursorOrdering"
	const n = 25

	var wg sync.WaitGroup
	ids := make([]int64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = NotifyDurable(ctx, conn, identity, "evt", fmt.Sprintf("msg-%d", i))
		}(i)
	}
	wg.Wait()

	seen := make(map[int64]struct{}, n)
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("insert %d failed: %v", i, errs[i])
		}
		if _, dup := seen[ids[i]]; dup {
			t.Fatalf("duplicate cursor id %d", ids[i])
		}
		seen[ids[i]] = struct{}{}
	}

	got, err := UnseenNotifications(ctx, conn, identity, 0, n)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("expected %d unseen, got %d", n, len(got))
	}
	if !sort.SliceIsSorted(got, func(a, b int) bool { return got[a].ID < got[b].ID }) {
		t.Fatalf("unseen notifications not returned in cursor order")
	}
}

// seedStaleNotification inserts an already-expired, unseen notification directly,
// bypassing cb_notify_durable's expires_at > created_at constraint by backdating
// created_at.
func seedStaleNotification(t *testing.T, ctx context.Context, conn Conn, identity, message string) {
	t.Helper()
	_, err := conn.Exec(ctx,
		`INSERT INTO cb_notifications (identity, topic, message, created_at, expires_at)
		 VALUES ($1, 'stale.topic', $2, now() - interval '1 hour', now() - interval '1 minute')`,
		identity, message)
	if err != nil {
		t.Fatalf("seeding stale notification: %v", err)
	}
}

func countNotifications(t *testing.T, ctx context.Context, conn Conn, identity string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM cb_notifications WHERE identity = $1`, identity).Scan(&n); err != nil {
		t.Fatalf("counting notifications: %v", err)
	}
	return n
}
