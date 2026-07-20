package wire

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// testIdentity returns a unique gw_-prefixed identity: the inbox table is
// shared across suite runs and the setup wipe targets the prefix.
func testIdentity(name string) string {
	return "gw_" + name + "_" + uuid.NewString()[:8]
}

// TestNotifyDurableAndUnseen verifies a basic append → unseen read round-trip:
// fields are persisted, rows come back in cursor order, and inboxes are isolated
// per identity.
func TestNotifyDurableAndUnseen(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	alice := testIdentity("alice")
	bob := testIdentity("bob")

	id1, err := NotifyDurable(ctx, pool, alice, "import.started", "started")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := NotifyDurable(ctx, pool, alice, "import.done", "done")
	if err != nil {
		t.Fatal(err)
	}
	if id2 <= id1 {
		t.Fatalf("expected monotonic ids, got %d then %d", id1, id2)
	}

	// Bob's notification must not leak into Alice's inbox.
	if _, err := NotifyDurable(ctx, pool, bob, "other", "nope"); err != nil {
		t.Fatal(err)
	}

	got, err := ReadUnseen(ctx, pool, alice, 0, 10)
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
	if !got[0].SeenAt.IsZero() || !got[0].ReadAt.IsZero() {
		t.Fatalf("expected a fresh row to be unseen and unread, got %+v", got[0])
	}
}

// TestNotifyDurableEmptyIdentity verifies the guard: the inbox is
// identity-keyed, so an empty identity raises instead of inserting a row
// nobody can address.
func TestNotifyDurableEmptyIdentity(t *testing.T) {
	pool := setupTest(t)

	_, err := NotifyDurable(t.Context(), pool, "", "evt", "orphan")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "IRD01" {
		t.Fatalf("expected an IRD01 raise, got %v", err)
	}
}

// TestReadUnseenPaging verifies cursor paging: afterID advances through
// the inbox and batchSize bounds the page size.
func TestReadUnseenPaging(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	identity := testIdentity("paging")
	var ids []int64
	for i := range 5 {
		id, err := NotifyDurable(ctx, pool, identity, "evt", fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// First page of 2.
	page1, err := ReadUnseen(ctx, pool, identity, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || page1[0].ID != ids[0] || page1[1].ID != ids[1] {
		t.Fatalf("unexpected first page: %+v", page1)
	}

	// Second page resumes after the last id of the first page.
	cursor := page1[len(page1)-1].ID
	page2, err := ReadUnseen(ctx, pool, identity, cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].ID != ids[2] || page2[1].ID != ids[3] {
		t.Fatalf("unexpected second page: %+v", page2)
	}

	// Final page has the remaining single row.
	cursor = page2[len(page2)-1].ID
	page3, err := ReadUnseen(ctx, pool, identity, cursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page3) != 1 || page3[0].ID != ids[4] {
		t.Fatalf("unexpected third page: %+v", page3)
	}
}

// TestMarkSeenUntil verifies the bounded watermark ack: marking through a mid-inbox
// id leaves only later rows unseen and reports the number of rows marked.
func TestMarkSeenUntil(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	identity := testIdentity("seenuntil")
	var ids []int64
	for i := range 4 {
		id, err := NotifyDurable(ctx, pool, identity, "evt", fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Mark seen through the second notification.
	marked, err := MarkSeenUntil(ctx, pool, identity, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if marked != 2 {
		t.Fatalf("expected 2 rows marked, got %d", marked)
	}

	got, err := ReadUnseen(ctx, pool, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != ids[2] || got[1].ID != ids[3] {
		t.Fatalf("expected only ids[2..3] unseen, got %+v", got)
	}

	// Re-acking the same cursor is a no-op (rows already seen).
	marked, err = MarkSeenUntil(ctx, pool, identity, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if marked != 0 {
		t.Fatalf("expected 0 rows marked on re-ack, got %d", marked)
	}
}

// TestMarkSeen verifies the precise ack: only the explicitly named ids are marked,
// leaving interleaved siblings untouched (the subset-scoped ack semantics a watermark
// can't express).
func TestMarkSeen(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	identity := testIdentity("seen")
	var ids []int64
	for i := range 5 {
		id, err := NotifyDurable(ctx, pool, identity, "evt", fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Ack a non-contiguous subset, skipping interleaved siblings.
	marked, err := MarkSeen(ctx, pool, identity, []int64{ids[0], ids[2], ids[4]})
	if err != nil {
		t.Fatal(err)
	}
	if marked != 3 {
		t.Fatalf("expected 3 rows marked, got %d", marked)
	}

	got, err := ReadUnseen(ctx, pool, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != ids[1] || got[1].ID != ids[3] {
		t.Fatalf("expected only ids[1] and ids[3] unseen, got %+v", got)
	}

	// Re-acking already-seen ids is a no-op; only the still-unseen id counts.
	marked, err = MarkSeen(ctx, pool, identity, []int64{ids[0], ids[1]})
	if err != nil {
		t.Fatal(err)
	}
	if marked != 1 {
		t.Fatalf("expected 1 row marked on partial re-ack, got %d", marked)
	}
}

// TestMarkReadImpliesSeen verifies the read verb: MarkRead stamps both
// timestamps (an opened row must leave the badge count), keeps first
// values, reports existence, and is idempotent.
func TestMarkReadImpliesSeen(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	identity := testIdentity("read")
	id, err := NotifyDurable(ctx, pool, identity, "evt", "open me")
	if err != nil {
		t.Fatal(err)
	}

	found, err := MarkRead(ctx, pool, identity, id)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected MarkRead to find the row")
	}

	var seenAt, readAt time.Time
	if err := pool.QueryRow(ctx,
		`SELECT seen_at, read_at FROM cb_wire_inbox WHERE id = $1`, id).Scan(&seenAt, &readAt); err != nil {
		t.Fatal(err)
	}
	if seenAt.IsZero() || readAt.IsZero() {
		t.Fatalf("expected both stamps set, got seen=%v read=%v", seenAt, readAt)
	}

	// The read row left the unseen set: the badge count drops.
	unseen, err := ReadUnseen(ctx, pool, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(unseen) != 0 {
		t.Fatalf("expected no unseen rows, got %+v", unseen)
	}

	// Marking an already-read row changes nothing and still returns true;
	// each stamp keeps its first value.
	found, err = MarkRead(ctx, pool, identity, id)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected MarkRead on a read row to return true")
	}
	var seenAt2, readAt2 time.Time
	if err := pool.QueryRow(ctx,
		`SELECT seen_at, read_at FROM cb_wire_inbox WHERE id = $1`, id).Scan(&seenAt2, &readAt2); err != nil {
		t.Fatal(err)
	}
	if !seenAt2.Equal(seenAt) || !readAt2.Equal(readAt) {
		t.Fatalf("expected stamps to keep their first values: seen %v→%v read %v→%v", seenAt, seenAt2, readAt, readAt2)
	}

	// A missing row reports false.
	found, err = MarkRead(ctx, pool, identity, id+1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected MarkRead on a missing row to return false")
	}
}

// TestMarkReadUntil verifies the "mark all as read" watermark: rows at or
// below the id turn read (and seen), a row already seen keeps its
// original seen stamp, and rows above the watermark stay untouched.
func TestMarkReadUntil(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	identity := testIdentity("readuntil")
	var ids []int64
	for i := range 4 {
		id, err := NotifyDurable(ctx, pool, identity, "evt", fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// The second row was already seen before the mark-all.
	if _, err := MarkSeen(ctx, pool, identity, []int64{ids[1]}); err != nil {
		t.Fatal(err)
	}
	var firstSeen time.Time
	if err := pool.QueryRow(ctx,
		`SELECT seen_at FROM cb_wire_inbox WHERE id = $1`, ids[1]).Scan(&firstSeen); err != nil {
		t.Fatal(err)
	}

	marked, err := MarkReadUntil(ctx, pool, identity, ids[2])
	if err != nil {
		t.Fatal(err)
	}
	if marked != 3 {
		t.Fatalf("expected 3 rows marked read, got %d", marked)
	}

	// The pre-seen row keeps its first seen stamp.
	var seenAfter time.Time
	if err := pool.QueryRow(ctx,
		`SELECT seen_at FROM cb_wire_inbox WHERE id = $1`, ids[1]).Scan(&seenAfter); err != nil {
		t.Fatal(err)
	}
	if !seenAfter.Equal(firstSeen) {
		t.Fatalf("expected seen stamp to keep its first value, %v → %v", firstSeen, seenAfter)
	}

	// The row above the watermark is still unseen and unread.
	unseen, err := ReadUnseen(ctx, pool, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(unseen) != 1 || unseen[0].ID != ids[3] {
		t.Fatalf("expected only the row above the watermark unseen, got %+v", unseen)
	}

	// Re-marking is a no-op: everything at or below is already read.
	marked, err = MarkReadUntil(ctx, pool, identity, ids[2])
	if err != nil {
		t.Fatal(err)
	}
	if marked != 0 {
		t.Fatalf("expected 0 rows on re-mark, got %d", marked)
	}
}

// TestUnseenFiltersStale verifies that notifications past their relevance window
// are filtered from the unseen read even though they still physically exist.
// The expires_at > created_at constraint forbids inserting an already-stale row
// through cb_wire_notify_durable, so the stale row is seeded directly with a past
// created_at.
func TestUnseenFiltersStale(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	identity := testIdentity("stale")

	seedInboxRow(t, ctx, identity, "stale", "created_at = now() - interval '1 hour', expires_at = now() - interval '1 minute'")

	// A fresh, still-relevant notification alongside it.
	fresh, err := NotifyDurable(ctx, pool, identity, "fresh", "live", NotifyDurableOpts{ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}

	got, err := ReadUnseen(ctx, pool, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != fresh {
		t.Fatalf("expected only the fresh row, got %+v", got)
	}

	// The stale row is still physically present until the prune tick.
	if n := countInboxRows(t, ctx, identity); n != 2 {
		t.Fatalf("expected 2 physical rows before the prune, got %d", n)
	}
}

// TestPruneTiers verifies _cb_wire_prune_inbox's whole contract with windows of
// read 1h / seen 2h / max age 3h: an explicit expiry always wins, each
// tier deletes only what its timestamp dates, and a row that was never
// seen and has no expiry survives until max age.
func TestPruneTiers(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	identity := testIdentity("prune")

	// Kept: read recently, seen recently, young.
	keptRead := seedInboxRow(t, ctx, identity, "kept-read",
		"created_at = now() - interval '150 minutes', seen_at = now() - interval '30 minutes', read_at = now() - interval '30 minutes'")
	// Deleted by the read tier: read longer than 1h ago.
	seedInboxRow(t, ctx, identity, "old-read",
		"created_at = now() - interval '150 minutes', seen_at = now() - interval '90 minutes', read_at = now() - interval '90 minutes'")
	// Kept: seen 90 minutes ago is within the 2h seen window, never read.
	keptSeen := seedInboxRow(t, ctx, identity, "kept-seen",
		"created_at = now() - interval '150 minutes', seen_at = now() - interval '90 minutes'")
	// Deleted by the seen tier: seen longer than 2h ago, never read.
	seedInboxRow(t, ctx, identity, "old-seen",
		"created_at = now() - interval '170 minutes', seen_at = now() - interval '130 minutes'")
	// Kept: never seen, no expiry, younger than the 3h max age — it waits to be seen.
	keptUnseen := seedInboxRow(t, ctx, identity, "kept-unseen",
		"created_at = now() - interval '170 minutes'")
	// Deleted by max age: never seen, no expiry, but older than 3h.
	seedInboxRow(t, ctx, identity, "too-old",
		"created_at = now() - interval '4 hours'")
	// Deleted by expiry, though young and unseen: an explicit expires_at always wins.
	seedInboxRow(t, ctx, identity, "expired",
		"created_at = now() - interval '30 minutes', expires_at = now() - interval '10 minutes'")
	// Kept: expiry still ahead.
	keptExpiring := seedInboxRow(t, ctx, identity, "kept-expiring",
		"created_at = now() - interval '30 minutes', expires_at = now() + interval '1 hour'")

	var deleted int64
	if err := pool.QueryRow(ctx,
		`SELECT _cb_wire_prune_inbox('1 hour'::interval, '2 hours'::interval, '3 hours'::interval)`).Scan(&deleted); err != nil {
		t.Fatal(err)
	}
	// Other tests' rows may be pruned in the same call, so check at least.
	if deleted < 4 {
		t.Fatalf("expected at least 4 rows deleted, got %d", deleted)
	}

	rows, err := pool.Query(ctx,
		`SELECT id FROM cb_wire_inbox WHERE identity = $1 ORDER BY id`, identity)
	if err != nil {
		t.Fatal(err)
	}
	var remaining []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		remaining = append(remaining, id)
	}
	want := []int64{keptRead, keptSeen, keptUnseen, keptExpiring}
	if fmt.Sprint(remaining) != fmt.Sprint(want) {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}
}

// TestNotifyDurableConcurrentCursorOrdering verifies that concurrent inserts get
// distinct, monotonically increasing cursor ids and read back in id order.
func TestNotifyDurableConcurrentCursorOrdering(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	identity := testIdentity("concurrent")
	const n = 25

	var wg sync.WaitGroup
	ids := make([]int64, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = NotifyDurable(ctx, pool, identity, "evt", fmt.Sprintf("msg-%d", i))
		}(i)
	}
	wg.Wait()

	seen := make(map[int64]struct{}, n)
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("insert %d failed: %v", i, errs[i])
		}
		if _, dup := seen[ids[i]]; dup {
			t.Fatalf("duplicate cursor id %d", ids[i])
		}
		seen[ids[i]] = struct{}{}
	}

	got, err := ReadUnseen(ctx, pool, identity, 0, n)
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

// seedInboxRow inserts a row directly with hand-set timestamps — the way
// to build the aged rows the retention tests need, which
// cb_wire_notify_durable's now() defaults cannot produce.
func seedInboxRow(t *testing.T, ctx context.Context, identity, message, timestamps string) int64 {
	t.Helper()
	var id int64
	if err := testPool.QueryRow(ctx,
		`INSERT INTO cb_wire_inbox (identity, topic, message) VALUES ($1, 'seed.topic', $2) RETURNING id`,
		identity, message).Scan(&id); err != nil {
		t.Fatalf("seeding inbox row: %v", err)
	}
	if _, err := testPool.Exec(ctx,
		`UPDATE cb_wire_inbox SET `+timestamps+` WHERE id = `+fmt.Sprint(id)); err != nil {
		t.Fatalf("aging inbox row: %v", err)
	}
	return id
}

func countInboxRows(t *testing.T, ctx context.Context, identity string) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM cb_wire_inbox WHERE identity = $1`, identity).Scan(&n); err != nil {
		t.Fatalf("counting inbox rows: %v", err)
	}
	return n
}
