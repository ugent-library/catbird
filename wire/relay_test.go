package wire

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/streams"
)

var streamsOnce sync.Once

// setupRelayTest migrates the stream module too: relays read streams, and
// the shared test database serves both schemas side by side.
func setupRelayTest(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool := setupTest(t)
	streamsOnce.Do(func() {
		db, err := sql.Open("pgx", testDSN)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		if err := streams.MigrateUpTo(context.Background(), db, streams.SchemaVersion); err != nil {
			panic(err)
		}
	})
	return pool
}

// testStream declares a fresh stream with a unique gws-prefixed name.
func testStream(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	name := "gws" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if err := streams.Ensure(t.Context(), pool, name); err != nil {
		t.Fatal(err)
	}
	return name
}

// publish appends a message and assigns its position, so the next
// deliver sees it.
func publish(t *testing.T, pool *pgxpool.Pool, stream, topic string, payload any, opts ...streams.PublishOpts) {
	t.Helper()
	if _, err := streams.Publish(t.Context(), pool, stream, topic, payload, opts...); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `SELECT _cb_stream_assign_positions($1)`, stream); err != nil {
		t.Fatal(err)
	}
}

// deliver runs one relay's delivery and returns how many messages it
// handled.
func deliver(t *testing.T, pool *pgxpool.Pool, relay string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT _cb_wire_relay_deliver($1)`, relay).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func testRelayName() string {
	return "gwr" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
}

// TestDefineRelay verifies the declaration contract: define creates the
// relay row and its filter-bearing cursor, an identical redeclaration
// writes nothing to either row, a filter change lands on the cursor only,
// a window change lands on the relay only, a stream move leaves no cursor
// behind, and delete removes both.
func TestDefineRelay(t *testing.T) {
	pool := setupRelayTest(t)
	ctx := t.Context()
	stream := testStream(t, pool)
	relay := testRelayName()

	if err := DefineRelay(ctx, pool, relay, stream, RelayOpts{
		Topic: "order.#", ExpiresAfter: time.Hour, StartPos: At(0),
	}); err != nil {
		t.Fatal(err)
	}
	var topic string
	var pos int64
	if err := pool.QueryRow(ctx,
		`SELECT pos, topic FROM cb_stream_cursors WHERE stream = $1 AND name = $2`,
		stream, relay).Scan(&pos, &topic); err != nil {
		t.Fatal(err)
	}
	if pos != 0 || topic != "order.#" {
		t.Fatalf("cursor = (pos %d, topic %s)", pos, topic)
	}

	rowVersion := func(q string, args ...any) string {
		t.Helper()
		var v string
		if err := pool.QueryRow(ctx, q, args...).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	relayV := rowVersion(`SELECT xmin::text FROM cb_wire_relays WHERE name = $1`, relay)
	cursorV := rowVersion(`SELECT xmin::text FROM cb_stream_cursors WHERE stream = $1 AND name = $2`, stream, relay)

	// An identical redeclaration writes nothing to either table.
	if err := DefineRelay(ctx, pool, relay, stream, RelayOpts{
		Topic: "order.#", ExpiresAfter: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if v := rowVersion(`SELECT xmin::text FROM cb_wire_relays WHERE name = $1`, relay); v != relayV {
		t.Fatal("identical redeclare wrote the relay row")
	}
	if v := rowVersion(`SELECT xmin::text FROM cb_stream_cursors WHERE stream = $1 AND name = $2`, stream, relay); v != cursorV {
		t.Fatal("identical redeclare wrote the cursor row")
	}

	// A filter change lands on the cursor only; a window change on the
	// relay only.
	if err := DefineRelay(ctx, pool, relay, stream, RelayOpts{
		Topic: "order.placed", ExpiresAfter: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if v := rowVersion(`SELECT xmin::text FROM cb_wire_relays WHERE name = $1`, relay); v != relayV {
		t.Fatal("filter change wrote the relay row")
	}
	if err := DefineRelay(ctx, pool, relay, stream, RelayOpts{
		Topic: "order.placed", ExpiresAfter: 2 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	var window time.Duration
	var windowStr string
	if err := pool.QueryRow(ctx,
		`SELECT expires_after::text FROM cb_wire_relays WHERE name = $1`, relay).Scan(&windowStr); err != nil {
		t.Fatal(err)
	}
	_ = window
	if windowStr != "02:00:00" {
		t.Fatalf("expires_after = %s, want 02:00:00", windowStr)
	}

	// A relay moved to another stream leaves no cursor behind.
	other := testStream(t, pool)
	if err := DefineRelay(ctx, pool, relay, other, RelayOpts{Topic: "order.placed"}); err != nil {
		t.Fatal(err)
	}
	var leftover int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_cursors WHERE stream = $1 AND name = $2`,
		stream, relay).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatal("stream move left the old cursor behind")
	}

	// Delete removes the relay and its cursor; a second delete reports false.
	deleted, err := DeleteRelay(ctx, pool, relay)
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("expected delete to report true")
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_cursors WHERE stream = $1 AND name = $2`,
		other, relay).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatal("delete left the cursor behind")
	}
	deleted, err = DeleteRelay(ctx, pool, relay)
	if err != nil {
		t.Fatal(err)
	}
	if deleted {
		t.Fatal("expected deleting a missing relay to report false")
	}

	// A bad name is refused.
	if err := DefineRelay(ctx, pool, "Bad Name", stream); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad name: err = %v, want ErrInvalid", err)
	}
}

// TestRelayDeliver verifies the delivery contract end to end: matching
// messages produce inbox rows for the stamped recipients and subscribed
// watchers — carrying the event's own topic and payload — non-matching
// and topicless messages advance without rows, and a second deliver does
// nothing (the cursor advanced in the same transaction).
func TestRelayDeliver(t *testing.T) {
	pool := setupRelayTest(t)
	ctx := t.Context()
	stream := testStream(t, pool)
	relay := testRelayName()

	alice := testRecipient("stamped")
	bob := testRecipient("watcher")
	base := "gw.rd." + uuid.NewString()[:8]

	if err := DefineRelay(ctx, pool, relay, stream, RelayOpts{
		Topic: base + ".#", StartPos: At(0),
	}); err != nil {
		t.Fatal(err)
	}
	if err := Subscribe(ctx, pool, bob, base+".#"); err != nil {
		t.Fatal(err)
	}

	publish(t, pool, stream, base+".placed", map[string]any{"n": 1},
		streams.PublishOpts{Recipients: []string{alice}})
	publish(t, pool, stream, "other.topic", map[string]any{"n": 2}) // filter drops it
	publish(t, pool, stream, "", map[string]any{"n": 3})            // no topic: nothing to deliver it as

	if n := deliver(t, pool, relay); n != 1 {
		t.Fatalf("delivered %d, want 1", n)
	}

	for _, recipient := range []string{alice, bob} {
		rows, err := ReadUnseen(ctx, pool, recipient, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 {
			t.Fatalf("%s inbox = %+v, want one row", recipient, rows)
		}
		if rows[0].Topic != base+".placed" || rows[0].Payload != `{"n": 1}` {
			t.Fatalf("%s row = %+v, want the event verbatim", recipient, rows[0])
		}
	}

	// The cursor advanced over everything scanned: nothing redelivers.
	if n := deliver(t, pool, relay); n != 0 {
		t.Fatalf("second deliver handled %d, want 0", n)
	}
	rows, err := ReadUnseen(ctx, pool, alice, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("redelivery wrote a duplicate inbox row: %+v", rows)
	}

	// An unknown relay delivers nothing rather than raising: the tick
	// races deletes.
	if n := deliver(t, pool, "gwrnope"); n != 0 {
		t.Fatalf("unknown relay delivered %d", n)
	}
}

// TestRelayExpiresAfter verifies the relevance window: anchored at the
// message, not at delivery — a message already past the window writes no
// inbox rows, a message inside it writes rows expiring at created_at +
// window.
func TestRelayExpiresAfter(t *testing.T) {
	pool := setupRelayTest(t)
	ctx := t.Context()
	stream := testStream(t, pool)
	relay := testRelayName()
	alice := testRecipient("window")
	topic := "gw.win." + uuid.NewString()[:8]

	if err := DefineRelay(ctx, pool, relay, stream, RelayOpts{
		Topic: topic, StartPos: At(0), ExpiresAfter: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	// A stale message: born two hours ago, window one hour — skipped.
	publish(t, pool, stream, topic, map[string]any{"stale": true},
		streams.PublishOpts{Recipients: []string{alice}})
	if _, err := pool.Exec(ctx,
		`UPDATE cb_stream_messages SET created_at = now() - interval '2 hours' WHERE stream = $1`,
		stream); err != nil {
		t.Fatal(err)
	}
	// A fresh one behind it.
	publish(t, pool, stream, topic, map[string]any{"fresh": true},
		streams.PublishOpts{Recipients: []string{alice}})

	if n := deliver(t, pool, relay); n != 2 {
		t.Fatalf("delivered %d, want 2", n)
	}

	rows, err := ReadUnseen(ctx, pool, alice, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Payload != `{"fresh": true}` {
		t.Fatalf("inbox = %+v, want only the fresh row", rows)
	}
	if rows[0].ExpiresAt.IsZero() {
		t.Fatal("expected the fresh row to carry the window's expiry")
	}
}

// TestRelaySSE verifies the live leg end to end: a published message
// reaches a connected client as a rendered frame — the channel carried
// only the address, the wire fetched the row and rendered it — and a
// token whose topics don't cover the event sees nothing.
func TestRelaySSE(t *testing.T) {
	pool := setupRelayTest(t)
	ctx := t.Context()
	stream := testStream(t, pool)
	relay := testRelayName()
	topic := "gw.live." + uuid.NewString()[:8]

	w := New(pool, testSecret, Opts{Notifier: testNotifier(t)})
	Render(w, topic, func(_ *http.Request, _ string, data map[string]any) (Fragment, error) {
		return Fragment{Data: fmt.Sprintf("<li>order %v</li>", data["n"])}, nil
	})
	startTestWire(t, w)

	if err := DefineRelay(ctx, pool, relay, stream, RelayOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}

	matching := connectSSE(t, w, w.Token([]string{topic}))
	outside := connectSSE(t, w, w.Token([]string{"unrelated." + uuid.NewString()[:8]}))

	publish(t, pool, stream, topic, map[string]any{"n": 7})
	if n := deliver(t, pool, relay); n != 1 {
		t.Fatalf("delivered %d, want 1", n)
	}

	events := readSSEEvents(t, matching, 1, 2*time.Second)
	if len(events) != 1 || events[0].event != topic {
		t.Fatalf("events = %+v, want one frame named %s", events, topic)
	}
	if events[0].data != "<li>order 7</li>" {
		t.Fatalf("frame data = %q, want the rendered fragment", events[0].data)
	}

	if events := readSSEEvents(t, outside, 1, 500*time.Millisecond); len(events) != 0 {
		t.Fatalf("outside events = %+v, want none", events)
	}
}

// TestSubscribeValidation verifies the pattern grammar: exact topics and
// prefix.# forms converge on redeclare, everything else is refused.
func TestSubscribeValidation(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()
	recipient := testRecipient("subval")

	for _, ok := range []string{"order.placed", "order.#", "#", "a"} {
		if err := Subscribe(ctx, pool, recipient, ok); err != nil {
			t.Fatalf("pattern %q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "order.*", "*", "order..placed", "order.", ".order", "order.#.x"} {
		if err := Subscribe(ctx, pool, recipient, bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("pattern %q: err = %v, want ErrInvalid", bad, err)
		}
	}
	if err := Subscribe(ctx, pool, "", "order.#"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty recipient: err = %v, want ErrInvalid", err)
	}

	// Unsubscribe reports what it removed.
	removed, err := Unsubscribe(ctx, pool, recipient, "order.#")
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("expected unsubscribe to report true")
	}
	removed, err = Unsubscribe(ctx, pool, recipient, "order.#")
	if err != nil {
		t.Fatal(err)
	}
	if removed {
		t.Fatal("expected unsubscribing a missing watch to report false")
	}
}

// TestTopicPatterns verifies the covering-pattern expansion the deliverer
// probes with: the topic itself, '#', and every prefix with '.#' — so
// 'p.#' covers p itself.
func TestTopicPatterns(t *testing.T) {
	pool := setupTest(t)

	var got []string
	if err := pool.QueryRow(t.Context(),
		`SELECT _cb_wire_topic_patterns('a.b.c')`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	want := []string{"a.b.c", "#", "a.#", "a.b.#", "a.b.c.#"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("patterns = %v, want %v", got, want)
	}
}

// TestSubscriptionExpiry verifies the watch lifetime: a lapsed watch stops
// matching immediately and the prune removes the row.
func TestSubscriptionExpiry(t *testing.T) {
	pool := setupRelayTest(t)
	ctx := t.Context()
	stream := testStream(t, pool)
	relay := testRelayName()
	bob := testRecipient("lapsed")
	topic := "gw.sexp." + uuid.NewString()[:8]

	if err := DefineRelay(ctx, pool, relay, stream, RelayOpts{Topic: topic, StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	if err := Subscribe(ctx, pool, bob, topic, SubscribeOpts{
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	// The watch lapses before anything is published.
	if _, err := pool.Exec(ctx,
		`UPDATE cb_wire_subscriptions SET expires_at = now() - interval '1 minute' WHERE recipient = $1`,
		bob); err != nil {
		t.Fatal(err)
	}

	publish(t, pool, stream, topic, map[string]any{"n": 1})
	if n := deliver(t, pool, relay); n != 1 {
		t.Fatalf("delivered %d, want 1", n)
	}
	rows, err := ReadUnseen(ctx, pool, bob, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("a lapsed watch still delivered: %+v", rows)
	}

	var pruned int64
	if err := pool.QueryRow(ctx, `SELECT _cb_wire_prune_subscriptions()`).Scan(&pruned); err != nil {
		t.Fatal(err)
	}
	if pruned < 1 {
		t.Fatalf("pruned %d watches, want at least 1", pruned)
	}
}
