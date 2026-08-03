package wire

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestPresence verifies the state contract: appear inserts and re-arms,
// reads return only live rows, disappear removes at once, and expiry
// removes by silence.
func TestPresence(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	topic := "gw.room." + uuid.NewString()[:8] + ".presence"
	alice := testRecipient("here")
	bob := testRecipient("also")

	if err := Appear(ctx, pool, topic, alice, map[string]any{"field": "title"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := Appear(ctx, pool, topic, bob, nil, time.Minute); err != nil {
		t.Fatal(err)
	}

	rows, err := PresenceAt(ctx, pool, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("presence = %+v, want two rows", rows)
	}
	var aliceRow *Presence
	for i := range rows {
		if rows[i].Recipient == alice {
			aliceRow = &rows[i]
		}
	}
	if aliceRow == nil || string(aliceRow.Payload) != `{"field": "title"}` {
		t.Fatalf("alice's row = %+v", aliceRow)
	}

	// A heartbeat re-arms: expires_at moves forward on the same row.
	var before time.Time
	if err := pool.QueryRow(ctx,
		`SELECT expires_at FROM cb_wire_presence WHERE topic = $1 AND recipient = $2`,
		topic, alice).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := Appear(ctx, pool, topic, alice, map[string]any{"field": "title"}, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	var after time.Time
	if err := pool.QueryRow(ctx,
		`SELECT expires_at FROM cb_wire_presence WHERE topic = $1 AND recipient = $2`,
		topic, alice).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.After(before) {
		t.Fatalf("heartbeat did not re-arm: %v → %v", before, after)
	}

	// The polite leave removes at once.
	gone, err := Disappear(ctx, pool, topic, bob)
	if err != nil {
		t.Fatal(err)
	}
	if !gone {
		t.Fatal("expected disappear to report true")
	}
	rows, err = PresenceAt(ctx, pool, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Recipient != alice {
		t.Fatalf("presence after leave = %+v, want alice alone", rows)
	}

	// Silence: an expired row never renders, pruned or not, and the prune
	// removes it.
	if _, err := pool.Exec(ctx,
		`UPDATE cb_wire_presence SET expires_at = now() - interval '1 second' WHERE topic = $1`,
		topic); err != nil {
		t.Fatal(err)
	}
	rows, err = PresenceAt(ctx, pool, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expired rows rendered: %+v", rows)
	}
	var pruned int64
	if err := pool.QueryRow(ctx, `SELECT cb_wire_prune_presence()`).Scan(&pruned); err != nil {
		t.Fatal(err)
	}
	if pruned < 1 {
		t.Fatalf("pruned %d presence rows, want at least 1", pruned)
	}

	// Guards: empty names and a zero ttl are refused.
	if err := Appear(ctx, pool, "", alice, nil, time.Minute); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty topic: err = %v, want ErrInvalid", err)
	}
	if err := Appear(ctx, pool, topic, "", nil, time.Minute); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty recipient: err = %v, want ErrInvalid", err)
	}
	if err := Appear(ctx, pool, topic, alice, nil, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero ttl: err = %v, want ErrInvalid", err)
	}
}

// TestPresenceNudges verifies the change discipline over SSE: arriving,
// changing detail, coming back from expired and leaving all nudge the
// topic's watchers, while a bare heartbeat is silent — heartbeats must
// never spam refetches.
func TestPresenceNudges(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()
	w := New(pool, testSecret, Opts{Notifier: testNotifier(t)})
	startTestWire(t, w)

	topic := "gw.pn." + uuid.NewString()[:8] + ".presence"
	alice := testRecipient("nudger")
	read := sseReader(connectSSE(t, w, w.Token([]string{topic})))

	// Arrival nudges: an empty frame named after the topic.
	if err := Appear(ctx, pool, topic, alice, map[string]any{"field": "title"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	events := read(1, 2*time.Second)
	if len(events) != 1 || events[0].event != topic || events[0].data != "" {
		t.Fatalf("arrival events = %+v, want one empty frame named %s", events, topic)
	}

	// A bare heartbeat is silent; the detail change right after is the
	// fence proving the heartbeat sent nothing.
	if err := Appear(ctx, pool, topic, alice, map[string]any{"field": "title"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := Appear(ctx, pool, topic, alice, map[string]any{"field": "abstract"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	events = read(2, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("heartbeat+change events = %+v, want exactly one (the change)", events)
	}

	// Leaving nudges too.
	if _, err := Disappear(ctx, pool, topic, alice); err != nil {
		t.Fatal(err)
	}
	events = read(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("leave events = %+v, want one", events)
	}
}
