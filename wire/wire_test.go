package wire

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/notify"
)

var testSecret = []byte("01234567890123456789012345678901") // 32 bytes

var (
	setupOnce sync.Once
	testPool  *pgxpool.Pool

	notifierOnce  sync.Once
	suiteNotifier *notify.Notifier
)

// testNotifier starts one notifier for the whole suite, the way a real
// process runs one for all its wires. It lives until the process ends.
func testNotifier(t testing.TB) *notify.Notifier {
	t.Helper()
	pool := setupTest(t)
	notifierOnce.Do(func() {
		suiteNotifier = notify.New(pool)
		go func() { _ = suiteNotifier.Start(context.Background()) }()
	})
	return suiteNotifier
}

// setupTest migrates once per process and wipes leftovers from earlier
// runs: inbox rows persist in the shared table, so every test uses
// gw_-prefixed identities and the wipe targets those.
func setupTest(t testing.TB) *pgxpool.Pool {
	t.Helper()
	setupOnce.Do(func() {
		db, err := sql.Open("pgx", testDSN)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		if err := MigrateUpTo(context.Background(), db, SchemaVersion); err != nil {
			panic(err)
		}
		if _, err := db.Exec(`DELETE FROM cb_wire_inbox WHERE identity LIKE 'gw\_%'`); err != nil {
			panic(err)
		}
		testPool, err = pgxpool.New(context.Background(), testDSN)
		if err != nil {
			panic(err)
		}
	})
	return testPool
}

// startTestWire runs the wire until the test ends and fails the test on
// anything but a clean shutdown. It leaves a moment for the notifier's
// LISTEN to apply, so an event sent right after is not lost.
func startTestWire(t testing.TB, w *Wire) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("wire: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("wire did not stop")
		}
	})
	time.Sleep(250 * time.Millisecond)
}

// connectSSE opens an SSE connection against the wire and returns the
// response, cleaned up with the test.
func connectSSE(t *testing.T, w *Wire, token string) *http.Response {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.ServeSSE(rw, r, r.URL.Query().Get("token"))
	}))
	t.Cleanup(srv.Close)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(srv.URL + "?token=" + token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE connect status = %d", resp.StatusCode)
	}
	time.Sleep(100 * time.Millisecond)
	return resp
}

// --- Token tests ---

func TestTokenMintVerify(t *testing.T) {
	w := New(setupTest(t), testSecret)

	token := w.Token([]string{"a.b", "c.*"}, TokenOpts{Identity: "alice", ValidFor: time.Hour})
	p, err := w.verifyToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if p.Identity != "alice" {
		t.Errorf("identity = %q, want alice", p.Identity)
	}
	if len(p.Topics) != 2 || p.Topics[0] != "a.b" || p.Topics[1] != "c.*" {
		t.Errorf("topics = %v", p.Topics)
	}
	if p.Expiry == 0 {
		t.Error("expected an expiry")
	}
}

func TestTokenNoOpts(t *testing.T) {
	w := New(setupTest(t), testSecret)

	token := w.Token([]string{"a"})
	p, err := w.verifyToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if p.Identity != "" || p.Expiry != 0 {
		t.Errorf("payload = %+v, want no identity, no expiry", p)
	}
}

func TestTokenExpired(t *testing.T) {
	w := New(setupTest(t), testSecret)

	// Build an expired token directly (Token ignores non-positive ValidFor).
	p := tokenPayload{Topics: []string{"a"}, Expiry: time.Now().Add(-time.Minute).Unix()}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	ct, err := encrypt(w.secret, b)
	if err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString(ct)

	if _, err := w.verifyToken(token); err == nil {
		t.Fatal("expected an expired-token error")
	}
}

func TestTokenWrongSecret(t *testing.T) {
	w := New(setupTest(t), testSecret)
	other := New(setupTest(t), []byte("abcdefghijklmnopqrstuvwxyz012345"))

	token := w.Token([]string{"a"})
	if _, err := other.verifyToken(token); err == nil {
		t.Fatal("expected an invalid-token error")
	}
}

func TestTokenInvalid(t *testing.T) {
	w := New(setupTest(t), testSecret)
	if _, err := w.verifyToken("not-a-token"); err == nil {
		t.Fatal("expected an error")
	}
}

// --- SSE tests ---

func TestServeSSEUnauthorized(t *testing.T) {
	w := New(setupTest(t), testSecret)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/events/bad-token", nil)
	w.ServeSSE(rr, req, "bad-token")

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestNotifySSE(t *testing.T) {
	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	startTestWire(t, w)

	topic := "gw.sse." + uuid.NewString()[:8]
	resp := connectSSE(t, w, w.Token([]string{topic}))

	// The cross-process path: pg NOTIFY with no sender to skip.
	if err := Notify(t.Context(), testPool, topic, "<div>hello</div>"); err != nil {
		t.Fatal(err)
	}

	events := readSSEEvents(t, resp, 1, 2*time.Second)
	if len(events) == 0 {
		t.Fatal("no SSE events received")
	}
	if events[0].event != topic {
		t.Errorf("event = %q, want %q", events[0].event, topic)
	}
	if events[0].data != "<div>hello</div>" {
		t.Errorf("data = %q, want %q", events[0].data, "<div>hello</div>")
	}
}

func TestNotifySSEWildcard(t *testing.T) {
	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	startTestWire(t, w)

	base := "gw.wc." + uuid.NewString()[:8]
	resp := connectSSE(t, w, w.Token([]string{base + ".#"}))

	subtopic := base + ".batch_edit.done"
	if err := Notify(t.Context(), testPool, subtopic, "finished"); err != nil {
		t.Fatal(err)
	}

	events := readSSEEvents(t, resp, 1, 2*time.Second)
	if len(events) == 0 {
		t.Fatal("no SSE events received for wildcard subscription")
	}
	if events[0].event != subtopic {
		t.Errorf("event = %q, want %q", events[0].event, subtopic)
	}
	if events[0].data != "finished" {
		t.Errorf("data = %q, want %q", events[0].data, "finished")
	}
}

// TestNilNotifierLocalDelivery proves the nil-notifier single-process
// configuration works: the wire's own Notify delivers to its SSE
// subscribers and Listen handlers without any LISTEN connection.
func TestNilNotifierLocalDelivery(t *testing.T) {
	w := New(setupTest(t), testSecret)
	startTestWire(t, w)

	topic := "gw.local." + uuid.NewString()[:8]

	var mu sync.Mutex
	var heard []string
	w.Listen(topic, func(ctx context.Context, topic, message string) {
		mu.Lock()
		heard = append(heard, message)
		mu.Unlock()
	})

	resp := connectSSE(t, w, w.Token([]string{topic}))

	if err := w.Notify(t.Context(), topic, "ping"); err != nil {
		t.Fatal(err)
	}

	events := readSSEEvents(t, resp, 1, 2*time.Second)
	if len(events) != 1 || events[0].data != "ping" {
		t.Fatalf("events = %+v, want one ping", events)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(heard) != 1 || heard[0] != "ping" {
		t.Fatalf("listen heard = %v, want [ping]", heard)
	}
}

// TestTwoWiresCrossDeliver proves two wires sharing one notifier
// cross-deliver: what one sends, the other's subscribers receive.
func TestTwoWiresCrossDeliver(t *testing.T) {
	a := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	b := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	startTestWire(t, a)
	startTestWire(t, b)

	topic := "gw.cross." + uuid.NewString()[:8]
	resp := connectSSE(t, b, b.Token([]string{topic}))

	if err := a.Notify(t.Context(), topic, "over"); err != nil {
		t.Fatal(err)
	}

	events := readSSEEvents(t, resp, 1, 2*time.Second)
	if len(events) != 1 || events[0].data != "over" {
		t.Fatalf("events = %+v, want one event from the other wire", events)
	}
}

// --- Listen tests ---

func TestListenExactTopic(t *testing.T) {
	topic := "gw.listen." + uuid.NewString()[:8]

	var mu sync.Mutex
	var received []string

	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	w.Listen(topic, func(ctx context.Context, topic, message string) {
		mu.Lock()
		received = append(received, topic)
		mu.Unlock()
	})
	startTestWire(t, w)

	if err := Notify(t.Context(), testPool, topic, ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0] != topic {
		t.Errorf("received = %v, want [%s]", received, topic)
	}
}

func TestListenWildcards(t *testing.T) {
	prefix := "gw.lwc." + uuid.NewString()[:8]

	var mu sync.Mutex
	var star, hash []string

	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	w.Listen(prefix+".*", func(ctx context.Context, topic, message string) {
		mu.Lock()
		star = append(star, topic)
		mu.Unlock()
	})
	w.Listen(prefix+".#", func(ctx context.Context, topic, message string) {
		mu.Lock()
		hash = append(hash, topic)
		mu.Unlock()
	})
	startTestWire(t, w)

	for _, topic := range []string{prefix, prefix + ".created", prefix + ".sub.created"} {
		if err := Notify(t.Context(), testPool, topic, ""); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// * matches exactly one extra token; # matches zero or more.
	if len(star) != 1 || star[0] != prefix+".created" {
		t.Errorf("star matches = %v, want [%s]", star, prefix+".created")
	}
	if len(hash) != 3 {
		t.Errorf("hash matches = %v, want all 3", hash)
	}
}

func TestListenMultipleHandlers(t *testing.T) {
	topic := "gw.lmulti." + uuid.NewString()[:8]

	var mu sync.Mutex
	var count int
	inc := func(ctx context.Context, topic, message string) {
		mu.Lock()
		count++
		mu.Unlock()
	}

	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	w.Listen(topic, inc)
	w.Listen(topic, inc)
	startTestWire(t, w)

	if err := Notify(t.Context(), testPool, topic, ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestListenNoMatch(t *testing.T) {
	prefix := "gw.lnomatch." + uuid.NewString()[:8]

	var mu sync.Mutex
	var received bool

	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	w.Listen(prefix+".specific", func(ctx context.Context, topic, message string) {
		mu.Lock()
		received = true
		mu.Unlock()
	})
	startTestWire(t, w)

	if err := Notify(t.Context(), testPool, prefix+".other", ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if received {
		t.Error("handler was called for non-matching topic")
	}
}

func TestListenSkipSelf(t *testing.T) {
	topic := "gw.lskipself." + uuid.NewString()[:8]

	var mu sync.Mutex
	var count int

	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	w.Listen(topic, func(ctx context.Context, topic, message string) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	startTestWire(t, w)

	// SentBy = this wire's ID: the wire skips the echo of what it already
	// delivered locally — here nothing was delivered locally, so nothing
	// arrives at all.
	if err := Notify(t.Context(), testPool, topic, "", NotifyOpts{SentBy: w.ID()}); err != nil {
		t.Fatal(err)
	}
	// No sender to skip: delivered.
	if err := Notify(t.Context(), testPool, topic, ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Errorf("count = %d, want 1 (skip-self should have prevented one delivery)", count)
	}
}

// --- Dispatch queue ---

// TestDispatchOverflowDrops proves the notifier-side callbacks never
// block: with the dispatch queue full they drop the event and return,
// so a stalled wire cannot stall the process's shared LISTEN connection.
func TestDispatchOverflowDrops(t *testing.T) {
	w := New(setupTest(t), testSecret) // never Started: nothing drains the queue

	for range dispatchSize {
		w.dispatch <- dispatchEvent{topic: "gw.fill"}
	}

	done := make(chan struct{})
	go func() {
		w.enqueueBus(`{"sent_by": null, "topic": "gw.overflow", "message": ""}`)
		w.enqueueInbox("gw_someone")
		w.enqueueInboxReconnect()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked on a full dispatch queue")
	}
	if len(w.dispatch) != dispatchSize {
		t.Fatalf("queue length = %d, want %d (dropped, not grown)", len(w.dispatch), dispatchSize)
	}
}

// --- Poll transport ---

func TestServePollUnauthorized(t *testing.T) {
	w := New(setupTest(t), testSecret)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/poll", nil)
	w.ServePoll(rr, req, "bad-token")

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestServePollRequiresIdentity(t *testing.T) {
	w := New(setupTest(t), testSecret)

	// A token without an identity can't address the identity-keyed inbox.
	token := w.Token([]string{"notif.#"})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/poll", nil)
	w.ServePoll(rr, req, token)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

// TestServePoll exercises the poll transport end to end: the same Render
// definition projects stored inbox rows, the token's topic scope filters the
// identity-keyed inbox to a subset, the cursor advances past skipped rows, and the
// poll is a pure read (no ack). The rows are written while no SSE client is
// connected — this is also the offline catch-up path: a client that missed
// every nudge finds the rows on its next poll.
func TestServePoll(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()
	w := New(pool, testSecret)

	identity := "gw_poll_" + uuid.NewString()[:8]
	base := "notif." + uuid.NewString()[:8]
	drawerTopic := base + ".message"
	otherTopic := "change." + uuid.NewString()[:8] // out of the token's scope

	// One renderer, shared by both transports: wrap the message in an <li>.
	w.Render(base+".#", func(r *http.Request, topic, message string) (Fragment, error) {
		return Fragment{Data: "<li>" + message + "</li>"}, nil
	})

	// Seed the inbox: two in-scope rows interleaved with one out-of-scope row.
	if _, err := NotifyDurable(ctx, pool, identity, drawerTopic, "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := NotifyDurable(ctx, pool, identity, otherTopic, "ignored"); err != nil {
		t.Fatal(err)
	}
	lastID, err := NotifyDurable(ctx, pool, identity, drawerTopic, "two")
	if err != nil {
		t.Fatal(err)
	}

	// Identity token scoped to the drawer subset only.
	token := w.Token([]string{base + ".#"}, TokenOpts{Identity: identity})

	rr := httptest.NewRecorder()
	w.ServePoll(rr, httptest.NewRequest("GET", "/poll?after=0", nil), token)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	// Only in-scope rows rendered, in cursor order; the out-of-scope row is skipped.
	if got := rr.Body.String(); got != "<li>one</li><li>two</li>" {
		t.Fatalf("body = %q", got)
	}
	// Cursor advances past every fetched row, including the skipped one.
	if got := rr.Header().Get("X-Wire-Cursor"); got != strconv.FormatInt(lastID, 10) {
		t.Fatalf("cursor = %q, want %d", got, lastID)
	}

	// Pure read: nothing was acked, so all three rows are still unseen.
	unseen, err := ReadUnseen(ctx, pool, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(unseen) != 3 {
		t.Fatalf("expected 3 rows still unseen (poll must not ack), got %d", len(unseen))
	}

	// Polling again from the returned cursor is caught up: empty body.
	rr2 := httptest.NewRecorder()
	w.ServePoll(rr2, httptest.NewRequest("GET", "/poll?after="+strconv.FormatInt(lastID, 10), nil), token)
	if rr2.Body.Len() != 0 {
		t.Fatalf("expected empty body on caught-up poll, got %q", rr2.Body.String())
	}
}

// --- The inbox nudge ---

// TestInboxNudgeReachesOnlyItsIdentity proves the nudge is
// identity-addressed: NotifyDurable for one identity sends the reserved
// inbox frame to that identity's connections and to nobody else.
func TestInboxNudgeReachesOnlyItsIdentity(t *testing.T) {
	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	startTestWire(t, w)

	topic := "gw.nudge." + uuid.NewString()[:8]
	alice := "gw_alice_" + uuid.NewString()[:8]
	bob := "gw_bob_" + uuid.NewString()[:8]

	aliceResp := connectSSE(t, w, w.Token([]string{topic}, TokenOpts{Identity: alice}))
	bobResp := connectSSE(t, w, w.Token([]string{topic}, TokenOpts{Identity: bob}))

	if _, err := NotifyDurable(t.Context(), testPool, alice, topic, "for alice"); err != nil {
		t.Fatal(err)
	}

	events := readSSEEvents(t, aliceResp, 1, 2*time.Second)
	if len(events) != 1 || events[0].event != "inbox" {
		t.Fatalf("alice events = %+v, want one inbox frame", events)
	}
	if events[0].data != "" {
		t.Fatalf("inbox frame data = %q, want empty (the client re-polls)", events[0].data)
	}

	if events := readSSEEvents(t, bobResp, 1, 500*time.Millisecond); len(events) != 0 {
		t.Fatalf("bob events = %+v, want none", events)
	}
}

// TestNotifyDurableRollback proves the failure pair: a rolled-back
// NotifyDurable delivers neither the row nor the nudge, and a committed
// one delivers both.
func TestNotifyDurableRollback(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()
	w := New(pool, testSecret, Opts{Notifier: testNotifier(t)})
	startTestWire(t, w)

	identity := "gw_rollback_" + uuid.NewString()[:8]
	topic := "gw.rollback." + uuid.NewString()[:8]
	resp := connectSSE(t, w, w.Token([]string{topic}, TokenOpts{Identity: identity}))

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NotifyDurable(ctx, tx, identity, topic, "never happened"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// The committed insert that follows is the fence: its nudge arriving
	// first (and alone) proves the rolled-back one never fired.
	if _, err := NotifyDurable(ctx, pool, identity, topic, "committed"); err != nil {
		t.Fatal(err)
	}

	events := readSSEEvents(t, resp, 2, 2*time.Second)
	if len(events) != 1 || events[0].event != "inbox" {
		t.Fatalf("events = %+v, want exactly one inbox frame", events)
	}

	unseen, err := ReadUnseen(ctx, pool, identity, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(unseen) != 1 || unseen[0].Message != "committed" {
		t.Fatalf("unseen = %+v, want only the committed row", unseen)
	}
}

// --- SSE test helpers ---

type sseEvent struct {
	event string
	data  string
}

func readSSEEvents(t *testing.T, resp *http.Response, count int, timeout time.Duration) []sseEvent {
	t.Helper()

	ch := make(chan sseEvent, count)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		var currentEvent, currentData string
		for scanner.Scan() {
			line := scanner.Text()
			if v, ok := strings.CutPrefix(line, "event: "); ok {
				currentEvent = v
			} else if v, ok := strings.CutPrefix(line, "data: "); ok {
				currentData = v
			} else if line == "data:" {
				currentData = ""
			} else if line == "" && currentEvent != "" {
				ch <- sseEvent{event: currentEvent, data: currentData}
				currentEvent = ""
				currentData = ""
			}
		}
	}()

	var events []sseEvent
	deadline := time.After(timeout)
	for range count {
		select {
		case ev := <-ch:
			events = append(events, ev)
		case <-deadline:
			return events
		}
	}
	return events
}
