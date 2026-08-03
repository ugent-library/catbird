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
// runs: rows persist in the shared tables, so every test uses gw_-prefixed
// recipients and gwr-prefixed relay names, and the wipe targets those.
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
		for _, q := range []string{
			`DELETE FROM cb_wire_inbox WHERE recipient LIKE 'gw\_%'`,
			`DELETE FROM cb_wire_subscriptions WHERE recipient LIKE 'gw\_%'`,
			`DELETE FROM cb_wire_presence WHERE recipient LIKE 'gw\_%'`,
			`DELETE FROM cb_wire_relays WHERE name LIKE 'gwr%'`,
		} {
			if _, err := db.Exec(q); err != nil {
				panic(err)
			}
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
// LISTEN to apply, so a frame sent right after is not lost.
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

	token := w.Token([]string{"a.b", "c.*"}, TokenOpts{Recipient: "alice", ValidFor: time.Hour})
	p, err := w.verifyToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if p.Recipient != "alice" {
		t.Errorf("recipient = %q, want alice", p.Recipient)
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
	if p.Recipient != "" || p.Expiry != 0 {
		t.Errorf("payload = %+v, want no recipient, no expiry", p)
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

// --- SSE surface ---

func TestServeSSEUnauthorized(t *testing.T) {
	w := New(setupTest(t), testSecret)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/events/bad-token", nil)
	w.ServeSSE(rr, req, "bad-token")

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

// --- Dispatch queue ---

// TestDispatchOverflowDrops proves the notifier-side callbacks never
// block: with the dispatch queue full they drop the frame and return,
// so a stalled wire cannot stall the process's shared LISTEN connection.
func TestDispatchOverflowDrops(t *testing.T) {
	w := New(setupTest(t), testSecret) // never Started: nothing drains the queue

	for range dispatchSize {
		w.dispatch <- frame{Topic: "gw.fill"}
	}

	done := make(chan struct{})
	go func() {
		w.enqueueFrame(`{"stream": "s", "pos": 1, "topic": "gw.overflow"}`)
		w.enqueueFrame(`{"topic": "gw.overflow.presence"}`)
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

func TestServePollRequiresRecipient(t *testing.T) {
	w := New(setupTest(t), testSecret)

	// A token without a recipient can't address the recipient-keyed inbox.
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
// recipient-keyed inbox to a subset, the cursor advances past skipped rows,
// and the poll is a pure read (no ack). The rows are written while no SSE
// client is connected — this is also the offline catch-up path: a client
// that missed every nudge finds the rows on its next poll.
func TestServePoll(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()
	w := New(pool, testSecret)

	recipient := "gw_poll_" + uuid.NewString()[:8]
	base := "notif." + uuid.NewString()[:8]
	drawerTopic := base + ".message"
	otherTopic := "change." + uuid.NewString()[:8] // out of the token's scope

	// One typed renderer, shared by both transports: wrap the payload in an <li>.
	Render(w, base+".#", func(r *http.Request, topic string, data string) (Fragment, error) {
		return Fragment{Data: "<li>" + data + "</li>"}, nil
	})

	// Seed the inbox: two in-scope rows interleaved with one out-of-scope row.
	if _, err := Send(ctx, pool, recipient, drawerTopic, "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := Send(ctx, pool, recipient, otherTopic, "ignored"); err != nil {
		t.Fatal(err)
	}
	lastID, err := Send(ctx, pool, recipient, drawerTopic, "two")
	if err != nil {
		t.Fatal(err)
	}

	// Recipient token scoped to the drawer subset only.
	token := w.Token([]string{base + ".#"}, TokenOpts{Recipient: recipient})

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
	unseen, err := ReadUnseen(ctx, pool, recipient, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(unseen) != 3 {
		t.Fatalf("expected 3 rows still unseen (poll must not ack), got %d", len(unseen))
	}

	// The JSON mode returns the same in-scope rows as data.
	rj := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/poll?after=0", nil)
	req.Header.Set("Accept", "application/json")
	w.ServePoll(rj, req, token)
	if ct := rj.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("json mode content type = %q", ct)
	}
	var rows []Notification
	if err := json.Unmarshal(rj.Body.Bytes(), &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Payload != `"one"` || rows[1].Payload != `"two"` {
		t.Fatalf("json rows = %+v", rows)
	}

	// Polling again from the returned cursor is caught up: empty body.
	rr2 := httptest.NewRecorder()
	w.ServePoll(rr2, httptest.NewRequest("GET", "/poll?after="+strconv.FormatInt(lastID, 10), nil), token)
	if rr2.Body.Len() != 0 {
		t.Fatalf("expected empty body on caught-up poll, got %q", rr2.Body.String())
	}
}

// --- The inbox nudge ---

// TestInboxNudgeReachesOnlyItsRecipient proves the nudge is
// recipient-addressed: Send for one recipient sends the reserved
// inbox frame to that recipient's connections and to nobody else.
func TestInboxNudgeReachesOnlyItsRecipient(t *testing.T) {
	w := New(setupTest(t), testSecret, Opts{Notifier: testNotifier(t)})
	startTestWire(t, w)

	topic := "gw.nudge." + uuid.NewString()[:8]
	alice := "gw_alice_" + uuid.NewString()[:8]
	bob := "gw_bob_" + uuid.NewString()[:8]

	aliceResp := connectSSE(t, w, w.Token([]string{topic}, TokenOpts{Recipient: alice}))
	bobResp := connectSSE(t, w, w.Token([]string{topic}, TokenOpts{Recipient: bob}))

	if _, err := Send(t.Context(), testPool, alice, topic, "for alice"); err != nil {
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

// TestSendRollback proves the failure pair: a rolled-back Send delivers
// neither the row nor the nudge, and a committed one delivers both.
func TestSendRollback(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()
	w := New(pool, testSecret, Opts{Notifier: testNotifier(t)})
	startTestWire(t, w)

	recipient := "gw_rollback_" + uuid.NewString()[:8]
	topic := "gw.rollback." + uuid.NewString()[:8]
	resp := connectSSE(t, w, w.Token([]string{topic}, TokenOpts{Recipient: recipient}))

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Send(ctx, tx, recipient, topic, "never happened"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// The committed insert that follows is the fence: its nudge arriving
	// first (and alone) proves the rolled-back one never fired.
	if _, err := Send(ctx, pool, recipient, topic, "committed"); err != nil {
		t.Fatal(err)
	}

	events := readSSEEvents(t, resp, 2, 2*time.Second)
	if len(events) != 1 || events[0].event != "inbox" {
		t.Fatalf("events = %+v, want exactly one inbox frame", events)
	}

	unseen, err := ReadUnseen(ctx, pool, recipient, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(unseen) != 1 || unseen[0].Payload != `"committed"` {
		t.Fatalf("unseen = %+v, want only the committed row", unseen)
	}
}

// --- The glue ---

func TestServeScript(t *testing.T) {
	w := New(setupTest(t), testSecret)

	rr := httptest.NewRecorder()
	w.ServeScript(rr, httptest.NewRequest("GET", "/wire.js", nil))

	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Fatalf("content type = %q", ct)
	}
	if !strings.Contains(rr.Body.String(), "window.wire") {
		t.Fatal("script body does not define window.wire")
	}
}

// --- SSE test helpers ---

type sseEvent struct {
	event string
	data  string
}

// sseReader starts one scanner goroutine for the connection and returns a
// pull function. A test that reads in phases must use one reader — a
// second scanner on the same body would race the first for its bytes.
func sseReader(resp *http.Response) func(count int, timeout time.Duration) []sseEvent {
	ch := make(chan sseEvent, 64)
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

	return func(count int, timeout time.Duration) []sseEvent {
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
}

func readSSEEvents(t *testing.T, resp *http.Response, count int, timeout time.Duration) []sseEvent {
	t.Helper()
	return sseReader(resp)(count, timeout)
}
