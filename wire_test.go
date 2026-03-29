package catbird

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var testSecret = []byte("01234567890123456789012345678901") // 32 bytes

func startTestWire(t *testing.T, wire *Wire) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)

	go func() {
		errCh <- wire.Start(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("wire.Start() failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("wire shutdown error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Log("wire did not shutdown within timeout")
		}
	})
}

func TestWireTokenMintVerify(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)

	token := wire.Token([]string{"work.01JABC", "user.01JXYZ"}, TokenOpts{
		Identity: "user-123",
		ValidFor: time.Hour,
	})

	payload, err := wire.verifyToken(token)
	if err != nil {
		t.Fatalf("verifyToken: %v", err)
	}
	if len(payload.Topics) != 2 || payload.Topics[0] != "work.01JABC" || payload.Topics[1] != "user.01JXYZ" {
		t.Errorf("topics = %v", payload.Topics)
	}
	if payload.Identity != "user-123" {
		t.Errorf("identity = %q", payload.Identity)
	}
}

func TestWireTokenNoOpts(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)

	token := wire.Token([]string{"topic-a"})
	payload, err := wire.verifyToken(token)
	if err != nil {
		t.Fatalf("verifyToken: %v", err)
	}
	if payload.Identity != "" {
		t.Errorf("expected empty identity, got %q", payload.Identity)
	}
	if payload.Expiry != 0 {
		t.Errorf("expected no expiry, got %d", payload.Expiry)
	}
}

func TestWireTokenExpired(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)

	// Build an expired token directly (Token() ignores non-positive ValidFor)
	p := tokenPayload{Topics: []string{"topic"}, Expiry: time.Now().Add(-time.Minute).Unix()}
	b, _ := json.Marshal(p)
	ct, _ := wireEncrypt(wire.secret, b)
	token := base64.RawURLEncoding.EncodeToString(ct)

	_, err := wire.verifyToken(token)
	if err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestWireTokenWrongSecret(t *testing.T) {
	getTestClient(t)
	wire1 := NewWire(testPool, testSecret)
	wire2 := NewWire(testPool, []byte("different-secret-key-32-bytesXXX"))

	token := wire1.Token([]string{"topic"})
	_, err := wire2.verifyToken(token)
	if err == nil {
		t.Fatal("expected error for wrong secret")
	}
}

func TestWireTokenInvalid(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)

	_, err := wire.verifyToken("not-a-valid-token")
	if err == nil {
		t.Fatal("expected error for garbage token")
	}
}

func TestWireServeSSEUnauthorized(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)
	startTestWire(t, wire)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/events/bad-token", nil)
	wire.ServeSSE(rr, req, "bad-token")

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestWireNotifySSE(t *testing.T) {
	cb := getTestClient(t)
	wire := NewWire(testPool, testSecret)
	startTestWire(t, wire)

	topic := "test." + uuid.NewString()[:8]
	token := wire.Token([]string{topic})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire.ServeSSE(w, r, r.URL.Query().Get("token"))
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(srv.URL + "?token=" + token)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	time.Sleep(100 * time.Millisecond)

	// Notify via Client (goes through pg_notify, cross-node path)
	if err := cb.Notify(t.Context(), topic, ""); err != nil {
		t.Fatal(err)
	}

	// Read SSE event — event name is the topic
	events := readSSEEvents(t, resp, 1, 2*time.Second)
	if len(events) == 0 {
		t.Fatal("no SSE events received")
	}
	if events[0].event != topic {
		t.Errorf("event = %q, want %q", events[0].event, topic)
	}
}

func TestWireNotifyWithData(t *testing.T) {
	cb := getTestClient(t)
	wire := NewWire(testPool, testSecret)
	startTestWire(t, wire)

	topic := "test." + uuid.NewString()[:8]
	token := wire.Token([]string{topic})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire.ServeSSE(w, r, r.URL.Query().Get("token"))
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(srv.URL + "?token=" + token)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	time.Sleep(100 * time.Millisecond)

	if err := cb.Notify(t.Context(), topic, `<div>hello</div>`); err != nil {
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

func TestWireLocalNotify(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)
	startTestWire(t, wire)

	topic := "test." + uuid.NewString()[:8]
	token := wire.Token([]string{topic})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire.ServeSSE(w, r, r.URL.Query().Get("token"))
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(srv.URL + "?token=" + token)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	time.Sleep(100 * time.Millisecond)

	// Notify via Wire (local + pg_notify)
	if err := wire.Notify(t.Context(), topic, ""); err != nil {
		t.Fatal(err)
	}

	events := readSSEEvents(t, resp, 1, 2*time.Second)
	if len(events) == 0 {
		t.Fatal("no SSE events received")
	}
	if events[0].event != topic {
		t.Errorf("event = %q, want %q", events[0].event, topic)
	}
}

func TestWirePresence(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)
	startTestWire(t, wire)

	topic := "presence." + uuid.NewString()[:8]
	token := wire.Token([]string{topic}, TokenOpts{Identity: "alice"})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire.ServeSSE(w, r, r.URL.Query().Get("token"))
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(srv.URL + "?token=" + token)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	identities, err := wire.Presence(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 || identities[0] != "alice" {
		t.Errorf("presence = %v, want [alice]", identities)
	}

	// Disconnect
	resp.Body.Close()
	time.Sleep(200 * time.Millisecond)

	identities, err = wire.Presence(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 0 {
		t.Errorf("presence after disconnect = %v, want []", identities)
	}
}

func TestWirePresenceMultipleConnections(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)
	startTestWire(t, wire)

	topic := "presence." + uuid.NewString()[:8]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire.ServeSSE(w, r, r.URL.Query().Get("token"))
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	// Two connections from same identity
	token1 := wire.Token([]string{topic}, TokenOpts{Identity: "bob"})
	resp1, err := client.Get(srv.URL + "?token=" + token1)
	if err != nil {
		t.Fatal(err)
	}
	defer resp1.Body.Close()

	token2 := wire.Token([]string{topic}, TokenOpts{Identity: "bob"})
	resp2, err := client.Get(srv.URL + "?token=" + token2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	time.Sleep(200 * time.Millisecond)

	// Should still show one identity (DISTINCT)
	identities, err := wire.Presence(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 || identities[0] != "bob" {
		t.Errorf("presence = %v, want [bob]", identities)
	}

	// Close first connection
	resp1.Body.Close()
	time.Sleep(200 * time.Millisecond)

	// Still present via second connection
	identities, err = wire.Presence(t.Context(), topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 || identities[0] != "bob" {
		t.Errorf("presence after first disconnect = %v, want [bob]", identities)
	}
}

func TestWireDeliverLocal(t *testing.T) {
	getTestClient(t)
	wire := NewWire(testPool, testSecret)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sub := &wireSubscriber{
		id:           uuid.NewString(),
		ch:           make(chan wireEvent, wireChannelSize),
		topics:       []string{"topic-a"},
		cancel:       cancel,
		lastDelivery: time.Now(),
	}

	wire.addSubscriber(sub)
	defer wire.removeSubscriber(sub)

	wire.deliverLocal("topic-a", wireEvent{topic: "topic-a", message: "pong"})

	select {
	case ev := <-sub.ch:
		if ev.topic != "topic-a" {
			t.Errorf("topic = %q, want %q", ev.topic, "topic-a")
		}
		if ev.message != "pong" {
			t.Errorf("message = %q, want %q", ev.message, "pong")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// Deliver to non-existent topic — should not panic
	wire.deliverLocal("no-such-topic", wireEvent{topic: "no-such-topic", message: ""})

	_ = ctx
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
