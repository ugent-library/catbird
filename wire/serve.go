package wire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fragment is a rendered projection of a (topic, message) for client delivery. Data is
// the transport-neutral substance (e.g. an HTML fragment): ServePoll emits it directly,
// ServeSSE puts it in data:, an HTMX-WebSocket transport would send it as the frame.
// Event and ID are SSE-native framing hints — SSE uses them for event:/id:, poll and
// HTMX-WebSocket ignore them, and a JSON-WebSocket or Web Push transport would re-encode
// them (message type / dedup tag). Structured push notifications are otherwise a
// different projection and aren't modeled here.
//
// The event name "inbox" is reserved: wire sends it (with empty data) when
// the connected identity's inbox grows, and the client answers by re-pulling
// its poll endpoint.
type Fragment struct {
	Event string // SSE event name (event:); defaults to the topic. Ignored by poll and HTMX-WebSocket.
	Data  string // rendered content (e.g. an HTML fragment) — the transport-neutral substance
	ID    string // SSE id (id:); optional dedup/identity hint. Informational (no Last-Event-ID replay).
}

// Write implements io.Writer, appending p to the Data field.
// This allows Fragment to be used as a target for io.WriterTo (e.g. Templ components).
func (f *Fragment) Write(p []byte) (int, error) {
	f.Data += string(p)
	return len(p), nil
}

// RenderHandler projects a wire event into a transport-neutral Fragment for client
// delivery. It receives the client's HTTP request for access to user context
// (auth, language, etc). Only topics with a registered renderer are delivered —
// the renderer acts as an allowlist.
type RenderHandler = func(r *http.Request, topic, message string) (Fragment, error)

// subscriber is one SSE connection: its event channel, the token's
// grants, and the request that opened it.
type subscriber struct {
	ch       chan event
	identity string
	topics   []string
	cancel   func()
	request  *http.Request // SSE client's HTTP request for Render context

	mu           sync.Mutex
	lastDelivery time.Time
}

// push hands an event to the connection without blocking. A full channel
// whose reader has not taken delivery for slowConsumerTimeout is a dead
// or stuck client: its connection is cancelled.
func (s *subscriber) push(ev event) {
	select {
	case s.ch <- ev:
	default:
		s.mu.Lock()
		if time.Since(s.lastDelivery) > slowConsumerTimeout {
			s.cancel()
		}
		s.mu.Unlock()
	}
}

// ServeSSE serves an SSE connection for the given token string.
// Invalid or expired tokens result in a 401 response.
func (w *Wire) ServeSSE(rw http.ResponseWriter, r *http.Request, token string) {
	payload, err := w.verifyToken(token)
	if err != nil {
		http.Error(rw, "Unauthorized", http.StatusUnauthorized)
		return
	}

	flusher, ok := rw.(http.Flusher)
	if !ok {
		http.Error(rw, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	sub := &subscriber{
		ch:           make(chan event, subscriberChannelSize),
		identity:     payload.Identity,
		topics:       payload.Topics,
		cancel:       cancel,
		request:      r,
		lastDelivery: time.Now(),
	}

	w.addSubscriber(sub)
	defer w.removeSubscriber(sub)

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	for {
		select {
		case ev := <-sub.ch:
			sub.mu.Lock()
			sub.lastDelivery = time.Now()
			sub.mu.Unlock()

			if ev.inbox {
				// The reserved inbox frame: no body, the client re-pulls
				// its poll endpoint.
				writeSSEEvent(rw, Fragment{Event: "inbox"})
				flusher.Flush()
				continue
			}

			fragments := w.render(sub.request, ev)
			if fragments == nil {
				// No renderer for this topic: SSE passes the raw event through.
				fragments = []Fragment{{Event: ev.topic, Data: ev.message}}
			}
			for _, f := range fragments {
				writeSSEEvent(rw, f)
			}
			if len(fragments) > 0 {
				flusher.Flush()
			}
		case <-ctx.Done():
			return
		}
	}
}

// ServePoll serves the durable inbox as an HTTP poll surface for the given token —
// the sibling transport to ServeSSE, sharing the same renderers. It renders the
// identity's unseen notifications (scoped to the token's topics) into a single HTTP
// body and is a pure read: it never acks. Seen-tracking flows through the explicit
// MarkSeenUntil/MarkSeen primitives, so opening the same surface in multiple tabs is
// idempotent and convergent.
//
// The cursor is the "after" query param (0 = from the start); the optional "limit"
// param caps the page. The new cursor (the max id fetched) is returned in the
// X-Wire-Cursor response header for the client's next poll. Topics without a renderer
// are skipped (renderer-as-allowlist). Invalid or expired tokens yield 401; a token
// without an identity yields 400 (the inbox is identity-keyed).
func (w *Wire) ServePoll(rw http.ResponseWriter, r *http.Request, token string) {
	payload, err := w.verifyToken(token)
	if err != nil {
		http.Error(rw, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if payload.Identity == "" {
		http.Error(rw, "Poll requires an identity token", http.StatusBadRequest)
		return
	}

	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)

	limit := pollDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = min(n, pollMaxLimit)
		}
	}

	rows, err := ReadUnseen(r.Context(), w.pool, payload.Identity, after, limit)
	if err != nil {
		w.logger.WarnContext(r.Context(), "catbird: wire poll read failed", "error", err)
		http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var body strings.Builder
	cursor := after
	for _, n := range rows {
		// Advance the cursor past every fetched row, even ones this poller skips:
		// the cursor is a per-poller delivery high-water mark (seen-state is separate),
		// and ids are monotonic, so skipping out-of-scope rows never hides future
		// in-scope ones.
		if n.ID > cursor {
			cursor = n.ID
		}
		// Scope to the token's granted topics — the inbox is identity-keyed, so a
		// poller only sees the subset its token covers (the SSE per-topic equivalent).
		if !payload.coversTopic(n.Topic) {
			continue
		}
		for _, f := range w.render(r, event{topic: n.Topic, message: n.Message}) {
			body.WriteString(f.Data)
		}
	}

	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("X-Wire-Cursor", strconv.FormatInt(cursor, 10))
	_, _ = io.WriteString(rw, body.String())
}

// writeSSEEvent writes a single fragment as an SSE frame to w. Multi-line data is
// split into separate "data:" fields per the SSE spec.
func writeSSEEvent(w io.Writer, ev Fragment) {
	fmt.Fprintf(w, "event: %s\n", ev.Event)
	if ev.Data == "" {
		fmt.Fprint(w, "data:\n")
	} else {
		for line := range strings.SplitSeq(ev.Data, "\n") {
			fmt.Fprintf(w, "data: %s\n", line)
		}
	}
	if ev.ID != "" {
		fmt.Fprintf(w, "id: %s\n", ev.ID)
	}
	fmt.Fprint(w, "\n")
}

// Render registers a typed render handler that unmarshals JSON messages into type T
// and passes them to fn for full Fragment control.
func Render[T any](w *Wire, pattern string, fn func(r *http.Request, topic string, data T) (Fragment, error)) {
	w.Render(pattern, func(r *http.Request, topic, message string) (Fragment, error) {
		var data T
		if err := json.Unmarshal([]byte(message), &data); err != nil {
			return Fragment{}, err
		}
		return fn(r, topic, data)
	})
}
