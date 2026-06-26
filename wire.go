package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	wireChannelSize         = 64
	wireSlowConsumerTimeout = 5 * time.Second
	wireHeartbeatInterval   = 5 * time.Second
	wirePollDefaultLimit    = 100
	wirePollMaxLimit        = 1000
)

// ListenHandler is called when a notification matches a registered pattern.
// Handlers run synchronously in the dispatch goroutine — don't block.
type ListenHandler = func(ctx context.Context, topic, message string)

// Fragment is a rendered projection of a (topic, message) for client delivery. Data is
// the transport-neutral substance (e.g. an HTML fragment): ServePoll emits it directly,
// ServeSSE puts it in data:, an HTMX-WebSocket transport would send it as the frame.
// Event and ID are SSE-native framing hints — SSE uses them for event:/id:, poll and
// HTMX-WebSocket ignore them, and a JSON-WebSocket or Web Push transport would re-encode
// them (message type / dedup tag). Structured push notifications are otherwise a
// different projection and aren't modeled here.
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

// RenderHandler projects a Wire event into a transport-neutral Fragment for client
// delivery. It receives the client's HTTP request for access to user context
// (auth, language, etc). Only topics with a registered renderer are delivered —
// the renderer acts as an allowlist.
type RenderHandler = func(r *http.Request, topic, message string) (Fragment, error)

// Wire is a real-time pub/sub layer with SSE support and presence tracking.
// It absorbs Listener's topic-matched dispatch, adds local delivery to
// Notify, and serves SSE connections with per-transport rendering.
// Create with NewWire, configure with builder methods, then call Start.
type Wire struct {
	id     string
	pool   *pgxpool.Pool
	secret []byte
	logger *slog.Logger
	schema string

	mu        sync.RWMutex
	topics    *topicTrie[*wireSubscriber]
	listeners *topicTrie[ListenHandler]
	renderers *topicTrie[RenderHandler]
}

type wireSubscriber struct {
	id       string // unique per connection, used for presence rows
	ch       chan wireEvent
	identity string
	topics   []string
	cancel   context.CancelFunc
	request  *http.Request // SSE client's HTTP request for Render context

	mu           sync.Mutex
	lastDelivery time.Time
}

type wireEvent struct {
	topic   string
	message string
}

type wireMessage struct {
	SentBy  *string `json:"sent_by"`
	Topic   string  `json:"topic"`
	Message string  `json:"message"`
}

// NewWire creates a new Wire instance.
// The secret must be exactly 32 bytes (AES-256).
func NewWire(pool *pgxpool.Pool, secret []byte) *Wire {
	if len(secret) != 32 {
		panic("catbird: Wire secret must be exactly 32 bytes")
	}
	return &Wire{
		id:        uuid.NewString(),
		pool:      pool,
		secret:    secret,
		logger:    slog.Default(),
		topics:    newTopicTrie[*wireSubscriber](),
		listeners: newTopicTrie[ListenHandler](),
		renderers: newTopicTrie[RenderHandler](),
	}
}

// WithLogger sets the Wire logger.
func (w *Wire) WithLogger(logger *slog.Logger) *Wire {
	w.logger = logger
	return w
}

// Listen registers a handler for the given topic pattern.
// Handlers fire on every node that receives the event (local or cross-node).
// They're for server-side side effects — logging, webhooks, triggering work.
// Patterns use the same syntax as Bind: "." separates tokens,
// "*" matches one token, "#" matches zero or more trailing tokens.
// Must be called before Start.
func (w *Wire) Listen(pattern string, fn ListenHandler) *Wire {
	w.listeners.add(pattern, fn)
	return w
}

// Render registers a render handler for the given topic pattern. The handler
// projects a matching (topic, message) into a transport-neutral Fragment.
// Multiple renderers matching the same topic each produce a fragment.
// Topics without a renderer are handled per-transport: ServeSSE passes them
// through raw, ServePoll skips them.
// Must be called before Start.
func (w *Wire) Render(pattern string, fn RenderHandler) *Wire {
	w.renderers.add(pattern, fn)
	return w
}

// Start runs the Wire's background loops: LISTEN relay, heartbeat, and
// forward bridges. Blocks until ctx is cancelled.
func (w *Wire) Start(ctx context.Context) error {
	if err := w.pool.QueryRow(ctx, "SELECT current_schema").Scan(&w.schema); err != nil {
		return fmt.Errorf("catbird: wire cannot determine schema: %w", err)
	}

	if _, err := w.pool.Exec(ctx,
		`INSERT INTO cb_wire_nodes (id) VALUES ($1)`, w.id); err != nil {
		return fmt.Errorf("catbird: wire cannot register node: %w", err)
	}

	w.logger.InfoContext(ctx, "wire: started", "node_id", w.id)

	var wg sync.WaitGroup

	wg.Go(func() { w.heartbeatLoop(ctx) })
	wg.Go(func() { w.listen(ctx) })

	<-ctx.Done()

	// Cleanup: delete node (CASCADE clears presence)
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := w.pool.Exec(cleanupCtx,
		`DELETE FROM cb_wire_nodes WHERE id = $1`, w.id); err != nil {
		w.logger.WarnContext(cleanupCtx, "wire: cleanup failed", "error", err)
	}

	wg.Wait()
	return nil
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

	sub := &wireSubscriber{
		id:           uuid.NewString(),
		ch:           make(chan wireEvent, wireChannelSize),
		identity:     payload.Identity,
		topics:       payload.Topics,
		cancel:       cancel,
		request:      r,
		lastDelivery: time.Now(),
	}

	w.addSubscriber(sub)
	defer w.removeSubscriber(sub)

	if sub.identity != "" && w.schema != "" {
		w.presenceJoin(ctx, sub)
		defer w.presenceLeave(sub)
	}

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

	limit := wirePollDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = min(n, wirePollMaxLimit)
		}
	}

	rows, err := UnseenNotifications(r.Context(), w.pool, payload.Identity, after, limit)
	if err != nil {
		w.logger.WarnContext(r.Context(), "wire: poll read failed", "error", err)
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
		for _, f := range w.render(r, wireEvent{topic: n.Topic, message: n.Message}) {
			body.WriteString(f.Data)
		}
	}

	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("X-Wire-Cursor", strconv.FormatInt(cursor, 10))
	_, _ = io.WriteString(rw, body.String())
}

// Presence returns the distinct identities connected to a topic across all nodes.
func (w *Wire) Presence(ctx context.Context, topic string) ([]string, error) {
	q := `SELECT DISTINCT identity FROM cb_wire_presence WHERE topic = $1 ORDER BY identity`
	rows, err := w.pool.Query(ctx, q, topic)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// ID returns the unique identifier for this Wire instance.
// Pass to NotifyOpts.From to skip delivery to this Wire.
func (w *Wire) ID() string {
	return w.id
}

// Notify delivers a notification to Listen handlers and SSE subscribers locally,
// then fires pg NOTIFY for cross-node delivery.
// The Wire's own LISTEN loop skips the echo (SentBy is set automatically).
func (w *Wire) Notify(ctx context.Context, topic, message string) error {
	w.deliverToListeners(ctx, topic, message)
	w.deliverLocal(topic, wireEvent{topic: topic, message: message})
	return Notify(ctx, w.pool, topic, message, NotifyOpts{SentBy: w.id})
}

// --- Internal methods ---

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

func (w *Wire) addSubscriber(sub *wireSubscriber) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, topic := range sub.topics {
		w.topics.add(topic, sub)
	}
}

func (w *Wire) removeSubscriber(sub *wireSubscriber) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, topic := range sub.topics {
		w.topics.remove(topic, func(s *wireSubscriber) bool { return s == sub })
	}
}

func (w *Wire) deliverToListeners(ctx context.Context, topic, message string) {
	w.mu.RLock()
	handlers := w.listeners.match(topic, nil)
	w.mu.RUnlock()

	for _, fn := range handlers {
		fn(ctx, topic, message)
	}
}

func (w *Wire) deliverLocal(topic string, ev wireEvent) {
	w.mu.RLock()
	subs := w.topics.match(topic, nil)
	w.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.ch <- ev:
		default:
			sub.mu.Lock()
			if time.Since(sub.lastDelivery) > wireSlowConsumerTimeout {
				sub.cancel()
			}
			sub.mu.Unlock()
		}
	}
}

func (w *Wire) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(wireHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := w.pool.Exec(ctx,
				`UPDATE cb_wire_nodes SET last_heartbeat_at = now() WHERE id = $1`,
				w.id); err != nil {
				w.logger.WarnContext(ctx, "wire: heartbeat failed", "error", err)
			}
		}
	}
}

func (w *Wire) listen(ctx context.Context) {
	for {
		if err := w.listenOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.WarnContext(ctx, "wire: connection lost, reconnecting", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

func (w *Wire) listenOnce(ctx context.Context) error {
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	pgConn := conn.Conn()

	ch := channelName(w.schema, "cb_wire")
	if _, err := pgConn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
		return err
	}

	w.logger.InfoContext(ctx, "wire: listening", "channel", ch)

	for {
		notification, waitErr := pgConn.WaitForNotification(ctx)
		if waitErr != nil {
			return waitErr
		}

		var msg wireMessage
		if err := json.Unmarshal([]byte(notification.Payload), &msg); err != nil {
			w.logger.WarnContext(ctx, "wire: invalid NOTIFY payload", "error", err)
			continue
		}

		// Skip messages from this node (already delivered locally)
		if msg.SentBy != nil && *msg.SentBy == w.id {
			continue
		}

		w.deliverToListeners(ctx, msg.Topic, msg.Message)
		w.deliverLocal(msg.Topic, wireEvent{topic: msg.Topic, message: msg.Message})
	}
}

func (w *Wire) presenceJoin(ctx context.Context, sub *wireSubscriber) {
	for _, topic := range sub.topics {
		if _, err := w.pool.Exec(ctx,
			`INSERT INTO cb_wire_presence (subscriber_id, identity, topic, node_id) VALUES ($1, $2, $3, $4)`,
			sub.id, sub.identity, topic, w.id); err != nil {
			w.logger.WarnContext(ctx, "wire: presence join failed", "topic", topic, "error", err)
		}
	}

	// Signal presence change
	for _, topic := range sub.topics {
		w.deliverLocal(topic, wireEvent{topic: topic})
	}
	for _, topic := range sub.topics {
		_, _ = w.pool.Exec(ctx,
			`SELECT cb_notify(topic => $1, "from" => $2::uuid)`,
			topic, w.id)
	}
}

func (w *Wire) presenceLeave(sub *wireSubscriber) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := w.pool.Exec(ctx,
		`DELETE FROM cb_wire_presence WHERE subscriber_id = $1`,
		sub.id); err != nil {
		w.logger.WarnContext(ctx, "wire: presence leave failed", "error", err)
	}

	for _, topic := range sub.topics {
		w.deliverLocal(topic, wireEvent{topic: topic})
	}
	for _, topic := range sub.topics {
		_, _ = w.pool.Exec(ctx,
			`SELECT cb_notify(topic => $1, "from" => $2::uuid)`,
			topic, w.id)
	}
}

// render matches the renderers for the event's topic and calls each match,
// returning the produced fragments. No renderer match returns nil — the
// no-renderer fallback (pass through raw, or skip) is a per-transport decision.
func (w *Wire) render(r *http.Request, ev wireEvent) []Fragment {
	w.mu.RLock()
	fns := w.renderers.match(ev.topic, nil)
	w.mu.RUnlock()

	if len(fns) == 0 {
		return nil
	}

	fragments := make([]Fragment, 0, len(fns))
	for _, fn := range fns {
		f, err := fn(r, ev.topic, ev.message)
		if err != nil {
			w.logger.Warn("wire: render error", "topic", ev.topic, "error", err)
			continue
		}
		if f.Event == "" {
			f.Event = ev.topic
		}
		fragments = append(fragments, f)
	}
	return fragments
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
