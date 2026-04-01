package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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
)

// ListenHandler is called when a notification matches a registered pattern.
// Handlers run synchronously in the dispatch goroutine — don't block.
type ListenHandler = func(ctx context.Context, topic, message string)

// SSEEvent represents a fully rendered SSE event ready for client delivery.
type SSEEvent struct {
	Event string // SSE event name; empty = use original topic
	Data  string // SSE data field
	ID    string // SSE id field; empty = omit
}

// Write implements io.Writer, appending p to the Data field.
// This allows SSEEvent to be used as a target for io.WriterTo (e.g. Templ components).
func (e *SSEEvent) Write(p []byte) (int, error) {
	e.Data += string(p)
	return len(p), nil
}

// SSERenderHandler transforms a Wire event into an SSE event for client delivery.
// It receives the SSE client's HTTP request for access to user context
// (auth, language, etc). Only topics with a registered renderer are
// delivered to SSE clients — the renderer acts as an allowlist.
type SSERenderHandler = func(r *http.Request, topic, message string) (SSEEvent, error)

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

	mu           sync.RWMutex
	topics       *topicTrie[*wireSubscriber]
	listeners    *topicTrie[ListenHandler]
	sseRenderers *topicTrie[SSERenderHandler]
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
		id:           uuid.NewString(),
		pool:         pool,
		secret:       secret,
		logger:       slog.Default(),
		topics:       newTopicTrie[*wireSubscriber](),
		listeners:    newTopicTrie[ListenHandler](),
		sseRenderers: newTopicTrie[SSERenderHandler](),
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

// RenderSSE registers an SSE render handler for the given topic pattern.
// Multiple renderers matching the same topic each produce an SSE event.
// Topics without a renderer pass through as-is.
// Must be called before Start.
func (w *Wire) RenderSSE(pattern string, fn SSERenderHandler) *Wire {
	w.sseRenderers.add(pattern, fn)
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

			events := w.renderSSE(sub.request, ev)
			for _, sse := range events {
				writeSSEEvent(rw, sse)
			}
			if len(events) > 0 {
				flusher.Flush()
			}
		case <-ctx.Done():
			return
		}
	}
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

// writeSSEEvent writes a single SSE event to w. Multi-line data is split
// into separate "data:" fields per the SSE spec.
func writeSSEEvent(w io.Writer, ev SSEEvent) {
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

// renderSSE matches sseRenderers for the event's topic and calls each match.
// No renderer match → pass through raw event as-is.
func (w *Wire) renderSSE(r *http.Request, ev wireEvent) []SSEEvent {
	w.mu.RLock()
	renderers := w.sseRenderers.match(ev.topic, nil)
	w.mu.RUnlock()

	if len(renderers) == 0 {
		return []SSEEvent{{Event: ev.topic, Data: ev.message}}
	}

	events := make([]SSEEvent, 0, len(renderers))
	for _, fn := range renderers {
		sse, err := fn(r, ev.topic, ev.message)
		if err != nil {
			w.logger.Warn("wire: sse render error", "topic", ev.topic, "error", err)
			continue
		}
		if sse.Event == "" {
			sse.Event = ev.topic
		}
		events = append(events, sse)
	}
	return events
}

// RenderSSE registers a typed SSE render handler that unmarshals JSON messages
// into type T and passes them to fn for full SSEEvent control.
func RenderSSE[T any](w *Wire, pattern string, fn func(r *http.Request, topic string, data T) (SSEEvent, error)) {
	w.RenderSSE(pattern, func(r *http.Request, topic, message string) (SSEEvent, error) {
		var data T
		if err := json.Unmarshal([]byte(message), &data); err != nil {
			return SSEEvent{}, err
		}
		return fn(r, topic, data)
	})
}
