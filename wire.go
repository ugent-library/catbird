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

// Notification represents a message for SSE delivery, used by both
// Notify (ephemeral) and Forward (durable) paths.
type Notification struct {
	Event string `json:"event"`
	Data  string `json:"data,omitempty"`
}

// Wire is an SSE toolkit for notifying browser clients and tracking presence.
// Create with NewWire, configure with builder methods, then call Start.
type Wire struct {
	id     string
	pool   *pgxpool.Pool
	secret []byte
	logger *slog.Logger
	schema string

	mu     sync.RWMutex
	topics map[string][]*wireSubscriber

	forwards []wireForward
}

// ForwardOpts configures Forward polling behavior.
// Zero values use the same defaults as ReadPoll.
type ForwardOpts ReadPollOpts

type wireForward struct {
	queue    string
	quantity int
	hideFor  time.Duration
	pollOpts ForwardOpts
}

type wireSubscriber struct {
	id       string // unique per connection, used for presence rows
	ch       chan wireEvent
	identity string
	topics   []string
	cancel   context.CancelFunc

	mu           sync.Mutex
	lastDelivery time.Time
}

type wireEvent struct {
	name string
	data string
}

type wireMessage struct {
	NodeID *string `json:"node_id"`
	Topic  string  `json:"topic"`
	Event  string  `json:"event"`
	Data   string  `json:"data"`
}

// NewWire creates a new Wire instance.
// The secret must be exactly 32 bytes (AES-256).
func NewWire(pool *pgxpool.Pool, secret []byte) *Wire {
	if len(secret) != 32 {
		panic("catbird: Wire secret must be exactly 32 bytes")
	}
	return &Wire{
		id:     uuid.NewString(),
		pool:   pool,
		secret: secret,
		logger: slog.Default(),
		topics: make(map[string][]*wireSubscriber),
	}
}

// WithLogger sets the Wire logger.
func (w *Wire) WithLogger(logger *slog.Logger) *Wire {
	w.logger = logger
	return w
}

// Forward registers a queue to bridge to SSE subscribers.
// Messages are read from the queue and delivered as SSE events.
// Must be called before Start.
func (w *Wire) Forward(queue string, quantity int, hideFor time.Duration, opts ...ForwardOpts) *Wire {
	f := wireForward{queue: queue, quantity: quantity, hideFor: hideFor}
	if len(opts) > 0 {
		f.pollOpts = opts[0]
	}
	w.forwards = append(w.forwards, f)
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
	wg.Go(func() { w.listenLoop(ctx) })

	for _, fwd := range w.forwards {
		wg.Go(func() { w.forwardLoop(ctx, fwd) })
	}

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

			writeSSEEvent(rw, ev)
			flusher.Flush()
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

// Notify delivers an event to SSE subscribers on the given topic.
// Delivers locally first, then fires pg NOTIFY for cross-node delivery.
func (w *Wire) Notify(ctx context.Context, topic, event, data string) error {
	w.deliverLocal(topic, wireEvent{name: event, data: data})

	_, err := w.pool.Exec(ctx,
		`SELECT cb_notify(topic => $1, event => $2, data => $3, node_id => $4::uuid)`,
		topic, event, ptrOrNil(data), w.id)
	return err
}

// --- Internal methods ---

// writeSSEEvent writes a single SSE event to w. Multi-line data is split
// into separate "data:" fields per the SSE spec.
func writeSSEEvent(w io.Writer, ev wireEvent) {
	fmt.Fprintf(w, "event: %s\n", ev.name)
	if ev.data == "" {
		fmt.Fprint(w, "data:\n")
	} else {
		for line := range strings.SplitSeq(ev.data, "\n") {
			fmt.Fprintf(w, "data: %s\n", line)
		}
	}
	fmt.Fprint(w, "\n")
}

func (w *Wire) addSubscriber(sub *wireSubscriber) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, topic := range sub.topics {
		w.topics[topic] = append(w.topics[topic], sub)
	}
}

func (w *Wire) removeSubscriber(sub *wireSubscriber) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, topic := range sub.topics {
		subs := w.topics[topic]
		for i, s := range subs {
			if s == sub {
				w.topics[topic] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		if len(w.topics[topic]) == 0 {
			delete(w.topics, topic)
		}
	}
}

func (w *Wire) deliverLocal(topic string, ev wireEvent) {
	w.mu.RLock()
	subs := w.topics[topic]
	w.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.ch <- ev:
		default:
			// Channel full — check for slow consumer.
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

func (w *Wire) listenLoop(ctx context.Context) {
	for {
		if err := w.listenOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.WarnContext(ctx, "wire: LISTEN connection lost, reconnecting", "error", err)
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

	ch := channelName(w.schema, "cb_wire")
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
		return err
	}

	w.logger.InfoContext(ctx, "wire: listening", "channel", ch)

	for {
		notification, waitErr := conn.Conn().WaitForNotification(ctx)
		if waitErr != nil {
			return waitErr
		}

		var msg wireMessage
		if err := json.Unmarshal([]byte(notification.Payload), &msg); err != nil {
			w.logger.WarnContext(ctx, "wire: invalid NOTIFY payload", "error", err)
			continue
		}

		// Skip messages from this node (already delivered locally)
		if msg.NodeID != nil && *msg.NodeID == w.id {
			continue
		}

		w.deliverLocal(msg.Topic, wireEvent{name: msg.Event, data: msg.Data})
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
		w.deliverLocal(topic, wireEvent{name: "presence"})
	}
	for _, topic := range sub.topics {
		_, _ = w.pool.Exec(ctx,
			`SELECT cb_notify(topic => $1, event => 'presence', node_id => $2::uuid)`,
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
		w.deliverLocal(topic, wireEvent{name: "presence"})
	}
	for _, topic := range sub.topics {
		_, _ = w.pool.Exec(ctx,
			`SELECT cb_notify(topic => $1, event => 'presence', node_id => $2::uuid)`,
			topic, w.id)
	}
}

func (w *Wire) forwardLoop(ctx context.Context, fwd wireForward) {
	w.logger.InfoContext(ctx, "wire: forwarding", "queue", fwd.queue)

	for {
		msgs, err := ReadPoll(ctx, w.pool, fwd.queue, fwd.quantity, fwd.hideFor, ReadPollOpts(fwd.pollOpts))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.WarnContext(ctx, "wire: forward read failed", "queue", fwd.queue, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		for _, msg := range msgs {
			var n Notification
			if err := json.Unmarshal(msg.Payload, &n); err != nil {
				w.logger.WarnContext(ctx, "wire: forward unmarshal failed", "error", err)
				_, _ = Delete(ctx, w.pool, fwd.queue, msg.ID)
				continue
			}

			// Deliver locally and relay cross-node
			w.deliverLocal(msg.Topic, wireEvent{name: n.Event, data: n.Data})

			_, _ = w.pool.Exec(ctx,
				`SELECT cb_notify(topic => $1, event => $2, data => $3, node_id => $4::uuid)`,
				msg.Topic, n.Event, ptrOrNil(n.Data), w.id)

			if _, err := Delete(ctx, w.pool, fwd.queue, msg.ID); err != nil {
				w.logger.WarnContext(ctx, "wire: forward delete failed", "error", err)
			}
		}
	}
}
