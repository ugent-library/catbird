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

// Wire is an SSE toolkit for notifying browser clients and tracking presence.
// Create with NewWire, configure with builder methods, then call Start.
type Wire struct {
	id     string
	pool   *pgxpool.Pool
	secret []byte
	logger *slog.Logger
	schema string

	mu     sync.RWMutex
	topics *topicTrie[*wireSubscriber]
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
		id:     uuid.NewString(),
		pool:   pool,
		secret: secret,
		logger: slog.Default(),
		topics: newTopicTrie[*wireSubscriber](),
	}
}

// WithLogger sets the Wire logger.
func (w *Wire) WithLogger(logger *slog.Logger) *Wire {
	w.logger = logger
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

// ID returns the unique identifier for this Wire instance.
// Pass to NotifyOpts.From to skip delivery to this Wire.
func (w *Wire) ID() string {
	return w.id
}

// Notify delivers a notification to SSE subscribers and Listeners on the given topic.
// Delivers locally first, then fires pg NOTIFY for cross-node delivery.
// The Wire's own LISTEN loop skips the echo (From is set automatically).
func (w *Wire) Notify(ctx context.Context, topic, message string) error {
	w.deliverLocal(topic, wireEvent{topic: topic, message: message})
	return Notify(ctx, w.pool, topic, message, NotifyOpts{SentBy: w.id})
}

// --- Internal methods ---

// writeSSEEvent writes a single SSE event to w. Multi-line data is split
// into separate "data:" fields per the SSE spec.
func writeSSEEvent(w io.Writer, ev wireEvent) {
	fmt.Fprintf(w, "event: %s\n", ev.topic)
	if ev.message == "" {
		fmt.Fprint(w, "data:\n")
	} else {
		for line := range strings.SplitSeq(ev.message, "\n") {
			fmt.Fprintf(w, "data: %s\n", line)
		}
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

func (w *Wire) deliverLocal(topic string, ev wireEvent) {
	w.mu.RLock()
	subs := w.topics.match(topic, nil)
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
