// Package wire delivers server events to web clients: an ephemeral
// pub/sub bus pushed over SSE, and a durable per-identity inbox read by
// poll. The two halves share rendering, tokens and the poll transport
// and are each usable alone. The surface is SSE + HTML fragments +
// JSON, aimed at server-rendered apps generally.
//
// Ephemeral events ride pg NOTIFY whole (channel cbw) and are
// at-most-once: a process that is down misses them. Durable
// notifications are rows in cb_wire_inbox, written by NotifyDurable in
// the caller's transaction; their nudge (channel cbw_inbox) tells the
// identity's connected clients to re-poll. Wire subscribes to its two
// channels on the process's shared notify.Notifier; the channel count
// never grows with topics, subscribers or identities.
package wire

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/notify"
)

// Conn is an interface for database connections compatible with pgx.Conn,
// pgxpool.Pool and pgx.Tx.
type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

const (
	// dispatchSize bounds the queue between the notifier's connection
	// goroutine and wire's dispatch goroutine. Overflow drops the event:
	// ephemeral delivery is at-most-once by design, and a slow Listen
	// handler must never stall the process's shared LISTEN connection.
	dispatchSize = 256

	subscriberChannelSize = 64
	slowConsumerTimeout   = 5 * time.Second
	pollDefaultLimit      = 100
	pollMaxLimit          = 1000
)

// ListenHandler is called when a notification matches a registered pattern.
// Handlers run synchronously in the dispatch goroutine — don't block.
type ListenHandler = func(ctx context.Context, topic, message string)

// Opts configures a Wire. Zero fields mean the defaults.
type Opts struct {
	// Notifier carries cross-process events. Nil is a working
	// single-process configuration: local delivery and both HTTP
	// surfaces function; only pushes from other processes need the
	// notifier, and clients catch up on those by poll.
	Notifier *notify.Notifier
	Logger   *slog.Logger // slog.Default()
}

// Wire is the delivery layer: topic-matched dispatch to Listen handlers
// and SSE connections, plus the poll surface over the inbox. Create with
// New, register Listen/Render handlers, then call Start.
type Wire struct {
	id       string
	pool     *pgxpool.Pool
	secret   []byte
	logger   *slog.Logger
	notifier *notify.Notifier
	dispatch chan dispatchEvent

	mu         sync.RWMutex
	topics     *topicTrie[*subscriber]
	listeners  *topicTrie[ListenHandler]
	renderers  *topicTrie[RenderHandler]
	identities map[string]map[*subscriber]struct{}
}

// event is what a subscriber's channel carries: a bus event to render,
// or an inbox nudge (re-poll; nothing to render).
type event struct {
	topic   string
	message string
	inbox   bool
}

// dispatchEvent is what the notifier callbacks hand to the dispatch
// goroutine.
type dispatchEvent struct {
	inbox    bool
	identity string // nudge target; empty nudges every connected identity
	topic    string
	message  string
}

// busMessage is the cbw channel's payload format, built by cb_wire_notify.
type busMessage struct {
	SentBy  *string `json:"sent_by"`
	Topic   string  `json:"topic"`
	Message string  `json:"message"`
}

// New creates a Wire. The secret signs connection tokens and must be
// exactly 32 bytes (AES-256).
func New(pool *pgxpool.Pool, secret []byte, opts ...Opts) *Wire {
	var o Opts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if len(secret) != 32 {
		panic("catbird: wire secret must be exactly 32 bytes")
	}
	return &Wire{
		id:         uuid.NewString(),
		pool:       pool,
		secret:     secret,
		logger:     o.Logger,
		notifier:   o.Notifier,
		dispatch:   make(chan dispatchEvent, dispatchSize),
		topics:     newTopicTrie[*subscriber](),
		listeners:  newTopicTrie[ListenHandler](),
		renderers:  newTopicTrie[RenderHandler](),
		identities: make(map[string]map[*subscriber]struct{}),
	}
}

// ID returns the unique identifier for this Wire instance.
// Pass it as NotifyOpts.SentBy to skip delivery to this Wire.
func (w *Wire) ID() string {
	return w.id
}

// Listen registers a handler for the given topic pattern.
// Handlers fire on every wire that receives the event (local or
// cross-process). They're for server-side side effects — logging,
// webhooks, triggering work. Patterns use "." to separate tokens,
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

// Notify delivers an ephemeral event to this wire's Listen handlers and
// SSE subscribers, then sends it to every other process over pg NOTIFY.
// This wire skips the echo of its own send.
// The payload must fit NOTIFY's 8000-byte limit: send a pointer to
// state, not the state. An oversized payload raises in the caller's
// transaction.
func (w *Wire) Notify(ctx context.Context, topic, message string) error {
	w.deliverToListeners(ctx, topic, message)
	w.deliverLocal(topic, event{topic: topic, message: message})
	return Notify(ctx, w.pool, topic, message, NotifyOpts{SentBy: w.id})
}

// NotifyOpts configures notification delivery.
type NotifyOpts struct {
	// SentBy identifies the sender. The Wire whose ID matches skips
	// delivery, avoiding echo. Use Wire.ID().
	SentBy string
}

// Notify sends an ephemeral event via pg NOTIFY. Every wire in every
// process picks it up and delivers to its local subscribers and Listen
// handlers; nothing is stored, delivery is at-most-once. Set
// NotifyOpts.SentBy to skip delivery to the sender.
// The payload must fit NOTIFY's 8000-byte limit: send a pointer to
// state, not the state. An oversized payload raises in the caller's
// transaction.
func Notify(ctx context.Context, conn Conn, topic, message string, opts ...NotifyOpts) error {
	var sentBy *string
	if len(opts) > 0 && opts[0].SentBy != "" {
		sentBy = &opts[0].SentBy
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_wire_notify(topic => $1, message => $2, sent_by => $3)`,
		topic, nullText(message), sentBy)
	return err
}

// Start runs the wire until ctx ends: it subscribes to the module's two
// channels on the notifier and drains the dispatch queue. With a nil
// notifier there is nothing to drain — Start only states the
// configuration and waits, and the wire serves local delivery and both
// HTTP surfaces as before.
func (w *Wire) Start(ctx context.Context) error {
	if w.notifier == nil {
		w.logger.InfoContext(ctx, "catbird: wire pushing within this process; clients catch up by poll")
		<-ctx.Done()
		return ctx.Err()
	}

	var schema string
	if err := w.pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return err
	}

	// The callbacks run on the notifier's connection goroutine and only
	// enqueue: everything that runs app code (Listen handlers, SSE
	// fan-out) happens on this goroutine, draining w.dispatch below.
	// The bus has no reconnect handler — ephemeral events missed while the
	// connection was down are simply missed. The inbox does: a reconnect
	// nudges every connected identity, since the nudges lost in the gap
	// named nobody.
	cancelBus := w.notifier.Subscribe(schema+".cbw", w.enqueueBus, nil)
	defer cancelBus()
	cancelInbox := w.notifier.Subscribe(schema+".cbw_inbox", w.enqueueInbox, w.enqueueInboxReconnect)
	defer cancelInbox()

	w.logger.InfoContext(ctx, "catbird: wire pushing on notify across processes")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-w.dispatch:
			if ev.inbox {
				w.deliverInbox(ev.identity)
				continue
			}
			w.deliverToListeners(ctx, ev.topic, ev.message)
			w.deliverLocal(ev.topic, event{topic: ev.topic, message: ev.message})
		}
	}
}

// enqueueBus handles a cbw notification on the notifier's goroutine.
func (w *Wire) enqueueBus(payload string) {
	var msg busMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		w.logger.Warn("catbird: wire ignoring invalid bus payload", "error", err)
		return
	}

	// Skip this wire's own send, already delivered locally by Notify.
	if msg.SentBy != nil && *msg.SentBy == w.id {
		return
	}

	select {
	case w.dispatch <- dispatchEvent{topic: msg.Topic, message: msg.Message}:
	default:
		w.logger.Warn("catbird: wire dispatch queue full, dropping event", "topic", msg.Topic)
	}
}

// enqueueInbox handles a cbw_inbox notification on the notifier's
// goroutine. The payload is the identity whose inbox grew.
func (w *Wire) enqueueInbox(identity string) {
	select {
	case w.dispatch <- dispatchEvent{inbox: true, identity: identity}:
	default:
		w.logger.Warn("catbird: wire dispatch queue full, dropping inbox nudge", "identity", identity)
	}
}

// enqueueInboxReconnect handles a reconnect on the cbw_inbox channel.
// Nudges sent while the connection was down are gone and named nobody, so
// every connected identity is told to look for itself — a broadcast
// dispatch event (empty identity).
func (w *Wire) enqueueInboxReconnect() {
	select {
	case w.dispatch <- dispatchEvent{inbox: true}:
	default:
		w.logger.Warn("catbird: wire dispatch queue full, dropping inbox reconnect nudge")
	}
}

// deliverInbox writes one reserved "inbox" frame to the identity's SSE
// connections; their clients answer by re-pulling their poll endpoint.
// An empty identity nudges every connected identity.
func (w *Wire) deliverInbox(identity string) {
	w.mu.RLock()
	var subs []*subscriber
	if identity == "" {
		for _, set := range w.identities {
			for sub := range set {
				subs = append(subs, sub)
			}
		}
	} else {
		for sub := range w.identities[identity] {
			subs = append(subs, sub)
		}
	}
	w.mu.RUnlock()

	for _, sub := range subs {
		sub.push(event{inbox: true})
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

func (w *Wire) deliverLocal(topic string, ev event) {
	w.mu.RLock()
	subs := w.topics.match(topic, nil)
	w.mu.RUnlock()

	for _, sub := range subs {
		sub.push(ev)
	}
}

// render matches the renderers for the event's topic and calls each match,
// returning the produced fragments. No renderer match returns nil — the
// no-renderer fallback (pass through raw, or skip) is a per-transport decision.
func (w *Wire) render(r *http.Request, ev event) []Fragment {
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
			w.logger.Warn("catbird: wire render error", "topic", ev.topic, "error", err)
			continue
		}
		if f.Event == "" {
			f.Event = ev.topic
		}
		fragments = append(fragments, f)
	}
	return fragments
}

func (w *Wire) addSubscriber(sub *subscriber) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, topic := range sub.topics {
		w.topics.add(topic, sub)
	}
	if sub.identity != "" {
		set, ok := w.identities[sub.identity]
		if !ok {
			set = make(map[*subscriber]struct{})
			w.identities[sub.identity] = set
		}
		set[sub] = struct{}{}
	}
}

func (w *Wire) removeSubscriber(sub *subscriber) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, topic := range sub.topics {
		w.topics.remove(topic, func(s *subscriber) bool { return s == sub })
	}
	if sub.identity != "" {
		set := w.identities[sub.identity]
		delete(set, sub)
		if len(set) == 0 {
			delete(w.identities, sub.identity)
		}
	}
}

func nullText(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
