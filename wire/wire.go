// Package wire delivers events to people: server-rendered fragments over
// SSE, a durable per-recipient inbox read by poll, and presence. Every
// message is a row — a relayed stream message (the log keeps it), an
// inbox row (kept until read or expired), or a presence row (newest
// wins, evaporates on silence) — and the NOTIFY channels carry only
// addresses: the receiving wire fetches the row, renders it and pushes
// it. Anything missed is refetchable, because everything is a row.
//
// The token's two claims name the two products: topics grant the live
// feed (at-most-once, while connected — no replay, no resume), recipient
// grants the inbox (exactly-once, presence-independent). Wire subscribes
// to its two channels on the process's shared notify.Notifier; the
// channel count never grows with topics, relays or recipients.
package wire

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

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
	// goroutine and wire's dispatch goroutine. Overflow drops the frame:
	// the live feed is at-most-once by design, and a slow fetch or a
	// stalled wire must never stall the process's shared LISTEN
	// connection.
	dispatchSize = 256

	subscriberChannelSize = 64
	slowConsumerTimeout   = 5 * time.Second
	pollDefaultLimit      = 100
	pollMaxLimit          = 1000
)

// Opts configures a Wire. Zero fields mean the defaults.
type Opts struct {
	// Notifier carries the live frames and inbox nudges between
	// processes. Nil is a working poll-only configuration: the inbox and
	// both HTTP surfaces function, and clients read rows on their own
	// schedule; only pushed frames need the notifier.
	Notifier *notify.Notifier
	Logger   *slog.Logger // slog.Default()
}

// Wire is the delivery layer: rendered frames to SSE connections, the
// poll surface over the inbox, and presence reads. Create with New,
// register Render handlers, then call Start.
type Wire struct {
	pool     *pgxpool.Pool
	secret   []byte
	logger   *slog.Logger
	notifier *notify.Notifier
	dispatch chan frame

	mu         sync.RWMutex
	topics     *topicTrie[*subscriber]
	renderers  *topicTrie[RenderHandler]
	recipients map[string]map[*subscriber]struct{}
}

// event is what a subscriber's channel carries: a live frame to render,
// or an inbox nudge (re-poll; nothing to render).
type event struct {
	topic   string
	payload string
	inbox   bool
}

// frame is a cbw or cbw_inbox channel payload: always an address, never
// content. A relayed message is (stream, pos, topic) — the dispatch
// fetches the row. A presence change is (topic) alone — nothing to
// fetch, watchers refetch the rows. An inbox nudge is (recipient); the
// zero recipient nudges every connected recipient (the reconnect
// broadcast).
type frame struct {
	Stream    string `json:"stream,omitempty"`
	Pos       int64  `json:"pos,omitempty"`
	Topic     string `json:"topic,omitempty"`
	inbox     bool
	recipient string
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
		pool:       pool,
		secret:     secret,
		logger:     o.Logger,
		notifier:   o.Notifier,
		dispatch:   make(chan frame, dispatchSize),
		topics:     newTopicTrie[*subscriber](),
		renderers:  newTopicTrie[RenderHandler](),
		recipients: make(map[string]map[*subscriber]struct{}),
	}
}

// Render registers a render handler for the given topic pattern. The
// handler projects a matching (topic, payload) into a Fragment — the
// same rendering wherever the event surfaces: a live SSE frame, the
// poll body, an inbox listing. Multiple renderers matching the same
// topic each produce a fragment. Topics without a renderer are handled
// per-transport: ServeSSE passes the payload through raw, ServePoll
// skips them. Must be called before Start.
func (w *Wire) Render(pattern string, fn RenderHandler) *Wire {
	w.renderers.add(pattern, fn)
	return w
}

// Start runs the wire until ctx ends: it subscribes to the module's two
// channels on the notifier and drains the dispatch queue. With a nil
// notifier there are no frames to drain — Start only states the
// configuration and waits, and the wire serves both HTTP surfaces as a
// poll-only configuration.
func (w *Wire) Start(ctx context.Context) error {
	if w.notifier == nil {
		w.logger.InfoContext(ctx, "catbird: wire serving by poll; a notifier adds live frames")
		<-ctx.Done()
		return ctx.Err()
	}

	var schema string
	if err := w.pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return err
	}

	// The callbacks run on the notifier's connection goroutine and only
	// enqueue: everything that touches the database or app code (the
	// fetch, rendering, SSE fan-out) happens on this goroutine, draining
	// w.dispatch below.
	// The frame channel has no reconnect handler — frames belong to the
	// at-most-once feed, and every row they point at is refetchable. The
	// inbox does: a reconnect nudges every connected recipient, since the
	// nudges lost in the gap named nobody.
	cancelFrames := w.notifier.Subscribe(schema+".cbw", w.enqueueFrame, nil)
	defer cancelFrames()
	cancelInbox := w.notifier.Subscribe(schema+".cbw_inbox", w.enqueueInbox, w.enqueueInboxReconnect)
	defer cancelInbox()

	w.logger.InfoContext(ctx, "catbird: wire pushing on notify across processes")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f := <-w.dispatch:
			switch {
			case f.inbox:
				w.deliverInbox(f.recipient)
			case f.Stream != "":
				// a relayed message: the frame is its address
				payload, ok := w.fetch(ctx, f.Stream, f.Pos)
				if ok {
					w.deliverLocal(f.Topic, event{topic: f.Topic, payload: payload})
				}
			default:
				// a presence change: nothing to fetch, watchers refetch
				w.deliverLocal(f.Topic, event{topic: f.Topic})
			}
		}
	}
}

// fetch reads the payload of the addressed stream message through the
// stream module's public SQL API. A missing row is not an error: the
// feed is at-most-once and retention may have taken the row; the frame
// is simply dropped.
func (w *Wire) fetch(ctx context.Context, stream string, pos int64) (string, bool) {
	var payload string
	err := w.pool.QueryRow(ctx,
		`SELECT m.payload::text FROM cb_stream_fetch($1, $2) m`, stream, pos).Scan(&payload)
	if err != nil {
		if err != pgx.ErrNoRows {
			w.logger.Warn("catbird: wire fetch failed", "stream", stream, "pos", pos, "error", err)
		}
		return "", false
	}
	return payload, true
}

// enqueueFrame handles a cbw notification on the notifier's goroutine.
func (w *Wire) enqueueFrame(payload string) {
	var f frame
	if err := json.Unmarshal([]byte(payload), &f); err != nil || f.Topic == "" {
		w.logger.Warn("catbird: wire ignoring invalid frame", "error", err)
		return
	}

	select {
	case w.dispatch <- f:
	default:
		w.logger.Warn("catbird: wire dispatch queue full, dropping frame", "topic", f.Topic)
	}
}

// enqueueInbox handles a cbw_inbox notification on the notifier's
// goroutine. The payload is the recipient whose inbox grew.
func (w *Wire) enqueueInbox(recipient string) {
	select {
	case w.dispatch <- frame{inbox: true, recipient: recipient}:
	default:
		w.logger.Warn("catbird: wire dispatch queue full, dropping inbox nudge", "recipient", recipient)
	}
}

// enqueueInboxReconnect handles a reconnect on the cbw_inbox channel.
// Nudges sent while the connection was down are gone and named nobody, so
// every connected recipient is told to look for itself — a broadcast
// frame (empty recipient).
func (w *Wire) enqueueInboxReconnect() {
	select {
	case w.dispatch <- frame{inbox: true}:
	default:
		w.logger.Warn("catbird: wire dispatch queue full, dropping inbox reconnect nudge")
	}
}

// deliverInbox writes one reserved "inbox" frame to the recipient's SSE
// connections; their clients answer by re-pulling their poll endpoint.
// An empty recipient nudges every connected recipient.
func (w *Wire) deliverInbox(recipient string) {
	w.mu.RLock()
	var subs []*subscriber
	if recipient == "" {
		for _, set := range w.recipients {
			for sub := range set {
				subs = append(subs, sub)
			}
		}
	} else {
		for sub := range w.recipients[recipient] {
			subs = append(subs, sub)
		}
	}
	w.mu.RUnlock()

	for _, sub := range subs {
		sub.push(event{inbox: true})
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
		f, err := fn(r, ev.topic, ev.payload)
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
	if sub.recipient != "" {
		set, ok := w.recipients[sub.recipient]
		if !ok {
			set = make(map[*subscriber]struct{})
			w.recipients[sub.recipient] = set
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
	if sub.recipient != "" {
		set := w.recipients[sub.recipient]
		delete(set, sub)
		if len(set) == 0 {
			delete(w.recipients, sub.recipient)
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
