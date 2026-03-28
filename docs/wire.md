# Wire

SSE toolkit for notifying browser clients, tracking presence, and triggering
server-side work. The catbird sits on the wire — clients connect to it and
listen. Same opt-in philosophy as the rest of catbird — if you don't create
a Wire, none of this code runs. Wire is a toolkit, not a handler — your app
owns all routing.

## Motivation

Catbird's queue/topic system is server-to-server. Applications also need
server-to-browser: notify when a task completes, show who else is
viewing a page, let a button kick off a flow. Today this requires a separate
service (Centrifugo, etc.) or custom glue code. The Wire brings this into
catbird as four methods on Wire and Client.

## Where it fits

```go
cb   := catbird.New(conn)                // Client — queues, tasks, flows, notify
w    := catbird.NewWorker(pool)          // Worker — executes handlers
wire := catbird.NewWire(pool, secret)    // Wire — SSE, presence, notify
```

Three peers in the same package. Client wraps `Conn` (pool, conn, or tx).
Worker and Wire require `*pgxpool.Pool` because they each acquire a
dedicated connection for LISTEN. Notify is available on both Client and Wire
(see [Notify](#notify-sse)).

## Capabilities

| Method | On | Description |
|---|---|---|
| `wire.Token(...)` | Wire | Mint a signed SSE connection token |
| `wire.ServeSSE(w, r, token)` | Wire | Serve an SSE connection |
| `wire.Presence(ctx, topic)` | Wire | Query who's connected to a topic |
| `wire.Forward(queue)` | Wire | Bridge a queue to SSE (durable notifications) |
| `cb.Notify(ctx, topic, event, data)` | Client | Send an ephemeral notification to SSE subscribers |

## API sketch

```go
wire := catbird.NewWire(pool, []byte("...")). // pool + signing secret (required)
    WithLogger(logger)

go wire.Start(ctx)

// Your router, your middleware, your auth
mux.Get("/events/{token}", func(w http.ResponseWriter, r *http.Request) {
    wire.ServeSSE(w, r, chi.URLParam(r, "token"))
})
```

## Tokens

Authentication is token-based. The server mints a signed token encoding which
topics the client may subscribe to. The token is embedded in the SSE
connection URL — no per-request auth callback, no query params to tamper with.

### Minting tokens

```go
// In your template handler — mint a token for this page/user.
token := wire.Token(
    []string{"work:01JABC", "user:01JXYZ"},
    catbird.TokenOpts{
        Identity:  user.ID.String(),            // for presence (optional)
        ValidFor: 1 * time.Hour, // token expiry (optional)
    },
)
```

The token is a signed, URL-safe string. It contains:
- **Topics**: which topics this connection may subscribe to
- **Identity**: opaque string identifying the client (for presence)
- **Expiry**: when the token becomes invalid

### TokenOpts

```go
type TokenOpts struct {
    Identity string        // presence identity (optional)
    ValidFor time.Duration // relative expiry from now (optional)
}
```

Pass no opts to use defaults:

```go
wire.Token([]string{"work:01JABC"})
```

### Token format

Tokens are opaque, URL-safe strings. Internal format:

```
base64url(AES-256-GCM(json(payload)))
```

Payload uses short keys for compactness:

```go
type tokenPayload struct {
    T []string `json:"t"`           // topics
    I string   `json:"i,omitempty"` // identity
    E int64    `json:"e,omitempty"` // expiry (unix seconds)
}
```

No padding (`base64.RawURLEncoding`). A typical token with 2 topics is
~150 characters — well within URL limits.

### Client connection

The token goes in the URL. `ServeSSE` verifies the signature, extracts
topics and identity, and streams events. Invalid or expired tokens cause
`ServeSSE` to write a 401 and return. No auth callback needed — auth is
baked into the token.

### htmx integration

The server renders the signed URL into the template:

```html
<div hx-ext="sse" sse-connect="/events/{{.Token}}">
  <header hx-get="/works/01JABC/header"
          hx-trigger="sse:updated" hx-swap="outerHTML">
    ...
  </header>

  <aside hx-get="/works/01JABC/presence"
         hx-trigger="sse:presence" hx-swap="innerHTML">
    ...
  </aside>
</div>
```

Token expiry and SSE reconnect work together naturally: when the token
expires, the Wire closes the connection. htmx reconnects with jitter, but
the old token is expired. The page needs a mechanism to obtain a fresh token
— either a full page reload, or an htmx swap that re-renders the
`sse-connect` attribute with a new token.


## Notify (SSE)

### Sending notifications

Notify is available on both Client and Wire.

```go
func (c *Client) Notify(ctx context.Context, topic, event string, data []byte) error
```

**`Client.Notify`** is the primary API. It fires a pg NOTIFY; every Wire
instance (on any node) picks it up and delivers to its local SSE
subscribers. Works from anywhere — HTTP handlers, task handlers, CLI
commands, separate processes. No reference to the Wire needed.

```go
cb.Notify(ctx, "work:01JABC", "updated", nil)
cb.Notify(ctx, "user:01JXYZ", "notification", nil)
cb.Notify(ctx, "broadcast", "maintenance", nil)

// With a data payload (HTML fragment, JSON, etc.)
cb.Notify(ctx, "user:01JXYZ", "notification", []byte(`<div class="toast">Done</div>`))
```

**`Wire.Notify`** does the same thing but also delivers to local subscribers
immediately (before the NOTIFY round-trip), skipping the local node when
the NOTIFY arrives (the payload includes the origin `node_id`). Available
when you happen to have a Wire reference; `cb.Notify` is preferred in most
code.

A catbird task handler can notify with the Client it already has:

```go
task := catbird.NewTask("import-plato").Do(func(ctx context.Context, in ImportInput) (ImportResult, error) {
    result, err := doImport(ctx, in)
    if err != nil {
        return result, err
    }
    cb.Notify(ctx, "work:"+result.WorkID, "updated", nil)
    return result, nil
})
```

### SSE format

```
event: updated
data:

event: notification
data: <div class="toast">Import completed: 42 works</div>
```

Events without data act as pure triggers — htmx can re-fetch content.
Events with data carry the payload directly for immediate swap.

### Event naming

SSE spec allows any character except LF, CR, and colon (`:`). No length
limit, no reserved names (default is `message` when absent). Case-sensitive.
In practice, stick to lowercase ASCII with hyphens: `updated`, `presence`,
`job-completed`. These work naturally as htmx triggers (`sse:updated`) and
are safe across all clients.

### htmx patterns

**Trigger pattern** — SSE event causes htmx to re-fetch content from the
server. Best for updates to visible resources. Keeps rendering server-side.

```html
<div hx-ext="sse" sse-connect="/events/{{.Token}}">
  <header hx-get="/works/01JABC/header"
          hx-trigger="sse:updated" hx-swap="outerHTML">
    ...
  </header>
</div>
```

**Swap pattern** — SSE event carries HTML directly. Best for notifications
and ephemeral UI that doesn't map to a fetchable resource.

```html
<div hx-ext="sse" sse-connect="/events/{{.Token}}">
  <div sse-swap="notification" hx-swap="afterbegin"></div>
</div>
```

### Cross-node relay

Notifications travel via pg NOTIFY on the schema-qualified channel
`{schema}.cb_wire` (built by `channelName(schema, "cb_wire")`).

**`Client.Notify`** — fires pg NOTIFY with no `node_id` (Client has no
node identity). Every Wire node delivers to its local subscribers:

1. `cb_notify(topic => $1, event => $2, data => $3)` — `node_id` is NULL
2. Each node's Wire LISTEN goroutine receives the notification
3. `node_id` is NULL → no skip → all nodes deliver

**`Wire.Notify`** — delivers locally first, then fires pg NOTIFY with its
own `node_id`. Other nodes deliver; the origin node skips (already delivered):

1. Deliver to local SSE subscribers immediately
2. `cb_notify(node_id => $1, topic => $2, event => $3, data => $4)`
3. Other nodes deliver; origin node sees its own `node_id` → skips

The Wire acquires one dedicated connection from the pool for LISTEN (same
pattern as the worker's notifier). If the LISTEN connection drops, events are
lost until reconnection — acceptable for ephemeral notifications. Clients reconnect
with jitter (htmx default: `full-jitter`), so they recover quickly.

**Payload size**: pg NOTIFY has an 8KB limit. `Notify` returns an error if
the serialized payload (topic + event + data) exceeds this limit. For the
trigger pattern (no data), payloads are tiny. For the swap pattern, keep
SSE data small or use the trigger pattern and let htmx re-fetch.

### Slow/stuck consumers

Each subscription has a bounded channel. If a client stops reading:

- Channel fills → new events for that client are dropped (not blocked)
- After 5 seconds with no successful writes, the connection is closed
  server-side

This prevents one slow client from blocking delivery to others or leaking
goroutines.

## Presence

Presence tracks which clients are connected to which topics. Enabled
automatically when tokens include an identity (`TokenOpts{Identity: ...}`).

### Lifecycle

1. SSE connection established → identity extracted from token
2. Presence `join` event sent to topic
3. Client disconnects → subscription removed → presence `leave` event sent

### Querying

```go
identities, err := wire.Presence(ctx, "work:01JABC") // []string
```

Always cross-node — queries the `cb_wire_presence` table.

### How it works

The Wire writes to `cb_wire_presence`:

- SSE connect → `INSERT INTO cb_wire_presence (identity, topic, node_id, connected_at)`
- SSE disconnect → `DELETE`
- `Presence()` → `SELECT DISTINCT identity` across all nodes
- Stale rows from crashed nodes are GC'd after 15 seconds (hardcoded, same pattern as worker staleness)

### Presence endpoint

Applications typically expose a presence endpoint that the SSE trigger
pattern can re-fetch:

```go
// In your app
func workPresence(w http.ResponseWriter, r *http.Request) {
    identities, _ := wire.Presence(r.Context(), "work:"+chi.URLParam(r, "id"))
    render(w, presenceTemplate(identities))
}
```

```html
<aside hx-get="/works/01JABC/presence"
       hx-trigger="sse:presence" hx-swap="innerHTML">
  ...
</aside>
```

## Forward (durable notifications)

`Notify` is ephemeral — if no SSE client is connected, the event is lost.
For durable notifications, publish to a queue and let Wire forward them.

### Notification struct

The same struct is used by both paths:

```go
type Notification struct {
    Event string `json:"event"`
    Data  []byte `json:"data,omitempty"`
}
```

### Ephemeral vs durable

```go
// Ephemeral — fire and forget
cb.Notify(ctx, "work:01JABC", "updated", nil)

// Durable — persisted in a queue until delivered
cb.Publish(ctx, "work.01JABC", catbird.Notification{Event: "updated"})
```

### Setup

```go
// Create a queue for browser-bound notifications
cb.CreateQueue(ctx, "browser-notifications")
cb.Bind(ctx, "browser-notifications", "work.#")
cb.Bind(ctx, "browser-notifications", "user.#")

// Wire reads from the queue and delivers to SSE subscribers
wire.Forward("browser-notifications")
```

`Forward` runs inside `wire.Start` alongside the LISTEN loop. It reads
messages from the queue, uses `msg.Topic` directly as the SSE topic
(no translation), and deserializes the payload as a `Notification`, then
delivers via the same `deliverLocal` path. Messages are deleted after
successful delivery.

Multiple Wire nodes share the queue as competing consumers — each message
is read by one node, which delivers locally and fires a pg NOTIFY so all
other nodes' SSE clients receive it too.

If no Wire is running, messages accumulate in the queue. When a Wire
starts, it drains the backlog.

## Configuration

### Builder methods

```go
wire := catbird.NewWire(pool, secret).
    WithLogger(logger)
```

All builder methods return `*Wire` for chaining. Presence is automatic
when tokens include `Identity` — no configuration needed.

## Lifecycle

```go
wire := catbird.NewWire(pool, secret)

// Start the LISTEN goroutine for cross-node relay.
// Blocks until ctx is cancelled. Run in a goroutine.
go wire.Start(ctx)

// Mount in your router.
mux.Get("/events/{token}", func(w http.ResponseWriter, r *http.Request) {
    wire.ServeSSE(w, r, chi.URLParam(r, "token"))
})

// Mint tokens in your template handlers.
token := wire.Token([]string{"work:01JABC"})

// Notify from anywhere via Client.
cb.Notify(ctx, "work:01JABC", "updated", nil)

// Query presence.
identities, err := wire.Presence(ctx, "work:01JABC")
```

On shutdown (`ctx` cancelled):
1. Close all active SSE connections (clients reconnect to other nodes)
2. Clean up presence entries for this node
3. Release the LISTEN connection

## Implementation notes

This section documents the internal design. The classic approach (gorilla
websocket Hub example, widely copied) uses a single goroutine running a
select loop over register/unregister/broadcast channels. That works for
demos but has real problems:

- The hub goroutine is a serialization point — all operations (subscribe,
  unsubscribe, broadcast) queue behind each other
- Broadcasting is O(n) inside the hub, blocking subscribes/unsubscribes
  for the entire fan-out duration
- No topic granularity — broadcast goes to everyone, filtering is
  after-the-fact

The Wire avoids all of these.

### Data structures

```go
type Wire struct {
    pool      *pgxpool.Pool
    secret    []byte
    logger    *slog.Logger

    mu        sync.RWMutex
    topics    map[string][]*subscriber  // topic → active subscribers
}

type subscriber struct {
    ch       chan event      // bounded, per-subscriber
    identity string         // from token, for presence
    cancel   context.CancelFunc  // close this connection
}

type event struct {
    name string  // SSE event name
    data []byte  // SSE data (may be nil)
}
```

No hub goroutine. The topic map is protected by a `sync.RWMutex`.
Publishing takes a read lock (concurrent publishes to different topics
don't block each other). Subscribe/unsubscribe take a write lock, but
only briefly — they modify a single topic's slice, not the whole map.

### Subscribe path

When an SSE connection is accepted:

1. Verify token (decrypt, check expiry)
2. Create a `subscriber` with a bounded channel (`make(chan event, channelSize)`)
3. Derive a per-connection `context.Context` from the request context
4. Write-lock `mu`, append subscriber to each topic's slice, unlock
5. If presence is enabled: write to `cb_wire_presence`, notify `join` event to topic
6. Enter the write loop (see below)
7. On context cancellation (client disconnect or server shutdown):
   write-lock `mu`, remove subscriber from each topic's slice, unlock.
   If presence: delete from `cb_wire_presence`, notify `leave` event.

Cleanup is driven by context cancellation — no explicit unregister channel,
no polling for dead connections.

### Write loop (per connection)

Each SSE connection runs a single goroutine:

```go
func (w *Wire) writeLoop(rw http.ResponseWriter, sub *subscriber) {
    flusher := rw.(http.Flusher)
    for {
        select {
        case ev := <-sub.ch:
            // Write SSE frame: "event: <name>\ndata: <data>\n\n"
            writeSSEFrame(rw, ev)
            flusher.Flush()
        case <-sub.ctx.Done():
            return
        }
    }
}
```

No write timeout on individual frames — if `Flush()` blocks because the
client's TCP buffer is full, the goroutine blocks on that single
connection. Other connections are unaffected (they have their own
goroutines). Slow consumer detection (see below) handles connections
that go too long without a successful write.

### Publish path (local delivery)

```go
func (w *Wire) deliverLocal(topic string, ev event) {
    w.mu.RLock()
    subs := w.topics[topic]  // slice header copy under read lock
    w.mu.RUnlock()

    for _, sub := range subs {
        select {
        case sub.ch <- ev:
            // delivered
        default:
            // channel full — drop for this subscriber
            // (tracked for slow consumer detection)
        }
    }
}
```

Key properties:
- **Read lock only** — concurrent publishes to different (or same) topics
  don't block each other
- **Non-blocking send** — a full channel means this subscriber is slow;
  the event is dropped for them, not for others. No goroutine blocks.
- **No per-publish goroutine** — fan-out is a simple loop in the caller's
  goroutine. At 1000 subscribers per topic, this is ~1000 non-blocking
  channel sends — microseconds.

### Publish path (cross-node via NOTIFY)

`Client.Notify` calls a SQL function:

```sql
-- cb_notify wraps pg_notify with schema-qualified channel and JSON payload.
-- node_id is NULL when called from Client.Notify (no self-skip).
SELECT cb_notify(node_id => $1, topic => $2, event => $3, data => $4);
```

On the receiving side, `Wire.Start` runs a LISTEN loop on a dedicated
connection (same pattern as the worker notifier):

```go
func (w *Wire) listen(ctx context.Context) error {
    conn, err := w.pool.Acquire(ctx)
    // ...
    _, err = conn.Exec(ctx, "LISTEN "+channelName(schema, "cb_wire"))
    // ...
    for {
        notification, err := conn.WaitForNotification(ctx)
        if err != nil {
            // reconnect with backoff
            continue
        }
        var msg wireMessage
        json.Unmarshal([]byte(notification.Payload), &msg)
        if msg.NodeID == w.id {
            continue // already delivered locally by Wire.Notify
        }
        w.deliverLocal(msg.Topic, event{name: msg.Event, data: msg.Data})
    }
}
```

The LISTEN goroutine is the only goroutine the Wire adds beyond the
per-connection write loops. It deserializes NOTIFY payloads and calls
`deliverLocal` — same path as direct local delivery.

### Slow consumer detection

Each subscriber has a bounded channel. When the channel is full, events
are dropped (non-blocking send). The Wire tracks consecutive drops per
subscriber. After 5 seconds with no successful delivery, the Wire calls
`sub.cancel()`, which cancels the connection context:

- The write loop's `<-sub.ctx.Done()` fires
- The HTTP handler returns
- The deferred cleanup removes the subscriber from the topic map
- The client (htmx) reconnects with jitter

This is entirely per-subscriber — one slow client doesn't affect others
and doesn't require any coordination.

### LISTEN connection resilience

The LISTEN connection can drop (network blip, PostgreSQL restart). When
this happens:

1. `WaitForNotification` returns an error
2. The Wire backs off (jitter) and reacquires a connection from the pool
3. Re-issues LISTEN on the schema-qualified channel
4. Resumes the notification loop

Events published during the gap are lost. This is acceptable — SSE is
ephemeral. Clients that missed updates will either:
- See stale UI until the next notification (trigger pattern)
- Miss a transient notification (swap pattern — gone anyway)

For durable delivery, use catbird queues.

### Presence schema

Tables live in migration `00016_wire_schema.sql`:

- **`cb_wire_nodes`** — UNLOGGED. One row per Wire instance. Heartbeated
  periodically; stale nodes (>15s) are GC'd by `cb_gc()`.
- **`cb_wire_presence`** — UNLOGGED. One row per (identity, topic, node)
  triple. FK CASCADE on `node_id` → stale node deletion cleans up presence
  automatically.

The `node_id` is a UUID generated at Wire startup (same pattern as
worker `id`). The `cb_notify` SQL function wraps `pg_notify` with the
schema-qualified channel name and JSON payload — available to any SQL
client, not just the Go SDK.

### Memory budget

Per SSE connection:
- 1 goroutine (~4KB stack, grows as needed)
- 1 channel buffer (`channelSize` × event size, default 64 × ~100 bytes ≈ 6KB)
- Subscriber struct overhead: ~100 bytes
- Total: ~10KB per connection

At 1000 connections: ~10MB. At 10,000: ~100MB. Well within budget for a
Go process.

The topic map itself is negligible — a map entry per topic, a slice of
pointers per topic. Even with 10,000 unique topics, the map overhead is
under 1MB.

## What this does NOT do

- **Message persistence**: `Notify` is ephemeral — if no SSE client is
  connected, the event is lost. Use `Forward` to bridge durable queue
  messages to SSE.
- **Client-to-client messaging**: all messages originate from the server.
  Clients trigger server-side work via your app's HTTP handlers.
- **Binary payloads**: SSE is text-only. Use base64 or link to a resource.
- **Authentication**: the Wire signs and verifies tokens but does not
  implement any external auth scheme. Your app mints tokens.
