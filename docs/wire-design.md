# Wire

## The Problem

Today Wire is SSE-only, Listener is pub/sub-only, and they duplicate
topic matching. There's no in-process pub/sub without round-tripping
through Postgres. No clean path from "event happened" to "HTML fragment
pushed to client". No replay on reconnect.

## Wire = Pub/Sub + SSE + Presence

Wire absorbs Listener and keeps SSE. One package for all real-time
concerns. Presence stays on Wire (transport-agnostic — if WebSocket
comes later, both transports share the same presence).

```go
wire := catbird.NewWire(pool, secret)

// Listen: server-side callbacks (replaces Listener.Handle)
wire.Listen("order.*", func(ctx context.Context, topic, message string) {
    log.Println(topic, message)
})

// Render: transform events before SSE dispatch
wire.Render("task.*.completed", func(r *http.Request, topic, message string) (string, error) {
    user := auth.UserFromRequest(r)
    return renderTaskDone(user, message), nil
})

// Notify: local dispatch + pg_notify for cross-node
wire.Notify(ctx, "order.created", `{"id": 123}`)

// SSE: app controls token retrieval
http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
    token := r.URL.Query().Get("token")
    wire.ServeSSE(w, r, token)
})

// Presence (transport-agnostic, stays on Wire)
wire.Presence(ctx, "dashboard")

go wire.Start(ctx)
```

### Listen vs Render

| Method         | Runs on          | Has request | Purpose                          |
|----------------|------------------|-------------|----------------------------------|
| `wire.Listen`  | every node       | no          | Side effects (logging, webhooks) |
| `wire.Render`  | node with client | yes         | Transform for SSE client         |

`Listen` handlers fire on every node. They're for side effects.

`Render` handlers run only on nodes with matching SSE clients.
They transform raw data into what the client sees (e.g., JSON → HTML).
Without a Render, clients get the raw message.

### Delivery

```
wire.Notify(ctx, "order.created", data)
  ├─ local: trie match → call Listen handlers
  │                     → for SSE clients: run Render → push
  └─ remote: pg_notify → other nodes' Wire → same local dispatch
```

### What Changes

| Today                        | New Wire                          |
|------------------------------|-----------------------------------|
| `Listener.Handle()`          | `wire.Listen()`                   |
| `Listener.Start()`           | absorbed into `wire.Start()`      |
| `Wire.deliverLocal()`        | `wire.Notify()` local path        |
| `Wire.Notify()` (no local)   | `wire.Notify()` (local + remote)  |
| Two tries (Wire + Listener)  | One trie                          |

Listener goes away entirely. Package-level `Notify()` stays as low-level
function; Wire uses it internally for the remote leg.

Worker notifier is unrelated plumbing — stays as-is.

### Render Variants

High-level generic functions for common patterns. Package-level because
Go can't have generic methods.

```go
// Stateless: fn(T) io.WriterTo
catbird.Render[TaskEvent](wire, "task.*", views.TaskCompleted)

// Request-aware: fn(*http.Request, T) io.WriterTo — for i18n, auth, etc.
catbird.RenderFunc[TaskEvent](wire, "task.*", views.TaskCompleted)

// Full control: raw strings, manual unmarshal
wire.Render("task.*", func(r *http.Request, topic, message string) (string, error) {
    // ...
})
```

Implementation:

```go
func Render[T any](w *Wire, pattern string, fn func(T) io.WriterTo) {
    w.Render(pattern, func(r *http.Request, topic, message string) (string, error) {
        var data T
        if err := json.Unmarshal([]byte(message), &data); err != nil {
            return "", err
        }
        var buf strings.Builder
        _, err := fn(data).WriteTo(&buf)
        return buf.String(), err
    })
}

func RenderFunc[T any](w *Wire, pattern string, fn func(*http.Request, T) io.WriterTo) {
    w.Render(pattern, func(r *http.Request, topic, message string) (string, error) {
        var data T
        if err := json.Unmarshal([]byte(message), &data); err != nil {
            return "", err
        }
        var buf strings.Builder
        _, err := fn(r, data).WriteTo(&buf)
        return buf.String(), err
    })
}
```

Nothing in catbird imports any template library. `io.WriterTo` is the
bridge — Templ components, `html/template`, or plain functions all work.

## Replay (Last-Event-ID)

Every SSE hub supports reconnection with message replay. When a client
disconnects and reconnects, the browser automatically sends
`Last-Event-ID` header. The hub replays missed events.

Wire is ephemeral — no event storage, no replay. For replay, bridge
through a queue (the durable bridge). The queue *is* the replay buffer.

### How It Works

```
1. Client connects SSE
2. Wire assigns event IDs to SSE messages (monotonic per connection)
3. Events also written to a queue with expiry (via durable bridge)
4. Client disconnects
5. Client reconnects with Last-Event-ID header
6. Wire reads missed events from queue, replays through Render
7. Resumes live streaming
```

Wire checks `r.Header.Get("Last-Event-ID")` on connect. If present and
a durable renderer is registered for the client's topics, Wire reads
buffered events from the queue and replays them before switching to
live streaming.

### No Replay (default)

Without a durable bridge, Wire doesn't store events. Reconnecting
clients miss whatever happened while disconnected. Fine for ephemeral
UI updates (cursor position, typing indicators).

### With Replay

Compose the existing pieces — queue for storage, Reader for bridging,
Render for presentation:

```go
// Queue with expiry = replay buffer
client.CreateQueue(ctx, conn, "notifications",
    catbird.WithExpiry(5 * time.Minute))

// Reader bridges queue → Wire
go catbird.Reader(ctx, conn, "notifications",
    func(ctx context.Context, msg Message) error {
        wire.Notify(ctx, msg.Topic, string(msg.Body))
        return nil
    },
)

// Render for SSE clients
catbird.RenderFunc[Notification](wire, "notifications.*", views.Notification)
```

On reconnect with `Last-Event-ID`, Wire reads missed events from
the queue and replays them through Render before switching to live
streaming. The queue expiry controls how far back replay can go.

## Reader

Continuous queue consumer, aligned with the existing `Read`/`ReadPoll` API:

```go
// Existing one-shot / poll:
catbird.Read(ctx, conn, "queue", quantity, hideFor)
catbird.ReadPoll(ctx, conn, "queue", quantity, hideFor, ReadPollOpts{...})

// Continuous reader — same pattern, same opts:
catbird.Reader(ctx, conn, "notifications",
    func(ctx context.Context, msg Message) error {
        // return nil → ack (delete)
        // return err → nack (message becomes visible again after hideFor)
        return nil
    },
    catbird.ReadPollOpts{
        Quantity:     10,
        HideFor:      30 * time.Second,
        PollFor:      5 * time.Second,
        PollInterval: 100 * time.Millisecond,
    },
)

// Or via Client:
client.Reader(ctx, "notifications", handler, catbird.ReadPollOpts{...})
```

Blocks until ctx is cancelled. Loops `ReadPoll` internally. Same opts.
Handler return nil = ack (delete). Return error = nack (re-hides,
retried after `HideFor`).

## Naming: Notify vs Publish

Two verbs, two delivery models.

```
Notify  = ephemeral (at-most-once, no storage)
Publish = durable   (at-least-once, routed to queues/tasks/flows)
```

| Method              | Delivery   | Mechanism                        |
|---------------------|------------|----------------------------------|
| `wire.Notify()`     | ephemeral  | local dispatch + pg_notify       |
| `client.Publish()`  | durable    | SQL `cb_publish` → bindings      |

### Notify Levels

| Function                     | Conn      | Local | Transactional | Use case              |
|------------------------------|-----------|-------|---------------|-----------------------|
| `Notify(ctx, conn, ...)`     | explicit  | no    | yes           | package-level, any Conn |
| `client.Notify(ctx, ...)`    | client    | no    | yes           | client API             |
| `wire.Notify(ctx, ...)`      | owns pool | yes   | no            | server-side pub/sub    |

`Notify(ctx, conn, topic, message)` — plain function, takes any `Conn`.
Works inside transactions; `pg_notify` fires on commit.

`client.Notify(ctx, ...)` — same thing, uses client's Conn. Stays.

`wire.Notify(ctx, ...)` — local dispatch immediately, plus pg_notify
for cross-node. For pub/sub outside of transactions.

## GetConn(ctx) — Handler Access to Conn

Handler context already carries a conn (in taskRunScope/flowRunScope).
`GetConn(ctx)` exposes it, giving handlers access to the full Conn
interface — including transactions:

```go
func(ctx context.Context, input Order) (Result, error) {
    conn := catbird.GetConn(ctx)

    tx, _ := conn.Begin(ctx)
    defer tx.Rollback(ctx)
    catbird.Send(ctx, tx, "audit", body)
    catbird.Notify(ctx, tx, "order.progress", data)  // fires on commit
    tx.Commit(ctx)

    return result, nil
}
```

For Client sugar, `catbird.New(conn)` already exists:

```go
c := catbird.New(catbird.GetConn(ctx))
c.Send(ctx, "audit", body)
```

Handler context functions for reference:

| Function                      | From ctx          | Purpose                |
|-------------------------------|-------------------|------------------------|
| `GetConn(ctx)`                | conn              | Raw Conn access (new)  |
| `Cancel(ctx)`                 | conn, name, runID | Cancel current run     |
| `CompleteEarly(ctx, out, r)`  | flow scope        | Complete flow early    |
| `GetTaskRunID(ctx)`           | task scope        | Current task run ID    |
| `GetFlowRunID(ctx)`           | flow scope        | Current flow run ID    |

## Comparison to Phoenix LiveView

Phoenix needs GenServers with assign tracking and server-side diffing.
Catbird doesn't — HTMX handles DOM patching client-side. The server sends
full fragments, HTMX swaps them in. More bytes over the wire, but zero
server-side diff machinery. Renderers are stateless pure functions.

```
Phoenix:  server renders → server diffs → minimal patch → client applies
Catbird:  server renders fragment → full fragment → HTMX swaps
```

## End-to-End: Task → HTMX Streaming

```
1. Browser connects SSE (token topics: ["task.invoice.*"])
   → Wire subscribes internally

2. POST /tasks/invoice → client.RunTask(ctx, "invoice", input)

3. Task emits progress:
   wire.Notify(ctx, "task.invoice.progress", `{"step":1}`)
   → Listen handlers fire (logging, etc.)
   → Render (with *http.Request) → HTML fragment → SSE push
   → pg_notify → other nodes do the same

4. Task completes → SQL _cb_notify("catbird.task.completed", ...)
   → pg_notify → Wire receives → Render → completion fragment → SSE

5. HTMX receives SSE event, swaps fragment into DOM

Task handler never touches HTML. Render has full request context.
```

## End-to-End: Reconnect with Replay

```
1. Client connected, receiving events on "notifications"
   → durable bridge: queue + Reader → Wire

2. Client loses connection (network drop, tab sleep)
   → Events keep arriving, buffered in queue with 5min expiry

3. Client reconnects, browser sends Last-Event-ID: "42"
   → Wire reads events after ID 42 from queue
   → Replays through Render → SSE push
   → Switches to live streaming

4. After 5 minutes, old events expire from queue via GC
```

## Open Questions

1. **Handler concurrency** — run Listen/Render synchronously (Listener
   today) or per-goroutine? Synchronous is simpler; slow Render blocks
   other clients on same event.

2. **Client targeting** — SSE fans out to all matching connections.
   Targeting a specific client needs topic scoping
   (`task.invoice.{request_id}.*`) or explicit addressing.

3. **Durable fan-out** — queue consumers compete (one claims each message).
   SSE is fan-out. The bridge solves this: queue → single consumer →
   wire.Notify (fan-out). Clear enough or needs a primitive?

4. **Render error handling** — skip SSE push? Log and send raw? Error
   fragment?

5. **Event IDs** — monotonic per connection, or global? Global needs a
   shared counter (sequence in PG?). Per-connection is simpler but
   doesn't support cross-node replay.

6. **Replay across nodes** — client connected to node A, reconnects to
   node B. If queue is shared (it is — it's in PG), replay works.
   But Render must be deterministic across nodes (same input → same
   output). Stateless renderers already guarantee this.
