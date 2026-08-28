# Catbird Lite

A PostgreSQL-backed job queue, stream, and small workflow engine. Four tables, plain SQL, no PL/pgSQL, no extensions. All logic lives in a thin client (Go today; other languages follow the same statements), so the whole system is the schema in `migrations/00001_lite.sql` plus the statements in `client.go`, `worker.go`, and `stream.go`.

## Tables

- `cb_messages` — every job input and every stream event is one row. Job inputs are written once; published messages are updated once, when the assigner sets their `position`. Measured: one dead tuple per published message, cleaned by a routine vacuum; see the position benchmark note below.
- `cb_claims` — one narrow row per job that still has to run. Updated on every claim and retry, deleted on completion. Aggressive autovacuum settings on the table keep it small.
- `cb_cursors` — one row per stream consumer: the highest position it processed.
- `cb_signals` — payloads delivered to a job that waits for them.
- `cb_outputs` — optional job results, written by the handler with `SetOutput` in its transaction, read with `Output`.

A partial index on `cb_claims (queue, visible_at) WHERE status = 0 AND dependencies = 0` holds only claimable rows. Dead rows and rows waiting on dependencies are not in it.

## Runtime

`catbird.New(pool, opts)` returns the process's `Runtime`. Workers, triggers and consumers are declared on it — `NewWorker(runtime, …)`, `NewTrigger(runtime, …)`, `NewConsumer(runtime, …)`, or the methods of the same names — and `Start(ctx)` runs them all: one `LISTEN` connection for every channel they need, the position assigner, and one goroutine per declared loop, until `ctx` ends and every loop has stopped. A process holds one connection for notifications however many workers and triggers it runs. Declaring after `Start` panics: the connection's channel set is fixed when it connects. A dropped connection is reconnected after `ReconnectAfter`; until then the loops run on their poll intervals, and after each connect every loop is woken once, because notifications sent in between are gone.

`Client` is the exception: it is a plain helper that works on any connection or transaction, so it is not created from the runtime.

## Jobs

`Enqueue` inserts the message and its claim in one statement and sends `NOTIFY` on the queue's channel. The runtime's connection listens on it and wakes the queue's workers, which also poll on an interval, so a lost notification delays a job rather than losing it.

**Claiming.** A worker takes up to `BatchSize` rows with `FOR UPDATE SKIP LOCKED`, sets `visible_at = now() + Lease`, and increments `attempts`. There is no "running" status: a job with `visible_at` in the future is either delayed, backing off after a failure, or claimed. Once `visible_at` passes, any worker may claim it again. That is how a crashed worker's job comes back.

**Completing.** The worker opens a transaction, passes it to the handler, and in the same transaction runs `DELETE FROM cb_claims WHERE message_id = $1 AND attempts = $2`. The handler's writes and the job's completion commit together. `attempts` is the lease token: if the lease expired and another worker claimed the job, `attempts` moved on, the delete finds nothing, and the late worker rolls back. Two workers may execute the same job; only the one holding the lease commits. Side effects outside the database (emails, HTTP calls) are not covered by this, so handlers that must not repeat them need their own idempotency key — `Message.ID` works.

**Lease rule.** A handler must finish within `Lease` or its work is discarded and the job runs again. Set `Lease` above your longest handler.

**Settings rule.** `Lease`, `MaxAttempts` and `Backoff` are worker settings; nothing about them is stored with the job. All workers on one queue must use the same values, otherwise how long a job may run and how often it is retried depend on which worker took the attempt.

**Failing.** A handler error sets `visible_at = now() + Backoff`. After `MaxAttempts` the claim is marked dead (`status = 1`), `Cancel` runs for its correlation id, and `OnDead` runs once outside the job transaction. A crash counts as a failed attempt like any other, so `OnDead` also fires for jobs that repeatedly crashed a worker.

**Cancel rule.** `Cancel(correlationID)` marks live claims dead. It stops jobs from starting; a job that is already running finishes and commits. Cancel does not undo anything.

## Dependencies and signals

`EnqueueOptions.Dependencies = n` creates a job that stays out of the ready index until `n` events arrive. Two kinds of event count:

- `ResolveDependency(childID)` — a parent step completed. Call it inside the parent's handler transaction so it commits with the parent's completion.
- `DeliverSignal(childID, name, payload)` — an external input arrived. The payload is stored in `cb_signals` and handed to the handler as `Message.Signals[name]` at claim time. Delivering the same name twice is a no-op.

The decrement is `UPDATE ... SET dependencies = dependencies - 1 WHERE dependencies > 0`, so concurrent parents do not lose an update. When the counter reaches 0 the statement also sends `NOTIFY`.

**Signal rule.** A signal must be counted in `Dependencies` before it is delivered. Delivering to a job that is not waiting returns `ErrNotFound`; nothing is stored. Signals that arrive before the job exists are the caller's problem to retry.

A permanently failed step cancels its siblings and children through the shared correlation id. Children then stay dead with `dependencies > 0` until `GC` removes them.

## Streams

`Publish` inserts a message with no claim. `NewConsumer(runtime, name, opts).FetchBatch(topic)` reads messages after the cursor on a topic and every topic under it (`order` covers `order.paid` and `order.paid.refund`; `""` covers everything), in position order. Topic names are literal; there is no pattern syntax. Finer selection is the consumer's code, or an optional payload filter added later as an extra clause. `Ack(position)` moves the cursor; it uses `GREATEST`, so the cursor does not move backwards when two consumers share it.

**Positions.** Message ids are handed out at `INSERT` time, so a message from a transaction that is still open can have a lower id than messages that already committed; a reader going by id would move past it and miss it. Readers therefore go by `position`, which the assigner sets on published messages in the order it sees them — commit order. A `stream` flag marks published messages; job inputs get no position, so a job created from an event is not itself an event and a trigger does not feed on its own output. A message from a long transaction gets its position when it commits; it arrives late, after messages published after it, but it arrives, once. This is the rule a plain `SELECT` follows: you see a row when its transaction commits.

The assigner is one statement under an advisory lock, run every `AssignEvery` (250 ms) by every process's `Runtime`. The lock makes one of them do the work; the rest do nothing. The statement only sets positions that are still empty, so even two assigners running at once cannot move a position a reader may already have passed. Nothing has to be deployed or configured; a message is readable within one tick of its commit. When the assigner assigned anything it sends `NOTIFY cb_stream` with the highest new position; the runtime's connection listens on that channel and wakes the triggers, so they fetch on arrival instead of polling. The cost is one update per published message. Against a variant with positions in a separate narrow table (measured, 200k × 500 B): the column writes ~45% more WAL and ~60% more heap, but publishes and reads ~65% faster, deletes 2× faster (no FK cascade), and needs one table and one index fewer. Vacuum time was under 0.3 s per 200k messages for both.

**Triggers.** `NewTrigger(runtime, name, topic, queue, opts)` declares a loop that runs from `Start`: fetch a batch, `Enqueue` each message on the target queue with dedup key `trigger:<name>:<message id>`, `Ack`, commit — all in one transaction. A crash before commit redoes the batch; the dedup keys make the redo a no-op. Several processes may run the same trigger: each only wastes reads, the cursor is monotone, and the dedup keys keep the output single. Run a trigger in one process if the extra reads matter; there is no leader election.

## Cron without a leader

Every process enqueues `cron:<name>:<minute>` as the dedup key when the minute starts, with the minute in UTC as `YYYY-MM-DDTHH:MMZ` (`cron:report:2026-08-28T09:30Z`). Exactly one insert goes through; the format is fixed because every process must produce the same key. A process that wakes late (suspended, paused) should compare the scheduled minute with the clock and skip if it is too far behind.

## Retention

`GC(retention)` deletes dead claims older than `retention` and then messages older than `retention` that have no claim. A delayed or waiting job keeps its message however old it is. `cb_signals` and `cb_claims` reference `cb_messages` with `ON DELETE CASCADE`, so nothing is orphaned.

## Known limits

- A worker processes one batch to completion before claiming the next, so one slow job holds up the other jobs of its batch.
- Rate limits and per-queue configuration are out of scope. Applications build them on their own tables. The browser layer is planned; see below.
- Without a `Logger`, failures are reported through `slog.Default()`. Errors the library cannot return to the caller are logged.

## Planned additions

These come from moving raven, the first application, onto catbird: its own event log stays its own table, and everything that moves data — change signals, cursors, jobs, browser delivery — comes from here. Each item says what it is and why it is needed.

### Streams

**Cursor lease.** `cb_cursors` gets `locked_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity'`. A consumer claims a cursor with `UPDATE cb_cursors SET locked_until = now() + lease WHERE name = $1 AND locked_until <= now() RETURNING last_position`; when the row is already leased the claim returns nothing and the consumer waits for the next wake-up. `Ack` keeps `GREATEST` and clears the lease; acking the unchanged position releases without advancing. Reason: an application runs several processes, and a consumer that indexes documents or calls an external API would otherwise do every batch once per process. A lease with a deadline also covers a process that is alive but stuck: when the deadline passes another process takes the cursor. Triggers did not need this because their dedup keys make a repeated batch harmless; a general handler has no such key.

**`Consumer.Handle(topic, handle func(ctx, []Message) error)`.** Declares a loop that `Start` runs: claim the cursor, fetch a batch, call the handler, ack; wake on `NOTIFY cb_stream`, poll on `PollInterval` as the fallback, and keep going while batches come back full. A handler error releases the cursor without advancing, so the batch is retried. Triggers become a handler that enqueues the batch and acks in one transaction.

**`PublishMany(ctx, db, topics, payloads)`.** One `INSERT … SELECT FROM unnest(...)`, so a transaction that changed 10,000 records publishes 10,000 signals in one statement instead of 10,000 round trips.

**`Read(ctx, pool, topics, after, limit)` and `LastPosition(ctx, db)`.** The read for a caller that holds its own position instead of a cursor: the wire, or a poll endpoint. `topics` is a list of subtrees; the query walks the position index from `after`. `LastPosition` is the current end of the stream, so a page can embed it and start its connection from there.

**`Message.CreatedAt`.** The column exists; the field lets a consumer or renderer skip messages that are too old to matter to it.

### Wire

The browser layer: stream messages pushed to browsers over SSE. One type, created from the runtime, no tables of its own, no token machinery.

```go
type WireOptions struct {
    BatchSize    int           // messages read per round; default 50
    PollInterval time.Duration // read anyway and send an SSE comment so proxies keep the connection open; default 15 s
    Logger       *slog.Logger
}

type ServeOptions struct {
    Topics []string // topic subtrees this connection may read
    Cursor string   // when set: start at this cb_cursors row and ack every position sent
    Render func(topic string, payload json.RawMessage) (Fragment, error) // nil sends the payload JSON
}

// Fragment is one frame's content. Event defaults to the topic; empty Data
// sends nothing. Write lets a template component render straight into it.
type Fragment struct {
    Event string
    Data  string
}

func NewWire(r *Runtime, opts WireOptions) *Wire
func (w *Wire) Serve(rw http.ResponseWriter, r *http.Request, opts ServeOptions)
```

**Who may read what** is the application's decision, made before it calls `Serve`: the route knows the user and passes the subtrees. Same-origin `EventSource` sends cookies, so a session cookie is enough; an application that wants grants in the URL signs them itself.

**One goroutine per connection.** After every wake-up from the runtime's connection — the assigner's `NOTIFY cb_stream` — the SSE connection runs `Read(topics, after, BatchSize)` and writes each row as a frame. The database does the topic matching, per connection. A slow browser slows only its own goroutine and catches up by position; there is no queue between the listener and the connections, so nothing is dropped and no slow-consumer policy is needed. Every `PollInterval` the connection reads anyway and sends `: ping`, which keeps a proxy from closing an idle stream.

**Where a connection starts.**

- With `Cursor`: at the `cb_cursors` row, created at 0 if missing. After a batch is written and flushed the connection acks the last position, so a message is shown once across page loads and tabs. Sent is seen. A crash between the flush and the ack shows the same message once more; nothing is lost. `Last-Event-ID` is ignored here: acks follow sends, so a reconnecting tab's id is not ahead of the cursor. This is the durable inbox: a notification is `Publish("user.<id>.<kind>", payload)`, the tray is `Serve` on `user.<id>` with cursor `user:<id>`, and retention is `GC`. Two tabs open at once may both show a message that arrived before either acked; per-tab cursors (`user:<id>:<tab>`) would avoid that at the price of showing everything in every tab.
- Without `Cursor`: at `Last-Event-ID` when the browser reconnects (checked first, because `EventSource` reconnects with the original URL and `?after=` is stale by then), else at `?after=`, else at `LastPosition`. The browser holds the position; a dropped connection resumes where it stopped for as long as `GC` still holds the rows. A page embeds `LastPosition` at render time so the messages between rendering and connecting are not missed. For a `Last-Event-ID` older than retention the connection simply gets what is left; a page that cannot afford the gap refetches its state.

**The frame.** Plain SSE, nothing framework-specific:

```
id: 4183
event: user.7f3a.batch_edit
data: <div class="alert">…</div>

```

`id:` is the position, `event:` is the topic unless `Render` set `Fragment.Event`, `data:` is `Fragment.Data` split into one `data:` line per line of text. When `Render` is nil the payload JSON is the data. A render error is logged and the message skipped; the position still advances, and a skipped message is read and skipped again after a reconnect, which is harmless.

**Rendering** is a closure the route builds, so it has the request's user, language and view context in hand without the library knowing about any of them:

```go
app.wire.Serve(w, r, catbird.ServeOptions{
    Topics: []string{"user." + user},
    Cursor: "user:" + user,
    Render: func(topic string, payload json.RawMessage) (catbird.Fragment, error) {
        f := catbird.Fragment{Event: "notification"}
        switch kind := topic[strings.LastIndex(topic, ".")+1:]; kind {
        case "batch_edit":
            var out BatchOutput
            if err := json.Unmarshal(payload, &out); err != nil {
                return f, err
            }
            return f, views.BatchEditNotification(vc, out).Render(r.Context(), &f)
        default:
            return catbird.Fragment{}, nil // unknown kind: nothing sent
        }
    },
})
```

**The browser side.** The htmx SSE extension and a plain `EventSource` both resend `Last-Event-ID` on reconnect by themselves:

```html
<div id="notifications" hx-ext="sse" sse-connect="/notifications/stream"
     sse-swap="notification" hx-swap="afterbegin"></div>

<div hx-ext="sse" sse-connect="/events?after={{ .LastPosition }}"
     sse-swap="record.work.0193…" hx-swap="innerHTML"></div>
```

`wire.js`, served by `ServeScript`, is the glue for pages without htmx: it connects an `EventSource` and re-dispatches each named event as a DOM event `wire:<topic>`, optionally swapping the data into elements whose `data-wire-swap` lists the topic. `EventSource` has no wildcard for event names, so a page names the topics it uses.

**Not in it, on purpose.**

- Inbox, watches and presence tables: the inbox is a cursor (above); "who is on this record" is an application table with a heartbeat column and a message on the record's topic when it changes.
- Poll transport: `Read(topics, after)` behind a GET is the whole thing.
- A shared read per process with in-memory fan-out. Every connection runs its own `Read` per wake-up, so hundreds of open tabs on a busy stream mean hundreds of small index reads per assigner tick. If that ever matters, one read per process with matching in memory can be added behind the same frame contract.

### Jobs

**Run status.** `Status(ctx, db, id)` returns queued, scheduled, running, dead or completed, with the attempt count: completed means the message exists and its claim is gone; a live claim with `visible_at` in the future is running or waiting to retry, which `attempts` and the worker's `MaxAttempts` tell apart. `cb_claims` gets a nullable `last_error TEXT`, written on every failed attempt, so an application can show why a run failed or is retrying.

**Cancelling a running job.** Today `Cancel` stops jobs from starting and a running job finishes. The addition: `Cancel` also sends `pg_notify('cb_queue_<queue>', 'cancel:<message id>')`, and a worker that is running that job cancels the handler's context. The handler decides what a cancelled context means — stop at the next safe point, or finish — so the database still does not interrupt a running job; it only tells the handler that a cancel arrived. Open decision: keep the weak cancel and have handlers that must stop early poll `cb_claims.status` at their own boundaries, or add the notification.

**Head-of-line blocking.** A worker finishes its whole batch before claiming again, so one long job holds up the others in its batch. The fix is a worker that keeps `BatchSize` jobs running and claims a new one whenever one finishes, instead of claiming in rounds. Until then, run long-running kinds on their own queue with `BatchSize: 1`.

**`Worker.RunOnce(ctx)`.** Claim one batch, run it, return. Tests run jobs deterministically without a background worker.

**Cron helper.** `RunCron(ctx, pool, name, every, queue, topic)` enqueues on the interval and once at start, so applications do not each rebuild the key format, which every client must produce identically. It sets both keys: `DedupKey` `cron:<name>:<minute>`, so several processes ticking in the same minute produce one job, and `UniqueKey` `cron:<name>`, so a run that takes longer than its interval does not pile up — while the previous run is still live the tick's enqueue does nothing and that tick is skipped, not queued.

**One live job per key.** `EnqueueOptions.UniqueKey`: a second `Enqueue` with the same key does nothing while a job with that key is still live — queued, delayed, running, or waiting to retry — and goes through again once that job completed or died. The key is a column on `cb_claims` with a partial unique index, `CREATE UNIQUE INDEX ... ON cb_claims (unique_key) WHERE status = 0`. Completion deletes the claim and a dead claim leaves the index, so the key frees itself in both cases; a retry keeps the claim live, so the key stays taken. `Enqueue` inserts the message only when no live claim holds the key, and the claim insert carries `ON CONFLICT (unique_key) WHERE status = 0 DO NOTHING`, so two enqueues at the same instant both return 0 for the loser. The loser may leave a message row without a claim; `GC` removes it with the other old messages.

This is a second key next to `DedupKey`, and both are needed. `DedupKey` lives as long as the message and is for keys that must not come back: a cron key, where a process that wakes late in the same minute would otherwise start the job again after the first run finished; a trigger key, where a redone batch would otherwise create a second job for a message whose first job already ran. `UniqueKey` is for "at most one of these at a time": a purge, a sync, a rebuild, which should run again later but not twice at once. Cron jobs need both, see the cron helper.
