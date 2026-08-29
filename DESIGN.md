# Catbird Lite

A PostgreSQL-backed job queue, stream, and small workflow engine. Five tables, plain SQL, no PL/pgSQL, no extensions. All logic lives in a thin client (Go today; other languages follow the same statements), so the whole system is the schema in `migrations/00001_lite.sql` plus the statements in `client.go`, `worker.go`, and `stream.go`.

## Tables

- `cb_messages` — every job input and every stream event is one row. Job inputs are written once; published messages are updated once, when the assigner sets their `position`. Measured: one dead tuple per published message, cleaned by a routine vacuum; see the position benchmark note below.
- `cb_claims` — one narrow row per job that still has to run. Updated on every claim and retry, deleted on completion. Aggressive autovacuum settings on the table keep it small.
- `cb_cursors` — one row per stream consumer: the highest position it processed.
- `cb_signals` — payloads delivered to a job that waits for them.
- `cb_outputs` — optional job results, written by the handler with `SetOutput` in its transaction, read with `Output`.

A partial index on `cb_claims (queue, visible_at) WHERE status = 0 AND dependencies = 0` holds only claimable rows. Dead rows and rows waiting on dependencies are not in it. Measured at 100k live claims, a 50-job claim reads 3 buffers.

A second partial index, `cb_claims (correlation_id) WHERE status = 0 AND correlation_id IS NOT NULL`, is `Cancel`'s. A worker cancels the correlation group of every job that dies, so the outage that kills a whole group runs `Cancel` once per dead job; without the index each of those is a scan of the table. Measured at 100k live claims: 834 buffers per call without it, 6 with. The `IS NOT NULL` half keeps jobs that have no correlation id out of a structure that can never match them — measured with 1% of jobs correlated, 2128 kB against 112 kB.

`cb_messages (topic text_pattern_ops, position) WHERE position IS NOT NULL` serves the subtree read. It cannot produce position order across topic values on its own, so the planner reaches it as a BitmapOr of the two arms of the match — `topic = $2` and the prefix range from `topic LIKE $3` — and sorts the result. That plan exists only when the planner knows the topic: a generic plan cannot turn `LIKE $3` into a range, so it falls back to walking the position index and filtering. The difference is large and is written up under "Streams" below. The index costs 16 MB per 300k messages, 2.4× the position index, and an entry on every insert and every position assignment.

The unique indexes on `cb_messages (position)` and `cb_messages (dedup_key)` are partial as well, over the rows that have a value. This is what keeps one table for both worlds cheap: a job input has neither a position nor usually a dedup key, and a full unique index writes an entry for every NULL — measured, 1272 kB per index per 200k job inputs, probed by nothing, and a third of the insert time. The deduplicating inserts name the predicate, `ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING`, so they still match the partial index.

Job inputs and stream messages share `cb_messages` because a payload has to live in a row that is never updated. `cb_claims` is rewritten on every claim and every retry, so a payload stored there would be rewritten with it; the message row is written once. Nothing reads across the two kinds — stream reads filter on `position`, job reads go through `cb_claims` — so the shared table is a storage decision, not a shared lifecycle.

## Runtime

`catbird.New(pool, opts)` returns the process's `Runtime`. Workers, triggers and consumers are declared on it — `NewWorker(runtime, …)`, `NewTrigger(runtime, …)`, `NewConsumer(runtime, …)`, or the methods of the same names — and `Start(ctx)` runs them all: one `LISTEN` connection for every channel they need, the position assigner, and one goroutine per declared loop, until `ctx` ends and every loop has stopped. A process holds one connection for notifications however many workers and triggers it runs. That connection is opened from the pool's configuration rather than taken from the pool: it is held for the life of the process, so from the pool it would be a connection the workers never get back — and when the workers hold every connection there is, it could not be opened at all and the process would fall back to polling with a reconnect error every `ReconnectAfter`. Declaring after `Start` panics: the connection's channel set is fixed when it connects. A dropped connection is reconnected after `ReconnectAfter`; until then the loops run on their poll intervals, and after each connect every loop is woken once, because notifications sent in between are gone.

`Client` is the exception: it is a plain helper that works on any connection or transaction, so it is not created from the runtime.

## Jobs

`Enqueue` inserts the message and its claim in one statement and sends `NOTIFY` on the queue's channel. The runtime's connection listens on it and wakes the queue's workers, which also poll on an interval, so a lost notification delays a job rather than losing it.

**Enqueueing many at once.** `EnqueueBatch` takes a queue, a slice of `BatchMessage` and one `EnqueueOptions` for the whole batch, and writes the messages and their claims with one statement. Claims are made only for the messages that were written, so a message whose dedup key is taken produces no job, as with the single verb. The options are shared — one delay, one correlation id, one dependency count — because the callers that need a batch either give every job the same options (a trigger) or are creating one group of them (the children of a step). The key that has to differ per job is on the message.

It returns how many jobs it created, not their ids, for the same reason `PublishBatch` does. A caller that has to resolve these jobs' dependencies later needs their ids, so it either enqueues them one at a time or gives each a dedup key and reads the ids back with `SELECT id, dedup_key FROM cb_messages WHERE dedup_key = ANY($1)`, which answers with the existing job's id when a key was already taken.

**Running jobs.** A worker keeps up to `BatchSize` jobs running at once. It claims as many jobs as it has free slots, runs each in its own goroutine, and claims again as soon as a slot frees, so one long job does not hold up the jobs beside it.

`BatchSize` is bounded by the pool. Every running job holds a pool connection for its transaction, so `NewWorker` keeps `BatchSize` at or below `MaxConns - 1`. Above that a worker does not get more work done: the extra jobs sit in `Begin` with their leases already running, and one that waits there long enough is claimed by another worker while the first still holds a slot for it. An unset `BatchSize` takes the smaller of the default and what the pool can carry; one the caller asked for is lowered with a warning, since it was asked for. The connection kept free is for the worker's own statements: the claim, and the retry a failing job writes once its own transaction has given its connection back. `Start` sees every declared loop at once and warns when together they hold more than the pool can carry; the per-worker bound cannot catch that, because each worker only knows itself. While jobs are still waiting it gives slots 5 milliseconds to free before claiming, so a queue of short jobs is claimed by one bigger statement instead of one statement per finished job; on an empty queue nothing is delayed and the loop waits for a `NOTIFY` or for `PollInterval`.

**Claiming.** A worker takes up to as many rows as it has free slots with `FOR UPDATE SKIP LOCKED`, sets `visible_at = now() + Lease`, and increments `attempts`. There is no "running" status: a job with `visible_at` in the future is either delayed, backing off after a failure, or claimed. Once `visible_at` passes, any worker may claim it again. That is how a crashed worker's job comes back.

**Completing.** The worker opens a transaction, passes it to the handler, and in the same transaction runs `DELETE FROM cb_claims WHERE message_id = $1 AND attempts = $2`. The handler's writes and the job's completion commit together. `attempts` is the lease token: if the lease expired and another worker claimed the job, `attempts` moved on, the delete finds nothing, and the late worker rolls back. Two workers may execute the same job; only the one holding the lease commits. Side effects outside the database (emails, HTTP calls) are not covered by this, so handlers that must not repeat them need their own idempotency key — `Message.ID` works.

**Lease rule.** A handler must finish within `Lease` or its work is discarded and the job runs again. Set `Lease` above your longest handler.

**Settings rule.** `Lease`, `MaxAttempts` and `Backoff` are worker settings; nothing about them is stored with the job. All workers on one queue must use the same values, otherwise how long a job may run and how often it is retried depend on which worker took the attempt.

**Failing.** A handler error sets `visible_at = now() + Backoff`. After `MaxAttempts` the claim is marked dead (`status = 1`), `Cancel` runs for its correlation id, and `OnDead` runs once outside the job transaction. A crash counts as a failed attempt like any other, so `OnDead` also fires for jobs that repeatedly crashed a worker.

The statements that record how an attempt ended do not run on the job's context. At shutdown that context is already canceled by the time the handler returns, so every one of them would fail at once and the job would keep the lease deadline its claim set — five minutes at the defaults — instead of coming back after `Backoff`. The attempt is over either way; recording how it ended is what has to survive, so it runs on a context shutdown does not cancel, with five seconds of its own. `OnDead` runs on that context and shares its budget. It is the caller's code, so more time would be defensible — a job dies once and this is the only notification of it — but shutdown waits for the jobs in flight, and a longer budget would let one slow callback hold a stopping process. A callback with more to do than fits should write a row and do the rest from a job.

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

**What a read costs.** A fetch is cheap for a consumer that keeps up, whatever plan it gets: the first rows after the cursor are the rows it wants. The case that separates the two plans is a consumer that has fallen behind on a topic that is a small share of traffic. Measured with a topic at 0.3% of a 300k-message stream and a cursor 15k messages behind, returning 50 rows:

| plan | buffers | time |
|---|---|---|
| BitmapOr on the topic index, then sort | 60 | 0.07 ms |
| walk the position index, filter on topic | 982 | 2.2 ms |

The second walks everything published since the cursor and throws away 14950 rows, so it gets more expensive exactly while a consumer is trying to catch up. Which one the planner picks is decided by whether it knows the topic when it plans, so **`FetchBatch` must reach a custom plan**. Two things would take it off one, and neither is visible from the query: a driver that keeps a server-side prepared statement long enough for PostgreSQL to switch it to a generic plan, and the `$2 = ''` arm, which a generic plan cannot fold away either. Under pgx's default `QueryExecModeCacheStatement` the statement is prepared, and PostgreSQL keeps the custom plan only while it estimates it cheaper than the generic one. Checking this is part of changing `FetchBatch`: `EXPLAIN` it with `plan_cache_mode = force_generic_plan` and see which plan comes back.

The scale limit past that is the width of the subtree, not the plan. If one is ever reached, the ways out are a `topic_id` column with a `(topic_id, position)` index for consumers that want one exact topic, or a table of `(prefix, position)` maintained on publish.

**Publishing many at once.** `PublishBatch` takes a slice of `BatchMessage` — topic, payload, dedup key — and writes them with one `INSERT ... SELECT FROM unnest(...)`: a transaction that changed ten thousand records announces them in one round trip. The messages travel as three arrays, so the number of messages is not limited by the number of statement parameters. It returns how many rows it wrote, not their ids: a message whose dedup key is already taken, or that repeats a key from its own batch, is skipped, and `RETURNING` cannot say which ones — it reads columns of `cb_messages`, and nothing there carries the input's place in the slice. A caller that needs an id per message calls `Publish` for each of them in one transaction. The assigner gives a batch its positions like any other publish, and drains a backlog larger than one statement within the same tick, so ten thousand messages are readable one tick after they commit.

**Positions.** Message ids are handed out at `INSERT` time, so a message from a transaction that is still open can have a lower id than messages that already committed; a reader going by id would move past it and miss it. Readers therefore go by `position`, which the assigner sets on published messages in the order it sees them — commit order. A `stream` flag marks published messages; job inputs get no position, so a job created from an event is not itself an event and a trigger does not feed on its own output. A message from a long transaction gets its position when it commits; it arrives late, after messages published after it, but it arrives, once. This is the rule a plain `SELECT` follows: you see a row when its transaction commits.

The assigner is one statement under an advisory lock, run every `AssignEvery` (250 ms) by every process's `Runtime`. The lock makes one of them do the work; the rest do nothing. The statement only sets positions that are still empty, so even two assigners running at once cannot move a position a reader may already have passed.

One statement takes at most 5000 messages, so a tick keeps running statements while its last one came back full, up to 20 of them. Without that a tick assigns 5000 and then sleeps out the rest of `AssignEvery` however long the backlog is, which caps the stream at 5000 per tick — 20k messages a second at the defaults — and above that rate leaves a backlog that grows without bound, silently, taking stream latency with it. `PublishBatch` is built for batches well above one statement's worth, so the bound is reached by a single batch and not only by sustained load. A tick that still has a backlog after 20 statements logs a warning; that is the signal that publishing is outrunning the assigner. Nothing has to be deployed or configured; a message is readable within one tick of its commit. When the assigner assigned anything it sends `NOTIFY cb_stream` with the highest new position; the runtime's connection listens on that channel and wakes the triggers, so they fetch on arrival instead of polling. The cost is one update per published message. Against a variant with positions in a separate narrow table (measured, 200k × 500 B): the column writes ~45% more WAL and ~60% more heap, but publishes and reads ~65% faster, deletes 2× faster (no FK cascade), and needs one table and one index fewer. Vacuum time was under 0.3 s per 200k messages for both.

**Triggers.** `NewTrigger(runtime, name, topic, queue, opts)` declares a loop that runs from `Start`: fetch a batch, `EnqueueBatch` it on the target queue with dedup key `trigger:<name>:<message id>` per message, `Ack`, commit — all in one transaction, and the whole batch is one statement. A crash before commit redoes the batch; the dedup keys make the redo a no-op. Several processes may run the same trigger: each only wastes reads, the cursor is monotone, and the dedup keys keep the output single. Run a trigger in one process if the extra reads matter; there is no leader election.

## Cron without a leader

Every process enqueues `cron:<name>:<minute>` as the dedup key when the minute starts, with the minute in UTC as `YYYY-MM-DDTHH:MMZ` (`cron:report:2026-08-28T09:30Z`). Exactly one insert goes through; the format is fixed because every process must produce the same key. A process that wakes late (suspended, paused) should compare the scheduled minute with the clock and skip if it is too far behind.

## Retention

`GC(retention)` deletes dead claims older than `retention` and then messages older than `retention` that have no claim. A delayed or waiting job keeps its message however old it is. `cb_signals` and `cb_claims` reference `cb_messages` with `ON DELETE CASCADE`, so nothing is orphaned.

## Known limits

Things this design does not do, or does badly. The ones with a fix in mind are under "Planned additions"; these are the ones a caller has to know about now.

- **A wrong dependency count is silent and permanent.** `EnqueueOptions.Dependencies` is a number the caller supplies and `ResolveDependency` counts down. One too high and the job never runs — no error, no status, nothing that distinguishes it from a job that is legitimately waiting. One too low and it runs before its parents finish. A second `ResolveDependency` for the same dependency returns `ErrNotFound`, but a missing one cannot be detected at all: nothing knows what the count was supposed to be. Nothing validates the graph, because nothing has the graph. Until something does, an application that builds flows should watch `SELECT * FROM cb_claims WHERE dependencies > 0 AND visible_at < now() - interval '1 hour'` itself.
- **A trigger does not preserve stream order.** It reads a position-ordered batch and enqueues it onto a queue that runs `BatchSize` jobs at once, so the jobs start in order and finish in any order. A consumer that needs ordering has to handle the batch itself rather than fan it out.
- **`Consumer` is not safe in more than one process.** `FetchBatch` and `Ack` are two statements with nothing between them, so two processes on the same cursor both handle every batch. Triggers survive this because their dedup keys make a repeated batch a no-op; a general consumer has no such key. Run one, until the cursor lease lands.
- **A publish costs two writes and two tuples.** The assigner's `UPDATE` is not a HOT update — `position` is covered by three indexes — so it writes a second tuple and, measured over 50k messages of ~450 bytes, more WAL and more time than the `INSERT` it annotates: 42.8 MB and 592 ms against 33.1 MB and 385 ms. This is the price of the position column, and the alternative measured worse on reads. It is the number a capacity plan needs.
- **`status = 1` does not say why.** A job that failed permanently and a job that `Cancel` stopped are the same row. There is no `last_error`, no run history, and no dead-letter table: `OnDead` is a callback, so if the process is down or the callback returns an error the fact is logged and gone. There is nothing to re-drive from.
- **One dedup-key namespace for both worlds.** `cb_messages.dedup_key` is unique across the table, so a `Publish` key and an `Enqueue` key collide. Prefix them if both are in use.
- **A dedup key lives as long as its message.** `GC` deletes messages by age and frees their keys with them, so a key stops deduplicating once its message ages out. Fine for `cron:<name>:<minute>`; retention has to exceed the window for any key used for idempotency.
- **An output lives as long as its message.** `cb_outputs` cascades from `cb_messages`, so retention has to exceed the longest flow or a late step cannot read an early step's output.
- **A delayed `Enqueue` still wakes the queue.** The wake only checks `dependencies = 0`, not `visible_at`, so every scheduled job wakes every worker on its queue for work none of them can claim yet.
- **`Enqueue` returns `(0, nil)` when the dedup key was taken** and gives no way to get the existing id, so a caller that needs it makes a second round trip.
- **`GC` scans `cb_messages`** — no index on `created_at`, measured at 18,750 buffers over 300k rows — and the runtime does not schedule it. The application has to call it.
- **No partitioning.** `cb_messages` is one table and retention is a row-by-row `DELETE` with the index churn that implies, rather than dropping a partition.
- **No schema versioning and no migration path.** One `.sql` file with goose markers, no runner, no version table, and no route from an installation of the earlier catbird. The second schema change has to invent all of that.
- **Positions follow insert order inside a tick.** The assigner orders its batch by `id`, so two messages that commit within the same window can get positions in insert order rather than commit order. Commit order is what holds across ticks, which is what a reader depends on: a message becomes visible only when its transaction commits.
- **`hashtext` is undocumented.** The assigner's advisory lock key is `hashtext('catbird')`, an internal function with no stability guarantee across major versions. A client in another language has to produce the same number.
- Rate limits and per-queue configuration are out of scope. Applications build them on their own tables. The browser layer is planned; see below.
- Without a `Logger`, failures are reported through `slog.Default()`. Errors the library cannot return to the caller are logged.

## Planned additions

These come from moving raven, the first application, onto catbird: its own event log stays its own table, and everything that moves data — change signals, cursors, jobs, browser delivery — comes from here. Each item says what it is and why it is needed.

### Streams

**Cursor lease.** `cb_cursors` gets `locked_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity'`. A consumer claims a cursor with `UPDATE cb_cursors SET locked_until = now() + lease WHERE name = $1 AND locked_until <= now() RETURNING last_position`; when the row is already leased the claim returns nothing and the consumer waits for the next wake-up. `Ack` keeps `GREATEST` and clears the lease; acking the unchanged position releases without advancing. Reason: an application runs several processes, and a consumer that indexes documents or calls an external API would otherwise do every batch once per process. A lease with a deadline also covers a process that is alive but stuck: when the deadline passes another process takes the cursor. Triggers did not need this because their dedup keys make a repeated batch harmless; a general handler has no such key.

**`Consumer.Handle(topic, handle func(ctx, []Message) error)`.** Declares a loop that `Start` runs: claim the cursor, fetch a batch, call the handler, ack; wake on `NOTIFY cb_stream`, poll on `PollInterval` as the fallback, and keep going while batches come back full. A handler error releases the cursor without advancing, so the batch is retried. Triggers become a handler that enqueues the batch and acks in one transaction.

**`Read(ctx, pool, topics, after, limit)` and `LastPosition(ctx, db)`.** The read for a caller that holds its own position instead of a cursor: the wire, or a poll endpoint. `topics` is a list of subtrees; the query walks the position index from `after`. `LastPosition` is the current end of the stream, so a page can embed it and start its connection from there.

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

**Exponential backoff.** `Backoff` becomes the delay after the first failed attempt rather than after every one, and `WorkerOptions.MaxBackoff` caps how far it grows. The wait after attempt n is a random duration below `min(Backoff * 2^(n-1), MaxBackoff)`, computed in the retry statement itself:

```sql
UPDATE cb_claims
SET visible_at = now() + least($3::interval * 2 ^ least(attempts - 1, 20), $4::interval) * random()
WHERE message_id = $1 AND attempts = $2 AND status = 0
```

Defaults change with it: `Backoff` one second and `MaxBackoff` one minute, instead of a flat minute. Both are worker settings under the settings rule, so all workers on a queue must use the same values.

Two reasons for growing the wait. A handler that fails because a service it calls is down retries at a fixed minute for as long as `MaxAttempts` allows, so five attempts cover five minutes of outage; doubling spends the same five attempts over half an hour. And an outage fails every job on the queue at once: with a fixed delay all of them come back in the same second, hit the service that is still down, and come back together again a minute later. The random draw spreads them, which is why the delay is drawn below the cap instead of being the cap. It also means the setting names a ceiling and not an average: a `Backoff` of one second retries after half a second on average.

The exponent is clamped at 20 so that a job which somehow reaches a high attempt count does not overflow the interval multiplication before `MaxBackoff` bounds it.

A queue that wants the old fixed pacing sets `Backoff` and `MaxBackoff` to the same value; the draw still spreads the retries below it, which is the part no queue benefits from losing.

**Run status.** `Status(ctx, db, id)` returns queued, scheduled, running, dead or completed, with the attempt count: completed means the message exists and its claim is gone; a live claim with `visible_at` in the future is running or waiting to retry, which `attempts` and the worker's `MaxAttempts` tell apart. `cb_claims` gets a nullable `last_error TEXT`, written on every failed attempt, so an application can show why a run failed or is retrying.

**Cancelling a running job.** Today `Cancel` stops jobs from starting and a running job finishes. The addition: `Cancel` also sends `pg_notify('cb_queue_<queue>', 'cancel:<message id>')`, and a worker that is running that job cancels the handler's context. The handler decides what a cancelled context means — stop at the next safe point, or finish — so the database still does not interrupt a running job; it only tells the handler that a cancel arrived. Open decision: keep the weak cancel and have handlers that must stop early poll `cb_claims.status` at their own boundaries, or add the notification.

**Extending a lease.** `Message.ExtendLease(ctx)` moves the claim's `visible_at` out by another `Lease` from now, so a handler that is still working keeps its job. It runs `UPDATE cb_claims SET visible_at = now() + lease WHERE message_id = $1 AND attempts = $2` on a pool connection, not on the handler's transaction: a change made inside that transaction is invisible to other workers until it commits, and by then the lease no longer matters. `attempts` is the same lease token completion uses, so an extension that arrives after the lease expired and another worker claimed the job updates nothing and returns `ErrLeaseLost`; the handler should stop, because its transaction will not commit either. The field is a closure the worker installs on the messages it hands to a job handler; on a message from a stream read it is nil.

Call it where the handler finished a piece of work — between two records of a batch edit, after one file of an import — not from a timer. A timer renews the lease of a handler that hangs as readily as one that progresses, and no other worker ever takes that job back.

With extension, `Lease` bounds one step of a handler instead of the whole handler. A queue whose jobs run for an hour can keep a lease of minutes, so a crashed worker's job comes back in minutes; without it the lease has to cover the longest handler, and every crash on that queue costs that long.

**Job timeout.** `WorkerOptions.Timeout` bounds one attempt: the handler and its completion run on a context with that deadline, and when it passes the context is cancelled, the transaction rolls back, and the attempt counts as failed — retry after `Backoff`, dead after `MaxAttempts`. The default is `Lease`, and `ExtendLease` moves the deadline out together with the lease, so the two never disagree. A `Timeout` above `Lease` is a mistake: past the lease another worker may claim the job and the first worker's transaction cannot commit any more. Like `Lease` and `MaxAttempts` it is a worker setting, so all workers on a queue must use the same value.

Without it, a handler that waits on a socket with no deadline of its own keeps its slot and its pool connection until the process restarts. The lease brings the job back for another worker but nothing stops the first attempt, so a queue can end up with every slot held by attempts nobody is waiting for.

A cancelled context stops the handler only where the handler looks at it. Database calls do — pgx cancels the running query — but a computation that never checks `ctx.Err()` runs on and keeps its slot; the timeout ends the attempt's bookkeeping, not the goroutine.

**`Worker.RunOnce(ctx)`.** Claim one batch, run it, return. Tests run jobs deterministically without a background worker.

**Cron helper.** `RunCron(ctx, pool, name, every, queue, topic)` enqueues on the interval and once at start, so applications do not each rebuild the key format, which every client must produce identically. It sets both keys: `DedupKey` `cron:<name>:<minute>`, so several processes ticking in the same minute produce one job, and `UniqueKey` `cron:<name>`, so a run that takes longer than its interval does not pile up — while the previous run is still live the tick's enqueue does nothing and that tick is skipped, not queued.

**One live job per key.** `EnqueueOptions.UniqueKey`: a second `Enqueue` with the same key does nothing while a job with that key is still live — queued, delayed, running, or waiting to retry — and goes through again once that job completed or died. The key is a column on `cb_claims` with a partial unique index, `CREATE UNIQUE INDEX ... ON cb_claims (unique_key) WHERE status = 0`. Completion deletes the claim and a dead claim leaves the index, so the key frees itself in both cases; a retry keeps the claim live, so the key stays taken. `Enqueue` inserts the message only when no live claim holds the key, and the claim insert carries `ON CONFLICT (unique_key) WHERE status = 0 DO NOTHING`, so two enqueues at the same instant both return 0 for the loser. The loser may leave a message row without a claim; `GC` removes it with the other old messages.

This is a second key next to `DedupKey`, and both are needed. `DedupKey` lives as long as the message and is for keys that must not come back: a cron key, where a process that wakes late in the same minute would otherwise start the job again after the first run finished; a trigger key, where a redone batch would otherwise create a second job for a message whose first job already ran. `UniqueKey` is for "at most one of these at a time": a purge, a sync, a rebuild, which should run again later but not twice at once. Cron jobs need both, see the cron helper.
