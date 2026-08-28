# Catbird Lite

A PostgreSQL-backed job queue, stream, and small workflow engine. Four tables, plain SQL, no PL/pgSQL, no extensions. All logic lives in a thin client (Go today; other languages follow the same statements), so the whole system is the schema in `migrations/00001_lite.sql` plus the statements in `client.go`, `worker.go`, and `stream.go`.

## Tables

- `cb_messages` — every job input and every stream event is one row. Job inputs are written once; published messages are updated once, when the assigner sets their `position`. Measured: one dead tuple per published message, cleaned by a routine vacuum; see the position benchmark note below.
- `cb_claims` — one narrow row per job that still has to run. Updated on every claim and retry, deleted on completion. Aggressive autovacuum settings on the table keep it small.
- `cb_cursors` — one row per stream consumer: the highest position it processed.
- `cb_signals` — payloads delivered to a job that waits for them.
- `cb_outputs` — optional job results, written by the handler with `SetOutput` in its transaction, read with `Output`.

A partial index on `cb_claims (queue, visible_at) WHERE status = 0 AND dependencies = 0` holds only claimable rows. Dead rows and rows waiting on dependencies are not in it.

## Jobs

`Enqueue` inserts the message and its claim in one statement and sends `NOTIFY` on the queue's channel. Workers `LISTEN` and also poll on an interval, so a lost notification delays a job rather than losing it.

**Claiming.** A worker takes up to `BatchSize` rows with `FOR UPDATE SKIP LOCKED`, sets `visible_at = now() + Lease`, and increments `attempts`. There is no "running" status: a job with `visible_at` in the future is either delayed, backing off after a failure, or claimed. Once `visible_at` passes, any worker may claim it again. That is how a crashed worker's job comes back.

**Completing.** The worker opens a transaction, passes it to the handler, and in the same transaction runs `DELETE FROM cb_claims WHERE message_id = $1 AND attempts = $2`. The handler's writes and the job's completion commit together. `attempts` is the lease token: if the lease expired and another worker claimed the job, `attempts` moved on, the delete finds nothing, and the late worker rolls back. Two workers may execute the same job; only the one holding the lease commits. Side effects outside the database (emails, HTTP calls) are not covered by this, so handlers that must not repeat them need their own idempotency key — `Message.ID` works.

**Lease rule.** A handler must finish within `Lease` or its work is discarded and the job runs again. Set `Lease` above your longest handler.

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

`Publish` inserts a message with no claim. `StreamConsumer.FetchBatch(pattern)` reads messages after the cursor on a topic and every topic under it (`order` covers `order.paid` and `order.paid.refund`; `""` covers everything), in position order. Topic names are literal; there is no pattern syntax. Finer selection is the consumer's code, or an optional payload filter added later as an extra clause. `Ack(position)` moves the cursor; it uses `GREATEST`, so the cursor does not move backwards when two consumers share it.

**Positions.** Message ids are handed out at `INSERT` time, so a message from a transaction that is still open can have a lower id than messages that already committed; a reader going by id would move past it and miss it. Readers therefore go by `position`, which the assigner sets on published messages in the order it sees them — commit order. A `stream` flag marks published messages; job inputs get no position, so a job created from an event is not itself an event and a trigger does not feed on its own output. A message from a long transaction gets its position when it commits; it arrives late, after messages published after it, but it arrives, once. This is the rule a plain `SELECT` follows: you see a row when its transaction commits.

The assigner is one statement under an advisory lock, run every `AssignEvery` (250 ms) by every `StreamConsumer` and every trigger. The lock makes one of them do the work; the rest do nothing. Nothing has to be deployed or configured; a message is readable within one tick of its commit. When the assigner assigned anything it sends `NOTIFY cb_stream` with the highest new position, so readers that `LISTEN` fetch on arrival instead of polling. The cost is one update per published message. Against a variant with positions in a separate narrow table (measured, 200k × 500 B): the column writes ~45% more WAL and ~60% more heap, but publishes and reads ~65% faster, deletes 2× faster (no FK cascade), and needs one table and one index fewer. Vacuum time was under 0.3 s per 200k messages for both.

**Triggers.** `RegisterTrigger(name, topic, queue)` runs a loop: fetch a batch, `Enqueue` each message on the target queue with dedup key `trigger:<name>:<message id>`, `Ack`, commit — all in one transaction. A crash before commit redoes the batch; the dedup keys make the redo a no-op. Several processes may run the same trigger: each only wastes reads, the cursor is monotone, and the dedup keys keep the output single. Run a trigger in one process if the extra reads matter; there is no leader election.

## Cron without a leader

Every process enqueues `cron:<name>:<minute>` as the dedup key when the minute starts. Exactly one insert goes through. A process that wakes late (suspended, paused) should compare the scheduled minute with the clock and skip if it is too far behind.

## Retention

`GC(retention)` deletes dead claims older than `retention` and then messages older than `retention` that have no claim. A delayed or waiting job keeps its message however old it is. `cb_signals` and `cb_claims` reference `cb_messages` with `ON DELETE CASCADE`, so nothing is orphaned.

## Known limits

- A worker processes one batch to completion before claiming the next, so one slow job holds up the other jobs of its batch.
- Rate limits, per-queue configuration, and a web/SSE layer are out of scope. Applications build them on their own tables and routes.
- Without a `Logger`, failures are reported through `slog.Default()`. Errors the library cannot return to the caller are logged.
