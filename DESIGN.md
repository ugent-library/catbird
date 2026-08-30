# Catbird Lite

A PostgreSQL-backed job queue, stream, and small workflow engine. Five tables, plain SQL, no PL/pgSQL, no extensions. All logic lives in the client statements (Go today; other languages follow the same statements), so the whole system is the schema in `migrations/00001_lite.sql` plus the statements in `client.go`, `worker.go` and `trigger.go`.

## Tables

- `cb_messages` — every job's message and every published message is one row. A job's message is written once; a published message is updated once, when the assigner sets its `position`. Measured: one dead tuple per published message, cleaned by a routine vacuum; see the position benchmark note below.
- `cb_claims` — one narrow row per job that still has to run. Updated on every claim and retry, deleted on completion. Aggressive autovacuum settings on the table keep it small.
- `cb_cursors` — one row per stream consumer: the highest position it processed.
- `cb_signals` — payloads delivered to a job that waits for them.
- `cb_outputs` — optional job results, written by the handler with `SetOutput` in its transaction, read with `Output`.

A partial index on `cb_claims (queue, visible_at) WHERE status = 0 AND dependencies = 0` holds only claimable rows. Dead rows and rows waiting on dependencies are not in it.

A second partial index, `cb_claims (correlation_id) WHERE status = 0 AND correlation_id IS NOT NULL`, is what `Cancel` probes. A worker cancels the correlation group of every job that dies with one, so a downstream outage that fails a whole group calls it once per dead job: 6 buffers each with the index, 834 and 4.9 ms without it at 100k live claims. Jobs with no correlation id stay out of it — 112 kB against 2128 kB with 1% of jobs correlated.

The unique indexes on `cb_messages (position)` and `cb_messages (dedup_key)` are partial as well, over the rows that have a value. This is what keeps one table for both worlds cheap: a job's message has neither a position nor usually a dedup key, and a full unique index writes an entry for every NULL — measured, 1272 kB per index per 200k job messages, probed by nothing, and a third of the insert time. The deduplicating inserts name the predicate, `ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING`, so they still match the partial index.

Job messages and published messages share `cb_messages` because a payload has to live in a row that is never updated. `cb_claims` is rewritten on every claim and every retry, so a payload stored there would be rewritten with it; the message row is written once. Nothing reads across the two kinds — stream reads filter on `position`, job reads go through `cb_claims` — so the shared table is a storage decision, not a shared lifecycle.

## Runtime

`catbird.New(pool, opts)` returns the process's `Runtime`. Workers and triggers are declared on it — `NewWorker(runtime, …)`, `NewTrigger(runtime, …)`, or the methods of the same names — and `Start(ctx)` runs them all: one `LISTEN` connection for every channel they need, the position assigner, and one goroutine per declared loop, until `ctx` ends and every loop has stopped. A process holds one connection for notifications however many workers and triggers it runs, and a running job holds none. Declaring after `Start` panics: the connection's channel set is fixed when it connects. A dropped connection is reconnected after `ReconnectAfter`; until then the loops run on their poll intervals, and after each connect every loop is woken once, because notifications sent in between are gone.

The statements a caller runs — `Publish`, `Enqueue`, `Complete`, `Cancel`, the stream reads and the rest — are package functions that take any connection or transaction, so they need no runtime and hold no state of their own. What a process configures lives on the `Runtime`.

## Jobs

`Enqueue` inserts the message and its claim in one statement and sends `NOTIFY` on the queue's channel. The runtime's connection listens on it and wakes the queue's workers, which also poll on an interval, so a lost notification delays a job rather than losing it.

**Enqueueing many at once.** `EnqueueBatch` takes a queue, a slice of `BatchMessage` and one `EnqueueOptions` for the whole batch, and writes the messages and their claims with one statement. Claims are made only for the messages that were written, so a message whose dedup key is taken produces no job, as with the single verb. The options are shared — one delay, one correlation id, one dependency count — because the callers that need a batch either give every job the same options (a trigger) or are creating one group of them (the children of a step). The key that has to differ per job is on the message.

It returns how many jobs it created, not their ids, for the same reason `PublishBatch` does. A caller that has to resolve these jobs' dependencies later needs their ids, so it either enqueues them one at a time or gives each a dedup key and reads the ids back with `SELECT id, dedup_key FROM cb_messages WHERE dedup_key = ANY($1)`, which answers with the existing job's id when a key was already taken.

**Running jobs.** A worker keeps up to `BatchSize` jobs running at once. It claims as many jobs as it has free slots, runs each in its own goroutine, and claims again as soon as a slot frees, so one long job does not hold up the jobs beside it. While jobs are still waiting it gives slots 5 milliseconds to free before claiming, so a queue of short jobs is claimed by one bigger statement instead of one statement per finished job; on an empty queue nothing is delayed and the loop waits for a `NOTIFY` or for `PollInterval`.

**Claiming.** A worker takes up to as many rows as it has free slots with `FOR UPDATE SKIP LOCKED`, sets `visible_at = now() + Lease`, and increments `attempts`. There is no "running" status: a job with `visible_at` in the future is either delayed, backing off after a failure, or claimed. Once `visible_at` passes, any worker may claim it again. That is how a crashed worker's job comes back.

**Completing.** A handler is given no connection and the worker holds none while it runs. Completion is one statement — the claim and the job's delivered signals deleted together — and `Complete` runs it on any connection or transaction:

```go
func(ctx context.Context, job *catbird.Job) error {
	tx, err := pool.Begin(ctx)
	if err != nil { return err }
	defer tx.Rollback(ctx)
	// the handler's own statements, wherever it wants them
	if err := catbird.Complete(ctx, tx, job); err != nil { return err }
	return tx.Commit(ctx)
}
```

A handler that calls it inside its own transaction ends the job in the same commit as its writes. A handler that returns `nil` without calling it is completed by the worker afterwards, on the pool. A handler that calls it and then rolls the transaction back has told the worker the job is finished when it is not: the mark it leaves records the statement, not the commit, so nothing deletes the claim until the lease runs out and the job runs again. `attempts` is the lease token in both cases: if the lease expired and another worker claimed the job, `attempts` moved on, nothing is deleted, and `Complete` returns `ErrLeaseExpired` so the late attempt rolls back. Two workers may execute the same job; only the one holding the lease commits.

**At-least-once is the model.** A job runs again after a crash, after a lease expires, and after any attempt that did not reach its completion. A handler therefore either makes its writes idempotent — `Job.ID` is the key to do it with — or completes in the same transaction as them, which is the only way an accumulating write like `balance = balance - 100` can be right. Effects outside the database were never covered by either and need the same idempotency key.

**What a running job costs.** Nothing on the pool: no transaction and no connection are held while a handler runs, so `BatchSize` is a number of goroutines and not a number of connections. The library's own statements need a handful of pool connections — the assigner, the claims and completions of its workers, a trigger's batch — and the `LISTEN` connection is not one of them: it is hijacked out of the pool, so a process opens `MaxConns + 1` connections and the pool keeps its full width. What the handlers hold comes on top of that, and it is what the pool has to be sized for; see Known limits.

**Lease rule.** A handler must finish within `Lease` or its work is discarded and the job runs again. Set `Lease` above your longest handler.

**Settings rule.** `Lease`, `MaxAttempts` and `Backoff` are worker settings; nothing about them is stored with the job. All workers on one queue must use the same values, otherwise how long a job may run and how often it is retried depend on which worker took the attempt.

**Failing.** A handler error sets `visible_at = now() + Backoff`. After `MaxAttempts` the claim is marked dead (`status = 1`), `Cancel` runs for its correlation id, and `OnDead` runs once. A crash counts as a failed attempt like any other, so `OnDead` also fires for jobs that repeatedly crashed a worker. A handler that completed the job and then returns an error is not retried: the retry carries the `attempts = $2` token and the claim it would correct is already gone.

Everything after the handler — the completion, the retry, the give-back — runs on a context detached from the worker's, with a few seconds of its own. At shutdown the worker's context is already canceled by the time the handler returns and pgx would reject those statements locally: the completion of work that is already done would be lost and the job would run a second time.

**Shutdown rule.** A job stopped by shutdown is not a failed job, and neither is a handler error that arrives once the worker's context is canceled: the two cannot be told apart, giving the attempt back is the safer mistake, and the error is logged. It is handed back: `attempts` is given back and `visible_at` is set to `now()`, so the next process claims it at once, and neither `MaxAttempts`, nor the cancel cascade, nor `OnDead` runs. Without this, three rolling deploys spend three of five attempts and 15 minutes of lease on a job that never ran wrong. The write carries the same `attempts = $2` lease token as the completion, so a job whose lease had already expired and been claimed elsewhere is left alone. A crash still costs an attempt, because a crashed worker writes nothing: the increment at claim time is the only thing that counts an attempt nobody saw end, and without it a job that kills its worker would be retried forever.

**Cancel rule.** `Cancel(correlationID)` marks live claims dead. It stops jobs from starting; a job that is already running finishes and completes. Cancel does not undo anything.

## Dependencies and signals

`EnqueueOptions.Dependencies = n` creates a job that stays out of the ready index until `n` events arrive. Two kinds of event count:

- `ResolveDependency(childID)` — a parent step completed. Call it inside the parent's handler transaction so it commits with the parent's completion.
- `DeliverSignal(childID, name, payload)` — an external input arrived. The payload is stored in `cb_signals` and handed to the handler as `Job.Signals[name]` at claim time. Delivering the same name twice is a no-op.

The decrement is `UPDATE ... SET dependencies = dependencies - 1 WHERE dependencies > 0`, so concurrent parents do not lose an update. When the counter reaches 0 the statement also sends `NOTIFY`.

**Signal rule.** A signal must be counted in `Dependencies` before it is delivered. Delivering to a job that is not waiting returns `ErrNotFound`; nothing is stored. Signals that arrive before the job exists are the caller's problem to retry.

A permanently failed step cancels its siblings and children through the shared correlation id. Children then stay dead with `dependencies > 0` until `GC` removes them.

## Streams

`Publish` inserts a message with no claim. Two functions read what it wrote, and both are pure reads on any connection or transaction:

- `ReadAfter(ctx, db, patterns, after, limit)` — for a caller that holds its own position: a poll endpoint, or a connection pushing to a browser.
- `Cursor{Name, Patterns}.Read(ctx, db, limit)` — for a caller that lets the database remember, with `Ack(position)` moving the cursor. `Ack` uses `GREATEST`, so a batch acked out of order cannot undo a later one's progress. `Read` is a cursor lookup and a `ReadAfter`, so the predicate below exists once.

The patterns and the cursor name sit in one value because a position only says how far a reader has come through the messages its own patterns select. Two readers sharing a name with different patterns skip each other's messages: the one reading less acks past what the other has not seen. The struct gives both calls one construction site; what it cannot catch is the same name built twice in different files, which stays covered by one reader per cursor, below.

**Patterns.** Three forms, AMQP's and the earlier design's: a topic on its own matches that topic exactly; a prefix followed by `.#` matches the prefix and everything under it, so `order.#` covers `order`, `order.paid` and `order.paid.refund` but not `orders`; `#` matches the stream. Anything else is `ErrBadPattern`. There is no `*`, and its absence is deliberate rather than unfinished: a wildcard inside a topic is not a prefix range, so it can use no index at any position, and a pattern holding one would quietly run as the slowest read in the system. Finer selection than a subtree is the reader's own code, or a payload condition added later as an extra clause.

The exact form is not just convenience. It is the only shape whose plan needs no sort — `(topic text_pattern_ops, position)` yields position order for one topic value and the `LIMIT` stops the scan — which is why the grammar makes it expressible instead of folding it into the subtree.

`LastPosition` is the end of the stream, so a page embeds it when it renders and a reader that connects afterwards misses nothing in between. `OldestPosition` is the low end: `GC` deletes messages a reader may still be behind, and the rows that survive carry no sign of the ones that did not, so a reader whose position is below `OldestPosition` knows to refetch its state instead of trusting what it got. That case matters most where readers poll — a backgrounded tab is away for as long as a laptop is shut.

**What a read costs.** A fetch is cheap for a consumer that keeps up whatever plan it gets: the first rows after the cursor are the rows it wants. The case that separates the plans is a consumer that has fallen behind on a topic that is a small share of traffic. Measured with a topic at 0.3% of a 300k-message stream and a cursor 15k messages behind, returning 50 rows:

| plan | buffers | time |
|---|---|---|
| BitmapOr on `cb_messages_topic_position_idx`, then sort | 60 | 0.07 ms |
| walk `cb_messages_position_idx`, filter on topic | 982 | 2.2 ms |

`(topic text_pattern_ops, position)` cannot produce position order across topic values on its own, but the planner does not need it to: it reads the two arms of the match — `topic = $2`, and the prefix range from `topic LIKE $3` — as a BitmapOr and sorts the result. The second plan walks everything published since the cursor and throws away 14950 rows, so it gets more expensive exactly while a consumer is trying to catch up.

Which one the planner picks is decided by whether it knows the topic when it plans, so **a one-subtree read has to reach a custom plan**. Two things would take it off one, and neither is visible in the query: a server-side prepared statement that PostgreSQL keeps long enough to switch to a generic plan, which cannot fold `LIKE $3` into a range; and a generic plan cannot fold the arms away either. Under pgx's default `QueryExecModeCacheStatement` the statement is prepared, and PostgreSQL keeps the custom plan only while it estimates it cheaper than the generic one. Checking this is part of changing the read: `EXPLAIN` it under `plan_cache_mode = force_generic_plan` and see which plan comes back. Measured on PostgreSQL 18; the rest of the numbers here are from 16.

**This is a property of one subtree, not of the read in general.** The bitmap plan has to sort to produce position order, so it cannot stop at the `LIMIT`; the position walk is already ordered and can. Add a second subtree, or move the cursor far enough back, and the planner takes the walk in every form of the query, custom plan included — three subtrees read from position 0 walked in all of them. Plan caching is one way to lose the bitmap plan and not the only one, and no amount of custom planning gets it back. What bounds the walk is retention: it can never scan past what `GC` has kept, which makes the `GC` window the lever on the cost of a read that has fallen behind.

**Each pattern is its own comparison, never an array.** A list compared with `= ANY` or `LIKE ANY` cannot be read as index arms — 188 buffer hits against 72 for three subtrees, on the same rows — so the compiler emits one arm per pattern and the statement text varies with how many there are.

The scale limit past that is the width of the subtree, not the plan. If one is ever reached, the ways out are a `topic_id` column with a `(topic_id, position)` index for consumers that want one exact topic, or a table of `(prefix, position)` maintained on publish.

**Publishing many at once.** `PublishBatch` takes a slice of `BatchMessage` — topic, payload, dedup key — and writes them with one `INSERT ... SELECT FROM unnest(...)`: a transaction that changed ten thousand records announces them in one round trip. The messages travel as three arrays, so the number of messages is not limited by the number of statement parameters. It returns how many rows it wrote, not their ids: a message whose dedup key is already taken, or that repeats a key from its own batch, is skipped, and `RETURNING` cannot say which ones — it reads columns of `cb_messages`, and nothing there carries the input's place in the slice. A caller that needs an id per message calls `Publish` for each of them in one transaction. The assigner gives a batch its positions like any other publish, and it drains what is waiting rather than one statement's worth, so ten thousand messages are readable one tick after the commit like any other publish.

**Positions.** Message ids are handed out at `INSERT` time, so a message from a transaction that is still open can have a lower id than messages that already committed; a reader going by id would move past it and miss it. Readers therefore go by `position`, which the assigner sets on published messages in the order it sees them — commit order. A `stream` flag marks published messages; a job's message gets no position, so a job created from a published message is not itself published and a trigger does not feed on its own output. A message from a long transaction gets its position when it commits; it arrives late, after messages published after it, but it arrives, once. This is the rule a plain `SELECT` follows: you see a row when its transaction commits.

The assigner is one statement under an advisory lock, run every `AssignEvery` (250 ms) by every process's `Runtime`. A tick assigns 5000 positions per statement and runs the statement again while its batch came back full, up to 20 rounds; a tick that used all 20 and was still behind logs a warning, because from there the backlog grows faster than the assigner drains it. The lock makes one of them do the work; the rest do nothing. The statement only sets positions that are still empty, so even two assigners running at once cannot move a position a reader may already have passed. Nothing has to be deployed or configured; a message is readable within one tick of its commit. When the assigner assigned anything it sends `NOTIFY cb_stream` with the highest new position; the runtime's connection listens on that channel and wakes the triggers, so they fetch on arrival instead of polling. The cost is one update per published message. Against a variant with positions in a separate narrow table (measured, 200k × 500 B): the column writes ~45% more WAL and ~60% more heap, but publishes and reads ~65% faster, deletes 2× faster (no FK cascade), and needs one table and one index fewer. Vacuum time was under 0.3 s per 200k messages for both.

**Triggers.** `NewTrigger(runtime, name, topic, queue, opts)` declares a loop that runs from `Start`: fetch a batch, `EnqueueBatch` it on the target queue with dedup key `trigger:<name>:<message id>` per message, `Ack`, commit — all in one transaction, and the whole batch is one statement. A crash before commit redoes the batch; the dedup keys make the redo a no-op. Several processes may run the same trigger: each only wastes reads, the cursor is monotone, and the dedup keys keep the output single. Run a trigger in one process if the extra reads matter; there is no leader election.

## Cron without a leader

Every process enqueues `cron:<name>:<minute>` as the dedup key when the minute starts, with the minute in UTC as `YYYY-MM-DDTHH:MMZ` (`cron:report:2026-08-28T09:30Z`). Exactly one insert goes through; the format is fixed because every process must produce the same key. A process that wakes late (suspended, paused) should compare the scheduled minute with the clock and skip if it is too far behind.

## Retention

`GC(retention)` deletes dead claims older than `retention` and then messages older than `retention` that have no claim. A delayed or waiting job keeps its message however old it is. `cb_signals` and `cb_claims` reference `cb_messages` with `ON DELETE CASCADE`, so nothing is orphaned.

## Bugs

Wrong today, each with a fix that is known and small.

**The failure path is barely tested.** One test covers shutdown handing a job back. Nothing in the suite fails a handler: no test for a handler error scheduling a retry, for `Backoff` timing, for `MaxAttempts` marking a job dead, for `OnDead` firing, for the cancel cascade `failed` triggers, or for `listen` reconnecting. `Cancel` appears once, as setup for a GC test. This is the least exercised part of the system.

## Known limits

Not bugs — what this design does not do, or does at a price. A caller has to know about these now.

**A wrong dependency count is silent and permanent.** `EnqueueOptions.Dependencies` is a number the caller supplies and `ResolveDependency` counts down. One too high and the job never runs: no error, no status, and nothing that distinguishes it from a job that is legitimately waiting. One too low and it runs before its parents finish. A second `ResolveDependency` for the same dependency returns `ErrNotFound`, but a missing one cannot be detected at all, because nothing knows what the count was supposed to be. Nothing validates the graph because nothing has the graph — the flow DSL this replaced validated it at construction. This is the most serious of these. Until something checks it, an application that builds flows should watch `SELECT * FROM cb_claims WHERE dependencies > 0 AND visible_at < now() - interval '1 hour'` itself.

**A handler that opens a connection has to fit in the pool.** The worker holds none while a handler runs, so `BatchSize` is a number of goroutines — but a handler that opens a transaction for its whole body holds a connection for that whole body, and the two defaults do not fit each other: `BatchSize` is 50 and pgxpool's `MaxConns` is `max(4, NumCPU)`. Fifty handlers then queue in `Begin` against eight connections with their leases already running, and one that waits there longer than `Lease` has its job claimed by a second worker while it still holds a slot for it. Nothing deadlocks, because the worker holds no connection while it waits for one, but the queue runs at the pool's width instead of `BatchSize`'s. Size `MaxConns` for the handlers, or set `BatchSize` to what the pool can carry.

**The shutdown give-back makes a lease token usable again.** Every other write raises `attempts`, so a token that has moved on never matches a second time; the give-back lowers it by one. A handler that overran its lease can therefore find its token live again: worker A claims at attempt 4 and overruns, B claims at 5, B is stopped by a shutdown and hands the job back to 4, and A's statements match the row once more. A late completion then deletes a claim A no longer holds, and a late failure can schedule the retry, mark the job dead, and run the cancel cascade and `OnDead` for an attempt the give-back said not to count. The window closes as soon as any worker claims the job, which is immediately, because the give-back sets `visible_at` to now. The lease rule is what keeps this out of reach: a handler that finishes inside `Lease` never carries a stale token. Closing it in the schema needs a claim counter that only rises, which is a column.

**A trigger does not preserve stream order.** It reads a position-ordered batch and enqueues it onto a queue that runs `BatchSize` jobs at once, so the jobs start in order and finish in any order. A consumer that needs ordering handles the batch itself instead of fanning it out.

**A cursor is not safe in more than one process.** `Read` and `Ack` are two statements with nothing between them, so two processes on the same cursor both handle every batch, and if their patterns differ they also ack past each other's messages. Triggers survive the first of these because their dedup keys make a repeated batch a no-op; a general reader has no such key. Run one per cursor. The cursor lease that would fix it is not being built: work that has to happen once per message across processes is a trigger and a worker, which have leases and retries already.

**A publish costs two writes and two tuples.** The assigner's `UPDATE` is never a HOT update — `position` is covered by three indexes — so it writes a second tuple and, measured over 50k messages of ~450 bytes, more WAL and more time than the `INSERT` it annotates: 42.8 MB and 592 ms against 33.1 MB and 385 ms. This is the price of the position column, and the alternative measured worse on reads. It is the number a capacity plan needs.

**`status = 1` does not say why.** A job that failed permanently and a job that `Cancel` stopped are the same row. There is no run history and no dead-letter table, and `OnDead` is a callback with nothing durable behind it: if the process is down or the callback returns an error, the fact is logged and gone. There is nothing to re-drive from. "Run status" below adds `last_error`; it does not add the distinction or the dead letter.

**One dedup-key namespace for both worlds.** `cb_messages.dedup_key` is unique across the table, so a `Publish` key and an `Enqueue` key collide. Prefix them if both are in use.

**A dedup key lives as long as its message, and so does an output.** `GC` deletes messages by age and frees their keys with them, so a key stops deduplicating once its message ages out — fine for `cron:<name>:<minute>`, but retention has to exceed the window for any key used for idempotency. `cb_outputs` cascades from `cb_messages` the same way, so retention also has to exceed the longest flow or a late step cannot read an early step's output.

**A delayed `Enqueue` still wakes the queue.** The wake checks only `dependencies = 0`, not `visible_at`, so every scheduled job wakes every worker on its queue for work none of them can claim yet.

**`Enqueue` returns `(0, nil)` when the dedup key was taken** and gives no way to get the existing id, so a caller that needs it makes a second round trip.

**`GC` scans `cb_messages`** — no index on `created_at`, measured at 18,750 buffers over 300k rows — and the runtime does not schedule it. The application has to call it.

**No partitioning.** `cb_messages` is one table for the whole database, and retention is a row-by-row `DELETE` with the index churn that implies rather than dropping a partition.

**No schema versioning and no migration path.** One `.sql` file with goose markers, no runner, no version table, and no route from an installation of the earlier catbird. The second schema change has to invent all of it.

**Positions follow insert order inside a tick.** The assigner orders its batch by `id`, so two messages that commit within one window can get positions in insert order rather than commit order. Commit order is what holds across ticks, which is what a reader depends on.

**`hashtext` is undocumented.** The assigner's advisory lock key is `hashtext('catbird')`, an internal function with no stability guarantee across major versions. A client in another language has to produce the same number.

**The assigner's statement depends on a referenced CTE running.** `pg_notify` fires from a CTE that the final `SELECT` has to reference, which is planner behaviour rather than anything the SQL states. A client in another language has to reproduce it exactly, and "the schema plus `client.go` is the whole contract" rests on it.

- Rate limits and per-queue configuration are out of scope. Applications build them on their own tables. The browser layer is planned; see below.
- Without a `Logger`, failures are reported through `slog.Default()`. Errors the library cannot return to the caller are logged.

## What to build, in order

The rule for everything below: a change leaves the core easier to read than it found it, or leaves it alone. Moderate growth is fine. A second version of a concept the reader already holds is not, however few lines it takes — that is what makes a small system stop being one. The core is about twelve hundred lines across four files, and that is the property worth protecting.

1. **The bugs above**, in the order they are listed there. Both are wrong behaviour rather than missing behaviour, and the failure path around them is barely tested.
2. ~~**`Read` and `LastPosition`.**~~ Built, as `ReadAfter`, `Cursor.Read`, `Ack`, `LastPosition` and `OldestPosition`; see "Streams" above.
3. **`Timeout`.** A `context.WithTimeout` around the handler in `run`. It bounds the attempt's bookkeeping, not the goroutine — a handler that never checks `ctx.Err()` keeps its slot either way — so it is a partial fix, worth its three lines.
4. **`RunOnce` and `Status`.** Both leaves. `RunOnce` is what makes a job test deterministic instead of a background worker and a sleep. `Status` is built without `last_error`; see below.
5. **Exponential backoff.** One dense line in `failed`, replacing a real failure: an outage fails every job on a queue at once and a fixed delay brings all of them back in the same second, at a service that is still down. The comment on it states that failure, not the arithmetic.
6. **The cron helper**, on `DedupKey` alone.
7. **`ExtendLease`**, as a function rather than a field on `Job`.
8. **Wire**, in a file of its own. It calls `ReadAfter` and touches nothing else, so however long it gets, the four core files do not get harder to read.

Four items below are not being built: the **cursor lease**, a **declared stream loop**, **`UniqueKey`** and **cancelling a running job**. Each stays in this document because the problem it names is real; what changed is the answer. Each carries its ruling in place, with what to watch for that would bring it back. One more is undecided rather than ruled: the **`last_error` column** on `cb_claims`, which costs nothing to read and widens the row this design keeps narrow.

## Planned additions

These come from moving raven, the first application, onto catbird: its own event log stays its own table, and everything that moves data — change signals, cursors, jobs, browser delivery — comes from here. Each item says what it is and why it is needed. Read them with the rulings above and the ones in place below.

### Streams

**Cursor lease.** `cb_cursors` gets `locked_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity'`. A consumer claims a cursor with `UPDATE cb_cursors SET locked_until = now() + lease WHERE name = $1 AND locked_until <= now() RETURNING last_position`; when the row is already leased the claim returns nothing and the consumer waits for the next wake-up. `Ack` keeps `GREATEST` and clears the lease; acking the unchanged position releases without advancing. Reason: an application runs several processes, and a consumer that indexes documents or calls an external API would otherwise do every batch once per process. A lease with a deadline also covers a process that is alive but stuck: when the deadline passes another process takes the cursor. Triggers did not need this because their dedup keys make a repeated batch harmless; a general handler has no such key.

**Not building this.** A trigger plus a worker already handles each message once across processes, with leases, retries, `MaxAttempts` and exactly-once completion that all exist and that a reader of this system already holds. A cursor lease would be a second kind of lease beside the job lease, claim-then-do in both stream loops, and a new failure mode when the process holding a cursor dies. What trigger-plus-worker gives up is ordered handling: a worker runs `BatchSize` jobs at once, so the messages start in order and finish in any order. Neither reason above — indexing documents, calling an external API — needs ordering. Build this if something does.

**A declared stream loop, `NewStreamHandler(runtime, cursor, handle func(ctx, []Message) error)`.** Declares a loop that `Start` runs: claim the cursor, fetch a batch, call the handler, ack; wake on `NOTIFY cb_stream`, poll on `PollInterval` as the fallback, and keep going while batches come back full. A handler error releases the cursor without advancing, so the batch is retried. Triggers become a handler that enqueues the batch and acks in one transaction.

**Not building this.** It needs the cursor lease, which is not being built, and it adds a fourth kind of declared loop for work a trigger and a worker already do. Same condition to revisit: something that needs the batch handled in order.

**`ReadAfter`, `LastPosition` and `OldestPosition`.** Built; see "Streams" above. What was planned as `Read` is `ReadAfter`, and the cursor read sits on it, so the subtree predicate exists once. Two things came out different from the plan. The pattern list is a grammar rather than a list of bare subtrees, which makes an exact topic — the only shape whose plan needs no sort — expressible for the first time. And the retention gap is reported by `OldestPosition` rather than beside the rows: a read that returns nothing is exactly the case a caller has to distinguish, and a value returned with the rows says nothing when there are none.

### Wire

The browser layer: stream messages pushed to browsers over SSE. One type, created from the runtime, no tables of its own, no token machinery. **It goes in a file of its own**: it calls `Read` and touches nothing else, so its length costs the core nothing.

```go
type WireOptions struct {
    BatchSize    int           // messages read per round; default 50
    PollInterval time.Duration // read anyway and send an SSE comment so proxies keep the connection open; default 15 s
    Logger       *slog.Logger
}

type ServeOptions struct {
    Patterns []string // what this connection may read, in the stream's pattern grammar
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

**One goroutine per connection.** After every wake-up from the runtime's connection — the assigner's `NOTIFY cb_stream` — the SSE connection runs `ReadAfter(patterns, after, BatchSize)` and writes each row as a frame. The database does the topic matching, per connection. A slow browser slows only its own goroutine and catches up by position; there is no queue between the listener and the connections, so nothing is dropped and no slow-consumer policy is needed. Every `PollInterval` the connection reads anyway and sends `: ping`, which keeps a proxy from closing an idle stream.

**One read per connection is the cost to watch.** Every connection running its own read means connections times assigner ticks queries a second, each of them re-deciding the plan above. The way out, if it is ever reached, is one reader per process running `ReadAfter` with `#` — no topic predicate, the cheapest read there is — and each connection matching in Go: a message reaches a connection when one of its topic's prefixes is in that connection's pattern set, which is a map lookup on the few prefixes a topic has. That is the earlier design's matching, which expanded a message's topic into its covering patterns and probed for them rather than scanning rows for a pattern; here it costs no table, because the process doing the matching is already there. It gives up what per-connection reads give for free — a connection that falls behind needs its own catch-up read instead of just being slow — so it is worth building only against a measurement.

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
- Poll transport: `ReadAfter(patterns, after)` behind a GET is the whole thing, which is where this starts.
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

**`Status` yes, `last_error` undecided.** `Status` reads columns that already exist, so it is a leaf query and costs nothing. The column is two lines in `failed` and nothing harder to read, but it puts error text in the one row that is rewritten on every claim and every retry, which is the row this design keeps narrow on purpose. Decide it on that, and if the answer is no, decide where a failed job's error text lives instead.

**Cancelling a running job.** Today `Cancel` stops jobs from starting and a running job finishes. The addition: `Cancel` also sends `pg_notify('cb_queue_<queue>', 'cancel:<job id>')`, and a worker that is running that job cancels the handler's context. The handler decides what a cancelled context means — stop at the next safe point, or finish — so the database still does not interrupt a running job; it only tells the handler that a cancel arrived. Open decision: keep the weak cancel and have handlers that must stop early poll `cb_claims.status` at their own boundaries, or add the notification.

**Not building this; the weak cancel is the rule.** The notification needs the worker to keep a map of running job id to cancel function, maintained on claim and on completion and read from the listen loop. It is the only addition on this list that puts mutable state in the middle of the claim loop. A handler that must stop early polls `cb_claims.status` at its own boundaries. Build it if a handler appears that cannot poll — one blocked in a call it does not control.

**Extending a lease.** `ExtendLease` moves the claim's `visible_at` out by another `Lease` from now, so a handler that is still working keeps its job. It runs `UPDATE cb_claims SET visible_at = now() + lease WHERE message_id = $1 AND attempts = $2` on a pool connection, not on the handler's transaction: a change made inside that transaction is invisible to other workers until it commits, and by then the lease no longer matters. `attempts` is the same lease token completion uses, so an extension that arrives after the lease expired and another worker claimed the job updates nothing and returns `ErrLeaseLost`; the handler should stop, because its transaction will not commit either. **Build it as a function, not a field.** `catbird.ExtendLease(ctx, db, job, lease)`, or a method on `Worker`. The other shape is a field on `Job` — a closure the worker installs on the jobs it hands to a handler. That closure carries a connection, and a handler is given none: it opens its own and decides how long to hold it, which is what keeps a running job off the pool. The statement and the lease-token check are the same either way.

Call it where the handler finished a piece of work — between two records of a batch edit, after one file of an import — not from a timer. A timer renews the lease of a handler that hangs as readily as one that progresses, and no other worker ever takes that job back.

With extension, `Lease` bounds one step of a handler instead of the whole handler. A queue whose jobs run for an hour can keep a lease of minutes, so a crashed worker's job comes back in minutes; without it the lease has to cover the longest handler, and every crash on that queue costs that long.

**Job timeout.** `WorkerOptions.Timeout` bounds one attempt: the handler and its completion run on a context with that deadline, and when it passes the context is cancelled, the transaction rolls back, and the attempt counts as failed — retry after `Backoff`, dead after `MaxAttempts`. The default is `Lease`, and `ExtendLease` moves the deadline out together with the lease, so the two never disagree. A `Timeout` above `Lease` is a mistake: past the lease another worker may claim the job and the first worker's transaction cannot commit any more. Like `Lease` and `MaxAttempts` it is a worker setting, so all workers on a queue must use the same value.

Without it, a handler that waits on a socket with no deadline of its own keeps its slot and its pool connection until the process restarts. The lease brings the job back for another worker but nothing stops the first attempt, so a queue can end up with every slot held by attempts nobody is waiting for.

A cancelled context stops the handler only where the handler looks at it. Database calls do — pgx cancels the running query — but a computation that never checks `ctx.Err()` runs on and keeps its slot; the timeout ends the attempt's bookkeeping, not the goroutine.

**`Worker.RunOnce(ctx)`.** Claim one batch, run it, return. Tests run jobs deterministically without a background worker.

**Cron helper.** `RunCron(ctx, pool, name, every, queue, topic)` enqueues on the interval and once at start, so applications do not each rebuild the key format, which every client must produce identically. It sets both keys: `DedupKey` `cron:<name>:<minute>`, so several processes ticking in the same minute produce one job, and `UniqueKey` `cron:<name>`, so a run that takes longer than its interval does not pile up — while the previous run is still live the tick's enqueue does nothing and that tick is skipped, not queued.

**Build it on `DedupKey` alone.** `UniqueKey` is not being built, so a cron handler that can overrun its interval opens with `pg_try_advisory_lock` on its own name and returns when it does not get it. That is the same skip, decided by the handler rather than by the enqueue.

**One live job per key.** `EnqueueOptions.UniqueKey`: a second `Enqueue` with the same key does nothing while a job with that key is still live — queued, delayed, running, or waiting to retry — and goes through again once that job completed or died. The key is a column on `cb_claims` with a partial unique index, `CREATE UNIQUE INDEX ... ON cb_claims (unique_key) WHERE status = 0`. Completion deletes the claim and a dead claim leaves the index, so the key frees itself in both cases; a retry keeps the claim live, so the key stays taken. `Enqueue` inserts the message only when no live claim holds the key, and the claim insert carries `ON CONFLICT (unique_key) WHERE status = 0 DO NOTHING`, so two enqueues at the same instant both return 0 for the loser. The loser may leave a message row without a claim; `GC` removes it with the other old messages.

This is a second key next to `DedupKey`, and both are needed. `DedupKey` lives as long as the message and is for keys that must not come back: a cron key, where a process that wakes late in the same minute would otherwise start the job again after the first run finished; a trigger key, where a redone batch would otherwise create a second job for a message whose first job already ran. `UniqueKey` is for "at most one of these at a time": a purge, a sync, a rebuild, which should run again later but not twice at once. Cron jobs need both, see the cron helper.

**Not building this.** Two kinds of key is a distinction that takes the paragraph above to explain, and it lands as a second conflict target inside `Enqueue` and `EnqueueBatch`, already the densest statements in the tree. Its one caller today is the cron helper not piling up overruns, and a cron handler that opens with `pg_try_advisory_lock` and returns early gets that with no column, no index and no second key. Build it when a second caller appears that cannot take a lock.
