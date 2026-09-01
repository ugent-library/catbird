# Catbird Lite

A PostgreSQL-backed job queue, stream, and small workflow engine. Four tables, plain SQL, no PL/pgSQL, no extensions. All logic lives in the client statements (Go today; other languages follow the same statements), so the whole system is the schema in `migrations/00001_lite.sql` plus the declarations in `job_type.go` and the statements in `client.go`, `worker.go` and `trigger.go`.

## Tables

- `cb_messages` — every job's payload and every published message is one row. A job's payload is written once; a published message is updated once, when the assigner sets its `position`. Measured: one dead tuple per published message, cleaned by a routine vacuum; see the position benchmark note below.
- `cb_claims` — one narrow row per job that still has to run: 74 bytes for a single-shot job. Updated on every claim and retry, deleted on completion. Aggressive autovacuum settings on the table keep it small. It carries `queue` (the claim key), `job_type` (which handler runs it), `group_id` (the workflow), `dependencies` with `dependency_job_ids` and `dependent_job_ids` (how many jobs it waits for, which ones they are, and which jobs wait for it), `awaits_signal` with `signal` (the gate and its payload), `died_at` (when the job died, after which no worker claims it again), and `last_error` (what the last failed attempt returned, cut to 256 characters and cleared by the next claim). The columns are declared widest first so the fixed-width ones pack without padding, which is 4 of those 74 bytes; the comments in the migration carry the grouping a reader would otherwise get from the order.

  What a job is doing is not a column. `died_at` is one timestamp — when this job died, never to be claimed again, which is also what `GC`'s age test runs from; NULL while it lives — and everything else a caller wants to know is derived from `visible_at`, `attempts`, `dependencies`, `awaits_signal` and `last_error` by `Status`. That is why the schema has no `status`: the word belongs to the derived answer, and a stored column of the same name would mean something narrower in the same statement.
- `cb_cursors` — one row per stream consumer: the highest position it processed.
- `cb_outputs` — optional job results. A handler records one with `SetOutput` and the completion writes it, in the statement that deletes the claim, so a result cannot outlive an attempt that never finished. `group_id` and `job_type` are copied from the claim beside it, so a later job of the same workflow reads an earlier one's result by what produced it. Read with `Output` and `Outputs`.

A partial index on `cb_claims (queue, visible_at) WHERE died_at IS NULL AND dependencies = 0` holds only claimable rows. Dead rows and rows waiting on other jobs are not in it, and a job waiting for a signal sits at the far end on `'infinity'`, where the claim's `LIMIT` never reaches it.

A second partial index, `cb_claims (group_id) WHERE died_at IS NULL AND group_id IS NOT NULL`, is what `Cancel` and `Signal` probe. A worker cancels the workflow of every job that dies in one, so a downstream outage that fails a whole workflow calls it once per dead job: 6 buffers each with the index, 834 and 4.9 ms without it at 100k live claims. `group_id` is NULL on a job that stands alone, so the volume of single-shot jobs stays out of it — 112 kB against 2128 kB with 1% of jobs in a workflow — and, more to the point, does not pay for a second index entry on every claim and retry.

The unique indexes on `cb_messages (position)` and `cb_messages (deduplication_key)` are partial as well, over the rows that have a value. This is what keeps one table for both worlds cheap: a job's message has neither a position nor usually a deduplication key, and a full unique index writes an entry for every NULL — measured, 1272 kB per index per 200k job messages, probed by nothing, and a third of the insert time. The deduplicating inserts name the predicate, `ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING`, so they still match the partial index.

Job messages and published messages share `cb_messages` because a payload has to live in a row that is never updated. `cb_claims` is rewritten on every claim and every retry, so a payload stored there would be rewritten with it; the message row is written once. Nothing reads across the two kinds — stream reads filter on `position`, job reads go through `cb_claims` — so the shared table is a storage decision, not a shared lifecycle.

**The signal payload is the one exception, and it earns it.** It lives on `cb_claims`, not in a table of its own, because it is created, read and deleted with the claim: a table beside it would exist only to be deleted by the same statement. The rewrite cost is bounded — a signal is on the few jobs that wait for one, a large one is TOASTed so the rewrite copies a pointer, and an absent one costs a null-bitmap bit — and it takes a per-job subquery out of the claim's hot path.

## Runtime

`catbird.New(pool, opts)` returns the process's `Runtime`. Job types are registered on it with `Handle`, each with the function that runs it; triggers with `Trigger`; and `Start(ctx)` runs them all: one `LISTEN` connection for every channel they need, the position assigner, and one goroutine per queue and per trigger, until `ctx` ends and every loop has stopped. Job types sharing a queue share one claim loop, so a process handling thirty kinds of work runs one worker, not thirty. A process holds one connection for notifications however many queues and triggers it runs, and a running job holds none. Registering after `Start` panics: the connection's channel set is fixed when it connects. A dropped connection is reconnected after `ReconnectAfter`; until then the loops run on their poll intervals, and after each connect every loop is woken once, because notifications sent in between are gone.

A process claims only the job types registered on it — the claim filters on `job_type = ANY($registered)`. A job of a type this process does not know is left where it is for a process that does, which is what makes a deploy that adds a type safe in either order.

The statements a caller runs — `Publish`, `Enqueue`, `Complete`, `Signal`, `Cancel`, the stream reads and the rest — are package functions that take any connection or transaction, so they need no runtime and hold no state of their own. What a process runs lives on the `Runtime`; what a kind of work is lives on its `JobType`.

## Declarations

Two values, both plain Go, neither written to the database.

**`NewQueue(name, opts)`** is a name and how work runs under it: `BatchSize`, `Lease`, `Timeout`, `PollInterval`. It answers one question — who competes with whom for slots — and it is the claim key, the single value the ready index is probed with. `Lease` is here rather than on the job type because the claim sets it for a whole batch in one statement, and the renewal renews one the same way; a job type whose handler runs far longer than its neighbours wants its own queue anyway, which is the same isolation argument.

**`NewJobType(name, queue, opts)`** is a kind of job: its name, its queue, and how a run of it is retried — `Signal`, `MaxAttempts`, `MinBackoff`, `MaxBackoff`, `OnDead`. Both the enqueue and the worker take the value, so what a caller creates and what a handler is given cannot disagree. That is the whole reason it exists, and it is the test for what belongs on it: the enqueue writes it into the row, and the handler's correctness depends on it. `Signal` passes. `MaxAttempts`, `MinBackoff`, `MaxBackoff` and `OnDead` are there because none of them touches the claim — the worker reads them from the type it dispatched on — and because they are plainly properties of the work.

**The function that runs it is not on the value.** It is given at registration: `rt.Handle(Review, handleReview)`. Everything on a job type is either stamped on a claim or decided about a run, and a handler is neither — it is this process's code, which a process that only enqueues has no use for and should not have to link. Putting it on the value also made a job type whose handler names it back a compile error, because Go's initialization analysis follows function bodies, and a handler that enqueues its own type is an ordinary thing: a retry loop, a paginated crawl, a reminder that re-arms itself.

What registration gives up in exchange is that `rt.Handle(Review, handlePublish)` compiles. That is a copy-paste error among adjacent lines which runs the wrong handler on the first job — loud, and nothing like the silent failures the rest of this design is built to remove.

Nothing is declared in the database: no definition table, no `Define` call, no deploy-order rule. Only the two names reach a row, so adding a job type is not a migration.

## Jobs

`Enqueue(ctx, db, jobType, payload, opts)` inserts the message and its claim in one statement and sends `NOTIFY` on the queue's channel. The runtime's connection listens on it and wakes the queue's workers, which also poll on an interval, so a lost notification delays a job rather than losing it. It returns the job's id, which is also the id of the workflow it starts — what `Signal`, `Cancel`, `Output` and `Outputs` address.

The wake fires only for a job that is claimable now. A delayed job and a job waiting for a signal wake nobody, because no worker could do anything with them.

**Enqueueing many at once.** `EnqueueBatch` takes a job type, a slice of `BatchMessage` and one `EnqueueOptions` for the whole batch, and writes the messages and their claims with one statement. Claims are made only for the messages that were written, so a message whose deduplication key is taken produces no job, as with the single verb. It is the volume path — what a trigger uses — and it is deliberately the plainer of the two: every job takes the same options, none of them starts a workflow, and a job type declared with `Signal` is refused, because a batch hands back no ids for a caller to signal. It returns how many jobs it created.

**Running jobs.** A worker keeps up to `BatchSize` jobs running at once. It claims as many jobs as it has free slots, runs each in its own goroutine, and claims again as soon as a slot frees, so one long job does not hold up the jobs beside it. While jobs are still waiting it gives slots 5 milliseconds to free before claiming, so a queue of short jobs is claimed by one bigger statement instead of one statement per finished job; on an empty queue nothing is delayed and the loop waits for a `NOTIFY` or for `PollInterval`.

**Claiming.** A worker takes up to as many rows as it has free slots with `FOR UPDATE SKIP LOCKED`, sets `visible_at = now() + Lease`, and increments `attempts`. Nothing records that a job is running: a job with `visible_at` in the future is either delayed, backing off after a failure, waiting for a signal, or claimed. Once `visible_at` passes, any worker may claim it again. That is how a crashed worker's job comes back. The claim also filters on `job_type = ANY($registered)`, so a process never takes work it has no handler for; dispatch is then a map lookup in Go.

**Completing.** A handler is given no connection and the worker holds none while it runs. Completion is one statement, and everything the attempt produced hangs off the one delete inside it:

- the claim, deleted under the `attempts` lease token;
- the result the handler recorded with `SetOutput`, written to `cb_outputs` with the claim's `group_id` and `job_type` beside it;
- one counted down from every job in `dependent_job_ids`, and a `NOTIFY` for each that reached zero;
- the jobs the handler recorded with `Enqueue` and `EnqueueAfter`, with their messages, their claims and their `NOTIFY`.

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

A handler that calls it inside its own transaction ends the job in the same commit as its writes. A handler that returns `nil` without calling it is completed by the worker afterwards, on the pool. A handler that calls it and then rolls the transaction back has told the worker the job is finished when it is not: the mark it leaves records the statement, not the commit, so nothing deletes the claim until the lease runs out and the job runs again. `attempts` is the lease token in every case: if the lease expired and another worker claimed the job, `attempts` moved on, the delete matches nothing, and everything hanging off it is skipped — no result, no countdown, no new jobs — and `Complete` returns `ErrLeaseExpired` so the late attempt rolls back. Two workers may execute the same job; only the one holding the lease commits.

**At-least-once is the model.** A job runs again after a crash, after a lease expires, and after any attempt that did not reach its completion. A handler therefore either makes its writes idempotent — `Job.ID` is the key to do it with — or completes in the same transaction as them, which is the only way an accumulating write like `balance = balance - 100` can be right. Effects outside the database were never covered by either and need the same idempotency key.

**What a running job costs.** Nothing on the pool: no transaction and no connection are held while a handler runs, so `BatchSize` is a number of goroutines and not a number of connections. The library's own statements need a handful of pool connections — the assigner, the claims, renewals and completions of its workers, a trigger's batch — and the `LISTEN` connection is not one of them: it is hijacked out of the pool, so a process opens `MaxConns + 1` connections and the pool keeps its full width. What the handlers hold comes on top of that, and it is what the pool has to be sized for; see Known limits.

**Lease and Timeout rule.** These two durations are the queue's important knobs, and how they compare is part of the setting. `Timeout` bounds one attempt: the handler's context is cancelled when it passes and the attempt counts as failed like any other — a shutdown gives the attempt back, a timeout does not, and it ends the attempt rather than the goroutine; see Known limits. `Lease` is how long an attempt keeps its claim, and so how long a job stays stuck when the process running it crashes. With `Timeout` inside `Lease` — the default is `Lease` less the few seconds the completion needs — a handler must finish within the lease or its work is discarded and the job runs again, so `Lease` is set above the longest handler on the queue. With `Timeout` above `Lease` the worker renews the leases of its running jobs, `Timeout` alone bounds the attempt, and `Lease` stays short: a queue of hour-long jobs keeps a lease of minutes, and a crash returns its jobs in minutes instead of an hour.

**Renewal.** Every half `Lease`, one statement per queue moves `visible_at` a full `Lease` out for every job whose handler context is still live, carrying the same `attempts` token as every other write — so one missed tick loses nothing, and a late tick writes nothing. Renewal follows the handler's context rather than the handler: past `Timeout` the context is spent and the job is renewed no further, so a handler that hangs there, even one that never looks at its context and never returns, loses the job to another worker about a lease later, exactly as an overrun works on a queue that does not renew. A renewal that matches no row means the claim is not this attempt's any more — the lease lapsed, or `Cancel` marked the job dead — and the handler's context is cancelled with `ErrLeaseExpired` as the cause, so the attempt is discarded rather than retried on a claim it lost. A queue that keeps `Timeout` inside `Lease` runs no renewal at all; what renewal costs where it runs is measured under "Extending a lease" in the planned additions.

**Settings rule.** `BatchSize`, `Lease`, `Timeout` and `PollInterval` are queue settings; `MaxAttempts`, `MinBackoff`, `MaxBackoff`, `Signal` and `OnDead` are job type settings. Both are declared once and shared by every process that uses them, so the old rule that all workers on a queue must agree is now a property of the value rather than a rule a reader has to hold — as far as one program goes. Across two binaries it is still convention: neither is stored, so two deployments declaring different values disagree silently. `Signal` needs the most care here, because the enqueue writes it into the claim row. A process still running an old declaration creates jobs with the old gate: they either wait for a signal that will never be sent, or run immediately and give the handler a nil `job.Signal`. And unlike the other settings, which only differ while both binaries are running, a claim row created with the wrong gate does not fix itself when the deploy completes.

**Failing.** A handler error schedules the next attempt: `visible_at` is set to `MinBackoff` plus a random share of what the doubling has added to it, and the doubling stops at `MaxBackoff`. The first retry is `MinBackoff` exactly, and no retry is ever sooner. After the job type's `MaxAttempts` the claim is marked dead, the job's workflow is cancelled, and `OnDead` runs once. The defaults err long — fifteen attempts, waits drawn between one second and ten minutes, about an hour of outage before a job dies — because dying is nearly irreversible: the workflow is cancelled and nothing re-drives a dead job. A crash counts as a failed attempt like any other, so `OnDead` also fires for jobs that repeatedly crashed a worker. A handler that completed the job and then returns an error is not retried: the retry carries the `attempts = $2` token and the claim it would correct is already gone.

Everything after the handler — the completion, the retry, the give-back — runs on a context detached from the worker's, with a few seconds of its own. At shutdown the worker's context is already canceled by the time the handler returns and pgx would reject those statements locally: the completion of work that is already done would be lost and the job would run a second time.

**Shutdown rule.** A job stopped by shutdown is not a failed job, and neither is a handler error that arrives once the worker's context is canceled: the two cannot be told apart, giving the attempt back is the safer mistake, and the error is logged. It is handed back: `attempts` is given back and `visible_at` is set to `now()`, so the next process claims it at once, and neither `MaxAttempts`, nor the cancel cascade, nor `OnDead` runs. Without this, three rolling deploys spend three attempts and 15 minutes of lease on a job that never ran wrong. The write carries the same `attempts = $2` lease token as the completion, so a job whose lease had already expired and been claimed elsewhere is left alone. A crash still costs an attempt, because a crashed worker writes nothing: the increment at claim time is the only thing that counts an attempt nobody saw end, and without it a job that kills its worker would be retried forever.

**Cancel rule.** `Cancel(groupID)` marks the workflow's live claims dead. It stops jobs from starting; a job that is already running finishes and completes — except on a renewing queue, where the next renewal misses the dead claim and cancels the handler's context within about half a lease. Cancel does not undo anything.

**Reading a job's state.** `Status(ctx, db, id)` gives one of eight words — queued, scheduled, running, waiting to retry, waiting for signal, waiting for jobs, dead, completed — with the attempts the job has spent and what its last failed attempt returned. Every state comes from columns that were already there, so it is one read of two rows by primary key: 10 buffers at 300k messages and 100k claims. The words come out of a `CASE` in the statement rather than branches in Go, so a client in another language copies the state machine instead of reinventing it. Completed is the absence of a claim, so it holds only as long as the message: once `GC` has collected it the job reports `ErrNotFound`, and so does a published message's id, which is not a job. The arms are ordered, so a job that is both gated and waiting for other jobs reports the signal.

**Why the claim clears the error text.** Running and waiting to retry are otherwise the same row — a live claim with `visible_at` in the future and an attempt spent — and nothing else in the schema separates a lease from a backoff. So the failure writes `last_error` and the claim sets it back to NULL: text on a row belongs to the attempt that is waiting, never to the one running. That costs the claim nothing on a job that has not failed: clearing a column that is already NULL measures the same as not naming it, 372 bytes of WAL per row either way, so the hot path of a healthy queue is what it was. Measure it interleaved — one claim of 2000 rows ranges from 299 to 408 bytes per row on page state alone, which is more than the difference under test. What the column costs falls on the jobs that fail — about 260 bytes of WAL per failed attempt and a 336-byte tuple against 74 while the job waits — and under a full retry storm clearing trades a fifth to a half of the heap growth for 6–29% more WAL than carrying the text through later attempts. It is not a run history: the next failure overwrites it and the completion deletes it with the row, so a job that failed twice and then succeeded leaves nothing behind.

## Workflows

There is no flow object: not in the API, not in the schema, not in the vocabulary. A workflow is a job whose handler asks for more jobs.

`Job` carries two methods beside `SetOutput`, and all three behave the same way — they record, and the completion writes:

- `job.Enqueue(jobType, payload)` — run this when I complete.
- `job.EnqueueAfter(jobType, payload)` — run this after the jobs I recorded with `Enqueue`. All of them, and nothing wider: not the rest of the workflow, and not what another handler is adding at the same time.

Everything a handler asks for joins its workflow, so `Signal`, `Cancel` and the result reads address all of it by one id. A handler that fails or crashes halfway records nothing and its retry starts with an empty buffer, so the jobs it asked for are created exactly once, by the attempt that completed.

**The count is derived, never supplied.** In the completion statement a job recorded with `EnqueueAfter` takes `dependencies = count(the buffer's jobs that do not wait)` and their ids in `dependency_job_ids`, and each of those carries the waiting job's id in `dependent_job_ids`. All three come from the ids the same statement hands out, through a `MATERIALIZED` CTE over `nextval` — materialized because `nextval` is volatile and an inlined CTE would be evaluated once per reference, giving a message and its claim different ids. Nothing outside catbird ever holds a count, which is what makes the old design's worst failure — a dependency count that is silently one too high and hangs the job forever — unreachable.

**What this shape does not do.** A job waits for the buffer that created it and for nothing else. There is no way to make a job wait for jobs another handler is adding, and no way to build a general graph up front. Deeper workflows compose instead: the joining job's handler asks for the next round, which is usually where the decision about what comes next actually lives.

**Signals are a gate.** A job type declared with `Signal: true` produces jobs that do not run until a payload arrives. The wait is a delay — `visible_at = 'infinity'` — so it needs no place in the ready index and no second wait mechanism; `Signal` writes the payload and sets `visible_at = now()`. Because the type declares it, `job.Signal` is never nil for a gated type and never anything but nil for an ungated one: there is no branch in the handler for "did I get one".

`Signal(ctx, db, groupID, jobType, payload)` addresses the job by the workflow and what it is, not by an id, because a job a handler asked for has no id until that handler's completion runs and no caller can hold one. Delivering to a workflow with no live gated job of that type — it already ran, the signal already arrived, the workflow is gone — returns `ErrNotFound`. Every live job of the type in the workflow is given the payload; one gate per type per workflow is what keeps that to one.

**No deadline.** A gate waits forever. An application that needs "approve within a week" runs a scheduled job over `cb_claims` and cancels; catbird will not tell it that a gate has been open too long. This is the one thing the gate deliberately does not do, and the cheap addition if it hurts is a deadline that marks the job dead and runs `OnDead`, so the timeout branch is a different function rather than a nil check.

**Reading an earlier job's result.** Two reads, and what separates them is whether the reader wants a set of results positionally or one producer by name.

`job.DependencyOutputs(ctx, db)` is the joining job's read: the results of the jobs it waited for, one element per job, in the order they were enqueued, `nil` where a job recorded none. It goes by the ids in `dependency_job_ids`, so it is a primary-key probe, it takes exactly the buffer this job waited for — not a later round of the same types, and not what another handler added — and a job that recorded nothing keeps its place, so the results still line up with the payloads the jobs were given. `nil` for a job that waited for nothing.

`Output(ctx, db, groupID, jobType, dest)` reads the result of the workflow's job of that type, and `Outputs` reads all of them in the order the jobs were created. This is the read for a caller outside the workflow, which holds only the workflow's id — the one `Enqueue` returned. Inside a workflow it is the wider read — it takes every job of the type, every round of it, and skips the jobs that recorded nothing — and it is still the right one for picking a single named producer out of a buffer of mixed types, where a position says nothing. A single-result read of several results returns `ErrAmbiguous` rather than picking one.

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

**Publishing many at once.** `PublishBatch` takes a slice of `BatchMessage` — topic, payload, deduplication key — and writes them with one `INSERT ... SELECT FROM unnest(...)`: a transaction that changed ten thousand records announces them in one round trip. The messages travel as three arrays, so the number of messages is not limited by the number of statement parameters. It returns how many rows it wrote, not their ids: a message whose deduplication key is already taken, or that repeats a key from its own batch, is skipped, and `RETURNING` cannot say which ones — it reads columns of `cb_messages`, and nothing there carries the input's place in the slice. A caller that needs an id per message calls `Publish` for each of them in one transaction. The assigner gives a batch its positions like any other publish, and it drains what is waiting rather than one statement's worth, so ten thousand messages are readable one tick after the commit like any other publish.

**Positions.** Message ids are handed out at `INSERT` time, so a message from a transaction that is still open can have a lower id than messages that already committed; a reader going by id would move past it and miss it. Readers therefore go by `position`, which the assigner sets on published messages in the order it sees them — commit order. A `stream` flag marks published messages; a job's message gets no position, so a job created from a published message is not itself published and a trigger does not feed on its own output. A message from a long transaction gets its position when it commits; it arrives late, after messages published after it, but it arrives, once. This is the rule a plain `SELECT` follows: you see a row when its transaction commits.

The assigner is one statement under an advisory lock, run every `AssignEvery` (250 ms) by every process's `Runtime`. A tick assigns 5000 positions per statement and runs the statement again while its batch came back full, up to 20 rounds; a tick that used all 20 and was still behind logs a warning, because from there the backlog grows faster than the assigner drains it. The lock makes one of them do the work; the rest do nothing. The statement only sets positions that are still empty, so even two assigners running at once cannot move a position a reader may already have passed. Nothing has to be deployed or configured; a message is readable within one tick of its commit. When the assigner assigned anything it sends `NOTIFY cb_stream` with the highest new position; the runtime's connection listens on that channel and wakes the triggers, so they fetch on arrival instead of polling. The cost is one update per published message. Against a variant with positions in a separate narrow table (measured, 200k × 500 B): the column writes ~45% more WAL and ~60% more heap, but publishes and reads ~65% faster, deletes 2× faster (no FK cascade), and needs one table and one index fewer. Vacuum time was under 0.3 s per 200k messages for both.

**Triggers.** `NewTrigger(runtime, name, topic, queue, opts)` declares a loop that runs from `Start`: fetch a batch, `EnqueueBatch` it on the target queue with deduplication key `trigger:<name>:<message id>` per message, `Ack`, commit — all in one transaction, and the whole batch is one statement. A crash before commit redoes the batch; the deduplication keys make the redo a no-op. Several processes may run the same trigger: each only wastes reads, the cursor is monotone, and the deduplication keys keep the output single. Run a trigger in one process if the extra reads matter; there is no leader election.

## Periodic jobs

A job type declared with `Schedule` is enqueued on the minutes the schedule names. The schedule is five fields — minute, hour, day of month, month, day of week — with `*`, numbers, ranges, lists and steps, evaluated in UTC; numbers only, no month or weekday names. `Handle` starts the tick loop with the worker, so a scheduled type ticks in the processes that can run it and an enqueue-only process never ticks. A scheduled job takes no payload: its input cannot vary from run to run, and the same code under two schedules or two arguments is two job types sharing one handler, which also gives each its own name on every claim, status and result.

Every handling process ticks — there is no leader — and one statement with two guards keeps the result single. The deduplication key `periodic:<type>:<minute>`, with the minute in UTC as `YYYY-MM-DDTHH:MMZ` (`periodic:report:2026-08-28T09:30Z`), collapses the processes ticking in the same minute into one job; the format is fixed because every process must produce the same key. And the insert runs only while no live claim of the type exists, so a tick during a live run writes nothing at all — no message row, no key. A run that outlives its schedule therefore swallows the ticks it covers and the next run starts on the first matching minute after it ends: at most one job of a scheduled type is live at a time, and no backlog of stale ticks can form. The minute is computed from the clock at every wake-up, never carried across the sleep, so a process that was suspended resumes into the current minute and cannot enqueue ticks the schedule has moved past. A failed insert is retried while its minute lasts, which the key makes free; without the retry a single-process deployment loses the tick to one dropped connection.

The guard counts every live job of the type, not only ticks, so "run it now" is a plain `Enqueue` of the type and the ticks skip while it goes. Three edges, all deliberate. The manual enqueue is counted but not itself guarded, so two manual enqueues can overlap. The guard repeats the ready index's `dependencies = 0` to keep its probe on that index, so a job of the type created inside a workflow is not counted while it still waits for other jobs. And a handler that overran `Timeout` and ignores its context can still be executing when its claim is gone — the timeout limit below — so the guarantee is one live claim, not one live goroutine.

A scheduled type cannot also declare `Signal`: a gated job never becomes claimable on its own, so its claim would hold the guard against every later tick. `NewJobType` refuses that, a schedule that does not parse, and one that matches no time, each with a panic at declaration rather than a failure on every tick.

## Retention

`GC(retention)` deletes claims dead longer than `retention` — the age runs from `died_at` — and then messages older than `retention` that have no claim. A delayed or waiting job keeps its message however old it is. `cb_claims` and `cb_outputs` reference `cb_messages` with `ON DELETE CASCADE`, so nothing is orphaned.

## Known limits

What this design does not do, or does at a price. A caller has to know about these now.

**A gate waits forever and nothing says so.** A job type declared with `Signal` produces jobs that sit on `visible_at = 'infinity'` until a payload arrives. If none ever does, the job is indistinguishable from one that is legitimately still waiting. There is no deadline and no report. An application with gates should watch `SELECT * FROM cb_claims WHERE awaits_signal AND signal IS NULL AND died_at IS NULL` itself and cancel what has waited too long. This is the most serious of these, and it is the same shape as the dependency-count limit it replaced — a job that quietly never runs — with the difference that the cause is now always outside catbird.

**A job can wait only for the buffer that created it.** `EnqueueAfter` waits for the other jobs the same handler recorded. There is no way to make a job wait for jobs another handler is adding, and no way to declare a graph up front. Deeper shapes compose through the joining job's handler, which is usually where the decision belongs anyway, but a genuine many-to-many dependency between two independently created sets is not expressible.

**A job a handler asked for takes no options.** `job.Enqueue` and `job.EnqueueAfter` take a job type and a payload, and nothing else: no delay, no deduplication key. A job that should run in an hour has to be enqueued by something else.

**A handler that opens a connection has to fit in the pool.** The worker holds none while a handler runs, so `BatchSize` is a number of goroutines — but a handler that opens a transaction for its whole body holds a connection for that whole body, and the two defaults do not fit each other: `BatchSize` is 50 and pgxpool's `MaxConns` is `max(4, NumCPU)`. Fifty handlers then queue in `Begin` against eight connections with their leases already running, and one that waits there longer than `Lease` has its job claimed by a second worker while it still holds a slot for it. Nothing deadlocks, because the worker holds no connection while it waits for one, but the queue runs at the pool's width instead of `BatchSize`'s. Size `MaxConns` for the handlers, or set `BatchSize` to what the pool can carry.

**`BatchSize` limits one process, not the queue.** Every process that registers a job type on the queue runs its own worker with its own `BatchSize` slots, so the real number of jobs running at once is `BatchSize` times the number of processes. Starting more processes multiplies the load on whatever the handlers call, and there is no setting that caps the queue as a whole. A queue that must never make more than a fixed number of concurrent calls has two options: run its job types in a single process, or build the limit in the application.

**The shutdown give-back makes a lease token usable again.** Every other write raises `attempts`, so a token that has moved on never matches a second time; the give-back lowers it by one. A handler that overran its lease can therefore find its token live again: worker A claims at attempt 4 and overruns, B claims at 5, B is stopped by a shutdown and hands the job back to 4, and A's statements match the row once more. A late completion then deletes a claim A no longer holds, and a late failure can schedule the retry, mark the job dead, and run the cancel cascade and `OnDead` for an attempt the give-back said not to count. The window closes as soon as any worker claims the job, which is immediately, because the give-back sets `visible_at` to now. The lease and timeout rule is what keeps this out of reach: a handler that finishes inside `Lease` never carries a stale token, and on a renewing queue the renewal keeps the token live while the handler runs, so only a handler that hangs past `Timeout` can carry one. Closing it in the schema needs a claim counter that only rises, which is a column.

**A timeout does not free a slot.** `Timeout` cancels the handler's context, which stops the calls that take one; a computation that never looks at it, or a socket read given no deadline, keeps its slot until it returns. The attempt is recorded as failed and the job retried on schedule either way, so the cost is throughput on that queue, not the job.

**A trigger does not preserve stream order.** It reads a position-ordered batch and enqueues it onto a queue that runs `BatchSize` jobs at once, so the jobs start in order and finish in any order. A consumer that needs ordering handles the batch itself instead of fanning it out.

**A cursor is not safe in more than one process.** `Read` and `Ack` are two statements with nothing between them, so two processes on the same cursor both handle every batch, and if their patterns differ they also ack past each other's messages. Triggers survive the first of these because their deduplication keys make a repeated batch a no-op; a general reader has no such key. Run one per cursor. The cursor lease that would fix it is not being built: work that has to happen once per message across processes is a trigger and a worker, which have leases and retries already.

**A publish costs two writes and two tuples.** The assigner's `UPDATE` is never a HOT update — `position` is covered by three indexes — so it writes a second tuple and, measured over 50k messages of ~450 bytes, more WAL and more time than the `INSERT` it annotates: 42.8 MB and 592 ms against 33.1 MB and 385 ms. This is the price of the position column, and the alternative measured worse on reads. It is the number a capacity plan needs.

**`died_at` says when, not why.** A job that failed permanently and a job that `Cancel` stopped are the same row. There is no run history and no dead-letter table, and `OnDead` is a callback with nothing durable behind it: if the process is down or the callback returns an error, the fact is logged and gone. There is nothing to re-drive from. `last_error` says what the last failed attempt returned, which is a hint and not the distinction: `Cancel` writes none, so a job canceled before it ever failed has no text, but one canceled after a failure keeps that failure's text and reads exactly like a job that spent its attempts.

**A job type nobody registers waits silently, and its backlog slows every claim on its queue.** The claim filters on the types the process handles, so a job whose type no running process declares is left where it is rather than failing. That is what makes a deploy safe in either order, and it is also why a typo in a deployment leaves jobs sitting in the queue with nothing to say so. The filter runs on the heap behind the ready index, so the waiting rows are walked by every claim on their queue — 1015 buffers and 7.2 ms per claim of 50 at 100k such rows, against 102 and 0.03 ms with none — and the cost grows with the backlog. A job type whose consumer runs occasionally, or can be down for a while, therefore gets a queue of its own: the queue leads the ready index, so its backlog sits in a range no other claim scans, and its name says whose consumer is missing. There is deliberately no warning: a process cannot tell a type nobody handles from a type whose handler is elsewhere, occasional, or behind, so any report it logs is wrong exactly for a deployment that splits its types on purpose. Telling those apart takes registration with liveness — a table of job types and when a handler last reported for each, read by nothing in the engine — and even that misreports the consumer that is off on purpose. Build it if a deployment shows silent backlogs on shared queues; until then the answer is the queue of its own above.

**One deduplication-key namespace for both worlds.** `cb_messages.deduplication_key` is unique across the table, so a `Publish` key and an `Enqueue` key collide. Prefix them if both are in use.

**A deduplication key lives as long as its message, and so does a result.** `GC` deletes messages by age and frees their keys with them, so a key stops deduplicating once its message ages out — fine for a tick's `periodic:<type>:<minute>`, but retention has to exceed the window for any key used for idempotency. `cb_outputs` cascades from `cb_messages` the same way, so retention also has to exceed the longest workflow or a late job cannot read an early one's result.

**`Enqueue` returns `(0, nil)` when the deduplication key was taken** and gives no way to get the existing id, so a caller that needs it makes a second round trip.

**`GC` scans `cb_messages`** — no index on `created_at`, measured at 18,750 buffers over 300k rows — and the runtime does not schedule it. The application has to call it.

**No partitioning.** `cb_messages` is one table for the whole database, and retention is a row-by-row `DELETE` with the index churn that implies rather than dropping a partition.

**No schema versioning and no migration path.** One `.sql` file with goose markers, no runner, no version table, and no route from an installation of the earlier catbird. The second schema change has to invent all of it; building that is on the build list below.

**Positions follow insert order inside a tick.** The assigner orders its batch by `id`, so two messages that commit within one window can get positions in insert order rather than commit order. Commit order is what holds across ticks, which is what a reader depends on.

**`hashtext` is undocumented.** The assigner's advisory lock key is `hashtext('catbird')`, an internal function with no stability guarantee across major versions. A client in another language has to produce the same number.

**The assigner's statement depends on a referenced CTE running.** `pg_notify` fires from a CTE that the final `SELECT` has to reference, which is planner behaviour rather than anything the SQL states. `Enqueue`, `Complete` and `Signal` do the same. A client in another language has to reproduce it exactly, and "the schema plus `client.go` is the whole contract" rests on it.

**The completion depends on `MATERIALIZED` meaning what it says.** The ids of the jobs a handler asked for come from `nextval` inside a CTE that two later CTEs read. Without the keyword PostgreSQL 12 and later inline it, evaluating the volatile function once per reference, and a message and its claim would get different ids. A client in another language has to write the keyword too.

- Rate limits and per-queue configuration are out of scope; applications build them on their own tables. Whether catbird should carry a circuit breaker — a queue that stops claiming while its handlers keep failing against a service that is down — is an open question. The browser layer is planned; see below.
- Without a `Logger`, failures are reported through `slog.Default()`. Errors the library cannot return to the caller are logged.

## What to build, in order

The rule for everything below: a change leaves the core easier to read than it found it, or leaves it alone. Moderate growth is fine. A second version of a concept the reader already holds is not, however few lines it takes — that is what makes a small system stop being one. The core is about thirteen hundred lines across five files, and that is the property worth protecting.

1. ~~**`Read` and `LastPosition`.**~~ Built, as `ReadAfter`, `Cursor.Read`, `Ack`, `LastPosition` and `OldestPosition`; see "Streams" above.
2. ~~**`Timeout`.**~~ Built, as `QueueOptions.Timeout`; see "Lease and Timeout rule" above.
3. ~~**`Status`.**~~ Built, with `last_error`; see "Reading a job's state" above.
4. ~~**Exponential backoff.**~~ Built, as `MinBackoff` and `MaxBackoff` on the job type; see "Failing" above.
5. ~~**The cron helper**, on `DeduplicationKey` alone.~~ Built wider than planned, as `Schedule` on the job type with a live-run guard; see "Periodic jobs" above.
6. ~~**Lease extension.**~~ Built, as renewal in the worker rather than an `ExtendLease` the handler calls; see "Lease and Timeout rule" above and "Extending a lease" below.
7. **Wire**, in a file of its own. It calls `ReadAfter` and touches nothing else, so however long it gets, the core files do not get harder to read.
8. **Typed handler input**, by reflection rather than generics: `newHandler` accepts a small set of signatures and folds the types away, so a handler takes its payload as a struct. It is entirely in the Go binding — no schema, no statements — so it stays available indefinitely. Its price is that the handler parameter widens from `Handler` to `any` and the signature check moves from the compiler to registration, for every handler. `(Out, error)` is the shape not to take with it: a handler that completes in its own transaction cannot return an output the completion already wrote, so `SetOutput` would have to stay beside it.
9. **A migration runner and schema versioning.** One `.sql` file, no version table, no path from an installation of the earlier catbird — the known limit above. The second schema change needs all of it, so it is built before that change and not during it.
10. **Metrics and telemetry.** What the queues are doing, readable without writing SQL: depth and age of the oldest ready job per queue, throughput, failures, dead jobs. The shape is undecided — counters the process exports, a read like `Status` for a whole queue, or a documented set of queries — and deciding that is part of the item.
11. **A review of what lives in Go but belongs in the database.** Every statement lives in `client.go`, so a second client reimplements them all — including the ones the known limits call fragile, where the SQL has to be reproduced exactly. Walk the statements and decide which should become SQL functions that clients call instead. The review is the item; moving anything is its own decision.

Four items below are not being built: the **cursor lease**, a **declared stream loop**, **cancelling a running job** and **`UniqueKey`**. Each stays in this document because the problem it names is real; what changed is the answer. Each carries its ruling in place, with what to watch for that would bring it back. One more is open rather than ruled out: **`RunOnce`** has no caller yet, so it is not built until one appears.

## Planned additions

These come from moving raven, the first application, onto catbird: its own event log stays its own table, and everything that moves data — change signals, cursors, jobs, browser delivery — comes from here. Each item says what it is and why it is needed. Read them with the rulings above and the ones in place below.

### Streams

**Cursor lease.** `cb_cursors` gets `locked_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity'`. A consumer claims a cursor with `UPDATE cb_cursors SET locked_until = now() + lease WHERE name = $1 AND locked_until <= now() RETURNING last_position`; when the row is already leased the claim returns nothing and the consumer waits for the next wake-up. `Ack` keeps `GREATEST` and clears the lease; acking the unchanged position releases without advancing. Reason: an application runs several processes, and a consumer that indexes documents or calls an external API would otherwise do every batch once per process. A lease with a deadline also covers a process that is alive but stuck: when the deadline passes another process takes the cursor. Triggers did not need this because their deduplication keys make a repeated batch harmless; a general handler has no such key.

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

- With `Cursor`: at the `cb_cursors` row, created at 0 if missing. After a batch is written and flushed the connection acks the last position, so a message is shown once across page loads and tabs. Sent is seen. A crash between the flush and the ack shows the same message once more; nothing is lost. `Last-Event-ID` is ignored here: acks follow sends, so a reconnecting tab's id is not ahead of the cursor. This is the durable inbox: a notification is `Publish("user.<id>.<kind>", payload)`, the tray is `Serve` on `user.<id>` with cursor `user:<id>`, and retention is `GC`. Note what retention means here: `GC` deletes messages by age whether or not they were ever seen, so a notification the user never opened disappears after the retention window. raven's current inbox keeps a notification until the user has seen it, however long that takes. That is a real change in behaviour and needs its own decision when wire is built. Two tabs open at once may both show a message that arrived before either acked; per-tab cursors (`user:<id>:<tab>`) would avoid that at the price of showing everything in every tab.
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

- Inbox, watches and presence tables: the inbox is a cursor (above); "who is on this record" is an application table with a heartbeat column and a message on the record's topic when it changes. Whether catbird should offer presence as a convenience on top of that is an open question; wire itself does not carry it.
- Poll transport: `ReadAfter(patterns, after)` behind a GET is the whole thing, which is where this starts.
- A shared read per process with in-memory fan-out. Every connection runs its own `Read` per wake-up, so hundreds of open tabs on a busy stream mean hundreds of small index reads per assigner tick. If that ever matters, one read per process with matching in memory can be added behind the same frame contract.

### Jobs

**Exponential backoff.** Built; see "Failing" above. `MinBackoff` is the wait after the first failed attempt and the shortest wait there is, and `JobTypeOptions.MaxBackoff` caps how far the doubling grows it. The wait after attempt n is drawn between `MinBackoff` and `min(MinBackoff * 2^(n-1), MaxBackoff)`, computed in the retry statement itself:

```sql
UPDATE cb_claims
SET visible_at = now() + $3::interval
    + (least($3::interval * 2 ^ least(attempts - 1, 20), $4::interval) - $3::interval) * random()
WHERE message_id = $1 AND attempts = $2 AND died_at IS NULL
```

The defaults are `MinBackoff` one second, `MaxBackoff` ten minutes and `MaxAttempts` fifteen, instead of the old flat minute and five attempts: together they ride out about an hour of outage before a job dies. Both backoff settings are job type settings — they never touch the claim — so two kinds of work on one queue back off on their own terms.

Two reasons for growing the wait. A handler that fails because a service it calls is down retries at a fixed minute for as long as `MaxAttempts` allows, so five attempts cover five minutes of outage; doubling spends the same five attempts over half an hour. And an outage fails every job on the queue at once: with a fixed delay all of them come back in the same second, hit the service that is still down, and come back together again a minute later. The random draw spreads them.

The exponent is clamped at 20 so that a job which somehow reaches a high attempt count does not overflow the interval multiplication before `MaxBackoff` bounds it.

A job type that wants fixed pacing sets `MinBackoff` and `MaxBackoff` to the same value: the distance the draw runs over is zero, and every retry waits exactly that long.

**The draw runs from `MinBackoff`.** Every retry spends one of `MaxAttempts`, so a wait that can be near zero burns one on a service that has had no time to recover — a job's early attempts gone inside a second. What it costs is the spread on the first retry, which is `MinBackoff` exactly for every job that failed; the jobs of an outage spread from the second retry on, and by their own failure times before that. A `MaxBackoff` under `MinBackoff` is raised to it.

**Run status.** Built, with `last_error`; see "Reading a job's state" above. Two things came out different from the plan.

The plan had `attempts` and the type's `MaxAttempts` telling a running job from one backing off. They cannot: `MaxAttempts` only says whether the next failure is the last, which `died_at` already records, and a package function that takes a `Conn` cannot reach the job type in the first place. Nothing in the schema separates a lease from a backoff, so the error text does it — which is what decided the column's shape rather than whether to have it.

And the column was decided by measuring. The claim is unchanged for a job that has not failed, because clearing a NULL column measures the same as not naming it. Where the two shapes differ is under a full retry storm, once rows actually carry text: clearing costs 6–29% more WAL over the whole run — the alternating tuple widths move rows between pages, which `pg_walinspect` shows as page splits and pruning records that carrying the text does not produce — and saves 20–53% of the heap growth, the gap widening the further behind vacuum falls. Its claim statement is the cheaper of the two there as well, 372 bytes per row against 629, because it writes a 74-byte tuple where the other writes 336. Both totals are small in absolute terms — tens of megabytes of WAL per 400k failed attempts on a table that stays under 10 MB — so cost decided nothing, and what settled it is that clearing needs one column where carrying the text needs two: a second column to record which attempt the text belongs to, measured at +2 bytes on every claim row.

Measuring this took two wrong answers first, both from comparing runs made one after another on tables at different churn levels. Interleave the statements on one table: the page state moves more than the change under test does.

**Cancelling a running job.** Today `Cancel` stops jobs from starting and a running job finishes. The addition: `Cancel` also sends `pg_notify('cb_queue_<queue>', 'cancel:<job id>')`, and a worker that is running that job cancels the handler's context. The handler decides what a cancelled context means — stop at the next safe point, or finish — so the database still does not interrupt a running job; it only tells the handler that a cancel arrived. Open decision: keep the weak cancel and have handlers that must stop early poll `cb_claims.died_at` at their own boundaries, or add the notification.

**Not building this; the weak cancel is the rule.** The notification needs the worker to keep a map of running job id to cancel function, maintained on claim and on completion and read from the listen loop. It is the only addition on this list that puts mutable state in the middle of the claim loop. A handler that must stop early polls `cb_claims.died_at` at its own boundaries. Build it if a handler appears that cannot poll — one blocked in a call it does not control.

Renewal since moved part of this: a renewing queue keeps that map anyway, for the ticker, and a renewal that misses a dead claim cancels the handler — so there a cancelled job's handler is stopped within about half a lease, notification or none. The ruling stands for every queue whose `Timeout` sits inside its `Lease`.

**Extending a lease.** Built, as renewal inside the worker; see "Lease and Timeout rule" above. Nothing new is exported: a queue opts in by setting `Timeout` above `Lease`, which is the same line that says its handlers outlive their leases, and the clamp that used to lower `Timeout` to `Lease` is gone — the comparison decides renewal instead. The plan here, an `ExtendLease(ctx, db, job, lease)` the handler calls where it finished a piece of work, was dropped for two reasons. It put a catbird call inside every long handler, a burden that grows with the application. And its answer to hangs — extension at progress points, so a stuck handler stops extending — only catches a hang between two calls the handler remembered to make. The worker's ticker renews every job whose handler context is still live, so the hang bound is `Timeout` for every handler, remembered or not, and a handler contains no queue code at all. The timer objection this entry used to carry — a timer renews a hung handler as readily as a progressing one — was an objection to tying renewal to the handler; tied to the handler's context it fails over correctly, because `Timeout` spends the context whether or not the goroutine ever returns. `Timeout` itself does not move with a renewal, which the plan thought it must: it may simply exceed `Lease`, and then it is the one bound on the attempt.

The worker keeps a map of its running jobs for the ticker to read, which is the mutable state the cancel ruling above declined. Renewal needs it for its own sake, and cancelling the handler when a renewal misses is then three lines, which is how `Cancel` came to reach running handlers on renewing queues without the notification.

What it costs, measured on the real schema (Postgres 18, one client; the ratios are the portable part, the absolute numbers are one machine's). A renewal is never HOT, because `visible_at` is in the ready index, and writes ~280 bytes of WAL against ~438 for a claim measured beside it. One statement renews a whole batch: 264 bytes a row against 294 renewed one by one, and one round trip instead of fifty. A tick of 50 jobs runs in 0.7 ms, of 5000 in 17 ms. Sustained — 500 jobs in flight, ticking for twelve simulated hours — the table holds flat at 120 kB with vacuum running between ticks, and reaches 7 MB of heap and 1.2 MB of ready index with vacuum off entirely; the primary key stays flat either way, bottom-up deletion pruning its duplicate entries, and the ready index is the one that needs vacuum, because renewals keep inserting at its right edge and the migration's 1% autovacuum setting is what keeps up with them. Load scales as jobs in flight over `Lease` and not with throughput: jobs shorter than half a lease are mostly never renewed, a full worker of 50 long jobs at the default lease writes ~0.3 MB an hour, and the one expensive corner is a lease of seconds under hundreds of concurrent long jobs — a lease no long-job queue has a reason to set, since `Lease` is only how long a crashed worker's jobs wait.

**Job timeout.** Built; see "Lease and Timeout rule" above. Two things came out different from the plan. The deadline is on the handler alone and not on its completion, which already runs on a context detached from the worker's so that work finishing at the deadline is not lost. And the default is under `Lease` rather than equal to it: at `Timeout == Lease` the timeout fires as the lease expires, so the retry it schedules updates no row. It is a queue setting because it is set against `Lease` — their comparison is what decides renewal; by every other test it looks like a job type one, since it never touches the claim.

**`Runtime.RunOnce(ctx, queue)`.** Claim one batch on that queue, run it, return. Tests run jobs deterministically without a background worker. It takes the queue rather than returning a handle from `Handle`, so registration stays a call that returns nothing.

**Deferred until something needs it.** An application's job tests call the handler as a plain function, and its loop tests call one step of the loop, so nothing in them claims a batch. What `RunOnce` covers that a direct call cannot is a handler that only records: `SetOutput`, `Enqueue` and `EnqueueAfter` write nothing until the completion runs, and their buffers on `Job` have no reader, so a test that calls such a handler directly sees nothing at all. Build it when a workflow handler has to be tested. Converting this repository's own tests to it is not a reason: they cover the claim loop, the notification and the poll fallback, all of which it skips.

**A `catbirdtest` package rather than a method on `Runtime`, when it comes back.** A test-only entry point on the central type is surface every caller reads and no application calls. What stops a separate package from holding it today is that claiming a batch needs `Runtime.workers`, `worker.claimBatch` and `worker.run`, all unexported, so the package reaches them only through something exported for it — which is the method again. What such a package can hold with no help from the core is the helper above it: apply the schema to an empty database, start the runtime, enqueue, wait until `cb_claims` is empty, stop. That is slower, it covers the loops `RunOnce` skips, and it is what every test of this system writes by hand.

**Cron helper.** Built, as `JobTypeOptions.Schedule`; see "Periodic jobs" above. Four things came out different from the plan here. The schedule is the five-field line rather than an interval, because "daily at 09:30" is not an interval and the interval is the line `*/5 * * * *`. It lives on the job type rather than in a helper's arguments, so two processes cannot disagree on it — the same reason `Signal` lives there — and `Handle` starts the ticking, so a scheduled job is only ever enqueued where something can run it. The tick's statement carries a second guard beside the key: it writes nothing while a live claim of the type exists, so at most one job of a scheduled type is live and overlap prevention moved out of the handler — the advisory lock this entry prescribed, per handler and easy to get wrong, is gone. And the payload carries nothing rather than the minute: the staleness test the minute existed for defended against a backlog of ticks behind a long run, and the guard makes that backlog impossible.

**One live job per key.** `EnqueueOptions.UniqueKey`: a second `Enqueue` with the same key does nothing while a job with that key is still live — queued, delayed, running, or waiting to retry — and goes through again once that job completed or died. The key is a column on `cb_claims` with a partial unique index, `CREATE UNIQUE INDEX ... ON cb_claims (unique_key) WHERE unique_key IS NOT NULL AND died_at IS NULL`. Completion deletes the claim and a dead claim leaves the index, so the key frees itself in both cases; a retry keeps the claim live, so the key stays taken. `Enqueue` inserts the message only when no live claim holds the key, and the claim insert carries the matching `ON CONFLICT ... DO NOTHING`, so two enqueues at the same instant both return 0 for the loser. The loser may leave a message row without a claim; `GC` removes it with the other old messages.

This is a second key next to `DeduplicationKey`. `DeduplicationKey` lives as long as the message and is for keys that must not come back: a tick's key, where a process that wakes late in the same minute would otherwise start the job again after the first run finished; a trigger key, where a redone batch would otherwise create a second job for a message whose first job already ran. `UniqueKey` is for "at most one of these at a time": a purge, a sync, a rebuild, which should run again later but not twice at once.

**Not building this.** Two kinds of key is a distinction that takes the paragraph above to explain, it lands as a second conflict target inside `Enqueue` and `EnqueueBatch`, already the densest statements in the tree, and the index is a second unique index on the row every claim and retry rewrites. Raven, whose jobs run on River and use River's unique jobs, was the test, and every one of its uses is served without the key. Its three periodic jobs are scheduled types, whose tick guard is this key's guarantee for exactly them — at most one live run — without a column or an index. Its blob deletes key on blob ids, which are minted once and never reused, so `DeduplicationKey` covers them — a key that must never come back is exactly what it is for. Its one real "at most one at a time" job, the per-user ORCID backfill, already filters against its own table of pushed records, so the advisory lock in the handler gives the same skip and a duplicate pass that slips through finds nothing to push. The shape that remains — a sweep enqueued on every change — this key gets wrong rather than fails to serve: an enqueue during a running sweep collapses into it, and a change committed after that sweep read its snapshot produces no job, so the work waits for an unrelated change to arrive. River's in-flight uniqueness loses the same wake-up; raven avoids it only because its indexer is a cursor consumer, not a job. What would bring the key back is a caller whose "at most one live" job is expensive and not idempotent, so that a duplicate run does damage the handler cannot cheaply refuse on its own.
