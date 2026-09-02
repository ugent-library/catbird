# Usage guide

This guide covers the choices an application needs to make when operating Catbird. Start by deciding whether you are publishing a durable stream message or enqueueing work: both use immutable messages, but only published messages are readable from the stream and only jobs have claims and retries.

## Start with a queue and job type

Put work with the same concurrency and runtime characteristics on one queue. `BatchSize` is the number of jobs one process may run from that queue at once. More processes multiply that concurrency.

Use distinct queues when a job type has a materially different runtime, concurrency limit, or downstream dependency. In particular, a job type that is handled only occasionally should have its own queue; otherwise its ready rows are examined by the workers sharing its queue.

Job types define retry behavior. The defaults are fifteen attempts, a one-second minimum backoff, and a ten-minute maximum backoff. Set `MinBackoff` and `MaxBackoff` to the same value for a fixed retry delay.

## Choose claim duration and handler timeout together

`ClaimDuration` determines how quickly a crashed worker's job can be reclaimed. `HandlerTimeout` determines how long a handler is allowed to run.

- With `HandlerTimeout` at or below `ClaimDuration`, handlers must finish before their claim expires. This is the default shape.
- With `HandlerTimeout` above `ClaimDuration`, Catbird renews claims while handlers run. Use it for long jobs that should still recover promptly after a process crash.

Each value left unset defaults from the other. Set only `HandlerTimeout` and the claim covers it plus a few seconds for the completion, so a queue of short jobs recovers crashed work quickly without renewal. Set neither and both are about five minutes.

Handlers must observe their context. A handler that ignores a timed-out context may keep its worker slot even after Catbird has retried the job elsewhere.

## Size the connection pool

Workers do not hold a connection while a handler runs. A handler that begins a transaction does.

Size `pgxpool` for the maximum number of concurrent handler transactions, plus the library's own database activity. Alternatively, limit `BatchSize` to the number of connections handlers can safely hold. Starting fifty handlers against an eight-connection pool lets jobs spend their claims waiting to begin a transaction.

## Complete with application writes

When a handler changes application data, prefer a transaction that includes `catbird.Complete`. This makes the application write, job output, workflow progression, and job completion one commit.

```go
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, "UPDATE invoices SET sent_at = now() WHERE id = $1", invoiceID); err != nil {
    return err
}
if err := catbird.Complete(ctx, tx, job); err != nil {
    return err
}
return tx.Commit(ctx)
```

`Complete` can return `ErrClaimLost`. Roll back in that case: another worker owns the job, and none of this attempt's Catbird effects were committed.

For effects outside PostgreSQL, use the idempotency mechanism supplied by the external system. Catbird can run a job more than once after a crash or an expired claim.

## Model workflows as handlers

Start a workflow with `Enqueue`; its returned ID addresses `Signal`, `Cancel`, and `GroupStatus`.

Use `job.Enqueue` for children that may run after the current job. Use `job.EnqueueAfter` for a join after siblings queued by the same handler. It does not create a general dependency graph and cannot wait on work created by another handler.

For a job type declared with `Signal: true`, arrange for the application to send exactly the signal its workflow needs. A gate has no deadline and otherwise waits indefinitely. Applications with approval deadlines should track them and call `Cancel` when they expire.

## Publish and consume streams

Use `Publish` for facts that consumers should see, and `Enqueue` for work that a worker should run. Enqueued jobs are not stream messages.

Use a stable cursor name with one stable pattern set. Sharing a cursor between readers, or changing its patterns, can make one reader acknowledge messages another reader has not processed. General cursor consumers run in one process; use a trigger plus worker for cross-process, at-least-once handling.

Set retention according to consumer recovery needs. `GC` removes old stream messages and releases their deduplication keys. A reader that finds `OldestPosition` ahead of its saved position must refetch its source of truth instead of trusting a partial stream catch-up.

Prefix deduplication keys by their domain because published messages and enqueued jobs share one key namespace. Retention must outlast the period in which a key must prevent duplicate work.

## Schedule and trigger work

Scheduled job types run in UTC. A scheduled type cannot require a signal. A manual enqueue of a scheduled type prevents ticks while that job is live, but two manual enqueues may overlap.

Triggers preserve the source topic and payload and create one job per matching stream message. Triggered jobs can run concurrently, so a trigger does not preserve completion order. Use a single handler over a cursor only when strict in-order batch handling is a real requirement.

## Monitor and clean up

Poll `Queues` for queue depth, state counts, dead jobs, and the age of the oldest claimable job. Use application metrics around handlers for throughput and failure rates; Catbird deliberately stores no run history.

Run `GC` on an application schedule. It removes dead claims after the requested retention period, then removes old messages whose jobs have completed. It does not run automatically.

## Use wire behind application authentication

`wire.Wire` signs a page's allowed stream topics and cursor. Its token narrows access; it does not authenticate a request or expire on its own. Keep the polling endpoint behind the application's normal session or authorization layer.

Token topic names may be visible in URLs and access logs. Put only values there that your application is comfortable exposing, or carry the token in a different route mechanism.