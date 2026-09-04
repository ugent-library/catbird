# Usage guide

This guide covers the choices an application needs to make when operating Catbird. Start by deciding whether you are publishing a durable stream message or enqueueing work: both use immutable messages, but only published messages are readable from the stream and only jobs have claims and retries.

## Apply the schema

`MigrateUp` applies every migration not yet applied, each in its own transaction with its `cb_migrations` row, and is safe to call from every process at start: two deploying at once queue on an advisory lock and the second finds the rows and skips. It takes anything with pgx's `Begin`, so a pool, a connection or a transaction, and it is the whole of what a caller with no migration tool needs.

An application that already runs a migration tool keeps one tool and one version table. `Migrations()` returns every version's up and down SQL as plain scripts, and the tool executes them on its own transaction; `cb_migrations` is then never created. Pin the catbird version the migration applies, so it produces the same schema whatever catbird version the build carries, and give a catbird release that adds a migration a migration of your own with a higher pin, bounded below by the old one. As a goose Go migration:

```go
func init() {
	goose.AddMigrationContext(addCatbirdUp, addCatbirdDown)
}

// Catbird's schema, pinned at its migration 1.
const catbirdVersion = 1

func addCatbirdUp(ctx context.Context, tx *sql.Tx) error {
	migrations, err := catbird.Migrations()
	if err != nil {
		return err
	}
	for _, m := range migrations {
		if m.Version > catbirdVersion {
			break
		}
		if _, err := tx.ExecContext(ctx, m.UpSQL); err != nil {
			return err
		}
	}
	return nil
}

func addCatbirdDown(ctx context.Context, tx *sql.Tx) error {
	migrations, err := catbird.Migrations()
	if err != nil {
		return err
	}
	for i := len(migrations) - 1; i >= 0; i-- {
		if migrations[i].Version > catbirdVersion {
			continue
		}
		if _, err := tx.ExecContext(ctx, migrations[i].DownSQL); err != nil {
			return err
		}
	}
	return nil
}
```

The scripts are plain DDL with no concurrent index builds, so they run inside the tool's transaction and the whole deploy rolls back as one. A down drops every catbird table, so a rollback discards queued jobs and stream messages.

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

`Consume` takes the same two settings with the same rule. A consumer whose batches take minutes sets `HandlerTimeout` for the batch and `ClaimDuration` for how soon a crashed process's cursor is taken over; the claim is renewed while the handler runs.

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

Use a stable cursor name with one stable pattern set. Sharing a cursor between readers, or changing its patterns, can make one reader acknowledge messages another reader has not processed.

`Consume` is the declared consumer. Register it in every process; one process at a time claims the cursor, handles a batch, and acks, and the others take over when a claim lapses. A consumer runs in one process at a time, so more processes give failover, not throughput. Parallel work is a trigger and a job type, or several consumers over disjoint patterns, each with its own cursor. Set `HandlerTimeout` to bound a batch and `ClaimDuration` to how long a crashed process may hold the cursor; with `HandlerTimeout` above `ClaimDuration` the claim is renewed while the handler runs, so a long batch is not taken over while it is still running. A handler error hands the same batch out again every `PollInterval` until it passes, so a handler that meets a message it cannot use logs it and returns nil. Per-message retries and a failed state are a trigger plus a job type. Delivery is at least once: derive what the handler writes from current state, or key it by the message id.

Set retention according to consumer recovery needs. `GC` removes old stream messages and releases their deduplication keys. A reader that finds `OldestPosition` ahead of its saved position must refetch its source of truth instead of trusting a partial stream catch-up.

Prefix deduplication keys by their domain because published messages and enqueued jobs share one key namespace. Retention must outlast the period in which a key must prevent duplicate work.

## Keep one live job per record

Give a job that must run one pass at a time per record, such as a user's backfill, a `UniqueKey` naming the record. A second `Enqueue` with the key does nothing while a job of the type carries it and enqueues normally once that job ended, whether it completed, failed or was canceled. The key is per job type, and it is free the moment the job ends, unlike a deduplication key, which stays taken for retention.

An enqueue dropped this way is not re-driven. A job type using the key derives its work from state, so the live run, or the next one, covers what the dropped enqueue was about. Triggers and `EnqueueBatch` take no unique key: a trigger must never drop a message, and many messages about one record becoming one run is `Consume`.

## Schedule and trigger work

Scheduled job types run in UTC. A scheduled type cannot require a signal. Every job of a scheduled type carries the type's name as its unique key, so a manual enqueue holds off the ticks while it is live and two manual enqueues cannot overlap. A scheduled type cannot be enqueued in a batch, by a trigger or from a handler.

Triggers preserve the source topic and payload and create one job per matching stream message. Triggered jobs can run concurrently, so a trigger does not preserve completion order. Use `Consume` when a batch should be handled in order, or reduced to the records it concerns, in one process at a time.

## Monitor and clean up

Poll `Queues` for queue depth, state counts, failed jobs, and the age of the oldest claimable job. Use application metrics around handlers for throughput and failure rates.

Read `Status` for one job and `GroupStatus` for a workflow. Both answer from the moment a job is enqueued until a retention period after it ended, and a job that ended reports how: its type, payload, attempts, the error that ended it, when it was created and ended, and the output it recorded. An application that hands a job id to a browser checks `Type` on the way back, so a URL for one kind of run cannot read another. Catbird keeps one result per job and no history of its attempts; a record that must outlive retention is the application's own table, written in the same transaction as `Enqueue` and completed in the same transaction as `Complete`.

Set `Options.Retention` and the runtime runs `GC` itself: once at start and then hourly, in every process, with one process at a time doing the deleting. `GC` removes the results of jobs that ended longer than the retention period ago, then removes old messages that no live job and no result refers to. `Retention` is not stored, so processes that disagree give the database the shorter one. Retention has to outlast the longest wait inside a workflow, because a job that waits longer than that for a dependency finds no output for it. An application that leaves `Retention` zero calls `GC` on its own schedule.

## Use wire behind application authentication

`wire.Wire` signs a page's allowed stream topics and cursor. Its token narrows access; it does not authenticate a request or expire on its own. Keep the polling endpoint behind the application's normal session or authorization layer.

Token topic names may be visible in URLs and access logs. Put only values there that your application is comfortable exposing, or carry the token in a different route mechanism.