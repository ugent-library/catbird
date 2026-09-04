# Architecture

Catbird is a PostgreSQL-backed stream. Its immutable message storage is the base substrate; queues and workflows layer jobs over messages. PostgreSQL is the only coordinator. The schema is plain SQL: no extensions, triggers, functions, or PL/pgSQL. Process-local loops live in the Go client.

`cb_messages` is the common storage layer. `Publish` creates a stream message that later receives a position. `Enqueue` creates a job message and its job row together. The two forms share immutable payload storage, while only published messages belong to stream reads and only messages with a job row belong to worker delivery.

## Boundaries

The package separates declarations, process machinery, and database operations.

- A `Queue` names the work that competes for the same worker slots. Its settings are `BatchSize`, `ClaimDuration`, `HandlerTimeout`, and `PollInterval`.
- A `JobType` names a kind of work and its retry policy. Its settings include `Signal`, `Schedule`, backoff, `MaxAttempts`, and `OnFailed`.
- A `Runtime` belongs to one process. It owns the PostgreSQL pool, one `LISTEN` connection, the stream position assigner, the GC loop when `Retention` is set, and the workers, periodic loops, triggers, and consumers registered in that process.
- Package functions such as `Enqueue`, `Complete`, `Publish`, and `Status` hold no state. They accept a pool, connection, or transaction.

Queue and job-type declarations are plain Go values. They are not stored in the database; only their names are written to job rows. A process claims only the job types it registered.

## Storage

The schema has five tables.

- `cb_messages` is the immutable message substrate. It stores every job payload and published stream message. Published messages receive a stream position after commit; job messages do not, so a job never feeds back into the stream.
- `cb_jobs` stores the narrow, mutable state of each live job: queue, job type, claim deadline, retry state, workflow dependencies, signal state, and the unique key at most one live job of the type carries. The statement that ends the job deletes the row, which is what frees the key.
- `cb_cursors` stores the most recently acknowledged stream position for a named consumer, and until when a `Consume` loop holds the cursor.
- `cb_job_results` stores one row per job that ended: how it ended, completed, failed or canceled, when it ended, the attempts it spent, the last error, and the output it recorded. The statement that deletes the job row writes it, so a job is in one of the two tables and never both. `Status` and `GroupStatus` read it for a job that is no longer live, and `GC` deletes it a retention period after the job ended.
- `cb_migrations` records applied schema migrations.

The hot ready index includes only jobs with no unresolved dependencies. Jobs waiting for a signal have `claimable_at` at infinity, keeping them out of ordinary ready work.

## Job delivery

Workers claim jobs with PostgreSQL row locks. A claim increments `Attempts` and moves the job's `claimable_at` a `ClaimDuration` into the future. If the process crashes or a claim expires, another worker can claim the job.

Jobs are at-least-once. Handlers that write application data should either make the write idempotent using `Job.ID`, or call `Complete` in the same transaction as the application write. External effects need their provider's idempotency mechanism as well.

Completion is the central atomic operation. It deletes the job row by matching on `Attempts` and, in the same statement, writes the job's result with any output, releases dependent jobs, and inserts jobs buffered through `Job.Enqueue` or `Job.EnqueueAfter`. An attempt that lost its claim cannot perform any of those effects. A job's last failed attempt and `Cancel` end a job the same way: the row is deleted and a result that says failed or canceled is written in one statement.

`HandlerTimeout` bounds a handler context. When `HandlerTimeout` exceeds `ClaimDuration`, the worker renews the claims of its running jobs until that context expires; this keeps crash recovery bounded by the shorter `ClaimDuration` while allowing longer handlers.

## Workflows

A workflow is not a separate object. It begins with a job; the ID returned by `Enqueue` is the workflow ID. Jobs created by a handler join that workflow.

Handlers build the next step while they run:

- `Job.Enqueue` schedules a child after the current job completes.
- `Job.EnqueueAfter` schedules a child after the sibling jobs recorded with `Enqueue` in that same handler finish.
- `Job.SetOutput` records an output as part of completion.

Dependency counts and job IDs are derived by the completion statement, not supplied by application code. This prevents a partially constructed workflow from becoming visible.

A job type with `Signal: true` is a gate. It waits until `Signal` supplies a payload for that job type in the workflow. `Cancel` ends every live job in a workflow as canceled. A job that is running when it is canceled is not interrupted, but its completion finds no row and writes nothing, as after a lost claim. `Cancel` does not undo what a completed job did.

## Inspection and retention

A job is inspectable through `Status` from the moment it is enqueued until a retention period after it ended: while it lives, from its `cb_jobs` row; once it ended, from its result. `GroupStatus` reads a workflow the same way. `GC` deletes results by when the job ended and then the messages no row refers to, so how long a job took does not shorten how long it can be inspected. A runtime with `Retention` set runs it hourly in every process; a try-lock lets one run at a time do the deleting and the others skip. Both deletes are index range scans from the old end of their table, so a run that finds nothing reads a few pages. A record that must outlive retention, or hold more than Catbird writes, is the application's own table.

## Streams

`Publish` writes a stream message; it does not create a job. The runtime's position assigner gives committed messages monotonically increasing positions. Readers advance by position, never by message ID, so a long transaction cannot be skipped because another transaction committed first.

Readers use either `ReadAfter` with a caller-held position or `Cursor.Read` and `Cursor.Ack` with a database-held position. Patterns are deliberately limited to an exact topic, a `prefix.#` subtree, or `#`. Each pattern compiles to an individual SQL comparison so PostgreSQL can use the stream indexes.

`Runtime.Trigger` turns matching stream messages into jobs. The read, the job creation, the deduplication, the cursor advance and the worker wake-up are one statement; several processes can run the same trigger, and a batch another process already handled creates no second job.

`Runtime.Consume` hands matching messages to a handler in batches, in position order. Before it reads, a process claims the cursor for `ClaimDuration`; after the handler it acks, and the ack matches on the claim, so a process whose claim lapsed while its handler was still running advances nothing and releases nothing. With `HandlerTimeout` above `ClaimDuration` the process renews the claim every half `ClaimDuration` while the handler's context lives, as the worker renews job claims, so a long batch is not taken over while it runs. One process runs a cursor at a time. The others find it claimed and wait, and take it over when the claim lapses. A handler error leaves the cursor in place and the same batch is handed out again; a consumer has no retry policy of its own.

## Periodic jobs

`JobTypeOptions.Schedule` uses a five-field UTC cron expression. Every process that handles the type ticks it, but a minute-specific deduplication key gives each matching minute one job. A second guard allows at most one live job of the type, so long runs swallow missed ticks rather than accumulating stale work.

## Browser delivery

The optional `wire` package depends only on the exported stream API. Its `Renderer` maps topic patterns, including named path segments, to handlers that render HTML fragments. `Wire` signs topic-and-cursor grants and serves polling responses. The core package imports no HTTP types.

## Migrations

Migrations are embedded SQL files with Goose markers. `MigrateUp` and `MigrateDownTo` apply them in individual transactions under an advisory lock. Callers using another migration tool can consume `MigrationsFS` or parsed `Migrations()` instead.