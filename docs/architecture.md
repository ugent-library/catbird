# Architecture

Catbird is a PostgreSQL-backed stream. Its immutable message storage is the base substrate; queues and workflows layer job claims over messages. PostgreSQL is the only coordinator. The schema is plain SQL: no extensions, triggers, functions, or PL/pgSQL. Process-local loops live in the Go client.

`cb_messages` is the common storage layer. `Publish` creates a stream message that later receives a position. `Enqueue` creates a job message and its claim together. The two forms share immutable payload storage, while only published messages belong to stream reads and only claimed messages belong to worker delivery.

## Boundaries

The package separates declarations, process machinery, and database operations.

- A `Queue` names the work that competes for the same worker slots. Its settings are `BatchSize`, `ClaimDuration`, `HandlerTimeout`, and `PollInterval`.
- A `JobType` names a kind of work and its retry policy. Its settings include `Signal`, `Schedule`, backoff, `MaxAttempts`, and `OnDead`.
- A `Runtime` belongs to one process. It owns the PostgreSQL pool, one `LISTEN` connection, the stream position assigner, and the workers, periodic loops, and triggers registered in that process.
- Package functions such as `Enqueue`, `Complete`, `Publish`, and `Status` hold no state. They accept a pool, connection, or transaction.

Queue and job-type declarations are plain Go values. They are not stored in the database; only their names are written to job rows. A process claims only the job types it registered.

## Storage

The schema has five tables.

- `cb_messages` is the immutable message substrate. It stores every job payload and published stream message. Published messages receive a stream position after commit; job messages do not, so a job never feeds back into the stream.
- `cb_claims` stores the narrow, mutable state of each live job: queue, job type, claim deadline, retry state, workflow dependencies, signal state, and death time. Completion deletes the claim.
- `cb_cursors` stores the most recently acknowledged stream position for a named consumer.
- `cb_outputs` stores optional job results. Results are written as part of job completion.
- `cb_migrations` records applied schema migrations.

The hot ready index includes only live claims with no unresolved dependencies. Jobs waiting for a signal use an infinite visibility time, keeping them out of ordinary ready work.

## Job delivery

Workers claim jobs with PostgreSQL row locks. A claim increments `Attempts` and moves the job's visibility time a `ClaimDuration` into the future. If the process crashes or a claim expires, another worker can claim the job.

Jobs are at-least-once. Handlers that write application data should either make the write idempotent using `Job.ID`, or call `Complete` in the same transaction as the application write. External effects need their provider's idempotency mechanism as well.

Completion is the central atomic operation. It deletes the claim by matching on `Attempts` and, in the same statement, writes any output, releases dependent jobs, and inserts jobs buffered through `Job.Enqueue` or `Job.EnqueueAfter`. An attempt that lost its claim cannot perform any of those effects.

`HandlerTimeout` bounds a handler context. When `HandlerTimeout` exceeds `ClaimDuration`, the worker renews the claims of its running jobs until that context expires; this keeps crash recovery bounded by the shorter `ClaimDuration` while allowing longer handlers.

## Workflows

A workflow is not a separate object. It begins with a job; the ID returned by `Enqueue` is the workflow ID. Jobs created by a handler join that workflow.

Handlers build the next step while they run:

- `Job.Enqueue` schedules a child after the current job completes.
- `Job.EnqueueAfter` schedules a child after the sibling jobs recorded with `Enqueue` in that same handler finish.
- `Job.SetOutput` records an output as part of completion.

Dependency counts and job IDs are derived by the completion statement, not supplied by application code. This prevents a partially constructed workflow from becoming visible.

A job type with `Signal: true` is a gate. It waits until `Signal` supplies a payload for that job type in the workflow. `Cancel` marks live jobs in a workflow dead and stops jobs that have not started; it does not undo completed effects.

## Streams

`Publish` writes a stream message; it does not create a job. The runtime's position assigner gives committed messages monotonically increasing positions. Readers advance by position, never by message ID, so a long transaction cannot be skipped because another transaction committed first.

Readers use either `ReadAfter` with a caller-held position or `Cursor.Read` and `Cursor.Ack` with a database-held position. Patterns are deliberately limited to an exact topic, a `prefix.#` subtree, or `#`. Each pattern compiles to an individual SQL comparison so PostgreSQL can use the stream indexes.

`Runtime.Trigger` turns matching stream messages into jobs. The read, the job creation, the deduplication, the cursor advance and the worker wake-up are one statement; several processes can run the same trigger, and a batch another process already handled creates no second job.

## Periodic jobs

`JobTypeOptions.Schedule` uses a five-field UTC cron expression. Every process that handles the type ticks it, but a minute-specific deduplication key gives each matching minute one job. A second guard allows at most one live claim for the type, so long runs swallow missed ticks rather than accumulating stale work.

## Browser delivery

The optional `wire` package depends only on the exported stream API. Its `Renderer` maps topic patterns, including named path segments, to handlers that render HTML fragments. `Wire` signs topic-and-cursor grants and serves polling responses. The core package imports no HTTP types.

## Migrations

Migrations are embedded SQL files with Goose markers. `MigrateUp` and `MigrateDownTo` apply them in individual transactions under an advisory lock. Callers using another migration tool can consume `MigrationsFS` or parsed `Migrations()` instead.