# Decisions

This document records the current boundaries of Catbird. Revisit a decision when its stated condition appears in a real caller, not simply because an alternative is possible.

## PostgreSQL is the coordinator, not an application runtime

**Decision:** Keep the schema to tables, indexes, and plain SQL statements. Do not add PL/pgSQL, database triggers, or generated schemas.

**Why:** The core remains inspectable, migrations remain portable, and PostgreSQL supplies the coordination it is good at: transactions, locks, indexes, and notifications.

**Revisit when:** A second client must run workers and reproducing the safety-critical statements becomes a demonstrated maintenance burden.

## Stream patterns are exact topics, subtrees, or everything

**Decision:** Accept only an exact topic, `prefix.#`, or `#` in stream reads.

**Why:** Each pattern can compile to indexed SQL predicates. Arbitrary segment wildcards do not provide the same predictable read plan.

**Revisit when:** A concrete consumer requires selection that cannot be represented by its own payload filtering or topic design.

## Workflows grow from a handler

**Decision:** A handler creates children with `Enqueue` and joins only the siblings it recorded with `EnqueueAfter`.

**Why:** Catbird derives dependency counts and IDs atomically. It avoids user-managed counters and avoids offering a general graph API with unclear ownership.

**Revisit when:** A caller needs a many-to-many dependency between independently created sets of jobs.

## Signals are indefinite gates

**Decision:** A signal-gated job waits until `Signal` arrives or its workflow is cancelled. Catbird does not impose a deadline.

**Why:** Approval and other business deadlines belong to the application that understands their policy.

**Revisit when:** Several applications need the same durable deadline and dead-job behavior.

## A cursor runs in one process at a time

**Decision:** `Consume` claims the cursor for `ClaimDuration` before it reads, renews the claim while a handler runs longer than that, and its ack matches on the claim. Every process may register the consumer; the others find the cursor claimed and wait, and one of them takes it over when the claim lapses. A consumer has no attempts, no backoff and no failed state: a failed batch is handed out again every `PollInterval` until it passes. More processes give failover, not throughput.

**Why:** The first consumers coalesce on purpose. An indexer reduces a window of five hundred events to the distinct records they concern and indexes each once, so one job per message is the wrong unit, and a batch running in several processes at once would do every batch once per process. Skipping a message whose handler keeps failing would leave a projection missing a record with nobody told, while a cursor that stops moving is visible. Per-message retry state has nowhere to live: a message row is written once and a cursor is one row. The handler decides what it can pass over; a trigger and a job type give a message retries and a failed state.

**Revisit when:** A consumer falls behind its stream. The addition is partitioned members: several cursors under one consumer name, each reading the messages whose key hashes to it, so order holds per key and the members run in parallel across processes. The key is a topic prefix, since the topic's last segment names the event; changing the member count is a new consumer.

## Polling is wire's first transport

**Decision:** `wire` serves polling requests. SSE and browser-held stream positions are deferred.

**Why:** Polling meets current needs while reusing the same renderer and cursor machinery without a connection per page.

**Revisit when:** A page has a latency requirement that its polling interval cannot meet.

## There is no global queue concurrency limit

**Decision:** `BatchSize` limits a queue in one process only.

**Why:** A database-wide limit adds coordination and needs application-specific failure and rate policies.

**Revisit when:** A shared downstream service needs a hard cross-process concurrency cap that cannot be enforced at the service boundary.

## A job's result is kept for retention, its history is not

**Decision:** Every job that ends leaves one row in `cb_job_results`: how it ended, completed, failed or canceled, when it ended, the attempts it spent, the last error, and its output. The ended state is stored as a word because nothing about a job that ended changes with time; a live job's state stays derived, because three of its states end by the clock. `GC` deletes it a retention period after the job ended. There is no row per attempt, and no row outlives retention.

**Why:** Any real use inspects jobs after they ran, and a run page needs the type, the timing and the outcome of a job that is no longer live. One row per ended job answers that at the cost of one insert per completion into a table the completion already wrote. Retention runs from the end of the job so a slow job is not inspectable for less time than a fast one. A row per attempt, and a record kept for good, are what the application knows how to shape and how long to keep; both grow without bound next to the hot table if Catbird keeps them.

**Revisit when:** A caller needs to see what an attempt before the last one returned, or when it ran, and cannot record that from its own handler.

## There is no live-only uniqueness key

**Decision:** Catbird has durable `DeduplicationKey`, not a key that becomes free when a job completes or fails.

**Why:** A second uniqueness model complicates the busiest statements and can lose a wake-up for a change that arrives during a running job.

**Revisit when:** An expensive, non-idempotent job needs at-most-one live run and the application cannot cheaply reject a duplicate.