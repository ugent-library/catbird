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

## Cursor consumers are single-process

**Decision:** General stream cursors have no claim. Run one consumer per cursor. Use a trigger and worker when work must be distributed across processes.

**Why:** A claimed cursor would duplicate the job claim and retry machinery. Triggers already make cross-process handling safe through atomic enqueue and deduplication.

**Revisit when:** A real consumer needs ordered, once-per-message batch handling across several processes.

## Polling is wire's first transport

**Decision:** `wire` serves polling requests. SSE and browser-held stream positions are deferred.

**Why:** Polling meets current needs while reusing the same renderer and cursor machinery without a connection per page.

**Revisit when:** A page has a latency requirement that its polling interval cannot meet.

## There is no global queue concurrency limit

**Decision:** `BatchSize` limits a queue in one process only.

**Why:** A database-wide limit adds coordination and needs application-specific failure and rate policies.

**Revisit when:** A shared downstream service needs a hard cross-process concurrency cap that cannot be enforced at the service boundary.

## There is no live-only uniqueness key

**Decision:** Catbird has durable `DeduplicationKey`, not a key that becomes free when a job completes or dies.

**Why:** A second uniqueness model complicates the busiest statements and can lose a wake-up for a change that arrives during a running job.

**Revisit when:** An expensive, non-idempotent job needs at-most-one live run and the application cannot cheaply reject a duplicate.