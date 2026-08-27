# Catbird Lite: Architectural Design Blueprint

Catbird Lite is a PostgreSQL-backed message queue and workflow engine designed for high concurrency and exact guarantees, without requiring database tuning, background daemons, or procedural SQL (PL/pgSQL).

It is built as an extremely lightweight "Dumb SQL / Thick Client" toolkit. The database manages atomic locks and persistence natively via B-Trees, while the language clients (Go, TypeScript, etc.) manage the loop orchestration.

## Core Philosophy: The Iron Triangle of Database Queues
Building queues on relational databases usually hits an "Iron Triangle". You can only pick two:
1.  **Code Simplicity** (No sweepers, state machines, or daemons).
2.  **Concurrency** (Thousands of workers, zero deadlocks).
3.  **Low Database Wear** (Virtually zero WAL generation or Vacuum bloat).

Traditional `SKIP LOCKED` implementations sacrifice #3—updating heavy JSON payload rows millions of times fills the database with dead tuples, crashing shared DBaaS instances due to MVCC bloat.

Catbird breaks the triangle by strictly separating **Immutable Facts** from **Mutable Leases**.

## 1. The Schema (Three Primitives)

The entire system is backed by exactly three static tables. There is no dynamic table generation and zero PL/pgSQL functions.

### `cb_messages` (The Immutable Stream)
This table acts as the unified payload store for both Jobs and Streams. It is strictly append-only.
Because massive JSON payloads are never `UPDATE`d, Postgres never creates dead tuples around them, completely eliminating the primary source of autovacuum bloat.

### `cb_cursors` (Stream Consumption)
A simple key-value table tracking the `last_id` a given stream consumer has successfully processed.

### `cb_claims` (Job Lifecycle & Routing)
The high-volatility table. When a message needs to be processed as a Job, a "Claim" row is inserted here. It tracks the lease timeout (`visible_at`) and retry attempts.

*MVCC Defense*: We enforce a microscopic autovacuum target (`autovacuum_vacuum_scale_factor = 0.01`) directly in the DDL of this specific table. Because the table is incredibly narrow (only a few integers and a timestamp), updates occur directly on disk pages and Postgres aggressively recycles dead Index/Heap blocks dynamically. When a job completes, the claim is deleted. A partial index (`status = 0 AND dependencies = 0`) guarantees that completed or paused jobs gracefully eject from the working B-Tree instantly.

## 2. The Client Toolkit Contract

All language implementations (Go, Python, TypeScript) conform to the exact same behavioral contract, relying purely on executing parameterized SQL statements.

### Exactly-Once Fencing (Atomic Handlers)
A primary challenge of external queue engines is Fencing. If a worker commits application logic but crashes before ACKing the queue, the job reruns and causes data corruption.
Catbird Lite solves this seamlessly: The Go framework manages a database `pgx.Tx` transaction, passes it *into* the User's Job Handler, and upon success executes the `DELETE LIMIT` of the claim in the *exact same transaction*. If the pod crashes, Postgres natively rolls back both the Application's modifications and the job claim perfectly.

### Subscriptions & Bridging (Fan-out on Read)
We do not copy payloads to route them. A message published to `payment.success` is written exactly once to `cb_messages`.
To bridge a Stream to a Job (e.g., "Run this job every time a payment succeeds"), the client runs a Subscription loop that reads the stream (safely avoiding commit-order stalling via Postgres `pg_snapshot_xmin`) and calls `Enqueue()` for every matching row, using the `message_id` as the Deduplication Key to survive Bridger crashes.

### Leaderless Cron
All client instances wake at `X:00` and generate identical deterministic strings (`cron:hourly:2026-08-27:00:00`). The database's `cb_messages.dedup_key` constraint guarantees only exactly one instance applies. Zero leader-election tracking required.

### DAGs and Joins (`dependencies` counter)
Workflows are achieved natively without PL/pgSQL generation. `cb_claims` has a `dependencies` integer column.
The partial index excludes any claim where `dependencies > 0`. When upstream steps complete, the client simply issues `UPDATE cb_claims SET dependencies = dependencies - 1`. When it hits 0, it automatically slides into the 'ready' index. (And if an upstream step fails permanently, an automatic `client.CancelCascade(correlation_id)` is invoked to clean up sibling nodes safely).

### Signals (External Payloads)
When a paused job is waiting for external human input (e.g., an approval payload), delivering the payload directly onto the `cb_messages` or `cb_claims` table breaks either their immutability or MVCC updatability patterns. 
Catbird Lite solves this explicitly with an append-only sidecar table: `cb_signals`. The worker natively parses these aggregates from Postgres via `jsonb_object_agg` right at checkout.

## 3. Operational Bounds 
The framework intentionally relies on external standard operations to cap table growth. 
Call `client.GC(7_days)` on a nightly cron task to transparently `DELETE` aged-out `status=3` claim rows and the historical log of immutable `cb_messages`, keeping disk requirements infinitely flat.