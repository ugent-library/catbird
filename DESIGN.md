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

## 4. Pending Implementations (Locked-in Requirements)
While the core architecture is proven, a few "Quality of Life" toolkit features remain functionally sketched in tests but lack standard library implementations in `client.go`:

*   **Managed Client-Side Cron:** The `example_test.go` proves exactly-once leaderless cron execution via `ON CONFLICT (dedup_key)`, but a first-class `CronWorker` abstraction needs to be added to the Go/TS SDK. This object must handle the loop, truncation alignment (waking precisely at the top of the minute), and explicit drift validation (`if time.Since(scheduled) > 30s { skip }`) to enforce distributed safety.
*   **Wait-For-Output (Persistent RPC):** As sketched, synchronous RPC execution requires the caller doing `LISTEN cb_rpc_{id}` combined with a targeted query to the `cb_signals` key-value store to retrieve the final execution payload. A `client.RunTaskSync()` wrapper needs to be implemented.
*   **Job & Queue Definitions (Config Tables):** The `streams` branch forces operators to define queues and assign maximum capacity/cleanup logic in table schemas statically. Catbird Lite implicitly creates queues using whatever behavior the calling Client opts into.
*   **Web: SSE, Presence, and Inbox (`cb_wire_*`):** `streams` explicitly embedded Server-Sent Events, distributed message routing, and read-receipts into raw PG `LISTEN/NOTIFY`. Lite completely abandons this infrastructure logic; developers must utilize standard WebSockets or SSE architecture on top of normal API routes.
*   **Global Concurrency Limits:** The `streams` branch allows declarative global rate limiting per queue. Our `Worker` toolkit currently hardcodes raw claims. To solve this, a `max_inflight` override could be handled dynamically via a dedicated limit table or application-layer rate limiting APIs.

## 5. Adversarial Concurrency Protections (Lessons from the `streams` Implementation)

By abandoning complex PL/pgSQL databases engines, Catbird Lite shifts significant responsibility into the thick Go/TS client SDK. To ensure `Catbird Lite` remains completely production safe at high scale, the toolkit applies pure SQL constraints to resolve distributed vulnerabilities:

### Solved Race Conditions & Vulnerabilities
*   **The Double-Execute Fencing Hole:** In naive Client SDKs, a worker executes local app logic, commits, and then sends `DELETE claim`. If it OOMs between those two steps, the job is double executed on retry. **Solved** by injecting the `pgx.Tx` interface deep into the Job Handler function. The user mutates their local application state *inside* the framework's transaction. `Complete()` happens perfectly atomically.
*   **Lost Updates on DAG Resolution:** If multiple upstream steps complete simultaneously, multiple workers fire `UPDATE cb_claims SET dependencies = dependencies - 1`. Normal read/modify/write loops would drop updates, permanently hanging child nodes. **Solved** by leveraging instantaneous atomic bounds checks: `WHERE dependencies > 0`.
*   **Signal Double-Decrements:** If webhooks are delivered dynamically on duplicate retries, blind updates would falsely trigger jobs before all input arrives. **Solved** explicitly by a unified CTE: `WITH sig AS (INSERT... ON CONFLICT DO NOTHING RETURNING id) UPDATE... WHERE message_id IN (sig)`.
*   **Leaderless Cron Drift:** Clients wake up to bridge Cron jobs, submitting unique determinist keys. **Mitigated** locally via the Client rejecting its own execution if the OS paused/suspended it past an acceptable time-horizon padding.
*   **Hot-Loop Index Bloat (MVCC Wear):** Updating `visible_at` constantly modifies a partial B-Tree. **Solved/Mitigated** cleanly via `autovacuum_vacuum_scale_factor = 0.01` and Postgres 14+ "Bottom-Up Index Deletion", maintaining index size trivially in memory without massive IO spikes on steady workloads.
*   **Subscription Re-processing Storms (Data Corruption):** If a Fan-Out-On-Read bridge crashes before moving the cursor, it reads historical messages again. **Solved** natively—not by arbitrary memory caching—but because the `Ack()` and the `Enqueue()`s are bound to the identical transaction span. If the pod crashes, all `ON CONFLICT` bridging rollbacks safely together. No corruption possible.

### Unsolved Vulnerabilities / Compromised Paradigms
*   **Commit-Order vs Insert-Order Gap (`xmin < pg_snapshot_xmin`):** Standard Postgres transactions assign IDs upon `INSERT`, but don't flush to readers globally until `COMMIT`. This breaks sequence-ordered reads. `streams` fixed this via a procedural `Assigner` daemon processing logical locks. Catbird Lite mitigates this by assigning an explicit `position` column updated on a background cron inside the SDK. (Without the daemon polling, readers would still stall out against the oldest open global transaction boundary indiscriminately).
*   **Subscription Leader Contention (Write Amplification):** High Availability deployments (e.g. 5x microservice replicas) will all race to `RegisterTrigger()` reading the stream simultaneously. **Mitigated**, not fully solved—while the DB prevents duplicate output via `dedup_key`, multiple bridger pods will hit `ON CONFLICT DO NOTHING` logic dynamically constantly, amplifying WAL and secondary-index evaluations heavily. Adding distributed connection locking in the toolkit API is necessary for raw enterprise deployments.