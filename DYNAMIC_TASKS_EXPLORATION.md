# Dynamic Task Spawning: Streaming and Unbounded Arrays

> **Note**: This document explores dynamic task patterns conceptually. Code examples may show pseudo-generic syntax for clarity. See [REFLECTION_API_DESIGN.md](REFLECTION_API_DESIGN.md) for the actual implementation approach using cached reflection for type validation.

> **Updated**: This document now uses **strong typing patterns** from [TYPED_COORDINATION.md](TYPED_COORDINATION.md) for all approaches. Dependencies are validated at build time via reflection; pool operations use `StepContext` interface.

## Problem Statement

Map steps (as designed in `MAP_STEPS_DESIGN.md`) handle fixed-size arrays efficiently:
- Small/medium arrays (1-5000 items): Spawn all tasks upfront, aggregate results
- Known array size at step start time
- All tasks spawned in single transaction

However, certain workloads require different patterns:

### Limitations of Map Steps

**Memory constraints:**
- Can't materialize 1M+ element array in single JSON value
- PostgreSQL `jsonb` size limits (~256MB compressed)
- Network transfer overhead for large payloads

**Unknown/unbounded sizes:**
- Database pagination: "Index all 10M records" (size unknown until query)
- External APIs: "Fetch all pages until exhausted" (unknown page count)
- Event streams: "Process incoming webhooks" (continuous, no end)

**Long-running operations:**
- Map step must complete before tasks spawn
- No incremental progress (all-or-nothing)
- Can't start processing early results while fetching continues

**Complex coordination:**
- Multiple sources feeding same task pool (diamond pattern)
- Dynamic task creation based on task results (recursive)
- Conditional task spawning mid-execution

### Target Use Cases

**1. Reindexing Flow** (User's Example)
```
Problem: Rebuild search index with zero downtime

Diamond pattern:
     [create-index]
        /        \
   [reindex]  [listen-changes]
       |           |
   (spawn 10M    (spawn changes
    index tasks)  as they arrive)
       \         /
    [switch-index]
        (wait for both task pools to complete)

Challenges:
- Unknown record count (millions)
- Can't materialize all records in array
- Need to process while listening for changes
- Both branches spawn tasks independently
- Final step waits for all tasks from both sources
```

**2. ETL Pipeline with Pagination**
```
[fetch-page-1] → spawn 1000 tasks
[fetch-page-2] → spawn 1000 tasks
[fetch-page-N] → spawn 1000 tasks
    ↓
[wait-all-tasks]
    ↓
[generate-report]

Challenges:
- Unknown page count until fetching
- Can't block on fetching all pages before processing
- Want to start processing early pages immediately
```

**3. Recursive Task Expansion**
```
[crawl-website]
    → spawn task for page A
        → discovers links B, C → spawn 2 more tasks
            → discover D, E, F → spawn 3 more tasks
    ...
    
Challenges:
- Task count unknown and grows dynamically
- Tasks spawn other tasks (recursive)
- Need termination detection (no more tasks + no in-flight)
```

**4. Long-Running Event Processing**
```
[listen-webhook] (runs indefinitely)
    → spawn task for each incoming webhook
    → never "completes" in traditional sense
    
Challenges:
- Unbounded execution time
- No natural completion point
- May want graceful drain/shutdown
```

## Design Space Exploration

### Approach 1: Generator Steps

**Concept**: Step yields tasks via iterator/channel instead of returning array.

#### API Design

```go
// GeneratorStep creates a step that yields tasks over time
GeneratorStep[Item, Result]("process", "source",
    // Generator function: yields items via channel
    func(ctx context.Context, in Input, yield chan<- Item) error {
        cursor := initCursor()
        
        for {
            batch, err := fetchBatch(cursor, 1000)
            if err != nil {
                return err
            }
            
            if len(batch) == 0 {
                break  // Exhausted
            }
            
            for _, item := range batch {
                select {
                case yield <- item:
                    // Item queued for processing
                case <-ctx.Done():
                    return ctx.Err()
                }
            }
            
            cursor = cursor.Next()
        }
        
        return nil  // Generator exhausted
    },
    
    // Task handler: processes individual items (same as MapStep)
    func(ctx context.Context, item Item) (Result, error) {
        return processItem(ctx, item)
    },
)
```

#### How It Works

**1. Generator Execution**
```
Worker 1: Picks up generator step
    ├─ Calls generator function
    ├─ Generator yields items to channel
    ├─ Worker spawns tasks for each yielded item
    │  └─ INSERT INTO cb_t_{flow} (flow_run_id, step_name, task_index, input)
    ├─ Continues until generator returns or context cancelled
    └─ Marks generator "spawning_complete"
```

**2. Task Processing** (parallel to generation)
```
Worker 2, 3, 4, ...: Poll tasks
    ├─ Read tasks from cb_t_{flow} (same as map step)
    ├─ Execute task handler
    └─ Complete task (increment completed_count)
```

**3. Completion Detection**
```
Last task completion:
    ├─ Check: generator_status='spawning_complete' AND all tasks done?
    ├─ If yes: Aggregate results, mark step complete
    └─ Trigger dependent steps
```

#### Database Schema

```sql
-- Extend cb_map_steps to track generator state
ALTER TABLE cb_map_steps ADD COLUMN is_generator boolean DEFAULT false;
ALTER TABLE cb_map_steps ADD COLUMN generator_status text;  -- 'running', 'spawning_complete', 'failed'

-- Track which worker is running generator (for crash recovery)
ALTER TABLE cb_s_{flow} ADD COLUMN generator_worker_id text;
ALTER TABLE cb_s_{flow} ADD COLUMN generator_started_at timestamptz;

-- Task spawning counter (no need to know total upfront)
ALTER TABLE cb_s_{flow} ADD COLUMN tasks_spawned int DEFAULT 0;
ALTER TABLE cb_s_{flow} ADD COLUMN tasks_completed int DEFAULT 0;
```

#### Pseudo-SQL for Generator Execution

```sql
-- Worker claims generator step
CREATE OR REPLACE FUNCTION cb_claim_generator_step(
    flow_name text,
    worker_id text
)
RETURNS TABLE (
    step_run_id bigint,
    step_name text,
    input jsonb,
    dependency_outputs jsonb
) AS $$
BEGIN
    RETURN QUERY
    WITH claimed AS (
        UPDATE cb_s_{flow} s
        SET status = 'generating',
            generator_worker_id = cb_claim_generator_step.worker_id,
            generator_started_at = now()
        WHERE s.id = (
            SELECT s2.id
            FROM cb_s_{flow} s2
            INNER JOIN cb_map_steps ms 
              ON ms.flow_name = cb_claim_generator_step.flow_name 
              AND ms.step_name = s2.step_name
            WHERE ms.is_generator = true
              AND s2.status = 'created'
              AND s2.remaining_dependencies = 0
            ORDER BY s2.created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING s.id, s.step_name, s.flow_run_id
    )
    SELECT c.id, c.step_name, f.input, get_dependency_outputs(c.flow_run_id, c.step_name)
    FROM claimed c
    INNER JOIN cb_f_{flow} f ON f.id = c.flow_run_id;
END;
$$;

-- Worker spawns tasks during generation
CREATE OR REPLACE FUNCTION cb_spawn_generator_tasks(
    flow_name text,
    step_run_id bigint,
    items jsonb  -- Array of items yielded in this batch
)
RETURNS void AS $$
DECLARE
    _step_name text;
    _flow_run_id bigint;
    _current_index int;
    _batch_size int := jsonb_array_length(items);
BEGIN
    -- Get current task count
    SELECT step_name, flow_run_id, tasks_spawned
    INTO _step_name, _flow_run_id, _current_index
    FROM cb_s_{flow}
    WHERE id = cb_spawn_generator_tasks.step_run_id;
    
    -- Insert tasks with sequential indices
    INSERT INTO cb_t_{flow} (flow_run_id, step_name, task_index, input)
    SELECT _flow_run_id, _step_name, _current_index + (idx - 1), elem
    FROM jsonb_array_elements(cb_spawn_generator_tasks.items) WITH ORDINALITY AS t(elem, idx);
    
    -- Increment counter
    UPDATE cb_s_{flow}
    SET tasks_spawned = tasks_spawned + _batch_size
    WHERE id = cb_spawn_generator_tasks.step_run_id;
END;
$$;

-- Worker marks generation complete
CREATE OR REPLACE FUNCTION cb_complete_generator(
    flow_name text,
    step_run_id bigint
)
RETURNS void AS $$
DECLARE
    _tasks_spawned int;
    _tasks_completed int;
BEGIN
    -- Mark generator done spawning
    UPDATE cb_s_{flow}
    SET status = 'aggregating',
        generator_worker_id = NULL
    WHERE id = cb_complete_generator.step_run_id
    RETURNING tasks_spawned, tasks_completed
    INTO _tasks_spawned, _tasks_completed;
    
    -- If all tasks already completed, aggregate immediately
    IF _tasks_spawned = _tasks_completed THEN
        PERFORM cb_aggregate_generator_results(cb_complete_generator.flow_name, cb_complete_generator.step_run_id);
    END IF;
END;
$$;

-- Called when last task completes
CREATE OR REPLACE FUNCTION cb_aggregate_generator_results(
    flow_name text,
    step_run_id bigint
)
RETURNS void AS $$
DECLARE
    _flow_run_id bigint;
    _step_name text;
    _aggregated_output jsonb;
BEGIN
    -- Get step info
    SELECT flow_run_id, step_name
    INTO _flow_run_id, _step_name
    FROM cb_s_{flow}
    WHERE id = cb_aggregate_generator_results.step_run_id
      AND status = 'aggregating';
    
    IF NOT FOUND THEN
        RETURN;  -- Already aggregated or wrong status
    END IF;
    
    -- Aggregate task outputs
    SELECT jsonb_agg(output ORDER BY task_index)
    INTO _aggregated_output
    FROM cb_t_{flow}
    WHERE flow_run_id = _flow_run_id
      AND step_name = _step_name;
    
    -- Mark step completed (same as map step from here)
    UPDATE cb_s_{flow}
    SET status = 'completed',
        completed_at = now(),
        output = _aggregated_output
    WHERE id = cb_aggregate_generator_results.step_run_id;
    
    -- Cascade to dependent steps...
END;
$$;
```

#### Go Implementation

```go
type generatorHandler struct {
    handlerOpts
    generatorFn func(context.Context, json.RawMessage, chan<- json.RawMessage) error
    taskFn      func(context.Context, mapTaskMessage) ([]byte, error)
}

func (w *Worker) runGenerator(ctx context.Context, step *Step) {
    // Claim generator step
    stepRun, err := claimGeneratorStep(ctx, w.conn, step.Name, w.id)
    if err != nil {
        return
    }
    
    // Channel for yielded items
    items := make(chan json.RawMessage, 100)  // Buffer for backpressure
    
    var wg sync.WaitGroup
    
    // Goroutine 1: Run generator function
    wg.Add(1)
    go func() {
        defer wg.Done()
        defer close(items)
        
        err := step.generatorHandler.generatorFn(ctx, stepRun.Input, items)
        if err != nil {
            failGeneratorStep(ctx, w.conn, stepRun.ID, err.Error())
        }
    }()
    
    // Goroutine 2: Spawn tasks from yielded items
    wg.Add(1)
    go func() {
        defer wg.Done()
        
        batch := make([]json.RawMessage, 0, 100)
        ticker := time.NewTicker(1 * time.Second)
        defer ticker.Stop()
        
        for {
            select {
            case item, ok := <-items:
                if !ok {
                    // Generator exhausted, flush final batch
                    if len(batch) > 0 {
                        spawnGeneratorTasks(ctx, w.conn, stepRun.ID, batch)
                    }
                    completeGenerator(ctx, w.conn, stepRun.ID)
                    return
                }
                
                batch = append(batch, item)
                
                // Batch full, spawn tasks
                if len(batch) >= 100 {
                    spawnGeneratorTasks(ctx, w.conn, stepRun.ID, batch)
                    batch = batch[:0]
                }
                
            case <-ticker.C:
                // Periodic flush even if batch not full
                if len(batch) > 0 {
                    spawnGeneratorTasks(ctx, w.conn, stepRun.ID, batch)
                    batch = batch[:0]
                }
                
            case <-ctx.Done():
                return
            }
        }
    }()
    
    wg.Wait()
}
```

#### Pros & Cons

**Pros:**
- ✅ No array materialization (streaming)
- ✅ Start processing early (tasks spawn as generated)
- ✅ Bounded memory (channel buffering + batching)
- ✅ Familiar iterator pattern from Go
- ✅ Works for unknown/unbounded sizes
- ✅ Reuses map step task infrastructure

**Cons:**
- ❌ Single generator worker (not parallelized)
- ❌ Generator must run on worker (not in PostgreSQL)
- ❌ Crash recovery complex (restart generation from beginning)
- ❌ No generator progress checkpointing
- ❌ Can't have multiple generators for same step

---

### Approach 2: Task Spawning via StepContext

**Concept**: Steps can call `stepCtx.SpawnTask()` API during execution to dynamically create tasks.

#### API Design

```go
// StepContext provides side-effect APIs including task spawning
type StepContext interface {
    FlowRunID() int64
    StepName() string
    FlowName() string
    
    // Spawn a task to this step's pool
    SpawnTask(ctx context.Context, input any) (taskID int64, error)    
    // Signal no more spawns (move pool to drain phase)
    PoolStartDrain(ctx context.Context) error
    // Wait for all spawned tasks to complete
    WaitTaskPool(ctx context.Context) error
    // Get pool status
    PoolStatus(ctx context.Context) (PoolStatus, error)
}

// Dynamic spawning step: uses typed dependency + StepContext
reindexStep := catbird.NewStep("reindex").
    "DependsOn(catbird.Dep[IndexID]("create-index").
    WithTaskPool("index-ops").
    WithHandler(
        func(
            ctx context.Context,
            stepCtx StepContext,
            indexID IndexID,  // Type-safe dependency injection
        ) (Summary, error) {
            // Page through database
            cursor := ""
            totalRecords := 0
            
            for {
                records, nextCursor, err := db.QueryPage(ctx, cursor, 1000)
                if err != nil {
                    return Summary{}, err
                }
                
                // Spawn indexing task for each record
                for _, record := range records {
                    _, err := stepCtx.SpawnTask(ctx, IndexOp{
                        Type:    "index",
                        IndexID: indexID,
                        Record:  record,
                    })
                    if err != nil {
                        return Summary{}, err
                    }
                    totalRecords++
                }
                
                if nextCursor == "" {
                    break  // No more pages
                }
                cursor = nextCursor
            }
            
            // Signal no more spawns
            if err := stepCtx.PoolStartDrain(ctx); err != nil {
                return Summary{}, err
            }
            
            // Wait for all indexing tasks to complete
            if err := stepCtx.WaitTaskPool(ctx); err != nil {
                return Summary{}, err
            }
            
            return Summary{Indexed: totalRecords}, nil
        },
    )
```

#### How It Works

**1. Step Execution with Pool**
```
Worker executes step:
    ├─ Handler receives StepContext (side-effect API)
    ├─ Dependencies injected as function parameters (type-safe)
    ├─ Handler calls stepCtx.SpawnTask(taskName, input)
    │  └─ INSERT into cb_pool_tasks (pool_name, flow_run_id, input)
    ├─ Handler calls stepCtx.PoolStartDrain()
    │  └─ UPDATE pool to mark no more spawns from this step
    ├─ Handler calls stepCtx.WaitTaskPool()
    │  └─ Blocks until all spawned tasks complete
    └─ Handler returns final output
```

**2. Task Polling** (pool workers)
```
Pool workers poll cb_pool_tasks:
    ├─ Read tasks WHERE status = 'created' AND pool_name = $pool
    ├─ Look up task handler by pool_name (registered on flow creation)
    ├─ Execute handler with task input
    └─ Mark task completed/failed
```

**3. WaitTaskPool Implementation**
```sql
-- Blocking wait for all tasks in pool
CREATE OR REPLACE FUNCTION cb_wait_pool(
    pool_name text,
    flow_run_id bigint,
    poll_interval_ms int DEFAULT 100
)
RETURNS void AS $$
DECLARE
    _pending_count int;
BEGIN
    LOOP
        SELECT count(*) INTO _pending_count
        FROM cb_pool_tasks
        WHERE pool_name = cb_wait_pool.pool_name
          AND flow_run_id = cb_wait_pool.flow_run_id
          AND status IN ('created', 'started');
        
        IF _pending_count = 0 THEN
            -- Check if any tasks failed
            IF EXISTS (
                SELECT 1 FROM cb_pool_tasks
                WHERE pool_name = cb_wait_pool.pool_name
                  AND flow_run_id = cb_wait_pool.flow_run_id
                  AND status = 'failed'
                LIMIT 1
            ) THEN
                RAISE EXCEPTION 'cb: pool % has failed tasks', pool_name;
            END IF;
            
            RETURN;
        END IF;
        
        PERFORM pg_sleep(poll_interval_ms / 1000.0);
    END LOOP;
END;
$$;
          AND status IN ('created', 'started');
        
        IF _pending_count = 0 THEN
            -- Check if any tasks failed
            SELECT count(*) INTO _failed_count
            FROM cb_dynamic_tasks
            WHERE flow_run_id = cb_wait_dynamic_tasks.flow_run_id
              AND spawned_by_step = cb_wait_dynamic_tasks.spawned_by_step
              AND status = 'failed';
            
            IF _failed_count > 0 THEN
                RAISE EXCEPTION 'cb: % dynamic tasks failed', _failed_count;
            END IF;
            
            RETURN true;
        END IF;
        
        PERFORM pg_sleep(poll_interval_ms / 1000.0);
    END LOOP;
END;
$$;
```

#### Database Schema

```sql
-- Dynamic tasks table (shared across all flows)
CREATE TABLE cb_dynamic_tasks (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    flow_run_id bigint NOT NULL,
    spawned_by_step text NOT NULL,  -- Which step spawned this task
    task_name text NOT NULL,         -- Handler to use
    input jsonb NOT NULL,
    output jsonb,
    error_message text,
    status text NOT NULL DEFAULT 'created',
    deliveries int NOT NULL DEFAULT 0,
    deliver_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    
    CONSTRAINT status_valid CHECK (status IN ('created', 'started', 'completed', 'failed'))
);

CREATE INDEX cb_dynamic_tasks_deliver_at_idx ON cb_dynamic_tasks(deliver_at) 
    WHERE status IN ('created', 'started');
CREATE INDEX cb_dynamic_tasks_flow_step_idx ON cb_dynamic_tasks(flow_run_id, spawned_by_step);
CREATE INDEX cb_dynamic_tasks_status_idx ON cb_dynamic_tasks(status);
```

#### Recursive Task Example

```go
// Web crawler that spawns more tasks as it discovers links
Task("crawl-page",
    func(ctx context.Context, url string, spawner TaskSpawner) (PageData, error) {
        // Fetch page
        html, err := http.Get(url)
        if err != nil {
            return PageData{}, err
        }
        
        // Extract links
        links := extractLinks(html)
        
        // Spawn crawl tasks for each new link
        for _, link := range links {
            if !isVisited(link) {
                spawner.Spawn(ctx, "crawl-page", link)  // Recursive!
            }
        }
        
        return PageData{URL: url, Title: extractTitle(html)}, nil
    },
    WithDynamicSpawning(true),  // Enable task spawning
)

// Start crawling
client.RunTask(ctx, "crawl-page", "https://example.com")

// Termination detection: No pending tasks + no executing tasks
```

#### Pros & Cons

**Pros:**
- ✅ Maximum flexibility (spawn anytime during execution)
- ✅ Supports recursive task spawning
- ✅ No generator-specific code path
- ✅ Step controls task lifecycle explicitly
- ✅ Can spawn different task types from same step

**Cons:**
- ❌ WaitAll blocks worker thread (inefficient)
- ❌ No streaming aggregation (must wait for all)
- ❌ Complex crash recovery (which tasks spawned before crash?)
- ❌ Recursive termination detection hard (need cycle detection)
- ❌ Testing/debugging harder (implicit task creation)

---

### Approach 3: Cursor/Pagination Pattern

> **Note**: This pattern is now subsumed by **GeneratorStep** (Approach 1). GeneratorStep is more universal and handles cursor pagination directly. Users can write the pagination loop in GeneratorStep just as easily. See [COORDINATION_PATTERNS.md - Pattern 2](COORDINATION_PATTERNS.md#pattern-2-generatorstep-streaming-arrays--pagination) for the recommended approach.

**Concept**: Framework handles pagination automatically; user provides cursor logic.

#### API Design

```go
// PaginatedStep automatically pages through data
PaginatedStep("fetch-records", "create-index",
    // Fetch page function: returns (items, nextCursor, error)
    func(ctx context.Context, cursor string) ([]Record, string, error) {
        records, next, err := db.QueryPage(ctx, cursor, 1000)
        return records, next, err
    },
    
    // Process item function (same as MapStep)
    func(ctx context.Context, record Record) (IndexResult, error) {
        return searchIndex.Index(ctx, record)
    },
    
    WithInitialCursor(""),  // Start cursor
)
```

#### How It Works

**State machine:**
```
1. Step starts with cursor=""
   ├─ Call fetchPage(cursor) → (items, nextCursor, err)
   ├─ Spawn map tasks for items
   ├─ Store nextCursor in step state
   └─ Mark status='paging'

2. All tasks from page complete
   ├─ Check: nextCursor != "" ?
   ├─ If yes: Call fetchPage(nextCursor) → repeat step 1
   └─ If no: Aggregate all results, mark completed

3. Final aggregation
   ├─ Collect outputs from all pages
   └─ Mark step completed
```

#### Database Schema

```sql
-- Extend cb_s_{flow} to track pagination state
ALTER TABLE cb_s_{flow} ADD COLUMN cursor text;
ALTER TABLE cb_s_{flow} ADD COLUMN page_number int DEFAULT 0;

-- Track tasks across pages
-- task_index is global across all pages
-- page_number identified which page spawned the task (if needed for pagination state recovery)
-- Typically stored in input JSONB instead
```

#### SQL Functions

```sql
-- Spawn tasks for a page and update cursor
CREATE OR REPLACE FUNCTION cb_page_tasks(
    flow_name text,
    step_run_id bigint,
    items jsonb,
    next_cursor text
)
RETURNS void AS $$
DECLARE
    _step_name text;
    _flow_run_id bigint;
    _current_page int;
    _current_task_index int;
BEGIN
    -- Get current state
    SELECT step_name, flow_run_id, page_number, tasks_spawned
    INTO _step_name, _flow_run_id, _current_page, _current_task_index
    FROM cb_s_{flow}
    WHERE id = cb_page_tasks.step_run_id;
    
    -- Spawn tasks for this page
    INSERT INTO cb_t_{flow} (flow_run_id, step_name, task_index, input)
    SELECT _flow_run_id, _step_name, _current_task_index + (idx - 1), elem
    FROM jsonb_array_elements(cb_page_tasks.items) WITH ORDINALITY AS t(elem, idx);
    
    -- Update step state
    UPDATE cb_s_{flow}
    SET cursor = cb_page_tasks.next_cursor,
        page_number = _current_page + 1,
        tasks_spawned = tasks_spawned + jsonb_array_length(cb_page_tasks.items),
        status = CASE 
            WHEN cb_page_tasks.next_cursor = '' THEN 'aggregating'
            ELSE 'paging'
        END
    WHERE id = cb_page_tasks.step_run_id;
    
    -- If last page, check if all tasks already done
    IF cb_page_tasks.next_cursor = '' THEN
        PERFORM cb_check_pagination_complete(cb_page_tasks.flow_name, cb_page_tasks.step_run_id);
    END IF;
END;
$$;

-- Worker executes next page fetch
CREATE OR REPLACE FUNCTION cb_claim_pagination_step(
    flow_name text,
    worker_id text
)
RETURNS TABLE (
    step_run_id bigint,
    step_name text,
    cursor text
) AS $$
BEGIN
    RETURN QUERY
    UPDATE cb_s_{flow} s
    SET status = 'fetching_page'
    WHERE s.id = (
        SELECT s2.id
        FROM cb_s_{flow} s2
        WHERE s2.status = 'paging'
          AND all page tasks completed  -- Need to check
        ORDER BY s2.page_number
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING s.id, s.step_name, s.cursor;
END;
$$;
```

#### Go Implementation

```go
type PageFetcher[T any] func(ctx context.Context, cursor string) (items []T, nextCursor string, err error)

func (w *Worker) executePaginatedStep(ctx context.Context, step *Step) {
    for {
        // Claim next page to fetch
        claim, err := claimPaginationStep(ctx, w.conn, step.Name, w.id)
        if err != nil || claim == nil {
            return  // No more pages or error
        }
        
        // Call user's page fetcher
        items, nextCursor, err := step.pageFetcher(ctx, claim.Cursor)
        if err != nil {
            failStep(ctx, w.conn, claim.StepRunID, err.Error())
            return
        }
        
        // Spawn tasks for this page
        itemsJSON, _ := json.Marshal(items)
        err = pageTasks(ctx, w.conn, claim.StepRunID, itemsJSON, nextCursor)
        if err != nil {
            return
        }
        
        // If last page (nextCursor empty), we're done fetching
        if nextCursor == "" {
            break
        }
    }
}
```

#### Pros & Cons

**Pros:**
- ✅ Simple user API (just provide fetch function)
- ✅ Framework handles pagination complexity
- ✅ Streaming (pages fetched as previous page processes)
- ✅ Bounded memory per page
- ✅ Clear progress tracking (page N of M)

**Cons:**
- ❌ Sequential page fetching (can't parallelize fetch)
- ❌ Requires cursor-based pagination (not offset/limit)
- ❌ Must wait for page N tasks before fetching page N+1 (could be inefficient)
- ❌ Less flexible than generator (no custom logic between pages)

---

### Approach 4: Multi-Source Task Pool (Diamond Pattern)

**Concept**: Multiple steps spawn tasks to the same pool via StepContext; flow waits for all.

#### API Design

```go
// Flow with diamond pattern: multiple spawning steps → shared pool → convergence
flow := catbird.NewFlow("reindex",
    // Phase 1: Create index
    InitialStep("create-index",
        func(ctx context.Context, config IndexConfig) (IndexID, error) {
            return searchIndex.Create(ctx, config)
        }),
    
    // Phase 2a: Reindex existing records (type-safe dependency + pool)
    NewStep("reindex-existing").
        "DependsOn(catbird.Dep[IndexID]("create-index").
        WithTaskPool("index-ops").  // Pool name
        WithHandler(
            func(
                ctx context.Context,
                stepCtx StepContext,
                config IndexConfig,
                indexID IndexID,  // Type-safe dependency
            ) (Summary, error) {
                cursor := ""
                spawned := 0
                
                for {
                    records, next, err := db.QueryPage(ctx, cursor, 1000)
                    if err != nil {
                        return Summary{}, err
                    }
                    
                    // Spawn to shared pool
                    for _, record := range records {
                        _, err := stepCtx.SpawnTask(ctx, IndexOp{
                            Type:    "insert",
                            IndexID: indexID,
                            Record:  record,
                        })
                        if err != nil {
                            return Summary{}, err
                        }
                        spawned++
                    }
                    
                    if next == "" {
                        break
                    }
                    cursor = next
                }
                
                // Signal no more spawns
                if err := stepCtx.PoolStartDrain(ctx); err != nil {
                    return Summary{}, err
                }
                
                return Summary{Spawned: spawned}, nil
            },
            WithConcurrency(1),  // Only one reindexer
        ),
    
    // Phase 2b: Listen for changes (producer step, same pool)
    NewProducerStep("listen-changes").
        "DependsOn(catbird.Dep[IndexID]("create-index").
        WithTaskPool("index-ops").  // SAME pool!
        WithDrainTimeout(5*time.Minute).
        WithIdleTimeout(30*time.Second).
        WithHandler(
            func(
                ctx context.Context,
                stepCtx StepContext,
                config IndexConfig,
                indexID IndexID,  // Type-safe dependency
            ) (Summary, error) {
                // ProducerStep auto-calls PoolStartDrain on exit
                changes := db.SubscribeChanges(ctx)
                spawned := 0
                
                for change := range changes {
                    _, err := stepCtx.SpawnTask(ctx, IndexOp{
                        Type:    change.Type,
                        IndexID: indexID,
                        Record:  change.Record,
                    })
                    if err != nil {
                        return Summary{}, err
                    }
                    spawned++
                }
                
                return Summary{Spawned: spawned}, nil
            },
        ),
    
    // Phase 3: Converge - wait for both branches + all tasks
    NewStep("switch-index").
        "DependsOn(catbird.Dep[Summary]("reindex-existing").
        "DependsOn(catbird.Dep[Summary]("listen-changes").
        WithTaskPool("index-ops").
        WithHandler(
            func(
                ctx context.Context,
                stepCtx StepContext,
                config IndexConfig,
                reindexResult Summary,   // Type-safe dependency
                listenResult Summary,    // Type-safe dependency
            ) error {
                // Both steps completed and called PoolStartDrain
                // Pool is in DRAINING phase
                // All spawners done, waiting for tasks
                
                if err := stepCtx.WaitTaskPool(ctx); err != nil {
                    return fmt.Errorf("pool drain failed: %w", err)
                }
                
                // All tasks done, safe to switch
                return searchIndex.SwitchActive(ctx, indexID)
            },
        ),
)
```

#### How It Works

**Task pool lifecycle:**
```
1. Flow run starts
   ├─ Create pool instance for "index-ops" in this flow run
   └─ Register pool handlers for spawned tasks


2. Steps execute and spawn to pool
   ├─ reindex-existing spawns 10M tasks
   ├─ listen-changes spawns 1K tasks (concurrently)
   └─ All tasks go to cb_task_pool_{name} with flow_run_id

3. Workers process tasks from pool
   ├─ Poll tasks WHERE flow_run_id = X
   ├─ Execute pool handler
   └─ No association with spawning step needed

4. Dependent step waits for pool
   ├─ Both source steps marked completed
   ├─ Dependent step starts
   ├─ Calls pool.WaitAll() → blocks until pool empty
   └─ Returns, flow continues
```

#### Database Schema

```sql
-- Task pool definition (like queue but for flow-scoped tasks)
CREATE TABLE cb_task_pools (
    name text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Task pool runs (instance per flow run)
CREATE TABLE cb_task_pool_runs (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pool_name text NOT NULL REFERENCES cb_task_pools(name),
    flow_run_id bigint NOT NULL,
    tasks_spawned int NOT NULL DEFAULT 0,
    tasks_completed int NOT NULL DEFAULT 0,
    tasks_failed int NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Tasks in pool (stored in dynamic task tables like cb_t_{flow} but with pool_name metadata)
CREATE TABLE cb_task_pool_tasks (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pool_run_id bigint NOT NULL REFERENCES cb_task_pool_runs(id) ON DELETE CASCADE,
    input jsonb NOT NULL,
    output jsonb,
    error_message text,
    status text NOT NULL DEFAULT 'created',
    deliveries int NOT NULL DEFAULT 0,
    deliver_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    
    CONSTRAINT status_valid CHECK (status IN ('created', 'started', 'completed', 'failed'))
);

CREATE INDEX cb_task_pool_tasks_deliver_at_idx ON cb_task_pool_tasks(deliver_at)
    WHERE status IN ('created', 'started');
CREATE INDEX cb_task_pool_tasks_pool_run_idx ON cb_task_pool_tasks(pool_run_id);
```

#### SQL Functions

```sql
-- Spawn task to pool
CREATE OR REPLACE FUNCTION cb_spawn_pool_task(
    pool_name text,
    flow_run_id bigint,
    input jsonb
)
RETURNS bigint AS $$
DECLARE
    _pool_run_id bigint;
    _task_id bigint;
BEGIN
    -- Get or create pool run
    SELECT id INTO _pool_run_id
    FROM cb_task_pool_runs
    WHERE pool_name = cb_spawn_pool_task.pool_name
      AND flow_run_id = cb_spawn_pool_task.flow_run_id;
    
    IF NOT FOUND THEN
        INSERT INTO cb_task_pool_runs (pool_name, flow_run_id)
        VALUES (cb_spawn_pool_task.pool_name, cb_spawn_pool_task.flow_run_id)
        RETURNING id INTO _pool_run_id;
    END IF;
    
    -- Insert task
    INSERT INTO cb_task_pool_tasks (pool_run_id, input)
    VALUES (_pool_run_id, cb_spawn_pool_task.input)
    RETURNING id INTO _task_id;
    
    -- Increment counter
    UPDATE cb_task_pool_runs
    SET tasks_spawned = tasks_spawned + 1
    WHERE id = _pool_run_id;
    
    RETURN _task_id;
END;
$$;

-- Wait for all pool tasks
CREATE OR REPLACE FUNCTION cb_wait_pool(
    pool_name text,
    flow_run_id bigint,
    poll_interval_ms int DEFAULT 100
)
RETURNS boolean AS $$
DECLARE
    _pool_run_id bigint;
    _pending_count int;
    _failed_count int;
BEGIN
    SELECT id INTO _pool_run_id
    FROM cb_task_pool_runs
    WHERE pool_name = cb_wait_pool.pool_name
      AND flow_run_id = cb_wait_pool.flow_run_id;
    
    IF NOT FOUND THEN
        RETURN true;  -- No pool = no tasks = done
    END IF;
    
    LOOP
        SELECT count(*) INTO _pending_count
        FROM cb_task_pool_tasks
        WHERE pool_run_id = _pool_run_id
          AND status IN ('created', 'started');
        
        IF _pending_count = 0 THEN
            -- Check failures
            SELECT count(*) INTO _failed_count
            FROM cb_task_pool_tasks
            WHERE pool_run_id = _pool_run_id
              AND status = 'failed';
            
            IF _failed_count > 0 THEN
                RAISE EXCEPTION 'cb: % pool tasks failed', _failed_count;
            END IF;
            
            RETURN true;
        END IF;
        
        PERFORM pg_sleep(poll_interval_ms / 1000.0);
    END LOOP;
END;
$$;
```

#### Pros & Cons

**Pros:**
- ✅ Solves diamond pattern perfectly
- ✅ Multiple sources can spawn to same pool
- ✅ Explicit "wait for all tasks" semantic
- ✅ Clear separation: steps vs tasks
- ✅ Pool reusable across flows

**Cons:**
- ❌ New abstraction to learn (task pools)
- ❌ WaitAll blocks worker (same as Approach 2)
- ❌ No task-to-step output mapping (pool outputs not aggregated to step output)
- ❌ Pool tasks can't depend on each other
- ❌ More tables/complexity

---

## Comparison Matrix

| Approach | Unbounded Size | Streaming | Multi-Source | Recursive | Memory Efficient | Complexity |
|----------|---------------|-----------|--------------|-----------|------------------|------------|
| **Generator Steps** | ✅ Yes | ✅ Yes | ❌ No | ❌ No | ✅ Low | Medium |
| **Task Spawning API** | ✅ Yes | ⚠️ Partial | ⚠️ Hacky | ✅ Yes | ⚠️ Medium | High |
| **Cursor/Pagination** | ✅ Yes | ✅ Yes | ❌ No | ❌ No | ✅ Low | Low |
| **Task Pool (Diamond)** | ✅ Yes | ✅ Yes | ✅ Yes | ⚠️ Partial | ✅ Low | High |

## Recommendation: Hybrid Approach

Implement **multiple patterns for different use cases**:

### Pattern 1: PaginatedMapStep (Most Common)

For database pagination, API pagination, large queries:

```go
PaginatedMapStep("index-records", "create-index",
    // Fetch function
    func(ctx context.Context, cursor string) ([]Record, string, error) {
        return db.QueryPage(ctx, cursor, 1000)
    },
    // Process function
    func(ctx context.Context, record Record) error {
        return index.Add(ctx, record)
    },
)
```

**Implementation:** Approach 3 (Cursor/Pagination)
- Simple user API
- Automatic batching
- Handles 99% of "large array" cases

### Pattern 2: GeneratorStep (Custom Streaming)

For complex streaming logic that doesn't fit pagination:

```go
GeneratorStep("process-stream", "source",
    func(ctx context.Context, in Input, yield func(Item) error) error {
        stream := openStream(in.URL)
        defer stream.Close()
        
        for stream.Next() {
            if err := yield(stream.Item()); err != nil {
                return err
            }
        }
        return nil
    },
    func(ctx context.Context, item Item) (Result, error) {
        return process(item)
    },
)
```

**Implementation:** Approach 1 (Generator)
- More flexible than pagination
- Handles streaming sources (websockets, file streams, etc.)

### Pattern 3: TaskPool (Multi-Source Diamond)

For complex coordination patterns:

```go
pool := TaskPool("operations", handler)

flow := NewFlow("complex",
    Step("branch-1", ..., WithTaskPool(pool)),
    Step("branch-2", ..., WithTaskPool(pool)),
    StepWithDeps("converge", Deps("branch-1", "branch-2"),
        func(ctx context.Context, pool TaskPool) error {
            pool.WaitAll(ctx)
            return finalize()
        }),
)
```

**Implementation:** Approach 4 (Task Pool)
- Explicit coordination
- Multiple spawners, single completion point
- Rare but powerful

## Implementation Roadmap

### Phase 1: PaginatedMapStep (MVP)

**Weeks 1-2:**
- Add cursor/pagination support to map steps
- Extend `cb_s_{flow}` with cursor tracking
- Implement `cb_page_tasks()` function
- Add `PaginatedMapStep()` Go API
- Tests: Basic pagination, empty pages, errors

**Value:** Handles 90% of large array use cases

### Phase 2: GeneratorStep

**Weeks 3-4:**
- Add generator state tracking to schema
- Implement `cb_claim_generator_step()`, `cb_spawn_generator_tasks()`
- Add `GeneratorStep()` Go API with channel-based yielding
- Worker loop for running generators
- Tests: Generator exhaustion, errors, concurrent spawning

**Value:** Handles custom streaming logic

### Phase 3: TaskPool (Advanced)

**Weeks 5-6:**
- New tables: `cb_task_pools`, `cb_task_pool_runs`, `cb_task_pool_tasks`
- Implement `cb_spawn_pool_task()`, `cb_wait_pool()`
- Add `TaskPool` Go API
- Worker polling for pool tasks
- Tests: Diamond pattern, multiple spawners, WaitAll

**Value:** Enables complex coordination patterns

### Phase 4: Optimization & Observability

**Weeks 7-8:**
- Dashboard support for paginated/generator steps
- Progress tracking (page N, tasks spawned/completed)
- Generator checkpointing (restart from last yielded)
- Pool task visibility
- Performance tuning for large task counts

## Open Questions & Future Work

1. **Generator checkpointing**: How to resume generation after crash?
   - Option A: Restart from beginning (simple, potentially wasteful)
   - Option B: Checkpoint every N items (complex, requires state serialization)

2. **WaitAll blocking**: Workers blocked on WaitAll are idle
   - Option A: Accept inefficiency (rare case)
   - Option B: Make WaitAll async (step yields, resumes when pool empty)

3. **Task ordering**: Should pools/generators preserve order?
   - Currently: task_index determines output order
   - Alternative: Best-effort ordering, no guarantees

4. **Error handling**: Partial failures in pools?
   - Currently: Any failure → fail entire pool/step
   - Alternative: Collect partial results, expose errors

5. **Recursive termination**: How to detect "all work done" in recursive crawling?
   - Track in-flight count + pending count
   - Quiescence detection: Nothing executing AND queue empty

6. **Memory limits**: What if generator yields faster than workers process?
   - Currently: Go channel buffering (bounded)
   - Alternative: Backpressure API, block yield on full queue

7. **Multiple cursors**: Should pagination support parallel cursors?
   - Example: Partition table, page each partition independently
   - Requires coordinating multiple cursor states

## Conclusion

**For very large/unbounded arrays, catbird needs streaming patterns:**

1. **PaginatedMapStep** - Automatic cursor-based pagination (most common)
2. **GeneratorStep** - Custom streaming via yield pattern (flexible)
3. **TaskPool** - Multi-source coordination for diamond flows (advanced)

These complement the existing **MapStep** design for fixed-size arrays:
- Small arrays (<100): Regular step with in-handler processing
- Medium arrays (100-5000): MapStep
- Large arrays (5000-1M): PaginatedMapStep
- Unbounded/streaming: GeneratorStep
- Multi-source: TaskPool

Each pattern maintains catbird's "database as coordinator" philosophy while addressing distinct scaling challenges.
