# Map Steps Design Document

> **Updated**: Map steps use the **reflection-based builder pattern** from [REFLECTION_API_DESIGN.md](REFLECTION_API_DESIGN.md). Dependencies are declared with `DependsOn(...string)`. Map steps are detected explicitly via proposal markers (e.g., `MapEach(...)`/`Map()`), not by `Dep[T]`.

## Overview

This document outlines the design for adding **map steps** to catbird - a feature that enables dynamic parallel processing of array elements within flows. This is inspired by pgflow's map feature while maintaining catbird's core philosophy of "database as coordinator."

## Motivation

### Current Limitations

Today, processing arrays in catbird flows requires handling all elements within a single step execution:

```go
catbird.NewStep("process_orders").
    DependsOn("fetch_orders").
    Handler(func(ctx context.Context, in Input, orders []Order) ([]OrderResult, error) {
        var results []OrderResult
        for _, order := range orders {
            result, err := processOrder(ctx, order)
            if err != nil {
                return nil, err  // One failure = entire array fails
            }
            results = append(results, result)
        }
        return results, nil
    }, nil)
```

**Problems:**
- **All-or-nothing retries**: If processing order #47 out of 100 fails, all 100 retry
- **No horizontal scaling**: One worker processes the entire array
- **Limited by step timeout**: Can't process large arrays that exceed timeout
- **No per-element visibility**: Can't see progress of individual items in dashboard
- **Sequential processing**: Even with goroutines, limited by single step's resources

### Value Proposition

Map steps enable:

1. **Independent task execution**: Each array element becomes a separate task with its own retry counter
2. **Horizontal scaling**: Multiple workers can process array elements in parallel across machines
3. **Granular failure handling**: Failed elements retry independently without affecting successful ones
4. **Progress visibility**: Dashboard shows individual task completion status
5. **Efficient resource utilization**: Tasks can be distributed across worker pool based on available capacity

### Use Cases

- **Batch API calls**: Process 100 API requests with independent retry logic
- **File processing**: Transform uploaded files in parallel with per-file error handling
- **Bulk notifications**: Send emails/SMS to users with individual delivery tracking
- **ETL pipelines**: Process database records in parallel with checkpoint recovery
- **Data validation**: Validate form submissions with per-item error reports

## Design Goals

1. **Database as coordinator**: All task distribution, tracking, and aggregation happens in PostgreSQL
2. **No extensions required**: Use only standard PostgreSQL features (PL/pgSQL, JSON functions, CTEs)
3. **Concurrent-safe**: Multiple workers can safely process tasks without coordination
4. **Type-safe**: Leverage Go generics for compile-time type checking
5. **Composable**: Map steps work naturally with existing features (conditions, signals, optional dependencies)
6. **Performance**: Optimized for hot path (task polling) vs setup path (flow creation)
7. **Observable**: Tasks visible in dashboard for debugging and monitoring

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Flow Definition (Go Code)                                        │
│                                                                   │
│  NewStep("fetch") → MapEach("process") → NewStep("aggregate")   │
│       │                     │                      │             │
│       └──── []Item ─────────┘                      │             │
│                                                     │             │
│       ┌───── []Result ──────────────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Database State (PostgreSQL)                                      │
│                                                                   │
│  cb_steps:        fetch → process (is_map=true) → aggregate      │
│  Step metadata:   process (array_source='fetch')                 │
│                                                                   │
│  cb_f_myflow:     flow_run#123 (status='started')               │
│  cb_s_myflow:     fetch#1 (completed, output=[...])             │
│                   process#1 (started, waiting for tasks)         │
│                   aggregate#1 (created, waiting for process)     │
│                                                                   │
│  cb_t_myflow:     process#1 task_index=0 (completed)            │
│                   process#1 task_index=1 (started)               │
│                   process#1 task_index=2 (started)               │
│                   ...                                             │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ Worker Processing                                                 │
│                                                                   │
│  Worker 1: Poll task_runs → Process task#0 → Complete           │
│  Worker 2: Poll task_runs → Process task#1 → Complete           │
│  Worker 3: Poll task_runs → Process task#2 → Complete           │
│                                                                   │
│  Last task completes → Aggregate outputs → Mark process done     │
│                      → Start aggregate step                       │
└─────────────────────────────────────────────────────────────────┘
```

### Component Overview

1. **Go API**: Type-safe flow definition with `NewStep(...)` plus map markers (`MapEach(...)` / `Map()`)
2. **PostgreSQL Schema**: Dedicated tables for map metadata and task runs
3. **SQL Functions**: Task spawning, polling, completion, and aggregation
4. **Worker**: Task polling loop and execution (similar to step worker)
5. **Dashboard**: Task-level visibility and progress tracking

## Database Schema

### New Tables

#### `cb_map_steps` - Map Step Metadata

Stores which steps are map steps and their configuration.

```sql
CREATE TABLE cb_map_steps (
    flow_name text NOT NULL,
    step_name text NOT NULL,
    array_source_step text,  -- NULL = map flow input directly
    PRIMARY KEY (flow_name, step_name),
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps(flow_name, name) ON DELETE CASCADE,
    FOREIGN KEY (flow_name, array_source_step) REFERENCES cb_steps(flow_name, name)
);

CREATE INDEX cb_map_steps_flow_idx ON cb_map_steps(flow_name);
```

**Columns:**
- `flow_name`: Flow this map step belongs to
- `step_name`: Name of the map step
- `array_source_step`: Step whose output array to map over (NULL for root maps that use flow input)

**Example data:**
```
flow_name         step_name        array_source_step
---------------------------------------------------------
process_orders    process_order    fetch_orders
batch_emails      send_email       NULL              -- Root map
```

#### `cb_t_{flow}` - Individual Task Executions

Stores runtime state for each array element being processed.

```sql
CREATE TABLE cb_t_myflow (
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    flow_run_id bigint NOT NULL,
    step_name text NOT NULL,
    task_index int NOT NULL,           -- Position in source array (0-based)
    status text NOT NULL DEFAULT 'created',
    input jsonb NOT NULL,               -- Individual array element
    output jsonb,
    error_message text,
    deliveries int NOT NULL DEFAULT 0,
    deliver_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    
    UNIQUE (flow_run_id, step_name, task_index),
    
    CONSTRAINT status_valid CHECK (status IN ('created', 'started', 'completed', 'failed')),
    CONSTRAINT deliveries_valid CHECK (deliveries >= 0),
    CONSTRAINT task_index_valid CHECK (task_index >= 0),
    CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
    CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
    CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
);

-- Hot path indexes: task polling uses these heavily
CREATE INDEX cb_t_myflow_deliver_at_idx ON cb_t_myflow(deliver_at) WHERE status IN ('created', 'started');
CREATE INDEX cb_t_myflow_flow_step_idx ON cb_t_myflow(flow_run_id, step_name);
CREATE INDEX cb_t_myflow_status_idx ON cb_t_myflow(status);

-- Reference flow_run_id to appropriate flow table
-- Note: This will be created dynamically per flow in cb_create_flow()
-- CREATED DYNAMICALLY per flow as cb_t_{flow}
```

**Design note**: Task runs are created dynamically as `cb_t_{flow}` per flow to avoid cross-flow table growth and enable per-flow optimization.

**Columns:**
- `id`: Unique task run identifier
- `flow_run_id`: Which flow execution this task belongs to
- `step_name`: Map step name (multiple tasks share same step_name)
- `task_index`: Position in source array (determines output order)
- `status`: Lifecycle state (created → started → completed/failed)
- `input`: The individual array element (JSON)
- `output`: Result from processing this element
- `error_message`: Failure reason if status='failed'
- `deliveries`: Retry counter (increments on failure)
- `deliver_at`: Visibility timeout for concurrency control
- Timestamps for observability

**Example data:**
```
id  flow_run_id  step_name      task_index  status      input                output
---------------------------------------------------------------------------------------
1   123          process_order  0           completed   {"id":1,"amt":100}   {"success":true}
2   123          process_order  1           started     {"id":2,"amt":200}   NULL
3   123          process_order  2           failed      {"id":3,"amt":50}    NULL
```

### Schema Changes to Existing Tables

#### `cb_s_{flow}` - Add Map Step Tracking

Map steps themselves appear in the step runs table but with special handling:

```sql
-- No schema changes needed!
-- Map step runs use existing columns:
--   - status: 'created' (initial), 'started' (tasks spawning), 'completed' (all tasks done)
--   - output: NULL until all tasks complete, then aggregated array
--   - remaining_dependencies: Works same as regular steps
```

**State transitions:**
1. **created**: Map step waiting for dependencies
2. **started**: Dependencies complete, tasks have been spawned
3. **completed**: All tasks finished successfully, output aggregated
4. **failed**: One or more tasks failed permanently

### Migration Strategy

New migration file: `migrations/00007_map_steps.sql`

```sql
-- +goose up
-- +goose statementbegin

-- Create map steps metadata table
CREATE TABLE cb_map_steps (
    flow_name text NOT NULL,
    step_name text NOT NULL,
    array_source_step text,
    PRIMARY KEY (flow_name, step_name),
    FOREIGN KEY (flow_name, step_name) REFERENCES cb_steps(flow_name, name) ON DELETE CASCADE,
    FOREIGN KEY (flow_name, array_source_step) REFERENCES cb_steps(flow_name, name)
);

CREATE INDEX cb_map_steps_flow_idx ON cb_map_steps(flow_name);

-- Task runs are created dynamically per flow (cb_t_{flow})
-- Template for dynamic creation in cb_create_flow():
--
-- CREATE TABLE cb_t_myflow (
--     -- schema as above
-- );

-- +goose statementend

-- +goose down
-- +goose statementbegin
DROP TABLE IF EXISTS cb_map_steps CASCADE;
-- Drop cb_t_{flow} tables in cb_delete_flow()
-- +goose statementend
```

## SQL Functions

### Setup Functions (Non-Hot Path)

#### `cb_create_flow()` - Enhanced for Map Steps

Modify to handle map step metadata and create task runs table.

```sql
CREATE OR REPLACE FUNCTION cb_create_flow(flow_definition jsonb)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _flow_name text := flow_definition->>'name';
    _steps jsonb := flow_definition->'steps';
    _step jsonb;
    _task_runs_table text;
    _is_map boolean;
    _array_source text;
BEGIN
    -- Existing flow creation logic...
    -- (create flow metadata, steps, dependencies)
    
    -- Process map steps
    FOR _step IN SELECT * FROM jsonb_array_elements(_steps)
    LOOP
        _is_map := COALESCE((_step->>'is_map')::boolean, false);
        
        IF _is_map THEN
            _array_source := _step->>'array_source';
            
            -- Insert map step metadata
            INSERT INTO cb_map_steps (flow_name, step_name, array_source_step)
            VALUES (_flow_name, _step->>'name', _array_source);
        END IF;
    END LOOP;
    
    -- Create task runs table for this flow
    _task_runs_table := cb_table_name(_flow_name, 'task_runs');
    
    EXECUTE format(
        $QUERY$
        CREATE TABLE IF NOT EXISTS %I (
            id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            flow_run_id bigint NOT NULL,
            step_name text NOT NULL,
            task_index int NOT NULL,
            status text NOT NULL DEFAULT 'created',
            input jsonb NOT NULL,
            output jsonb,
            error_message text,
            deliveries int NOT NULL DEFAULT 0,
            deliver_at timestamptz NOT NULL DEFAULT now(),
            created_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz,
            completed_at timestamptz,
            failed_at timestamptz,
            
            UNIQUE (flow_run_id, step_name, task_index),
            FOREIGN KEY (flow_run_id) REFERENCES %I (id) ON DELETE CASCADE,
            
            CONSTRAINT status_valid CHECK (status IN ('created', 'started', 'completed', 'failed')),
            CONSTRAINT deliveries_valid CHECK (deliveries >= 0),
            CONSTRAINT task_index_valid CHECK (task_index >= 0),
            CONSTRAINT completed_at_or_failed_at CHECK (NOT (completed_at IS NOT NULL AND failed_at IS NOT NULL)),
            CONSTRAINT completed_and_output CHECK (NOT (status = 'completed' AND output IS NULL)),
            CONSTRAINT failed_and_error_message CHECK (NOT (status = 'failed' AND (error_message IS NULL OR error_message = '')))
        )
        $QUERY$,
        _task_runs_table,
        cb_table_name(_flow_name, 'f')
    );
    
    -- Create indexes for task polling (hot path)
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (deliver_at) WHERE status IN (''created'', ''started'')',
        _task_runs_table || '_deliver_at_idx', _task_runs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (flow_run_id, step_name)',
        _task_runs_table || '_flow_step_idx', _task_runs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (status)',
        _task_runs_table || '_status_idx', _task_runs_table);
END;
$$;
```

### Runtime Functions (Hot Path)

#### `cb_start_steps()` - Enhanced to Spawn Map Tasks

Modify to detect map steps and spawn tasks when they become ready.

```sql
CREATE OR REPLACE FUNCTION cb_start_steps(flow_name text, flow_run_id bigint)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _f_table text := cb_table_name(cb_start_steps.flow_name, 'f');
    _s_table text := cb_table_name(cb_start_steps.flow_name, 's');
    _task_runs_table text := cb_table_name(cb_start_steps.flow_name, 'task_runs');
    _step_to_process record;
    _is_map_step boolean;
    _array_source text;
    _array_output jsonb;
    _array_length int;
    _flow_input jsonb;
BEGIN
    -- Loop to handle cascading dependency resolution
    LOOP
        -- Find steps ready to execute (existing logic)
        -- ...
        
        FOR _step_to_process IN SELECT ... WHERE remaining_dependencies = 0 AND status = 'created'
        LOOP
            -- Check if this is a map step
            SELECT true, array_source_step
            INTO _is_map_step, _array_source
            FROM cb_map_steps
            WHERE flow_name = cb_start_steps.flow_name
              AND step_name = _step_to_process.step_name;
            
            IF _is_map_step THEN
                -- Get array to map over
                IF _array_source IS NULL THEN
                    -- Root map: use flow input
                    EXECUTE format('SELECT input FROM %I WHERE id = $1', _f_table)
                    INTO _array_output
                    USING cb_start_steps.flow_run_id;
                ELSE
                    -- Dependent map: use source step output
                    EXECUTE format('SELECT output FROM %I WHERE flow_run_id = $1 AND step_name = $2', _s_table)
                    INTO _array_output
                    USING cb_start_steps.flow_run_id, _array_source;
                END IF;
                
                -- Validate it's an array
                IF jsonb_typeof(_array_output) != 'array' THEN
                    -- Fail the flow - expected array but got something else
                    EXECUTE format('UPDATE %I SET status = ''failed'', error_message = $2 WHERE id = $1', _s_table)
                    USING _step_to_process.id, 'Map step expected array input but received ' || jsonb_typeof(_array_output);
                    CONTINUE;
                END IF;
                
                _array_length := jsonb_array_length(_array_output);
                
                -- Empty array optimization: complete step immediately
                IF _array_length = 0 THEN
                    EXECUTE format('UPDATE %I SET status = ''completed'', output = ''[]''::jsonb, completed_at = now() WHERE id = $1', _s_table)
                    USING _step_to_process.id;
                    
                    -- Decrement dependents (existing logic will cascade)
                    -- ...
                    CONTINUE;
                END IF;
                
                -- Spawn tasks for each array element (bulk insert)
                EXECUTE format(
                    $QUERY$
                    INSERT INTO %I (flow_run_id, step_name, task_index, input)
                    SELECT $1, $2, (idx - 1)::int, elem
                    FROM jsonb_array_elements($3) WITH ORDINALITY AS t(elem, idx)
                    $QUERY$,
                    _task_runs_table
                )
                USING cb_start_steps.flow_run_id, _step_to_process.step_name, _array_output;
                
                -- Mark map step as started (waiting for tasks to complete)
                EXECUTE format('UPDATE %I SET status = ''started'', started_at = now() WHERE id = $1', _s_table)
                USING _step_to_process.id;
            ELSE
                -- Regular step: existing activation logic
                -- ...
            END IF;
        END LOOP;
        
        -- Exit loop when no more steps activated
        -- ...
    END LOOP;
END;
$$;
```

#### `cb_read_map_tasks()` - Poll Tasks for Execution

Workers call this to fetch tasks ready for processing.

```sql
CREATE OR REPLACE FUNCTION cb_read_map_tasks(
    flow_name text,
    quantity int,
    hide_for int,
    poll_for int DEFAULT 10000,
    poll_interval int DEFAULT 250
)
RETURNS TABLE (
    id bigint,
    flow_run_id bigint,
    step_name text,
    task_index int,
    deliveries int,
    input jsonb
)
LANGUAGE plpgsql AS $$
DECLARE
    _task_runs_table text := cb_table_name(cb_read_map_tasks.flow_name, 'task_runs');
    _poll_deadline timestamptz := clock_timestamp() + make_interval(secs => cb_read_map_tasks.poll_for / 1000.0);
    _result_count int;
BEGIN
    IF cb_read_map_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    -- Polling loop with backoff
    LOOP
        RETURN QUERY EXECUTE format(
            $QUERY$
            WITH updated AS (
                UPDATE %I t
                SET status = CASE 
                        WHEN t.status = 'created' THEN 'started'
                        ELSE t.status 
                    END,
                    started_at = CASE 
                        WHEN t.status = 'created' THEN now()
                        ELSE t.started_at
                    END,
                    deliveries = t.deliveries + 1,
                    deliver_at = clock_timestamp() + $2
                WHERE t.id IN (
                    SELECT t2.id
                    FROM %I t2
                    WHERE t2.deliver_at <= clock_timestamp()
                      AND t2.status IN ('created', 'started')
                    ORDER BY t2.deliver_at
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING t.id, t.flow_run_id, t.step_name, t.task_index, t.deliveries, t.input
            )
            SELECT * FROM updated
            $QUERY$,
            _task_runs_table, _task_runs_table
        )
        USING cb_read_map_tasks.quantity,
              make_interval(secs => cb_read_map_tasks.hide_for / 1000.0);
        
        GET DIAGNOSTICS _result_count = ROW_COUNT;
        
        IF _result_count > 0 OR clock_timestamp() >= _poll_deadline THEN
            RETURN;
        END IF;
        
        -- Sleep before next poll
        PERFORM pg_sleep(cb_read_map_tasks.poll_interval / 1000.0);
    END LOOP;
END;
$$;
```

**Performance notes:**
- Uses `FOR UPDATE SKIP LOCKED` for lock-free concurrent polling
- Bulk updates in single CTE (no N+1 queries)
- Indexed on `deliver_at` for fast visibility timeout filtering
- Poll-and-sleep pattern prevents thundering herd

#### `cb_hide_map_tasks()` - Retry Failed Tasks

Re-hide tasks that failed temporarily (e.g., network error, rate limit).

```sql
CREATE OR REPLACE FUNCTION cb_hide_map_tasks(
    flow_name text,
    ids bigint[],
    hide_for int
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _task_runs_table text := cb_table_name(cb_hide_map_tasks.flow_name, 'task_runs');
BEGIN
    IF cb_hide_map_tasks.hide_for <= 0 THEN
        RAISE EXCEPTION 'cb: hide_for must be greater than 0';
    END IF;

    EXECUTE format(
        $QUERY$
        UPDATE %I
        SET deliver_at = (clock_timestamp() + $2)
        WHERE id = ANY($1)
        $QUERY$,
        _task_runs_table
    )
    USING cb_hide_map_tasks.ids,
          make_interval(secs => cb_hide_map_tasks.hide_for / 1000.0);
END;
$$;
```

#### `cb_complete_map_task()` - Mark Task Complete and Aggregate

Called when a task finishes successfully. Aggregates if all tasks complete.

```sql
CREATE OR REPLACE FUNCTION cb_complete_map_task(
    flow_name text,
    task_id bigint,
    output jsonb
)
RETURNS void
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    _task_runs_table text := cb_table_name(cb_complete_map_task.flow_name, 'task_runs');
    _s_table text := cb_table_name(cb_complete_map_task.flow_name, 's');
    _f_table text := cb_table_name(cb_complete_map_task.flow_name, 'f');
    _flow_run_id bigint;
    _step_name text;
    _total_tasks int;
    _completed_tasks int;
    _failed_tasks int;
    _aggregated_output jsonb;
    _step_run_id bigint;
BEGIN
    -- Mark this task completed
    EXECUTE format(
        $QUERY$
        UPDATE %I
        SET status = 'completed',
            completed_at = now(),
            output = $2
        WHERE id = $1
          AND status = 'started'
        RETURNING flow_run_id, step_name
        $QUERY$,
        _task_runs_table
    )
    INTO _flow_run_id, _step_name
    USING cb_complete_map_task.task_id, cb_complete_map_task.output;
    
    IF _flow_run_id IS NULL THEN
        RETURN; -- Task not found or already completed
    END IF;
    
    -- Check task completion status
    EXECUTE format(
        $QUERY$
        SELECT count(*),
               count(*) FILTER (WHERE status = 'completed'),
               count(*) FILTER (WHERE status = 'failed')
        FROM %I
        WHERE flow_run_id = $1
          AND step_name = $2
        $QUERY$,
        _task_runs_table
    )
    INTO _total_tasks, _completed_tasks, _failed_tasks
    USING _flow_run_id, _step_name;
    
    -- Check if any tasks failed permanently
    IF _failed_tasks > 0 THEN
        -- Fail the entire map step
        EXECUTE format(
            $QUERY$
            UPDATE %I
            SET status = 'failed',
                failed_at = now(),
                error_message = $3
            WHERE flow_run_id = $1
              AND step_name = $2
              AND status = 'started'
            RETURNING id
            $QUERY$,
            _s_table
        )
        INTO _step_run_id
        USING _flow_run_id, _step_name, 
              format('%s task(s) failed permanently', _failed_tasks);
        
        IF _step_run_id IS NOT NULL THEN
            -- Fail the flow
            EXECUTE format(
                $QUERY$
                UPDATE %I
                SET status = 'failed',
                    failed_at = now(),
                    error_message = $2
                WHERE id = $1
                  AND status = 'started'
                $QUERY$,
                _f_table
            )
            USING _flow_run_id,
                  format('Map step "%s" failed', _step_name);
        END IF;
        
        RETURN;
    END IF;
    
    -- Check if all tasks completed successfully
    IF _total_tasks = _completed_tasks THEN
        -- Aggregate outputs into array preserving order
        EXECUTE format(
            $QUERY$
            SELECT jsonb_agg(output ORDER BY task_index)
            FROM %I
            WHERE flow_run_id = $1
              AND step_name = $2
            $QUERY$,
            _task_runs_table
        )
        INTO _aggregated_output
        USING _flow_run_id, _step_name;
        
        -- Mark map step completed
        EXECUTE format(
            $QUERY$
            UPDATE %I
            SET status = 'completed',
                completed_at = now(),
                output = $3
            WHERE flow_run_id = $1
              AND step_name = $2
              AND status = 'started'
            RETURNING id
            $QUERY$,
            _s_table
        )
        INTO _step_run_id
        USING _flow_run_id, _step_name, _aggregated_output;
        
        IF _step_run_id IS NOT NULL THEN
            -- Decrement flow's remaining_steps
            EXECUTE format(
                $QUERY$
                UPDATE %I
                SET remaining_steps = remaining_steps - 1
                WHERE id = $1
                $QUERY$,
                _f_table
            )
            USING _flow_run_id;
            
            -- Decrement dependent steps' remaining_dependencies
            EXECUTE format(
                $QUERY$
                UPDATE %I
                SET remaining_dependencies = remaining_dependencies - 1
                WHERE flow_run_id = $1
                  AND step_name IN (
                      SELECT step_name
                      FROM cb_step_dependencies
                      WHERE flow_name = $2
                        AND dependency_name = $3
                  )
                $QUERY$,
                _s_table
            )
            USING _flow_run_id, cb_complete_map_task.flow_name, _step_name;
            
            -- Trigger dependent steps to start
            PERFORM cb_start_steps(cb_complete_map_task.flow_name, _flow_run_id);
            
            -- Check if flow completed
            EXECUTE format(
                $QUERY$
                UPDATE %I f
                SET status = 'completed',
                    completed_at = now(),
                    output = (
                        SELECT jsonb_object_agg(step_name, output)
                        FROM %I
                        WHERE flow_run_id = f.id
                          AND status = 'completed'
                    )
                WHERE f.id = $1
                  AND f.remaining_steps = 0
                  AND f.status = 'started'
                $QUERY$,
                _f_table, _s_table
            )
            USING _flow_run_id;
        END IF;
    END IF;
END;
$$;
```

**Key logic:**
- Atomic completion check (count completed/failed tasks)
- If any task failed → fail entire map step → fail flow
- If all tasks completed → aggregate → mark step done → cascade dependencies
- Order preservation via `ORDER BY task_index`

#### `cb_fail_map_task()` - Mark Task as Permanently Failed

```sql
CREATE OR REPLACE FUNCTION cb_fail_map_task(
    flow_name text,
    task_id bigint,
    error_message text
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    _task_runs_table text := cb_table_name(cb_fail_map_task.flow_name, 'task_runs');
BEGIN
    EXECUTE format(
        $QUERY$
        UPDATE %I
        SET status = 'failed',
            failed_at = now(),
            error_message = $2
        WHERE id = $1
          AND status = 'started'
        $QUERY$,
        _task_runs_table
    )
    USING cb_fail_map_task.task_id, cb_fail_map_task.error_message;
    
    -- cb_complete_map_task will detect failed tasks and fail the step
    PERFORM cb_complete_map_task(cb_fail_map_task.flow_name, cb_fail_map_task.task_id, NULL);
END;
$$;
```

## Go API

### Design Pattern: Reflection-Based Builders

Map steps use the same **reflection-based builder pattern** as all flow steps (from [REFLECTION_API_DESIGN.md](REFLECTION_API_DESIGN.md)), with array support:

```go
// All flow steps use reflection-based builders: NewStep, DependsOn, Handler, etc.
// Map steps extend this with explicit map markers (MapEach / MapInput)

// Simple map step (no dependencies - operate on flow input array)
mapStep1 := catbird.NewStep("process-items").
    Map().
    Handler(
        func(
            ctx context.Context,
            item Item,  // Each element from input array
        ) (Result, error) {
            return processItem(ctx, item)
        },
        &catbird.HandlerOpts{Concurrency: 10},  // Process 10 items in parallel
    )

// Map step with dependency (operate on dependency output array)
mapStep2 := catbird.NewStep("transform").
    DependsOn("fetch-records").
    MapEach("fetch-records").
    Handler(
        func(
            ctx context.Context,
            record Record,  // Each record from the array
        ) (Transformed, error) {
            return transform(ctx, record)
        },
        &catbird.HandlerOpts{
            Concurrency:    20,
            MaxRetries:     3,
            CircuitBreaker: catbird.NewCircuitBreaker(5, 30*time.Second),
        },
    )

// Map step with signal and condition
mapStep3 := catbird.NewStep("validate-with-approval").
    DependsOn("collect-submissions").
    MapEach("collect-submissions").
    Signal(true).
    Condition("input.requires_review").
    Handler(
        func(
            ctx context.Context,
            submission FormSubmission,
            approval ApprovalSig,  // Injected from Signal
        ) (ValidationResult, error) {
            return validateSubmission(ctx, submission, approval)
        },
        nil,
    )
```

### Array Detection

Map steps are detected explicitly using map markers:

```go
// Mark a step as a map step by pointing at the array source
step := NewStep("process").
    DependsOn("fetch-orders").
    MapEach("fetch-orders").
    Handler(func(ctx context.Context, order Order) (Result, error) { ... }, nil)

// Handler signature analysis (via reflection) validates array processing:
// - Source step output is []Order at runtime
// - Handler accepts single Order
// - Runtime wrapper spawns tasks for each element

// Framework handles:
// - Spawning individual tasks for each array element
// - Parallel execution (bounded by HandlerOpts.Concurrency)
// - Independent retry per element
// - Result aggregation (preserves input order)
```

### Proposed Builder Additions (Not Yet Implemented)

These are proposal-only helpers to support map-step behavior while keeping the current builder style:

- `MapEach(sourceStep string)`: mark a step as a map step and declare which dependency provides the array.
- `Map()`: mark a step as mapping directly over the flow input array.
- `Reduce(fn)`: optional reducer to transform the aggregated `[]Out` into a different final output.
- `WithPriority(fn)`: optional per-item priority hook for polling order.
- `WithProgressCallback(fn)`: optional progress hook for UI/telemetry.

If adopted, these methods should be implemented as pure builder metadata (no runtime side effects) and serialized into the flow definition alongside `condition`, `signal`, and `depends_on`.

### Type Definitions (Internal)

For reference, the internal types used:

```go
// MapTaskMessage: internal representation of a task
type mapTaskMessage struct {
    ID         int64           `json:"id"`
    FlowRunID  int64           `json:"flow_run_id"`
    StepName   string          `json:"step_name"`
    TaskIndex  int             `json:"task_index"`   // Position in array (for ordering)
    Deliveries int             `json:"deliveries"`  // Retry counter
    Input      json.RawMessage `json:"input"`       // One array element
}

// mapHandler: wraps the user's element processing function
type mapHandler struct {
    handlerOpts
    fn func(context.Context, mapTaskMessage) ([]byte, error)
}
```

#### Flow Serialization

```go
// Extend Step.MarshalJSON to include map metadata
func (s *Step) MarshalJSON() ([]byte, error) {
    type StepJSON struct {
        Name            string            `json:"name"`
        IsMapStep       bool              `json:"is_map,omitempty"`
        ArraySourceStep string            `json:"array_source,omitempty"`
        Condition       string            `json:"condition,omitempty"`
        Signal       bool              `json:"signal"`
        DependsOn       []*StepDependency `json:"depends_on,omitempty"`
    }
    
    return json.Marshal(StepJSON{
        Name:            s.Name,
        IsMapStep:       s.IsMapStep,
        ArraySourceStep: s.ArraySourceStep,
        Condition:       s.Condition,
        Signal:       s.Signal,
        DependsOn:       s.DependsOn,
    })
}
```

### Worker Implementation

#### Map Task Worker

```go
// mapTaskWorker processes map tasks for a specific step
type mapTaskWorker struct {
    conn     Conn
    logger   *slog.Logger
    flowName string
    step     *MapStep
    
    inFlight   map[int64]struct{}
    inFlightMu sync.Mutex
}

func newMapTaskWorker(conn Conn, logger *slog.Logger, flowName string, step *MapStep) *mapTaskWorker {
    return &mapTaskWorker{
        conn:     conn,
        logger:   logger,
        flowName: flowName,
        step:     step,
        inFlight: make(map[int64]struct{}),
    }
}

func (w *mapTaskWorker) start(shutdownCtx, handlerCtx context.Context, wg *WaitGroup) {
    messages := make(chan mapTaskMessage)
    
    // Producer: poll for tasks
    wg.Go(func() {
        defer close(messages)
        
        retryAttempt := 0
        
        for {
            select {
            case <-shutdownCtx.Done():
                return
            default:
            }
            
            msgs, err := w.readMapTasks(shutdownCtx)
            if err != nil {
                if errors.Is(err, context.Canceled) {
                    return
                }
                
                w.logger.ErrorContext(shutdownCtx, "worker: cannot read map tasks",
                    "flow", w.flowName, "step", w.step.Name, "error", err)
                
                delay := backoffWithFullJitter(retryAttempt, 250*time.Millisecond, 5*time.Second)
                retryAttempt++
                timer := time.NewTimer(delay)
                select {
                case <-shutdownCtx.Done():
                    timer.Stop()
                    return
                case <-timer.C:
                }
                continue
            }
            
            retryAttempt = 0
            
            if len(msgs) == 0 {
                continue
            }
            
            w.markInFlight(msgs)
            
            for _, msg := range msgs {
                select {
                case <-shutdownCtx.Done():
                    return
                case messages <- msg:
                }
            }
        }
    })
    
    // Consumers: process tasks with concurrency control
    concurrency := w.step.Handler.handlerOpts.concurrency
    for i := 0; i < concurrency; i++ {
        wg.Go(func() {
            for msg := range messages {
                w.processTask(handlerCtx, msg)
                w.unmarkInFlight(msg.ID)
            }
        })
    }
}

func (w *mapTaskWorker) readMapTasks(ctx context.Context) ([]mapTaskMessage, error) {
    quantity := w.step.Handler.batchSize
    if quantity == 0 {
        quantity = 10 // Default batch size
    }
    
    hideFor := 30000 // 30 seconds
    pollFor := 10000 // 10 seconds
    pollInterval := 250 // 250ms
    
    rows, err := w.conn.Query(ctx,
        "SELECT * FROM cb_read_map_tasks($1, $2, $3, $4, $5)",
        w.flowName, quantity, hideFor, pollFor, pollInterval)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var msgs []mapTaskMessage
    for rows.Next() {
        var msg mapTaskMessage
        err := rows.Scan(&msg.ID, &msg.FlowRunID, &msg.StepName, 
            &msg.TaskIndex, &msg.Deliveries, &msg.Input)
        if err != nil {
            return nil, err
        }
        msgs = append(msgs, msg)
    }
    
    return msgs, rows.Err()
}

func (w *mapTaskWorker) processTask(ctx context.Context, msg mapTaskMessage) {
    logger := w.logger.With(
        "flow", w.flowName,
        "step", w.step.Name,
        "task_id", msg.ID,
        "task_index", msg.TaskIndex,
        "delivery", msg.Deliveries,
    )
    
    // Check circuit breaker
    if w.step.Handler.circuitBreaker != nil {
        if err := w.step.Handler.circuitBreaker.Call(ctx, func() error {
            return w.executeTask(ctx, msg, logger)
        }); err != nil {
            logger.ErrorContext(ctx, "worker: circuit breaker open", "error", err)
            // Circuit open - hide task for retry
            w.hideTask(ctx, msg.ID)
        }
    } else {
        _ = w.executeTask(ctx, msg, logger)
    }
}

func (w *mapTaskWorker) executeTask(ctx context.Context, msg mapTaskMessage, logger *slog.Logger) error {
    // Execute handler
    output, err := w.step.Handler.fn(ctx, msg)
    
    if err != nil {
        logger.ErrorContext(ctx, "worker: map task handler error", "error", err)
        
        // Check retry limit
        if msg.Deliveries >= w.step.Handler.maxRetries {
            logger.ErrorContext(ctx, "worker: map task max retries exceeded")
            // Permanently fail task
            if failErr := w.failTask(ctx, msg.ID, err.Error()); failErr != nil {
                logger.ErrorContext(ctx, "worker: cannot fail map task", "error", failErr)
            }
            return err
        }
        
        // Calculate backoff for retry
        delay := backoffWithFullJitter(
            msg.Deliveries-1,
            w.step.Handler.backoffMin,
            w.step.Handler.backoffMax,
        )
        
        logger.InfoContext(ctx, "worker: retrying map task", "retry_after", delay)
        
        // Hide for retry
        if hideErr := w.hideTaskFor(ctx, msg.ID, delay); hideErr != nil {
            logger.ErrorContext(ctx, "worker: cannot hide map task", "error", hideErr)
        }
        
        return err
    }
    
    // Success - complete task
    logger.InfoContext(ctx, "worker: map task completed")
    
    if err := w.completeTask(ctx, msg.ID, output); err != nil {
        logger.ErrorContext(ctx, "worker: cannot complete map task", "error", err)
        return err
    }
    
    return nil
}

func (w *mapTaskWorker) completeTask(ctx context.Context, taskID int64, output []byte) error {
    _, err := w.conn.Exec(ctx,
        "SELECT cb_complete_map_task($1, $2, $3)",
        w.flowName, taskID, output)
    return err
}

func (w *mapTaskWorker) failTask(ctx context.Context, taskID int64, errorMsg string) error {
    _, err := w.conn.Exec(ctx,
        "SELECT cb_fail_map_task($1, $2, $3)",
        w.flowName, taskID, errorMsg)
    return err
}

func (w *mapTaskWorker) hideTask(ctx context.Context, taskID int64) error {
    return w.hideTaskFor(ctx, taskID, 30*time.Second)
}

func (w *mapTaskWorker) hideTaskFor(ctx context.Context, taskID int64, duration time.Duration) error {
    _, err := w.conn.Exec(ctx,
        "SELECT cb_hide_map_tasks($1, $2, $3)",
        w.flowName, []int64{taskID}, int(duration.Milliseconds()))
    return err
}

func (w *mapTaskWorker) markInFlight(msgs []mapTaskMessage) {
    w.inFlightMu.Lock()
    defer w.inFlightMu.Unlock()
    for _, msg := range msgs {
        w.inFlight[msg.ID] = struct{}{}
    }
}

func (w *mapTaskWorker) unmarkInFlight(id int64) {
    w.inFlightMu.Lock()
    defer w.inFlightMu.Unlock()
    delete(w.inFlight, id)
}
```

#### Worker Integration

```go
// Enhanced Worker.Start() to detect map steps
func (w *Worker) Start(ctx context.Context) error {
    // ... existing initialization ...
    
    for _, flow := range w.flows {
        for _, step := range flow.Steps {
            if step.IsMapStep {
                // Start map task worker
                mtw := newMapTaskWorker(w.conn, w.logger, flow.Name, step)
                w.wg.Go(func() {
                    mtw.start(w.shutdownCtx, w.handlerCtx, &w.wg)
                })
            } else {
                // Start regular step worker (existing code)
                sw := newStepWorker(w.conn, w.logger, flow.Name, step)
                w.wg.Go(func() {
                    sw.start(w.shutdownCtx, w.handlerCtx, &w.wg)
                })
            }
        }
    }
    
    // ... existing scheduler, GC, heartbeat ...
    
    return nil
}
```

## Usage Examples

### Basic Map Step

```go
// Process uploaded files in parallel
flow := catbird.NewFlow("file-processor").
    AddStep(catbird.NewStep("upload").
        Handler(func(ctx context.Context, req UploadRequest) ([]File, error) {
            return req.Files, nil
        }, nil)).
    AddStep(catbird.NewStep("process-file").
        DependsOn("upload").
        MapEach("upload").
        Handler(
            func(ctx context.Context, req UploadRequest, file File) (FileResult, error) {
                // Process each file independently
                data, err := os.ReadFile(file.Path)
                if err != nil {
                    return FileResult{}, err
                }

                checksum := sha256.Sum256(data)

                return FileResult{
                    Name:     file.Name,
                    Size:     len(data),
                    Checksum: hex.EncodeToString(checksum[:]),
                }, nil
            },
            &catbird.HandlerOpts{
                Concurrency: 5, // Process 5 files at a time
                MaxRetries:  3, // Retry failed files up to 3 times
            },
        )).
    AddStep(catbird.NewStep("summary").
        DependsOn("process-file").
        Handler(
            func(ctx context.Context, req UploadRequest, results []FileResult) (Summary, error) {
                var totalSize int
                for _, r := range results {
                    totalSize += r.Size
                }
                return Summary{
                    FileCount: len(results),
                    TotalSize: totalSize,
                }, nil
            }, nil))

// Run it
client := catbird.New(conn)
handle, err := client.RunFlow(ctx, "file-processor", UploadRequest{
    Files: []File{
        {Name: "doc1.pdf", Path: "/tmp/doc1.pdf"},
        {Name: "doc2.pdf", Path: "/tmp/doc2.pdf"},
        {Name: "doc3.pdf", Path: "/tmp/doc3.pdf"},
    },
}, nil)

var output Summary
err = handle.WaitForOutput(ctx, &output)
// output: Summary{FileCount: 3, TotalSize: 45600}
```

### Map with API Calls

```go
// Batch email sending with circuit breaker protection
flow := catbird.NewFlow("email-campaign").
    AddStep(catbird.NewStep("get-subscribers").
        Handler(func(ctx context.Context, campaignID string) ([]Subscriber, error) {
            return db.GetActiveSubscribers(ctx, campaignID)
        }, nil)).
    AddStep(catbird.NewStep("send-email").
        DependsOn("get-subscribers").
        MapEach("get-subscribers").
        Handler(
            func(ctx context.Context, campaignID string, sub Subscriber) (EmailResult, error) {
                // Circuit breaker: fail fast if email service is down
                return emailService.Send(ctx, sub.Email)
            },
            &catbird.HandlerOpts{
                Concurrency:    20,
                MaxRetries:     2,
                CircuitBreaker: catbird.NewCircuitBreaker(10, 30*time.Second),
            },
        )).
    AddStep(catbird.NewStep("report").
        DependsOn("send-email").
        Handler(
            func(ctx context.Context, campaignID string, results []EmailResult) (Report, error) {
                sent, failed := 0, 0
                for _, r := range results {
                    if r.Success {
                        sent++
                    } else {
                        failed++
                    }
                }
                return Report{
                    Total:  len(results),
                    Sent:   sent,
                    Failed: failed,
                }, nil
            }, nil,
        ))
```

### Root Map (Map Flow Input)

```go
// Process array passed directly as flow input
flow := catbird.NewFlow("batch-validator").
    AddStep(catbird.NewStep("validate").
        Map().
        Handler(func(ctx context.Context, all []FormData, data FormData) (ValidationResult, error) {
            errors := validateForm(data)
            return ValidationResult{
                ID:     data.ID,
                Valid:  len(errors) == 0,
                Errors: errors,
            }, nil
        }, nil))

// Run with array input
handle, err := client.RunFlow(ctx, "batch-validator", []FormData{
    {ID: 1, Name: "Alice", Email: "alice@example.com"},
    {ID: 2, Name: "Bob", Email: "invalid-email"},
}, nil)
var results []ValidationResult
err = handle.WaitForOutput(ctx, &results)
// results: []ValidationResult{
//     {ID: 1, Valid: true, Errors: []},
//     {ID: 2, Valid: false, Errors: ["invalid email"]},
// }
```

### Multiple Map Steps

```go
// Multiple independent maps operating in parallel
flow := catbird.NewFlow("data-pipeline").
    AddStep(catbird.NewStep("extract").
        Handler(func(ctx context.Context, query string) (DataSet, error) {
            return DataSet{
                Users:    fetchUsers(query),
                Orders:   fetchOrders(query),
                Products: fetchProducts(query),
            }, nil
        }, nil)).
    AddStep(catbird.NewStep("split_users").
        DependsOn("extract").
        Handler(func(ctx context.Context, q string, ds DataSet) ([]User, error) {
            return ds.Users, nil
        }, nil)).
    AddStep(catbird.NewStep("split_orders").
        DependsOn("extract").
        Handler(func(ctx context.Context, q string, ds DataSet) ([]Order, error) {
            return ds.Orders, nil
        }, nil)).
    AddStep(catbird.NewStep("enrich_user").
        DependsOn("split_users").
        MapEach("split_users").
        Handler(func(ctx context.Context, q string, user User) (EnrichedUser, error) {
            return enrichUserData(ctx, user)
        }, nil)).
    AddStep(catbird.NewStep("enrich_order").
        DependsOn("split_orders").
        MapEach("split_orders").
        Handler(func(ctx context.Context, q string, order Order) (EnrichedOrder, error) {
            return enrichOrderData(ctx, order)
        }, nil)).
    AddStep(catbird.NewStep("combine").
        DependsOn("enrich_user", "enrich_order").
        Handler(func(ctx context.Context, q string, users []EnrichedUser, orders []EnrichedOrder) (Report, error) {
            return generateReport(users, orders), nil
        }, nil))
```

### Conditional Map Step

```go
// Skip expensive processing for non-premium users
flow := catbird.NewFlow("order-processor").
    AddStep(catbird.NewStep("get_order").
        Handler(func(ctx context.Context, orderID string) (Order, error) {
            return db.GetOrder(ctx, orderID)
        }, nil)).
    AddStep(catbird.NewStep("get_items").
        DependsOn("get_order").
        Handler(func(ctx context.Context, id string, order Order) ([]OrderItem, error) {
            return order.Items, nil
        }, nil)).
    AddStep(catbird.NewStep("apply_premium_discount").
        DependsOn("get_items").
        MapEach("get_items").
        Condition("get_order.is_premium").
        Handler(func(ctx context.Context, id string, item OrderItem) (OrderItem, error) {
            item.Price = item.Price * 0.9  // 10% discount
            return item, nil
        }, nil)).
    AddStep(catbird.NewStep("finalize").
        DependsOn("get_order", "apply_premium_discount").
        Handler(func(ctx context.Context, id string, order Order, items catbird.Optional[[]OrderItem]) (Invoice, error) {
            finalItems := order.Items
            if items.IsSet {
                finalItems = items.Value
            }
            return generateInvoice(finalItems), nil
        }, nil))
```

## Edge Cases and Error Handling

### Empty Arrays

**Scenario**: Source step returns `[]`

**Behavior**:
- No tasks spawned
- Map step marked `completed` immediately with output `[]`
- Dependent steps activated in same transaction
- Zero overhead (no task rows created)

### Type Mismatches

**Scenario**: Source step returns non-array (e.g., `{"status": "ok"}`)

**Behavior**:
- `cb_start_steps()` detects `jsonb_typeof() != 'array'`
- Map step marked `failed` with error: "Map step expected array input but received object"
- Flow fails immediately

### Partial Failures

**Scenario**: 97/100 tasks succeed, 3 fail after max retries

**Behavior**:
- Failed tasks marked `status='failed'` in `cb_t_{flow}`
- `cb_complete_map_task()` detects `_failed_tasks > 0`
- Map step marked `failed` with message: "3 task(s) failed permanently"
- Flow marked `failed`
- No partial results exposed (all-or-nothing semantics)

### Task Retry Exhaustion

**Scenario**: Task fails 3 times (max retries = 3)

**Flow**:
1. Attempt 1: Handler returns error → hide for retry (2s backoff)
2. Attempt 2: Handler returns error → hide for retry (4s backoff)
3. Attempt 3: Handler returns error → `deliveries >= maxRetries`
4. Call `cb_fail_map_task()` → mark task `failed`
5. `cb_complete_map_task()` detects failure → fail map step → fail flow

### Circuit Breaker Activation

**Scenario**: External API down, 5 consecutive tasks fail

**Flow**:
1. Tasks 1-5: All fail quickly due to API timeout
2. Circuit breaker opens after 5th failure
3. Subsequent tasks: Fail immediately with "circuit open" error
4. Worker hides tasks for retry
5. After circuit cooldown (e.g., 60s), circuit transitions to half-open
6. Next task succeeds → circuit closes, normal processing resumes

### Concurrent Task Completion

**Scenario**: Two tasks complete simultaneously on different workers

**Behavior**:
- Both workers call `cb_complete_map_task()`
- PostgreSQL transaction isolation ensures atomic aggregation check
- One transaction sees "99/100 complete" → waits
- Other transaction sees "100/100 complete" → aggregates outputs
- No duplicate aggregation (idempotent due to transaction isolation)

### Worker Crash During Task Execution

**Scenario**: Worker crashes while processing task

**Recovery**:
1. Task remains in `status='started'` with `deliver_at` in past
2. Visibility timeout expires (30s)
3. Another worker polls and gets same task (via `FOR UPDATE SKIP LOCKED`)
4. Task `deliveries` increments, processing retries
5. Original worker's partial work discarded

## Performance Considerations

### Scalability Limits

**Small arrays (1-50 items)**:
- Negligible overhead
- Single worker can process entire array quickly
- Consider skipping map step and using in-handler goroutines

**Medium arrays (50-500 items)**:
- Sweet spot for map steps
- Horizontal scaling provides clear benefit
- Per-task retries valuable for flaky operations

**Large arrays (500-5000 items)**:
- Bulk spawning performs well (single INSERT with unnest)
- Index on `deliver_at` keeps polling fast
- Consider chunking for very large arrays

**Very large arrays (5000+ items)**:
- Consider pagination or streaming pattern instead
- Creating 10K task rows may cause table bloat
- Monitor `cb_t_{flow}` table size and vacuum frequency

### Indexing Strategy

**Hot path indexes** (created automatically):
```sql
CREATE INDEX ON cb_t_{flow} (deliver_at) WHERE status IN ('created', 'started');
CREATE INDEX ON cb_t_{flow} (flow_run_id, step_name);
CREATE INDEX ON cb_t_{flow} (status);
```

**Query patterns**:
- Task polling: `WHERE deliver_at <= now() AND status IN ('created', 'started')` → uses `deliver_at` index
- Aggregation check: `WHERE flow_run_id = X AND step_name = Y` → uses `flow_step_idx`
- Dashboard queries: `WHERE status = 'failed'` → uses `status_idx`

### Optimization Techniques

1. **Batch task spawning**: Single INSERT with `jsonb_array_elements()` (not N inserts)
2. **Lock-free polling**: `FOR UPDATE SKIP LOCKED` prevents worker contention
3. **Bulk aggregation**: `jsonb_agg(output ORDER BY task_index)` in single query
4. **Visibility timeout**: Prevents duplicate processing without explicit locks
5. **Partial index on deliver_at**: Only indexes tasks eligible for polling

### Resource Usage

**Database**:
- 100-element array → 100 rows in `cb_t_{flow}`
- Each row: ~200 bytes (ID, indexes, JSON payload)
- Total: ~20KB + index overhead
- Cleaned up on flow deletion (cascade)

**Workers**:
- Each map step: One goroutine per worker per flow
- Concurrency controlled via handler option (default: 1)
- Memory: Message batch size × JSON payload size

**Network**:
- Polling queries: ~1KB per batch request
- Task completion: ~1KB per task (depends on output size)
- Minimal overhead compared to task execution time

## Dashboard Integration

### Task Visibility

**Flow detail page** should show:
```
Flow Run #123: process-orders
Status: Started
Progress: 47/100 tasks completed

Steps:
  ✓ fetch_orders (completed in 2.3s)
  ⟳ process_order (map step, 47/100 tasks)
    ├─ Completed: 47
    ├─ Started: 12
    ├─ Created: 39
    ├─ Failed: 2
    └─ [View Tasks] → Drill down to task list
  ⏸ generate_invoice (waiting)
```

**Task list page** (`/flows/{name}/runs/{id}/steps/{step}/tasks`):
```
Map Step: process_order
Tasks: 100

[Filter: All | Completed | Failed | Started]

Task #   Status      Input                    Output      Duration  Deliveries
-----------------------------------------------------------------------------------
0        Completed   {"order_id": 1}         {...}       1.2s      1
1        Completed   {"order_id": 2}         {...}       0.8s      1
2        Failed      {"order_id": 3}         [error]     5.0s      3
3        Started     {"order_id": 4}         ...         ...       2
...
```

**Queries**:
```sql
-- Get task summary
SELECT status, count(*)
FROM cb_t_process_orders
WHERE flow_run_id = 123 AND step_name = 'process_order'
GROUP BY status;

-- Get task details
SELECT id, task_index, status, input, output, error_message, deliveries,
       extract(epoch from (completed_at - started_at)) as duration_sec
FROM cb_t_process_orders
WHERE flow_run_id = 123 AND step_name = 'process_order'
ORDER BY task_index;
```

## Migration Path

### Phase 1: Core Implementation (MVP)

**Goal**: Basic map step functionality working end-to-end

**Deliverables**:
1. Migration `00007_map_steps.sql`
2. `cb_map_steps` table and task tracking
3. Enhanced `cb_create_flow()` for map metadata
4. Enhanced `cb_start_steps()` for task spawning
5. New functions: `cb_read_map_tasks()`, `cb_complete_map_task()`, `cb_fail_map_task()`
6. Go API: `NewStep(...)` plus `MapEach(...)` / `Map()` markers
7. Worker: `mapTaskWorker` implementation
8. Tests: Basic map step flow execution
9. Documentation: README examples

**Limitations**:
- No dashboard visibility (tasks invisible in UI)
- No condition support on map steps
- No signal support
- Fixed failure strategy (all-or-nothing)

### Phase 2: Integration & Observability

**Goal**: Integrate with existing features and add visibility

**Deliverables**:
1. Dashboard task list views
2. Task-level progress tracking
3. Condition support on map steps (skip all tasks)
4. Optional dependency support (map step as conditional dependency)
5. Metrics: Task completion rates, duration histograms
6. Enhanced logging: Per-task trace IDs

### Phase 3: Advanced Features

**Goal**: Optimize for production use cases

**Deliverables**:
1. Custom aggregation strategies (reduce functions)
2. Partial result streaming (expose completed tasks before all done)
3. Nested maps (map of maps)
4. Task-level deduplication (idempotency within map)
5. Task prioritization (process certain indices first)
6. Chunking helpers (automatically split large arrays)

### Phase 4: Performance Optimization

**Goal**: Scale to large arrays efficiently

**Deliverables**:
1. Partitioned task tables (by flow_run_id range)
2. Task archival (move completed tasks to cold storage)
3. Streaming task spawning (don't materialize entire array)
4. Adaptive concurrency (scale based on task duration)
5. Performance benchmarks: 100, 1K, 10K element arrays

## Testing Strategy

### Unit Tests

```go
// Test map step creation
func TestMapStepCreation(t *testing.T) {
    client := getTestClient(t)

    flow := catbird.NewFlow("test").
        AddStep(catbird.NewStep("fetch").
            Handler(func(ctx context.Context, _ int) ([]int, error) {
                return []int{1, 2, 3}, nil
            }, nil)).
        AddStep(catbird.NewStep("process").
            DependsOn("fetch").
            MapEach("fetch").
            Handler(func(ctx context.Context, _ int, x int) (int, error) {
                return x * 2, nil
            }, nil))

    require.NoError(t, catbird.CreateFlow(t.Context(), client.Conn, flow))
}

// Test task spawning
func TestTaskSpawning(t *testing.T) {
    client := getTestClient(t)

    flow := catbird.NewFlow("spawn-test").
        AddStep(catbird.NewStep("fetch").
            Handler(func(ctx context.Context, _ int) ([]int, error) {
                return []int{1, 2, 3}, nil
            }, nil)).
        AddStep(catbird.NewStep("process").
            DependsOn("fetch").
            MapEach("fetch").
            Handler(func(ctx context.Context, _ int, x int) (int, error) {
                return x * 2, nil
            }, nil))

    require.NoError(t, client.CreateFlow(t.Context(), flow))
    worker := client.NewWorker(t.Context(), nil).AddFlow(flow)
    go worker.Start(t.Context())

    handle, err := client.RunFlow(t.Context(), "spawn-test", 0, nil)
    require.NoError(t, err)

    // Wait briefly for fetch to complete and tasks to spawn
    time.Sleep(100 * time.Millisecond)

    // Verify tasks created
    var count int
    err = client.Conn.QueryRow(t.Context(),
        "SELECT count(*) FROM cb_t_spawn_test WHERE flow_run_id = $1",
        handle.ID).Scan(&count)
    require.NoError(t, err)
    require.Equal(t, 3, count)
}

// Test empty array
func TestEmptyArray(t *testing.T) {
    client := getTestClient(t)

    flow := catbird.NewFlow("empty-test").
        AddStep(catbird.NewStep("fetch").
            Handler(func(ctx context.Context, _ int) ([]int, error) {
                return []int{}, nil
            }, nil)).
        AddStep(catbird.NewStep("process").
            DependsOn("fetch").
            MapEach("fetch").
            Handler(func(ctx context.Context, _ int, x int) (int, error) {
                return x * 2, nil
            }, nil))

    require.NoError(t, client.CreateFlow(t.Context(), flow))
    worker := client.NewWorker(t.Context(), nil).AddFlow(flow)
    go worker.Start(t.Context())

    handle, err := client.RunFlow(t.Context(), "empty-test", 0, nil)
    require.NoError(t, err)

    var output []int
    err = handle.WaitForOutput(t.Context(), &output)
    require.NoError(t, err)
    require.Len(t, output, 0)
}

// Test type mismatch
func TestTypeMismatch(t *testing.T) {
    client := getTestClient(t)

    flow := catbird.NewFlow("mismatch-test").
        AddStep(catbird.NewStep("fetch").
            Handler(func(ctx context.Context, _ int) (string, error) {
                return "not an array", nil  // Bug: should return []
            }, nil)).
        AddStep(catbird.NewStep("process").
            DependsOn("fetch").
            MapEach("fetch").
            Handler(func(ctx context.Context, _ int, x int) (int, error) {
                return x * 2, nil
            }, nil))

    require.NoError(t, client.CreateFlow(t.Context(), flow))
    worker := client.NewWorker(t.Context(), nil).AddFlow(flow)
    go worker.Start(t.Context())

    handle, err := client.RunFlow(t.Context(), "mismatch-test", 0, nil)
    require.NoError(t, err)

    var output []int
    err = handle.WaitForOutput(t.Context(), &output)
    require.Error(t, err)
    require.Contains(t, err.Error(), "expected array")
}
```

### Integration Tests

```go
// Test full map step execution
func TestMapStepExecution(t *testing.T) {
    client := getTestClient(t)

    flow := catbird.NewFlow("map-exec-test").
        AddStep(catbird.NewStep("numbers").
            Handler(func(ctx context.Context, _ int) ([]int, error) {
                return []int{1, 2, 3, 4, 5}, nil
            }, nil)).
        AddStep(catbird.NewStep("double").
            DependsOn("numbers").
            MapEach("numbers").
            Handler(func(ctx context.Context, _ int, x int) (int, error) {
                return x * 2, nil
            }, nil))

    require.NoError(t, client.CreateFlow(t.Context(), flow))

    worker := client.NewWorker(t.Context(), nil).AddFlow(flow)
    go worker.Start(t.Context())

    handle, err := client.RunFlow(t.Context(), "map-exec-test", 0, nil)
    require.NoError(t, err)

    var output []int
    err = handle.WaitForOutput(t.Context(), &output)
    require.NoError(t, err)

    require.Equal(t, []int{2, 4, 6, 8, 10}, output)
}

// Test task retry on failure
func TestTaskRetry(t *testing.T) {
    client := getTestClient(t)

    attempts := make(map[int]int)
    var mu sync.Mutex

    flow := catbird.NewFlow("retry-test").
        AddStep(catbird.NewStep("numbers").
            Handler(func(ctx context.Context, _ int) ([]int, error) {
                return []int{1, 2, 3}, nil
            }, nil)).
        AddStep(catbird.NewStep("flaky").
            DependsOn("numbers").
            MapEach("numbers").
            Handler(func(ctx context.Context, _ int, x int) (int, error) {
                mu.Lock()
                attempts[x]++
                attempt := attempts[x]
                mu.Unlock()

                if x == 2 && attempt < 3 {
                    return 0, fmt.Errorf("simulated error")
                }

                return x * 10, nil
            }, &catbird.HandlerOpts{
                MaxRetries: 3,
                Backoff:    catbird.NewFullJitterBackoff(10*time.Millisecond, 100*time.Millisecond),
            }))

    require.NoError(t, client.CreateFlow(t.Context(), flow))
    worker := client.NewWorker(t.Context(), nil).AddFlow(flow)
    go worker.Start(t.Context())

    handle, err := client.RunFlow(t.Context(), "retry-test", 0, nil)
    require.NoError(t, err)

    var output []int
    err = handle.WaitForOutput(t.Context(), &output)
    require.NoError(t, err)

    require.Equal(t, []int{10, 20, 30}, output)
    require.Equal(t, 1, attempts[1])
    require.Equal(t, 3, attempts[2])  // Failed twice, succeeded third time
    require.Equal(t, 1, attempts[3])
}
```

## Future Considerations

### Nested Maps

Support maps of maps for 2D array processing:

```go
catbird.NewStep("process_batch").
    DependsOn("batches").
    MapEach("batches").
    Handler(func(ctx context.Context, _ any, batch []Item) ([]Result, error) {
        // Inner map: process each item in batch
        return catbird.MapInHandler(ctx, batch, processItem)
    }, nil)
```

**Challenge**: Maintaining order across nested levels

### Streaming Results

Expose partial results as tasks complete:

```go
catbird.NewStep("process").
    DependsOn("fetch").
    MapEach("fetch").
    Handler(func(...) {...}, nil).
    WithProgressCallback(func(completed, total int, latestResult Result) {
        updateProgressBar(completed, total)
    })
```

**Challenge**: Maintaining transaction semantics while streaming

### Custom Aggregation

Allow user-defined reduce functions:

```go
catbird.NewStep("process").
    DependsOn("fetch").
    MapEach("fetch").
    Handler(func(ctx context.Context, _ any, item Item) (Result, error) {...}, nil).
    Reduce(func(results []Result) (Summary, error) {
        return Summary{Max: maxBy(results, r => r.Score)}, nil
    })
```

**Challenge**: Executing Go code in SQL context

### Priority Ordering

Process certain tasks before others:

```go
catbird.NewStep("process").
    DependsOn("fetch").
    MapEach("fetch").
    Handler(func(...) {...}, nil).
    WithPriority(func(item Item) int {
        return item.Urgency  // Higher urgency tasks polled first
    })
```

**Challenge**: Ordering in SQL `WHERE deliver_at <= now() ORDER BY priority`

## Open Questions

1. **Should map steps support signals?** If yes, broadcast to all tasks or specific task index?
2. **Should failed tasks be retryable manually?** (Dashboard "Retry Task #47" button)
3. **Should partial success be an option?** (Complete map step even if some tasks failed)
4. **Should we support map over object keys?** (`{"a": 1, "b": 2}` → map over `[["a", 1], ["b", 2]]`)
5. **Should task outputs be deduplicated?** (If same task retries and produces different output)
6. **Should there be a maximum array size limit?** (Prevent accidental 1M element arrays)
7. **Should task polling be flow-specific or global?** (All workers poll all flows' tasks vs dedicated workers)

## Conclusion

Map steps bring powerful parallel processing capabilities to catbird while maintaining its core philosophy of database-driven coordination. The design:

- ✅ **Preserves catbird's principles**: Database as sole coordinator, no extensions, concurrent-safe
- ✅ **Integrates naturally**: Works with existing features (conditions, retries, circuit breakers)
- ✅ **Type-safe**: Leverages Go generics for compile-time safety
- ✅ **Performant**: Optimized for hot path (polling) and cold path (setup)
- ✅ **Observable**: Dashboard integration for task-level visibility
- ✅ **Incremental**: Can be implemented in phases without breaking changes

The implementation complexity is justified by the significant value it provides for real-world batch processing use cases.
