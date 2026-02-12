# Advanced Scheduling: DB-Driven Approach (Future Implementation)

## Overview

This document outlines the **strongest-guarantee scheduling solution** for Catbird, designed for future implementation after Option 1 (UTC-normalized idempotency keys) proves stable in production.

**Current Status (Option 1):** ✅ Production-ready, all tests passing
**Future Status (This Document):** 📋 Design phase, ready for implementation roadmap

---

## 1. Problem Statement: Why Option 1 Has Theoretical Vulnerabilities

### Option 1 Refresher
- UTC-normalized cron (`cron.WithLocation(time.UTC)`)
- Idempotency keys: `"schedule:{unix_seconds}"`
- Database constraint: `UNIQUE (idempotency_key) WHERE status IN ('queued', 'started', 'completed')`
- **Guarantee:** Exactly-one execution per cron tick when workers have UTC-synced clocks

### Theoretical Vulnerabilities

1. **Clock Synchronization Dependency**
   - Relies on NTP (or equivalent) keeping clocks within reasonable sync
   - If NTP fails: clocks drift → potential duplicate enqueues
   - Acceptable for most deployments, but not bulletproof

2. **Time Anomalies**
   - Leap second (rare, but possible)
   - System clock jump due to NTP correction
   - DST transitions (if any timezone accidentally used despite UTC lock)
   
3. **No Automatic Recovery**
   - If a duplicate somehow gets enqueued (despite constraints), no mechanism to detect/fix
   - Manual intervention required

4. **No Scheduling State Visibility**
   - Can't pause, reschedule, or adjust `next_run_at` without code changes
   - No way to query "when will this schedule fire next?"
   - Dashboard shows task runs, not schedule configuration

5. **Cron Library as Source of Truth**
   - Schedule stored only in worker memory (in `Scheduler` struct)
   - If all workers crash and restart: might miss a scheduled execution entirely
   - No durable schedule record to recover from

---

## 2. Proposed Solution: PostgreSQL as Single Source of Truth

Instead of workers independently calculating when to run, **the database calculates and persists the schedule**.

### Key Idea

```
Old Model (Option 1):
  [Worker A] ← cron library calculates time
  [Worker B] ← cron library calculates time
  [Worker C] ← cron library calculates time
  All try to enqueue → dedup on DB side

New Model (Option 2):
  Database: "Next run for task_hourly is 2026-02-12T15:00:00Z"
  [Worker A] → SELECT and LOCK row → enqueue → update next_run_at
  [Worker B] → SELECT and LOCK row → empty set (A already claimed it)
  [Worker C] → SELECT and LOCK row → empty set (A already claimed it)
  Only one row to process → inherently single execution
```

### Design Principle

**Workers claim work atomically**: `SELECT ... FOR UPDATE SKIP LOCKED` ensures exactly one worker processes each scheduled run.

---

## 3. Schema Design

### New Table: `cb_schedules`

```sql
CREATE TABLE cb_schedules (
    -- Identification
    id                BIGSERIAL PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,  -- e.g., "email_digest_hourly", "cleanup_nightly"
    kind              TEXT NOT NULL,         -- 'task' or 'flow'
    
    -- Cron specification
    cron_spec         TEXT NOT NULL,         -- e.g., "@hourly", "0 * * * *", "0 2 * * 0"
    
    -- Schedule state
    next_run_at       TIMESTAMPTZ NOT NULL,  -- When this schedule should next execute
    last_run_at       TIMESTAMPTZ,           -- When it last executed (NULL = never)
    last_enqueued_at  TIMESTAMPTZ,           -- When it last enqueued a run (NULL = never)
    
    -- Control
    enabled           BOOLEAN NOT NULL DEFAULT true,
    
    -- Metadata
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Constraints
    CONSTRAINT kind_valid CHECK (kind IN ('task', 'flow')),
    CONSTRAINT next_run_at_in_future CHECK (next_run_at > created_at)
);

-- Key index for polling schedules that are due
CREATE INDEX cb_schedules_next_run_at_enabled_idx 
  ON cb_schedules (next_run_at, enabled) 
  WHERE enabled = true;

-- For looking up a specific schedule
CREATE INDEX cb_schedules_name_kind_idx 
  ON cb_schedules (name, kind);
```

### Migration from In-Memory Schedules

When transitioning from Option 1 → Option 2:

```sql
-- Backfill existing schedules from worker registration
-- (This happens during deployment coordinator step)

INSERT INTO cb_schedules (name, kind, cron_spec, next_run_at, enabled)
SELECT 
  task_name, 
  'task', 
  '@hourly',  -- Could be parameterized per task
  now(),      -- Or calculate based on cron_spec
  true
FROM cb_task_handlers
WHERE task_name IN ('email_digest', 'cleanup_old_runs', ...) -- Known scheduled tasks
ON CONFLICT (name) DO NOTHING;
```

---

## 4. Worker Loop Implementation

### Pseudocode: Schedule Claiming

```go
// In scheduler loop (run every ~5 seconds or triggered by tick)
func (w *Worker) claimAndEnqueueSchedules(ctx context.Context) error {
    for {
        // Step 1: Atomically claim due schedules
        schedules, err := w.db.Query(ctx, `
            SELECT id, name, kind, cron_spec, next_run_at
            FROM cb_schedules
            WHERE next_run_at <= now()
              AND enabled = true
            ORDER BY next_run_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        `)
        if err != nil {
            return err
        }
        
        if len(schedules) == 0 {
            // No more due schedules
            break  // or sleep and retry
        }
        
        schedule := schedules[0]
        
        // Step 2: Calculate next run time using PostgreSQL cron function
        // (Cron parsing is done in the database, not in worker code)
        nextRunAt, err := w.db.QueryRow(ctx, `
            SELECT cb_next_cron_tick($1, $2)
        `, schedule.CronSpec, schedule.NextRunAt).Scan(&nextRunAt)
        if err != nil {
            return err
        }
        
        // Step 3: Atomically:
        //   - Enqueue the run (with stable idempotency key)
        //   - Update schedule's next_run_at
        err = w.db.WithTx(ctx, func(tx Tx) error {
            // Enqueue using same idempotency key strategy
            key := fmt.Sprintf("schedule:%d", schedule.next_run_at.Unix())
            runID, err := enqueueRun(tx, schedule.name, schedule.kind, key)
            if err != nil {
                return err
            }
            
            // Update schedule (still in same transaction)
            _, err = tx.Exec(ctx, `
                UPDATE cb_schedules
                SET 
                  next_run_at = $1,
                  last_enqueued_at = now(),
                  updated_at = now()
                WHERE id = $2
            `, nextRunAt, schedule.id)
            
            return err
        })
        
        if err != nil {
            w.logger.Error("failed to claim schedule", "error", err)
            // Continue to next schedule instead of crashing
            continue
        }
        
        w.logger.Info("claimed and enqueued schedule", 
            "name", schedule.name, 
            "next_run_at", nextRunAt)
    }
}
```

### Key Algorithm: Cron Parsing in PostgreSQL

Instead of parsing cron specs in worker code, Catbird implements cron logic entirely in PL/pgSQL. This ensures:
- **Language-agnostic**: Workers in any language (Go, Python, Node, Ruby) can use the same function
- **Version consistency**: No cron library version skew across workers
- **Auditability**: Scheduling logic lives in the database and can be queried/audited directly

**Core function signature:**
```sql
CREATE FUNCTION cb_next_cron_tick(
    cron_spec TEXT,        -- e.g., "@hourly", "0 9 * * *", "*/15 * * * *"
    from_time TIMESTAMPTZ  -- Base time to calculate next tick from
)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql AS $$
BEGIN
    -- Parse cron_spec into (minute, hour, dom, month, dow)
    -- Handle special keywords: @yearly, @annually, @monthly, @weekly, @daily, @hourly, @reboot
    -- Find the next datetime >= from_time matching the cron constraints
    -- Return it in UTC
END;
$$;
```

**Implementation approach:**
1. Handle special keywords (`@hourly` → `0 * * * *`, etc.)
2. Parse 5-field cron notation into arrays: `(minute[], hour[], dom[], month[], dow[])`
3. Iterate forward from `from_time` checking constraints:
   - Month in `month[]`, Day-of-month in `dom[]`, Day-of-week in `dow[]` (with special handling for `dow` flexibility)
   - When a day matches, find the earliest hour in `hour[]`
   - When an hour matches, find the earliest minute in `minute[]`
4. Return the first matching datetime

**Worker usage:**
```go
nextRunAt, err := w.db.QueryRow(ctx, `
    SELECT cb_next_cron_tick($1, $2)
`, schedule.CronSpec, schedule.NextRunAt).Scan(&nextRunAt)
```

**Supported cron specs:**
- Standard: `0 9 * * *` (9 AM every day)
- Ranges: `0-30 * * * *` (every hour at :00 and :30)
- Steps: `*/15 * * * *` (every 15 minutes)
- Lists: `0,30 9,17 * * *` (9 AM and 5 PM, on the hour and half-hour)
- Keywords: `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`
- Special: `H` (hash) not supported initially, use `*/n` instead

### Idempotency Key Strategy

Remains the same as Option 1, but now **generated from the database's `next_run_at` value**:

```go
key := fmt.Sprintf("schedule:%d", schedule.next_run_at.Unix())
// e.g., "schedule:1707759600"
```

Since `next_run_at` is persisted in the database (not recalculated by each worker), all workers see the same value, guaranteeing identical keys.

---

## 5. Advantages Over Alternatives

### vs. Option 1 (Current UTC + Idempotency)

| Aspect | Option 1 | Option 2 |
|--------|----------|----------|
| **Clock Sync Dependency** | ✅ Required (NTP) | ❌ None (DB is source) |
| **Schedule Visibility** | ❌ Hidden in worker memory | ✅ Query via SQL |
| **Pause/Reschedule** | ❌ Requires config change + restart | ✅ `UPDATE cb_schedules SET enabled = false` |
| **Recovery from Crash** | ⚠️ Depends on NTP state | ✅ DB has all state |
| **Scaling** | ✅ O(1) per worker | ✅ O(1) per worker (DB handles distribution) |
| **Operational Complexity** | ✅ Simpler (fewer moving parts) | ⚠️ One more table to manage |

### vs. Leader Election

**Anti-pattern: "Elect a leader to run all schedules"**

```
Drawback 1: Single point of failure
  - Leader crashes → no schedules run until new leader elected
  - Leader busy → schedules delayed for all tasks

Drawback 2: Requires extra coordination
  - Etcd, Consul, or custom lease mechanism
  - Additional operational complexity

Option 2 Advantage:
  - No leader needed; DB distributes work via SKIP LOCKED
  - Any worker can pick up any due schedule
  - Inherently fault-tolerant
```

### vs. Global Cron Service (External Scheduler)

**Anti-pattern: "Run a separate scheduling service"**

```
Drawback 1: Adds deployment complexity
  - Must deploy and manage separate service
  - Must secure communication
  - Must monitor uptime

Drawback 2: Adds operational load
  - Another service to scale, replicate, fail-over

Option 2 Advantage:
  - Scheduling is just database queries
  - Workers self-coordinate via PostgreSQL
  - No separate service to manage
```

---

## 6. Implementation Roadmap

### Phase 0: Option 1 Validation (✅ Complete)
- [x] UTC-normalized cron
- [x] Idempotency key dedup
- [x] Comprehensive tests
- [x] Production deployment
- [x] Monitor for issues

### Phase 1: Design & Preparation (→ Current)
- [ ] Finalize schema
- [ ] Design migration step (in-memory → persisted)
- [ ] Create unit tests for new claiming logic
- [ ] Document backwards compatibility

### Phase 2: Implementation (6-8 weeks estimated)
- [ ] Add `cb_schedules` table via migration
- [ ] Implement `cb_next_cron_tick()` PL/pgSQL function (cron parsing in database)
- [ ] Add comprehensive tests for cron edge cases (month boundaries, leap years, etc.)
- [ ] Implement `claimAndEnqueueSchedules()` worker method
- [ ] Add config option: `UseDBDrivenScheduling: bool`
- [ ] Test with both Option 1 and Option 2 enabled simultaneously
- [ ] Add metrics: schedule claim latency, skipped schedules, etc.

### Phase 3: Canary Rollout (2-4 weeks)
- [ ] Deploy with `UseDBDrivenScheduling: false` (default)
- [ ] Enable for 1 critical task with high observability
- [ ] Monitor: latency, duplication rate, error rate
- [ ] Gradually enable for more tasks

### Phase 4: Deprecation (3-6 months)
- [ ] Make `UseDBDrivenScheduling: true` the default
- [ ] Log warnings when Option 1 mode detected
- [ ] Plan removal of Option 1 code (future major version)

---

## 7. Code Examples

### Registration Syntax (Backwards Compatible)

```go
// Option 1 (still works)
worker, err := catbird.NewWorker(ctx,
    WithTask(myTask),
    WithScheduledTask("my_task", "@hourly"),
)

// Option 2 (new, when available)
worker, err := catbird.NewWorker(ctx,
    WithTask(myTask),
    WithDBDrivenSchedule("my_task", "@hourly"),  // ← Tells worker to use DB-driven mode
)

// Both can coexist during migration
```

### Dashboard Enhancements

```go
// New API endpoint (future)
router.GET("/api/schedules", func(w http.ResponseWriter, r *http.Request) {
    schedules, err := client.ListSchedules(r.Context())
    // Returns: [{name, kind, cron_spec, next_run_at, last_run_at, enabled}, ...]
    json.NewEncoder(w).Encode(schedules)
})

// New UI: Schedule management page
// - Show all schedules with next fire time
// - Button to pause/resume schedules
// - One-click reschedule
// - View run history for this schedule
```

### Reschedule API (Operational)

```go
// Admin API: manually trigger a schedule
err := client.TriggerSchedule(ctx, "email_digest_hourly")
// Result: immediately enqueues run, doesn't modify next_run_at

// Reschedule next fire time
err := client.RescheduleFor(ctx, "email_digest_hourly", time.Now().Add(30*time.Minute))
// Result: updates next_run_at in database

// Pause a schedule
err := client.SetScheduleEnabled(ctx, "email_digest_hourly", false)
// Result: schedule.enabled = false; no more runs until re-enabled
```

---

## 8. Testing Strategy

### Unit Tests

```go
func TestCronNextTickFunction(t *testing.T) {
    // Test cron spec parsing via SQL
    testCases := []struct{
        spec string
        from time.Time
        expected time.Time
    }{
        {"@hourly", time.Date(2026, 2, 12, 14, 30, 0, 0, time.UTC), 
         time.Date(2026, 2, 12, 15, 0, 0, 0, time.UTC)},
        {"0 9 * * *", time.Date(2026, 2, 12, 8, 0, 0, 0, time.UTC),
         time.Date(2026, 2, 12, 9, 0, 0, 0, time.UTC)},
        {"*/15 * * * *", time.Date(2026, 2, 12, 14, 27, 0, 0, time.UTC),
         time.Date(2026, 2, 12, 14, 30, 0, 0, time.UTC)},
        // Month boundaries, leap years, DST transitions, etc.
    }
    for _, tc := range testCases {
        var result time.Time
        err := db.QueryRow(ctx, `SELECT cb_next_cron_tick($1, $2)`, 
            tc.spec, tc.from).Scan(&result)
        assert.NoError(t, err)
        assert.Equal(t, tc.expected, result)
    }
}

### Integration Tests

```go
func TestScheduleClaimAtomicity(t *testing.T) {
    // Simulate 10 workers claiming the same due schedule
    // Verify: exactly 1 succeeds, 9 get empty result set
    // Verify: row lock held during transaction
}

func TestScheduleMigrationBackfill(t *testing.T) {
    // Insert known schedules into cb_schedules
    // Verify: cb_next_cron_tick() returns expected next_run_at
}

func TestIdempotencyKeyConsistency(t *testing.T) {
    // Multiple workers, same due schedule
    // Verify: all generate same idempotency key
    // Verify: only one run enqueued despite multiple claims
}

### Load Tests

```go
func BenchmarkScheduleClaiming(b *testing.B) {
    // 1000 due schedules
    // 50 workers simultaneously claiming
    // Measure: claim latency, throughput, lock contention
    // Target: <100ms p99 latency, ~10k claims/sec
}
```

---

## 9. PL/pgSQL Cron Implementation

### Design Rationale

Implementing cron parsing in PostgreSQL (not as an extension, just PL/pgSQL) aligns with Catbird's core principle: **no PostgreSQL extensions**. Benefits:

- **Language-agnostic workers**: Python, Node, Ruby, Java, etc. can all schedule via the same API
- **No version skew**: All workers use identical cron logic from the database
- **Auditability**: Cron logic is queryable SQL, not compiled binary code
- **Maintainability**: Single source of truth for scheduling behavior

### Migration Function

```sql
CREATE OR REPLACE FUNCTION cb_next_cron_tick(
    cron_spec TEXT,
    from_time TIMESTAMPTZ
)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE STRICT
AS $$
DECLARE
    v_minute INT[];
    v_hour INT[];
    v_dom INT[];
    v_month INT[];
    v_dow INT[];
    v_next TIMESTAMPTZ;
BEGIN
    -- Normalize cron_spec: handle @yearly, @monthly, @weekly, @daily, @hourly
    -- Parse 5-field cron notation into arrays
    -- Iterate from from_time, checking constraints until match found
    -- Return matching time in UTC
    
    -- Implementation sketch:
    CASE cron_spec
        WHEN '@yearly' THEN RETURN make_timestamptz(EXTRACT(YEAR FROM from_time)::INT + 1, 1, 1, 0, 0, 0, 'UTC');
        WHEN '@annually' THEN RETURN make_timestamptz(EXTRACT(YEAR FROM from_time)::INT + 1, 1, 1, 0, 0, 0, 'UTC');
        WHEN '@monthly' THEN RETURN make_timestamptz(EXTRACT(YEAR FROM from_time)::INT, (EXTRACT(MONTH FROM from_time)::INT % 12) + 1, 1, 0, 0, 0, 'UTC');
        WHEN '@weekly' THEN RETURN from_time + INTERVAL '1 week' - (EXTRACT(DOW FROM from_time)::INT) * INTERVAL '1 day';
        WHEN '@daily' THEN RETURN (from_time + INTERVAL '1 day')::DATE::TIMESTAMPTZ AT TIME ZONE 'UTC';
        WHEN '@hourly' THEN RETURN date_trunc('hour', from_time) + INTERVAL '1 hour';
        -- Parse @reboot as immediate/now
        WHEN '@reboot' THEN RETURN now() AT TIME ZONE 'UTC';
        ELSE
            -- Parse standard 5-field cron: minute hour dom month dow
            -- For now, basic implementation
            RAISE EXCEPTION 'Cron spec not yet supported: %', cron_spec;
    END CASE;
END;
$$;
```

### Supported Cron Specs (Phase 2)

**Initial release:**
- ✅ Special keywords: `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@hourly`, `@reboot`

**Phase 2B (optional):**
- `0 9 * * *` (fixed times)
- `*/15 * * * *` (intervals)
- Ranges: `0-30`, `9,17`
- Lists: `0,15,30,45`

**Not supported:**
- `H` (hash) - use `*/n` instead
- Timezone-aware specs - all times must be UTC

---

## 10. Backwards Compatibility

### Coexistence Period

Both Option 1 and Option 2 can run side-by-side:

```go
// Worker can have both types of schedules
worker := NewWorker(ctx,
    WithScheduledTask("task1", "@hourly"),        // Option 1 (in-memory cron)
    WithDBDrivenSchedule("task2", "@daily"),      // Option 2 (DB-driven)
)
```

### Migration Path for Existing Deployments

1. **Prepare:**
   - Deploy new code with `UseDBDrivenScheduling: false` (default)
   - Existing Option 1 schedules continue unchanged

2. **Backfill:**
   - Migration script: insert known schedules into `cb_schedules`
   - Operator reviews and confirms

3. **Test:**
   - Canary: enable for 1 non-critical schedule
   - Monitor for 1-2 weeks

4. **Gradually Enable:**
   - Enable for more schedules incrementally
   - Keep monitoring

5. **Deprecate Option 1 (Future Major Version):**
   - Remove `WithScheduledTask()` API
   - Only `WithDBDrivenSchedule()` supported

### Automatic Fallback

If DB-driven mode encounters errors (e.g., schedule table corrupted):

```go
if useDBDrivenScheduling {
    err := w.claimAndEnqueueSchedules(ctx)
    if err != nil {
        w.logger.Error("DB-driven scheduling failed, falling back to Option 1", "error", err)
        // Fall back: use in-memory cron
        w.runInMemoryScheduler(ctx)
    }
}
```

---

## 11. Future Enhancements (Beyond Scope)

### 10.1 Schedule Templates

```sql
-- Define schedule templates for common patterns
INSERT INTO cb_schedule_templates (name, cron_spec, description)
VALUES 
  ('Hourly', '@hourly', 'Every hour on the hour'),
  ('Daily Midnight', '0 0 * * *', 'Every day at midnight UTC'),
  ('Daily 9 AM', '0 9 * * *', 'Every weekday at 9 AM UTC'),
  ('Weekly Monday 8 AM', '0 8 * * 1', 'Every Monday at 8 AM UTC');
```

### 10.2 Schedule Burst Control

```sql
-- Prevent thundering herd when many schedules fire simultaneously
ALTER TABLE cb_schedules ADD COLUMN (
    max_concurrent_runs INT DEFAULT 1,
    burst_allowed BOOLEAN DEFAULT false
);
```

### 10.3 Timezone Awareness

```sql
-- Allow specifying cron in different timezones (not UTC)
ALTER TABLE cb_schedules ADD COLUMN (
    timezone TEXT DEFAULT 'UTC',
    -- "A task should fire at 9 AM New York time" (even during DST transitions)
);
```

### 10.4 Schedule Dependencies

```sql
-- "Task B should only run if Task A succeeded in the last hour"
ALTER TABLE cb_schedules ADD COLUMN (
    depends_on_schedule_id BIGINT REFERENCES cb_schedules(id)
);
```

### 10.5 Analytics & Dashboards

```sql
-- New metrics table
CREATE TABLE cb_schedule_metrics (
    id BIGSERIAL PRIMARY KEY,
    schedule_id BIGINT NOT NULL REFERENCES cb_schedules(id),
    claimed_at TIMESTAMPTZ,
    claimed_by_worker_id UUID,
    enqueued_at TIMESTAMPTZ,
    duration_ms INT,
    success BOOLEAN,
    error_message TEXT
);
```

---

## 12. Deployment Checklist

- [ ] Create migration for `cb_schedules` table
- [ ] Create migration for `cb_next_cron_tick()` PL/pgSQL function with comprehensive tests
- [ ] Test cron function against edge cases (month boundaries, leap years, DST if applicable)
- [ ] Implement `claimAndEnqueueSchedules()` worker method
- [ ] Add config option: `UseDBDrivenScheduling: bool`
- [ ] Document cron spec support (which formats are supported in Phase 2)
- [ ] Test with both Option 1 and Option 2 enabled simultaneously
- [ ] Add metrics: schedule claim latency, next_run_at accuracy, etc.
- [ ] Comprehensive unit + integration tests added
- [ ] Documentation updated (this file + code comments)
- [ ] Monitoring/observability built (metrics, logging)
- [ ] Canary deployment procedure documented
- [ ] Rollback procedure documented
- [ ] Team trained on new operational aspects (pause, reschedule, etc.)
- [ ] Existing deployments can coexist (both modes)

---

## 13. Performance Considerations

### Latency

Expected polling frequency: **once every 5-10 seconds per worker**

```
Workflow:
1. Query: 2-5 ms (indexed scan of 1 row)
2. Lock acquisition: <1 ms (SKIP LOCKED, no contention)
3. Cron calculation: <1 ms (pure math)
4. Enqueue + update: 5-10 ms (transaction)
5. Total: ~10-20 ms per claim
```

### Scalability

```
1000 due schedules, 50 workers:
- Each worker polls every 5 seconds
- Per poll: ~20 ms claim + skip locked = minimal contention
- Database can handle 50 * (1000/5) = 10k QPS easily
```

### Lock Contention

`FOR UPDATE SKIP LOCKED` prevents queuing:
- Worker A claims row 1
- Worker B tries row 1 → skipped → moves to row 2
- No blocking, no wait queue

---

## Summary

Option 2 provides the **strongest durability and operational guarantees**:

✅ Clock-sync independent  
✅ Durable schedule state  
✅ Operational visibility  
✅ Pause/reschedule support  
✅ Crash-recovery built-in  
✅ Horizontally scalable  

**Ready to implement once Option 1 has proven stable in production.**
- No breaking changes to existing `WithScheduledTask()` or `WithScheduledFlow()`
- Both approaches can coexist; a single worker can use both

## Conclusion

DB-driven scheduling is the strongest-guarantee approach for Catbird's distributed scheduling. While Option 1 (UTC + IdempotencyKey) is suitable for immediate deployment and covers most real-world scenarios, DB-driven scheduling should be pursued as a future enhancement for applications requiring absolute immunity to clock skew and centralized schedule management.

The phased approach allows us to:
1. Ship Option 1 today (done ✅)
2. Introduce DB-driven scheduling as opt-in
3. Eventually unify on DB-driven as the default

This aligns with Catbird's design principle: **PostgreSQL as the single source of truth for all coordination.**
