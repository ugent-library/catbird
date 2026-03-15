# SQL API Reference

## SQL Usage Examples

### Queues

```sql
-- Create a queue
SELECT cb_create_queue(
	name => 'my_queue'
);

-- Send a message
SELECT cb_send(
	queue => 'my_queue',
	payload => '{"user_id": 123, "action": "process"}'::jsonb
);

-- Publish to topic-bound queues
SELECT cb_publish(
	topic => 'events.user.created',
	payload => '{"user_id": 456}'::jsonb,
	idempotency_key => 'user-456-created'
);

-- Read messages (with 30 second visibility timeout)
SELECT * FROM cb_read(
	queue => 'my_queue',
	quantity => 10,
	hide_for => 30000
);

-- Delete a message
SELECT cb_delete(
	queue => 'my_queue',
	id => 1
);

-- Bind queue to topic pattern
SELECT cb_bind(
	queue_name => 'user_events',
	pattern => 'events.user.*'
);
SELECT cb_unbind(
	queue_name => 'user_events',
	pattern => 'events.user.*'
);
```

### Tasks

```sql
-- Create a task definition
SELECT cb_create_task(
	name => 'send_email'
);

-- Run a task
SELECT * FROM cb_run_task(
	name => 'send_email',
	input => '{"to": "user@example.com"}'::jsonb
);
```

### Workflows

```sql
-- Create a flow definition
SELECT cb_create_flow(
		name => 'order_processing',
		steps => '[
			{"name": "validate"},
			{"name": "charge", "depends_on": [{"name": "validate"}]},
			{"name": "ship", "depends_on": [{"name": "charge"}]}
		]'::jsonb
);

-- Create a flow with a map step
SELECT cb_create_flow(
		name => 'map_example',
		steps => '[
			{"name": "numbers"},
			{"name": "double", "step_type": "mapper", "map_source_step_name": "numbers", "depends_on": [{"name": "numbers"}]}
		]'::jsonb
);

-- Run a flow
SELECT * FROM cb_run_flow(
		name => 'order_processing',
		input => '{"order_id": 123}'::jsonb
);
```

### Monitoring task and flow runs

You can query task and flow run information directly:

```sql
-- List recent task runs (replace send_email with your task name)
SELECT
	id,
	concurrency_key,
	idempotency_key,
	status,
	input,
	output,
	error_message,
	started_at,
	completed_at,
	failed_at
FROM cb_t_send_email
ORDER BY started_at DESC
LIMIT 20;

-- Get flow run (replace order_processing with your flow name)
SELECT
	id,
	concurrency_key,
	idempotency_key,
	status,
	input,
	output,
	error_message,
	started_at,
	completed_at,
	failed_at
FROM cb_f_order_processing
WHERE id = $1;
```

### External archiving

For long-term archiving, export rows before they are deleted using a standard
watermark-based query and write to your own storage (S3, data warehouse, etc.).
Catbird does not manage the export destination or cursor state.

```sql
SELECT *
FROM cb_t_my_task
WHERE status IN ('completed', 'failed', 'skipped', 'canceled')
	AND finished_at < now() - interval '30 days'
	AND finished_at > $watermark
ORDER BY finished_at, id
LIMIT $batch_size;
```

## Public SQL API

These are the functions most app code and external clients care about.

**Queues & topics**

### `cb_create_queue`
- **What it does**: Create a queue definition.
- **Inputs**: `cb_create_queue(name text, expires_at timestamptz = null, description text = null)`
- **Returns**: `RETURNS void`

### `cb_delete_queue`
- **What it does**: Delete a queue definition and all its messages.
- **Inputs**: `cb_delete_queue(name text)`
- **Returns**: `RETURNS boolean`

### `cb_bind`
- **What it does**: Subscribe a queue to a topic pattern.
- **Inputs**: `cb_bind(queue_name text, pattern text)`
- **Returns**: `RETURNS void`

### `cb_unbind`
- **What it does**: Unsubscribe a queue from a topic pattern.
- **Inputs**: `cb_unbind(queue_name text, pattern text)`
- **Returns**: `RETURNS boolean`

### `cb_send` (single message)
- **What it does**: Send one message to a specific queue.
- **Inputs**: `cb_send(queue text, payload jsonb, topic text = null, idempotency_key text = null, headers jsonb = null, visible_at timestamptz = null)`
- **Returns**: `RETURNS bigint`

### `cb_send` (batch)
- **What it does**: Send multiple messages to a specific queue in one call.
- **Inputs**: `cb_send(queue text, payloads jsonb[], topic text = null, idempotency_keys text[] = null, headers jsonb[] = null, visible_at timestamptz = null)`
- **Returns**: `RETURNS bigint[]`

### `cb_publish` (single message)
- **What it does**: Publish one message to all queues matching a topic.
- **Inputs**: `cb_publish(topic text, payload jsonb, idempotency_key text = null, headers jsonb = null, visible_at timestamptz = null)`
- **Returns**: `RETURNS int`

### `cb_publish` (batch)
- **What it does**: Publish multiple messages to all queues matching a topic.
- **Inputs**: `cb_publish(topic text, payloads jsonb[], idempotency_keys text[] = null, headers jsonb[] = null, visible_at timestamptz = null)`
- **Returns**: `RETURNS int`

### `cb_read`
- **What it does**: Read messages from a queue.
- **Inputs**: `cb_read(queue text, quantity int, hide_for int)`
- **Returns**: `RETURNS SETOF cb_message`

### `cb_read_poll`
- **What it does**: Read messages from a queue with polling.
- **Inputs**: `cb_read_poll(queue text, quantity int, hide_for int, poll_for int, poll_interval int)`
- **Returns**: `RETURNS SETOF cb_message`

### `cb_hide` (single message)
- **What it does**: Hide one message from being read.
- **Inputs**: `cb_hide(queue text, id bigint, hide_for int)`
- **Returns**: `RETURNS boolean`

### `cb_hide` (batch)
- **What it does**: Hide multiple messages from being read.
- **Inputs**: `cb_hide(queue text, ids bigint[], hide_for int)`
- **Returns**: `RETURNS bigint[]`

### `cb_delete` (single message)
- **What it does**: Delete one message from a queue.
- **Inputs**: `cb_delete(queue text, id bigint)`
- **Returns**: `RETURNS boolean`

### `cb_delete` (batch)
- **What it does**: Delete multiple messages from a queue.
- **Inputs**: `cb_delete(queue text, ids bigint[])`
- **Returns**: `RETURNS bigint[]`

**Task definitions & runs**

### `cb_create_task`
- **What it does**: Create a task definition.
- **Inputs**: `cb_create_task(name text, description text = null, condition text = null, retention_period interval = null)`
- **Returns**: `RETURNS void`

### `cb_delete_task`
- **What it does**: Delete a task definition and all its runs.
- **Inputs**: `cb_delete_task(name text)`
- **Returns**: `RETURNS boolean`

### `cb_run_task`
- **What it does**: Create a task run (enqueue a task execution).
- **Inputs**: `cb_run_task(name text, input jsonb, concurrency_key text = NULL, idempotency_key text = NULL, headers jsonb = NULL, visible_at timestamptz = NULL)`
- **Returns**: `RETURNS bigint`

### `cb_wait_task_output`
- **What it does**: Long-poll for task completion without client-side polling loops.
- **Inputs**: `cb_wait_task_output(task_name text, run_id bigint, poll_for int DEFAULT 5000, poll_interval int DEFAULT 200)`
- **Returns**: `RETURNS TABLE(status text, output jsonb, error_message text)`

### `cb_cancel_task`
- **What it does**: Cancel a task run; if it is already terminal, this is a no-op.
- **Inputs**: `cb_cancel_task(name text, run_id bigint, reason text DEFAULT NULL)`
- **Returns**: `RETURNS boolean` (`true` when the run exists, `false` when the run does not exist)

**Flow definitions & runs**

### `cb_create_flow`
- **What it does**: Create a flow definition.
- **Inputs**: `cb_create_flow(name text, description text DEFAULT NULL, steps jsonb DEFAULT '[]'::jsonb, output_priority text[] DEFAULT NULL, retention_period interval DEFAULT NULL)`
- **Expected `steps` shape**:
	- `steps` must be a JSON array of step objects.
	- Each step object:
		- `name` (required, string): step name (`a-z`, `0-9`, `_`; not `input` or `signal`).
		- `description` (optional, string)
		- `depends_on` (optional, array): list of dependency objects `{ "name": "<step_name>" }`.
		- `condition` (optional, string): condition expression.
		- `signal` (optional, boolean; default `false`)
		- `step_type` (optional, string; default `"normal"`): one of `normal | mapper | generator | reducer`.
		- `map_source_step_name` (optional, string): only valid when `step_type = "mapper"`; if set, it must also appear in `depends_on`.
		- `reduce_source_step_name` (optional, string): required when `step_type = "reducer"`; must also appear in `depends_on`.
- **Returns**: `RETURNS void`

### `cb_delete_flow`
- **What it does**: Delete a flow definition and all its runs.
- **Inputs**: `cb_delete_flow(name text)`
- **Returns**: `RETURNS boolean`

### `cb_run_flow`
- **What it does**: Create a flow run (enqueue a flow execution).
- **Inputs**: `cb_run_flow(name text, input jsonb, concurrency_key text = NULL, idempotency_key text = NULL, headers jsonb = NULL, visible_at timestamptz = NULL)`
- **Returns**: `RETURNS bigint`

### `cb_wait_flow_output`
- **What it does**: Long-poll for flow completion without client-side polling loops.
- **Inputs**: `cb_wait_flow_output(flow_name text, run_id bigint, poll_for int DEFAULT 5000, poll_interval int DEFAULT 200)`
- **Returns**: `RETURNS TABLE(status text, output jsonb, error_message text)`

### `cb_signal_flow`
- **What it does**: Deliver a signal to a waiting step run.
- **Inputs**: `cb_signal_flow(flow_name text, flow_run_id bigint, step_name text, input jsonb)`
- **Returns**: `RETURNS boolean`

### `cb_cancel_flow`
- **What it does**: Cancel a flow run; active work may pass through `canceling` before `canceled`.
- **Inputs**: `cb_cancel_flow(name text, run_id bigint, reason text DEFAULT NULL)`
- **Returns**: `RETURNS boolean` (`true` when the run exists, `false` when the run does not exist)

**Scheduling setup**

### `cb_create_task_schedule`
- **What it does**: Create or replace a cron schedule for a task with static JSON input and catch-up policy.
- **Inputs**: `cb_create_task_schedule(task_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb, catch_up text DEFAULT 'one')`
- **Returns**: `RETURNS void`
- **Catch-up policies**: `'skip'` (skip all missed ticks), `'one'` (enqueue one catch-up, default), `'all'` (replay every missed tick)

### `cb_create_flow_schedule`
- **What it does**: Create or replace a cron schedule for a flow with static JSON input and catch-up policy.
- **Inputs**: `cb_create_flow_schedule(flow_name text, cron_spec text, input jsonb DEFAULT '{}'::jsonb, catch_up text DEFAULT 'one')`
- **Returns**: `RETURNS void`
- **Catch-up policies**: `'skip'` (skip all missed ticks), `'one'` (enqueue one catch-up, default), `'all'` (replay every missed tick)

**Maintenance**

### `cb_gc`
- **What it does**: Garbage collection for stale workers, expired queues, and old task/flow runs.
- **Inputs**: `cb_gc()`
- **Returns**: `RETURNS jsonb`
- **Returned JSON shape**:
	- `{ "expired_queues_deleted": int, "stale_workers_deleted": int, "task_runs_purged": int, "flow_runs_purged": int }`

### `cb_purge_task_runs`
- **What it does**: Delete terminal task runs older than a given duration.
- **Inputs**: `cb_purge_task_runs(name text, older_than interval)`
- **Returns**: `RETURNS int`

### `cb_purge_flow_runs`
- **What it does**: Delete terminal flow runs older than a given duration.
- **Inputs**: `cb_purge_flow_runs(name text, older_than interval)`
- **Returns**: `RETURNS int`

## Internal / runtime SQL API

These are mostly used by Catbird workers and scheduler internals. Most users should not call them directly.

**Task worker runtime**

### `cb_claim_tasks`
- **What it does**: Claim task runs from the queue.
- **Inputs**: `cb_claim_tasks(name text, quantity int, hide_for int)`
- **Returns**: `RETURNS SETOF cb_task_claim`

### `cb_hide_tasks`
- **What it does**: Hide task runs from being read by workers.
- **Inputs**: `cb_hide_tasks(name text, ids bigint[], hide_for integer)`
- **Returns**: `RETURNS void`

### `cb_complete_task`
- **What it does**: Mark a task run as completed.
- **Inputs**: `cb_complete_task(name text, id bigint, output jsonb)`
- **Returns**: `RETURNS void`

### `cb_fail_task`
- **What it does**: Mark a task run as failed.
- **Inputs**: `cb_fail_task(name text, id bigint, error_message text)`
- **Returns**: `RETURNS void`

### `cb_claim_task_on_fail`
- **What it does**: Claim failed task runs for on-fail handling.
- **Inputs**: `cb_poll_task_on_fail(name text, quantity int)`
- **Returns**: `RETURNS TABLE(id bigint, input jsonb, error_message text, attempts int, on_fail_attempts int, started_at timestamptz, failed_at timestamptz, concurrency_key text, idempotency_key text)`

### `cb_complete_task_on_fail`
- **What it does**: Mark on-fail handling as completed for a task run.
- **Inputs**: `cb_complete_task_on_fail(name text, id bigint)`
- **Returns**: `RETURNS void`

### `cb_fail_task_on_fail`
- **What it does**: Mark on-fail handling as failed and schedule retry (`retry_delay` is in milliseconds).
- **Inputs**: `cb_fail_task_on_fail(name text, id bigint, error_message text, retry_exhausted boolean, retry_delay bigint)`
- **Returns**: `RETURNS void`

**Flow worker runtime**

### `cb_start_steps`
- **What it does**: Start steps in a flow that are ready to run.
- **Inputs**: `cb_start_steps(flow_name text, flow_run_id bigint, initial_visible_at timestamptz DEFAULT NULL)`
- **Returns**: `RETURNS void`

### `cb_claim_steps`
- **What it does**: Claim step runs from a flow.
- **Inputs**: `cb_claim_steps(flow_name text, step_name text, quantity int, hide_for int)`
- **Returns**: `RETURNS SETOF cb_step_claim`

### `cb_hide_steps`
- **What it does**: Hide step runs from being read by workers.
- **Inputs**: `cb_hide_steps(flow_name text, step_name text, ids bigint[], hide_for integer)`
- **Returns**: `RETURNS void`

### `cb_complete_step`
- **What it does**: Mark a step run as completed.
- **Inputs**: `cb_complete_step(flow_name text, step_name text, step_id bigint, output jsonb)`
- **Returns**: `RETURNS void`

### `cb_fail_step`
- **What it does**: Mark a step run as failed.
- **Inputs**: `cb_fail_step(flow_name text, step_name text, step_id bigint, error_message text)`
- **Returns**: `RETURNS void`

### `cb_claim_map_tasks`
- **What it does**: Claim map-item tasks for a specific map step.
- **Inputs**: `cb_claim_map_tasks(flow_name text, step_name text, quantity int, hide_for int)`
- **Returns**: `RETURNS TABLE(id bigint, flow_run_id bigint, attempts int, input jsonb, step_outputs jsonb, signal_input jsonb, item jsonb)`

### `cb_hide_map_tasks`
- **What it does**: Hide map tasks from workers for a duration.
- **Inputs**: `cb_hide_map_tasks(flow_name text, step_name text, ids bigint[], hide_for int)`
- **Returns**: `RETURNS void`

### `cb_spawn_generator_map_tasks`
- **What it does**: Spawn map tasks for a generator step batch.
- **Inputs**: `cb_spawn_generator_map_tasks(flow_name text, step_name text, step_id bigint, items jsonb, visible_at timestamptz DEFAULT NULL)`
- **Returns**: `RETURNS int`

### `cb_complete_generator_step`
- **What it does**: Mark generator as complete and finalize parent step if all map tasks are complete.
- **Inputs**: `cb_complete_generator_step(flow_name text, step_name text, step_id bigint)`
- **Returns**: `RETURNS void`

### `cb_fail_generator_step`
- **What it does**: Mark generator as failed and fail parent step/flow.
- **Inputs**: `cb_fail_generator_step(flow_name text, step_name text, step_id bigint, error_message text)`
- **Returns**: `RETURNS void`

### `cb_complete_map_task`
- **What it does**: Complete one map task and complete parent step when all items finish.
- **Inputs**: `cb_complete_map_task(flow_name text, step_name text, map_task_id bigint, output jsonb)`
- **Returns**: `RETURNS void`

### `cb_fail_map_task`
- **What it does**: Fail one map task and fail parent step/flow.
- **Inputs**: `cb_fail_map_task(flow_name text, step_name text, map_task_id bigint, error_message text)`
- **Returns**: `RETURNS void`

### `cb_wait_flow_step_output`
- **What it does**: Long-poll for a step reaching terminal state.
- **Inputs**: `cb_wait_flow_step_output(flow_name text, run_id bigint, step_name text, poll_for int DEFAULT 5000, poll_interval int DEFAULT 200)`
- **Returns**: `RETURNS TABLE(status text, output jsonb, error_message text)`

### `cb_get_flow_step_output`
- **What it does**: Fetch a completed step output on demand.
- **Inputs**: `cb_get_flow_step_output(flow_name text, flow_run_id bigint, step_name text)`
- **Returns**: `RETURNS jsonb`

### `cb_get_flow_step_status`
- **What it does**: Read status details for a single step run.
- **Inputs**: `cb_get_flow_step_status(flow_name text, run_id bigint, step_name text)`
- **Returns**: `RETURNS TABLE(id bigint, status text, attempts int, output jsonb, error_message text, created_at timestamptz, visible_at timestamptz, started_at timestamptz, completed_at timestamptz, failed_at timestamptz, skipped_at timestamptz, canceled_at timestamptz)`

### `cb_complete_flow_early`
- **What it does**: Complete a flow run early and cancel remaining work.
- **Inputs**: `cb_complete_flow_early(flow_name text, flow_run_id bigint, step_name text, output jsonb, reason text DEFAULT NULL)`
- **Returns**: `RETURNS boolean` (`true` when the run exists, `false` when the run does not exist)

### `cb_claim_flow_on_fail`
- **What it does**: Claim failed flow runs for on-fail handling.
- **Inputs**: `cb_poll_flow_on_fail(name text, quantity int)`
- **Returns**: `RETURNS TABLE(id bigint, input jsonb, error_message text, on_fail_attempts int, started_at timestamptz, failed_at timestamptz, concurrency_key text, idempotency_key text, failed_step_name text, failed_step_input jsonb, failed_step_signal_input jsonb, failed_step_attempts int)`

### `cb_complete_flow_on_fail`
- **What it does**: Mark on-fail handling as completed for a flow run.
- **Inputs**: `cb_complete_flow_on_fail(name text, id bigint)`
- **Returns**: `RETURNS void`

### `cb_fail_flow_on_fail`
- **What it does**: Mark on-fail handling as failed and schedule retry (`retry_delay` is in milliseconds).
- **Inputs**: `cb_fail_flow_on_fail(name text, id bigint, error_message text, retry_exhausted boolean, retry_delay bigint)`
- **Returns**: `RETURNS void`

**Flow cancellation internals**

### `cb_maybe_finalize_flow_cancellation`
- **What it does**: Finalize flow cancellation when no active step/map-task work remains.
- **Inputs**: `cb_maybe_finalize_flow_cancellation(name text, run_id bigint)`
- **Returns**: `RETURNS boolean`

### `cb_finalize_flow_cancellation`
- **What it does**: Force final cancellation state for a flow run and stop remaining pending work.
- **Inputs**: `cb_finalize_flow_cancellation(name text, run_id bigint)`
- **Returns**: `RETURNS boolean`

### `cb_cancel_step_run`
- **What it does**: Cancel a specific step run within a flow.
- **Inputs**: `cb_cancel_step_run(flow_name text, step_id bigint)`
- **Returns**: `RETURNS bigint`

### `cb_cancel_map_task_run`
- **What it does**: Cancel a specific map task run for a flow step.
- **Inputs**: `cb_cancel_map_task_run(flow_name text, map_task_id bigint)`
- **Returns**: `RETURNS bigint`

**Scheduler internals**

### `cb_advance_task_schedule`
- **What it does**: Move a task schedule’s `next_run_at` to its next cron tick, respecting the catch-up policy.
- **Inputs**: `cb_advance_task_schedule(id bigint, policy text DEFAULT ‘one’)`
- **Returns**: `RETURNS void`

### `cb_advance_flow_schedule`
- **What it does**: Move a flow schedule’s `next_run_at` to its next cron tick, respecting the catch-up policy.
- **Inputs**: `cb_advance_flow_schedule(id bigint, policy text DEFAULT ‘one’)`
- **Returns**: `RETURNS void`

### `cb_execute_due_task_schedules`
- **What it does**: Execute due task schedules for selected task names in batches.
- **Inputs**: `cb_execute_due_task_schedules(task_names text[], batch_size int DEFAULT 32)`
- **Returns**: `RETURNS int`

### `cb_execute_due_flow_schedules`
- **What it does**: Execute due flow schedules for selected flow names in batches.
- **Inputs**: `cb_execute_due_flow_schedules(flow_names text[], batch_size int DEFAULT 32)`
- **Returns**: `RETURNS int`

**Worker bookkeeping & garbage collection**

### `cb_worker_started`
- **What it does**: Register a worker with the system.
- **Inputs**: `cb_worker_started(id uuid, task_handlers jsonb, step_handlers jsonb)`
- **Returns**: `RETURNS void`

### `cb_worker_heartbeat`
- **What it does**: Update worker heartbeat and run cleanup.
- **Inputs**: `cb_worker_heartbeat(id uuid)`
- **Returns**: `RETURNS jsonb`
- **Returned JSON shape**:
	- `{ "gc_info": { "expired_queues_deleted": int, "stale_workers_deleted": int, "task_runs_purged": int, "flow_runs_purged": int } }`

**Utility helpers (advanced)**

### `cb_parse_condition`
- **What it does**: Canonical condition parser for all clients (go, python, js, ruby, java, etc.).
- **Inputs**: `cb_parse_condition(expr text)`
- **Returns**: `RETURNS jsonb`

### `cb_parse_condition_value`
- **What it does**: Parse a condition value string into appropriate JSON type.
- **Inputs**: `cb_parse_condition_value(val_str text, op text)`
- **Returns**: `RETURNS jsonb`

### `cb_evaluate_condition`
- **What it does**: Evaluate a parsed condition against step output.
- **Inputs**: `cb_evaluate_condition(condition jsonb, output jsonb)`
- **Returns**: `RETURNS boolean`

### `cb_get_jsonb_field`
- **What it does**: Extract a value from jsonb using a field path.
- **Inputs**: `cb_get_jsonb_field(obj jsonb, field_path text)`
- **Returns**: `RETURNS jsonb`

### `cb_evaluate_condition_expr`
- **What it does**: Wrapper that takes text condition.
- **Inputs**: `cb_evaluate_condition_expr(condition_expr text, output jsonb)`
- **Returns**: `RETURNS boolean`

### `cb_cron_expand_spec`
- **What it does**: Expand cron aliases/presets into canonical cron field format.
- **Inputs**: `cb_cron_expand_spec(cron_spec text)`
- **Returns**: `RETURNS text`

### `cb_cron_parse_field`
- **What it does**: Parse one cron field expression into the allowed integer values for that field.
- **Inputs**: `cb_cron_parse_field(field text, min_value int, max_value int, allow_seven_as_sunday boolean DEFAULT false)`
- **Returns**: `RETURNS int[]`

### `cb_cron_matches`
- **What it does**: Determine whether a timestamp matches a cron specification.
- **Inputs**: `cb_cron_matches(cron_spec text, ts timestamptz)`
- **Returns**: `RETURNS boolean`

### `cb_next_cron_tick`
- **What it does**: Compute the next timestamp after `from_time` that matches a cron specification.
- **Inputs**: `cb_next_cron_tick(cron_spec text, from_time timestamptz)`
- **Returns**: `RETURNS timestamptz`

### `cb_table_name`
- **What it does**: Generate a PostgreSQL table name for queue/task/flow storage.
- **Inputs**: `cb_table_name(name text, prefix text)`
- **Returns**: `RETURNS text`
