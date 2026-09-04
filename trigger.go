package catbird

import (
	"context"
	"time"
)

// TriggerOptions are the optional parts of Trigger. Zero values take the
// defaults.
type TriggerOptions struct {
	BatchSize    int           // messages turned into jobs per round; default 50
	PollInterval time.Duration // wait when the stream is empty; default 2 seconds
}

func (o TriggerOptions) withDefaults() TriggerOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = 50
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	return o
}

// trigger turns stream messages into jobs.
type trigger struct {
	runtime *Runtime
	name    string
	jobType *JobType
	cursor  Cursor
	opts    TriggerOptions
}

// Trigger registers a trigger on the runtime: once it is started, every message
// matching patterns becomes a job of jobType. The patterns are the ones the
// stream reads take — a topic, a prefix followed by ".#", or "#" — and a pattern
// that does not compile panics here rather than failing on every round once the
// runtime is started. So does a job type declared with Signal: a trigger's jobs
// have no ids for a caller to signal, as with EnqueueBatch. So does a scheduled
// type: its jobs carry the type's name as their unique key, and a trigger cannot
// give them one, because a trigger never drops the message a duplicate would
// come from.
//
// The jobs carry the stream message's topic, so a handler can see which message
// caused it; the topic decides nothing, the job type does. The enqueues and the
// cursor advance commit together, and each job carries a deduplication key
// derived from the message id, so a crash or a second process running the same
// trigger cannot produce a second job for the same message.
func (r *Runtime) Trigger(name string, patterns []string, jobType *JobType, opts TriggerOptions) {
	if _, _, err := compilePatterns(patterns, 1); err != nil {
		panic(err)
	}
	if jobType.opts.Signal {
		panic("catbird: trigger " + name + ": job type " + jobType.name + " waits for a signal and cannot be enqueued by a trigger")
	}
	if jobType.opts.Schedule != "" {
		panic("catbird: trigger " + name + ": job type " + jobType.name + " is scheduled and cannot be enqueued by a trigger")
	}
	t := &trigger{
		runtime: r,
		name:    name,
		jobType: jobType,
		cursor:  Cursor{Name: "trigger:" + name, Patterns: patterns},
		opts:    opts.withDefaults(),
	}
	r.declare("cb_stream", t.start)
}

// start runs the trigger until ctx is canceled: a batch whenever the assigner
// announces new positions, and every PollInterval in case a notification was
// lost.
func (t *trigger) start(ctx context.Context) {
	wake, unsubscribe := t.runtime.subscribe("cb_stream")
	defer unsubscribe()

	for ctx.Err() == nil {
		n, err := t.enqueueNextBatch(ctx)
		if err != nil && ctx.Err() == nil {
			t.runtime.opts.Logger.Error("catbird: trigger failed", "trigger", t.name, "err", err)
		}
		if err == nil && n == t.opts.BatchSize {
			continue // the stream may hold more
		}
		select {
		case <-ctx.Done():
		case <-wake:
		case <-time.After(t.opts.PollInterval):
		}
	}
}

// enqueueNextBatch reads the next stream messages matching the trigger's
// patterns and enqueues a job for each, all in one statement: the read after
// the cursor, the job messages and their job rows, the cursor moved to the last
// position read, and one wake for the target queue. Each job's deduplication
// key is trigger:<name>: followed by the source message's id, so a second
// process running the same trigger reads the same batch but creates no second
// job. Returns how many messages it read — not how many jobs it created,
// because a batch another process already handled is still progress past the
// cursor.
//
// One statement rather than Cursor.Read feeding EnqueueBatch, because a
// trigger transforms nothing: the topic and the payload pass through and the
// key derives from the id. Reading the batch into Go and enqueueing it back
// would move every payload out of the database and straight in again — five
// round trips (read, begin, enqueue, ack, commit) against one, and the
// payloads across the wire twice — to compute nothing. EnqueueBatch is for a
// caller whose rows are not already in cb_messages.
//
// The cursor insert is guarded, so an empty batch acks nothing and an idle
// trigger writes no rows. The wake reads the job rows through LIMIT 1 and the
// final SELECT references it so that it runs, both as in EnqueueBatch.
func (t *trigger) enqueueNextBatch(ctx context.Context) (int, error) {
	matchSQL, args, err := compilePatterns(t.cursor.Patterns, 6)
	if err != nil {
		return 0, err
	}
	var handled int
	err = t.runtime.pool.QueryRow(ctx, `
		WITH source AS (
			SELECT id, position, topic, payload
			FROM cb_messages
			WHERE position > COALESCE((SELECT last_position FROM cb_cursors WHERE name = $1), 0)
			  AND `+matchSQL+`
			ORDER BY position ASC
			LIMIT $2
		),
		message AS (
			INSERT INTO cb_messages (topic, payload, deduplication_key)
			SELECT topic, payload, $3::text || id FROM source
			ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING
			RETURNING id
		),
		job AS (
			INSERT INTO cb_jobs (message_id, queue, job_type, claimable_at)
			SELECT id, $4, $5, now() FROM message
			RETURNING message_id
		),
		cursor AS (
			INSERT INTO cb_cursors (name, last_position)
			SELECT $1, max(position) FROM source HAVING count(*) > 0
			ON CONFLICT (name) DO UPDATE SET last_position = GREATEST(cb_cursors.last_position, EXCLUDED.last_position)
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || $4, '') FROM (SELECT 1 FROM job LIMIT 1) one
		)
		SELECT count(*) FROM source LEFT JOIN wake ON true
	`, append([]any{t.cursor.Name, t.opts.BatchSize, "trigger:" + t.name + ":",
		t.jobType.queue.name, t.jobType.name}, args...)...).Scan(&handled)
	return handled, err
}
