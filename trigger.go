package catbird

import (
	"context"
	"strconv"
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
// runtime is started.
//
// The jobs carry the stream message's topic, so a handler can see which message
// caused it; the topic decides nothing, the job type does. The enqueues and the
// cursor advance commit in one transaction, and each job carries a
// deduplication key derived from the message id, so a crash or a second
// process running the same trigger cannot produce a second job for the same
// message.
func (r *Runtime) Trigger(name string, patterns []string, jobType *JobType, opts TriggerOptions) {
	if _, _, err := compilePatterns(patterns, 1); err != nil {
		panic(err)
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

// enqueueNextBatch reads the next batch of matching stream messages, enqueues a
// job for each, advances the cursor, and commits — all in one transaction. The
// whole batch is one EnqueueBatch statement, so a trigger costs one round trip
// per batch and wakes the target queue once. Returns how many messages it
// handled.
func (t *trigger) enqueueNextBatch(ctx context.Context) (int, error) {
	msgs, err := t.cursor.Read(ctx, t.runtime.pool, t.opts.BatchSize)
	if err != nil || len(msgs) == 0 {
		return 0, err
	}

	jobs := make([]BatchMessage, len(msgs))
	for i, m := range msgs {
		jobs[i] = BatchMessage{
			Topic:            m.Topic,
			Payload:          m.Payload,
			DeduplicationKey: "trigger:" + t.name + ":" + strconv.FormatInt(m.ID, 10),
		}
	}

	tx, err := t.runtime.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	if _, err := EnqueueBatch(ctx, tx, t.jobType, jobs, EnqueueOptions{}); err != nil {
		return 0, err
	}
	if err := t.cursor.Ack(ctx, tx, msgs[len(msgs)-1].Position); err != nil {
		return 0, err
	}
	return len(msgs), tx.Commit(ctx)
}
