package catbird

import (
	"context"
	"log/slog"
	"time"
)

// Handler runs one job. It is given no connection: a handler that needs one
// opens it, and decides for itself how long to hold it. A handler that returns
// nil without calling Complete leaves the job for the worker to complete
// afterwards, which means its writes committed before the job ended and a crash
// in between runs them again. See Complete.
type Handler func(ctx context.Context, job *Job) error

// QueueOptions are the optional parts of NewQueue. Zero values take the defaults.
type QueueOptions struct {
	BatchSize    int           // jobs running at once; default 50
	Lease        time.Duration // how long one attempt may run; default 5 minutes
	PollInterval time.Duration // wake-up interval when no notification arrives; default 5 seconds
	Logger       *slog.Logger  // default: the runtime's logger
}

func (o QueueOptions) withDefaults() QueueOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = 50
	}
	if o.Lease <= 0 {
		o.Lease = 5 * time.Minute
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 5 * time.Second
	}
	return o
}

// Queue is a name and how work runs under it: how many jobs run at once, and
// how long one attempt may take. It decides who competes with whom — every job
// type on one queue takes its slots from the same BatchSize — and it is the
// claim key, the single value a worker probes the ready index with.
//
// Lease is here rather than on the job type because the claim sets it for a
// whole batch in one statement. A job type whose handler runs much longer than
// its neighbours wants its own queue, which is also what its handler holding a
// slot for that long already argues for.
type Queue struct {
	name string
	opts QueueOptions
}

// NewQueue declares a queue. Nothing is written anywhere: the value is the
// declaration, and only its name reaches a row.
func NewQueue(name string, opts QueueOptions) *Queue {
	return &Queue{name: name, opts: opts.withDefaults()}
}

// Name is the queue's name, as it is stored on a claim.
func (q *Queue) Name() string { return q.name }

// JobTypeOptions are the optional parts of NewJobType. Zero values take the
// defaults.
type JobTypeOptions struct {
	// Signal makes every job of this type wait for a payload before it runs.
	// The enqueue stamps it on the row, so the handler is always given one:
	// job.Signal is never nil for a type declared with it, and never anything
	// but nil for a type without it.
	Signal      bool
	MaxAttempts int // attempts before the job is dead; default 5

	// MinBackoff is the wait after the first failed attempt and the shortest
	// wait there is; doubling it per attempt stops at MaxBackoff. Each wait is
	// drawn at random between the two, so jobs that failed together come back
	// apart. Defaults are one second and one minute.
	MinBackoff time.Duration
	MaxBackoff time.Duration

	OnDead Handler // runs once after the last failed attempt
}

func (o JobTypeOptions) withDefaults() JobTypeOptions {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = time.Second
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = time.Minute
	}
	if o.MaxBackoff < o.MinBackoff {
		// A ceiling under the floor would draw the wait backwards and schedule
		// the retry in the past, so the floor wins and the wait does not grow.
		o.MaxBackoff = o.MinBackoff
	}
	return o
}

// JobType is a kind of job: its name, the queue it runs on, and how a run of it
// is retried. Everything on it is either stamped on the claim or decided about a
// run; the function that runs it is not here, because a process that only
// enqueues has no use for it. Both the enqueue and the worker take the value, so
// what a caller creates and what a handler is given cannot disagree — a type
// declared with Signal is always enqueued waiting for one.
//
// Nothing is declared in the database. Only the name reaches a row, beside the
// queue's, so adding a job type is not a migration.
type JobType struct {
	name  string
	queue *Queue
	opts  JobTypeOptions
}

// NewJobType declares a job type. Runtime.Handle gives it a handler and makes a
// process run it; enqueueing it needs only the value.
func NewJobType(name string, queue *Queue, opts JobTypeOptions) *JobType {
	if queue == nil {
		panic("catbird: job type " + name + " has no queue")
	}
	return &JobType{name: name, queue: queue, opts: opts.withDefaults()}
}

// Name is the job type's name, as it is stored on a claim and on a result.
func (t *JobType) Name() string { return t.name }

// Queue is the queue this type's jobs are claimed from.
func (t *JobType) Queue() *Queue { return t.queue }
