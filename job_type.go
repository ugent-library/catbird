package catbird

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

// Handler runs one job. It is given no connection: a handler that needs one
// opens it, and decides for itself how long to hold it. A handler that returns
// nil without calling Complete leaves the job for the worker to complete
// afterwards, which means its writes committed before the job ended and a crash
// in between runs them again. See Complete.
//
// It is an interface for the reason http.Handler is: a handler with
// dependencies is a struct implementing it instead of a closure over half the
// application, and logging, metrics or panic recovery wrap every handler as a
// func(Handler) Handler. A plain function registers with Runtime.HandleFunc, or
// converts with HandlerFunc.
type Handler interface {
	HandleJob(ctx context.Context, job *Job) error
}

// HandlerFunc makes a plain function a Handler, like http.HandlerFunc.
type HandlerFunc func(ctx context.Context, job *Job) error

// HandleJob calls fn.
func (fn HandlerFunc) HandleJob(ctx context.Context, job *Job) error { return fn(ctx, job) }

// JobHandler is the Handler for a function that takes its payload decoded: it
// unmarshals the job's payload into a T and passes it beside the job, so the
// payload's shape is checked by the compiler instead of read off json.Unmarshal
// calls at the top of the handler. A payload that does not unmarshal fails the
// attempt like any other handler error.
func JobHandler[T any](fn func(ctx context.Context, payload T, job *Job) error) Handler {
	return HandlerFunc(func(ctx context.Context, job *Job) error {
		var payload T
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		return fn(ctx, payload, job)
	})
}

// QueueOptions are the optional parts of NewQueue. Zero values take the
// defaults. Lease and Timeout are the two durations to set with care, and how
// they compare is itself a setting: with Timeout inside Lease — the default —
// a handler must finish within the lease, and with Timeout above Lease the
// worker renews the leases of its running jobs, so a handler may run to
// Timeout while a crashed worker's job still comes back within Lease.
type QueueOptions struct {
	BatchSize int // jobs running at once in this process; default 50

	// Lease is how long one attempt keeps its claim before any worker may take
	// the job again — and so how long a job stays stuck when the process
	// running it crashes. It bounds the handler only on a queue that does not
	// renew; see Timeout. Default 5 minutes.
	Lease time.Duration

	PollInterval time.Duration // wake-up interval when no notification arrives; default 5 seconds

	// Timeout bounds one attempt: the handler's context is cancelled when it
	// passes and the attempt counts as failed like any other. It ends the
	// attempt and not the goroutine — a handler that never looks at its context
	// keeps its slot until it returns. The default keeps the completion's few
	// seconds inside the lease, which makes the lease the bound a handler must
	// finish within: past it another worker may hold the claim, and the late
	// attempt's writes match no row.
	//
	// A Timeout above Lease is how a queue runs jobs longer than its lease: the
	// worker then renews the leases of its running jobs every half Lease, so
	// Timeout alone bounds an attempt and Lease decides how soon a crashed
	// worker's job is claimed again — a queue of hour-long jobs keeps a lease
	// of minutes. Renewal follows the handler's context: a handler that hangs
	// past Timeout is renewed no further, and its job comes back about a lease
	// later like any other overrun.
	Timeout time.Duration

	Logger *slog.Logger // default: the runtime's logger
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
	if o.Timeout <= 0 {
		// Room for the completion inside the lease, and never less than half of
		// it, so a short lease still leaves the handler most of its time.
		o.Timeout = max(o.Lease-afterHandlerTimeout, o.Lease/2)
	}
	return o
}

// Queue is a name and how work runs under it: how many jobs run at once, how
// long an attempt keeps its claim, and how long a handler may run. It decides
// who competes with whom — every job type on one queue takes its slots from the
// same BatchSize — and it is the claim key, the single value a worker probes the
// ready index with.
//
// Lease is here rather than on the job type because the claim sets it for a
// whole batch in one statement, and the renewal, on a queue whose Timeout
// exceeds its Lease, renews a whole batch the same way. A job type whose
// handler runs much longer than its neighbours wants its own queue, which is
// also what its handler holding a slot for that long already argues for.
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
	Signal bool

	// Schedule makes every process that handles this type enqueue a job of it
	// on the minutes the schedule names — five fields separated by spaces:
	// minute (0-59), hour (0-23), day of month (1-31), month (1-12), day of
	// week (0-6, Sunday is 0 and 7 also means Sunday). Each field is "*", a
	// number, a range "8-17", a list "0,30", or a step "*/5", "8-17/2";
	// numbers only, evaluated in UTC. As in cron, when both day fields are
	// restricted a day matching either one counts.
	//
	// The schedule is on the type rather than given at registration so that
	// two processes cannot disagree on it, for the same reason Signal is. At
	// most one job of a scheduled type is live at a time: a tick during a
	// live run — a tick's job or a manual Enqueue of the type — writes
	// nothing, so a run that outlives its schedule swallows the ticks it
	// covers and the next run starts on the first matching minute after it
	// ends. A scheduled job takes no payload; the same code under two
	// schedules or two arguments is two job types sharing one handler.
	Schedule string

	// MaxAttempts is how many attempts a job gets before it is dead; default
	// 15, which with the default backoff rides out about an hour of outage.
	// Dying is the expensive outcome — the workflow is cancelled, OnDead runs
	// once, and nothing re-drives a dead job — so the default errs long.
	MaxAttempts int

	// MinBackoff is the wait after the first failed attempt and the shortest
	// wait there is; doubling it per attempt stops at MaxBackoff. Each wait is
	// drawn at random between the two, so jobs that failed together come back
	// apart. Defaults are one second and ten minutes.
	MinBackoff time.Duration
	MaxBackoff time.Duration

	OnDead Handler // runs once after the last failed attempt
}

func (o JobTypeOptions) withDefaults() JobTypeOptions {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 15
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = time.Second
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 10 * time.Minute
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
	name     string
	queue    *Queue
	opts     JobTypeOptions
	schedule *schedule // parsed opts.Schedule; nil on an unscheduled type
}

// NewJobType declares a job type. Runtime.Handle gives it a handler and makes a
// process run it; enqueueing it needs only the value.
//
// A bad Schedule panics here rather than failing on every tick once the
// runtime is started, and so does a Schedule beside Signal: a gated job never
// becomes claimable on its own, so its claim would hold the live-run guard
// against every later tick.
func NewJobType(name string, queue *Queue, opts JobTypeOptions) *JobType {
	if queue == nil {
		panic("catbird: job type " + name + " has no queue")
	}
	t := &JobType{name: name, queue: queue, opts: opts.withDefaults()}
	if opts.Schedule != "" {
		if opts.Signal {
			panic("catbird: job type " + name + " waits for a signal and cannot run on a schedule")
		}
		s, err := parseSchedule(opts.Schedule)
		if err != nil {
			panic(err)
		}
		if s.next(time.Now()).IsZero() {
			panic("catbird: job type " + name + ": schedule " + opts.Schedule + " matches no time")
		}
		t.schedule = &s
	}
	return t
}

// Name is the job type's name, as it is stored on a claim and on a result.
func (t *JobType) Name() string { return t.name }

// Queue is the queue this type's jobs are claimed from.
func (t *JobType) Queue() *Queue { return t.queue }
