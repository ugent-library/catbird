package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// JobOpts is the job's whole config: a zero field means the stock value,
// never "keep the current value".
type JobOpts struct {
	// Queue names the pool the job routes to; "" means 'default'. The
	// pool's row carries the job's retry and claim terms, and it must be
	// defined first.
	Queue string
	// OnFail names the job spawned when a run of this job is given up
	// on — after max_attempts failed or crashed starts. It receives
	// {job, error, input} describing the failed step, must be defined
	// first (a job may name itself), and applies when this job is the
	// run's birth job.
	OnFail string
	// Retention is how long this job's finished runs are kept before the
	// prune tick deletes them: 30 days by default, Forever to keep them
	// all. It applies when this job is the run's birth job.
	Retention time.Duration
}

// Define declares a job and its whole config in one call, mirroring
// cb_job_define: creating and updating are the same call, and an identical
// declaration writes nothing. A queue or on_fail naming something not
// defined is a deploy error.
func Define(ctx context.Context, conn Conn, job string, opts ...JobOpts) error {
	var o JobOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx, `SELECT cb_job_define($1, $2, $3, $4)`,
		job, nullText(o.Queue), nullText(o.OnFail), nullInterval(o.Retention))
	return wrapErr(err)
}

// QueueOpts are the pool's terms, written whole: a zero field means the
// stock value — the same values the migration seeds for 'default' — never
// "keep the current value".
type QueueOpts struct {
	// ClaimTTL is the lease length: how long a worker may go without
	// extending before its steps count as crashed and fall to another
	// worker. Stock: 30s.
	ClaimTTL time.Duration
	// ClaimBatchSize is how many steps one claim call hands out. Stock: 10.
	ClaimBatchSize int
	// MaxAttempts bounds how many times a step's handler may begin —
	// verdicts and crashes spend the same budget. Stock: 3.
	MaxAttempts int
	// Backoff paces retries: FullJitterBackoff, FixedBackoff or
	// NoBackoff. Stock: full_jitter, 1s to 1m.
	Backoff Backoff
}

// DefineQueue declares a pool and its terms in one call, mirroring
// cb_job_define_queue. 'default' is seeded by the migration and
// redeclarable like any pool.
func DefineQueue(ctx context.Context, conn Conn, queue string, opts ...QueueOpts) error {
	var o QueueOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_job_define_queue($1, $2, $3, $4, $5, $6, $7)`,
		queue,
		nullInterval(o.ClaimTTL),
		nullInt(o.ClaimBatchSize),
		nullInt(o.MaxAttempts),
		nullText(string(o.Backoff.Kind)),
		nullInterval(o.Backoff.Base),
		nullInterval(o.Backoff.Max))
	return wrapErr(err)
}

// ScheduleOpts tune a schedule: a zero field means the default.
type ScheduleOpts struct {
	// CatchUp decides what a backlog gets when the tick was down past one
	// or more slots: CatchUpAll fires a run per missed slot, CatchUpSkip
	// (the default) drops the backlog and fires only an on-time slot.
	CatchUp CatchUpPolicy
	// Input is what every scheduled run is created with.
	Input any
	// StartAt sets the next fire directly — the one deliberate poke at
	// engine-managed state.
	StartAt time.Time
}

// DefineSchedule declares a scheduled run: every interval, the tick
// creates a run of the job. The interval must be hours or less — days,
// months and years need cron. A redeclaration keeps the firing phase; a
// changed cadence re-anchors it to now + every.
func DefineSchedule(ctx context.Context, conn Conn, name, job string, every time.Duration, opts ...ScheduleOpts) error {
	var o ScheduleOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if every <= 0 {
		return fmt.Errorf("catbird: schedule %s needs a positive interval (got %s)", name, every)
	}

	var input any
	if o.Input != nil {
		b, err := json.Marshal(o.Input)
		if err != nil {
			return err
		}
		input = json.RawMessage(b)
	}

	_, err := conn.Exec(ctx,
		`SELECT cb_job_define_schedule($1, $2, $3, $4, $5, $6)`,
		name, job, nullInterval(every), nullText(string(o.CatchUp)),
		input, nullTime(o.StartAt))
	return wrapErr(err)
}

// DeleteSchedule stops a schedule. It reports whether one existed;
// deleting a missing schedule is a no-op.
func DeleteSchedule(ctx context.Context, conn Conn, name string) (bool, error) {
	var deleted bool
	err := conn.QueryRow(ctx, `SELECT cb_job_delete_schedule($1)`, name).Scan(&deleted)
	return deleted, wrapErr(err)
}
