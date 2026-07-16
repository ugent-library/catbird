package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RunOpts tunes a run. Zero fields mean the defaults.
type RunOpts struct {
	// Key deduplicates and names the run: a second Run of the same job
	// with the same key — live or finished, within the job's retention —
	// returns the existing run's id, and GetRunByKey finds the run again.
	Key string
	// Delay holds the first step back: it becomes claimable after this.
	Delay time.Duration
}

// Run creates a run of the job and returns its id. It works on any
// connection, so an application can enqueue a run inside its own
// transaction. existing reports that a run with this job and key was
// already there and nothing was created.
func Run(ctx context.Context, conn Conn, job string, input any, opts ...RunOpts) (runID int64, existing bool, err error) {
	var o RunOpts
	if len(opts) > 0 {
		o = opts[0]
	}

	var in any
	if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			return 0, false, err
		}
		in = json.RawMessage(b)
	}

	err = conn.QueryRow(ctx,
		`SELECT r.run_id, r.existing FROM cb_job_run($1, $2, $3, $4) r`,
		job, in, nullText(o.Key), nullInterval(o.Delay),
	).Scan(&runID, &existing)
	return runID, existing, wrapErr(err)
}

// RunInfo is the durable run handle: one row in cb_job_runs, queryable by
// id or by (job, key) for as long as the job's retention keeps it.
type RunInfo struct {
	ID         int64
	Job        string // the birth job
	Key        string
	Status     string
	Input      json.RawMessage
	Output     json.RawMessage
	Error      string
	CreatedAt  time.Time
	FinishedAt time.Time // zero until the run is terminal
}

const runInfoColumns = `id, job, key, status, input, output, error, created_at, finished_at`

// GetRun looks a run up by id.
func GetRun(ctx context.Context, conn Conn, runID int64) (*RunInfo, error) {
	return scanRunInfo(conn.QueryRow(ctx,
		`SELECT `+runInfoColumns+` FROM cb_job_runs WHERE id = $1`, runID))
}

// GetRunByKey looks a run up by its application key.
func GetRunByKey(ctx context.Context, conn Conn, job, key string) (*RunInfo, error) {
	return scanRunInfo(conn.QueryRow(ctx,
		`SELECT `+runInfoColumns+` FROM cb_job_runs WHERE job = $1 AND key = $2`, job, key))
}

func scanRunInfo(row pgx.Row) (*RunInfo, error) {
	var r RunInfo
	var key, errMsg *string
	var finishedAt *time.Time
	if err := row.Scan(&r.ID, &r.Job, &key, &r.Status, &r.Input, &r.Output,
		&errMsg, &r.CreatedAt, &finishedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if key != nil {
		r.Key = *key
	}
	if errMsg != nil {
		r.Error = *errMsg
	}
	if finishedAt != nil {
		r.FinishedAt = *finishedAt
	}
	return &r, nil
}

// WaitOpts tunes WaitForOutput. Zero fields mean the defaults.
type WaitOpts struct {
	PollInterval time.Duration // 250ms
}

// WaitForOutput polls the run until it is terminal and unmarshals its
// output into out (pass nil to only wait). A failed run returns
// ErrRunFailed with the run's error, a canceled run ErrRunCanceled with
// the cancel reason. ctx bounds the wait.
func WaitForOutput(ctx context.Context, conn Conn, runID int64, out any, opts ...WaitOpts) error {
	var o WaitOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	poll := o.PollInterval
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}

	timer := time.NewTimer(poll)
	defer timer.Stop()
	for {
		var status string
		var output json.RawMessage
		var errMsg *string
		err := conn.QueryRow(ctx,
			`SELECT status, output, error FROM cb_job_runs WHERE id = $1`, runID,
		).Scan(&status, &output, &errMsg)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		switch status {
		case StatusCompleted:
			if out == nil || output == nil {
				return nil
			}
			return json.Unmarshal(output, out)
		case StatusFailed:
			if errMsg != nil {
				return fmt.Errorf("%w: %s", ErrRunFailed, *errMsg)
			}
			return ErrRunFailed
		case StatusCanceled:
			if errMsg != nil {
				return fmt.Errorf("%w: %s", ErrRunCanceled, *errMsg)
			}
			return ErrRunCanceled
		}

		timer.Reset(poll)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Cancel cancels a run. Steps not yet started never run; a handler that is
// already running has its context canceled on the worker's next extend. A
// 'failing' run keeps its failed verdict — cancel only stops the cleanup
// chain. Reports false when the run does not exist or is already finished.
func Cancel(ctx context.Context, conn Conn, runID int64, reason string) (bool, error) {
	var applied bool
	err := conn.QueryRow(ctx, `SELECT cb_job_cancel($1, $2)`,
		runID, nullText(reason)).Scan(&applied)
	return applied, wrapErr(err)
}
