package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// stepStatus reads a step's status by name; "" when the step does not
// exist yet.
func stepStatus(t *testing.T, pool *pgxpool.Pool, runID int64, name string) string {
	t.Helper()
	var status string
	err := pool.QueryRow(context.Background(),
		`SELECT status FROM cb_job_steps WHERE run_id = $1 AND name = $2`,
		runID, name).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func TestPlanChain(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq1"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_chain_first", "go_chain_second"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq1"}); err != nil {
			t.Fatal(err)
		}
	}

	w := NewWorker(pool)
	w.Handle("go_chain_first", func(ctx context.Context, p *Plan, in struct {
		N int `json:"n"`
	}) (map[string]string, error) {
		runIn, err := RunInput[map[string]int](p)
		if err != nil || runIn["n"] != 1 {
			return nil, fmt.Errorf("run input = %v (%v)", runIn, err)
		}
		p.Step("go_chain_second", map[string]int{"n": in.N + 1})
		p.SetRunOutput(map[string]string{"run": "set early"})
		return map[string]string{"first": "done"}, nil
	})
	w.Handle("go_chain_second", func(ctx context.Context, in struct {
		N int `json:"n"`
	}) (map[string]int, error) {
		return map[string]int{"n": in.N * 10}, nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_chain_first", map[string]int{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	// the run's output is only what SetRunOutput set; the finishing
	// step's own output stays on its row
	var out map[string]string
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out["run"] != "set early" {
		t.Fatalf("run output = %v", out)
	}

	var completed int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_job_steps WHERE run_id = $1 AND status = 'completed'`, id).
		Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 2 {
		t.Fatalf("completed steps = %d", completed)
	}
	var firstOut, secondIn string
	var parent int64
	if err := pool.QueryRow(ctx,
		`SELECT s1.output::text, s2.input::text, s2.parent_step_id
		 FROM cb_job_steps s1, cb_job_steps s2
		 WHERE s1.run_id = $1 AND s1.id = 1 AND s2.run_id = $1 AND s2.id = 2`, id).
		Scan(&firstOut, &secondIn, &parent); err != nil {
		t.Fatal(err)
	}
	if firstOut != `{"first": "done"}` || secondIn != `{"n": 2}` || parent != 1 {
		t.Fatalf("steps = (%s, %s, %d)", firstOut, secondIn, parent)
	}
}

func TestFanOutBarrier(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq2"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_fb_split", "go_fb_work", "go_fb_join"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq2"}); err != nil {
			t.Fatal(err)
		}
	}

	w := NewWorker(pool)
	w.Handle("go_fb_split", func(ctx context.Context, p *Plan, in struct{}) (map[string]int, error) {
		for i := 1; i <= 3; i++ {
			p.Step("go_fb_work", map[string]int{"i": i})
		}
		p.After().Step("go_fb_join", nil)
		return map[string]int{"parts": 3}, nil
	})
	w.Handle("go_fb_work", func(ctx context.Context, in struct {
		I int `json:"i"`
	}) (map[string]int, error) {
		return map[string]int{"sq": in.I * in.I}, nil
	})
	w.Handle("go_fb_join", func(ctx context.Context, p *Plan, in struct{}) error {
		outs, err := StepOutputs[struct {
			Sq int `json:"sq"`
		}](p, "go_fb_work")
		if err != nil {
			return err
		}
		sum := 0
		for _, o := range outs {
			sum += o.Sq
		}
		split, err := StepOutput[map[string]int](p, "go_fb_split")
		if err != nil {
			return err
		}
		if _, err := StepOutput[map[string]int](p, "go_fb_work"); err == nil ||
			!strings.Contains(err.Error(), "StepOutputs") {
			return fmt.Errorf("StepOutput on a fan-out = %v", err)
		}
		p.SetRunOutput(map[string]int{"sum": sum, "parts": split["parts"]})
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_fb_split", nil)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]int
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out["sum"] != 14 || out["parts"] != 3 {
		t.Fatalf("output = %v", out)
	}

	var completed int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_job_steps WHERE run_id = $1 AND status = 'completed'`, id).
		Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != 5 {
		t.Fatalf("completed steps = %d", completed)
	}
}

func TestSignalDelivered(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq3"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_sig_start", "go_sig_approve"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq3"}); err != nil {
			t.Fatal(err)
		}
	}

	w := NewWorker(pool)
	w.Handle("go_sig_start", func(ctx context.Context, p *Plan, in struct{}) error {
		p.Step("go_sig_approve", map[string]int{"doc": 7}, WaitsForSignal())
		return nil
	})
	w.Handle("go_sig_approve", func(ctx context.Context, p *Plan, in struct {
		Doc int `json:"doc"`
	}) error {
		sig, err := SignalInput[map[string]bool](p)
		if err != nil {
			return err
		}
		p.SetRunOutput(map[string]any{"doc": in.Doc, "ok": sig["ok"]})
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_sig_start", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "the approve step to wait for its signal", func() bool {
		return stepStatus(t, pool, id, "go_sig_approve") == StatusWaitingForSignal
	})

	accepted, err := Signal(ctx, pool, id, "go_sig_approve", map[string]bool{"ok": true})
	if err != nil || !accepted {
		t.Fatalf("signal = (%v, %v)", accepted, err)
	}

	var out struct {
		Doc int  `json:"doc"`
		OK  bool `json:"ok"`
	}
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out.Doc != 7 || !out.OK {
		t.Fatalf("output = %+v", out)
	}

	// a finished or missing run accepts nothing, and neither is an error
	accepted, err = Signal(ctx, pool, id, "go_sig_approve", nil)
	if err != nil || accepted {
		t.Fatalf("signal after finish = (%v, %v)", accepted, err)
	}
	accepted, err = Signal(ctx, pool, id+999999, "go_sig_approve", nil)
	if err != nil || accepted {
		t.Fatalf("signal to missing run = (%v, %v)", accepted, err)
	}
}

func TestSignalBuffered(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq4"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_buf_start", "go_buf_wait"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq4"}); err != nil {
			t.Fatal(err)
		}
	}

	started := make(chan int64, 1)
	proceed := make(chan struct{})
	w := NewWorker(pool)
	w.Handle("go_buf_start", func(ctx context.Context, p *Plan, in struct{}) error {
		started <- p.runID
		select {
		case <-proceed:
		case <-ctx.Done():
			return ctx.Err()
		}
		p.Step("go_buf_wait", nil, WaitsForSignal())
		return nil
	})
	w.Handle("go_buf_wait", func(ctx context.Context, p *Plan, in struct{}) error {
		sig, err := SignalInput[string](p)
		if err != nil {
			return err
		}
		p.SetRunOutput(sig)
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_buf_start", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	// the signal arrives before its step exists: buffered, and consumed
	// the moment the step is added — it never waits
	accepted, err := Signal(ctx, pool, id, "go_buf_wait", "hello")
	if err != nil || !accepted {
		t.Fatalf("signal = (%v, %v)", accepted, err)
	}
	close(proceed)

	var out string
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out != "hello" {
		t.Fatalf("output = %q", out)
	}
	var slots int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_job_signals WHERE run_id = $1`, id).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if slots != 0 {
		t.Fatalf("unconsumed signal slots = %d", slots)
	}
}

func TestAfterStepWaitsForSignal(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq5"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_bw_start", "go_bw_work", "go_bw_gate"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq5"}); err != nil {
			t.Fatal(err)
		}
	}

	w := NewWorker(pool)
	w.Handle("go_bw_start", func(ctx context.Context, p *Plan, in struct{}) error {
		p.Step("go_bw_work", nil)
		p.After().Step("go_bw_gate", nil, WaitsForSignal())
		return nil
	})
	w.Handle("go_bw_work", okHandler)
	w.Handle("go_bw_gate", func(ctx context.Context, p *Plan, in struct{}) error {
		p.SetRunOutput("opened")
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_bw_start", nil)
	if err != nil {
		t.Fatal(err)
	}
	// both waits, in sequence: once the run's other steps drain, the gate
	// step moves on to waiting for its signal
	waitFor(t, 5*time.Second, "the gate step to wait for its signal", func() bool {
		return stepStatus(t, pool, id, "go_bw_gate") == StatusWaitingForSignal
	})

	accepted, err := Signal(ctx, pool, id, "go_bw_gate", nil)
	if err != nil || !accepted {
		t.Fatalf("signal = (%v, %v)", accepted, err)
	}
	var out string
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out != "opened" {
		t.Fatalf("output = %q", out)
	}
}

func TestStepUndefinedPanics(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq6", QueueOpts{
		MaxAttempts: 1, Backoff: NoBackoff(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_undef", JobOpts{Queue: "go_pq6"}); err != nil {
		t.Fatal(err)
	}

	w := NewWorker(pool)
	w.Handle("go_undef", func(ctx context.Context, p *Plan, in struct{}) error {
		p.Step("go_undef_missing", nil)
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_undef", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = WaitForOutput(ctx, pool, id, nil, fastWait)
	if !errors.Is(err, ErrRunFailed) ||
		!strings.Contains(err.Error(), "go_undef_missing not defined") {
		t.Fatalf("wait: %v", err)
	}
}

func TestDuplicateSignalStepName(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq7", QueueOpts{
		MaxAttempts: 1, Backoff: NoBackoff(),
	}); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_dup", "go_dup_sig"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq7"}); err != nil {
			t.Fatal(err)
		}
	}

	w := NewWorker(pool)
	w.Handle("go_dup", func(ctx context.Context, p *Plan, in struct{}) error {
		// legal in the buffer, refused by the engine: two unresolved
		// signal-waiting steps with one name
		p.Step("go_dup_sig", nil, WaitsForSignal())
		p.Step("go_dup_sig", nil, WaitsForSignal())
		return nil
	})
	w.Handle("go_dup_sig", okHandler)
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_dup", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = WaitForOutput(ctx, pool, id, nil, fastWait)
	if !errors.Is(err, ErrRunFailed) ||
		!strings.Contains(err.Error(), "signal-waiting step") {
		t.Fatalf("wait: %v", err)
	}
	// the refused completion changed nothing: the run holds only its
	// first step, and the failure sits on that step's attempt row
	var steps int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_job_steps WHERE run_id = $1`, id).Scan(&steps); err != nil {
		t.Fatal(err)
	}
	if steps != 1 {
		t.Fatalf("steps = %d", steps)
	}
	var attemptErr string
	if err := pool.QueryRow(ctx,
		`SELECT error FROM cb_job_attempts WHERE run_id = $1 AND step_id = 1 AND attempt = 1`, id).
		Scan(&attemptErr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(attemptErr, "signal-waiting step") {
		t.Fatalf("attempt error = %q", attemptErr)
	}
}
