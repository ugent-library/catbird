package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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

// TestGiveUpCancelsWaits: a give-up cancels every waiting step — both
// waiting kinds — so a barrier never runs when the run it waited on
// failed (cleanup-despite-failure is on_fail's road).
func TestGiveUpCancelsWaits(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq8", QueueOpts{
		MaxAttempts: 1, Backoff: NoBackoff(),
	}); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_gcw_split", "go_gcw_poison", "go_gcw_sig", "go_gcw_bar"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq8"}); err != nil {
			t.Fatal(err)
		}
	}

	w := NewWorker(pool)
	w.Handle("go_gcw_split", func(ctx context.Context, p *Plan, in struct{}) error {
		p.Step("go_gcw_poison", nil)
		p.Step("go_gcw_sig", nil, WaitsForSignal())
		p.After().Step("go_gcw_bar", nil)
		return nil
	})
	w.Handle("go_gcw_poison", func(ctx context.Context, in struct{}) error {
		return errors.New("poison")
	})
	w.Handle("go_gcw_sig", okHandler)
	w.Handle("go_gcw_bar", okHandler)
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_gcw_split", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = WaitForOutput(ctx, pool, id, nil, fastWait)
	if !errors.Is(err, ErrRunFailed) || !strings.Contains(err.Error(), "poison") {
		t.Fatalf("wait: %v", err)
	}

	for name, want := range map[string]string{
		"go_gcw_split":  StatusCompleted,
		"go_gcw_poison": StatusFailed,
		"go_gcw_sig":    StatusCanceled,
		"go_gcw_bar":    StatusCanceled,
	} {
		if got := stepStatus(t, pool, id, name); got != want {
			t.Fatalf("step %s = %s, want %s", name, got, want)
		}
	}
}

// TestOnFailCleanupChain: the cleanup chain runs under 'failing' with the
// full surface — it adds steps, barriers and signal waits — and its end
// finalizes the run 'failed' with the original verdict, not the cleanup's
// story.
func TestOnFailCleanupChain(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq9", QueueOpts{
		MaxAttempts: 1, Backoff: NoBackoff(),
	}); err != nil {
		t.Fatal(err)
	}
	// on_fail must name a job defined earlier
	for _, job := range []string{"go_ofc_clean", "go_ofc_verify", "go_ofc_gate"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq9"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := Define(ctx, pool, "go_ofc_main", JobOpts{
		Queue: "go_pq9", OnFail: "go_ofc_clean",
	}); err != nil {
		t.Fatal(err)
	}

	w := NewWorker(pool)
	w.Handle("go_ofc_main", func(ctx context.Context, in struct{}) error {
		return errors.New("main exploded")
	})
	w.Handle("go_ofc_clean", func(ctx context.Context, p *Plan, in struct {
		Job   string `json:"job"`
		Error string `json:"error"`
	}) error {
		if in.Job != "go_ofc_main" || !strings.Contains(in.Error, "main exploded") {
			return fmt.Errorf("cleanup input = %+v", in)
		}
		p.Step("go_ofc_verify", nil)
		p.After().Step("go_ofc_gate", nil, WaitsForSignal())
		return nil
	})
	w.Handle("go_ofc_verify", okHandler)
	w.Handle("go_ofc_gate", okHandler)
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_ofc_main", nil)
	if err != nil {
		t.Fatal(err)
	}
	// the chain drains to its signal gate while the run is still 'failing'
	waitFor(t, 5*time.Second, "the cleanup gate to wait for its signal", func() bool {
		return stepStatus(t, pool, id, "go_ofc_gate") == StatusWaitingForSignal
	})
	info, err := GetRun(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusFailing {
		t.Fatalf("run status = %s mid-cleanup", info.Status)
	}

	// a 'failing' run accepts signals — its cleanup may wait for an operator
	accepted, err := Signal(ctx, pool, id, "go_ofc_gate", nil)
	if err != nil || !accepted {
		t.Fatalf("signal = (%v, %v)", accepted, err)
	}
	err = WaitForOutput(ctx, pool, id, nil, fastWait)
	if !errors.Is(err, ErrRunFailed) || !strings.Contains(err.Error(), "main exploded") {
		t.Fatalf("wait: %v", err)
	}
	for _, name := range []string{"go_ofc_clean", "go_ofc_verify", "go_ofc_gate"} {
		if got := stepStatus(t, pool, id, name); got != StatusCompleted {
			t.Fatalf("cleanup step %s = %s", name, got)
		}
	}
}

// TestSignalSlotOverwrite: a buffered signal nobody consumed yet is
// overwritten by a newer one — the slot holds the latest payload.
func TestSignalSlotOverwrite(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq10"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_slot_start", "go_slot_x"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq10"}); err != nil {
			t.Fatal(err)
		}
	}

	started := make(chan struct{}, 1)
	proceed := make(chan struct{})
	w := NewWorker(pool)
	w.Handle("go_slot_start", func(ctx context.Context, p *Plan, in struct{}) error {
		started <- struct{}{}
		select {
		case <-proceed:
		case <-ctx.Done():
			return ctx.Err()
		}
		p.Step("go_slot_x", nil, WaitsForSignal())
		return nil
	})
	w.Handle("go_slot_x", func(ctx context.Context, p *Plan, in struct{}) error {
		sig, err := SignalInput[string](p)
		if err != nil {
			return err
		}
		p.SetRunOutput(sig)
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_slot_start", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	for _, payload := range []string{"old", "new"} {
		accepted, err := Signal(ctx, pool, id, "go_slot_x", payload)
		if err != nil || !accepted {
			t.Fatalf("signal %q = (%v, %v)", payload, accepted, err)
		}
	}
	close(proceed)

	var out string
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out != "new" {
		t.Fatalf("output = %q", out)
	}
}

// TestSequentialSignalSteps: one unresolved signal-waiting step per name
// is the limit — a second step with the same name is legal once the first
// has resolved.
func TestSequentialSignalSteps(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq11"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_seq_start", "go_seq_x"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq11"}); err != nil {
			t.Fatal(err)
		}
	}

	w := NewWorker(pool)
	w.Handle("go_seq_start", func(ctx context.Context, p *Plan, in struct{}) error {
		p.Step("go_seq_x", map[string]bool{"first": true}, WaitsForSignal())
		return nil
	})
	w.Handle("go_seq_x", func(ctx context.Context, p *Plan, in struct {
		First bool `json:"first"`
	}) error {
		sig, err := SignalInput[string](p)
		if err != nil {
			return err
		}
		if in.First {
			p.Step("go_seq_x", map[string]bool{"first": false}, WaitsForSignal())
			return nil
		}
		p.SetRunOutput(sig)
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_seq_start", nil)
	if err != nil {
		t.Fatal(err)
	}
	stepStatusByID := func(stepID int64) string {
		var status string
		err := pool.QueryRow(context.Background(),
			`SELECT status FROM cb_job_steps WHERE run_id = $1 AND id = $2`,
			id, stepID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ""
		}
		if err != nil {
			t.Fatal(err)
		}
		return status
	}

	waitFor(t, 5*time.Second, "the first x step to wait", func() bool {
		return stepStatusByID(2) == StatusWaitingForSignal
	})
	if accepted, err := Signal(ctx, pool, id, "go_seq_x", "one"); err != nil || !accepted {
		t.Fatalf("first signal = (%v, %v)", accepted, err)
	}
	waitFor(t, 5*time.Second, "the second x step to wait", func() bool {
		return stepStatusByID(3) == StatusWaitingForSignal
	})
	if accepted, err := Signal(ctx, pool, id, "go_seq_x", "two"); err != nil || !accepted {
		t.Fatalf("second signal = (%v, %v)", accepted, err)
	}

	var out string
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out != "two" {
		t.Fatalf("output = %q", out)
	}
}

// TestBothWaitsEarlySignal: a signal that arrives while its step still
// waits for the run's other steps is buffered and consumed at the phase
// dispatch — the step goes straight to work, skipping the signal wait.
func TestBothWaitsEarlySignal(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq12"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_bwe_start", "go_bwe_work", "go_bwe_gate"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq12"}); err != nil {
			t.Fatal(err)
		}
	}

	workStarted := make(chan struct{}, 1)
	proceed := make(chan struct{})
	w := NewWorker(pool)
	w.Handle("go_bwe_start", func(ctx context.Context, p *Plan, in struct{}) error {
		p.Step("go_bwe_work", nil)
		p.After().Step("go_bwe_gate", nil, WaitsForSignal())
		return nil
	})
	w.Handle("go_bwe_work", func(ctx context.Context, in struct{}) error {
		workStarted <- struct{}{}
		select {
		case <-proceed:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	w.Handle("go_bwe_gate", func(ctx context.Context, p *Plan, in struct{}) error {
		sig, err := SignalInput[string](p)
		if err != nil {
			return err
		}
		p.SetRunOutput(sig)
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_bwe_start", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-workStarted
	if got := stepStatus(t, pool, id, "go_bwe_gate"); got != StatusWaitingForSteps {
		t.Fatalf("gate = %s before the drain", got)
	}
	accepted, err := Signal(ctx, pool, id, "go_bwe_gate", "early")
	if err != nil || !accepted {
		t.Fatalf("signal = (%v, %v)", accepted, err)
	}
	close(proceed)

	var out string
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out != "early" {
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

// TestBarrierAtDrain: the completion that drains the run to zero can
// itself add a barrier — that barrier is part of the phase the same call
// dispatches.
func TestBarrierAtDrain(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pq13"); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_bad_split", "go_bad_work", "go_bad_bar1", "go_bad_bar2"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_pq13"}); err != nil {
			t.Fatal(err)
		}
	}

	w := NewWorker(pool)
	w.Handle("go_bad_split", func(ctx context.Context, p *Plan, in struct{}) error {
		p.Step("go_bad_work", nil)
		p.Step("go_bad_work", nil)
		p.After().Step("go_bad_bar1", nil)
		return nil
	})
	w.Handle("go_bad_work", okHandler)
	w.Handle("go_bad_bar1", func(ctx context.Context, p *Plan, in struct{}) error {
		// bar1 is the only step the run owes: this completion drains the
		// run and dispatches the barrier it adds here in the same call
		p.After().Step("go_bad_bar2", nil)
		return nil
	})
	w.Handle("go_bad_bar2", func(ctx context.Context, p *Plan, in struct{}) error {
		p.SetRunOutput("done")
		return nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_bad_split", nil)
	if err != nil {
		t.Fatal(err)
	}
	var out string
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out != "done" {
		t.Fatalf("output = %q", out)
	}
	if got := stepStatus(t, pool, id, "go_bad_bar2"); got != StatusCompleted {
		t.Fatalf("bar2 = %s", got)
	}
}

// TestRacingClaims: clearing a lapsed started row is idempotent under
// racing claim calls — every lapsed step is handed out exactly once
// across concurrent claimers.
func TestRacingClaims(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	const runs = 20
	if err := DefineQueue(ctx, pool, "go_pq14", QueueOpts{
		MaxAttempts: 3, Backoff: NoBackoff(), ClaimTTL: 200 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_race", JobOpts{Queue: "go_pq14"}); err != nil {
		t.Fatal(err)
	}

	type pair struct{ run, step int64 }
	claim := func(worker string) ([]pair, error) {
		rows, err := pool.Query(ctx,
			`SELECT c.run_id, c.step_id FROM cb_job_claim($1, $2) c`,
			[]string{"go_pq14"}, worker)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var ps []pair
		for rows.Next() {
			var p pair
			if err := rows.Scan(&p.run, &p.step); err != nil {
				return nil, err
			}
			ps = append(ps, p)
		}
		return ps, rows.Err()
	}

	for range runs {
		if _, _, err := Run(ctx, pool, "go_race", nil); err != nil {
			t.Fatal(err)
		}
	}
	// a ghost worker starts every step, then goes silent past the lease
	for got := 0; got < runs; {
		ps, err := claim("ghost")
		if err != nil {
			t.Fatal(err)
		}
		if len(ps) == 0 {
			t.Fatal("ghost ran dry before starting every step")
		}
		for _, p := range ps {
			var name *string
			if err := pool.QueryRow(ctx,
				`SELECT s.name FROM cb_job_start($1, $2, $3) s`,
				p.run, p.step, "ghost").Scan(&name); err != nil {
				t.Fatal(err)
			}
			if name == nil {
				t.Fatal("ghost's start returned nothing")
			}
		}
		got += len(ps)
	}
	time.Sleep(300 * time.Millisecond) // every lease lapses

	// two claimers race the repair until every step is handed out
	var mu sync.Mutex
	seen := make(map[pair]int)
	var firstErr error
	deadline := time.Now().Add(10 * time.Second)
	var wg sync.WaitGroup
	for _, worker := range []string{"racer1", "racer2"} {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				ps, err := claim(worker)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				for _, p := range ps {
					seen[p]++
				}
				done := firstErr != nil || len(seen) >= runs
				mu.Unlock()
				if done {
					return
				}
				if len(ps) == 0 {
					time.Sleep(10 * time.Millisecond)
				}
			}
		})
	}
	wg.Wait()

	if firstErr != nil {
		t.Fatal(firstErr)
	}
	if len(seen) != runs {
		t.Fatalf("handed out %d steps, want %d", len(seen), runs)
	}
	for p, n := range seen {
		if n != 1 {
			t.Fatalf("step (%d, %d) handed out %d times", p.run, p.step, n)
		}
	}
	// only the ghost's starts were spent: repair cleared, never restarted
	var attempts int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_job_attempts a
		 JOIN cb_job_runs r ON r.id = a.run_id
		 WHERE r.job = 'go_race'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != runs {
		t.Fatalf("attempts = %d, want %d", attempts, runs)
	}
}
