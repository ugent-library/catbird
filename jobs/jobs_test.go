package jobs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/notify"
)

var (
	setupOnce sync.Once
	testPool  *pgxpool.Pool

	notifierOnce  sync.Once
	suiteNotifier *notify.Notifier
)

// testNotifier starts one notifier for the whole suite, the way a real
// process runs one for all its workers. It lives until the process ends.
func testNotifier(t testing.TB) *notify.Notifier {
	t.Helper()
	pool := setupTest(t)
	notifierOnce.Do(func() {
		suiteNotifier = notify.New(pool)
		go func() { _ = suiteNotifier.Start(context.Background()) }()
	})
	return suiteNotifier
}

// setupTest migrates once per process and wipes leftovers from earlier
// runs: rows persist in the shared tables, so every test uses go_-prefixed
// names and the wipe targets those.
func setupTest(t testing.TB) *pgxpool.Pool {
	t.Helper()
	setupOnce.Do(func() {
		db, err := sql.Open("pgx", testDSN)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		if err := MigrateUpTo(context.Background(), db, SchemaVersion); err != nil {
			panic(err)
		}
		for _, q := range []string{
			`DELETE FROM cb_job_signals s USING cb_job_runs r WHERE s.run_id = r.id AND r.job LIKE 'go_%'`,
			`DELETE FROM cb_job_attempts a USING cb_job_runs r WHERE a.run_id = r.id AND r.job LIKE 'go_%'`,
			`DELETE FROM cb_job_steps s USING cb_job_runs r WHERE s.run_id = r.id AND r.job LIKE 'go_%'`,
			`DELETE FROM cb_job_runs WHERE job LIKE 'go_%'`,
			`DELETE FROM cb_job_schedules WHERE name LIKE 'go_%'`,
			`DELETE FROM cb_jobs WHERE name LIKE 'go_%'`,
			`DELETE FROM cb_job_queues WHERE name LIKE 'go_%'`,
		} {
			if _, err := db.Exec(q); err != nil {
				panic(err)
			}
		}
		testPool, err = pgxpool.New(context.Background(), testDSN)
		if err != nil {
			panic(err)
		}
	})
	return testPool
}

// startTestWorker runs the worker until the test ends and fails the test
// on anything but a clean shutdown.
func startTestWorker(t testing.TB, w *Worker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("worker: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
	})
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func okHandler(ctx context.Context, in struct{}) (struct{}, error) {
	return struct{}{}, nil
}

var fastWait = WaitOpts{PollInterval: 20 * time.Millisecond}

func TestDefine(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_dq", QueueOpts{
		ClaimTTL:       10 * time.Second,
		ClaimBatchSize: 3,
		MaxAttempts:    5,
		Backoff:        FixedBackoff(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_dj_fail", JobOpts{Queue: "go_dq"}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_dj", JobOpts{
		Queue: "go_dq", OnFail: "go_dj_fail", Retention: Forever,
	}); err != nil {
		t.Fatal(err)
	}
	if err := DefineSchedule(ctx, pool, "go_dsched", "go_dj", time.Minute,
		ScheduleOpts{CatchUp: CatchUpAll}); err != nil {
		t.Fatal(err)
	}

	var claimTTL string
	var batch, maxAttempts int
	var kind, base string
	if err := pool.QueryRow(ctx,
		`SELECT claim_ttl::text, claim_batch_size, max_attempts, backoff_kind::text, backoff_base::text
		 FROM cb_job_queues WHERE name = 'go_dq'`).
		Scan(&claimTTL, &batch, &maxAttempts, &kind, &base); err != nil {
		t.Fatal(err)
	}
	if claimTTL != "00:00:10" || batch != 3 || maxAttempts != 5 || kind != "fixed" || base != "00:00:02" {
		t.Fatalf("queue terms = (%s, %d, %d, %s, %s)", claimTTL, batch, maxAttempts, kind, base)
	}

	var queue, onFail string
	var foreverRetention bool
	if err := pool.QueryRow(ctx,
		`SELECT queue, on_fail, retention = cb_forever() FROM cb_jobs WHERE name = 'go_dj'`).
		Scan(&queue, &onFail, &foreverRetention); err != nil {
		t.Fatal(err)
	}
	if queue != "go_dq" || onFail != "go_dj_fail" || !foreverRetention {
		t.Fatalf("job config = (%s, %s, forever=%v)", queue, onFail, foreverRetention)
	}

	var schedJob, every, catchUp string
	if err := pool.QueryRow(ctx,
		`SELECT job, every::text, catch_up::text FROM cb_job_schedules WHERE name = 'go_dsched'`).
		Scan(&schedJob, &every, &catchUp); err != nil {
		t.Fatal(err)
	}
	if schedJob != "go_dj" || every != "00:01:00" || catchUp != "all" {
		t.Fatalf("schedule = (%s, %s, %s)", schedJob, every, catchUp)
	}

	// an identical declaration writes nothing
	xmins := func() (out [3]string) {
		for i, q := range []string{
			`SELECT xmin::text FROM cb_job_queues WHERE name = 'go_dq'`,
			`SELECT xmin::text FROM cb_jobs WHERE name = 'go_dj'`,
			`SELECT xmin::text FROM cb_job_schedules WHERE name = 'go_dsched'`,
		} {
			if err := pool.QueryRow(ctx, q).Scan(&out[i]); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	before := xmins()
	if err := DefineQueue(ctx, pool, "go_dq", QueueOpts{
		ClaimTTL:       10 * time.Second,
		ClaimBatchSize: 3,
		MaxAttempts:    5,
		Backoff:        FixedBackoff(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_dj", JobOpts{
		Queue: "go_dq", OnFail: "go_dj_fail", Retention: Forever,
	}); err != nil {
		t.Fatal(err)
	}
	if err := DefineSchedule(ctx, pool, "go_dsched", "go_dj", time.Minute,
		ScheduleOpts{CatchUp: CatchUpAll}); err != nil {
		t.Fatal(err)
	}
	if after := xmins(); after != before {
		t.Fatalf("identical define wrote rows: %v -> %v", before, after)
	}

	// a changed declaration applies
	if err := DefineQueue(ctx, pool, "go_dq", QueueOpts{
		ClaimTTL:       10 * time.Second,
		ClaimBatchSize: 3,
		MaxAttempts:    4,
		Backoff:        FixedBackoff(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT max_attempts FROM cb_job_queues WHERE name = 'go_dq'`).Scan(&maxAttempts); err != nil {
		t.Fatal(err)
	}
	if maxAttempts != 4 {
		t.Fatalf("max_attempts = %d after redeclare", maxAttempts)
	}

	// a typo is a deploy error, and the failed call leaves nothing behind
	if err := Define(ctx, pool, "go_dx", JobOpts{Queue: "go_nope"}); !errors.Is(err, ErrNotDefined) {
		t.Fatalf("undeclared queue: %v", err)
	}
	if err := Define(ctx, pool, "go_dx", JobOpts{OnFail: "go_nope"}); !errors.Is(err, ErrNotDefined) {
		t.Fatalf("undeclared on_fail: %v", err)
	}
	if err := DefineSchedule(ctx, pool, "go_dsched2", "go_nope", time.Minute); !errors.Is(err, ErrNotDefined) {
		t.Fatalf("undeclared schedule job: %v", err)
	}
	var leftover int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_jobs WHERE name = 'go_dx'`).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("failed define left %d jobs behind", leftover)
	}

	// a job may name itself as its own on_fail
	if err := Define(ctx, pool, "go_dself", JobOpts{OnFail: "go_dself"}); err != nil {
		t.Fatal(err)
	}
}

func TestRunDedupAndLookup(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_ddq"); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_dedup", JobOpts{Queue: "go_ddq"}); err != nil {
		t.Fatal(err)
	}

	id1, existing, err := Run(ctx, pool, "go_dedup", map[string]int{"n": 1}, RunOpts{Key: "k1"})
	if err != nil || existing {
		t.Fatalf("first run: id=%d existing=%v err=%v", id1, existing, err)
	}
	id2, existing, err := Run(ctx, pool, "go_dedup", map[string]int{"n": 2}, RunOpts{Key: "k1"})
	if err != nil || !existing || id2 != id1 {
		t.Fatalf("dedup: id=%d existing=%v err=%v", id2, existing, err)
	}

	info, err := GetRunByKey(ctx, pool, "go_dedup", "k1")
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != id1 || info.Status != StatusRunning || string(info.Input) != `{"n": 1}` {
		t.Fatalf("by key: %+v", info)
	}
	info, err = GetRun(ctx, pool, id1)
	if err != nil || info.Job != "go_dedup" || info.Key != "k1" {
		t.Fatalf("by id: %+v err=%v", info, err)
	}

	if _, err := GetRun(ctx, pool, -1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing run: %v", err)
	}
	if _, _, err := Run(ctx, pool, "go_undefined", nil); !errors.Is(err, ErrNotDefined) {
		t.Fatalf("undefined job: %v", err)
	}

	// a run enqueued in a rolled-back transaction never happened
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Run(ctx, tx, "go_dedup", nil, RunOpts{Key: "k2"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := GetRunByKey(ctx, pool, "go_dedup", "k2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back run: %v", err)
	}
}

// A worker without a notifier wakes by poll alone — the configuration
// for transaction-pooled connections, where LISTEN cannot work.
func TestWorkerPollOnly(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_pollq"); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_poll_echo", JobOpts{Queue: "go_pollq"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(pool, WorkerOpts{PollInterval: 50 * time.Millisecond})
	w.Handle("go_poll_echo", func(ctx context.Context, p *Plan, in int) (int, error) {
		p.SetRunOutput(in)
		return in, nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_poll_echo", 7)
	if err != nil {
		t.Fatal(err)
	}
	var out int
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out != 7 {
		t.Fatalf("output = %d", out)
	}
}

func TestWorker(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq1"); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_double", JobOpts{Queue: "go_wq1"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_double", func(ctx context.Context, p *Plan, in struct {
		N int `json:"n"`
	}) (map[string]int, error) {
		out := map[string]int{"doubled": in.N * 2}
		p.SetRunOutput(out)
		return out, nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_double", map[string]int{"n": 21})
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Doubled int `json:"doubled"`
	}
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out.Doubled != 42 {
		t.Fatalf("output = %+v", out)
	}

	info, err := GetRun(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusCompleted || info.FinishedAt.IsZero() {
		t.Fatalf("run = %+v", info)
	}

	var stepStatus string
	var attempt int
	var worker *string
	if err := pool.QueryRow(ctx,
		`SELECT status, attempt, worker FROM cb_job_steps WHERE run_id = $1 AND id = 1`, id).
		Scan(&stepStatus, &attempt, &worker); err != nil {
		t.Fatal(err)
	}
	if stepStatus != StatusCompleted || attempt != 1 || worker != nil {
		t.Fatalf("step = (%s, %d, %v)", stepStatus, attempt, worker)
	}
	var attemptStatus string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM cb_job_attempts WHERE run_id = $1 AND step_id = 1 AND attempt = 1`, id).
		Scan(&attemptStatus); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != StatusCompleted {
		t.Fatalf("attempt status = %s", attemptStatus)
	}
}

func TestWorkerRetry(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq2", QueueOpts{
		MaxAttempts: 3, Backoff: NoBackoff(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_flaky", JobOpts{Queue: "go_wq2"}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	w := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_flaky", func(ctx context.Context, p *Plan, in struct{}) (string, error) {
		if calls.Add(1) < 3 {
			return "", errors.New("not yet")
		}
		p.SetRunOutput("done")
		return "done", nil
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_flaky", nil)
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

	var failed, completed int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status = 'failed'),
		       count(*) FILTER (WHERE status = 'completed')
		FROM cb_job_attempts WHERE run_id = $1`, id).Scan(&failed, &completed); err != nil {
		t.Fatal(err)
	}
	if failed != 2 || completed != 1 {
		t.Fatalf("attempts = %d failed, %d completed", failed, completed)
	}
}

func TestWorkerGiveUp(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq3", QueueOpts{
		MaxAttempts: 2, Backoff: NoBackoff(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_poison", JobOpts{Queue: "go_wq3"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_poison", func(ctx context.Context, in struct{}) (struct{}, error) {
		panic("kaboom")
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_poison", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = WaitForOutput(ctx, pool, id, nil, fastWait)
	if !errors.Is(err, ErrRunFailed) || !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("wait: %v", err)
	}

	info, err := GetRun(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusFailed || !strings.Contains(info.Error, "handler panic") {
		t.Fatalf("run = %+v", info)
	}
	var attempts int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_job_attempts WHERE run_id = $1 AND status = 'failed'`, id).
		Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d", attempts)
	}
}

func TestOnFailChain(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	type failure struct {
		Job   string `json:"job"`
		Error string `json:"error"`
		Input struct {
			N int `json:"n"`
		} `json:"input"`
	}
	got := make(chan failure, 1)

	if err := DefineQueue(ctx, pool, "go_wq4", QueueOpts{
		MaxAttempts: 1, Backoff: NoBackoff(),
	}); err != nil {
		t.Fatal(err)
	}
	// on_fail must name a job defined earlier: go_cleanup comes first
	if err := Define(ctx, pool, "go_cleanup", JobOpts{Queue: "go_wq4"}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_boom", JobOpts{Queue: "go_wq4", OnFail: "go_cleanup"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_cleanup", func(ctx context.Context, in failure) (struct{}, error) {
		got <- in
		return struct{}{}, nil
	})
	w.Handle("go_boom", func(ctx context.Context, in struct{}) (struct{}, error) {
		return struct{}{}, errors.New("boom")
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_boom", map[string]int{"n": 7})
	if err != nil {
		t.Fatal(err)
	}
	err = WaitForOutput(ctx, pool, id, nil, fastWait)
	if !errors.Is(err, ErrRunFailed) || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("wait: %v", err)
	}

	select {
	case f := <-got:
		if f.Job != "go_boom" || f.Error != "boom" || f.Input.N != 7 {
			t.Fatalf("on_fail input = %+v", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("on_fail handler never ran")
	}

	// the failed step and the completed cleanup step, run ended 'failed'
	var cleanupStatus, cleanupName string
	var parent int64
	if err := pool.QueryRow(ctx,
		`SELECT name, status, parent_step_id FROM cb_job_steps WHERE run_id = $1 AND id = 2`, id).
		Scan(&cleanupName, &cleanupStatus, &parent); err != nil {
		t.Fatal(err)
	}
	if cleanupName != "go_cleanup" || cleanupStatus != StatusCompleted || parent != 1 {
		t.Fatalf("cleanup step = (%s, %s, parent %d)", cleanupName, cleanupStatus, parent)
	}
}

func TestCancelMidHandler(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq5", QueueOpts{
		ClaimTTL: 300 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_block", JobOpts{Queue: "go_wq5"}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	handlerDone := make(chan error, 1)
	w := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_block", func(ctx context.Context, in struct{}) (struct{}, error) {
		started <- struct{}{}
		<-ctx.Done()
		handlerDone <- ctx.Err()
		return struct{}{}, ctx.Err()
	})
	startTestWorker(t, w)

	id, _, err := Run(ctx, pool, "go_block", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	if applied, err := Cancel(ctx, pool, id, "operator says no"); err != nil || !applied {
		t.Fatalf("cancel: applied=%v err=%v", applied, err)
	}

	// the worker notices on the extend cadence and cancels the handler
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was never canceled")
	}

	err = WaitForOutput(ctx, pool, id, nil, fastWait)
	if !errors.Is(err, ErrRunCanceled) || !strings.Contains(err.Error(), "operator says no") {
		t.Fatalf("wait: %v", err)
	}

	// nobody reported the canceled attempt: its row records no verdict
	var attemptStatus *string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM cb_job_attempts WHERE run_id = $1 AND step_id = 1 AND attempt = 1`, id).
		Scan(&attemptStatus); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != nil {
		t.Fatalf("attempt status = %v", *attemptStatus)
	}
}

func TestWorkerShutdown(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq6", QueueOpts{
		MaxAttempts: 3, Backoff: NoBackoff(), ClaimTTL: 5 * time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_slow", JobOpts{Queue: "go_wq6"}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	handler := func(ctx context.Context, p *Plan, in struct{}) (string, error) {
		if calls.Add(1) == 1 {
			started <- struct{}{}
			<-ctx.Done()
			return "", ctx.Err()
		}
		p.SetRunOutput("second time lucky")
		return "second time lucky", nil
	}

	w1 := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w1.Handle("go_slow", handler)
	w1ctx, w1cancel := context.WithCancel(context.Background())
	w1done := make(chan error, 1)
	go func() { w1done <- w1.Start(w1ctx) }()

	id, _, err := Run(ctx, pool, "go_slow", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	// graceful shutdown mid-handler: the spent start is reported as a verdict
	w1cancel()
	if err := <-w1done; !errors.Is(err, context.Canceled) {
		t.Fatalf("worker 1: %v", err)
	}

	var attemptStatus, attemptError string
	if err := pool.QueryRow(ctx,
		`SELECT status, error FROM cb_job_attempts WHERE run_id = $1 AND step_id = 1 AND attempt = 1`, id).
		Scan(&attemptStatus, &attemptError); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != StatusFailed || attemptError != "catbird: worker shutdown" {
		t.Fatalf("attempt 1 = (%s, %s)", attemptStatus, attemptError)
	}
	var stepStatus string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM cb_job_steps WHERE run_id = $1 AND id = 1`, id).Scan(&stepStatus); err != nil {
		t.Fatal(err)
	}
	if stepStatus != StatusQueued {
		t.Fatalf("step status = %s after shutdown", stepStatus)
	}

	// redelivery: a fresh worker finishes the run
	w2 := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w2.Handle("go_slow", handler)
	startTestWorker(t, w2)
	var out string
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	if out != "second time lucky" {
		t.Fatalf("output = %q", out)
	}
}

// TestSilenceRoad walks the crash road by hand: a worker that started a
// step and never reported. The lapsed lease is repaired by claim, and
// start — not claim — does the give-up.
func TestSilenceRoad(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq7", QueueOpts{
		MaxAttempts: 1, Backoff: NoBackoff(), ClaimTTL: 200 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_silent", JobOpts{Queue: "go_wq7"}); err != nil {
		t.Fatal(err)
	}

	claim := func(worker string) int {
		rows, err := pool.Query(ctx,
			`SELECT c.run_id FROM cb_job_claim($1, $2) c`, []string{"go_wq7"}, worker)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		return n
	}
	start := func(runID int64, worker string) *string {
		var name *string
		if err := pool.QueryRow(ctx,
			`SELECT s.name FROM cb_job_start($1, 1, $2) s`, runID, worker).Scan(&name); err != nil {
			t.Fatal(err)
		}
		return name
	}

	id, _, err := Run(ctx, pool, "go_silent", nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := claim("ghost"); n != 1 {
		t.Fatalf("ghost claimed %d steps", n)
	}
	if name := start(id, "ghost"); name == nil {
		t.Fatal("ghost's start returned nothing")
	}
	time.Sleep(300 * time.Millisecond) // the lease lapses in silence

	// the repair pass clears the crashed row but hands nothing out
	if n := claim("w2"); n != 0 {
		t.Fatalf("repair pass handed out %d steps", n)
	}
	// at the attempt limit the repaired row is due at once, not backoff-paced
	if n := claim("w2"); n != 1 {
		t.Fatalf("second claim handed out %d steps", n)
	}
	// starting would exceed the budget: give-up instead
	if name := start(id, "w2"); name != nil {
		t.Fatalf("start after exhaustion returned %q", *name)
	}

	info, err := GetRun(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != StatusFailed ||
		info.Error != "attempts exhausted; last attempt ended in silence" {
		t.Fatalf("run = %+v", info)
	}
	// one attempt row, no verdict: recorded silence
	var attempts, verdicts int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(status) FROM cb_job_attempts WHERE run_id = $1`, id).
		Scan(&attempts, &verdicts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || verdicts != 0 {
		t.Fatalf("attempts = %d, verdicts = %d", attempts, verdicts)
	}

	// the justice half: a leased but never started step lapses uncharged
	id2, _, err := Run(ctx, pool, "go_silent", nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := claim("ghost2"); n != 1 {
		t.Fatalf("ghost2 claimed %d steps", n)
	}
	time.Sleep(300 * time.Millisecond)
	if n := claim("w3"); n != 1 {
		t.Fatalf("lapsed unstarted step not re-handed: %d", n)
	}
	var attempt int
	if err := pool.QueryRow(ctx,
		`SELECT attempt FROM cb_job_steps WHERE run_id = $1 AND id = 1`, id2).Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	if attempt != 0 {
		t.Fatalf("unstarted lapse spent %d attempts", attempt)
	}
}

func TestScheduledRuns(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq8"); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_tick", JobOpts{Queue: "go_wq8"}); err != nil {
		t.Fatal(err)
	}
	if err := DefineSchedule(ctx, pool, "go_sched_t", "go_tick", 200*time.Millisecond,
		ScheduleOpts{
			Input:   map[string]string{"from": "schedule"},
			StartAt: time.Now(),
		}); err != nil {
		t.Fatal(err)
	}

	tctx, tcancel := context.WithCancel(context.Background())
	tdone := make(chan error, 1)
	go func() {
		tdone <- StartTicker(tctx, pool, TickerOpts{ScheduleInterval: 50 * time.Millisecond})
	}()
	defer func() {
		tcancel()
		<-tdone
	}()

	var runID int64
	waitFor(t, 5*time.Second, "a scheduled run", func() bool {
		err := pool.QueryRow(ctx,
			`SELECT id FROM cb_job_runs WHERE job = 'go_tick' LIMIT 1`).Scan(&runID)
		return err == nil
	})
	info, err := GetRun(ctx, pool, runID)
	if err != nil {
		t.Fatal(err)
	}
	if string(info.Input) != `{"from": "schedule"}` {
		t.Fatalf("scheduled input = %s", info.Input)
	}

	// the fire re-armed the schedule
	var rearmed bool
	if err := pool.QueryRow(ctx,
		`SELECT next_at > now() - interval '200 milliseconds'
		 FROM cb_job_schedules WHERE name = 'go_sched_t'`).Scan(&rearmed); err != nil {
		t.Fatal(err)
	}
	if !rearmed {
		t.Fatal("schedule was not re-armed")
	}

	if deleted, err := DeleteSchedule(ctx, pool, "go_sched_t"); err != nil || !deleted {
		t.Fatalf("delete: %v %v", deleted, err)
	}
	if deleted, err := DeleteSchedule(ctx, pool, "go_sched_t"); err != nil || deleted {
		t.Fatalf("second delete: %v %v", deleted, err)
	}
}

// TestWorkerStartChecks: a worker refuses to start when its handlers and
// cb_jobs disagree — in either direction — or a handler has the wrong
// shape.
func TestWorkerStartChecks(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq9"); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_cov_a", JobOpts{Queue: "go_wq9"}); err != nil {
		t.Fatal(err)
	}
	// handled by another fleet, but routed to the same queue
	if err := Define(ctx, pool, "go_cov_ext", JobOpts{Queue: "go_wq9"}); err != nil {
		t.Fatal(err)
	}

	// a claim is indiscriminate within its pool: partial coverage refuses
	w := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_cov_a", okHandler)
	if err := w.Start(ctx); err == nil || !strings.Contains(err.Error(), "go_cov_ext") {
		t.Fatalf("partial coverage: %v", err)
	}

	// a handler for a job nobody defined is a deploy mistake
	w = NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_cov_a", okHandler)
	w.Handle("go_cov_ext", okHandler)
	w.Handle("go_cov_typo", okHandler)
	if err := w.Start(ctx); err == nil || !strings.Contains(err.Error(), "go_cov_typo") {
		t.Fatalf("undefined job: %v", err)
	}

	// a wrong handler shape surfaces at Start
	w = NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_cov_a", func() {})
	if err := w.Start(ctx); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("wrong shape: %v", err)
	}
}

// TestWorkerReleasesUnknownStep: a job defined after the worker started (a
// rolling deploy) is handed back with a pause, uncharged.
func TestWorkerReleasesUnknownStep(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := DefineQueue(ctx, pool, "go_wq10"); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_rd_a", JobOpts{Queue: "go_wq10"}); err != nil {
		t.Fatal(err)
	}
	w := NewWorker(pool, WorkerOpts{Notifier: testNotifier(t)})
	w.Handle("go_rd_a", okHandler)
	startTestWorker(t, w)

	// prove the worker is past its startup checks before the new job
	// arrives, or it would refuse to start instead of releasing
	id0, _, err := Run(ctx, pool, "go_rd_a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := WaitForOutput(ctx, pool, id0, nil, fastWait); err != nil {
		t.Fatal(err)
	}

	if err := Define(ctx, pool, "go_rd_b", JobOpts{Queue: "go_wq10"}); err != nil {
		t.Fatal(err)
	}
	id, _, err := Run(ctx, pool, "go_rd_b", nil)
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, "the step to be released with a pause", func() bool {
		var attempt int
		var paused bool
		if err := pool.QueryRow(ctx, `
			SELECT attempt, worker IS NULL AND claimable_at > now() + interval '2 seconds'
			FROM cb_job_steps WHERE run_id = $1 AND id = 1`, id).Scan(&attempt, &paused); err != nil {
			return false
		}
		if attempt != 0 {
			t.Fatalf("release spent %d attempts", attempt)
		}
		return paused
	})
}
