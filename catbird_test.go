package catbird_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
)

const testDSN = "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable"

// setupTestDB connects and recreates the schema from the migration file.
func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	b, err := os.ReadFile("migrations/00001_lite.sql")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	up := strings.Split(string(b), "-- +goose down")[0]

	_, err = pool.Exec(ctx, `
		DROP TABLE IF EXISTS cb_outputs, cb_signals, cb_claims, cb_cursors, cb_messages CASCADE;
		DROP SEQUENCE IF EXISTS cb_position_seq;
	`)
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err = pool.Exec(ctx, up); err != nil {
		t.Fatalf("schema: %v", err)
	}
	return pool
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func TestTortureThroughput(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numProducers = 10
	const msgsPerProducer = 1000
	const totalMsgs = numProducers * msgsPerProducer

	start := time.Now()
	var prodWg sync.WaitGroup
	for i := 0; i < numProducers; i++ {
		prodWg.Add(1)
		go func(prodID int) {
			defer prodWg.Done()
			for j := 0; j < msgsPerProducer; j++ {
				_, err := catbird.Enqueue(ctx, pool, "torture.task", "torture_queue", map[string]int{"prod": prodID, "task": j}, catbird.EnqueueOptions{})
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(i)
	}
	prodWg.Wait()
	writeDur := time.Since(start)
	t.Logf("wrote %d messages in %v (%.0f/s)", totalMsgs, writeDur, float64(totalMsgs)/writeDur.Seconds())

	var processed int32
	handler := func(ctx context.Context, job *catbird.Job) error {
		// The transactional path: the handler's own writes and the completion
		// of the job in one commit.
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}
		if err := catbird.Complete(ctx, tx, job); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		atomic.AddInt32(&processed, 1)
		return nil
	}

	workStart := time.Now()
	var workerWg sync.WaitGroup
	rt := catbird.New(pool, catbird.Options{})
	for i := 0; i < 5; i++ {
		catbird.NewWorker(rt, "torture_queue", handler, catbird.WorkerOptions{})
	}
	workerWg.Add(1)
	go func() {
		defer workerWg.Done()
		rt.Start(ctx)
	}()

	for atomic.LoadInt32(&processed) < totalMsgs {
		if ctx.Err() != nil {
			t.Fatalf("timed out: processed %d of %d", atomic.LoadInt32(&processed), totalMsgs)
		}
		time.Sleep(50 * time.Millisecond)
	}
	workDur := time.Since(workStart)
	t.Logf("processed %d messages in %v (%.0f/s)", totalMsgs, workDur, float64(totalMsgs)/workDur.Seconds())

	// The counter moves inside the handler, before the commit; give the last commits a moment.
	time.Sleep(time.Second)
	cancel()
	workerWg.Wait()

	if n := count(t, pool, "SELECT count(*) FROM cb_claims"); n != 0 {
		t.Errorf("expected 0 claims left, got %d", n)
	}
}

// One long job does not hold up the jobs beside it: the worker keeps BatchSize
// jobs running and claims a new one whenever a slot frees.
func TestLongJobDoesNotHoldUpTheQueue(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The long job is enqueued first, so the worker claims it first.
	release := make(chan struct{})
	if _, err := catbird.Enqueue(ctx, pool, "long", "mixed", nil, catbird.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	const short = 20
	batch := make([]catbird.BatchMessage, short)
	for i := range batch {
		batch[i] = catbird.BatchMessage{Topic: "short", Payload: i}
	}
	if _, err := catbird.EnqueueBatch(ctx, pool, "mixed", batch, catbird.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	done := make(chan string, short+1)
	rt := catbird.New(pool, catbird.Options{})
	rt.Worker("mixed", func(ctx context.Context, m *catbird.Job) error {
		if m.Topic == "long" {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		done <- m.Topic
		return nil
	}, catbird.WorkerOptions{BatchSize: 4, PollInterval: 100 * time.Millisecond})

	workers, stop := context.WithCancel(ctx)
	defer stop()
	go rt.Start(workers)

	// Every short job finishes while the long one is still running.
	for range short {
		select {
		case topic := <-done:
			if topic != "short" {
				t.Fatalf("finished %q before the long job was released", topic)
			}
		case <-ctx.Done():
			t.Fatalf("the short jobs waited for the long job: %d of %d done",
				len(done), short)
		}
	}
	// A job reports itself inside its handler, before its transaction commits,
	// so wait for the claims to go: only the long job's may remain.
	for count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'mixed'") > 1 {
		if ctx.Err() != nil {
			t.Fatal("short jobs finished but their claims stayed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(release)
	select {
	case topic := <-done:
		if topic != "long" {
			t.Fatalf("finished %q, want the long job", topic)
		}
	case <-ctx.Done():
		t.Fatal("the long job did not finish after release")
	}
}

func TestExactlyOnceDedup(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	opts := catbird.EnqueueOptions{DedupKey: "deterministic-hash-12345"}

	id, err := catbird.Enqueue(ctx, pool, "task", "dedup_queue", nil, opts)
	if err != nil || id == 0 {
		t.Fatalf("first enqueue: id=%d err=%v", id, err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := catbird.Enqueue(ctx, pool, "task", "dedup_queue", nil, opts)
			if err != nil {
				t.Errorf("duplicate enqueue: %v", err)
			}
			if id != 0 {
				t.Errorf("duplicate enqueue returned id %d, want 0", id)
			}
		}()
	}
	wg.Wait()

	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'dedup_queue'"); n != 1 {
		t.Fatalf("expected 1 claim, got %d", n)
	}
}

func TestDependenciesAndSignals(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	// Two parent steps and one external signal.
	childID, err := catbird.Enqueue(ctx, pool, "join_task", "dag_queue", nil, catbird.EnqueueOptions{Dependencies: 3})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE status = 0 AND dependencies = 0"); n != 0 {
		t.Fatalf("expected 0 ready claims, got %d", n)
	}

	if err := catbird.ResolveDependency(ctx, pool, childID); err != nil {
		t.Fatalf("resolve 1: %v", err)
	}
	if err := catbird.ResolveDependency(ctx, pool, childID); err != nil {
		t.Fatalf("resolve 2: %v", err)
	}
	if err := catbird.DeliverSignal(ctx, pool, childID, "human_approval", map[string]bool{"ok": true}); err != nil {
		t.Fatalf("signal: %v", err)
	}
	// The same signal again is a no-op, not an error.
	if err := catbird.DeliverSignal(ctx, pool, childID, "human_approval", map[string]bool{"ok": false}); err != nil {
		t.Fatalf("duplicate signal: %v", err)
	}
	// The job no longer waits: another signal or resolution is refused.
	if err := catbird.DeliverSignal(ctx, pool, childID, "other", nil); !errors.Is(err, catbird.ErrNotFound) {
		t.Fatalf("signal to non-waiting job: got %v, want ErrNotFound", err)
	}
	if err := catbird.ResolveDependency(ctx, pool, childID); !errors.Is(err, catbird.ErrNotFound) {
		t.Fatalf("resolve on non-waiting job: got %v, want ErrNotFound", err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_signals"); n != 1 {
		t.Fatalf("expected 1 signal row, got %d", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE status = 0 AND dependencies = 0"); n != 1 {
		t.Fatalf("expected 1 ready claim, got %d", n)
	}

	ran := make(chan catbird.Job, 1)
	rt := catbird.New(pool, catbird.Options{})
	catbird.NewWorker(rt, "dag_queue", func(ctx context.Context, m *catbird.Job) error {
		ran <- *m
		return nil
	}, catbird.WorkerOptions{PollInterval: 50 * time.Millisecond})
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go rt.Start(runCtx)

	select {
	case m := <-ran:
		var ok struct {
			OK bool `json:"ok"`
		}
		if err := json.Unmarshal(m.Signals["human_approval"], &ok); err != nil || !ok.OK {
			t.Fatalf("signal payload: %s (%v)", m.Signals["human_approval"], err)
		}
	case <-runCtx.Done():
		t.Fatal("job did not run")
	}
	time.Sleep(200 * time.Millisecond)
	if n := count(t, pool, "SELECT count(*) FROM cb_signals"); n != 0 {
		t.Fatalf("expected signals deleted on completion, got %d", n)
	}
}

// A handler that outlives its lease loses its work; the attempt that holds the
// lease commits.
func TestLeaseExpiryFence(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS lease_test (attempt INT)"); err != nil {
		t.Fatal(err)
	}
	pool.Exec(ctx, "TRUNCATE lease_test")

	if _, err := catbird.Enqueue(ctx, pool, "slow", "lease_queue", nil, catbird.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	var calls int32
	handler := func(ctx context.Context, job *catbird.Job) error {
		atomic.AddInt32(&calls, 1)
		if job.Attempts == 1 {
			time.Sleep(800 * time.Millisecond) // past the lease
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "INSERT INTO lease_test (attempt) VALUES ($1)", job.Attempts); err != nil {
			return err
		}
		if err := catbird.Complete(ctx, tx, job); err != nil {
			return err // the late attempt gets ErrLeaseExpired and its insert is rolled back
		}
		return tx.Commit(ctx)
	}
	opts := catbird.WorkerOptions{Lease: 200 * time.Millisecond, PollInterval: 50 * time.Millisecond}
	rt := catbird.New(pool, catbird.Options{})
	catbird.NewWorker(rt, "lease_queue", handler, opts)
	catbird.NewWorker(rt, "lease_queue", handler, opts)
	go rt.Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for count(t, pool, "SELECT count(*) FROM cb_claims") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("claim was not completed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(time.Second) // let the late first attempt finish and be discarded

	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("expected 2 handler calls, got %d", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM lease_test"); n != 1 {
		t.Errorf("expected exactly one committed attempt, got %d", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM lease_test WHERE attempt = 2"); n != 1 {
		t.Errorf("expected attempt 2 to be the one committed")
	}
}

func TestTriggerBridgesPayloadUnchanged(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := catbird.Publish(ctx, pool, "image.uploaded", map[string]string{"url": "https://example.com/a.png"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := catbird.Publish(ctx, pool, "image_x", nil, ""); err != nil {
		t.Fatal(err)
	}

	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	catbird.NewTrigger(rt, "img", []string{"image.#"}, "image_processing", catbird.TriggerOptions{PollInterval: 50 * time.Millisecond})

	got := make(chan catbird.Job, 16)
	catbird.NewWorker(rt, "image_processing", func(ctx context.Context, m *catbird.Job) error {
		got <- *m
		return nil
	}, catbird.WorkerOptions{PollInterval: 50 * time.Millisecond})
	go rt.Start(ctx)

	select {
	case m := <-got:
		var p struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(m.Payload, &p); err != nil || p.URL != "https://example.com/a.png" {
			t.Fatalf("payload arrived as %s (%v)", m.Payload, err)
		}
		if m.Topic != "image.uploaded" {
			t.Fatalf("topic %q", m.Topic)
		}
	case <-ctx.Done():
		t.Fatal("trigger did not bridge the message")
	}
	time.Sleep(300 * time.Millisecond)
	if n := count(t, pool, "SELECT count(*) FROM cb_claims"); n != 0 {
		t.Errorf("expected only the matching message bridged and completed, %d claims left", n)
	}
	if n := count(t, pool, "SELECT last_position FROM cb_cursors WHERE name = 'trigger:img'"); n != 1 {
		t.Errorf("cursor at %d, want 1", n)
	}
}

func TestGCKeepsLiveClaims(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	if _, err := catbird.Enqueue(ctx, pool, "later", "gc_queue", nil, catbird.EnqueueOptions{Delay: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := catbird.Enqueue(ctx, pool, "doomed", "gc_queue", nil, catbird.EnqueueOptions{CorrelationID: "wf1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catbird.Publish(ctx, pool, "event", nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := catbird.Cancel(ctx, pool, "wf1"); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE status = 0 AND dependencies = 0 AND visible_at <= now()"); n != 0 {
		t.Fatalf("canceled job still claimable")
	}

	time.Sleep(20 * time.Millisecond)
	if err := catbird.GC(ctx, pool, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims"); n != 1 {
		t.Errorf("expected the delayed claim to survive GC, got %d claims", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_messages"); n != 1 {
		t.Errorf("expected only the delayed job's message to survive, got %d", n)
	}
}

// A message published in a transaction that commits late is read after the
// messages that committed before it — never skipped.
func TestStreamLateCommitIsNotSkipped(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	go rt.Start(ctx)
	cursor := catbird.Cursor{Name: "late", Patterns: []string{"ev"}}

	// Message 1 is inserted first but its transaction stays open.
	slow, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catbird.Publish(ctx, slow, "ev", "first inserted, last committed", ""); err != nil {
		t.Fatal(err)
	}
	// Messages 2 and 3 commit right away.
	for _, p := range []string{"second", "third"} {
		if _, err := catbird.Publish(ctx, pool, "ev", p, ""); err != nil {
			t.Fatal(err)
		}
	}

	read := func(want []string) {
		t.Helper()
		var got []string
		deadline := time.Now().Add(2 * time.Second)
		for len(got) < len(want) && time.Now().Before(deadline) {
			msgs, err := cursor.Read(ctx, pool, 50)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range msgs {
				var s string
				json.Unmarshal(m.Payload, &s)
				got = append(got, s)
				if err := cursor.Ack(ctx, pool, m.Position); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("read %v, want %v", got, want)
		}
	}

	read([]string{"second", "third"}) // the open transaction's message is not there yet
	if err := slow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	read([]string{"first inserted, last committed"}) // and it shows up once it commits
}

func TestOutputAndStreamNotify(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id, err := catbird.Enqueue(ctx, pool, "sum", "out_queue", []int{1, 2, 3}, catbird.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var result int
	if err := catbird.Output(ctx, pool, id, &result); !errors.Is(err, catbird.ErrNotFound) {
		t.Fatalf("output before completion: %v", err)
	}
	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	catbird.NewWorker(rt, "out_queue", func(ctx context.Context, job *catbird.Job) error {
		var in []int
		json.Unmarshal(job.Payload, &in)
		sum := 0
		for _, n := range in {
			sum += n
		}
		return job.SetOutput(sum) // the worker writes it with the completion
	}, catbird.WorkerOptions{PollInterval: 50 * time.Millisecond})
	go rt.Start(ctx)

	for {
		if err := catbird.Output(ctx, pool, id, &result); err == nil {
			if result != 6 {
				t.Fatalf("output %d, want 6", result)
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatal("no output")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The assigner announces new positions on channel cb_stream.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN cb_stream"); err != nil {
		t.Fatal(err)
	}
	if _, err := catbird.Publish(ctx, pool, "ev", nil, ""); err != nil {
		t.Fatal(err)
	}
	n, err := conn.Conn().WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("no notification: %v", err)
	}
	if n.Payload != "1" {
		t.Fatalf("notified position %q, want 1", n.Payload)
	}
}

// A result is written by the completion and by nothing else. An attempt that
// records one and then fails leaves nothing behind, so a reader never sees the
// result of work that did not finish, and the attempt that completes the job is
// the one whose result is readable.
func TestFailedAttemptWritesNoOutput(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const backoff = time.Second

	id, err := catbird.Enqueue(ctx, pool, "flaky", "output_retries", nil, catbird.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}

	rt := catbird.New(pool, catbird.Options{})
	rt.Worker("output_retries", func(ctx context.Context, job *catbird.Job) error {
		if err := job.SetOutput(job.Attempts); err != nil {
			return err
		}
		if job.Attempts == 1 {
			return errors.New("the first attempt fails after recording its result")
		}
		return nil
	}, catbird.WorkerOptions{Backoff: backoff, PollInterval: 25 * time.Millisecond})

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go rt.Start(runCtx)

	// The retry row is the first moment the failed attempt is over: it is
	// written after the handler returned, and the backoff holds it there.
	waitFor(t, 5*time.Second, "the failed attempt scheduled no retry", func() bool {
		return count(t, pool, `
			SELECT count(*) FROM cb_claims
			WHERE queue = 'output_retries' AND status = 0 AND attempts = 1
			  AND visible_at > now()
		`) == 1
	})
	var raw json.RawMessage
	if err := catbird.Output(ctx, pool, id, &raw); !errors.Is(err, catbird.ErrNotFound) {
		t.Fatalf("the failed attempt left a result behind: %v", err)
	}

	waitFor(t, 5*time.Second, "the job was never completed", func() bool {
		return count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'output_retries'") == 0
	})
	var attempt int
	if err := catbird.Output(ctx, pool, id, &attempt); err != nil {
		t.Fatal(err)
	}
	if attempt != 2 {
		t.Fatalf("output %d, want 2: the result of the attempt that completed the job", attempt)
	}
}

// PublishBatch writes one row per message in a single statement, skips the keys
// that are already taken, and the batch is read in position order like any
// other published message.
func TestPublishBatchSkipsTakenKeys(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := catbird.Publish(ctx, pool, "record.work.1", "first", "taken"); err != nil {
		t.Fatal(err)
	}

	n, err := catbird.PublishBatch(ctx, pool, []catbird.BatchMessage{
		{Topic: "record.work.2", Payload: "second"},
		{Topic: "record.work.3", Payload: "skipped", DedupKey: "taken"}, // published above
		{Topic: "record.work.4", Payload: "third", DedupKey: "once"},
		{Topic: "record.work.5", Payload: "skipped", DedupKey: "once"}, // repeats a key from this batch
		{Topic: "other", Payload: "not on this topic"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("wrote %d messages, want 3", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_messages"); n != 4 {
		t.Errorf("%d messages in the table, want 4 with the one published before the batch", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims"); n != 0 {
		t.Errorf("published messages got %d claims, want none", n)
	}

	// An empty batch is a no-op.
	if n, err := catbird.PublishBatch(ctx, pool, nil); err != nil || n != 0 {
		t.Fatalf("empty batch wrote %d (%v)", n, err)
	}

	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	go rt.Start(ctx)
	cursor := catbird.Cursor{Name: "batch", Patterns: []string{"record.work.#"}}

	var got []string
	deadline := time.Now().Add(2 * time.Second)
	for len(got) < 3 && time.Now().Before(deadline) {
		msgs, err := cursor.Read(ctx, pool, 50)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			var s string
			json.Unmarshal(m.Payload, &s)
			got = append(got, s)
			if err := cursor.Ack(ctx, pool, m.Position); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if want := "first,second,third"; strings.Join(got, ",") != want {
		t.Fatalf("read %v, want %v", got, want)
	}
}

// One tick of the assigner assigns a whole backlog, not one statement's worth.
// A batch larger than the statement's limit used to wait a tick per 5000
// messages, so the last of these would only be readable four ticks after its
// commit.
func TestAssignerDrainsABacklogInOneTick(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const messages = 12000 // more than two statements' worth
	batch := make([]catbird.BatchMessage, messages)
	for i := range batch {
		batch[i] = catbird.BatchMessage{Topic: "backlog", Payload: i}
	}
	if n, err := catbird.PublishBatch(ctx, pool, batch); err != nil || n != messages {
		t.Fatalf("published %d messages (%v), want %d", n, err, messages)
	}

	// The tick is long enough that a second one cannot rescue an assigner that
	// stops after its first statement: the first fires at 2s, the deadline is
	// half a second before the second.
	rt := catbird.New(pool, catbird.Options{AssignEvery: 2 * time.Second})
	go rt.Start(ctx)

	assigned := 0
	deadline := time.Now().Add(3500 * time.Millisecond)
	for time.Now().Before(deadline) && assigned < messages {
		assigned = count(t, pool, "SELECT count(*) FROM cb_messages WHERE position IS NOT NULL")
		time.Sleep(50 * time.Millisecond)
	}
	if assigned != messages {
		t.Fatalf("%d messages have a position after one tick, want %d", assigned, messages)
	}
}

// EnqueueBatch creates one job per message that survives deduplication, and
// wakes the queue once for the whole batch — never once per job, and not at all
// while the jobs are still waiting on dependencies.
func TestEnqueueBatchWakesTheQueueOnce(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// This job's key is taken before the listener starts, so its notification
	// is not delivered here.
	if _, err := catbird.Enqueue(ctx, pool, "resize", "images", nil, catbird.EnqueueOptions{DedupKey: "taken"}); err != nil {
		t.Fatal(err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN cb_queue_images"); err != nil {
		t.Fatal(err)
	}

	n, err := catbird.EnqueueBatch(ctx, pool, "images", []catbird.BatchMessage{
		{Topic: "resize", Payload: 1, DedupKey: "taken"}, // enqueued above
		{Topic: "resize", Payload: 2, DedupKey: "once"},
		{Topic: "resize", Payload: 3, DedupKey: "once"}, // repeats a key from this batch
		{Topic: "resize", Payload: 4},
	}, catbird.EnqueueOptions{CorrelationID: "batch1"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("created %d jobs, want 2", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'images'"); n != 3 {
		t.Errorf("%d claims, want 3 with the job enqueued before the batch", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE correlation_id = 'batch1'"); n != 2 {
		t.Errorf("%d claims carry the batch's correlation id, want 2", n)
	}

	// A batch that still waits on a dependency stays out of the ready index and
	// sends nothing.
	if n, err := catbird.EnqueueBatch(ctx, pool, "images", []catbird.BatchMessage{
		{Topic: "resize", Payload: 5},
		{Topic: "resize", Payload: 6},
	}, catbird.EnqueueOptions{Dependencies: 1}); err != nil || n != 2 {
		t.Fatalf("waiting batch created %d jobs (%v)", n, err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE dependencies = 1"); n != 2 {
		t.Errorf("%d claims waiting on a dependency, want 2", n)
	}

	// The ready batch wakes the queue; the waiting batch, in its own
	// transaction, must not.
	first, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := conn.Conn().WaitForNotification(first); err != nil {
		t.Fatalf("the batch did not wake the queue: %v", err)
	}
	second, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	if _, err := conn.Conn().WaitForNotification(second); err == nil {
		t.Error("the batch waiting on a dependency woke the queue")
	}
}

// A trigger enqueues its whole batch in one statement, and a redone batch still
// produces one job per message.
func TestTriggerBatchIsEnqueuedOnce(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgs := make([]catbird.BatchMessage, 20)
	for i := range msgs {
		msgs[i] = catbird.BatchMessage{Topic: "record.work", Payload: i}
	}
	if n, err := catbird.PublishBatch(ctx, pool, msgs); err != nil || n != 20 {
		t.Fatalf("published %d messages (%v)", n, err)
	}

	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	catbird.NewTrigger(rt, "indexer", []string{"record.#"}, "index_queue", catbird.TriggerOptions{PollInterval: 50 * time.Millisecond})
	go rt.Start(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'index_queue'") < 20 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d jobs arrived", count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'index_queue'"))
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // let the trigger run again on its cursor
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'index_queue'"); n != 20 {
		t.Errorf("%d jobs for 20 messages", n)
	}
}

// CreatedAt is the message row's insert time, filled in both on a stream read
// and on a claimed job. The window is read from the database, so the test does
// not depend on the test process and the server agreeing on the clock.
func TestCreatedAtIsSetOnStreamAndJobMessages(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbNow := func() time.Time {
		t.Helper()
		var now time.Time
		if err := pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
			t.Fatal(err)
		}
		return now
	}
	start := dbNow()

	if _, err := catbird.Publish(ctx, pool, "age", "published", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := catbird.Enqueue(ctx, pool, "age", "age_queue", "enqueued", catbird.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	jobCreatedAt := make(chan time.Time, 1)
	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	catbird.NewWorker(rt, "age_queue", func(ctx context.Context, m *catbird.Job) error {
		jobCreatedAt <- m.CreatedAt
		return nil
	}, catbird.WorkerOptions{PollInterval: 50 * time.Millisecond})
	cursor := catbird.Cursor{Name: "age", Patterns: []string{"age"}}
	go rt.Start(ctx)

	var streamCreatedAt time.Time
	for streamCreatedAt.IsZero() {
		msgs, err := cursor.Read(ctx, pool, 50)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) > 0 {
			streamCreatedAt = msgs[0].CreatedAt
		} else if ctx.Err() != nil {
			t.Fatal("message never got a position")
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}

	var jobCreated time.Time
	select {
	case <-ctx.Done():
		t.Fatal("job never ran")
	case jobCreated = <-jobCreatedAt:
	}
	end := dbNow()

	for name, got := range map[string]time.Time{"stream": streamCreatedAt, "job": jobCreated} {
		if got.Before(start) || got.After(end) {
			t.Fatalf("%s message CreatedAt %v, want between %v and %v", name, got, start, end)
		}
	}
}

// Shutdown does not spend an attempt. A job stopped in the middle of its
// handler is handed back: the attempt is returned and the job is claimable
// again at once, so a rolling deploy costs neither retries nor lease time.
func TestShutdownReturnsTheJob(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := catbird.Enqueue(ctx, pool, "slow", "deploys", nil, catbird.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	rt := catbird.New(pool, catbird.Options{})
	rt.Worker("deploys", func(ctx context.Context, job *catbird.Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, catbird.WorkerOptions{BatchSize: 1, PollInterval: 100 * time.Millisecond})

	workers, stop := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() {
		rt.Start(workers)
		close(stopped)
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("the job never started")
	}
	stop()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("the runtime did not stop")
	}

	var attempts int
	var claimable bool
	err := pool.QueryRow(ctx, `
		SELECT attempts, visible_at <= now() FROM cb_claims WHERE queue = 'deploys'
	`).Scan(&attempts, &claimable)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d, want 0: shutdown spent an attempt on a job that did not fail", attempts)
	}
	if !claimable {
		t.Error("the job is not claimable again: shutdown left the lease deadline in place")
	}
}

// A handler that completes the job and then returns an error is not retried:
// the completion committed, and the retry carries the attempts lease token, so
// it finds no claim to correct.
func TestCompletedJobIsNotRetriedAfterAnError(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := catbird.Enqueue(ctx, pool, "done", "late_error", nil, catbird.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	var calls int32
	ran := make(chan struct{}, 4)
	rt := catbird.New(pool, catbird.Options{})
	rt.Worker("late_error", func(ctx context.Context, job *catbird.Job) error {
		atomic.AddInt32(&calls, 1)
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if err := catbird.Complete(ctx, tx, job); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		ran <- struct{}{}
		return errors.New("the work is done, the handler is unhappy")
	}, catbird.WorkerOptions{Backoff: 50 * time.Millisecond, PollInterval: 50 * time.Millisecond})

	runCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	go rt.Start(runCtx)

	select {
	case <-ran:
	case <-runCtx.Done():
		t.Fatal("the job did not run")
	}
	<-runCtx.Done()

	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'late_error'"); n != 0 {
		t.Errorf("%d claims left: the error undid a completion", n)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("the handler ran %d times, want 1: the completed job was retried", n)
	}
}

// waitFor polls cond every 10 milliseconds until it holds, and fails with msg
// when it has not held within timeout.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A handler that returns an error spends the attempt and keeps the job: the
// claim stays where it is and its next attempt is held back by Backoff.
// Without the wait, a queue whose downstream is down retries every failing job
// as fast as it can claim it.
func TestHandlerErrorSchedulesARetryAfterBackoff(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const backoff = 500 * time.Millisecond

	if _, err := catbird.Enqueue(ctx, pool, "flaky", "retries", nil, catbird.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	var calls int32
	rt := catbird.New(pool, catbird.Options{})
	rt.Worker("retries", func(ctx context.Context, job *catbird.Job) error {
		atomic.AddInt32(&calls, 1)
		if job.Attempts == 1 {
			return errors.New("the first attempt fails")
		}
		return nil
	}, catbird.WorkerOptions{Backoff: backoff, PollInterval: 25 * time.Millisecond})

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go rt.Start(runCtx)

	// How far off the retry is has to be read in the database's clock: the
	// backoff is an interval added to the server's now(), and the claim that
	// picks the job up again compares against the same now().
	waitFor(t, 5*time.Second, "the failed attempt scheduled no retry a backoff away", func() bool {
		return count(t, pool, `
			SELECT count(*) FROM cb_claims
			WHERE queue = 'retries' AND status = 0 AND attempts = 1
			  AND visible_at > now() + $1::interval
		`, backoff/2) == 1
	})
	waitFor(t, 5*time.Second, "the failed job was never retried", func() bool {
		return atomic.LoadInt32(&calls) == 2
	})
	waitFor(t, 5*time.Second, "the second attempt did not complete the job", func() bool {
		return count(t, pool, "SELECT count(*) FROM cb_claims WHERE queue = 'retries'") == 0
	})
}

// The last attempt ends the job: the claim is marked dead, no worker claims it
// again, and OnDead runs once with the attempt that failed.
func TestMaxAttemptsMarksTheJobDeadAndRunsOnDead(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	id, err := catbird.Enqueue(ctx, pool, "doomed", "dead_end", nil, catbird.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}

	var calls int32
	dead := make(chan *catbird.Job, 4)
	rt := catbird.New(pool, catbird.Options{})
	rt.Worker("dead_end", func(ctx context.Context, job *catbird.Job) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("this handler never succeeds")
	}, catbird.WorkerOptions{
		MaxAttempts:  3,
		Backoff:      20 * time.Millisecond,
		PollInterval: 25 * time.Millisecond,
		OnDead: func(ctx context.Context, job *catbird.Job) error {
			dead <- job
			return nil
		},
	})

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go rt.Start(runCtx)

	var job *catbird.Job
	select {
	case job = <-dead:
	case <-ctx.Done():
		t.Fatal("OnDead never ran")
	}
	if job.ID != id {
		t.Errorf("OnDead got job %d, want %d", job.ID, id)
	}
	if job.Attempts != 3 {
		t.Errorf("OnDead got attempt %d, want the last one, 3", job.Attempts)
	}

	// Backoff and poll interval are both far shorter than this wait, so a claim
	// that still matched the dead row would have run the handler again by now.
	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Errorf("the handler ran %d times, want MaxAttempts, 3", n)
	}
	if n := len(dead); n != 0 {
		t.Errorf("OnDead ran %d more times, want once", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE message_id = $1 AND status = 1 AND attempts = 3", id); n != 1 {
		t.Error("the dead job's claim is not marked dead at its last attempt")
	}
}

// A job that dies with a correlation id stops the rest of its group: every
// sibling that has not run is marked dead and no worker claims it. A job in
// another group is left alone.
func TestADeadJobCancelsItsCorrelationGroup(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := catbird.Enqueue(ctx, pool, "step.one", "cascade", nil, catbird.EnqueueOptions{CorrelationID: "wf1"}); err != nil {
		t.Fatal(err)
	}
	// The sibling and the bystander sit on a queue no worker in this test runs,
	// so what reaches them is the cascade and nothing else.
	sibling, err := catbird.Enqueue(ctx, pool, "step.two", "cascade_rest", nil, catbird.EnqueueOptions{CorrelationID: "wf1"})
	if err != nil {
		t.Fatal(err)
	}
	bystander, err := catbird.Enqueue(ctx, pool, "unrelated", "cascade_rest", nil, catbird.EnqueueOptions{CorrelationID: "wf2"})
	if err != nil {
		t.Fatal(err)
	}

	rt := catbird.New(pool, catbird.Options{})
	rt.Worker("cascade", func(ctx context.Context, job *catbird.Job) error {
		return errors.New("the first step fails")
	}, catbird.WorkerOptions{MaxAttempts: 1, PollInterval: 25 * time.Millisecond})

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go rt.Start(runCtx)

	waitFor(t, 5*time.Second, "the sibling of the dead job was never canceled", func() bool {
		return count(t, pool, "SELECT count(*) FROM cb_claims WHERE message_id = $1 AND status = 1", sibling) == 1
	})
	if n := count(t, pool, `
		SELECT count(*) FROM cb_claims
		WHERE message_id = $1 AND status = 0 AND dependencies = 0 AND visible_at <= now()
	`, bystander); n != 1 {
		t.Error("the cascade took a job from another correlation group")
	}
}

// Cancel stops the jobs of a group that have not started; the one already
// running finishes and completes. A cancel that also undid a running job would
// leave its writes half done with no claim left to retry them.
func TestCancelStopsWaitingJobsAndLetsARunningOneFinish(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	running, err := catbird.Enqueue(ctx, pool, "running", "cancels", nil, catbird.EnqueueOptions{CorrelationID: "wf"})
	if err != nil {
		t.Fatal(err)
	}
	// The delay keeps the second job out of the first claim, so the worker's one
	// slot goes to the job the handler below blocks in.
	waiting, err := catbird.Enqueue(ctx, pool, "waiting", "cancels", nil, catbird.EnqueueOptions{CorrelationID: "wf", Delay: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var ranWaiting atomic.Bool
	rt := catbird.New(pool, catbird.Options{})
	rt.Worker("cancels", func(ctx context.Context, job *catbird.Job) error {
		if job.Topic == "waiting" {
			ranWaiting.Store(true)
			return nil
		}
		close(started)
		<-release
		return nil
	}, catbird.WorkerOptions{BatchSize: 1, PollInterval: 25 * time.Millisecond})

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go rt.Start(runCtx)

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("the first job never started")
	}
	if err := catbird.Cancel(ctx, pool, "wf"); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE message_id = $1 AND status = 1", waiting); n != 1 {
		t.Error("the waiting job was not canceled")
	}
	close(release)

	waitFor(t, 5*time.Second, "the running job's claim is still there: Cancel stopped a job that had already started", func() bool {
		return count(t, pool, "SELECT count(*) FROM cb_claims WHERE message_id = $1", running) == 0
	})

	// Past its delay the canceled job would be claimable again if the cancel
	// had not taken it out of the ready index.
	time.Sleep(500 * time.Millisecond)
	if ranWaiting.Load() {
		t.Error("the canceled job ran")
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE message_id = $1 AND status = 1 AND attempts = 0", waiting); n != 1 {
		t.Error("the canceled job was claimed after all: its claim is not the untouched row Cancel left")
	}
}

// waitForPositions blocks until the assigner has given out n positions.
func waitForPositions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int64) {
	t.Helper()
	for {
		last, err := catbird.LastPosition(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		if last >= n {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("only %d of %d messages got a position", last, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The three pattern forms: a topic on its own matches exactly, a prefix with
// ".#" matches the prefix and everything under it, and "#" matches the stream.
// "orders" is in the stream because a subtree has to stop at the separator.
func TestReadPatternForms(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, topic := range []string{"order", "order.paid", "order.paid.refund", "orders", "other"} {
		if _, err := catbird.Publish(ctx, pool, topic, topic, ""); err != nil {
			t.Fatal(err)
		}
	}
	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	go rt.Start(ctx)
	waitForPositions(t, ctx, pool, 5)

	for _, tt := range []struct {
		patterns []string
		want     string
	}{
		{[]string{"order"}, "order"},
		{[]string{"order.#"}, "order,order.paid,order.paid.refund"},
		{[]string{"order.paid.#"}, "order.paid,order.paid.refund"},
		{[]string{"orders", "other"}, "orders,other"},
		{[]string{"#"}, "order,order.paid,order.paid.refund,orders,other"},
		{[]string{"order.nothing.#"}, ""},
	} {
		msgs, err := catbird.ReadAfter(ctx, pool, tt.patterns, 0, 50)
		if err != nil {
			t.Fatalf("%v: %v", tt.patterns, err)
		}
		var topics []string
		for _, m := range msgs {
			topics = append(topics, m.Topic)
		}
		if got := strings.Join(topics, ","); got != tt.want {
			t.Errorf("%v read %q, want %q", tt.patterns, got, tt.want)
		}
	}
}

// A pattern that is not one of the three forms is refused, so a caller who
// expects a wildcard is told instead of quietly reading nothing.
func TestReadRejectsPatterns(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, patterns := range [][]string{
		nil,
		{},
		{""},
		{"*"},
		{"order.*"},
		{"order.#.paid"},
		{"ord#er"},
		{"#", "order.*"},
	} {
		if _, err := catbird.ReadAfter(ctx, pool, patterns, 0, 50); !errors.Is(err, catbird.ErrBadPattern) {
			t.Errorf("%v was accepted (%v)", patterns, err)
		}
	}
}

// A reader that was away longer than GC keeps messages gets the rows that are
// left with nothing in them to say the rest is gone. OldestPosition is what
// tells it: past the position the reader held, messages were removed.
func TestStreamWindow(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, payload := range []string{"first", "second", "third"} {
		if _, err := catbird.Publish(ctx, pool, "ev", payload, ""); err != nil {
			t.Fatal(err)
		}
	}
	rt := catbird.New(pool, catbird.Options{AssignEvery: 20 * time.Millisecond})
	go rt.Start(ctx)
	waitForPositions(t, ctx, pool, 3)

	if oldest, err := catbird.OldestPosition(ctx, pool); err != nil || oldest != 1 {
		t.Fatalf("oldest position %d (%v), want 1", oldest, err)
	}

	// What GC does to a reader that stopped at position 0.
	if _, err := pool.Exec(ctx, `DELETE FROM cb_messages WHERE position <= 2`); err != nil {
		t.Fatal(err)
	}
	msgs, err := catbird.ReadAfter(ctx, pool, []string{"ev"}, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Position != 3 {
		t.Fatalf("read %d messages, want the one that survived", len(msgs))
	}
	oldest, err := catbird.OldestPosition(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if oldest <= 0+1 {
		t.Errorf("oldest position %d does not report the gap after position 0", oldest)
	}
}

// The LISTEN connection is hijacked out of the pool, so the pool keeps its full
// width while a process listens. A one-connection pool is the sharpest form of
// it: the listener takes the only connection, and everything else — here the
// assigner — still has to be able to work.
func TestListenDoesNotSpendAPoolConnection(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	narrow, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer narrow.Close()

	rt := catbird.New(narrow, catbird.Options{})
	done := make(chan struct{})
	go func() { defer close(done); rt.Start(ctx) }()

	if _, err := catbird.Publish(ctx, pool, "ev.one", nil, ""); err != nil {
		t.Fatal(err)
	}
	waitForPositions(t, ctx, pool, 1)

	if stat := narrow.Stat(); stat.AcquiredConns() != 0 {
		t.Errorf("%d pool connections still held by the listener, want 0", stat.AcquiredConns())
	}
	cancel()
	<-done
}

// The one LISTEN connection comes back after the server drops it, and a job
// enqueued afterwards still wakes its worker. The poll interval here is a
// minute, so a worker that runs the job did not fall back to polling for it.
func TestListenReconnectsAfterTheConnectionDrops(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ran := make(chan int64, 4)
	rt := catbird.New(pool, catbird.Options{ReconnectAfter: 100 * time.Millisecond})
	rt.Worker("reconnect", func(ctx context.Context, job *catbird.Job) error {
		ran <- job.ID
		return nil
	}, catbird.WorkerOptions{PollInterval: time.Minute})

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go rt.Start(runCtx)

	// The runtime's is the only connection that runs a LISTEN, so its backend
	// is the one whose last statement was one.
	listener := func() int {
		return count(t, pool, `
			SELECT coalesce(max(pid), 0) FROM pg_stat_activity
			WHERE datname = current_database() AND query LIKE 'LISTEN %'
		`)
	}
	var dropped int
	waitFor(t, 10*time.Second, "the runtime never listened", func() bool {
		dropped = listener()
		return dropped != 0
	})
	if _, err := pool.Exec(ctx, "SELECT pg_terminate_backend($1)", dropped); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "the listen connection did not come back", func() bool {
		pid := listener()
		return pid != 0 && pid != dropped
	})
	// A reconnect wakes every loop once, so the worker has already been nudged,
	// found nothing and gone back to waiting. What wakes it below is the
	// enqueue's own notification.
	time.Sleep(200 * time.Millisecond)

	id, err := catbird.Enqueue(ctx, pool, "after.reconnect", "reconnect", nil, catbird.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ran:
		if got != id {
			t.Errorf("ran job %d, want %d", got, id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the job did not run: no notification arrived after the reconnect")
	}
}
