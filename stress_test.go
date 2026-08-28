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
	client := catbird.NewClient()

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
				_, err := client.Enqueue(ctx, pool, "torture.task", "torture_queue", map[string]int{"prod": prodID, "task": j}, catbird.EnqueueOptions{})
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
	handler := func(ctx context.Context, tx catbird.Conn, msg catbird.Message) error {
		// Use the transaction so the Conn path is exercised.
		if _, err := tx.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}
		atomic.AddInt32(&processed, 1)
		return nil
	}

	workStart := time.Now()
	var workerWg sync.WaitGroup
	for i := 0; i < 5; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			catbird.NewWorker(pool, "torture_queue", handler, catbird.WorkerOptions{}).Start(ctx)
		}()
	}

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

func TestExactlyOnceDedup(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()
	client := catbird.NewClient()
	opts := catbird.EnqueueOptions{DedupKey: "deterministic-hash-12345"}

	id, err := client.Enqueue(ctx, pool, "task", "dedup_queue", nil, opts)
	if err != nil || id == 0 {
		t.Fatalf("first enqueue: id=%d err=%v", id, err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := client.Enqueue(ctx, pool, "task", "dedup_queue", nil, opts)
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
	client := catbird.NewClient()

	// Two parent steps and one external signal.
	childID, err := client.Enqueue(ctx, pool, "join_task", "dag_queue", nil, catbird.EnqueueOptions{Dependencies: 3})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE status = 0 AND dependencies = 0"); n != 0 {
		t.Fatalf("expected 0 ready claims, got %d", n)
	}

	if err := client.ResolveDependency(ctx, pool, childID); err != nil {
		t.Fatalf("resolve 1: %v", err)
	}
	if err := client.ResolveDependency(ctx, pool, childID); err != nil {
		t.Fatalf("resolve 2: %v", err)
	}
	if err := client.DeliverSignal(ctx, pool, childID, "human_approval", map[string]bool{"ok": true}); err != nil {
		t.Fatalf("signal: %v", err)
	}
	// The same signal again is a no-op, not an error.
	if err := client.DeliverSignal(ctx, pool, childID, "human_approval", map[string]bool{"ok": false}); err != nil {
		t.Fatalf("duplicate signal: %v", err)
	}
	// The job no longer waits: another signal or resolution is refused.
	if err := client.DeliverSignal(ctx, pool, childID, "other", nil); !errors.Is(err, catbird.ErrNotFound) {
		t.Fatalf("signal to non-waiting job: got %v, want ErrNotFound", err)
	}
	if err := client.ResolveDependency(ctx, pool, childID); !errors.Is(err, catbird.ErrNotFound) {
		t.Fatalf("resolve on non-waiting job: got %v, want ErrNotFound", err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_signals"); n != 1 {
		t.Fatalf("expected 1 signal row, got %d", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE status = 0 AND dependencies = 0"); n != 1 {
		t.Fatalf("expected 1 ready claim, got %d", n)
	}

	ran := make(chan catbird.Message, 1)
	worker := catbird.NewWorker(pool, "dag_queue", func(ctx context.Context, tx catbird.Conn, m catbird.Message) error {
		ran <- m
		return nil
	}, catbird.WorkerOptions{PollInterval: 50 * time.Millisecond})
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go worker.Start(runCtx)

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
	client := catbird.NewClient()

	if _, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS lease_test (attempt INT)"); err != nil {
		t.Fatal(err)
	}
	pool.Exec(ctx, "TRUNCATE lease_test")

	if _, err := client.Enqueue(ctx, pool, "slow", "lease_queue", nil, catbird.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	var calls int32
	handler := func(ctx context.Context, tx catbird.Conn, m catbird.Message) error {
		atomic.AddInt32(&calls, 1)
		if m.Attempts == 1 {
			time.Sleep(800 * time.Millisecond) // past the lease
		}
		_, err := tx.Exec(ctx, "INSERT INTO lease_test (attempt) VALUES ($1)", m.Attempts)
		return err
	}
	opts := catbird.WorkerOptions{Lease: 200 * time.Millisecond, PollInterval: 50 * time.Millisecond}
	go catbird.NewWorker(pool, "lease_queue", handler, opts).Start(ctx)
	go catbird.NewWorker(pool, "lease_queue", handler, opts).Start(ctx)

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
	client := catbird.NewClient()

	if _, err := client.Publish(ctx, pool, "image.uploaded", map[string]string{"url": "https://example.com/a.png"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Publish(ctx, pool, "other.topic", nil, ""); err != nil {
		t.Fatal(err)
	}

	client.RegisterTrigger(ctx, pool, "img", "image.%", "image_processing", catbird.StreamOptions{AssignEvery: 20 * time.Millisecond, PollInterval: 50 * time.Millisecond})

	got := make(chan catbird.Message, 16)
	go catbird.NewWorker(pool, "image_processing", func(ctx context.Context, tx catbird.Conn, m catbird.Message) error {
		got <- m
		return nil
	}, catbird.WorkerOptions{PollInterval: 50 * time.Millisecond}).Start(ctx)

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
	client := catbird.NewClient()

	if _, err := client.Enqueue(ctx, pool, "later", "gc_queue", nil, catbird.EnqueueOptions{Delay: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Enqueue(ctx, pool, "doomed", "gc_queue", nil, catbird.EnqueueOptions{CorrelationID: "wf1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Publish(ctx, pool, "event", nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := client.Cancel(ctx, pool, "wf1"); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE status = 0 AND dependencies = 0 AND visible_at <= now()"); n != 0 {
		t.Fatalf("canceled job still claimable")
	}

	time.Sleep(20 * time.Millisecond)
	if err := client.GC(ctx, pool, 10*time.Millisecond); err != nil {
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
	client := catbird.NewClient()

	consumer := catbird.NewStreamConsumer(ctx, pool, "late", catbird.StreamOptions{AssignEvery: 20 * time.Millisecond})

	// Message 1 is inserted first but its transaction stays open.
	slow, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Publish(ctx, slow, "ev", "first inserted, last committed", ""); err != nil {
		t.Fatal(err)
	}
	// Messages 2 and 3 commit right away.
	for _, p := range []string{"second", "third"} {
		if _, err := client.Publish(ctx, pool, "ev", p, ""); err != nil {
			t.Fatal(err)
		}
	}

	read := func(want []string) {
		t.Helper()
		var got []string
		deadline := time.Now().Add(2 * time.Second)
		for len(got) < len(want) && time.Now().Before(deadline) {
			msgs, err := consumer.FetchBatch(ctx, "ev")
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range msgs {
				var s string
				json.Unmarshal(m.Payload, &s)
				got = append(got, s)
				if err := consumer.Ack(ctx, pool, m.Position); err != nil {
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
	client := catbird.NewClient()

	id, err := client.Enqueue(ctx, pool, "sum", "out_queue", []int{1, 2, 3}, catbird.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Output(ctx, pool, id); !errors.Is(err, catbird.ErrNotFound) {
		t.Fatalf("output before completion: %v", err)
	}
	go catbird.NewWorker(pool, "out_queue", func(ctx context.Context, tx catbird.Conn, m catbird.Message) error {
		var in []int
		json.Unmarshal(m.Payload, &in)
		sum := 0
		for _, n := range in {
			sum += n
		}
		return client.SetOutput(ctx, tx, m.ID, sum)
	}, catbird.WorkerOptions{PollInterval: 50 * time.Millisecond}).Start(ctx)

	for {
		out, err := client.Output(ctx, pool, id)
		if err == nil {
			if string(out) != "6" {
				t.Fatalf("output %s, want 6", out)
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
	catbird.NewStreamConsumer(ctx, pool, "notify", catbird.StreamOptions{AssignEvery: 20 * time.Millisecond})
	if _, err := client.Publish(ctx, pool, "ev", nil, ""); err != nil {
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
