package stream

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	testPool *pgxpool.Pool
	testOnce sync.Once
)

func TestMain(m *testing.M) {
	code := m.Run()
	if testPool != nil {
		testPool.Close()
	}
	os.Exit(code)
}

func setupTest(t *testing.T) *pgxpool.Pool {
	t.Helper()
	testOnce.Do(func() {
		ctx := context.Background()
		db, err := sql.Open("pgx", testDSN)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		if err := MigrateUpTo(ctx, db, SchemaVersion); err != nil {
			panic(err)
		}
		testPool, err = pgxpool.New(ctx, testDSN)
		if err != nil {
			panic(err)
		}
		// test.sh recreates cb_tst per run, but bare go test reuses it:
		// drop this suite's streams so positions start from scratch.
		if _, err := testPool.Exec(ctx, `DELETE FROM cb_streams WHERE name LIKE 'go\_%'`); err != nil {
			panic(err)
		}
	})
	return testPool
}

// section A of scripts/stream_test.sql: publish / assign / cursor read
func TestPublishAssignRead(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_a"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		ref, err := Publish(ctx, pool, "go_a", "order.placed", i)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Kind != RefMessage || ref.Existing {
			t.Fatalf("ref = %+v, want a fresh message", ref)
		}
	}

	var n int
	if err := pool.QueryRow(ctx, "SELECT _cb_stream_assign_positions('go_a')").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("assigned %d, want 5", n)
	}

	if err := EnsureCursor(ctx, pool, "go_a", "idx", CursorOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	msgs, err := Read(ctx, pool, "go_a", "idx", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 5 {
		t.Fatalf("read %d messages, want 5", len(msgs))
	}
	for i, m := range msgs {
		if m.Pos != int64(i+1) {
			t.Fatalf("msgs[%d].Pos = %d, want %d", i, m.Pos, i+1)
		}
		if string(m.Payload) != strconv.Itoa(i+1) {
			t.Fatalf("msgs[%d].Payload = %s, want %d", i, m.Payload, i+1)
		}
		if m.Topic != "order.placed" || m.Stream != "go_a" {
			t.Fatalf("msgs[%d] = %+v", i, m)
		}
	}

	// caught up
	msgs, err = Read(ctx, pool, "go_a", "idx", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("read %d messages after catching up, want 0", len(msgs))
	}

	// re-ensure never moves an existing cursor
	if err := EnsureCursor(ctx, pool, "go_a", "idx", CursorOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	var pos int64
	if err := pool.QueryRow(ctx,
		"SELECT pos FROM cb_stream_cursors WHERE stream = 'go_a' AND name = 'idx'").Scan(&pos); err != nil {
		t.Fatal(err)
	}
	if pos != 5 {
		t.Fatalf("cursor pos = %d after re-ensure, want 5", pos)
	}
}

func TestPublishRefs(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_refs"); err != nil {
		t.Fatal(err)
	}

	// keep-oldest key dedup
	r1, err := Publish(ctx, pool, "go_refs", "t", 1, PublishOpts{Key: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if r1.Kind != RefMessage || r1.Existing {
		t.Fatalf("r1 = %+v, want a fresh message", r1)
	}
	r2, err := Publish(ctx, pool, "go_refs", "t", 2, PublishOpts{Key: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if !r2.Existing || r2.ID != r1.ID {
		t.Fatalf("r2 = %+v, want existing ref to %d", r2, r1.ID)
	}

	// a delayed publish parks in pending
	r3, err := Publish(ctx, pool, "go_refs", "t", 3, PublishOpts{Delay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if r3.Kind != RefPending {
		t.Fatalf("r3 = %+v, want pending", r3)
	}

	// reserved header keys are rejected
	if _, err := Publish(ctx, pool, "go_refs", "t", 4,
		PublishOpts{Headers: map[string]any{"cb_sneaky": 1}}); err == nil {
		t.Fatal("publish with cb_ header succeeded, want error")
	}

	// undefined stream maps to the sentinel
	if _, err := Publish(ctx, pool, "go_nope", "t", 1); !errors.Is(err, ErrNotDefined) {
		t.Fatalf("err = %v, want ErrNotDefined", err)
	}

	// batch publish
	ids, err := PublishPayloads(ctx, pool, "go_refs", "t", []any{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 {
		t.Fatalf("published %d payloads, want 3", len(ids))
	}
}

func TestConsume(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_c"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureCursor(ctx, pool, "go_c", "worker", CursorOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}

	publish := func(from, to int) {
		t.Helper()
		for i := from; i <= to; i++ {
			if _, err := Publish(ctx, pool, "go_c", "t", i); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_c')"); err != nil {
			t.Fatal(err)
		}
	}
	publish(1, 5)

	got := make(chan Message, 16)
	failedOnce := false // only the consumer goroutine touches it
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Consume(cctx, pool, "go_c", "worker", func(_ context.Context, batch []Message) error {
			if !failedOnce {
				failedOnce = true
				return errors.New("boom")
			}
			for _, m := range batch {
				got <- m
			}
			return nil
		}, ConsumeOpts{PollInterval: 20 * time.Millisecond})
	}()

	expect := func(from, to int) {
		t.Helper()
		for i := from; i <= to; i++ {
			select {
			case m := <-got:
				if m.Pos != int64(i) {
					t.Fatalf("pos = %d, want %d", m.Pos, i)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for pos %d", i)
			}
		}
	}

	// the failed first batch rolls back and redelivers whole, in order
	expect(1, 5)

	// caught up: new messages arrive on the next tick
	publish(6, 7)
	expect(6, 7)

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("consume returned %v, want context.Canceled", err)
	}

	// an undefined cursor fails fast instead of retrying
	err := Consume(ctx, pool, "go_c", "ghost", func(context.Context, []Message) error {
		return nil
	})
	if !errors.Is(err, ErrNotDefined) {
		t.Fatalf("consume on undefined cursor returned %v, want ErrNotDefined", err)
	}
}

func TestRunJobs(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	// a real retention so the prune job has something to enforce
	if err := Ensure(ctx, pool, "go_jobs", EnsureOpts{Retention: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if err := EnsureCursor(ctx, pool, "go_jobs", "w", CursorOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}

	jctx, cancel := context.WithCancel(ctx)
	jobsDone := make(chan error, 1)
	go func() {
		jobsDone <- RunJobs(jctx, pool, JobsOpts{
			AssignPositionsInterval: 20 * time.Millisecond,
			DeliverInterval:         20 * time.Millisecond,
			PruneInterval:           50 * time.Millisecond,
		})
	}()

	got := make(chan Message, 16)
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- Consume(jctx, pool, "go_jobs", "w", func(_ context.Context, batch []Message) error {
			for _, m := range batch {
				got <- m
			}
			return nil
		}, ConsumeOpts{PollInterval: 20 * time.Millisecond})
	}()

	expect := func(from, to int) {
		t.Helper()
		for i := from; i <= to; i++ {
			select {
			case m := <-got:
				if m.Pos != int64(i) {
					t.Fatalf("pos = %d, want %d", m.Pos, i)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for pos %d", i)
			}
		}
	}

	// the assign job numbers messages: publish with no manual assign call
	for i := 1; i <= 3; i++ {
		if _, err := Publish(ctx, pool, "go_jobs", "t", i); err != nil {
			t.Fatal(err)
		}
	}
	expect(1, 3)

	// the deliver job moves a due delayed message into the stream
	ref, err := Publish(ctx, pool, "go_jobs", "t", 4, PublishOpts{Delay: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Kind != RefPending {
		t.Fatalf("ref = %+v, want pending", ref)
	}
	expect(4, 4)

	// the deliver job fires a due schedule
	if err := DefineSchedule(ctx, pool, "go_jobs", "beat", ScheduleOpts{
		Every:   time.Hour,
		StartAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	expect(5, 5)

	// the prune job enforces retention on backdated rows
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_messages
		SET created_at = now() - interval '2 hours'
		WHERE stream = 'go_jobs' AND pos <= 3`); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var left int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM cb_stream_messages
			WHERE stream = 'go_jobs' AND pos <= 3`).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("prune left %d backdated messages", left)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	if err := <-jobsDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("jobs returned %v, want context.Canceled", err)
	}
	if err := <-consumeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("consume returned %v, want context.Canceled", err)
	}
}

func TestDefineSchedule(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_sched"); err != nil {
		t.Fatal(err)
	}

	// a cadence is always required
	if err := DefineSchedule(ctx, pool, "go_sched", "digest", ScheduleOpts{}); err == nil {
		t.Fatal("define without cadence succeeded, want error")
	}
	if err := DefineSchedule(ctx, pool, "go_sched", "digest",
		ScheduleOpts{Every: -time.Minute}); err == nil {
		t.Fatal("define with negative cadence succeeded, want error")
	}

	if err := DefineSchedule(ctx, pool, "go_sched", "digest", ScheduleOpts{
		Every:   time.Hour,
		Topic:   "digest.due",
		Payload: map[string]any{"kind": "digest"},
	}); err != nil {
		t.Fatal(err)
	}

	var every, topic, payload, catchUp string
	row := func() {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT every::text, coalesce(topic, ''), payload::text, catch_up::text
			FROM cb_stream_schedules WHERE stream = 'go_sched' AND name = 'digest'`,
		).Scan(&every, &topic, &payload, &catchUp); err != nil {
			t.Fatal(err)
		}
	}

	row()
	if every != "01:00:00" || topic != "digest.due" || payload != `{"kind": "digest"}` || catchUp != "skip" {
		t.Fatalf("schedule = %s %s %s %s", every, topic, payload, catchUp)
	}

	// an identical declaration writes nothing
	xmin := func() (x string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT xmin::text FROM cb_stream_schedules
			WHERE stream = 'go_sched' AND name = 'digest'`).Scan(&x); err != nil {
			t.Fatal(err)
		}
		return x
	}
	x1 := xmin()
	if err := DefineSchedule(ctx, pool, "go_sched", "digest", ScheduleOpts{
		Every:   time.Hour,
		Topic:   "digest.due",
		Payload: map[string]any{"kind": "digest"},
	}); err != nil {
		t.Fatal(err)
	}
	if x2 := xmin(); x2 != x1 {
		t.Fatalf("identical declaration rewrote the row: xmin %s -> %s", x1, x2)
	}

	// the call is the whole schedule: omitted fields reset to the defaults
	if err := DefineSchedule(ctx, pool, "go_sched", "digest",
		ScheduleOpts{Every: time.Hour, CatchUp: CatchUpAll}); err != nil {
		t.Fatal(err)
	}
	row()
	if every != "01:00:00" || topic != "" || payload != "{}" || catchUp != "all" {
		t.Fatalf("schedule after re-declaration = %s %q %s %s", every, topic, payload, catchUp)
	}

	deleted, err := DeleteSchedule(ctx, pool, "go_sched", "digest")
	if err != nil || !deleted {
		t.Fatalf("delete = %v, %v, want true", deleted, err)
	}
	deleted, err = DeleteSchedule(ctx, pool, "go_sched", "digest")
	if err != nil || deleted {
		t.Fatalf("second delete = %v, %v, want false", deleted, err)
	}
}

func TestEnsureQueue(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_q"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureQueue(ctx, pool, "go_q", "mailer", QueueOpts{
		StartPos:    At(0),
		ClaimTTL:    time.Minute,
		MaxAttempts: 5,
		BackoffKind: BackoffFixed,
		OnFail:      FailDrop,
	}); err != nil {
		t.Fatal(err)
	}

	var claimTTL, backoffKind, onFail string
	var maxAttempts, maxCrashes int
	row := func() {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT claim_ttl::text, max_attempts, max_crashes, backoff_kind::text, on_fail::text
			FROM cb_stream_queues WHERE stream = 'go_q' AND name = 'mailer'`,
		).Scan(&claimTTL, &maxAttempts, &maxCrashes, &backoffKind, &onFail); err != nil {
			t.Fatal(err)
		}
	}

	row()
	if claimTTL != "00:01:00" || maxAttempts != 5 || backoffKind != "fixed" || onFail != "drop" {
		t.Fatalf("queue = %s %d %s %s", claimTTL, maxAttempts, backoffKind, onFail)
	}
	if maxCrashes != 3 { // unmentioned: the default
		t.Fatalf("max_crashes = %d, want the default 3", maxCrashes)
	}

	// ensure is birth-only: an existing queue is never modified
	if err := EnsureQueue(ctx, pool, "go_q", "mailer", QueueOpts{MaxCrashes: 7}); err != nil {
		t.Fatal(err)
	}
	row()
	if maxCrashes != 3 || maxAttempts != 5 || backoffKind != "fixed" {
		t.Fatalf("queue after re-ensure = %d %d %s, want unchanged", maxCrashes, maxAttempts, backoffKind)
	}
}
