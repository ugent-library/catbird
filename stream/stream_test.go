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

	"github.com/jackc/pgx/v5/pgconn"
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
		// drop this suite's streams — including the retry and dead letter
		// streams failures spawn from them — so positions start from scratch.
		if _, err := testPool.Exec(ctx, `DELETE FROM cb_streams
			WHERE name LIKE 'go\_%' OR name LIKE 'sr.go\_%' OR name LIKE 'sd.go\_%'`); err != nil {
			panic(err)
		}
	})
	return testPool
}

// claimRange claims the next batch for a consumer; ok reports whether there
// was anything to claim.
func claimRange(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	stream, queue, consumer string, batchSize int,
) (fromPos, toPos int64, ok bool) {
	t.Helper()
	var from, to *int64
	var expiresAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT c.from_pos, c.to_pos, c.expires_at FROM cb_stream_claim($1, $2, $3, $4) c`,
		stream, queue, consumer, batchSize).Scan(&from, &to, &expiresAt); err != nil {
		t.Fatal(err)
	}
	if from == nil {
		return 0, 0, false
	}
	return *from, *to, true
}

// checkClaims asserts the structural rule every claim branch must preserve:
// open and closed claims exactly tile the region (closed_pos, claimed_pos] —
// no gaps, no overlaps, the first claim right after closed_pos, the last one
// ending at claimed_pos.
func checkClaims(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stream, queue string) {
	t.Helper()
	var closedPos, claimedPos int64
	if err := pool.QueryRow(ctx,
		`SELECT closed_pos, claimed_pos FROM cb_stream_queues WHERE stream = $1 AND name = $2`,
		stream, queue).Scan(&closedPos, &claimedPos); err != nil {
		t.Fatal(err)
	}
	if closedPos > claimedPos {
		t.Fatalf("closed_pos %d > claimed_pos %d", closedPos, claimedPos)
	}

	rows, err := pool.Query(ctx,
		`SELECT from_pos, to_pos, crashes FROM cb_stream_claims
		 WHERE stream = $1 AND queue = $2 ORDER BY from_pos`,
		stream, queue)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	expected := closedPos + 1
	for rows.Next() {
		var fromPos, toPos int64
		var crashes int
		if err := rows.Scan(&fromPos, &toPos, &crashes); err != nil {
			t.Fatal(err)
		}
		if fromPos != expected {
			t.Fatalf("tiling broken: claim starts at %d, expected %d", fromPos, expected)
		}
		if crashes < 0 {
			t.Fatal("negative crash count")
		}
		expected = toPos + 1
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if expected != claimedPos+1 {
		t.Fatalf("tiling broken: claims end at %d, claimed_pos is %d", expected-1, claimedPos)
	}
}

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

func TestConsumeQueue(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_cq"); err != nil {
		t.Fatal(err)
	}
	// no backoff so the failed message retries immediately
	if err := EnsureQueue(ctx, pool, "go_cq", "m", QueueOpts{
		StartPos:    At(0),
		BackoffKind: BackoffNone,
	}); err != nil {
		t.Fatal(err)
	}

	jctx, cancel := context.WithCancel(ctx)
	jobsDone := make(chan error, 1)
	go func() {
		jobsDone <- RunJobs(jctx, pool, JobsOpts{
			AssignPositionsInterval: 20 * time.Millisecond,
			DeliverInterval:         20 * time.Millisecond,
		})
	}()

	for i := 1; i <= 5; i++ {
		if _, err := Publish(ctx, pool, "go_cq", "t", i); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	counts := map[string]int{}
	handled := make(chan struct{}, 32)
	failedOnce := false
	queueDone := make(chan error, 1)
	go func() {
		queueDone <- ConsumeQueue(jctx, pool, "go_cq", "m", func(_ context.Context, m Message) error {
			mu.Lock()
			defer mu.Unlock()
			if string(m.Payload) == "3" && !failedOnce {
				failedOnce = true
				return errors.New("boom")
			}
			counts[string(m.Payload)]++
			handled <- struct{}{}
			return nil
		}, ConsumeQueueOpts{PollInterval: 20 * time.Millisecond})
	}()

	// all five process; the failed one comes back through the retry stream
	for range 5 {
		select {
		case <-handled:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for messages")
		}
	}
	mu.Lock()
	for i := 1; i <= 5; i++ {
		if counts[strconv.Itoa(i)] != 1 {
			t.Fatalf("counts = %v, want each of 1..5 exactly once", counts)
		}
	}
	mu.Unlock()

	var retried int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_messages WHERE stream = 'sr.go_cq.m'`).Scan(&retried); err != nil {
		t.Fatal(err)
	}
	if retried != 1 {
		t.Fatalf("retry stream holds %d messages, want 1", retried)
	}

	// the base queue drains: closed_pos reaches the tail
	deadline := time.Now().Add(5 * time.Second)
	for {
		var closed int64
		if err := pool.QueryRow(ctx,
			`SELECT closed_pos FROM cb_stream_queues WHERE stream = 'go_cq' AND name = 'm'`).Scan(&closed); err != nil {
			t.Fatal(err)
		}
		if closed == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("closed_pos = %d, want 5", closed)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	if err := <-queueDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("queue consume returned %v, want context.Canceled", err)
	}
	if err := <-jobsDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("jobs returned %v, want context.Canceled", err)
	}

	// adoption: a dead consumer's claim expires and another consumer takes it
	if err := EnsureQueue(ctx, pool, "go_cq", "adopt", QueueOpts{
		StartPos: At(0),
		ClaimTTL: 50 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_claim('go_cq', 'adopt', 'dead')`); err != nil {
		t.Fatal(err)
	}

	adopted := make(chan Message, 16)
	actx, acancel := context.WithCancel(ctx)
	adoptDone := make(chan error, 1)
	go func() {
		adoptDone <- ConsumeQueue(actx, pool, "go_cq", "adopt", func(_ context.Context, m Message) error {
			adopted <- m
			return nil
		}, ConsumeQueueOpts{PollInterval: 20 * time.Millisecond})
	}()
	for range 5 {
		select {
		case <-adopted:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the expired claim to be adopted")
		}
	}
	acancel()
	if err := <-adoptDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("adopting consume returned %v, want context.Canceled", err)
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

	nextAt := func() (ts time.Time) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT next_at FROM cb_stream_schedules
			WHERE stream = 'go_sched' AND name = 'digest'`).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		return ts
	}
	firstNextAt := nextAt()

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

	// the call is the whole schedule: omitted fields reset to the defaults,
	// and the same cadence keeps the phase
	if err := DefineSchedule(ctx, pool, "go_sched", "digest",
		ScheduleOpts{Every: time.Hour, CatchUp: CatchUpAll}); err != nil {
		t.Fatal(err)
	}
	row()
	if every != "01:00:00" || topic != "" || payload != "{}" || catchUp != "all" {
		t.Fatalf("schedule after re-declaration = %s %q %s %s", every, topic, payload, catchUp)
	}
	if !nextAt().Equal(firstNextAt) {
		t.Fatal("same cadence moved next_at")
	}

	// a new cadence re-anchors next_at
	if err := DefineSchedule(ctx, pool, "go_sched", "digest",
		ScheduleOpts{Every: 2 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if nextAt().Equal(firstNextAt) {
		t.Fatal("cadence change did not re-anchor next_at")
	}

	// an explicit start_at wins over the re-anchor: the deliberate state poke
	start := time.Now().Add(10 * time.Minute).Truncate(time.Microsecond)
	if err := DefineSchedule(ctx, pool, "go_sched", "digest",
		ScheduleOpts{Every: 3 * time.Hour, StartAt: start}); err != nil {
		t.Fatal(err)
	}
	if got := nextAt(); !got.Equal(start) {
		t.Fatalf("next_at = %v, want the explicit start %v", got, start)
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

// claims: batch, per-call ttl, out-of-order close, release, adopt, fence
func TestClaims(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_b"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := Publish(ctx, pool, "go_b", "t", i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_b')"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureQueue(ctx, pool, "go_b", "mailer", QueueOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}

	// c1 takes the first batch
	if from, to, ok := claimRange(t, ctx, pool, "go_b", "mailer", "c1", 3); !ok || from != 1 || to != 3 {
		t.Fatalf("claim = %d..%d, %v, want 1..3", from, to, ok)
	}

	// c2 takes the rest with a per-call ttl, and the claim stores it
	var from2, to2 *int64
	var exp *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT c.from_pos, c.to_pos, c.expires_at FROM cb_stream_claim($1, $2, $3, $4, $5) c`,
		"go_b", "mailer", "c2", 3, nullInterval(15*time.Minute)).Scan(&from2, &to2, &exp); err != nil {
		t.Fatal(err)
	}
	if from2 == nil || *from2 != 4 || *to2 != 5 {
		t.Fatalf("claim = %v..%v, want 4..5", from2, to2)
	}
	var ttl string
	if err := pool.QueryRow(ctx,
		`SELECT ttl::text FROM cb_stream_claims WHERE stream = 'go_b' AND from_pos = 4`).Scan(&ttl); err != nil {
		t.Fatal(err)
	}
	if ttl != "00:15:00" {
		t.Fatalf("stored ttl = %s, want 00:15:00", ttl)
	}

	// caught up: nothing to claim
	if from, to, ok := claimRange(t, ctx, pool, "go_b", "mailer", "c1", 3); ok {
		t.Fatalf("claim past the tail = %d..%d, want nothing", from, to)
	}
	checkClaims(t, ctx, pool, "go_b", "mailer")

	// the owner extends its claim
	var newExp *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT cb_stream_extend_claim('go_b', 'mailer', 'c1', 1)`).Scan(&newExp); err != nil {
		t.Fatal(err)
	}
	if newExp == nil {
		t.Fatal("extend by the owner returned NULL")
	}

	closedPos := func() (pos int64) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT closed_pos FROM cb_stream_queues WHERE stream = 'go_b' AND name = 'mailer'`).Scan(&pos); err != nil {
			t.Fatal(err)
		}
		return pos
	}

	// out-of-order close: the open claim at 1..3 holds the floor
	if _, err := pool.Exec(ctx, `SELECT cb_stream_close_claim('go_b', 'mailer', 'c2', 4)`); err != nil {
		t.Fatal(err)
	}
	if pos := closedPos(); pos != 0 {
		t.Fatalf("closed_pos = %d after out-of-order close, want 0", pos)
	}
	checkClaims(t, ctx, pool, "go_b", "mailer")

	// c1 releases 1..3 and c3 adopts it; c1 is fenced out from then on
	if _, err := pool.Exec(ctx, `SELECT cb_stream_release_claim('go_b', 'mailer', 'c1', 1)`); err != nil {
		t.Fatal(err)
	}
	if from, to, ok := claimRange(t, ctx, pool, "go_b", "mailer", "c3", 10); !ok || from != 1 || to != 3 {
		t.Fatalf("claim = %d..%d, %v, want to adopt 1..3", from, to, ok)
	}
	if err := pool.QueryRow(ctx,
		`SELECT cb_stream_extend_claim('go_b', 'mailer', 'c1', 1)`).Scan(&newExp); err != nil {
		t.Fatal(err)
	}
	if newExp != nil {
		t.Fatal("zombie extend not fenced")
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_close_claim('go_b', 'mailer', 'c1', 1)`); err != nil {
		t.Fatal(err)
	}
	var closed bool
	if err := pool.QueryRow(ctx,
		`SELECT closed FROM cb_stream_claims WHERE stream = 'go_b' AND from_pos = 1`).Scan(&closed); err != nil {
		t.Fatal(err)
	}
	if closed {
		t.Fatal("zombie close not fenced")
	}

	// the owner's close chases closed_pos through the earlier out-of-order one
	if _, err := pool.Exec(ctx, `SELECT cb_stream_close_claim('go_b', 'mailer', 'c3', 1)`); err != nil {
		t.Fatal(err)
	}
	if pos := closedPos(); pos != 5 {
		t.Fatalf("closed_pos = %d, want 5", pos)
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_claims WHERE stream = 'go_b'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%d claims left after closing, want 0", count)
	}
	checkClaims(t, ctx, pool, "go_b", "mailer")

	// an undefined queue fails fast; a non-positive ttl is rejected
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_claim('go_b', 'nope', 'c1', 3)`); !errors.Is(wrapErr(err), ErrNotDefined) {
		t.Fatalf("claim on undefined queue returned %v, want ErrNotDefined", err)
	}
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_extend_claim('go_b', 'mailer', 'c1', 1, interval '0')`); !errors.Is(wrapErr(err), ErrInvalid) {
		t.Fatalf("extend with zero ttl returned %v, want ErrInvalid", err)
	}
}

// fail: retry stream, duplicate fail, exhaustion to the dead letter stream,
// drop policy
func TestFail(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_fail"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureQueue(ctx, pool, "go_fail", "payer", QueueOpts{
		StartPos:    At(0),
		MaxAttempts: 2,
		BackoffKind: BackoffFixed,
		BackoffBase: time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_fail", "t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_fail')"); err != nil {
		t.Fatal(err)
	}

	if from, to, ok := claimRange(t, ctx, pool, "go_fail", "payer", "c1", 10); !ok || from != 1 || to != 1 {
		t.Fatalf("claim = %d..%d, %v, want 1..1", from, to, ok)
	}
	// a duplicate fail for the same message collapses into one retry
	for range 2 {
		if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('go_fail', 'payer', 1, 'boom')`); err != nil {
			t.Fatal(err)
		}
	}
	var pending int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_pending WHERE stream = 'sr.go_fail.payer'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("retry stream holds %d pending messages, want 1", pending)
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_close_claim('go_fail', 'payer', 'c1', 1)`); err != nil {
		t.Fatal(err)
	}

	// the backoff delay passes and the retry counts its attempt
	time.Sleep(50 * time.Millisecond)
	if _, err := pool.Exec(ctx, `SELECT _cb_stream_deliver_pending()`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT _cb_stream_assign_positions('sr.go_fail.payer')`); err != nil {
		t.Fatal(err)
	}
	var attempt string
	if err := pool.QueryRow(ctx,
		`SELECT headers->>'cb_attempt' FROM cb_stream_messages WHERE stream = 'sr.go_fail.payer'`).Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	if attempt != "1" {
		t.Fatalf("cb_attempt = %s, want 1", attempt)
	}

	// the second failure exhausts max_attempts: the message is archived with
	// its origin, not retried again
	if from, to, ok := claimRange(t, ctx, pool, "sr.go_fail.payer", "payer", "c1", 10); !ok || from != 1 || to != 1 {
		t.Fatalf("retry claim = %d..%d, %v, want 1..1", from, to, ok)
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('sr.go_fail.payer', 'payer', 1, 'boom again')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_close_claim('sr.go_fail.payer', 'payer', 'c1', 1)`); err != nil {
		t.Fatal(err)
	}
	var dead int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_messages WHERE stream = 'sd.go_fail'`).Scan(&dead); err != nil {
		t.Fatal(err)
	}
	if dead != 1 {
		t.Fatalf("dead letter stream holds %d messages, want 1", dead)
	}
	var originPos string
	if err := pool.QueryRow(ctx,
		`SELECT headers->>'cb_origin_pos' FROM cb_stream_messages WHERE stream = 'sd.go_fail'`).Scan(&originPos); err != nil {
		t.Fatal(err)
	}
	if originPos != "1" {
		t.Fatalf("cb_origin_pos = %s, want 1", originPos)
	}

	// auto-created stream retention: retries are handled history and kept a
	// bounded while, dead letters have not been handled and stay forever
	var retryBounded bool
	if err := pool.QueryRow(ctx,
		`SELECT retention = interval '7 days' FROM cb_streams WHERE name = 'sr.go_fail.payer'`).Scan(&retryBounded); err != nil {
		t.Fatal(err)
	}
	if !retryBounded {
		t.Fatal("retry stream retention should default to 7 days")
	}
	var forever bool
	if err := pool.QueryRow(ctx,
		`SELECT retention = cb_forever() FROM cb_streams WHERE name = 'sd.go_fail'`).Scan(&forever); err != nil {
		t.Fatal(err)
	}
	if !forever {
		t.Fatal("dead letter stream should keep forever")
	}

	// on_fail = 'drop': retries stop and nothing is archived
	if err := Ensure(ctx, pool, "go_drop"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureQueue(ctx, pool, "go_drop", "binman", QueueOpts{
		StartPos:    At(0),
		MaxAttempts: 1,
		OnFail:      FailDrop,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_drop", "t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_drop')"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := claimRange(t, ctx, pool, "go_drop", "binman", "c1", 10); !ok {
		t.Fatal("nothing to claim")
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('go_drop', 'binman', 1, 'nope')`); err != nil {
		t.Fatal(err)
	}
	var streams int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_streams WHERE name LIKE '%go\_drop%'`).Scan(&streams); err != nil {
		t.Fatal(err)
	}
	if streams != 1 {
		t.Fatalf("found %d go_drop streams, want just the base one", streams)
	}
}

// the crash ladder: whole-range redelivery below the limit, split at the
// limit, solo trial, archive above it
func TestCrashLadder(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_ladder"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureQueue(ctx, pool, "go_ladder", "runner", QueueOpts{
		StartPos:   At(0),
		MaxCrashes: 1,
	}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := Publish(ctx, pool, "go_ladder", "t", i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_ladder')"); err != nil {
		t.Fatal(err)
	}

	// a crashed consumer leaves its claim to run out; force that instantly
	expire := func(fromPos int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE cb_stream_claims SET expires_at = clock_timestamp()
			WHERE stream = 'go_ladder' AND from_pos = $1`, fromPos); err != nil {
			t.Fatal(err)
		}
	}

	// fresh claim, then one whole-range redelivery (crash 1 = the limit)
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c1", 10); !ok || from != 1 || to != 3 {
		t.Fatalf("claim = %d..%d, %v, want 1..3", from, to, ok)
	}
	expire(1)
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c2", 10); !ok || from != 1 || to != 3 {
		t.Fatalf("claim = %d..%d, %v, want the whole 1..3 redelivered", from, to, ok)
	}
	checkClaims(t, ctx, pool, "go_ladder", "runner")

	// at the limit nobody knows which message is to blame: the caller gets
	// the head solo and the tail respawns already expired
	expire(1)
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c3", 10); !ok || from != 1 || to != 1 {
		t.Fatalf("claim = %d..%d, %v, want the split head 1..1", from, to, ok)
	}
	checkClaims(t, ctx, pool, "go_ladder", "runner")
	// message 1 was innocent: it closes normally
	if _, err := pool.Exec(ctx, `SELECT cb_stream_close_claim('go_ladder', 'runner', 'c3', 1)`); err != nil {
		t.Fatal(err)
	}

	// message 2 gets its solo slice, crashes alone, and is archived in the
	// same call that hands message 3 its own trial
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c4", 10); !ok || from != 2 || to != 2 {
		t.Fatalf("claim = %d..%d, %v, want 2..2", from, to, ok)
	}
	expire(2)
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c5", 10); !ok || from != 3 || to != 3 {
		t.Fatalf("claim = %d..%d, %v, want 3..3", from, to, ok)
	}
	checkClaims(t, ctx, pool, "go_ladder", "runner")
	var dead int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_messages WHERE stream = 'sd.go_ladder'`).Scan(&dead); err != nil {
		t.Fatal(err)
	}
	if dead != 1 {
		t.Fatalf("dead letter stream holds %d messages, want the solo crasher", dead)
	}

	if _, err := pool.Exec(ctx, `SELECT cb_stream_close_claim('go_ladder', 'runner', 'c5', 3)`); err != nil {
		t.Fatal(err)
	}
	var closedPos int64
	if err := pool.QueryRow(ctx,
		`SELECT closed_pos FROM cb_stream_queues WHERE stream = 'go_ladder' AND name = 'runner'`).Scan(&closedPos); err != nil {
		t.Fatal(err)
	}
	if closedPos != 3 {
		t.Fatalf("closed_pos = %d, want 3", closedPos)
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_claims WHERE stream = 'go_ladder'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%d claims left, want 0", count)
	}
	checkClaims(t, ctx, pool, "go_ladder", "runner")
}

func TestPublishPayloads(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_batch"); err != nil {
		t.Fatal(err)
	}
	payloads := make([]any, 100)
	for i := range payloads {
		payloads[i] = i
	}
	ids, err := PublishPayloads(ctx, pool, "go_batch", "bulk", payloads)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 100 {
		t.Fatalf("published %d payloads, want 100", len(ids))
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_batch')"); err != nil {
		t.Fatal(err)
	}
	var lastPos int64
	if err := pool.QueryRow(ctx,
		`SELECT last_pos FROM cb_streams WHERE name = 'go_batch'`).Scan(&lastPos); err != nil {
		t.Fatal(err)
	}
	if lastPos != 100 {
		t.Fatalf("last_pos = %d, want 100", lastPos)
	}
}

// retention: initial value, prune, forever, gap read, and re-ensure never
// modifying an existing stream
func TestPruneMessages(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	countMessages := func(cond string) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM cb_stream_messages WHERE `+cond).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	prune := func(stream string) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT _cb_stream_prune_messages($1)`, stream).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// go_ret carries a 7-day retention, an initial value at creation
	if err := Ensure(ctx, pool, "go_ret", EnsureOpts{Retention: 7 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := Publish(ctx, pool, "go_ret", "evt", i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_ret')"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_messages
		SET created_at = clock_timestamp() - interval '10 days'
		WHERE stream = 'go_ret' AND pos <= 6`); err != nil {
		t.Fatal(err)
	}
	if err := EnsureCursor(ctx, pool, "go_ret", "reader", CursorOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}

	// go_ret_keep sets no retention: keep forever
	if err := Ensure(ctx, pool, "go_ret_keep"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := Publish(ctx, pool, "go_ret_keep", "evt", i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_ret_keep')"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_messages
		SET created_at = clock_timestamp() - interval '10 days'
		WHERE stream = 'go_ret_keep'`); err != nil {
		t.Fatal(err)
	}

	othersBefore := countMessages(`stream NOT IN ('go_ret', 'go_ret_keep')`)

	// the stream's own retention applies
	if n := prune("go_ret"); n != 6 {
		t.Fatalf("pruned %d messages, want 6", n)
	}
	if n := countMessages(`stream = 'go_ret'`); n != 4 {
		t.Fatalf("%d survivors, want 4", n)
	}

	// a stream that never set a retention defaults to forever and keeps all
	var forever bool
	if err := pool.QueryRow(ctx,
		`SELECT retention = cb_forever() FROM cb_streams WHERE name = 'go_ret_keep'`).Scan(&forever); err != nil {
		t.Fatal(err)
	}
	if !forever {
		t.Fatal("new stream did not default to forever")
	}
	if n := prune("go_ret_keep"); n != 0 {
		t.Fatalf("forever stream pruned %d messages", n)
	}
	if n := countMessages(`stream = 'go_ret_keep'`); n != 3 {
		t.Fatalf("forever stream kept %d messages, want 3", n)
	}

	// pruning one stream never touches another
	if othersAfter := countMessages(`stream NOT IN ('go_ret', 'go_ret_keep')`); othersAfter != othersBefore {
		t.Fatalf("prune hit other streams: %d -> %d", othersBefore, othersAfter)
	}

	// one read skips the pruned gap and the cursor lands on the tail
	msgs, err := Read(ctx, pool, "go_ret", "reader", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 4 {
		t.Fatalf("read %d messages, want the 4 survivors", len(msgs))
	}
	if msgs[0].Pos != 7 {
		t.Fatalf("first survivor at pos %d, want 7", msgs[0].Pos)
	}
	var cursorPos int64
	if err := pool.QueryRow(ctx,
		`SELECT pos FROM cb_stream_cursors WHERE stream = 'go_ret' AND name = 'reader'`).Scan(&cursorPos); err != nil {
		t.Fatal(err)
	}
	if cursorPos != 10 {
		t.Fatalf("cursor pos = %d, want 10", cursorPos)
	}

	// survivors are recent: a second prune removes nothing
	if n := prune("go_ret"); n != 0 {
		t.Fatalf("second prune deleted %d survivors", n)
	}

	// whatever a re-ensure mentions, an existing stream is never modified;
	// retention changes are plain UPDATEs
	checkRetention := func(want string) {
		t.Helper()
		var eq bool
		var got string
		if err := pool.QueryRow(ctx,
			`SELECT retention = $1::interval, retention::text FROM cb_streams WHERE name = 'go_ret'`,
			want).Scan(&eq, &got); err != nil {
			t.Fatal(err)
		}
		if !eq {
			t.Fatalf("retention = %s, want %s", got, want)
		}
	}
	if err := Ensure(ctx, pool, "go_ret"); err != nil {
		t.Fatal(err)
	}
	checkRetention("7 days")
	if err := Ensure(ctx, pool, "go_ret", EnsureOpts{Retention: 14 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	checkRetention("7 days")
	if _, err := pool.Exec(ctx,
		`UPDATE cb_streams SET retention = interval '14 days' WHERE name = 'go_ret'`); err != nil {
		t.Fatal(err)
	}
	checkRetention("14 days")

	// the Forever constant is the SQL sentinel; zero and other negatives are
	// rejected, never silently coerced to forever
	if err := Ensure(ctx, pool, "go_ret_f", EnsureOpts{Retention: Forever}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT retention = cb_forever() FROM cb_streams WHERE name = 'go_ret_f'`).Scan(&forever); err != nil {
		t.Fatal(err)
	}
	if !forever {
		t.Fatal("Forever did not store the sentinel")
	}
	if err := Ensure(ctx, pool, "go_ret", EnsureOpts{Retention: -5 * 24 * time.Hour}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative retention returned %v, want ErrInvalid", err)
	}
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_ensure('go_ret', interval '0')`); !errors.Is(wrapErr(err), ErrInvalid) {
		t.Fatalf("zero retention returned %v, want ErrInvalid", err)
	}
}

// key prune: age out, keep pending, forever, and the delivery refresh
func TestPruneKeys(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	countKeys := func(cond string) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM cb_stream_keys WHERE `+cond).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	prune := func(stream string) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT _cb_stream_prune_keys($1)`, stream).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	refKind := func(key string) (kind string) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT ref_kind FROM cb_stream_keys WHERE stream = 'go_keys' AND key = $1`, key).Scan(&kind); err != nil {
			t.Fatal(err)
		}
		return kind
	}

	if err := Ensure(ctx, pool, "go_keys", EnsureOpts{Retention: 7 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_keys", "t", 1, PublishOpts{Key: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_keys", "t", 2, PublishOpts{Key: "young"}); err != nil {
		t.Fatal(err)
	}
	// 'stuck' waits undelivered; its key must outlive any retention
	if _, err := Publish(ctx, pool, "go_keys", "t", 3, PublishOpts{Key: "stuck", Delay: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_keys')"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_keys
		SET ref_created_at = clock_timestamp() - interval '10 days'
		WHERE stream = 'go_keys' AND key IN ('old', 'stuck')`); err != nil {
		t.Fatal(err)
	}

	// a stream that never set a retention keeps even ancient keys
	if err := Ensure(ctx, pool, "go_keys_keep"); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_keys_keep", "t", 1, PublishOpts{Key: "k1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_keys
		SET ref_created_at = clock_timestamp() - interval '10 days'
		WHERE stream = 'go_keys_keep'`); err != nil {
		t.Fatal(err)
	}

	othersBefore := countKeys(`stream NOT IN ('go_keys', 'go_keys_keep')`)

	if n := prune("go_keys"); n != 1 {
		t.Fatalf("pruned %d keys, want 1", n)
	}
	if n := countKeys(`stream = 'go_keys' AND key = 'old'`); n != 0 {
		t.Fatal("aged key survived")
	}
	if n := countKeys(`stream = 'go_keys' AND key = 'young'`); n != 1 {
		t.Fatal("young key pruned")
	}
	if kind := refKind("stuck"); kind != "pending" {
		t.Fatalf("stuck key ref_kind = %s, want the undelivered pending kept", kind)
	}
	// messages are the message janitor's job: key prune left them alone
	var messages int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_messages WHERE stream = 'go_keys'`).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if messages != 2 {
		t.Fatalf("key prune left %d messages, want 2", messages)
	}

	if n := prune("go_keys_keep"); n != 0 {
		t.Fatalf("forever stream pruned %d keys", n)
	}

	// pruning one stream never touches another's keys
	if othersAfter := countKeys(`stream NOT IN ('go_keys', 'go_keys_keep')`); othersAfter != othersBefore {
		t.Fatalf("key prune hit other streams: %d -> %d", othersBefore, othersAfter)
	}

	// an undefined stream raises
	if _, err := pool.Exec(ctx,
		`SELECT _cb_stream_prune_keys('go_ghost')`); !errors.Is(wrapErr(err), ErrNotDefined) {
		t.Fatalf("prune on undefined stream returned %v, want ErrNotDefined", err)
	}

	// the delivery refresh: a key that waited out the whole retention window
	// gets a fresh clock when its message is delivered; without the bump the
	// next prune would drop the key while its message is minutes old, letting
	// a duplicate publish through
	if _, err := Publish(ctx, pool, "go_keys", "t", 4, PublishOpts{Key: "reborn", Delay: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_keys
		SET ref_created_at = clock_timestamp() - interval '10 days'
		WHERE stream = 'go_keys' AND key = 'reborn'`); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := pool.Exec(ctx, `SELECT _cb_stream_deliver_pending()`); err != nil {
		t.Fatal(err)
	}
	if n := prune("go_keys"); n != 0 {
		t.Fatalf("prune deleted %d just-delivered keys", n)
	}
	if kind := refKind("reborn"); kind != "message" {
		t.Fatalf("reborn key ref_kind = %s, want the ref swapped to message", kind)
	}
}

// schedule delivery: on-time fire with the template copied, 'all' and 'skip'
// catch-up, re-arm, the interval guard (the declare semantics live in
// TestDefineSchedule)
func TestDeliverSchedules(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_beats"); err != nil {
		t.Fatal(err)
	}

	deliver := func() (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT _cb_stream_deliver_schedules()`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	countTopic := func(topic string) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM cb_stream_messages WHERE stream = 'go_beats' AND topic = $1`,
			topic).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	nextAt := func(name string) (ts time.Time) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT next_at FROM cb_stream_schedules WHERE stream = 'go_beats' AND name = $1`,
			name).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		return ts
	}

	// on-time tick: half an interval behind, one fires, and the appended
	// message carries the schedule's template payload
	if err := DefineSchedule(ctx, pool, "go_beats", "ontime", ScheduleOpts{
		Every:   time.Hour,
		Topic:   "ontime",
		Payload: map[string]any{"k": 1},
		StartAt: time.Now().Add(-30 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if n := deliver(); n != 1 {
		t.Fatalf("on-time delivered %d, want 1", n)
	}
	if n := countTopic("ontime"); n != 1 {
		t.Fatalf("on-time appended %d messages, want 1", n)
	}
	var payload string
	if err := pool.QueryRow(ctx,
		`SELECT payload::text FROM cb_stream_messages WHERE stream = 'go_beats' AND topic = 'ontime'`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != `{"k": 1}` {
		t.Fatalf("payload = %s, want the template copied", payload)
	}
	if !nextAt("ontime").After(time.Now()) {
		t.Fatal("re-arm left next_at in the past")
	}

	// 'all' catch-up: 3.5 intervals behind, all 4 ticks fire in one delivery
	if err := DefineSchedule(ctx, pool, "go_beats", "catchup", ScheduleOpts{
		Every:   time.Hour,
		Topic:   "catchup",
		CatchUp: CatchUpAll,
		StartAt: time.Now().Add(-3*time.Hour - 30*time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if n := deliver(); n != 4 {
		t.Fatalf("'all' catch-up delivered %d, want 4", n)
	}
	if n := countTopic("catchup"); n != 4 {
		t.Fatalf("'all' catch-up appended %d messages, want 4", n)
	}
	if !nextAt("catchup").After(time.Now()) {
		t.Fatal("catch-up next_at not in the future")
	}

	// 'skip' with a whole missed tick behind it: fire nothing, jump ahead
	if err := DefineSchedule(ctx, pool, "go_beats", "skipbeat", ScheduleOpts{
		Every:   time.Hour,
		Topic:   "skipbeat",
		StartAt: time.Now().Add(-3*time.Hour - 30*time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if n := deliver(); n != 0 {
		t.Fatalf("'skip' backlog delivered %d, want 0", n)
	}
	if n := countTopic("skipbeat"); n != 0 {
		t.Fatal("'skip' backlog fired anyway")
	}
	if !nextAt("skipbeat").After(time.Now()) {
		t.Fatal("'skip' did not jump ahead")
	}

	// 'skip' still fires an on-time tick: the policy governs missed ticks only
	if err := DefineSchedule(ctx, pool, "go_beats", "skipnow", ScheduleOpts{
		Every:   time.Hour,
		Topic:   "skipnow",
		StartAt: time.Now().Add(-30 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if n := deliver(); n != 1 {
		t.Fatalf("'skip' on-time delivered %d, want 1", n)
	}

	// a calendar cadence is rejected: fixed-duration epoch math must stay
	// exact. The Go API cannot even express one (Duration is fixed), so the
	// guard is checked at the SQL surface and at the table
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_define_schedule('go_beats', 'daily', every => interval '1 day')`); !errors.Is(wrapErr(err), ErrInvalid) {
		t.Fatalf("calendar cadence returned %v, want ErrInvalid", err)
	}
	var pgErr *pgconn.PgError
	if _, err := pool.Exec(ctx, `INSERT INTO cb_stream_schedules (stream, name, every, next_at)
		VALUES ('go_beats', 'direct', interval '1 month', clock_timestamp())`); !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("direct insert of a calendar cadence returned %v, want a check violation", err)
	}
}
