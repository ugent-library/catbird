package streams

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ugent-library/catbird/notify"
)

var (
	testPool *pgxpool.Pool
	testOnce sync.Once

	notifierOnce  sync.Once
	suiteNotifier *notify.Notifier
)

func TestMain(m *testing.M) {
	code := m.Run()
	if testPool != nil {
		testPool.Close()
	}
	os.Exit(code)
}

func setupTest(t testing.TB) *pgxpool.Pool {
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
		// drop this suite's streams (their subscriptions, claims and retry
		// rows cascade) so positions start from scratch.
		if _, err := testPool.Exec(ctx, `DELETE FROM cb_streams WHERE name LIKE 'go\_%'`); err != nil {
			panic(err)
		}
	})
	return testPool
}

// claimRange claims the next batch for a consumer; ok reports whether there
// was anything to claim.
func claimRange(t *testing.T, ctx context.Context, pool *pgxpool.Pool,
	stream, subscription, consumer string,
) (fromPos, toPos int64, ok bool) {
	t.Helper()
	var from, to *int64
	var expiresAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT c.from_pos, c.to_pos, c.expires_at FROM cb_stream_claim($1, $2, $3) c`,
		stream, subscription, consumer).Scan(&from, &to, &expiresAt); err != nil {
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
func checkClaims(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stream, subscription string) {
	t.Helper()
	var closedPos, claimedPos int64
	if err := pool.QueryRow(ctx,
		`SELECT closed_pos, claimed_pos FROM cb_stream_subscriptions WHERE stream = $1 AND name = $2`,
		stream, subscription).Scan(&closedPos, &claimedPos); err != nil {
		t.Fatal(err)
	}
	if closedPos > claimedPos {
		t.Fatalf("closed_pos %d > claimed_pos %d", closedPos, claimedPos)
	}

	rows, err := pool.Query(ctx,
		`SELECT from_pos, to_pos FROM cb_stream_claims
		 WHERE stream = $1 AND subscription = $2 ORDER BY from_pos`,
		stream, subscription)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	expected := closedPos + 1
	for rows.Next() {
		var fromPos, toPos int64
		if err := rows.Scan(&fromPos, &toPos); err != nil {
			t.Fatal(err)
		}
		if fromPos != expected {
			t.Fatalf("tiling broken: claim starts at %d, expected %d", fromPos, expected)
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
	refs, err := PublishMessages(ctx, pool, "go_refs", []BatchMessage{
		{Topic: "t", Payload: 1}, {Topic: "t", Payload: 2}, {Topic: "t", Payload: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Fatalf("published %d messages, want 3", len(refs))
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

// testNotifier starts one notifier for the whole suite, the way a real
// process runs one for all its consumers. It lives until the process ends.
func testNotifier(t testing.TB) *notify.Notifier {
	t.Helper()
	pool := setupTest(t)
	notifierOnce.Do(func() {
		suiteNotifier = notify.New(pool)
		go func() { _ = suiteNotifier.Start(context.Background()) }()
	})
	return suiteNotifier
}

// With every poll interval at 10s and every timeout at 5s, only the
// notify path can deliver: publish wakes the assigner, assigned positions
// wake the cursor and subscription consumers, and a delayed message's
// due time wakes delivery.
func TestNotifyWake(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()
	n := testNotifier(t)

	if err := Ensure(ctx, pool, "go_nfy"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureCursor(ctx, pool, "go_nfy", "w", CursorOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_nfy", "q"); err != nil {
		t.Fatal(err)
	}

	slow := 10 * time.Second
	jctx, cancel := context.WithCancel(ctx)
	ticksDone := make(chan error, 1)
	go func() {
		ticksDone <- StartTicker(jctx, pool, TickerOpts{
			AssignPositionsInterval: slow,
			DeliverInterval:         slow,
			Notifier:                n,
		})
	}()

	cursorGot := make(chan Message, 16)
	cursorDone := make(chan error, 1)
	go func() {
		cursorDone <- Consume(jctx, pool, "go_nfy", "w", func(_ context.Context, batch []Message) error {
			for _, m := range batch {
				cursorGot <- m
			}
			return nil
		}, ConsumeOpts{PollInterval: slow, Notifier: n})
	}()

	subGot := make(chan Message, 16)
	subDone := make(chan error, 1)
	go func() {
		subDone <- ConsumeSubscription(jctx, pool, "go_nfy", "q", func(_ context.Context, m Message) error {
			subGot <- m
			return nil
		}, ConsumeSubscriptionOpts{PollInterval: slow, Notifier: n})
	}()

	// give the new subscriptions a moment to reach the LISTEN connection:
	// a notification sent before that is gone, and this test has no poll
	// to fall back on
	time.Sleep(500 * time.Millisecond)

	expect := func(c chan Message, pos int64) {
		t.Helper()
		select {
		case m := <-c:
			if m.Pos != pos {
				t.Fatalf("pos = %d, want %d", m.Pos, pos)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("pos %d did not arrive within the notify window", pos)
		}
	}

	if _, err := Publish(ctx, pool, "go_nfy", "t", 1); err != nil {
		t.Fatal(err)
	}
	expect(cursorGot, 1)
	expect(subGot, 1)

	// a delayed message rides the due-time wake of the delivery tick
	if _, err := Publish(ctx, pool, "go_nfy", "t", 2, PublishOpts{Delay: 300 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	expect(cursorGot, 2)
	expect(subGot, 2)

	cancel()
	for _, done := range []chan error{ticksDone, cursorDone, subDone} {
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
}

func TestStartTicker(t *testing.T) {
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
	ticksDone := make(chan error, 1)
	go func() {
		ticksDone <- StartTicker(jctx, pool, TickerOpts{
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
	if err := <-ticksDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("jobs returned %v, want context.Canceled", err)
	}
	if err := <-consumeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("consume returned %v, want context.Canceled", err)
	}
}

func TestConsumeSubscription(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_cq"); err != nil {
		t.Fatal(err)
	}
	// no backoff so the failed message retries immediately
	if err := EnsureSubscription(ctx, pool, "go_cq", "m", SubscriptionOpts{
		StartPos:    At(0),
		BackoffKind: BackoffNone,
	}); err != nil {
		t.Fatal(err)
	}

	jctx, cancel := context.WithCancel(ctx)
	ticksDone := make(chan error, 1)
	go func() {
		ticksDone <- StartTicker(jctx, pool, TickerOpts{
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
	subDone := make(chan error, 1)
	go func() {
		subDone <- ConsumeSubscription(jctx, pool, "go_cq", "m", func(_ context.Context, m Message) error {
			mu.Lock()
			defer mu.Unlock()
			if string(m.Payload) == "3" && !failedOnce {
				failedOnce = true
				return errors.New("boom")
			}
			counts[string(m.Payload)]++
			handled <- struct{}{}
			return nil
		}, ConsumeSubscriptionOpts{PollInterval: 20 * time.Millisecond})
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

	// "3" failed first and succeeded on retry (the counts prove it): its retry
	// row is resolved and gone once the handler that re-ran it closed its claim
	retryRows := func(stream, subscription string) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM cb_stream_retries WHERE stream = $1 AND subscription = $2`,
			stream, subscription).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	retryDeadline := time.Now().Add(5 * time.Second)
	for retryRows("go_cq", "m") != 0 {
		if time.Now().After(retryDeadline) {
			t.Fatalf("retry rows = %d after draining, want 0", retryRows("go_cq", "m"))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// the base subscription drains: closed_pos reaches the tail
	deadline := time.Now().Add(5 * time.Second)
	for {
		var closed int64
		if err := pool.QueryRow(ctx,
			`SELECT closed_pos FROM cb_stream_subscriptions WHERE stream = 'go_cq' AND name = 'm'`).Scan(&closed); err != nil {
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
	if err := <-subDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("subscription consume returned %v, want context.Canceled", err)
	}
	if err := <-ticksDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("jobs returned %v, want context.Canceled", err)
	}

	// adoption: a failed consumer's claim expires and another consumer takes it
	if err := EnsureSubscription(ctx, pool, "go_cq", "adopt", SubscriptionOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	// crash detection latency is engine mechanics, tuned on the row
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_subscriptions SET claim_ttl = interval '50 milliseconds'
		WHERE stream = 'go_cq' AND name = 'adopt'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_claim('go_cq', 'adopt', 'failed')`); err != nil {
		t.Fatal(err)
	}

	adopted := make(chan Message, 16)
	actx, acancel := context.WithCancel(ctx)
	adoptDone := make(chan error, 1)
	go func() {
		adoptDone <- ConsumeSubscription(actx, pool, "go_cq", "adopt", func(_ context.Context, m Message) error {
			adopted <- m
			return nil
		}, ConsumeSubscriptionOpts{PollInterval: 20 * time.Millisecond})
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

// Concurrent ensures of new streams must serialize, not deadlock: the
// partition DDL locks cb_streams through the cloned foreign key, so an
// ensure that inserted its row before taking the ensure lock could
// deadlock with one holding the lock. Two booting nodes is the
// production shape.
func TestEnsureConcurrent(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	const workers = 4
	errs := make(chan error, workers)
	for w := range workers {
		go func() {
			for i := range 5 {
				// distinct new streams and shared ones, interleaved
				if err := Ensure(ctx, pool, fmt.Sprintf("go_enc%d_%d", w, i)); err != nil {
					errs <- err
					return
				}
				if err := Ensure(ctx, pool, "go_enc_shared"); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range workers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnsureSubscription(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_q"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_q", "mailer", SubscriptionOpts{
		StartPos:    At(0),
		MaxAttempts: 5,
		BackoffKind: BackoffFixed,
		OnFail:      FailDelete,
	}); err != nil {
		t.Fatal(err)
	}

	var claimTTL, backoffKind, onFail string
	var maxAttempts int
	row := func() {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT claim_ttl::text, max_attempts, backoff_kind::text, on_fail::text
			FROM cb_stream_subscriptions WHERE stream = 'go_q' AND name = 'mailer'`,
		).Scan(&claimTTL, &maxAttempts, &backoffKind, &onFail); err != nil {
			t.Fatal(err)
		}
	}

	row()
	if claimTTL != "00:00:30" || maxAttempts != 5 || backoffKind != "fixed" || onFail != "delete" {
		t.Fatalf("subscription = %s %d %s %s", claimTTL, maxAttempts, backoffKind, onFail)
	}

	// ensure is birth-only: an existing subscription is never modified
	if err := EnsureSubscription(ctx, pool, "go_q", "mailer", SubscriptionOpts{MaxAttempts: 9}); err != nil {
		t.Fatal(err)
	}
	row()
	if maxAttempts != 5 || backoffKind != "fixed" {
		t.Fatalf("subscription after re-ensure = %d %s, want unchanged", maxAttempts, backoffKind)
	}

	// batch size is subscription policy, born at ensure
	if err := EnsureSubscription(ctx, pool, "go_q", "sized", SubscriptionOpts{StartPos: At(0), ClaimBatchSize: 7}); err != nil {
		t.Fatal(err)
	}
	var batch int
	if err := pool.QueryRow(ctx, `SELECT claim_batch_size FROM cb_stream_subscriptions
		WHERE stream = 'go_q' AND name = 'sized'`).Scan(&batch); err != nil {
		t.Fatal(err)
	}
	if batch != 7 {
		t.Fatalf("claim_batch_size = %d, want 7", batch)
	}

	// dotted names are not valid stream names
	if err := EnsureSubscription(ctx, pool, "sd.go_q", "triage"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("subscription on a dotted stream returned %v, want ErrInvalid", err)
	}
}

// the loop owns the clock: a handler slower than the claim ttl keeps its
// claim through extension — processed once, nothing retried
func TestConsumeSubscriptionSlowHandler(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_slow"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_slow", "s", SubscriptionOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	// a short crash-detection window, so the handler outlives it three times
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_subscriptions SET claim_ttl = interval '300 milliseconds'
		WHERE stream = 'go_slow' AND name = 's'`); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_slow", "t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_slow')"); err != nil {
		t.Fatal(err)
	}

	handled := make(chan struct{}, 4)
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- ConsumeSubscription(cctx, pool, "go_slow", "s", func(hctx context.Context, m Message) error {
			select {
			case <-time.After(900 * time.Millisecond): // three claim ttls
			case <-hctx.Done():
				return hctx.Err()
			}
			handled <- struct{}{}
			return nil
		}, ConsumeSubscriptionOpts{PollInterval: 20 * time.Millisecond})
	}()

	select {
	case <-handled:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the slow handler")
	}

	// the claim survived its handler: the range closes and nothing crashed
	deadline := time.Now().Add(5 * time.Second)
	for {
		var closed int64
		if err := pool.QueryRow(ctx,
			`SELECT closed_pos FROM cb_stream_subscriptions WHERE stream = 'go_slow' AND name = 's'`).Scan(&closed); err != nil {
			t.Fatal(err)
		}
		if closed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("closed_pos = %d, want 1", closed)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var retried int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_retries WHERE stream = 'go_slow' AND subscription = 's'`,
	).Scan(&retried); err != nil {
		t.Fatal(err)
	}
	if retried != 0 {
		t.Fatalf("%d retry rows appeared, want none", retried)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("consume returned %v, want context.Canceled", err)
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
	// batch size is subscription policy: three per claim
	if err := EnsureSubscription(ctx, pool, "go_b", "mailer", SubscriptionOpts{
		StartPos:       At(0),
		ClaimBatchSize: 3,
	}); err != nil {
		t.Fatal(err)
	}

	// c1 takes the first batch
	if from, to, ok := claimRange(t, ctx, pool, "go_b", "mailer", "c1"); !ok || from != 1 || to != 3 {
		t.Fatalf("claim = %d..%d, %v, want 1..3", from, to, ok)
	}

	// c2 takes the rest with a per-call ttl, and the claim stores it
	var from2, to2 *int64
	var exp *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT c.from_pos, c.to_pos, c.expires_at FROM cb_stream_claim($1, $2, $3, $4) c`,
		"go_b", "mailer", "c2", nullInterval(15*time.Minute)).Scan(&from2, &to2, &exp); err != nil {
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
	if from, to, ok := claimRange(t, ctx, pool, "go_b", "mailer", "c1"); ok {
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
			`SELECT closed_pos FROM cb_stream_subscriptions WHERE stream = 'go_b' AND name = 'mailer'`).Scan(&pos); err != nil {
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
	if from, to, ok := claimRange(t, ctx, pool, "go_b", "mailer", "c3"); !ok || from != 1 || to != 3 {
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

	// an undefined subscription fails fast; a non-positive ttl is rejected
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_claim('go_b', 'nope', 'c1')`); !errors.Is(wrapErr(err), ErrNotDefined) {
		t.Fatalf("claim on undefined subscription returned %v, want ErrNotDefined", err)
	}
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_extend_claim('go_b', 'mailer', 'c1', 1, interval '0')`); !errors.Is(wrapErr(err), ErrInvalid) {
		t.Fatalf("extend with zero ttl returned %v, want ErrInvalid", err)
	}
}

// fail: a base failure seeds one retry row, a duplicate collapses into it,
// exhaustion gives up on it as a failed row, delete keeps nothing, a zombie is a no-op
func TestFail(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_fail"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_fail", "payer", SubscriptionOpts{
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

	retryRow := func(stream, subscription string, originPos int64) (attempt int, lastError string, failed, found bool) {
		t.Helper()
		err := pool.QueryRow(ctx,
			`SELECT attempt, coalesce(last_error, ''), failed FROM cb_stream_retries
			 WHERE stream = $1 AND subscription = $2 AND origin_pos = $3`,
			stream, subscription, originPos).Scan(&attempt, &lastError, &failed)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, "", false, false
		}
		if err != nil {
			t.Fatal(err)
		}
		return attempt, lastError, failed, true
	}
	rowCount := func(stream string) (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM cb_stream_retries WHERE stream = $1`, stream).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	if from, to, ok := claimRange(t, ctx, pool, "go_fail", "payer", "c1"); !ok || from != 1 || to != 1 {
		t.Fatalf("claim = %d..%d, %v, want 1..1", from, to, ok)
	}
	// a base message's first failure seeds one retry row (attempt 1); a
	// duplicate fail by the same base holder collapses into it
	for range 2 {
		if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('go_fail', 'payer', 'c1', 1, 'boom')`); err != nil {
			t.Fatal(err)
		}
	}
	if a, e, failed, ok := retryRow("go_fail", "payer", 1); !ok || a != 1 || e != "boom" || failed {
		t.Fatalf("retry row = attempt %d, err %q, failed %v, found %v; want 1, boom, false", a, e, failed, ok)
	}
	if n := rowCount("go_fail"); n != 1 {
		t.Fatalf("a duplicate fail made %d rows, want 1", n)
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_close_claim('go_fail', 'payer', 'c1', 1)`); err != nil {
		t.Fatal(err)
	}

	// after the backoff the row is handed out solo, minting its second try
	time.Sleep(20 * time.Millisecond)
	if from, to, ok := claimRange(t, ctx, pool, "go_fail", "payer", "c2"); !ok || from != 1 || to != 1 {
		t.Fatalf("retry claim = %d..%d, %v, want the solo 1..1", from, to, ok)
	}
	if a, _, _, _ := retryRow("go_fail", "payer", 1); a != 2 {
		t.Fatalf("attempt = %d after hand-out, want 2", a)
	}

	// failing at max_attempts gives up: the row is marked failed with its last verdict
	if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('go_fail', 'payer', 'c2', 1, 'boom again')`); err != nil {
		t.Fatal(err)
	}
	if a, e, failed, ok := retryRow("go_fail", "payer", 1); !ok || !failed || e != "boom again" {
		t.Fatalf("retry row = attempt %d, err %q, failed %v; want a failed row with the last verdict", a, e, failed)
	}
	if n := rowCount("go_fail"); n != 1 {
		t.Fatalf("go_fail holds %d retry rows, want 1", n)
	}

	// a superseded consumer's late report on the failed row is a silent no-op
	if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('go_fail', 'payer', 'zombie', 1, 'late')`); err != nil {
		t.Fatal(err)
	}
	if _, e, failed, _ := retryRow("go_fail", "payer", 1); !failed || e != "boom again" {
		t.Fatalf("failed row changed after a zombie report: err %q, failed %v", e, failed)
	}

	// on_fail = 'delete' with a one-strike budget: the failure keeps nothing
	if err := Ensure(ctx, pool, "go_drop"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_drop", "binman", SubscriptionOpts{
		StartPos:    At(0),
		MaxAttempts: 1,
		OnFail:      FailDelete,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_drop", "t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_drop')"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := claimRange(t, ctx, pool, "go_drop", "binman", "c1"); !ok {
		t.Fatal("nothing to claim")
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('go_drop', 'binman', 'c1', 1, 'nope')`); err != nil {
		t.Fatal(err)
	}
	if n := rowCount("go_drop"); n != 0 {
		t.Fatalf("drop kept %d retry rows, want none", n)
	}
}

// crash handling: a crashed range becomes silence retry rows (attempt 0), a
// release refunds the try a hand-out minted, and repeated crashes give up on the
// row to a failed silence row at max_attempts
func TestQuarantine(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_ladder"); err != nil {
		t.Fatal(err)
	}
	// no backoff, so a repaired retry row is due again at once
	if err := EnsureSubscription(ctx, pool, "go_ladder", "runner", SubscriptionOpts{
		StartPos:    At(0),
		MaxAttempts: 2,
		BackoffKind: BackoffNone,
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
	expireClaim := func(fromPos int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE cb_stream_claims SET expires_at = clock_timestamp()
			WHERE stream = 'go_ladder' AND from_pos = $1`, fromPos); err != nil {
			t.Fatal(err)
		}
	}
	// a crashed solo retry claim: its lease (claimable_at) lapses
	expireLease := func(originPos int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE cb_stream_retries SET claimable_at = clock_timestamp()
			WHERE stream = 'go_ladder' AND origin_pos = $1`, originPos); err != nil {
			t.Fatal(err)
		}
	}
	attemptOf := func(originPos int64) (a int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT attempt FROM cb_stream_retries
			WHERE stream = 'go_ladder' AND origin_pos = $1`, originPos).Scan(&a); err != nil {
			t.Fatal(err)
		}
		return a
	}

	// c1 claims the whole range, then crashes: the next claim records it as retries
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c1"); !ok || from != 1 || to != 3 {
		t.Fatalf("claim = %d..%d, %v, want 1..3", from, to, ok)
	}
	expireClaim(1)
	if _, _, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c2"); ok {
		t.Fatal("expected nothing on the call that records the crashed range as retries")
	}
	checkClaims(t, ctx, pool, "go_ladder", "runner")
	var closedPos int64
	if err := pool.QueryRow(ctx,
		`SELECT closed_pos FROM cb_stream_subscriptions WHERE stream = 'go_ladder' AND name = 'runner'`).Scan(&closedPos); err != nil {
		t.Fatal(err)
	}
	if closedPos != 3 {
		t.Fatalf("closed_pos = %d, want 3", closedPos)
	}

	// three silence rows, one per message, attempt 0 and due
	var silence int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cb_stream_retries
		WHERE stream = 'go_ladder' AND attempt = 0 AND last_error = 'silence' AND NOT failed`).Scan(&silence); err != nil {
		t.Fatal(err)
	}
	if silence != 3 {
		t.Fatalf("%d silence rows, want 3", silence)
	}

	// isolate row 1 for the lifecycle below; park the others out of the way
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_retries SET claimable_at = clock_timestamp() + interval '1 hour'
		WHERE stream = 'go_ladder' AND origin_pos <> 1`); err != nil {
		t.Fatal(err)
	}

	// a release hands the solo claim back uncharged: the minted try is refunded
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c3"); !ok || from != 1 || to != 1 {
		t.Fatalf("claim = %d..%d, %v, want the solo 1..1", from, to, ok)
	}
	if a := attemptOf(1); a != 1 {
		t.Fatalf("attempt = %d after hand-out, want 1", a)
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_release_claim('go_ladder', 'runner', 'c3', 1)`); err != nil {
		t.Fatal(err)
	}
	if a := attemptOf(1); a != 0 {
		t.Fatalf("attempt = %d after release, want the try refunded to 0", a)
	}

	// hand it out again (try 1), crash it, and let the next claim repair and
	// re-hand it (try 2 = max_attempts)
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c4"); !ok || from != 1 || to != 1 {
		t.Fatalf("claim = %d..%d, %v, want 1..1 again", from, to, ok)
	}
	expireLease(1)
	if from, to, ok := claimRange(t, ctx, pool, "go_ladder", "runner", "c5"); !ok || from != 1 || to != 1 {
		t.Fatalf("claim = %d..%d, %v, want the repaired 1..1", from, to, ok)
	}
	if a := attemptOf(1); a != 2 {
		t.Fatalf("attempt = %d, want 2", a)
	}

	// crash once more: at attempt 2 = max_attempts the next claim gives up
	expireLease(1)
	claimRange(t, ctx, pool, "go_ladder", "runner", "c6") // gives up on row 1 while repairing
	var failed bool
	var lastErr string
	if err := pool.QueryRow(ctx, `SELECT failed, coalesce(last_error, '') FROM cb_stream_retries
		WHERE stream = 'go_ladder' AND origin_pos = 1`).Scan(&failed, &lastErr); err != nil {
		t.Fatal(err)
	}
	if !failed || lastErr != "silence" {
		t.Fatalf("row 1 = failed %v, last_error %q; want a failed silence row", failed, lastErr)
	}
}

// a message the consumer already failed keeps its verdict row when the range
// is later recorded as retries: the crash's silence does not overwrite the verdict
func TestFailThenQuarantine(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_fq"); err != nil {
		t.Fatal(err)
	}
	// a long backoff keeps the verdict row out of the way (not due), so the
	// next claim reaches the crash-recording stage instead of serving that row
	if err := EnsureSubscription(ctx, pool, "go_fq", "q", SubscriptionOpts{
		StartPos:    At(0),
		MaxAttempts: 3,
		BackoffKind: BackoffFixed,
		BackoffBase: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if _, err := Publish(ctx, pool, "go_fq", "t", i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_fq')"); err != nil {
		t.Fatal(err)
	}

	if from, to, ok := claimRange(t, ctx, pool, "go_fq", "q", "c1"); !ok || from != 1 || to != 2 {
		t.Fatalf("claim = %d..%d, %v, want 1..2", from, to, ok)
	}
	// message 1 gets a verdict; message 2 is left to crash with the range
	if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('go_fq', 'q', 'c1', 1, 'boom')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_claims SET expires_at = clock_timestamp() WHERE stream = 'go_fq'`); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := claimRange(t, ctx, pool, "go_fq", "q", "c2"); ok {
		t.Fatal("expected nothing on the call that records the crashed range as retries")
	}

	// message 1 kept its verdict row; message 2 got a silence row
	kind := func(originPos int64) (attempt int, lastError string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT attempt, coalesce(last_error, '') FROM cb_stream_retries
			WHERE stream = 'go_fq' AND origin_pos = $1`, originPos).Scan(&attempt, &lastError); err != nil {
			t.Fatal(err)
		}
		return attempt, lastError
	}
	if a, e := kind(1); a != 1 || e != "boom" {
		t.Fatalf("row 1 = attempt %d, err %q; want the kept verdict (1, boom)", a, e)
	}
	if a, e := kind(2); a != 0 || e != "silence" {
		t.Fatalf("row 2 = attempt %d, err %q; want a silence row (0, silence)", a, e)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cb_stream_retries WHERE stream = 'go_fq'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("go_fq holds %d retry rows, want 2", rows)
	}
}

func TestPublishMessages(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_batch"); err != nil {
		t.Fatal(err)
	}

	// per-message topics, headers, keys and delay in one call; refs come
	// back in input order
	refs, err := PublishMessages(ctx, pool, "go_batch", []BatchMessage{
		{Topic: "a.b", Payload: 1},
		{Topic: "c", Payload: 2, Headers: map[string]any{"h": "v"}},
		{Payload: 3, Key: "k1"},
		{Payload: 4, Key: "k1"},        // duplicate key: keep-oldest
		{Payload: 5, Delay: time.Hour}, // parked as pending
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 5 {
		t.Fatalf("got %d refs, want 5", len(refs))
	}
	for i, r := range refs[:3] {
		if r.Kind != RefMessage || r.Existing {
			t.Fatalf("refs[%d] = %+v, want a fresh message", i, r)
		}
	}
	if !refs[3].Existing || refs[3].ID != refs[2].ID {
		t.Fatalf("refs[3] = %+v, want the existing ref of refs[2] (%+v)", refs[3], refs[2])
	}
	if refs[4].Kind != RefPending || refs[4].Existing {
		t.Fatalf("refs[4] = %+v, want a fresh pending", refs[4])
	}

	// topics landed per message; absent topic and headers stay NULL and {}
	var topics []string
	rows, err := pool.Query(ctx,
		`SELECT coalesce(topic, '') FROM cb_stream_messages WHERE stream = 'go_batch' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if topics, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		t.Fatal(err)
	}
	if want := []string{"a.b", "c", ""}; !slices.Equal(topics, want) {
		t.Fatalf("topics = %v, want %v", topics, want)
	}
	var headers, noHeaders string
	if err := pool.QueryRow(ctx, `SELECT headers::text FROM cb_stream_messages
		WHERE stream = 'go_batch' AND topic = 'c'`).Scan(&headers); err != nil {
		t.Fatal(err)
	}
	if headers != `{"h": "v"}` {
		t.Fatalf("headers = %s", headers)
	}
	if err := pool.QueryRow(ctx, `SELECT headers::text FROM cb_stream_messages
		WHERE stream = 'go_batch' AND topic = 'a.b'`).Scan(&noHeaders); err != nil {
		t.Fatal(err)
	}
	if noHeaders != `{}` {
		t.Fatalf("nil Headers landed as %s, want {}", noHeaders)
	}

	// the delayed message is pending, not published
	var pending int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_pending WHERE stream = 'go_batch'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("pending = %d, want 1", pending)
	}

	// delay and deliver_at on one element are mutually exclusive
	if _, err := PublishMessages(ctx, pool, "go_batch", []BatchMessage{
		{Payload: 1, Delay: time.Minute, DeliverAt: time.Now().Add(time.Minute)},
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}

	// cb_ headers are reserved, same as single publish
	if _, err := PublishMessages(ctx, pool, "go_batch", []BatchMessage{
		{Payload: 1, Headers: map[string]any{"cb_sneaky": 1}},
	}); err == nil {
		t.Fatal("publish with cb_ header succeeded, want error")
	}

	// a nil or empty batch is a no-op
	if refs, err := PublishMessages(ctx, pool, "go_batch", nil); err != nil || refs != nil {
		t.Fatalf("nil batch: refs = %v, err = %v", refs, err)
	}

	// malformed envelopes are rejected at the SQL surface (unreachable
	// from the Go client): non-array, non-object element, missing payload
	for _, bad := range []string{`{"payload": 1}`, `[1]`, `[{"topic": "t"}]`} {
		if _, err := pool.Exec(ctx,
			`SELECT cb_stream_publish_messages('go_batch', $1::jsonb)`, bad); !errors.Is(wrapErr(err), ErrInvalid) {
			t.Fatalf("messages = %s: err = %v, want ErrInvalid", bad, err)
		}
	}

	// positions cover exactly the immediate messages
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_batch')"); err != nil {
		t.Fatal(err)
	}
	var lastPos int64
	if err := pool.QueryRow(ctx,
		`SELECT last_pos FROM cb_streams WHERE name = 'go_batch'`).Scan(&lastPos); err != nil {
		t.Fatal(err)
	}
	if lastPos != 3 {
		t.Fatalf("last_pos = %d, want 3", lastPos)
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

// a poison handler: panics become failed attempts, and attempts exhaust to
// a failed retry row through the real loop
func TestConsumeSubscriptionPoisonHandler(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_poison"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_poison", "p", SubscriptionOpts{
		StartPos:    At(0),
		MaxAttempts: 2,
		BackoffKind: BackoffNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_poison", "t", 1); err != nil {
		t.Fatal(err)
	}

	jctx, cancel := context.WithCancel(ctx)
	ticksDone := make(chan error, 1)
	go func() {
		ticksDone <- StartTicker(jctx, pool, TickerOpts{
			AssignPositionsInterval: 20 * time.Millisecond,
			DeliverInterval:         20 * time.Millisecond,
		})
	}()

	var mu sync.Mutex
	calls := 0
	subDone := make(chan error, 1)
	go func() {
		subDone <- ConsumeSubscription(jctx, pool, "go_poison", "p", func(context.Context, Message) error {
			mu.Lock()
			calls++
			mu.Unlock()
			panic("boom")
		}, ConsumeSubscriptionOpts{PollInterval: 20 * time.Millisecond})
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var failed int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM cb_stream_retries WHERE stream = 'go_poison' AND subscription = 'p' AND failed`).Scan(&failed); err != nil {
			t.Fatal(err)
		}
		if failed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("poison message never went to a failed row")
		}
		time.Sleep(20 * time.Millisecond)
	}
	var attempt int
	var origin int64
	var errText string
	if err := pool.QueryRow(ctx, `SELECT attempt, origin_pos, coalesce(last_error, '')
		FROM cb_stream_retries WHERE stream = 'go_poison' AND subscription = 'p' AND failed`).Scan(&attempt, &origin, &errText); err != nil {
		t.Fatal(err)
	}
	if attempt != 2 || origin != 1 {
		t.Fatalf("failed row = attempt %d, origin %d; want 2 and 1", attempt, origin)
	}
	if !strings.HasPrefix(errText, "catbird: handler panic") {
		t.Fatalf("last_error = %q, want the recovered panic", errText)
	}
	mu.Lock()
	if calls != 2 {
		t.Fatalf("handler ran %d times, want 2", calls)
	}
	mu.Unlock()

	cancel()
	if err := <-subDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("subscription consume returned %v, want context.Canceled", err)
	}
	if err := <-ticksDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("jobs returned %v, want context.Canceled", err)
	}
}

// worker pools: competing consumers split the stream and nothing is lost
func TestConsumeSubscriptionCompeting(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_pool"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_pool", "w", SubscriptionOpts{
		StartPos:       At(0),
		ClaimBatchSize: 7,
	}); err != nil {
		t.Fatal(err)
	}
	msgs := make([]BatchMessage, 50)
	for i := range msgs {
		msgs[i] = BatchMessage{Topic: "t", Payload: i + 1}
	}
	if _, err := PublishMessages(ctx, pool, "go_pool", msgs); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_pool')"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	counts := map[string]int{}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 3)
	for range 3 {
		go func() {
			done <- ConsumeSubscription(cctx, pool, "go_pool", "w", func(_ context.Context, m Message) error {
				mu.Lock()
				counts[string(m.Payload)]++
				mu.Unlock()
				return nil
			}, ConsumeSubscriptionOpts{PollInterval: 20 * time.Millisecond})
		}()
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		var closed int64
		if err := pool.QueryRow(ctx,
			`SELECT closed_pos FROM cb_stream_subscriptions WHERE stream = 'go_pool' AND name = 'w'`).Scan(&closed); err != nil {
			t.Fatal(err)
		}
		if closed == 50 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("closed_pos = %d, want 50", closed)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	for range 3 {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("consume returned %v, want context.Canceled", err)
		}
	}

	// at-least-once: every message was handled; claims are exclusive, so
	// without expiries each exactly once
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i <= 50; i++ {
		if counts[strconv.Itoa(i)] < 1 {
			t.Fatalf("message %d was never handled", i)
		}
	}
}

// a frozen consumer discovers it lost its claim and stands down without
// spending an attempt
func TestConsumeSubscriptionClaimLost(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_zombie"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_zombie", "z", SubscriptionOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	// a tiny claim window, so the freeze outlives it quickly
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_subscriptions SET claim_ttl = interval '60 milliseconds'
		WHERE stream = 'go_zombie' AND name = 'z'`); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_zombie", "t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_zombie')"); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{}, 4)
	canceled := make(chan struct{}, 4)
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- ConsumeSubscription(cctx, pool, "go_zombie", "z", func(hctx context.Context, m Message) error {
			started <- struct{}{}
			<-hctx.Done() // frozen: never finishes on its own
			canceled <- struct{}{}
			return hctx.Err()
		}, ConsumeSubscriptionOpts{PollInterval: 20 * time.Millisecond})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never started")
	}

	// the extend cadence keeps reviving the claim; force the loss in the gap
	// between two extends: expire it, then let a competitor act on it — a
	// solo expired claim is recorded as retries by the competitor's claim call
	stolen := false
	for range 200 {
		if _, err := pool.Exec(ctx, `UPDATE cb_stream_claims SET expires_at = clock_timestamp()
			WHERE stream = 'go_zombie'`); err != nil {
			t.Fatal(err)
		}
		claimRange(t, ctx, pool, "go_zombie", "z", "thief")
		var copies int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM cb_stream_retries WHERE stream = 'go_zombie' AND subscription = 'z'`).Scan(&copies); err != nil {
			t.Fatal(err)
		}
		if copies == 1 {
			stolen = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !stolen {
		t.Fatal("could not take the claim between two extends")
	}

	// the zombie's next extend discovers the loss and cancels its handler
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("the frozen handler was never canceled")
	}

	// the zombie recorded no verdict: its range became one silence retry row.
	// another consumer may re-hand that due row (its attempt then climbs) —
	// ordinary retrying, not the zombie's charge — so assert on the verdict, not
	// the count of tries.
	var n int
	var lastErr string
	var failed bool
	if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(max(last_error), ''), coalesce(bool_or(failed), false)
		FROM cb_stream_retries WHERE stream = 'go_zombie' AND subscription = 'z'`).Scan(&n, &lastErr, &failed); err != nil {
		t.Fatal(err)
	}
	if n != 1 || lastErr != "silence" || failed {
		t.Fatalf("retry rows = %d, last_error %q, failed %v; want one silence row the zombie never charged", n, lastErr, failed)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("consume returned %v, want context.Canceled", err)
	}
}

// a retry actually waits its backoff before it can be delivered
func TestRetryBackoffTiming(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_wait"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_wait", "r", SubscriptionOpts{
		StartPos:    At(0),
		MaxAttempts: 3,
		BackoffKind: BackoffFixed,
		BackoffBase: 300 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(ctx, pool, "go_wait", "t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_wait')"); err != nil {
		t.Fatal(err)
	}
	if from, to, ok := claimRange(t, ctx, pool, "go_wait", "r", "c1"); !ok || from != 1 || to != 1 {
		t.Fatalf("claim = %d..%d, %v, want 1..1", from, to, ok)
	}
	if _, err := pool.Exec(ctx, `SELECT cb_stream_fail('go_wait', 'r', 'c1', 1, 'boom')`); err != nil {
		t.Fatal(err)
	}

	// the retry row is parked with the full fixed delay ahead of it
	var wait float64
	if err := pool.QueryRow(ctx, `SELECT extract(epoch FROM (claimable_at - clock_timestamp()))
		FROM cb_stream_retries WHERE stream = 'go_wait' AND subscription = 'r' AND origin_pos = 1`).Scan(&wait); err != nil {
		t.Fatal(err)
	}
	if wait < 0.2 || wait > 0.31 {
		t.Fatalf("retry scheduled %.3fs out, want about 0.3s", wait)
	}

	// not due yet: a claim finds nothing
	if _, _, ok := claimRange(t, ctx, pool, "go_wait", "r", "c2"); ok {
		t.Fatal("the retry was claimable before its backoff passed")
	}

	// after the wait it is due and handed out solo
	time.Sleep(350 * time.Millisecond)
	if from, to, ok := claimRange(t, ctx, pool, "go_wait", "r", "c3"); !ok || from != 1 || to != 1 {
		t.Fatalf("retry claim = %d..%d, %v, want 1..1 after the backoff", from, to, ok)
	}
}

// the topic-pattern compiler: match semantics and rejected grammar
func TestCompileTopic(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	matches := []struct {
		topic, pattern string
		want           bool
	}{
		{"a.b", "a.b", true},
		{"a.b", "a.c", false},
		{"record.work.created", "record.*.created", true},
		{"record.work.updated", "record.*.created", false},
		{"record.a.b.created", "record.*.created", false}, // * is exactly one segment
		{"a.b.c", "a.*.c", true},
		{"a.b", "*.b", true},
		{"record.work", "record.work.#", true}, // # matches the zero-segment tail
		{"record.work.updated.minor", "record.work.#", true},
		{"record.workx", "record.work.#", false},
		{"record", "record.work.#", false},
		{"a", "#", true},
		{"a.b.c", "#", true},
	}
	for _, m := range matches {
		var got bool
		if err := pool.QueryRow(ctx,
			`SELECT $1 ~ _cb_stream_compile_topic($2)`, m.topic, m.pattern).Scan(&got); err != nil {
			t.Fatalf("%q ~ %q: %v", m.topic, m.pattern, err)
		}
		if got != m.want {
			t.Errorf("%q ~ %q = %v, want %v", m.topic, m.pattern, got, m.want)
		}
	}

	for _, bad := range []string{
		"", "a..b", ".a", "a.", "a.#.b", "a*", "a#", "#a", "a b",
	} {
		_, err := pool.Exec(ctx, `SELECT _cb_stream_compile_topic($1)`, bad)
		if !errors.Is(wrapErr(err), ErrInvalid) {
			t.Errorf("pattern %q: err = %v, want ErrInvalid", bad, err)
		}
	}
}

// the condition compiler: match semantics, per-column disassembly and
// rejected grammar
func TestCompileCondition(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	// evaluate a compiled condition against probe headers and payload the
	// way the read paths do: in WHERE, an unknown result (e.g. a
	// type-mismatched comparison makes @@ return NULL) drops the row, so
	// coalesce to false here
	eval := func(condition, headers, payload string) bool {
		t.Helper()
		var got bool
		if err := pool.QueryRow(ctx, `
			SELECT coalesce(
				(c.headers_condition IS NULL OR $2::jsonb @@ c.headers_condition)
			AND (c.payload_condition IS NULL OR $3::jsonb @@ c.payload_condition), false)
			FROM _cb_stream_compile_condition($1) c`,
			condition, headers, payload).Scan(&got); err != nil {
			t.Fatalf("%q: %v", condition, err)
		}
		return got
	}

	cases := []struct {
		condition, headers, payload string
		want                        bool
	}{
		// exists tests presence, not non-nullness
		{`exists($.payload.made_public_at)`, `{}`, `{"made_public_at": "2026-01-01"}`, true},
		{`exists($.payload.made_public_at)`, `{}`, `{"made_public_at": null}`, true},
		{`exists($.payload.made_public_at)`, `{}`, `{}`, false},
		// equality is strict across JSON types, numeric within number
		{`$.payload.type == "work"`, `{}`, `{"type": "work"}`, true},
		{`$.payload.type == "work"`, `{}`, `{"type": "Work"}`, false},
		{`$.headers.attempt == 3`, `{"attempt": 3}`, `{}`, true},
		{`$.headers.attempt == 3`, `{"attempt": 3.0}`, `{}`, true},
		{`$.headers.attempt == 3`, `{"attempt": "3"}`, `{}`, false},
		{`$.payload.public == true`, `{}`, `{"public": true}`, true},
		{`$.payload.public == true`, `{}`, `{"public": "true"}`, false},
		// lax array unwrapping: equality holds if any element equals
		{`$.payload.tags == "urgent"`, `{}`, `{"tags": ["urgent", "review"]}`, true},
		{`$.payload.tags == "urgent"`, `{}`, `{"tags": ["later"]}`, false},
		{`$.payload.type == "work"`, `{}`, `{"type": ["work"]}`, true},
		// conjuncts mix namespaces and all must hold
		{`$.payload.access.status == "open" && exists($.headers.trace_id)`,
			`{"trace_id": "t1"}`, `{"access": {"status": "open"}}`, true},
		{`$.payload.access.status == "open" && exists($.headers.trace_id)`,
			`{}`, `{"access": {"status": "open"}}`, false},
	}
	for _, c := range cases {
		if got := eval(c.condition, c.headers, c.payload); got != c.want {
			t.Errorf("%q on headers=%s payload=%s = %v, want %v",
				c.condition, c.headers, c.payload, got, c.want)
		}
	}

	// conjuncts land in their own columns; absent namespaces stay NULL
	var headersNull, payloadNull bool
	if err := pool.QueryRow(ctx, `
		SELECT c.headers_condition IS NULL, c.payload_condition IS NULL
		FROM _cb_stream_compile_condition('exists($.payload.a)') c`).Scan(&headersNull, &payloadNull); err != nil {
		t.Fatal(err)
	}
	if !headersNull || payloadNull {
		t.Fatalf("payload-only condition: headers IS NULL = %v, payload IS NULL = %v",
			headersNull, payloadNull)
	}

	for _, bad := range []string{
		"",
		`$.payload.a`,     // bare path, no operator
		`$.payload.n > 5`, // comparisons are deferred
		`$.payload.a != 1`,
		`exists($.payload.a) || exists($.payload.b)`,
		`not exists($.payload.a)`,
		`$.payload.a == null`,             // exists covers presence
		`$.payload.items[0] == "x"`,       // arrays are deferred
		`$.payload."@type" == "Announce"`, // quoted segments are deferred
		`$.topic == "a.b"`,                // topic is its own parameter
		`$.payload.msg == "a && b"`,       // && in a string fails loud
		`payload.a == 1`,                  // missing $.
		`$.body.a == 1`,                   // unknown namespace
		`exists($.payload.a) && && exists($.payload.b)`,
	} {
		_, err := pool.Exec(ctx, `SELECT * FROM _cb_stream_compile_condition($1)`, bad)
		if !errors.Is(wrapErr(err), ErrInvalid) {
			t.Errorf("condition %q: err = %v, want ErrInvalid", bad, err)
		}
	}
}

// cursors with a topic pattern and a condition: matches only, NULL topics
// excluded, scanned-range advancement, unfiltered independence
func TestFilteredCursor(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_fcur"); err != nil {
		t.Fatal(err)
	}
	if _, err := PublishMessages(ctx, pool, "go_fcur", []BatchMessage{
		{Topic: "a.b", Payload: map[string]any{"flag": true}},
		{Topic: "a.c", Payload: map[string]any{}},
		{Topic: "b.d", Payload: map[string]any{"flag": false}},
		{Payload: map[string]any{"flag": true}}, // no topic
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_fcur')"); err != nil {
		t.Fatal(err)
	}

	readTopics := func(cursor string, opts CursorOpts) []string {
		t.Helper()
		if err := EnsureCursor(ctx, pool, "go_fcur", cursor, opts); err != nil {
			t.Fatal(err)
		}
		msgs, err := Read(ctx, pool, "go_fcur", cursor, 100)
		if err != nil {
			t.Fatal(err)
		}
		topics := make([]string, len(msgs))
		for i, m := range msgs {
			topics[i] = m.Topic
		}
		return topics
	}

	// topic pattern: matches only, and a NULL topic never matches
	if got := readTopics("bytopic", CursorOpts{StartPos: At(0), Topic: "a.#"}); !slices.Equal(got, []string{"a.b", "a.c"}) {
		t.Fatalf("topic-filtered read = %v", got)
	}
	// condition: the event payload decides
	if got := readTopics("bycond", CursorOpts{StartPos: At(0), Condition: `$.payload.flag == true`}); !slices.Equal(got, []string{"a.b", ""}) {
		t.Fatalf("condition-filtered read = %v", got)
	}
	// topic and condition AND together
	if got := readTopics("byboth", CursorOpts{StartPos: At(0), Topic: "a.#", Condition: `$.payload.flag == true`}); !slices.Equal(got, []string{"a.b"}) {
		t.Fatalf("combined read = %v", got)
	}
	// an unfiltered cursor on the same stream sees everything
	if got := readTopics("plain", CursorOpts{StartPos: At(0)}); len(got) != 4 {
		t.Fatalf("unfiltered read = %v", got)
	}

	// a zero-match read still advances over the whole scanned range
	if got := readTopics("bynone", CursorOpts{StartPos: At(0), Topic: "z.#"}); len(got) != 0 {
		t.Fatalf("zero-match read = %v", got)
	}
	var pos, lastPos int64
	if err := pool.QueryRow(ctx, `SELECT c.pos, s.last_pos FROM cb_stream_cursors c
		JOIN cb_streams s ON s.name = c.stream
		WHERE c.stream = 'go_fcur' AND c.name = 'bynone'`).Scan(&pos, &lastPos); err != nil {
		t.Fatal(err)
	}
	if pos != lastPos {
		t.Fatalf("cursor pos = %d, want %d: a zero-match read must advance", pos, lastPos)
	}
}

// subscriptions with a topic pattern: late binding replays history, claims cover
// non-matches, retries keep the filter, and crash recording leaks nothing
func TestFilteredSubscription(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_fqt"); err != nil {
		t.Fatal(err)
	}
	// history exists before the subscription: binding late replays it
	if _, err := PublishMessages(ctx, pool, "go_fqt", []BatchMessage{
		{Topic: "record.work.created", Payload: 1},
		{Topic: "record.person.created", Payload: 2},
		{Topic: "record.work.updated", Payload: 3},
		{Topic: "record.person.updated", Payload: 4},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "SELECT _cb_stream_assign_positions('go_fqt')"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_fqt", "orcid", SubscriptionOpts{
		StartPos:    At(0),
		Topic:       "record.work.#",
		BackoffKind: BackoffNone, // retries land immediately, visible below
	}); err != nil {
		t.Fatal(err)
	}

	from, to, ok := claimRange(t, ctx, pool, "go_fqt", "orcid", "w1")
	if !ok {
		t.Fatal("nothing to claim, want the replayed history")
	}
	type claimed struct {
		Pos   int64
		Topic string
	}
	rows, err := pool.Query(ctx, `SELECT m.pos, coalesce(m.topic, '')
		FROM cb_stream_read_claim('go_fqt', 'orcid', $1, $2) m`, from, to)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[claimed])
	if err != nil {
		t.Fatal(err)
	}
	topics := make([]string, len(got))
	for i, m := range got {
		topics[i] = m.Topic
	}
	if want := []string{"record.work.created", "record.work.updated"}; !slices.Equal(topics, want) {
		t.Fatalf("claimed messages = %v, want %v", topics, want)
	}

	// a failed match becomes a retry row carrying its topic intact
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_fail('go_fqt', 'orcid', 'w1', $1, 'boom')`, got[0].Pos); err != nil {
		t.Fatal(err)
	}
	var retryTopic string
	if err := pool.QueryRow(ctx, `SELECT topic FROM cb_stream_retries
		WHERE stream = 'go_fqt' AND subscription = 'orcid' AND origin_pos = $1`, got[0].Pos).Scan(&retryTopic); err != nil {
		t.Fatal(err)
	}
	if retryTopic != "record.work.created" {
		t.Fatalf("retry topic = %q", retryTopic)
	}

	// closing advances over the non-matching positions too
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_close_claim('go_fqt', 'orcid', 'w1', $1)`, from); err != nil {
		t.Fatal(err)
	}
	var closedPos, lastPos int64
	if err := pool.QueryRow(ctx, `SELECT q.closed_pos, s.last_pos
		FROM cb_stream_subscriptions q JOIN cb_streams s ON s.name = q.stream
		WHERE q.stream = 'go_fqt' AND q.name = 'orcid'`).Scan(&closedPos, &lastPos); err != nil {
		t.Fatal(err)
	}
	if closedPos != lastPos {
		t.Fatalf("closed_pos = %d, want %d", closedPos, lastPos)
	}

	// a crash materializes retry rows only for the subscription's own
	// messages: crash a claim whose range holds matches and non-matches, then
	// count which become rows
	if err := EnsureSubscription(ctx, pool, "go_fqt", "q2", SubscriptionOpts{
		StartPos:    At(0),
		Topic:       "record.work.#",
		BackoffKind: BackoffNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE cb_stream_subscriptions
		SET claim_ttl = interval '30 milliseconds'
		WHERE stream = 'go_fqt' AND name = 'q2'`); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := claimRange(t, ctx, pool, "go_fqt", "q2", "c1"); !ok {
		t.Fatal("nothing to claim")
	}
	time.Sleep(80 * time.Millisecond) // expire: the crashed range is recorded as retries
	if _, _, ok := claimRange(t, ctx, pool, "go_fqt", "q2", "c2"); ok {
		t.Fatal("claim handed out again, want the crashed range recorded as retries")
	}
	rows, err = pool.Query(ctx, `SELECT coalesce(topic, '') FROM cb_stream_retries
		WHERE stream = 'go_fqt' AND subscription = 'q2' ORDER BY origin_pos`)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"record.work.created", "record.work.updated"}; !slices.Equal(retried, want) {
		t.Fatalf("retried = %v, want %v: non-matches must never become rows", retried, want)
	}
}

// a live worker on a filtered subscription: the loop delivers exactly the matching
// messages, keeps up over the positions it never delivers, and a failed
// match comes back through the retry stream into the same loop
func TestConsumeSubscriptionFiltered(t *testing.T) {
	pool := setupTest(t)
	ctx := t.Context()

	if err := Ensure(ctx, pool, "go_cqf"); err != nil {
		t.Fatal(err)
	}
	if err := EnsureSubscription(ctx, pool, "go_cqf", "f", SubscriptionOpts{
		StartPos:    At(0),
		Topic:       "job.#",
		Condition:   `$.payload.run == true`,
		BackoffKind: BackoffNone,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := PublishMessages(ctx, pool, "go_cqf", []BatchMessage{
		{Topic: "job.a", Payload: map[string]any{"run": true, "n": 1}},
		{Topic: "job.b", Payload: map[string]any{"run": false, "n": 2}},   // condition drops it
		{Topic: "other.x", Payload: map[string]any{"run": true, "n": 3}},  // topic drops it
		{Topic: "job.fail", Payload: map[string]any{"run": true, "n": 4}}, // fails once
		{Topic: "job.c", Payload: map[string]any{"run": true, "n": 5}},
		{Payload: map[string]any{"run": true, "n": 6}}, // no topic: never matches
	}); err != nil {
		t.Fatal(err)
	}

	jctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ticksDone := make(chan error, 1)
	go func() {
		ticksDone <- StartTicker(jctx, pool, TickerOpts{
			AssignPositionsInterval: 20 * time.Millisecond,
			DeliverInterval:         20 * time.Millisecond,
		})
	}()

	var mu sync.Mutex
	deliveries := map[int]int{}
	subDone := make(chan error, 1)
	go func() {
		subDone <- ConsumeSubscription(jctx, pool, "go_cqf", "f", func(_ context.Context, m Message) error {
			var p struct {
				N int `json:"n"`
			}
			if err := json.Unmarshal(m.Payload, &p); err != nil {
				return err
			}
			mu.Lock()
			deliveries[p.N]++
			seen := deliveries[p.N]
			mu.Unlock()
			if p.N == 4 && seen == 1 {
				return errors.New("first try fails")
			}
			return nil
		}, ConsumeSubscriptionOpts{PollInterval: 20 * time.Millisecond})
	}()

	// done when the base subscription's closed position reaches the stream's tail
	// and the failed match has come back around as a retry row
	deadline := time.Now().Add(10 * time.Second)
	for {
		var caughtUp bool
		if err := pool.QueryRow(ctx, `SELECT q.closed_pos = s.last_pos
			FROM cb_stream_subscriptions q JOIN cb_streams s ON s.name = q.stream
			WHERE q.stream = 'go_cqf' AND q.name = 'f'`).Scan(&caughtUp); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		retried := deliveries[4] == 2
		snapshot := maps.Clone(deliveries)
		mu.Unlock()
		if caughtUp && retried {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker never caught up: deliveries = %v, caught up = %v", snapshot, caughtUp)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-subDone
	<-ticksDone

	mu.Lock()
	defer mu.Unlock()
	if want := map[int]int{1: 1, 4: 2, 5: 1}; !maps.Equal(deliveries, want) {
		t.Fatalf("deliveries = %v, want %v: the loop must deliver exactly the matches", deliveries, want)
	}
}
