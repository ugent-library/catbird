package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird/streams"
)

var (
	triggerOnce sync.Once
	triggerErr  error
)

// setupTriggerTest installs the stream schema beside the job schema —
// triggers are the one feature that needs both — and wipes this file's
// leftovers. Production jobs code never imports streams; this test file
// does, to run the stream migrations.
//
// Streams and triggers here are named gj_, not go_: the streams suite runs
// in parallel under go test ./... and wipes and reuses go_ streams, so the
// two packages must not share stream names.
func setupTriggerTest(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool := setupTest(t)
	triggerOnce.Do(func() {
		db, err := sql.Open("pgx", testDSN)
		if err != nil {
			triggerErr = err
			return
		}
		defer db.Close()
		if err := streams.MigrateUpTo(context.Background(), db, streams.SchemaVersion); err != nil {
			triggerErr = err
			return
		}
		for _, q := range []string{
			`DELETE FROM cb_triggers WHERE name LIKE 'gj_%'`,
			`DELETE FROM cb_streams WHERE name LIKE 'gj_%'`, // cursors cascade
		} {
			if _, err := db.Exec(q); err != nil {
				triggerErr = err
				return
			}
		}
	})
	if triggerErr != nil {
		t.Fatal(triggerErr)
	}
	return pool
}

func ensureStream(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stream string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `SELECT cb_stream_ensure($1)`, stream); err != nil {
		t.Fatal(err)
	}
}

// publish appends one message; positions are assigned separately with
// assignPositions, the way the stream ticker would.
func publish(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stream, topic, payload string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`SELECT cb_stream_publish($1, $2, $3)`, stream, topic, payload); err != nil {
		t.Fatal(err)
	}
}

func assignPositions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stream string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`SELECT _cb_stream_assign_positions($1)`, stream); err != nil {
		t.Fatal(err)
	}
}

// deliverTrigger calls the delivery function for one trigger and returns
// how many messages it delivered.
func deliverTrigger(t *testing.T, ctx context.Context, pool *pgxpool.Pool, trigger string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT _cb_job_run_triggered($1)`, trigger).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func cursorPos(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stream, cursor string) int64 {
	t.Helper()
	var pos int64
	if err := pool.QueryRow(ctx,
		`SELECT pos FROM cb_stream_cursors WHERE stream = $1 AND name = $2`,
		stream, cursor).Scan(&pos); err != nil {
		t.Fatal(err)
	}
	return pos
}

func cursorCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, stream, cursor string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_stream_cursors WHERE stream = $1 AND name = $2`,
		stream, cursor).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func runCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, job string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_job_runs WHERE job = $1`, job).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// rowVersion reads a row's xmin, which changes exactly when the row is
// written — the no-op assertions compare it across redeclares.
func rowVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) string {
	t.Helper()
	var v string
	if err := pool.QueryRow(ctx, query, args...).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestDefineTrigger(t *testing.T) {
	pool := setupTriggerTest(t)
	ctx := t.Context()

	ensureStream(t, ctx, pool, "gj_s1")
	ensureStream(t, ctx, pool, "gj_s2")
	if err := Define(ctx, pool, "go_tg_job"); err != nil {
		t.Fatal(err)
	}

	// Refusals: a broken trigger is refused at define, and the failed
	// statement leaves no trigger row behind.
	for _, bad := range []struct {
		msg  string
		err  error
		call func() error
	}{
		{"invalid name", ErrInvalid, func() error {
			return DefineTrigger(ctx, pool, "gj bad", "gj_s1", "go_tg_job")
		}},
		{"undefined job", ErrNotDefined, func() error {
			return DefineTrigger(ctx, pool, "gj_t1", "gj_s1", "go_missing")
		}},
		{"undefined stream", ErrNotDefined, func() error {
			return DefineTrigger(ctx, pool, "gj_t1", "gj_missing", "go_tg_job")
		}},
		{"invalid topic", ErrInvalid, func() error {
			return DefineTrigger(ctx, pool, "gj_t1", "gj_s1", "go_tg_job",
				TriggerOpts{Topic: "a..b"})
		}},
		{"invalid condition", ErrInvalid, func() error {
			return DefineTrigger(ctx, pool, "gj_t1", "gj_s1", "go_tg_job",
				TriggerOpts{Condition: "bogus"})
		}},
	} {
		if err := bad.call(); !errors.Is(err, bad.err) {
			t.Fatalf("%s: got %v, want %v", bad.msg, err, bad.err)
		}
	}
	var leaked int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_triggers WHERE name = 'gj_t1'`).Scan(&leaked); err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Fatalf("refused define left %d trigger rows", leaked)
	}

	// Declare whole: trigger row plus its cursor bearing the filter.
	if err := DefineTrigger(ctx, pool, "gj_t1", "gj_s1", "go_tg_job",
		TriggerOpts{Topic: "order.created", StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	var topic string
	var pos int64
	if err := pool.QueryRow(ctx,
		`SELECT pos, topic FROM cb_stream_cursors WHERE stream = 'gj_s1' AND name = 'gj_t1'`).
		Scan(&pos, &topic); err != nil {
		t.Fatal(err)
	}
	if pos != 0 || topic != "order.created" {
		t.Fatalf("cursor = (pos %d, topic %s)", pos, topic)
	}

	// An identical redeclaration writes nothing to either table.
	triggerV := rowVersion(t, ctx, pool, `SELECT xmin::text FROM cb_triggers WHERE name = 'gj_t1'`)
	cursorV := rowVersion(t, ctx, pool,
		`SELECT xmin::text FROM cb_stream_cursors WHERE stream = 'gj_s1' AND name = 'gj_t1'`)
	if err := DefineTrigger(ctx, pool, "gj_t1", "gj_s1", "go_tg_job",
		TriggerOpts{Topic: "order.created"}); err != nil {
		t.Fatal(err)
	}
	if v := rowVersion(t, ctx, pool, `SELECT xmin::text FROM cb_triggers WHERE name = 'gj_t1'`); v != triggerV {
		t.Fatal("identical redeclare wrote the trigger row")
	}
	if v := rowVersion(t, ctx, pool,
		`SELECT xmin::text FROM cb_stream_cursors WHERE stream = 'gj_s1' AND name = 'gj_t1'`); v != cursorV {
		t.Fatal("identical redeclare wrote the cursor row")
	}

	// A filter change lands on the cursor only and keeps its position.
	if err := DefineTrigger(ctx, pool, "gj_t1", "gj_s1", "go_tg_job",
		TriggerOpts{Topic: "order.#"}); err != nil {
		t.Fatal(err)
	}
	if v := rowVersion(t, ctx, pool, `SELECT xmin::text FROM cb_triggers WHERE name = 'gj_t1'`); v != triggerV {
		t.Fatal("filter change wrote the trigger row")
	}
	if err := pool.QueryRow(ctx,
		`SELECT pos, topic FROM cb_stream_cursors WHERE stream = 'gj_s1' AND name = 'gj_t1'`).
		Scan(&pos, &topic); err != nil {
		t.Fatal(err)
	}
	if pos != 0 || topic != "order.#" {
		t.Fatalf("after filter change: cursor = (pos %d, topic %s)", pos, topic)
	}

	// start_pos repositions an existing trigger.
	if err := DefineTrigger(ctx, pool, "gj_t1", "gj_s1", "go_tg_job",
		TriggerOpts{Topic: "order.#", StartPos: At(7)}); err != nil {
		t.Fatal(err)
	}
	if pos = cursorPos(t, ctx, pool, "gj_s1", "gj_t1"); pos != 7 {
		t.Fatalf("start_pos poke: pos = %d, want 7", pos)
	}

	// A trigger moved to another stream leaves no cursor behind.
	if err := DefineTrigger(ctx, pool, "gj_t1", "gj_s2", "go_tg_job",
		TriggerOpts{Topic: "order.#"}); err != nil {
		t.Fatal(err)
	}
	if n := cursorCount(t, ctx, pool, "gj_s1", "gj_t1"); n != 0 {
		t.Fatal("old stream kept the cursor after the move")
	}
	if n := cursorCount(t, ctx, pool, "gj_s2", "gj_t1"); n != 1 {
		t.Fatal("new stream did not get a cursor")
	}

	// Delete removes the trigger and its cursor; deleting again is a no-op.
	deleted, err := DeleteTrigger(ctx, pool, "gj_t1")
	if err != nil || !deleted {
		t.Fatalf("delete = (%v, %v), want (true, nil)", deleted, err)
	}
	if n := cursorCount(t, ctx, pool, "gj_s2", "gj_t1"); n != 0 {
		t.Fatal("delete left the cursor behind")
	}
	deleted, err = DeleteTrigger(ctx, pool, "gj_t1")
	if err != nil || deleted {
		t.Fatalf("second delete = (%v, %v), want (false, nil)", deleted, err)
	}
}

func TestTriggerDelivery(t *testing.T) {
	pool := setupTriggerTest(t)
	ctx := t.Context()

	ensureStream(t, ctx, pool, "gj_orders")
	if err := DefineQueue(ctx, pool, "go_tgq"); err != nil {
		t.Fatal(err)
	}
	if err := Define(ctx, pool, "go_tg_confirm", JobOpts{Queue: "go_tgq"}); err != nil {
		t.Fatal(err)
	}
	// Defined before anything is published, so the tail start covers it all.
	if err := DefineTrigger(ctx, pool, "gj_conf", "gj_orders", "go_tg_confirm",
		TriggerOpts{Topic: "order.created"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = DeleteTrigger(context.Background(), pool, "gj_conf") })

	// The outbox shape: the application publishes in its own transaction.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range []struct{ topic, payload string }{
		{"order.created", `{"n": 1}`},
		{"order.deleted", `{"n": -1}`},
		{"order.created", `{"n": 2}`},
		{"invoice.created", `{"n": -1}`},
		{"order.created", `{"n": 3}`},
	} {
		if _, err := tx.Exec(ctx, `SELECT cb_stream_publish($1, $2, $3)`,
			"gj_orders", m.topic, m.payload); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	assignPositions(t, ctx, pool, "gj_orders")

	// The Go tick loop delivers: one run per matching message.
	n, err := runTriggered(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("delivered %d messages, want 3", n)
	}

	// The run's input is the payload exactly as published; the key is the
	// trigger's name and the message's position.
	rows, err := pool.Query(ctx,
		`SELECT id, key, input FROM cb_job_runs WHERE job = 'go_tg_confirm' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var runIDs []int64
	var keys []string
	sum := 0
	for rows.Next() {
		var id int64
		var key string
		var input json.RawMessage
		if err := rows.Scan(&id, &key, &input); err != nil {
			t.Fatal(err)
		}
		var in struct{ N int }
		if err := json.Unmarshal(input, &in); err != nil {
			t.Fatal(err)
		}
		runIDs = append(runIDs, id)
		keys = append(keys, key)
		sum += in.N
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(runIDs) != 3 || sum != 6 {
		t.Fatalf("runs = %d with payload sum %d, want 3 and 6", len(runIDs), sum)
	}
	if keys[0] != "gj_conf:1" || keys[1] != "gj_conf:3" || keys[2] != "gj_conf:5" {
		t.Fatalf("run keys = %v", keys)
	}
	// The cursor advanced over the non-matching messages too.
	if pos := cursorPos(t, ctx, pool, "gj_orders", "gj_conf"); pos != 5 {
		t.Fatalf("cursor pos = %d, want 5", pos)
	}

	// A worker executes the runs; the handler takes the payload's own shape.
	w := NewWorker(pool)
	w.Handle("go_tg_confirm", func(ctx context.Context, in struct{ N int }) (struct{}, error) {
		return struct{}{}, nil
	})
	startTestWorker(t, w)
	for _, id := range runIDs {
		if err := WaitForOutput(ctx, pool, id, nil, fastWait); err != nil {
			t.Fatalf("run %d: %v", id, err)
		}
	}
}

func TestTriggerExactlyOnce(t *testing.T) {
	pool := setupTriggerTest(t)
	ctx := t.Context()

	ensureStream(t, ctx, pool, "gj_once")
	if err := Define(ctx, pool, "go_tg_once_job"); err != nil {
		t.Fatal(err)
	}
	if err := DefineTrigger(ctx, pool, "gj_once_t", "gj_once", "go_tg_once_job"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = DeleteTrigger(context.Background(), pool, "gj_once_t") })

	publish(t, ctx, pool, "gj_once", "e.v", `{"i": 1}`)
	publish(t, ctx, pool, "gj_once", "e.v", `{"i": 2}`)
	assignPositions(t, ctx, pool, "gj_once")

	// A batch that rolls back leaves nothing: no runs, cursor unmoved.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRow(ctx,
		`SELECT _cb_job_run_triggered('gj_once_t')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("delivered %d in the doomed batch, want 2", n)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := runCount(t, ctx, pool, "go_tg_once_job"); n != 0 {
		t.Fatalf("rolled-back batch left %d runs", n)
	}
	if pos := cursorPos(t, ctx, pool, "gj_once", "gj_once_t"); pos != 0 {
		t.Fatalf("rolled-back batch moved the cursor to %d", pos)
	}

	// Committed: delivered once, then nothing more to read.
	if n := deliverTrigger(t, ctx, pool, "gj_once_t"); n != 2 {
		t.Fatalf("delivered %d, want 2", n)
	}
	if n := deliverTrigger(t, ctx, pool, "gj_once_t"); n != 0 {
		t.Fatalf("second delivery read %d, want 0", n)
	}
	if n := runCount(t, ctx, pool, "go_tg_once_job"); n != 2 {
		t.Fatalf("runs = %d, want 2", n)
	}

	// A cursor reset replays the messages; the run keys make the replay
	// idempotent.
	if _, err := pool.Exec(ctx,
		`UPDATE cb_stream_cursors SET pos = 0 WHERE stream = 'gj_once' AND name = 'gj_once_t'`); err != nil {
		t.Fatal(err)
	}
	if n := deliverTrigger(t, ctx, pool, "gj_once_t"); n != 2 {
		t.Fatalf("replay delivered %d, want 2", n)
	}
	if n := runCount(t, ctx, pool, "go_tg_once_job"); n != 2 {
		t.Fatalf("replay created runs: %d, want 2", n)
	}
}

func TestTriggerStall(t *testing.T) {
	pool := setupTriggerTest(t)
	ctx := t.Context()

	ensureStream(t, ctx, pool, "gj_stall")
	if err := Define(ctx, pool, "go_tg_gone"); err != nil {
		t.Fatal(err)
	}
	if err := DefineTrigger(ctx, pool, "gj_stall_t", "gj_stall", "go_tg_gone"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = DeleteTrigger(context.Background(), pool, "gj_stall_t") })

	publish(t, ctx, pool, "gj_stall", "e.v", `{"i": 1}`)
	publish(t, ctx, pool, "gj_stall", "e.v", `{"i": 2}`)
	assignPositions(t, ctx, pool, "gj_stall")

	// The job's row is deleted out from under the trigger (raw SQL — there
	// is no delete API). Delivery stalls loudly at the cursor.
	if _, err := pool.Exec(ctx, `DELETE FROM cb_jobs WHERE name = 'go_tg_gone'`); err != nil {
		t.Fatal(err)
	}
	n, err := runTriggered(ctx, pool)
	if !errors.Is(err, ErrNotDefined) {
		t.Fatalf("stalled tick error = %v, want ErrNotDefined", err)
	}
	if n != 0 {
		t.Fatalf("stalled tick delivered %d, want 0", n)
	}
	if n := runCount(t, ctx, pool, "go_tg_gone"); n != 0 {
		t.Fatalf("stalled trigger created %d runs", n)
	}
	if pos := cursorPos(t, ctx, pool, "gj_stall", "gj_stall_t"); pos != 0 {
		t.Fatalf("stalled trigger moved the cursor to %d", pos)
	}

	// Defining the job again resumes delivery; the backlog lands whole.
	if err := Define(ctx, pool, "go_tg_gone"); err != nil {
		t.Fatal(err)
	}
	n, err = runTriggered(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("resumed delivery = %d, want 2", n)
	}
	if n := runCount(t, ctx, pool, "go_tg_gone"); n != 2 {
		t.Fatalf("runs after resume = %d, want 2", n)
	}
}

func TestTriggerStartPos(t *testing.T) {
	pool := setupTriggerTest(t)
	ctx := t.Context()

	ensureStream(t, ctx, pool, "gj_pos")
	publish(t, ctx, pool, "gj_pos", "e.v", `{"i": 1}`)
	publish(t, ctx, pool, "gj_pos", "e.v", `{"i": 2}`)
	publish(t, ctx, pool, "gj_pos", "e.v", `{"i": 3}`)
	assignPositions(t, ctx, pool, "gj_pos")
	if err := Define(ctx, pool, "go_tg_pos_job"); err != nil {
		t.Fatal(err)
	}

	// The default start is the tail: history is skipped.
	if err := DefineTrigger(ctx, pool, "gj_tail", "gj_pos", "go_tg_pos_job"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = DeleteTrigger(context.Background(), pool, "gj_tail") })
	if n := deliverTrigger(t, ctx, pool, "gj_tail"); n != 0 {
		t.Fatalf("tail trigger delivered %d, want 0", n)
	}

	// At(0) repositions to the beginning and the history delivers.
	if err := DefineTrigger(ctx, pool, "gj_tail", "gj_pos", "go_tg_pos_job",
		TriggerOpts{StartPos: At(0)}); err != nil {
		t.Fatal(err)
	}
	if n := deliverTrigger(t, ctx, pool, "gj_tail"); n != 3 {
		t.Fatalf("from the beginning: delivered %d, want 3", n)
	}

	// At(2) skips everything at or below position 2.
	if err := DefineTrigger(ctx, pool, "gj_from2", "gj_pos", "go_tg_pos_job",
		TriggerOpts{StartPos: At(2)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = DeleteTrigger(context.Background(), pool, "gj_from2") })
	if n := deliverTrigger(t, ctx, pool, "gj_from2"); n != 1 {
		t.Fatalf("from position 2: delivered %d, want 1", n)
	}
	var fromKey string
	if err := pool.QueryRow(ctx,
		`SELECT key FROM cb_job_runs WHERE job = 'go_tg_pos_job' AND key LIKE 'gj_from2:%'`).
		Scan(&fromKey); err != nil {
		t.Fatal(err)
	}
	if fromKey != "gj_from2:3" {
		t.Fatalf("run key = %s, want gj_from2:3", fromKey)
	}
}

func TestJobsWithoutStreams(t *testing.T) {
	ctx := t.Context()

	// The job schema installs and runs in a database that has never seen
	// the stream schema; only the trigger feature refuses, loudly.
	admin, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `DROP DATABASE IF EXISTS cb_tst_bare WITH (FORCE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE cb_tst_bare`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(),
			`DROP DATABASE IF EXISTS cb_tst_bare WITH (FORCE)`)
	})

	bareDSN := strings.Replace(testDSN, "/cb_tst?", "/cb_tst_bare?", 1)
	db, err := sql.Open("pgx", bareDSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := MigrateUpTo(ctx, db, SchemaVersion); err != nil {
		db.Close()
		t.Fatalf("migrate up without streams: %v", err)
	}
	db.Close()

	pool, err := pgxpool.New(ctx, bareDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// The rest of the engine works: declare and run a job.
	if err := Define(ctx, pool, "go_bare_job"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Run(ctx, pool, "go_bare_job", nil); err != nil {
		t.Fatal(err)
	}

	// Defining a trigger names the missing module.
	err = DefineTrigger(ctx, pool, "gj_bare", "some_stream", "go_bare_job")
	if !errors.Is(err, ErrStreamsRequired) {
		t.Fatalf("define without streams = %v, want ErrStreamsRequired", err)
	}

	// The trigger tick is quiet when no triggers exist.
	n, err := runTriggered(ctx, pool)
	if err != nil || n != 0 {
		t.Fatalf("tick without streams = (%d, %v), want (0, nil)", n, err)
	}
}
