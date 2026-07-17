package jobs

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// requireSlowTests skips the test unless CB_SLOW_TESTS is enabled — the
// same gate the root suite uses.
func requireSlowTests(t *testing.T) {
	t.Helper()
	v := strings.TrimSpace(strings.ToLower(os.Getenv("CB_SLOW_TESTS")))
	if v == "1" || v == "true" || v == "yes" {
		return
	}
	t.Skip("slow test skipped; set CB_SLOW_TESTS=1 to include")
}

// TestWideMapStress is the wide-map M4b exit item: hundreds of sibling
// steps completing into one run row. Every completion takes the run-row
// lock first, so siblings serialize there — the connection-occupancy
// watch item (D30). The test proves the steps_remaining accounting stays
// exact under that contention — the barrier fires exactly once, after all
// siblings — and logs the completion rate, which is what decides whether
// the one-call-one-transaction shape needs the mitigation ladder's next
// rung.
func TestWideMapStress(t *testing.T) {
	requireSlowTests(t)
	setupTest(t)
	ctx := t.Context()

	const siblings = 400
	const workers = 4

	// its own pool: each worker's notifier takes a connection out of the
	// pool for LISTEN, and four workers plus their queries would starve
	// the shared one
	cfg, err := pgxpool.ParseConfig(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close) // registered before the workers, so they stop first

	if err := DefineQueue(ctx, pool, "go_wms_q", QueueOpts{ClaimBatchSize: 25}); err != nil {
		t.Fatal(err)
	}
	for _, job := range []string{"go_wms_split", "go_wms_work", "go_wms_join"} {
		if err := Define(ctx, pool, job, JobOpts{Queue: "go_wms_q"}); err != nil {
			t.Fatal(err)
		}
	}

	for range workers {
		w := NewWorker(pool)
		w.Handle("go_wms_split", func(ctx context.Context, p *Plan, in struct{}) error {
			for i := range siblings {
				p.Step("go_wms_work", i)
			}
			p.After().Step("go_wms_join", nil)
			return nil
		})
		w.Handle("go_wms_work", func(ctx context.Context, in int) (int, error) {
			return in, nil
		})
		w.Handle("go_wms_join", func(ctx context.Context, p *Plan, in struct{}) error {
			outs, err := StepOutputs[int](p, "go_wms_work")
			if err != nil {
				return err
			}
			sum := 0
			for _, n := range outs {
				sum += n
			}
			p.SetRunOutput(map[string]int{"count": len(outs), "sum": sum})
			return nil
		})
		startTestWorker(t, w)
	}

	started := time.Now()
	id, _, err := Run(ctx, pool, "go_wms_split", nil)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Count int `json:"count"`
		Sum   int `json:"sum"`
	}
	if err := WaitForOutput(ctx, pool, id, &out, fastWait); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)

	if out.Count != siblings || out.Sum != siblings*(siblings-1)/2 {
		t.Fatalf("join saw %d siblings with sum %d", out.Count, out.Sum)
	}

	// exact accounting: every step completed, every start spent exactly
	// once — a retry or a false crash under contention would show up here
	var steps, notCompleted, attempts int
	if err := pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE status <> 'completed')
		 FROM cb_job_steps WHERE run_id = $1`, id).Scan(&steps, &notCompleted); err != nil {
		t.Fatal(err)
	}
	if steps != siblings+2 || notCompleted != 0 {
		t.Fatalf("steps = %d (%d not completed)", steps, notCompleted)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM cb_job_attempts WHERE run_id = $1`, id).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != siblings+2 {
		t.Fatalf("attempts = %d, want %d (retries under contention)", attempts, siblings+2)
	}

	// the churn note (D34): completions leave dead tuples on cb_job_steps
	// for autovacuum; the count is informational, stats update lazily
	var deadTuples int64
	_ = pool.QueryRow(ctx,
		`SELECT coalesce(n_dead_tup, 0) FROM pg_stat_user_tables
		 WHERE relname = 'cb_job_steps'`).Scan(&deadTuples)

	t.Logf("%d siblings into one run in %s — %.0f completions/s across %d workers; cb_job_steps dead tuples after: %d",
		siblings, elapsed.Round(time.Millisecond),
		float64(siblings)/elapsed.Seconds(), workers, deadTuples)
}
