package jobs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The M4a exit benchmarks. BenchmarkRunThroughput measures run-to-completion
// latency through a live worker (notify + claim, no tick in the path):
// ns/op is the latency. The pipelined and engine benchmarks measure
// completions per second against the old engine's BenchmarkTaskThroughput*
// envelope; the engine pair decouples the engine from the submitter, which
// is what lets a fleet of workers show up in the numbers.

const benchmarkPipelineBatchSize = 64

// setupBenchmark defines a dedicated pool and job, starts workers for it,
// and runs one warm-up run so the timer never sees worker startup.
func setupBenchmark(b *testing.B, workers int) {
	b.Helper()
	pool := setupTest(b)
	ctx := context.Background()

	if err := DefineQueue(ctx, pool, "go_bench_q", QueueOpts{ClaimBatchSize: 256}); err != nil {
		b.Fatal(err)
	}
	if err := Define(ctx, pool, "go_bench", JobOpts{Queue: "go_bench_q"}); err != nil {
		b.Fatal(err)
	}

	// One pool per worker, like the separate processes of a real fleet —
	// shared, they starve each other of connections.
	for range workers {
		wpool, err := pgxpool.New(ctx, testDSN)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(wpool.Close)

		w := NewWorker(wpool)
		w.Handle("go_bench", func(ctx context.Context, in int) (int, error) {
			return in + 1, nil
		})
		startTestWorker(b, w)
	}

	runID, _, err := Run(ctx, pool, "go_bench", -1)
	if err != nil {
		b.Fatal(err)
	}
	if err := WaitForOutput(ctx, pool, runID, nil, WaitOpts{PollInterval: time.Millisecond}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRunThroughput(b *testing.B) {
	setupBenchmark(b, 1)
	pool, ctx := testPool, context.Background()
	wait := WaitOpts{PollInterval: time.Millisecond}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		runID, _, err := Run(ctx, pool, "go_bench", i)
		if err != nil {
			b.Fatal(err)
		}

		var out int
		if err := WaitForOutput(ctx, pool, runID, &out, wait); err != nil {
			b.Fatal(err)
		}
		if out != i+1 {
			b.Fatalf("unexpected run output: got %d, want %d", out, i+1)
		}
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "runs/s")
	}
}

func BenchmarkRunThroughputPipelined(b *testing.B) {
	setupBenchmark(b, 1)
	pool, ctx := testPool, context.Background()
	wait := WaitOpts{PollInterval: time.Millisecond}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; {
		batchSize := min(benchmarkPipelineBatchSize, b.N-i)
		runIDs := make([]int64, 0, batchSize)

		for j := range batchSize {
			runID, _, err := Run(ctx, pool, "go_bench", i+j)
			if err != nil {
				b.Fatal(err)
			}
			runIDs = append(runIDs, runID)
		}

		for j, runID := range runIDs {
			var out int
			if err := WaitForOutput(ctx, pool, runID, &out, wait); err != nil {
				b.Fatal(fmt.Errorf("run %d: %w", runID, err))
			}
			if want := i + j + 1; out != want {
				b.Fatalf("unexpected run output: got %d, want %d", out, want)
			}
		}

		i += batchSize
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "runs/s")
	}
}

func BenchmarkEngineThroughput(b *testing.B) {
	benchmarkEngineThroughput(b, 1)
}

// The worker is one sequential loop; scaling is a fleet. Four workers on
// one pool, competing for the same claims over a standing backlog.
func BenchmarkEngineThroughputFleet(b *testing.B) {
	benchmarkEngineThroughput(b, 4)
}

// benchmarkEngineThroughput submits every run up front and then waits for
// the completed count, so the engine drains a standing backlog and the
// submitter's round trips overlap the workers instead of pacing them.
func benchmarkEngineThroughput(b *testing.B, workers int) {
	setupBenchmark(b, workers)
	pool, ctx := testPool, context.Background()

	// Count completions only among the runs this benchmark creates: the id
	// watermark keeps the count query off the rows earlier benchmarks left.
	var watermark int64
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(max(id), 0) FROM cb_job_runs`).Scan(&watermark); err != nil {
		b.Fatal(err)
	}
	completed := func() int {
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM cb_job_runs WHERE id > $1 AND status = 'completed'`,
			watermark,
		).Scan(&n); err != nil {
			b.Fatal(err)
		}
		return n
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, _, err := Run(ctx, pool, "go_bench", i); err != nil {
			b.Fatal(err)
		}
	}

	deadline := time.Now().Add(10 * time.Minute)
	for completed() < b.N {
		if time.Now().After(deadline) {
			b.Fatalf("timed out draining the backlog: %d of %d runs completed", completed(), b.N)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "runs/s")
	}
}
