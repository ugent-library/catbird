package catbird_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ugent-library/catbird"
)

// A scheduled type is ticked by the process that handles it: on a matching
// minute a job is enqueued, keyed periodic:<type>:<minute>, and runs like any
// other. "* * * * *" matches every minute, so the first tick is immediate.
func TestPeriodicRuns(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := catbird.NewQueue("periodic_queue", catbird.QueueOptions{})
	report := catbird.NewJobType("periodic.report", queue, catbird.JobTypeOptions{Schedule: "* * * * *"})

	var ran atomic.Int32
	rt := catbird.New(pool, catbird.Options{})
	rt.Handle(report, func(ctx context.Context, job *catbird.Job) error {
		ran.Add(1)
		return nil
	})
	go rt.Start(ctx)

	waitFor(t, 10*time.Second, "the scheduled job did not run", func() bool { return ran.Load() >= 1 })
	if n := count(t, pool, "SELECT count(*) FROM cb_messages WHERE deduplication_key LIKE 'periodic:periodic.report:%'"); n < 1 {
		t.Fatalf("no message carries the tick's deduplication key")
	}
}

// While a live job of the type exists — here a manual Enqueue, which counts
// like a tick's job — a tick writes nothing: no claim, no message, no key.
func TestPeriodicSkipsWhileAJobLives(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := catbird.NewQueue("periodic_queue", catbird.QueueOptions{})
	report := catbird.NewJobType("periodic.report", queue, catbird.JobTypeOptions{Schedule: "* * * * *"})

	if _, err := catbird.Enqueue(ctx, pool, report, nil, catbird.EnqueueOptions{Delay: time.Hour}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var ran atomic.Int32
	rt := catbird.New(pool, catbird.Options{})
	rt.Handle(report, func(ctx context.Context, job *catbird.Job) error {
		ran.Add(1)
		return nil
	})
	go rt.Start(ctx)

	time.Sleep(2 * time.Second)
	if n := ran.Load(); n != 0 {
		t.Fatalf("a job ran %d times while the manual job lives", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_claims WHERE job_type = 'periodic.report'"); n != 1 {
		t.Fatalf("%d claims, want only the manual job's", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_messages WHERE deduplication_key LIKE 'periodic:%'"); n != 0 {
		t.Fatalf("%d tick messages written during a live job, want 0", n)
	}
}

// Several processes handling a scheduled type all tick it; the deduplication
// key makes their same-minute ticks one job, and the live-run guard holds the
// later minutes while that job runs.
func TestPeriodicOneJobAcrossProcesses(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := catbird.NewQueue("periodic_queue", catbird.QueueOptions{})
	report := catbird.NewJobType("periodic.report", queue, catbird.JobTypeOptions{Schedule: "* * * * *"})

	release := make(chan struct{})
	handle := func(ctx context.Context, job *catbird.Job) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}
	for range 2 {
		rt := catbird.New(pool, catbird.Options{})
		rt.Handle(report, handle)
		go rt.Start(ctx)
	}

	waitFor(t, 10*time.Second, "no tick produced a job", func() bool {
		return count(t, pool, "SELECT count(*) FROM cb_claims WHERE job_type = 'periodic.report'") == 1
	})
	time.Sleep(time.Second)
	if n := count(t, pool, "SELECT count(*) FROM cb_messages WHERE deduplication_key LIKE 'periodic:%'"); n != 1 {
		t.Fatalf("%d tick messages, want 1", n)
	}
	close(release)
}

func TestScheduleDeclarationPanics(t *testing.T) {
	queue := catbird.NewQueue("periodic_queue", catbird.QueueOptions{})
	expectPanic := func(name string, opts catbird.JobTypeOptions) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: expected a panic", name)
			}
		}()
		catbird.NewJobType(name, queue, opts)
	}
	expectPanic("periodic.bad_syntax", catbird.JobTypeOptions{Schedule: "not a schedule"})
	expectPanic("periodic.gated", catbird.JobTypeOptions{Schedule: "* * * * *", Signal: true})
	expectPanic("periodic.never", catbird.JobTypeOptions{Schedule: "0 0 31 2 *"})
}
