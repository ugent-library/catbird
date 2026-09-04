package catbird_test

import (
	"context"
	"strings"
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
	rt.HandleFunc(report, func(ctx context.Context, job *catbird.Job) error {
		ran.Add(1)
		return nil
	})
	go rt.Start(ctx)

	waitFor(t, 10*time.Second, "the scheduled job did not run", func() bool { return ran.Load() >= 1 })
	if n := count(t, pool, "SELECT count(*) FROM cb_messages WHERE deduplication_key LIKE 'periodic:periodic.report:%'"); n < 1 {
		t.Fatalf("no message carries the tick's deduplication key")
	}
}

// While a live job of the type exists — here a manual Enqueue, which carries
// the type's unique key like a tick's job — a tick writes nothing: no job, no
// message, no key.
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
	rt.HandleFunc(report, func(ctx context.Context, job *catbird.Job) error {
		ran.Add(1)
		return nil
	})
	go rt.Start(ctx)

	time.Sleep(2 * time.Second)
	if n := ran.Load(); n != 0 {
		t.Fatalf("a job ran %d times while the manual job lives", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_jobs WHERE job_type = 'periodic.report'"); n != 1 {
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
		rt.HandleFunc(report, handle)
		go rt.Start(ctx)
	}

	waitFor(t, 10*time.Second, "no tick produced a job", func() bool {
		return count(t, pool, "SELECT count(*) FROM cb_jobs WHERE job_type = 'periodic.report'") == 1
	})
	time.Sleep(time.Second)
	if n := count(t, pool, "SELECT count(*) FROM cb_messages WHERE deduplication_key LIKE 'periodic:%'"); n != 1 {
		t.Fatalf("%d tick messages, want 1", n)
	}
	close(release)
}

// Every job of a scheduled type carries the type's name as its unique key, so
// two manual enqueues cannot overlap and a caller cannot give such a job a key
// of its own. The ways of creating a job that cannot carry the key refuse the
// type: a batch, a trigger, and a handler's Enqueue, which fails the completion.
func TestScheduledTypeHasOneLiveJob(t *testing.T) {
	pool := setupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queue := catbird.NewQueue("periodic_queue", catbird.QueueOptions{PollInterval: 25 * time.Millisecond})
	// No process handles this type, so nothing ticks it and every job of it
	// below is one the test enqueued.
	report := catbird.NewJobType("periodic.report", queue, catbird.JobTypeOptions{Schedule: "* * * * *"})
	plain := catbird.NewJobType("plain", queue, catbird.JobTypeOptions{MaxAttempts: 1})

	first, err := catbird.Enqueue(ctx, pool, report, nil, catbird.EnqueueOptions{})
	if err != nil || first == 0 {
		t.Fatalf("first enqueue: id=%d err=%v", first, err)
	}
	if id, err := catbird.Enqueue(ctx, pool, report, nil, catbird.EnqueueOptions{}); err != nil || id != 0 {
		t.Errorf("a second manual enqueue while the first is live: id=%d err=%v, want 0 and no error", id, err)
	}
	if _, err := catbird.Enqueue(ctx, pool, report, nil, catbird.EnqueueOptions{UniqueKey: "mine"}); err == nil {
		t.Error("a caller's own UniqueKey on a scheduled type was accepted")
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_jobs WHERE job_type = 'periodic.report' AND unique_key = 'periodic.report'"); n != 1 {
		t.Errorf("%d jobs of the type carry its name as their key, want 1", n)
	}

	if _, err := catbird.EnqueueBatch(ctx, pool, report, []catbird.BatchMessage{{Topic: "periodic.report"}}, catbird.EnqueueOptions{}); err == nil {
		t.Error("a batch of a scheduled type was accepted")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("a trigger on a scheduled type did not panic")
			}
		}()
		catbird.New(pool, catbird.Options{}).Trigger("report", []string{"report.#"}, report, catbird.TriggerOptions{})
	}()

	// A handler that enqueues a scheduled type fails its completion, so its
	// attempt fails and no job of the type is created.
	rt := catbird.New(pool, catbird.Options{})
	rt.HandleFunc(plain, func(ctx context.Context, job *catbird.Job) error {
		job.Enqueue(report, nil)
		return nil
	})
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go rt.Start(runCtx)
	id, err := catbird.Enqueue(ctx, pool, plain, nil, catbird.EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var js catbird.JobStatus
	waitFor(t, 5*time.Second, "the job that enqueued a scheduled type did not fail", func() bool {
		js, err = catbird.Status(ctx, pool, id)
		return err == nil && js.State == catbird.StateFailed
	})
	if !strings.Contains(js.Error, "cannot be enqueued from a handler") {
		t.Errorf("the failed job's error is %q, want the scheduled-type refusal", js.Error)
	}
	if n := count(t, pool, "SELECT count(*) FROM cb_jobs WHERE job_type = 'periodic.report'"); n != 1 {
		t.Errorf("%d jobs of the scheduled type after the handler's enqueue, want the manual one only", n)
	}
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
