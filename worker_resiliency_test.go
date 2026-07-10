package catbird

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A poll that stops responding must surface as a logged error and the loop must
// keep going, not park silently. The fake poll models pgx honoring the context
// deadline: it blocks until either its context is cancelled (the deadline the
// loop now sets) or the test lets it return, which is what pgx does once a
// socket read deadline fires on a hung connection.
func TestClaimLoopRecoversFromHungPoll(t *testing.T) {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	defer handlerCancel()

	release := make(chan struct{})
	var pollErrCount atomic.Int64
	handled := make(chan int, 1)

	cfg := claimLoopConfig[int]{
		concurrency: 1,
		pollTimeout: 100 * time.Millisecond,
		pollClaims: func(ctx context.Context) ([]int, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return []int{1}, nil
			}
		},
		handleClaim: func(_ context.Context, msg int) {
			select {
			case handled <- msg:
			default:
			}
		},
		logPollError: func(_ context.Context, _ error) {
			pollErrCount.Add(1)
		},
	}

	var wg sync.WaitGroup
	runClaimLoop(shutdownCtx, handlerCtx, &wg, cfg)

	// The stuck poll must be logged, not swallowed by a parked goroutine.
	deadline := time.After(2 * time.Second)
	for pollErrCount.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("a stuck poll never surfaced an error; the loop parked silently")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Once the connection recovers, the same loop resumes claiming.
	close(release)
	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("the loop did not resume claiming after the poll recovered")
	}

	shutdownCancel()
	wg.Wait()
}

// Shutting the worker down mid-poll must end the loop quietly. A poll cancelled
// by shutdown is not a fault and must not be logged as one — this is the case
// the timeout fix must keep distinct from a poll that hit its own deadline.
func TestClaimLoopShutdownExitsQuietly(t *testing.T) {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	defer handlerCancel()

	polling := make(chan struct{}, 1)
	var pollErrCount atomic.Int64

	cfg := claimLoopConfig[int]{
		concurrency: 1,
		pollTimeout: 5 * time.Second, // long, so shutdown wins the race, not the deadline
		pollClaims: func(ctx context.Context) ([]int, error) {
			select {
			case polling <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		handleClaim: func(_ context.Context, _ int) {},
		logPollError: func(_ context.Context, _ error) {
			pollErrCount.Add(1)
		},
	}

	var wg sync.WaitGroup
	runClaimLoop(shutdownCtx, handlerCtx, &wg, cfg)

	<-polling        // a poll is in flight
	shutdownCancel() // shut down before the poll's own deadline

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the claim loop did not exit on shutdown")
	}

	if n := pollErrCount.Load(); n != 0 {
		t.Fatalf("shutdown was logged as a poll error %d time(s); it should exit quietly", n)
	}
}

// The NOTIFY listener waits with a deadline now, so a quiet connection hits that
// deadline repeatedly. It must ping and keep listening across those idle
// timeouts rather than tear down, and still deliver a notification afterwards.
func TestWorkerNotifierSurvivesIdleTimeouts(t *testing.T) {
	getTestClient(t) // initializes testPool

	const idle = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := newWorkerNotifier(testPool, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n.pollTimeout = idle
	const channel = "cb_test_notifier_idle"
	sig := make(chan struct{}, 1)
	n.subscribe(channel, sig)

	run, err := n.listen(ctx)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() { run(); close(done) }()

	// Stay idle well past several wait deadlines so the ping-based liveness
	// check runs repeatedly.
	time.Sleep(5 * idle)

	if _, err := testPool.Exec(ctx, "SELECT pg_notify($1, $2)", channel, ""); err != nil {
		t.Fatalf("pg_notify: %v", err)
	}

	select {
	case <-sig:
	case <-time.After(2 * time.Second):
		t.Fatal("notification not delivered; the listener did not survive its idle timeouts")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not stop on shutdown")
	}
}

// This drives the real thing the fix depends on: a genuine claim query, blocked
// server-side (here by an ACCESS EXCLUSIVE lock, the issue's deterministic
// stand-in for a stuck connection), must abort on its context deadline rather
// than block forever, and must claim normally once the block clears. It is what
// proves pgx turns the deadline into a real cancellation — the piece the
// loop-level unit tests stub out.
func TestClaimQueryAbortsOnDeadlineUnderLock(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	const flowName = "claim_deadline_flow"
	flow := NewFlow(flowName)
	flow.AddStep(NewStep("step1").Do(func(_ context.Context, in string) (string, error) {
		return in, nil
	}))
	if err := client.CreateFlow(ctx, flow); err != nil {
		t.Fatalf("create flow: %v", err)
	}
	// Enqueue a run but start no worker, so the root step sits queued and
	// claimable.
	if _, err := client.RunFlow(ctx, flowName, "input"); err != nil {
		t.Fatalf("run flow: %v", err)
	}

	w := newStepWorker(testPool, slog.New(slog.NewTextHandler(io.Discard, nil)), flowName, flow.steps[0], nil, nil)
	table := fmt.Sprintf("cb_s_%s", strings.ToLower(flowName))

	// Block the step table so the claim query can make no progress.
	lockTx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if _, err := lockTx.Exec(ctx, "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock table: %v", err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = lockTx.Rollback(context.Background())
		}
	}
	defer release()

	// Under the lock the poll must hit its deadline, not hang.
	pollCtx, pollCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	start := time.Now()
	_, pollErr := w.pollClaims(pollCtx)
	pollCancel()
	elapsed := time.Since(start)

	if !errors.Is(pollErr, context.DeadlineExceeded) {
		t.Fatalf("blocked poll did not abort on the deadline: err=%v", pollErr)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("poll took %s; it hung rather than honoring the deadline", elapsed)
	}

	// Once the lock is gone, the same poll claims the queued step.
	release()

	recoverCtx, recoverCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recoverCancel()
	claims, err := w.pollClaims(recoverCtx)
	if err != nil {
		t.Fatalf("poll after lock released: %v", err)
	}
	if len(claims) == 0 {
		t.Fatal("expected to claim the queued step after the lock was released")
	}
}
