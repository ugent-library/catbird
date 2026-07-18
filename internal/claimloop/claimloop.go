// Package claimloop holds the loop mechanics every claim-based consumer
// shares: the streams subscription consumer and the jobs worker. Each
// module owns its contract — what to claim, how to report — while this
// package owns the timing: poll when idle, back off on errors, and keep a
// claim alive while a handler runs.
package claimloop

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// ErrLost reports that a claim expired and another consumer adopted it.
var ErrLost = errors.New("catbird: claim lost")

// Name generates a unique name for a consumer or worker: the hostname,
// pid and a random suffix. kind is each module's own word for what it
// names — the streams consumer, the jobs worker — used when there is no
// hostname to build on.
func Name(kind string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = kind
	}
	host = strings.ReplaceAll(host, ".", "_") // keep the generated name dot-free
	var r [8]byte
	rand.Read(r[:])
	return fmt.Sprintf("%s_%d_%x", host, os.Getpid(), r)
}

// Options tunes Run. PollInterval is required; zero values elsewhere mean the
// defaults.
type Options struct {
	PollInterval time.Duration        // wait between passes when there is nothing to do
	MaxBackoff   time.Duration        // cap for the error backoff; 30s by default
	Wake         <-chan struct{}      // optional: ends an idle or backoff wait early
	Fatal        func(err error) bool // optional: a process error that ends the loop
}

// Run repeats process until ctx ends. A pass that found work runs again at
// once; an idle pass waits PollInterval (or a Wake signal); a failing pass is
// retried with exponential backoff. A process error for which Fatal
// returns true ends the loop.
func Run(ctx context.Context, o Options, process func(ctx context.Context) (worked bool, err error)) error {
	maxBackoff := o.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	timer := time.NewTimer(o.PollInterval)
	defer timer.Stop()
	wait := func(d time.Duration) error {
		timer.Reset(d)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-o.Wake:
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
			return nil
		}
	}

	backoff := o.PollInterval
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		worked, err := process(ctx)

		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil && o.Fatal != nil && o.Fatal(err):
			return err
		case err != nil:
			if err := wait(backoff); err != nil {
				return err
			}
			backoff = min(backoff*2, maxBackoff)
		case worked:
			backoff = o.PollInterval
		default:
			backoff = o.PollInterval
			if err := wait(o.PollInterval); err != nil {
				return err
			}
		}
	}
}

// Handle runs one handler call while the keepAlive cadence keeps the claim
// alive. The verdict is the handler's own outcome — its error, or its panic
// reported as one. err is the loop's outcome: ErrLost when the claim
// expired anyway and another consumer took it, or the failed keepAlive. On
// a non-nil err the handler is canceled and awaited first, so one consumer
// never runs two handlers at once, and the verdict is meaningless.
// stillRunning, when given, is called after each successful keepAlive so
// the caller can log a slow handler.
func Handle(ctx context.Context, interval time.Duration,
	keepAlive func() (bool, error),
	stillRunning func(elapsed time.Duration),
	handler func(ctx context.Context) error,
) (verdict error, err error) {
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("catbird: handler panic: %v", r)
			}
		}()
		done <- handler(hctx)
	}()

	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	started := time.Now()
	for {
		select {
		case verdict := <-done:
			return verdict, nil
		case <-ticker.C:
			ok, err := keepAlive()
			if err == nil && ok {
				if stillRunning != nil {
					stillRunning(time.Since(started))
				}
				continue
			}
			// the claim is no longer ours, or keepAlive cannot be delivered:
			// stop the handler and wait for it before handing off
			cancel()
			<-done
			if err != nil {
				return nil, err
			}
			return nil, ErrLost
		}
	}
}
