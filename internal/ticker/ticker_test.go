package ticker

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// A woken tick runs at once instead of waiting out its interval.
func TestWake(t *testing.T) {
	wake := make(chan struct{}, 1)
	ran := make(chan struct{}, 16)

	tk := New(slog.Default())
	tk.Add(Tick{
		Name:  "test.wake",
		Every: time.Minute,
		Wake:  wake,
		Run: func(ctx context.Context) (int, error) {
			ran <- struct{}{}
			return 0, nil
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = tk.Start(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	// the first pass runs unprompted
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("first pass did not run")
	}

	// with a minute-long interval, only a wake explains a prompt second pass
	wake <- struct{}{}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("wake did not end the idle wait")
	}
}
