package ticker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Tick struct {
	Name  string
	Every time.Duration
	Run   func(ctx context.Context) (int, error)
}

type Ticker struct {
	logger *slog.Logger
	ticks  []Tick
}

func New(logger *slog.Logger) *Ticker {
	return &Ticker{logger: logger}
}

// Add registers a tick. All ticks must be added before Start.
func (t *Ticker) Add(tick Tick) {
	t.ticks = append(t.ticks, tick)
}

// Start runs every tick on its interval. It blocks until ctx is done, then
// waits for in-flight runs to finish.
func (t *Ticker) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, tick := range t.ticks {
		wg.Go(func() {
			t.run(ctx, tick)
		})
	}
	wg.Wait()
	return ctx.Err()
}

func (t *Ticker) run(ctx context.Context, tick Tick) {
	timer := time.NewTimer(tick.Every)
	defer timer.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		n, err := tick.Run(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown, not a tick failure
			}
			t.logger.Error("catbird: tick failed", "tick", tick.Name, "error", err)
		} else if n > 0 {
			continue
		}

		timer.Reset(tick.Every)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}
