package ticker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Job struct {
	Name  string
	Every time.Duration
	Run   func(ctx context.Context) (int, error)
}

type Ticker struct {
	logger *slog.Logger
	jobs   []Job
}

func New(logger *slog.Logger) *Ticker {
	return &Ticker{logger: logger}
}

// Add registers a job. All jobs must be added before Start.
func (t *Ticker) Add(job Job) {
	t.jobs = append(t.jobs, job)
}

// Start runs every job on its interval. It blocks until ctx is done, then
// waits for in-flight runs to finish.
func (t *Ticker) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, job := range t.jobs {
		wg.Go(func() {
			t.run(ctx, job)
		})
	}
	wg.Wait()
	return ctx.Err()
}

func (t *Ticker) run(ctx context.Context, job Job) {
	timer := time.NewTimer(job.Every)
	defer timer.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		n, err := job.Run(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown, not a job failure
			}
			t.logger.Error("catbird: ticker job failed", "job", job.Name, "error", err)
		} else if n > 0 {
			continue
		}

		timer.Reset(job.Every)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}
