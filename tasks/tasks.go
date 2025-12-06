package tasks

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ugent-library/catbird"
)

type Task struct {
	name        string
	topics      []string
	fn          func(context.Context, catbird.Message) error
	concurrency int
	hideFor     time.Duration
	retries     int
}

type TaskOpts struct {
	Concurrency int
	HideFor     time.Duration
	Retries     int
}

func New(name string, topics []string, fn func(context.Context, catbird.Message) error, opts TaskOpts) *Task {
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}

	return &Task{
		name:        name,
		topics:      topics,
		fn:          fn,
		concurrency: opts.Concurrency,
		hideFor:     opts.HideFor,
		retries:     opts.Retries,
	}
}

type Runner struct {
	client *catbird.Client
	tasks  []*Task
	logger *slog.Logger
}

type RunnerOpts struct {
	Tasks  []*Task
	Logger *slog.Logger
}

func NewRunner(client *catbird.Client, opts RunnerOpts) *Runner {
	r := &Runner{
		client: client,
		tasks:  opts.Tasks,
		logger: opts.Logger,
	}

	return r
}

// TODO extend hide for
func (r *Runner) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, t := range r.tasks {
		queue := "t_" + t.name

		r.client.CreateQueue(ctx, queue, t.topics, catbird.QueueOpts{})

		for i := 0; i < t.concurrency; i++ {
			g.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return nil
					default:
						msgs, err := r.client.ReadPoll(ctx, queue, 1, t.hideFor, catbird.ReadPollOpts{})
						if err != nil {
							r.logger.Error("tasks: cannot read message", "task", t.name, "error", err)
							continue
						}
						if len(msgs) > 0 {
							msg := msgs[0]
							if err = t.fn(ctx, msg); err != nil {
								r.logger.Error("tasks: task failed", "task", t.name, "error", err)
								// leave message in queue for next try
								if t.retries == 0 || msg.Deliveries < t.retries {
									continue
								}
							}
							if _, err = r.client.Delete(ctx, queue, msg.ID); err != nil {
								r.logger.Error("tasks: cannot delete message", "task", t.name, "error", err)
							}
						}
					}
				}
			})
		}
	}

	return g.Wait()
}
