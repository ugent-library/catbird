package tasks

import (
	"context"
	"log/slog"

	"github.com/ugent-library/catbird"
)

type Task struct {
	name    string
	topic   string
	fn      func(context.Context, *catbird.Message) error
	workers int
}

type TaskOpts struct {
	Workers int
}

func New(name, topic string, fn func(context.Context, *catbird.Message) error, opts TaskOpts) *Task {
	if opts.Workers == 0 {
		opts.Workers = 1
	}

	return &Task{
		name:    name,
		topic:   topic,
		fn:      fn,
		workers: opts.Workers,
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

func (r *Runner) Run(ctx context.Context) error {
	return nil
}
