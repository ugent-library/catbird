package catbird

// import (
// 	"context"
// 	"fmt"
// 	"log/slog"
// 	"sync"
// 	"time"

// 	"github.com/robfig/cron/v3"
// 	"github.com/ugent-library/catbird"
// )

// type Task struct {
// 	name        string
// 	queue       string
// 	topics      []string
// 	fn          func(context.Context, catbird.Message) error
// 	concurrency int
// 	hideFor     time.Duration
// 	retries     int
// 	timeout     time.Duration
// 	schedule    string
// }

// type TaskOpts struct {
// 	Concurrency int
// 	HideFor     time.Duration
// 	Retries     int
// 	Timeout     time.Duration
// 	Schedule    string
// }

// func New(name string, topics []string, fn func(context.Context, catbird.Message) error, opts TaskOpts) *Task {
// 	if opts.Concurrency == 0 {
// 		opts.Concurrency = 1
// 	}

// 	return &Task{
// 		name:        name,
// 		queue:       "t_" + name,
// 		topics:      topics,
// 		fn:          fn,
// 		concurrency: opts.Concurrency,
// 		hideFor:     opts.HideFor,
// 		retries:     opts.Retries,
// 		timeout:     opts.Timeout,
// 		schedule:    opts.Schedule,
// 	}
// }

// type Runner struct {
// 	conn      catbird.Conn
// 	tasks     []*Task
// 	logger    *slog.Logger
// 	scheduler *cron.Cron
// 	timeout   time.Duration
// }

// type RunnerOpts struct {
// 	Tasks  []*Task
// 	Logger *slog.Logger
// 	Tmeout time.Duration
// }

// func NewRunner(conn catbird.Conn, opts RunnerOpts) (*Runner, error) {
// 	r := &Runner{
// 		conn:    conn,
// 		tasks:   opts.Tasks,
// 		logger:  opts.Logger,
// 		timeout: opts.Tmeout,
// 	}

// 	for _, t := range opts.Tasks {
// 		if t.schedule == "" {
// 			continue
// 		}
// 		if r.scheduler == nil {
// 			r.scheduler = cron.New(cron.WithSeconds())
// 		}
// 		_, err := r.scheduler.AddFunc(t.schedule, func() {
// 			for _, topic := range t.topics {
// 				err := catbird.Dispatch(context.TODO(), r.conn, topic, &struct{}{}, catbird.DispatchOpts{DeduplicationID: "scheduled:" + t.name})
// 				if err != nil {
// 					r.logger.Error("tasks: failed to schedule task", "task", t.name, "error", err)
// 				}
// 			}
// 		})
// 		if err != nil {
// 			return nil, fmt.Errorf("tasks: failed to register schedule for task %s: %w", t.name, err)
// 		}
// 	}

// 	return r, nil
// }

// // TODO extend hide_for if necessary
// func (r *Runner) Run(ctx context.Context) {
// 	var wg sync.WaitGroup

// 	if r.scheduler != nil {
// 		r.scheduler.Start()

// 		wg.Go(func() {
// 			<-ctx.Done()

// 			stopCtx := r.scheduler.Stop()
// 			select {
// 			case <-stopCtx.Done():
// 			case <-time.After(r.timeout):
// 			}
// 		})
// 	}

// 	for _, t := range r.tasks {
// 		catbird.CreateQueue(ctx, r.conn, t.queue, catbird.QueueOpts{Topics: t.topics})

// 		for i := 0; i < t.concurrency; i++ {
// 			wg.Go(func() {
// 				for {
// 					select {
// 					case <-ctx.Done():
// 						return
// 					default:
// 						r.runTask(ctx, t)
// 						time.Sleep(1 * time.Second) // TODO
// 					}
// 				}
// 			})
// 		}
// 	}

// 	wg.Wait()
// }

// func (r *Runner) runTask(ctx context.Context, t *Task) {
// 	msgs, err := catbird.ReadPoll(ctx, r.conn, t.queue, 1, t.hideFor, catbird.ReadPollOpts{})
// 	if err != nil {
// 		r.logger.Error("tasks: cannot read message", "task", t.name, "error", err)
// 		return
// 	}

// 	if len(msgs) == 0 {
// 		return
// 	}

// 	msg := msgs[0]

// 	runCtx := ctx
// 	if t.timeout > 0 {
// 		var cancel context.CancelFunc
// 		runCtx, cancel = context.WithTimeout(ctx, time.Duration(t.timeout))
// 		defer cancel()
// 	}

// 	if err = t.fn(runCtx, msg); err != nil {
// 		r.logger.Error("tasks: task failed", "task", t.name, "error", err)
// 		// leave message in queue for next try
// 		// TODO backoff
// 		if t.retries > 0 && msg.Deliveries == t.retries {
// 			if _, err = catbird.Fail(ctx, r.conn, t.queue, msg.ID); err != nil {
// 				r.logger.Error("tasks: cannot fail message", "task", t.name, "error", err)
// 			}
// 		}
// 		return
// 	}

// 	if _, err = catbird.Archive(ctx, r.conn, t.queue, msg.ID); err != nil {
// 		r.logger.Error("tasks: cannot archive message", "task", t.name, "error", err)
// 	}
// }
