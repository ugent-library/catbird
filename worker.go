package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

type Worker struct {
	id      string
	conn    Conn
	tasks   []*Task
	log     *slog.Logger
	timeout time.Duration
}

type WorkerOpts struct {
	Tasks   []*Task
	Log     *slog.Logger
	Timeout time.Duration
}

func NewWorker(conn Conn, opts WorkerOpts) (*Worker, error) {
	w := &Worker{
		id:      uuid.NewString(),
		conn:    conn,
		tasks:   opts.Tasks,
		log:     opts.Log,
		timeout: opts.Timeout,
	}

	return w, nil
}

func (w *Worker) Start(ctx context.Context) error {
	var scheduler *cron.Cron
	var taskNames []string
	var wg sync.WaitGroup

	for _, t := range w.tasks {
		taskNames = append(taskNames, t.name)

		if t.schedule != "" {
			if scheduler == nil {
				scheduler = cron.New(cron.WithSeconds())
			}

			var entryID cron.EntryID
			entryID, err := scheduler.AddFunc(t.schedule, func() {
				entry := scheduler.Entry(entryID)
				scheduledTime := entry.Prev
				if scheduledTime.IsZero() {
					// use Next if Prev is not set, which will only happen for the first run
					scheduledTime = entry.Next
				}
				dedupID := fmt.Sprintf("%s-cron-%s", t.name, scheduledTime)
				_, err := RunTask(ctx, w.conn, t.name, struct{}{}, RunTaskOpts{dedupID})
				if err != nil {
					w.log.Error("worker: failed to schedule task", "task", t.name, "error", err)
				}
			})
			if err != nil {
				return fmt.Errorf("worker: failed to register schedule for task %s: %w", t.name, err)
			}
		}

		if err := CreateTask(ctx, w.conn, t); err != nil {
			return err
		}
	}

	if _, err := w.conn.Exec(ctx, `SELECT * FROM cb_worker_started(id => $1, tasks => $2);`, w.id, taskNames); err != nil {
		return err
	}

	wg.Go(func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := w.conn.Exec(ctx, `SELECT * FROM cb_worker_heartbeat(id => $1);`, w.id); err != nil {
					w.log.Error("worker: cannot send heartbeat", "error", err)
				}
			}
		}
	})

	if scheduler != nil {
		scheduler.Start()

		wg.Go(func() {
			<-ctx.Done()

			stopCtx := scheduler.Stop()
			select {
			case <-stopCtx.Done():
			case <-time.After(w.timeout):
			}
		})
	}

	for _, t := range w.tasks {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					w.runTask(ctx, t)
				}
			}
		})
	}

	wg.Wait()

	return nil
}

type taskPayload struct {
	RunID string          `json:"run_id"`
	Input json.RawMessage `json:"input"`
}

func (w *Worker) runTask(ctx context.Context, t *Task) {
	msgs, err := ReadPoll(ctx, w.conn, t.queue, t.batchSize, t.hideFor, ReadPollOpts{})
	if err != nil {
		w.log.Error("task: cannot read message", "task", t.name, "error", err)
		return
	}

	if len(msgs) == 0 {
		time.Sleep(1 * time.Second) // TODO
		return
	}

	// TODO concurrency
	for _, msg := range msgs {
		log.Printf("message payload for task %s: %s", t.name, msg.Payload) // remove or log debug

		runCtx := ctx
		if t.timeout > 0 {
			var cancel context.CancelFunc
			runCtx, cancel = context.WithTimeout(ctx, time.Duration(t.timeout))
			defer cancel()
		}

		var p taskPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			w.log.Error("task: cannot decode payload", "task", t.name, "error", err)
		}

		out, err := t.fn(runCtx, p.Input)

		if err != nil {
			w.log.Error("task: failed", "task", t.name, "error", err)

			if msg.Deliveries > t.retries {
				q := `SELECT * FROM cb_fail_task(run_id => $1, error_message => $2);`
				if _, err := w.conn.Exec(ctx, q, p.RunID, err.Error()); err != nil {
					w.log.Error("task: cannot mark task as failed", "task", t.name, "error", err)
				}
			} else if t.delay > 0 {
				delay := t.delay
				if t.jitter > 0 {
					delay += time.Duration((1 - rand.Float64()*2) * float64(t.jitter))
				}

				if _, err := Hide(ctx, w.conn, t.queue, msg.ID, delay); err != nil {
					w.log.Error("task: cannot delay next task run", "task", t.name, "error", err)
				}
			}
		} else {
			q := `SELECT * FROM cb_complete_task(run_id => $1, output => $2);`
			if _, err := w.conn.Exec(ctx, q, p.RunID, out); err != nil {
				w.log.Error("task: cannot mark task as completed", "task", t.name, "error", err)
			}
		}
	}
}
