package catbird

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"log/slog"
)

type Worker struct {
	conn   Conn
	tasks  []*Task
	logger *slog.Logger
}

type WorkerOpts struct {
	Tasks  []*Task
	Logger *slog.Logger
}

func NewWorker(conn Conn, opts WorkerOpts) (*Worker, error) {
	r := &Worker{
		conn:   conn,
		tasks:  opts.Tasks,
		logger: opts.Logger,
	}

	return r, nil
}

func (w *Worker) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for _, t := range w.tasks {
		CreateTask(ctx, w.conn, t)

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
}

type taskPayload struct {
	RunID string          `json:"run_id"`
	Input json.RawMessage `json:"input"`
}

func (w *Worker) runTask(ctx context.Context, t *Task) {
	msgs, err := ReadPoll(ctx, w.conn, t.queue, 1, t.hideFor, ReadPollOpts{})
	if err != nil {
		w.logger.Error("task: cannot read message", "task", t.name, "error", err)
		return
	}

	if len(msgs) == 0 {
		time.Sleep(1 * time.Second) // TODO backoff etc
		return
	}

	for _, msg := range msgs {
		var p taskPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			w.logger.Error("task: cannot decode payload", "task", t.name, "error", err)
		}

		out, err := t.fn(ctx, p.Input)

		if err != nil {
			w.logger.Error("task: failed", "task", t.name, "error", err)

			q := `SELECT * FROM cb_fail_task(run_id => $1, error_message => $2);`
			if _, err := w.conn.Exec(ctx, q, p.RunID, err.Error()); err != nil {
				w.logger.Error("task: cannot mark task as failed", "task", t.name, "error", err)

			}
		} else {
			q := `SELECT * FROM cb_complete_task(run_id => $1, output => $2);`
			if _, err := w.conn.Exec(ctx, q, p.RunID, out); err != nil {
				w.logger.Error("task: cannot mark task as completed", "task", t.name, "error", err)

			}
		}
	}
}
