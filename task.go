package catbird

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

type Task struct {
	name    string
	queue   string
	hideFor time.Duration
	fn      func(context.Context, []byte) ([]byte, error)
}

type TaskOpts struct {
	HideFor time.Duration
}

func NewTask[Input, Output any](name string, fn func(context.Context, Input) (Output, error), opts TaskOpts) *Task {
	return &Task{
		name:    name,
		queue:   "t_" + name,
		hideFor: opts.HideFor,
		fn: func(ctx context.Context, b []byte) ([]byte, error) {
			var in Input
			if err := json.Unmarshal(b, &in); err != nil {
				return nil, err
			}

			out, err := fn(ctx, in)
			if err != nil {
				return nil, err
			}

			return json.Marshal(out)
		},
	}
}

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
		CreateQueue(ctx, w.conn, t.queue, QueueOpts{})

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

			if err = failStep(ctx, w.conn, p.FlowRunID, p.StepName, err.Error()); err != nil {
				w.logger.Error("task: cannot fail step", "task", t.name, "error", err)
			}
		} else if err := completeStep(ctx, w.conn, p.FlowRunID, p.StepName, out); err != nil {
			w.logger.Error("task: cannot complete step", "task", t.name, "error", err)
		}
	}
}

type taskPayload struct {
	FlowRunID string          `json:"flow_run_id"`
	FlowName  string          `json:"flow_name"`
	StepName  string          `json:"step_name"`
	Input     json.RawMessage `json:"input"`
}

func completeStep(ctx context.Context, conn Conn, flowRunID string, stepName string, out []byte) error {
	q := `SELECT * FROM cb_complete_step(flow_run_id => $1, step_name => $2, output => $3);`
	_, err := conn.Exec(ctx, q, flowRunID, stepName, out)
	return err
}

func failStep(ctx context.Context, conn Conn, flowRunID string, stepName string, errMsg string) error {
	q := `SELECT * FROM cb_fail_step(flow_run_id => $1, step_name => $2, error_message => $3);`
	_, err := conn.Exec(ctx, q, flowRunID, stepName, errMsg)
	return err
}
