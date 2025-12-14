package catbird

import (
	"context"
	"encoding/json"
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

func CreateTask(ctx context.Context, conn Conn, task *Task) error {
	q := `SELECT * FROM cb_create_task(name => $1);`
	_, err := conn.Exec(ctx, q, task.name)
	if err != nil {
		return err
	}
	return nil
}

func RunTask(ctx context.Context, conn Conn, name string, input any) (string, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	q := `SELECT * FROM cb_run_task(name => $1, input => $2);`
	var runID string
	err = conn.QueryRow(ctx, q, name, b).Scan(&runID)
	if err != nil {
		return "", err
	}
	return runID, err
}
