package catbird

import (
	"context"
	"encoding/json"
)

type Flow struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

type Step struct {
	Name      string   `json:"name"`
	TaskName  string   `json:"task_name,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
}

func CreateFlow(ctx context.Context, conn Conn, flow *Flow) error {
	b, err := json.Marshal(flow.Steps)
	if err != nil {
		return err
	}
	q := `SELECT * FROM cb_create_flow(name => $1, steps => $2);`
	_, err = conn.Exec(ctx, q, flow.Name, b)
	if err != nil {
		return err
	}
	return nil
}

func RunFlow(ctx context.Context, conn Conn, name string, input any) (string, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	q := `SELECT * FROM cb_run_flow(name => $1, input => $2);`
	var runID string
	err = conn.QueryRow(ctx, q, name, b).Scan(&runID)
	if err != nil {
		return "", err
	}
	return runID, err
}
