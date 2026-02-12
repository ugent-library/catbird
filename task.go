package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Task represents a task definition with a generic typed handler
type Task struct {
	Name    string `json:"name"`
	handler *taskHandler
}

type taskMessage struct {
	ID         int64           `json:"id"`
	Deliveries int             `json:"deliveries"`
	Input      json.RawMessage `json:"input"`
}

type taskHandler struct {
	handlerOpts
	fn func(context.Context, taskMessage) ([]byte, error)
}

// NewTask creates a new task with a generic handler function
// The handler receives typed input and returns typed output
// Input and output types are automatically marshaled to/from JSON
func NewTask[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...HandlerOpt) *Task {
	h := &taskHandler{
		handlerOpts: handlerOpts{
			concurrency: 1,
			batchSize:   10,
		},
		fn: func(ctx context.Context, msg taskMessage) ([]byte, error) {
			var in In
			if err := json.Unmarshal(msg.Input, &in); err != nil {
				return nil, err
			}

			out, err := fn(ctx, in)
			if err != nil {
				return nil, err
			}

			return json.Marshal(out)
		},
	}

	for _, opt := range opts {
		opt(&h.handlerOpts)
	}

	if err := h.handlerOpts.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for task %s: %v", name, err))
	}

	return &Task{
		Name:    name,
		handler: h,
	}
}

type TaskInfo struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateTask creates a new task definition.
func CreateTask(ctx context.Context, conn Conn, task *Task) error {
	q := `SELECT * FROM cb_create_task(name => $1);`
	_, err := conn.Exec(ctx, q, task.Name)
	if err != nil {
		return err
	}
	return nil
}

// GetTask retrieves task metadata by name.
func GetTask(ctx context.Context, conn Conn, name string) (*TaskInfo, error) {
	q := `SELECT name, created_at FROM cb_tasks WHERE name = $1;`
	return scanTask(conn.QueryRow(ctx, q, name))
}

// ListTasks returns all tasks
func ListTasks(ctx context.Context, conn Conn) ([]*TaskInfo, error) {
	q := `SELECT name, created_at FROM cb_tasks ORDER BY name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleTask)
}

type RunTaskOpts struct {
	DeduplicationID string
}

// RunTask enqueues a task execution and returns a handle for monitoring
// progress and retrieving output.
func RunTask(ctx context.Context, conn Conn, name string, input any) (*RunHandle, error) {
	return RunTaskWithOpts(ctx, conn, name, input, RunTaskOpts{})
}

// RunTaskWithOpts enqueues a task with options for deduplication and returns
// a handle for monitoring.
func RunTaskWithOpts(ctx context.Context, conn Conn, name string, input any, opts RunTaskOpts) (*RunHandle, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	q := `SELECT * FROM cb_run_task(name => $1, input => $2, deduplication_id => $3);`
	var id int64
	err = conn.QueryRow(ctx, q, name, b, ptrOrNil(opts.DeduplicationID)).Scan(&id)
	if err != nil {
		return nil, err
	}

	return &RunHandle{conn: conn, getFn: GetTaskRun, Name: name, ID: id}, nil
}

// GetTaskRun retrieves a specific task run result by ID.
func GetTaskRun(ctx context.Context, conn Conn, name string, id int64) (*RunInfo, error) {
	tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, deduplication_id, status, input, output, error_message, started_at, completed_at, failed_at, skipped_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanRun(conn.QueryRow(ctx, query, id))
}

// ListTaskRuns returns recent task runs for the specified task.
func ListTaskRuns(ctx context.Context, conn Conn, name string) ([]*RunInfo, error) {
	tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, deduplication_id, status, input, output, error_message, started_at, completed_at, failed_at, skipped_at FROM %s ORDER BY started_at DESC LIMIT 20;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleRun)
}

func scanCollectibleTask(row pgx.CollectableRow) (*TaskInfo, error) {
	return scanTask(row)
}

func scanTask(row pgx.Row) (*TaskInfo, error) {
	rec := TaskInfo{}

	if err := row.Scan(
		&rec.Name,
		&rec.CreatedAt,
	); err != nil {
		return nil, err
	}

	return &rec, nil
}
