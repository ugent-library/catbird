package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Task is a reflection-based task with optional handler.
// Use NewTask().Handler(fn, opts) for tasks with handlers.
// Use NewTask() for definition-only tasks.
type Task struct {
	name          string
	condition     string
	schedule      string
	scheduleInput func(context.Context) (any, error)
	handler       func(context.Context, json.RawMessage) (json.RawMessage, error)
	handlerOpts   *HandlerOpts
}

// NewTask creates a new task definition with the given name.
// Chain .Handler() to add a handler, otherwise returns a definition-only task.
func NewTask(name string) *Task {
	return &Task{name: name}
}

// Condition sets the condition expression for the task.
func (t *Task) Condition(condition string) *Task {
	t.condition = condition
	return t
}

func (t *Task) Schedule(schedule string, inputFunc func(context.Context) (any, error)) *Task {
	t.schedule = schedule
	t.scheduleInput = inputFunc
	return t
}

// Handler sets the task handler function and execution options.
// fn must have signature (context.Context, In) (Out, error).
// If opts is nil, defaults are used (concurrency: 1, batchSize: 10).
func (t *Task) Handler(fn any, opts *HandlerOpts) *Task {
	handler, err := makeTaskHandler(fn, t.name)
	if err != nil {
		panic(err)
	}
	t.handler = handler
	t.handlerOpts = applyDefaultHandlerOpts(opts)
	return t
}

// MarshalJSON serializes the task for JSON output
func (t *Task) MarshalJSON() ([]byte, error) {
	type serializableTask struct {
		Name      string `json:"name"`
		Condition string `json:"condition,omitempty"`
	}
	return json.Marshal(serializableTask{
		Name:      t.name,
		Condition: t.condition,
	})
}

// makeTaskHandler uses reflection once to extract types and create cached wrapper
func makeTaskHandler(fn any, name string) (func(context.Context, json.RawMessage) (json.RawMessage, error), error) {
	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

	// Validate signature at build time
	if fnType.Kind() != reflect.Func {
		return nil, fmt.Errorf("handler must be a function")
	}
	if fnType.NumIn() != 2 || fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, fmt.Errorf("handler must have signature (context.Context, In) (Out, error)")
	}
	if fnType.NumOut() != 2 || !fnType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return nil, fmt.Errorf("handler must return (Out, error)")
	}

	// Extract types once (build time) - CACHED
	inputType := fnType.In(1)

	// Return wrapper with all type info cached in closure
	return func(ctx context.Context, inputJSON json.RawMessage) (json.RawMessage, error) {
		// RUNTIME PATH - minimal overhead:
		// 1. Allocate input value using cached type
		inputVal := reflect.New(inputType)

		// 2. Unmarshal JSON (unavoidable cost)
		if err := json.Unmarshal(inputJSON, inputVal.Interface()); err != nil {
			return nil, fmt.Errorf("unmarshal input: %w", err)
		}

		// 3. Call handler via cached reflect.Value (~0.5-1μs overhead)
		results := fnVal.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			inputVal.Elem(),
		})

		// 4. Check error
		if !results[1].IsNil() {
			return nil, results[1].Interface().(error)
		}

		// 5. Marshal output (unavoidable cost)
		return json.Marshal(results[0].Interface())
	}, nil
}

type taskMessage struct {
	ID         int64           `json:"id"`
	Deliveries int             `json:"deliveries"`
	Input      json.RawMessage `json:"input"`
}

type TaskInfo struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateTask creates a new task definition.
func CreateTask(ctx context.Context, conn Conn, task *Task) error {
	q := `SELECT * FROM cb_create_task(name => $1);`
	_, err := conn.Exec(ctx, q, task.name)
	if err != nil {
		return err
	}

	if strings.TrimSpace(task.condition) != "" {
		condQuery := `UPDATE cb_tasks SET condition = cb_parse_condition($1) WHERE name = $2;`
		if _, err := conn.Exec(ctx, condQuery, task.condition, task.name); err != nil {
			return err
		}
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

type RunOpts struct {
	ConcurrencyKey string // Prevents overlapping runs; allows reruns after completion
	IdempotencyKey string // Prevents all duplicate runs; permanent across all statuses
}

// RunTask enqueues a task execution and returns a handle for monitoring
// progress and retrieving output.
func RunTask(ctx context.Context, conn Conn, name string, input any, opts *RunOpts) (*RunHandle, error) {
	if opts == nil {
		opts = &RunOpts{}
	}

	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	q := `SELECT * FROM cb_run_task(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4);`
	var id int64
	err = conn.QueryRow(ctx, q, name, b, ptrOrNil(opts.ConcurrencyKey), ptrOrNil(opts.IdempotencyKey)).Scan(&id)
	if err != nil {
		return nil, err
	}

	return &RunHandle{conn: conn, getFn: GetTaskRun, Name: name, ID: id}, nil
}

// GetTaskRun retrieves a specific task run result by ID.
func GetTaskRun(ctx context.Context, conn Conn, name string, id int64) (*RunInfo, error) {
	tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at, skipped_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanRun(conn.QueryRow(ctx, query, id))
}

// ListTaskRuns returns recent task runs for the specified task.
func ListTaskRuns(ctx context.Context, conn Conn, name string) ([]*RunInfo, error) {
	tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(name))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at, skipped_at FROM %s ORDER BY started_at DESC LIMIT 20;`, pgx.Identifier{tableName}.Sanitize())
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
