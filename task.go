package catbird

import (
	"context"
	"encoding/json"
	"errors"
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
	name        string
	condition   string
	handler     func(context.Context, json.RawMessage) (json.RawMessage, error)
	handlerOpts *HandlerOpts
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

// Handler sets the task handler function and execution options.
// fn must have signature (context.Context, In) (Out, error).
// If opts is omitted, defaults are used (concurrency: 1, batchSize: 10).
func (t *Task) Handler(fn any, opts ...HandlerOpts) *Task {
	handler, err := makeTaskHandler(fn, t.name)
	if err != nil {
		panic(err)
	}
	t.handler = handler
	t.handlerOpts = applyDefaultHandlerOpts(opts...)
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

type taskClaim struct {
	ID       int64           `json:"id"`
	Attempts int             `json:"attempts"`
	Input    json.RawMessage `json:"input"`
}

type TaskInfo struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// TaskScheduleInfo contains metadata about a scheduled task.
type TaskScheduleInfo struct {
	TaskName       string    `json:"task_name"`
	CronSpec       string    `json:"cron_spec"`
	NextRunAt      time.Time `json:"next_run_at"`
	LastRunAt      time.Time `json:"last_run_at,omitzero"`
	LastEnqueuedAt time.Time `json:"last_enqueued_at,omitzero"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// TaskRunInfo represents the details of a task execution.
type TaskRunInfo struct {
	ID             int64           `json:"id"`
	ConcurrencyKey string          `json:"concurrency_key,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Status         string          `json:"status"`
	Input          json.RawMessage `json:"input,omitempty"`
	Output         json.RawMessage `json:"output,omitempty"`
	ErrorMessage   string          `json:"error_message,omitempty"`
	StartedAt      time.Time       `json:"started_at,omitzero"`
	CompletedAt    time.Time       `json:"completed_at,omitzero"`
	FailedAt       time.Time       `json:"failed_at,omitzero"`
	SkippedAt      time.Time       `json:"skipped_at,omitzero"`
}

// OutputAs unmarshals the output of a completed task run.
// Returns an error if the task run has failed or is not completed yet.
func (r *TaskRunInfo) OutputAs(out any) error {
	if r.Status == "failed" {
		return fmt.Errorf("%w: %s", ErrRunFailed, r.ErrorMessage)
	}
	if r.Status != "completed" {
		return fmt.Errorf("run not completed: current status is %s", r.Status)
	}
	return json.Unmarshal(r.Output, out)
}

// TaskHandle is a handle to a task execution.
type TaskHandle struct {
	conn Conn
	Name string
	ID   int64
}

// WaitForOutput blocks until the task execution completes and unmarshals the output.
// Pass optional WaitOpts to customize polling behavior; defaults are used when omitted.
func (h *TaskHandle) WaitForOutput(ctx context.Context, out any, opts ...WaitOpts) error {
	var pollFor time.Duration
	var pollInterval time.Duration

	if len(opts) > 0 {
		pollFor = opts[0].PollFor
		pollInterval = opts[0].PollInterval
	}

	pollForMs, pollIntervalMs := resolvePollDurations(defaultPollFor, defaultPollInterval, pollFor, pollInterval)

	q := `
		SELECT status, output, error_message
		FROM cb_wait_task_output(task_name => $1, run_id => $2, poll_for => $3, poll_interval => $4);
	`

	for {
		var status string
		var output json.RawMessage
		var errorMessage *string

		err := h.conn.QueryRow(ctx, q, h.Name, h.ID, pollForMs, pollIntervalMs).Scan(&status, &output, &errorMessage)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			return err
		}

		switch status {
		case "completed":
			return json.Unmarshal(output, out)
		case "failed":
			if errorMessage != nil {
				return fmt.Errorf("%w: %s", ErrRunFailed, *errorMessage)
			}
			return ErrRunFailed
		case "skipped":
			return fmt.Errorf("run skipped: condition not met")
		}
	}
}

// CreateTask creates one or more task definitions.
func CreateTask(ctx context.Context, conn Conn, tasks ...*Task) error {
	q := `SELECT * FROM cb_create_task(name => $1);`
	for _, task := range tasks {
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
	}

	return nil
}

// GetTask retrieves task metadata by name.
func GetTask(ctx context.Context, conn Conn, taskName string) (*TaskInfo, error) {
	q := `SELECT name, created_at FROM cb_tasks WHERE name = $1;`
	return scanTask(conn.QueryRow(ctx, q, taskName))
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
	ConcurrencyKey string // Prevents overlapping runs; allows reruns after completion
	IdempotencyKey string // Prevents all duplicate runs; permanent across all statuses
	VisibleAt      time.Time
}

// RunTask enqueues a task execution and returns a handle for monitoring
// progress and retrieving output.
func RunTask(ctx context.Context, conn Conn, taskName string, input any, opts ...RunTaskOpts) (*TaskHandle, error) {
	q, args, err := RunTaskQuery(taskName, input, opts...)
	if err != nil {
		return nil, err
	}

	var id int64
	err = conn.QueryRow(ctx, q, args...).Scan(&id)
	if err != nil {
		return nil, err
	}

	return &TaskHandle{conn: conn, Name: taskName, ID: id}, nil
}

// RunTaskQuery builds the SQL query and args for a RunTask operation.
// Pass no opts to use defaults.
func RunTaskQuery(taskName string, input any, opts ...RunTaskOpts) (string, []any, error) {
	var resolved RunTaskOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	b, err := json.Marshal(input)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT * FROM cb_run_task(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4, visible_at => $5);`
	args := []any{taskName, b, ptrOrNil(resolved.ConcurrencyKey), ptrOrNil(resolved.IdempotencyKey), ptrOrNil(resolved.VisibleAt)}

	return q, args, nil
}

// GetTaskRun retrieves a specific task run result by ID.
func GetTaskRun(ctx context.Context, conn Conn, taskName string, taskRunID int64) (*TaskRunInfo, error) {
	tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(taskName))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at, skipped_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanTaskRun(conn.QueryRow(ctx, query, taskRunID))
}

// ListTaskRuns returns recent task runs for the specified task.
func ListTaskRuns(ctx context.Context, conn Conn, taskName string) ([]*TaskRunInfo, error) {
	tableName := fmt.Sprintf("cb_t_%s", strings.ToLower(taskName))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, output, error_message, started_at, completed_at, failed_at, skipped_at FROM %s ORDER BY started_at DESC LIMIT 20;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleTaskRun)
}

func scanCollectibleTaskRun(row pgx.CollectableRow) (*TaskRunInfo, error) {
	return scanTaskRun(row)
}

func scanTaskRun(row pgx.Row) (*TaskRunInfo, error) {
	rec := TaskRunInfo{}

	var concurrencyKey *string
	var idempotencyKey *string
	var input *json.RawMessage
	var output *json.RawMessage
	var errorMessage *string
	var completedAt *time.Time
	var failedAt *time.Time
	var skippedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&concurrencyKey,
		&idempotencyKey,
		&rec.Status,
		&input,
		&output,
		&errorMessage,
		&rec.StartedAt,
		&completedAt,
		&failedAt,
		&skippedAt,
	); err != nil {
		return nil, err
	}

	if concurrencyKey != nil {
		rec.ConcurrencyKey = *concurrencyKey
	}
	if idempotencyKey != nil {
		rec.IdempotencyKey = *idempotencyKey
	}
	if input != nil {
		rec.Input = *input
	}
	if output != nil {
		rec.Output = *output
	}
	if errorMessage != nil {
		rec.ErrorMessage = *errorMessage
	}
	if completedAt != nil {
		rec.CompletedAt = *completedAt
	}
	if failedAt != nil {
		rec.FailedAt = *failedAt
	}
	if skippedAt != nil {
		rec.SkippedAt = *skippedAt
	}

	return &rec, nil
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

// CreateTaskSchedule creates a cron-based schedule for a task.
func CreateTaskSchedule(ctx context.Context, conn Conn, taskName, cronSpec string, opts ...ScheduleOpts) error {
	var inputJSON []byte
	var err error

	var resolved ScheduleOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	if resolved.Input == nil {
		inputJSON = []byte("{}")
	} else {
		inputJSON, err = json.Marshal(resolved.Input)
		if err != nil {
			return fmt.Errorf("failed to marshal task schedule input: %w", err)
		}
	}

	_, err = conn.Exec(ctx, `SELECT cb_create_task_schedule($1, $2, $3);`, taskName, cronSpec, inputJSON)
	if err != nil {
		return fmt.Errorf("failed to create task schedule %q: %w", taskName, err)
	}
	return nil
}

// ListTaskSchedules returns all task schedules ordered by next_run_at.
func ListTaskSchedules(ctx context.Context, conn Conn) ([]*TaskScheduleInfo, error) {
	q := `SELECT task_name, cron_spec, next_run_at, last_run_at, last_enqueued_at, enabled, created_at, updated_at
		FROM cb_task_schedules
		ORDER BY next_run_at ASC;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*TaskScheduleInfo, error) {
		var s TaskScheduleInfo
		var lastRunAt *time.Time
		var lastEnqueuedAt *time.Time
		err := row.Scan(&s.TaskName, &s.CronSpec, &s.NextRunAt, &lastRunAt, &lastEnqueuedAt, &s.Enabled, &s.CreatedAt, &s.UpdatedAt)
		if lastRunAt != nil {
			s.LastRunAt = *lastRunAt
		}
		if lastEnqueuedAt != nil {
			s.LastEnqueuedAt = *lastEnqueuedAt
		}
		return &s, err
	})
}
