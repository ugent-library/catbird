package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type Task interface {
	Name() string
	// handle executes the task handler with JSON input, returns JSON output
	// This is unexported (lowercase) so only the catbird package can call it
	handle(ctx context.Context, inputJSON []byte) (outputJSON []byte, err error)
	// handlerOpts returns the handler execution options (concurrency, retries, etc.)
	// Returns nil for definition-only tasks (no handler)
	handlerOpts() *handlerOpts
	// condition returns the task condition expression (empty if none)
	condition() string
}

type DefineTaskOpts struct {
	Condition string
}

type TaskOpts struct {
	Condition      string
	Concurrency    int
	BatchSize      int
	Timeout        time.Duration
	MaxRetries     int
	MinDelay       time.Duration
	MaxDelay       time.Duration
	CircuitBreaker *CircuitBreaker
}

// TypedTask is a task with compile-time type safety
type TypedTask[In, Out any] struct {
	name    string
	handler func(context.Context, In) (Out, error)
	opts    *handlerOpts
	config  *DefineTaskOpts
}

// Name returns the task name (implements TaskInterface)
func (t *TypedTask[In, Out]) Name() string {
	return t.name
}

// handlerOpts returns the handler execution options (implements TaskInterface)
func (t *TypedTask[In, Out]) handlerOpts() *handlerOpts {
	if t.handler == nil {
		return nil // Definition-only task
	}
	return t.opts
}

func (t *TypedTask[In, Out]) condition() string {
	if t.config == nil {
		return ""
	}
	return t.config.Condition
}

// handle executes the task handler (implements TaskInterface)
// This is the worker's entry point for executing tasks
func (t *TypedTask[In, Out]) handle(ctx context.Context, inputJSON []byte) ([]byte, error) {
	if t.handler == nil {
		return nil, fmt.Errorf("task %s has no handler (definition-only)", t.name)
	}

	// 1. Unmarshal input (standard library JSON)
	var in In
	if err := json.Unmarshal(inputJSON, &in); err != nil {
		return nil, fmt.Errorf("unmarshal input: %w", err)
	}

	// 2. Execute handler (direct function call, zero reflection overhead)
	out, err := t.handler(ctx, in)
	if err != nil {
		return nil, err
	}

	// 3. Marshal output (standard library JSON)
	outputJSON, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal output: %w", err)
	}

	return outputJSON, nil
}

// Run executes the task with typed input, returns typed handle
func (t *TypedTask[In, Out]) Run(ctx context.Context, client *Client, input In, opts *RunOpts) (*TypedHandle[Out], error) {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	if opts == nil {
		opts = &RunOpts{}
	}

	// Use the existing RunTask SQL function
	q := `SELECT * FROM cb_run_task(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4);`
	var id int64
	err = client.Conn.QueryRow(ctx, q, t.name, inputJSON, ptrOrNil(opts.ConcurrencyKey), ptrOrNil(opts.IdempotencyKey)).Scan(&id)
	if err != nil {
		return nil, err
	}

	return &TypedHandle[Out]{
		conn:  client.Conn,
		name:  t.name,
		id:    id,
		getFn: GetTaskRun,
	}, nil
}

// NewTask creates a task with handler (full definition)
func NewTask[In, Out any](
	name string,
	handler func(context.Context, In) (Out, error),
	opts *TaskOpts,
) *TypedTask[In, Out] {
	t := DefineTaskOpts{}
	h := handlerOpts{
		concurrency: 1,
		batchSize:   10,
	}

	if opts != nil {
		t.Condition = opts.Condition
		h.concurrency = opts.Concurrency
		h.batchSize = opts.BatchSize
		h.timeout = opts.Timeout
		h.maxRetries = opts.MaxRetries
		h.minDelay = opts.MinDelay
		h.maxDelay = opts.MaxDelay
		h.circuitBreaker = opts.CircuitBreaker
		if h.concurrency == 0 {
			h.concurrency = 1
		}
		if h.batchSize == 0 {
			h.batchSize = 10
		}
	}

	if err := h.validate(); err != nil {
		panic(fmt.Errorf("invalid handler options for task %s: %v", name, err))
	}

	return &TypedTask[In, Out]{
		name:    name,
		handler: handler,
		opts:    &h,
		config:  &t,
	}
}

// NewTaskDefinition creates a task without handler (definition-only)
// Useful when handler is registered elsewhere (different worker/language)
func NewTaskDefinition[In, Out any](name string, opts *DefineTaskOpts) *TypedTask[In, Out] {
	t := DefineTaskOpts{}
	if opts != nil {
		t = *opts
	}

	return &TypedTask[In, Out]{
		name:    name,
		handler: nil,
		config:  &t,
	}
}

// TypedHandle is a handle to a task or flow execution with typed output
type TypedHandle[Out any] struct {
	conn  Conn
	name  string
	id    int64
	getFn func(context.Context, Conn, string, int64) (*RunInfo, error)
}

// ID returns the run ID
func (h *TypedHandle[Out]) ID() int64 {
	return h.id
}

// WaitForOutput blocks until the task/flow completes and returns typed output
func (h *TypedHandle[Out]) WaitForOutput(ctx context.Context) (Out, error) {
	var zero Out

	// Wait for completion using existing polling logic
	oldHandle := &RunHandle{
		conn:  h.conn,
		getFn: h.getFn,
		Name:  h.name,
		ID:    h.id,
	}

	var out Out
	if err := oldHandle.WaitForOutput(ctx, &out); err != nil {
		return zero, err
	}

	return out, nil
}

// Wait blocks until the task/flow completes or fails
func (h *TypedHandle[Out]) Wait(ctx context.Context) error {
	oldHandle := &RunHandle{
		conn:  h.conn,
		getFn: h.getFn,
		Name:  h.name,
		ID:    h.id,
	}

	// Use a dummy variable to wait
	var dummy any
	return oldHandle.WaitForOutput(ctx, &dummy)
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
func CreateTask(ctx context.Context, conn Conn, task Task) error {
	q := `SELECT * FROM cb_create_task(name => $1);`
	_, err := conn.Exec(ctx, q, task.Name())
	if err != nil {
		return err
	}

	if strings.TrimSpace(task.condition()) != "" {
		condQuery := `UPDATE cb_tasks SET condition = cb_parse_condition($1) WHERE name = $2;`
		if _, err := conn.Exec(ctx, condQuery, task.condition(), task.Name()); err != nil {
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
