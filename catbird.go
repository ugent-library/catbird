package catbird

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	StatusCreated   = "created"
	StatusStarted   = "started"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

var (
	DefaultPollFor      = 5 * time.Second
	DefaultPollInterval = 100 * time.Millisecond
)

type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Message struct {
	ID              int64           `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Topic           string          `json:"topic"`
	Payload         json.RawMessage `json:"payload"`
	Deliveries      int             `json:"deliveries"`
	CreatedAt       time.Time       `json:"created_at"`
	DeliverAt       time.Time       `json:"deliver_at"`
}

type Task struct {
	name     string
	queue    string
	hideFor  time.Duration
	retries  int
	delay    time.Duration
	jitter   time.Duration
	timeout  time.Duration
	schedule string
	fn       func(context.Context, []byte) ([]byte, error)
}

type Flow struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

type Step struct {
	Name      string   `json:"name"`
	TaskName  string   `json:"task_name,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
	Map       string   `json:"map,omitempty"`
}

type QueueInfo struct {
	Name     string    `json:"name"`
	Topics   []string  `json:"topics,omitempty"`
	Unlogged bool      `json:"unlogged"`
	DeleteAt time.Time `json:"delete_at,omitzero"`
}

type WorkerInfo struct {
	ID              string    `json:"id"`
	Tasks           []string  `json:"tasks"`
	StartedAt       time.Time `json:"started_at"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
}

type TaskRunInfo struct {
	ID           string          `json:"id"`
	Status       string          `json:"status"`
	Output       json.RawMessage `json:"output,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	StartedAt    time.Time       `json:"started_at,omitzero"`
	CompletedAt  time.Time       `json:"completed_at,omitzero"`
	FailedAt     time.Time       `json:"failed_at,omitzero"`
}

type QueueOpts struct {
	Topics   []string
	DeleteAt time.Time
	Unlogged bool
}

func CreateQueue(ctx context.Context, conn Conn, name string, opts QueueOpts) error {
	if opts.DeleteAt.IsZero() {
		q := `SELECT cb_create_queue(name => $1, topics => $2, unlogged => $3);`
		_, err := conn.Exec(ctx, q, name, opts.Topics, opts.Unlogged)
		return err
	} else {
		q := `SELECT cb_create_queue(name => $1, topics => $2, delete_at => $3, unlogged => $4);`
		_, err := conn.Exec(ctx, q, name, opts.Topics, opts.DeleteAt, opts.Unlogged)
		return err
	}
}

func DeleteQueue(ctx context.Context, conn Conn, name string) (bool, error) {
	q := `SELECT * FROM cb_delete_queue(name => $1);`
	existed := false
	err := conn.QueryRow(ctx, q, name).Scan(&existed)
	return existed, err
}

func ListQueues(ctx context.Context, conn Conn) ([]QueueInfo, error) {
	q := `SELECT name, topics, unlogged, delete_at FROM cb_queues;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleQueue)
}

type DispatchOpts struct {
	DeduplicationID string
	DeliverAt       time.Time
}

func Dispatch(ctx context.Context, conn Conn, topic string, payload any, opts DispatchOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		q := `SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''));`
		_, err := conn.Exec(ctx, q, topic, b, opts.DeduplicationID)
		return err
	} else {
		q := `SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`
		_, err := conn.Exec(ctx, q, topic, b, opts.DeduplicationID, opts.DeliverAt)
		return err
	}
}

type SendOpts struct {
	DeduplicationID string
	Topic           string
	DeliverAt       time.Time
}

func Send(ctx context.Context, conn Conn, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		q := `SELECT cb_send(queue => $1, payload => $2, deduplication_id => nullif($3, ''));`
		_, err := conn.Exec(ctx, q, queue, b, opts.DeduplicationID)
		return err
	} else {
		q := `SELECT cb_send(queue => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`
		_, err := conn.Exec(ctx, q, queue, b, opts.DeduplicationID, opts.DeliverAt)
		return err
	}
}

func Read(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read(queue => $1, quantity => $2, hide_for => $3);`
	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleMessage)
}

type ReadPollOpts struct {
	PollFor      time.Duration
	PollInterval time.Duration
}

func ReadPoll(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration, opts ReadPollOpts) ([]Message, error) {
	if opts.PollFor == 0 {
		opts.PollFor = DefaultPollFor
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = DefaultPollInterval
	}

	q := `SELECT * FROM cb_read_poll(
			queue => $1,
			quantity => $2,
			hide_for => $3,
			poll_for => $4,
			poll_interval => $5);`

	rows, err := conn.Query(ctx, q,
		queue,
		quantity,
		hideFor.Seconds(),
		opts.PollFor.Seconds(),
		opts.PollInterval.Milliseconds(),
	)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, scanCollectibleMessage)
}

func Hide(ctx context.Context, conn Conn, queue string, id int64, hideFor time.Duration) (bool, error) {
	q := `SELECT * FROM cb_hide(queue => $1, id => $2, hide_for => $3);`
	exists := false
	err := conn.QueryRow(ctx, q, queue, id, hideFor.Seconds()).Scan(&exists)
	return exists, err
}

func HideMany(ctx context.Context, conn Conn, queue string, ids []int64, hideFor time.Duration) error {
	q := `SELECT * FROM cb_hide_many(queue => $1, ids => $2, hide_for => $3);`
	_, err := conn.Exec(ctx, q, queue, ids, hideFor.Seconds())
	return err
}

func Delete(ctx context.Context, conn Conn, queue string, id int64) (bool, error) {
	q := `SELECT * FROM cb_delete(queue => $1, id => $2);`
	existed := false
	err := conn.QueryRow(ctx, q, queue, id).Scan(&existed)
	return existed, err
}

func DeleteMany(ctx context.Context, conn Conn, queue string, ids []int64) error {
	q := `SELECT * FROM cb_delete(queue => $1, ids => $2);`
	_, err := conn.Exec(ctx, q, queue, ids)
	return err
}

type TaskOpts struct {
	HideFor  time.Duration
	Retries  int
	Delay    time.Duration
	Jitter   time.Duration
	Timeout  time.Duration
	Schedule string
}

func NewTask[Input, Output any](name string, fn func(context.Context, Input) (Output, error), opts TaskOpts) *Task {
	return &Task{
		name:     name,
		queue:    "t_" + name,
		hideFor:  opts.HideFor,
		retries:  opts.Retries,
		delay:    opts.Delay,
		jitter:   opts.Jitter,
		timeout:  opts.Timeout,
		schedule: opts.Schedule,
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

type RunTaskOpts struct {
	DeduplicationID string
}

func RunTask(ctx context.Context, conn Conn, name string, input any, opts RunTaskOpts) (string, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	q := `SELECT * FROM cb_run_task(name => $1, input => $2, deduplication_id => nullif($3, ''));`
	var runID string
	err = conn.QueryRow(ctx, q, name, b, opts.DeduplicationID).Scan(&runID)
	if err != nil {
		return "", err
	}
	return runID, err
}

func GetTaskRun(ctx context.Context, conn Conn, id string) (*TaskRunInfo, error) {
	q := `
		SELECT id, status, output, error_message, started_at, completed_at, failed_at
		FROM cb_task_runs
		WHERE id = $1;`
	return scanTaskRun(conn.QueryRow(ctx, q, id))
}

func RunTaskWait(ctx context.Context, conn Conn, name string, input any, opts RunTaskOpts) (*TaskRunInfo, error) {
	id, err := RunTask(ctx, conn, name, input, opts)
	if err != nil {
		return nil, err
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			info, err := GetTaskRun(ctx, conn, id)
			if err != nil {
				return nil, err
			}
			if info.Status == StatusStarted {
				continue
			}
			return info, nil
		}
	}
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

func ListWorkers(ctx context.Context, conn Conn) ([]WorkerInfo, error) {
	q := `
		SELECT w.id, array_agg(w_t.task_name) AS tasks, w.started_at, w.last_heartbeat_at
		FROM cb_workers w
		JOIN cb_worker_tasks w_t ON w_t.worker_id = w.id
		GROUP BY w.id
		ORDER BY w.started_at DESC;`

	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleWorker)

}

func GC(ctx context.Context, conn Conn) error {
	q := `SELECT cb_gc();`
	_, err := conn.Exec(ctx, q)
	return err
}

func GCTask(conn Conn) *Task {
	return NewTask("gc", func(ctx context.Context, input struct{}) (struct{}, error) {
		return struct{}{}, GC(ctx, conn)
	}, TaskOpts{
		HideFor:  10 * time.Second,
		Schedule: "@every 10m",
	})
}

func EnqueueDispatch(batch *pgx.Batch, topic string, payload any, opts DispatchOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		batch.Queue(
			`SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''));`,
			topic, b, opts.DeduplicationID,
		)
	} else {
		batch.Queue(
			`SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`,
			topic, b, opts.DeduplicationID, opts.DeliverAt,
		)
	}

	return nil
}

func EnqueueSend(batch *pgx.Batch, queue string, payload any, opts SendOpts) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if opts.DeliverAt.IsZero() {
		batch.Queue(
			`SELECT cb_send(topic => $1, payload => $2, deduplication_id => nullif($3, ''));`,
			queue, b, opts.DeduplicationID,
		)
	} else {
		batch.Queue(
			`SELECT cb_send(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`,
			queue, b, opts.DeduplicationID, opts.DeliverAt,
		)
	}

	return nil
}

func scanCollectibleMessage(row pgx.CollectableRow) (Message, error) {
	return scanMessage(row)
}

func scanMessage(row pgx.Row) (Message, error) {
	rec := Message{}

	var deduplicationID *string
	var topic *string

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&topic,
		&rec.Payload,
		&rec.Deliveries,
		&rec.CreatedAt,
		&rec.DeliverAt,
	); err != nil {
		return rec, err
	}

	if topic != nil {
		rec.Topic = *topic
	}
	if deduplicationID != nil {
		rec.DeduplicationID = *deduplicationID
	}

	return rec, nil
}

func scanCollectibleQueue(row pgx.CollectableRow) (QueueInfo, error) {
	return scanQueue(row)
}

func scanQueue(row pgx.Row) (QueueInfo, error) {
	rec := QueueInfo{}

	var deleteAt *time.Time

	if err := row.Scan(
		&rec.Name,
		&rec.Topics,
		&rec.Unlogged,
		&deleteAt,
	); err != nil {
		return rec, err
	}

	if deleteAt != nil {
		rec.DeleteAt = *deleteAt
	}

	return rec, nil
}

func scanCollectibleWorker(row pgx.CollectableRow) (WorkerInfo, error) {
	return scanWorker(row)
}

func scanWorker(row pgx.Row) (WorkerInfo, error) {
	rec := WorkerInfo{}

	if err := row.Scan(
		&rec.ID,
		&rec.Tasks,
		&rec.StartedAt,
		&rec.LastHeartbeatAt,
	); err != nil {
		return rec, err
	}

	return rec, nil
}

func scanTaskRun(row pgx.Row) (*TaskRunInfo, error) {
	rec := TaskRunInfo{}

	var output *json.RawMessage
	var errorMessage *string
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&rec.Status,
		&output,
		&errorMessage,
		&rec.StartedAt,
		&completedAt,
		&failedAt,
	); err != nil {
		return nil, err
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

	return &rec, nil
}
