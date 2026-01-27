package catbird

import (
	"context"
	"encoding/json"
	"log/slog"
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

type FlowOpt interface {
	applyToFlow(*Flow)
}

type WorkerOpt interface {
	applyToWorker(*Worker)
}

type stepOpt struct {
	step *Step
}

func WithStep(name string) *stepOpt {
	return &stepOpt{step: &Step{Name: name}}
}

func (o *stepOpt) DependsOn(steps ...string) *stepOpt {
	o.step.DependsOn = append(o.step.DependsOn, steps...)
	return o
}

func (o *stepOpt) applyToFlow(f *Flow) {
	f.Steps = append(f.Steps, o.step)
}

type loggerOpt struct {
	logger *slog.Logger
}

func (o loggerOpt) applyToWorker(w *Worker) {
	w.logger = o.logger
}

func WithLogger(logger *slog.Logger) *loggerOpt {
	return &loggerOpt{logger: logger}
}

type timeoutOpt struct {
	timeout time.Duration
}

func (o timeoutOpt) applyToWorker(w *Worker) {
	w.timeout = o.timeout
}

func WithTimeout(timeout time.Duration) *timeoutOpt {
	return &timeoutOpt{timeout: timeout}
}

type gcOpt struct {
	schedule string
}

func (o *gcOpt) Schedule(schedule string) *gcOpt {
	o.schedule = schedule
	return o
}

func (o gcOpt) applyToWorker(w *Worker) {
	w.handlers = append(w.handlers, NewTaskHandler("gc", func(ctx context.Context, input struct{}) (struct{}, error) {
		return struct{}{}, GC(ctx, w.conn)
	}, TaskHandlerOpts{
		Schedule: o.schedule,
	}))
}

func WithGC() *gcOpt {
	return &gcOpt{schedule: "@every 10m"}
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

type Flow struct {
	Name  string  `json:"name"`
	Steps []*Step `json:"steps"`
}

type Step struct {
	Name      string   `json:"name"`
	DependsOn []string `json:"depends_on,omitempty"`
}

func NewFlow(name string, opts ...FlowOpt) *Flow {
	f := &Flow{
		Name: name,
	}
	for _, opt := range opts {
		opt.applyToFlow(f)
	}
	return f
}

type QueueInfo struct {
	Name      string    `json:"name"`
	Topics    []string  `json:"topics,omitempty"`
	Unlogged  bool      `json:"unlogged"`
	CreatedAt time.Time `json:"created_at"`
	DeleteAt  time.Time `json:"delete_at,omitzero"`
}

type TaskInfo struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type TaskRunInfo struct {
	ID              string          `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Status          string          `json:"status"`
	Output          json.RawMessage `json:"output,omitempty"`
	ErrorMessage    string          `json:"error_message,omitempty"`
	StartedAt       time.Time       `json:"started_at,omitzero"`
	CompletedAt     time.Time       `json:"completed_at,omitzero"`
	FailedAt        time.Time       `json:"failed_at,omitzero"`
}

func (info *TaskRunInfo) OutputAs(o any) error {
	return json.Unmarshal(info.Output, o)
}

type FlowInfo struct {
	Name      string    `json:"name"`
	Steps     []Step    `json:"steps"`
	CreatedAt time.Time `json:"created_at"`
}

type FlowRunInfo struct {
	ID              string          `json:"id"`
	DeduplicationID string          `json:"deduplication_id,omitempty"`
	Status          string          `json:"status"`
	Input           json.RawMessage `json:"input,omitempty"`
	Output          json.RawMessage `json:"output,omitempty"`
	StartedAt       time.Time       `json:"started_at,omitzero"`
	CompletedAt     time.Time       `json:"completed_at,omitzero"`
	FailedAt        time.Time       `json:"failed_at,omitzero"`
}

func (info *FlowRunInfo) OutputAs(o any) error {
	return json.Unmarshal(info.Output, o)
}

type WorkerInfo struct {
	ID              string             `json:"id"`
	TaskHandlers    []*TaskhandlerInfo `json:"task_handlers"`
	StepHandlers    []*StephandlerInfo `json:"step_handlers"`
	StartedAt       time.Time          `json:"started_at"`
	LastHeartbeatAt time.Time          `json:"last_heartbeat_at"`
}

type TaskhandlerInfo struct {
	TaskName string `json:"task_name"`
}

type StephandlerInfo struct {
	FlowName string `json:"flow_name"`
	StepName string `json:"step_name"`
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

func GetQueue(ctx context.Context, conn Conn, name string) (*QueueInfo, error) {
	q := `SELECT name, topics, unlogged, created_at, delete_at FROM cb_queues WHERE name = $1;`
	return scanQueue(conn.QueryRow(ctx, q, name))
}

func DeleteQueue(ctx context.Context, conn Conn, name string) (bool, error) {
	q := `SELECT * FROM cb_delete_queue(name => $1);`
	existed := false
	err := conn.QueryRow(ctx, q, name).Scan(&existed)
	return existed, err
}

func ListQueues(ctx context.Context, conn Conn) ([]*QueueInfo, error) {
	q := `SELECT name, topics, unlogged, created_at, delete_at FROM cb_queues;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleQueue)
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

func CreateTask(ctx context.Context, conn Conn, name string) error {
	q := `SELECT * FROM cb_create_task(name => $1);`
	_, err := conn.Exec(ctx, q, name)
	if err != nil {
		return err
	}
	return nil
}

func GetTask(ctx context.Context, conn Conn, name string) (*TaskInfo, error) {
	q := `SELECT name, created_at FROM cb_tasks WHERE name = $1;`
	return scanTask(conn.QueryRow(ctx, q, name))
}

func ListTasks(ctx context.Context, conn Conn) ([]*TaskInfo, error) {
	q := `SELECT name, created_at FROM cb_tasks;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleTask)
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

func GetTaskRun(ctx context.Context, conn Conn, id string) (*TaskRunInfo, error) {
	q := `
		SELECT id, deduplication_id, status, output, error_message, started_at, completed_at, failed_at
		FROM cb_task_runs
		WHERE id = $1;`
	return scanTaskRun(conn.QueryRow(ctx, q, id))
}

func ListTaskRuns(ctx context.Context, conn Conn, taskName string) ([]*TaskRunInfo, error) {
	q := `
		SELECT id, deduplication_id, status, output, error_message, started_at, completed_at, failed_at
		FROM cb_task_runs
		WHERE task_name = $1
		ORDER BY started_at DESC
		LIMIT 20;`
	rows, err := conn.Query(ctx, q, taskName)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleTaskRun)
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

func GetFlow(ctx context.Context, conn Conn, name string) (*FlowInfo, error) {
	q := `
		SELECT f.name, s.steps AS steps, f.created_at
		FROM cb_flows f
		LEFT JOIN LATERAL (
			SELECT s.flow_name,
				jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
					'name', s.name,
					'depends_on', (
						SELECT jsonb_agg(s_d.dependency_name)
						FROM cb_step_dependencies AS s_d
						WHERE s_d.flow_name = s.flow_name
						AND s_d.step_name = s.name
					)
				)) ORDER BY s.idx) FILTER (WHERE s.idx IS NOT NULL) AS steps
			FROM cb_steps s
			WHERE s.flow_name = f.name
			GROUP BY flow_name
		) s ON s.flow_name = f.name
		WHERE f.name = $1;`
	return scanFlow(conn.QueryRow(ctx, q, name))
}

func ListFlows(ctx context.Context, conn Conn) ([]*FlowInfo, error) {
	q := `
		SELECT f.name, s.steps AS steps, f.created_at
		FROM cb_flows f
		LEFT JOIN LATERAL (
			SELECT s.flow_name,
				jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
					'name', s.name,
					'depends_on', (
						SELECT jsonb_agg(s_d.dependency_name)
						FROM cb_step_dependencies AS s_d
						WHERE s_d.flow_name = s.flow_name
						AND s_d.step_name = s.name
					)
				)) ORDER BY s.idx) FILTER (WHERE s.idx IS NOT NULL) AS steps
			FROM cb_steps s
			WHERE s.flow_name = f.name
			GROUP BY flow_name
		) s ON s.flow_name = f.name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleFlow)
}

type RunFlowOpts struct {
	DeduplicationID string
}

func RunFlow(ctx context.Context, conn Conn, name string, input any, opts RunFlowOpts) (string, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	q := `SELECT * FROM cb_run_flow(name => $1, input => $2, deduplication_id => nullif($3, ''));`
	var runID string
	err = conn.QueryRow(ctx, q, name, b, opts.DeduplicationID).Scan(&runID)
	if err != nil {
		return "", err
	}
	return runID, err
}

func RunFlowWait(ctx context.Context, conn Conn, name string, input any, opts RunFlowOpts) (*FlowRunInfo, error) {
	id, err := RunFlow(ctx, conn, name, input, opts)
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
			info, err := GetFlowRun(ctx, conn, id)
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

func GetFlowRun(ctx context.Context, conn Conn, id string) (*FlowRunInfo, error) {
	q := `
		SELECT id, deduplication_id, status, input, output, started_at, completed_at, failed_at
		FROM cb_flow_runs
		WHERE id = $1;`
	return scanFlowRun(conn.QueryRow(ctx, q, id))
}

func ListFlowRuns(ctx context.Context, conn Conn, flowName string) ([]*FlowRunInfo, error) {
	q := `
		SELECT id, deduplication_id, status, input, output, started_at, completed_at, failed_at
		FROM cb_flow_runs
		WHERE flow_name = $1
		ORDER BY started_at DESC
		LIMIT 20;`
	rows, err := conn.Query(ctx, q, flowName)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleFlowRun)
}

func ListWorkers(ctx context.Context, conn Conn) ([]*WorkerInfo, error) {
	q := `
		SELECT w.id, w.started_at, w.last_heartbeat_at, t.task_handlers AS task_handlers, s.step_handlers AS step_handlers
		FROM cb_workers w
		LEFT JOIN LATERAL (
			SELECT t.worker_id,
   			       json_agg(json_build_object('task_name', t.task_name) ORDER BY t.task_name) FILTER (WHERE t.worker_id IS NOT NULL) AS task_handlers
			FROM cb_task_handlers t
			WHERE t.worker_id = w.id
			GROUP BY worker_id
		) t ON t.worker_id = w.id
		LEFT JOIN LATERAL (
			SELECT s.worker_id,
   			       json_agg(json_build_object('flow_name', s.flow_name, 'step_name', s.step_name) ORDER BY s.flow_name, s.step_name) FILTER (WHERE s.worker_id IS NOT NULL) AS step_handlers
			FROM cb_step_handlers s
			WHERE s.worker_id = w.id
			GROUP BY worker_id
		) s ON s.worker_id = w.id
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

func scanCollectibleQueue(row pgx.CollectableRow) (*QueueInfo, error) {
	return scanQueue(row)
}

func scanQueue(row pgx.Row) (*QueueInfo, error) {
	rec := QueueInfo{}

	var deleteAt *time.Time

	if err := row.Scan(
		&rec.Name,
		&rec.Topics,
		&rec.Unlogged,
		&rec.CreatedAt,
		&deleteAt,
	); err != nil {
		return nil, err
	}

	if deleteAt != nil {
		rec.DeleteAt = *deleteAt
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

func scanCollectibleTaskRun(row pgx.CollectableRow) (*TaskRunInfo, error) {
	return scanTaskRun(row)
}

func scanTaskRun(row pgx.Row) (*TaskRunInfo, error) {
	rec := TaskRunInfo{}

	var deduplicationID *string
	var output *json.RawMessage
	var errorMessage *string
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&rec.Status,
		&output,
		&errorMessage,
		&rec.StartedAt,
		&completedAt,
		&failedAt,
	); err != nil {
		return nil, err
	}

	if deduplicationID != nil {
		rec.DeduplicationID = *deduplicationID
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

func scanCollectibleFlow(row pgx.CollectableRow) (*FlowInfo, error) {
	return scanFlow(row)
}

func scanFlow(row pgx.Row) (*FlowInfo, error) {
	rec := FlowInfo{}

	var steps json.RawMessage

	if err := row.Scan(
		&rec.Name,
		&steps,
		&rec.CreatedAt,
	); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(steps, &rec.Steps); err != nil {
		return nil, err
	}

	return &rec, nil
}

func scanCollectibleFlowRun(row pgx.CollectableRow) (*FlowRunInfo, error) {
	return scanFlowRun(row)
}

func scanFlowRun(row pgx.Row) (*FlowRunInfo, error) {
	rec := FlowRunInfo{}

	var deduplicationID *string
	var output *json.RawMessage
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&deduplicationID,
		&rec.Status,
		&rec.Input,
		&output,
		&rec.StartedAt,
		&completedAt,
		&failedAt,
	); err != nil {
		return nil, err
	}

	if deduplicationID != nil {
		rec.DeduplicationID = *deduplicationID
	}
	if output != nil {
		rec.Output = *output
	}
	if completedAt != nil {
		rec.CompletedAt = *completedAt
	}
	if failedAt != nil {
		rec.FailedAt = *failedAt
	}

	return &rec, nil
}

func scanCollectibleWorker(row pgx.CollectableRow) (*WorkerInfo, error) {
	return scanWorker(row)
}

func scanWorker(row pgx.Row) (*WorkerInfo, error) {
	rec := WorkerInfo{}

	var taskHandlers json.RawMessage
	var stepHandlers json.RawMessage

	if err := row.Scan(
		&rec.ID,
		&rec.StartedAt,
		&rec.LastHeartbeatAt,
		&taskHandlers,
		&stepHandlers,
	); err != nil {
		return nil, err
	}

	if taskHandlers != nil {
		if err := json.Unmarshal(taskHandlers, &rec.TaskHandlers); err != nil {
			return nil, err
		}
	}
	if stepHandlers != nil {
		if err := json.Unmarshal(stepHandlers, &rec.StepHandlers); err != nil {
			return nil, err
		}
	}

	return &rec, nil
}
