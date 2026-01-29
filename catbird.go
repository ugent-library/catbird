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
	DefaultPollDuration = 5 * time.Second
	DefaultPollInterval = 100 * time.Millisecond
)

type Conn interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type queueOpts struct {
	topics   []string
	deleteAt *time.Time
	unlogged bool
}

type QueueOpt interface {
	applyToQueueOpts(*queueOpts)
}

type dispatchOpts struct {
	deliverAt       *time.Time
	deduplicationID string
}

type DispatchOpt interface {
	applyToDispatchOpts(*dispatchOpts)
}

type readPollOpts struct {
	duration time.Duration
	interval time.Duration
}

type ReadPollOpt interface {
	applyToReadPollOpts(*readPollOpts)
}

type sendOpts struct {
	topic           string
	deliverAt       *time.Time
	deduplicationID string
}

type SendOpt interface {
	applyToSendOpts(*sendOpts)
}

type FlowOpt interface {
	applyToFlow(*Flow)
}

type WorkerOpt interface {
	applyToWorker(*Worker)
}

type HandlerOpt interface {
	applyToHandler(*Handler)
}

type runTaskOpts struct {
	deduplicationID string
}

type RunTaskOpt interface {
	applyToRunTaskOpts(*runTaskOpts)
}

type runFlowOpts struct {
	deduplicationID string
}

type RunFlowOpt interface {
	applyToRunFlowOpts(*runFlowOpts)
}

type durationOpt struct {
	duration time.Duration
}

func WithDuration(d time.Duration) *durationOpt {
	return &durationOpt{duration: d}
}

func (o durationOpt) applyToReadPollOpts(opts *readPollOpts) {
	opts.duration = o.duration
}

type intervalOpt struct {
	interval time.Duration
}

func WithInterval(d time.Duration) *intervalOpt {
	return &intervalOpt{interval: d}
}

func (o intervalOpt) applyToReadPollOpts(opts *readPollOpts) {
	opts.interval = o.interval
}

type topicOpt struct {
	topic string
}

func WithTopic(topic string) *topicOpt {
	return &topicOpt{topic: topic}
}

func (o topicOpt) applyToSendOpts(opts *sendOpts) {
	opts.topic = o.topic
}

type topicsOpt struct {
	topics []string
}

func WithTopics(topics ...string) *topicsOpt {
	return &topicsOpt{topics: topics}
}

func (o topicsOpt) applyToQueueOpts(opts *queueOpts) {
	opts.topics = append(opts.topics, o.topics...)
}

type unloggedOpt struct {
	unlogged bool
}

func WithUnlogged(unlogged bool) *unloggedOpt {
	return &unloggedOpt{unlogged: unlogged}
}

func (o unloggedOpt) applyToQueueOpts(opts *queueOpts) {
	opts.unlogged = o.unlogged
}

type deleteAtOpt struct {
	deleteAt time.Time
}

func WithDeleteAt(t time.Time) *deleteAtOpt {
	return &deleteAtOpt{deleteAt: t}
}

func (o deleteAtOpt) applyToQueueOpts(opts *queueOpts) {
	opts.deleteAt = &o.deleteAt
}

type deduplicationIDOpt struct {
	deduplicationID string
}

func WithDeduplicationID(id string) *deduplicationIDOpt {
	return &deduplicationIDOpt{deduplicationID: id}
}

func (o deduplicationIDOpt) applyToSendOpts(opts *sendOpts) {
	opts.deduplicationID = o.deduplicationID
}

func (o deduplicationIDOpt) applyToDispatchOpts(opts *dispatchOpts) {
	opts.deduplicationID = o.deduplicationID
}

func (o deduplicationIDOpt) applyToRunTaskOpts(opts *runTaskOpts) {
	opts.deduplicationID = o.deduplicationID
}

func (o deduplicationIDOpt) applyToRunFlowOpts(opts *runFlowOpts) {
	opts.deduplicationID = o.deduplicationID
}

type deliverAtOpt struct {
	deliverAt time.Time
}

func WithDeliverAt(deliverAt time.Time) *deliverAtOpt {
	return &deliverAtOpt{deliverAt: deliverAt}
}

func (o deliverAtOpt) applyToSendOpts(opts *sendOpts) {
	opts.deliverAt = &o.deliverAt
}

func (o deliverAtOpt) applyToDispatchOpts(opts *dispatchOpts) {
	opts.deliverAt = &o.deliverAt
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

func WithLogger(l *slog.Logger) *loggerOpt {
	return &loggerOpt{logger: l}
}

func (o loggerOpt) applyToWorker(w *Worker) {
	w.logger = o.logger
}

type timeoutOpt struct {
	timeout time.Duration
}

func WithTimeout(d time.Duration) *timeoutOpt {
	return &timeoutOpt{timeout: d}
}

func (o timeoutOpt) applyToWorker(w *Worker) {
	w.timeout = o.timeout
}

func (o timeoutOpt) applyToHandler(h *Handler) {
	h.timeout = o.timeout
}

type concurrencyOpt struct {
	concurrency int
}

func WithConcurrency(n int) *concurrencyOpt {
	return &concurrencyOpt{concurrency: n}
}

func (o concurrencyOpt) applyToHandler(h *Handler) {
	h.concurrency = o.concurrency
}

type batchSizeOpt struct {
	batchSize int
}

func WithBatchSize(n int) *batchSizeOpt {
	return &batchSizeOpt{batchSize: n}
}

func (o batchSizeOpt) applyToHandler(h *Handler) {
	h.batchSize = o.batchSize
}

type retriesOpt struct {
	retries int
	delay   time.Duration
}

func WithRetries(n int) *retriesOpt {
	return &retriesOpt{retries: n}
}

func (o *retriesOpt) Delay(d time.Duration) *retriesOpt {
	o.delay = d
	return o
}

func (o retriesOpt) applyToHandler(h *Handler) {
	h.retries = o.retries
	h.delay = o.delay
}

type gcOpt struct {
	schedule string
}

func WithGC() *gcOpt {
	return &gcOpt{schedule: "@every 10m"}
}

func (o *gcOpt) Schedule(schedule string) *gcOpt {
	o.schedule = schedule
	return o
}

func (o gcOpt) applyToWorker(w *Worker) {
	w.handlers = append(w.handlers, NewPeriodicTaskHandler("gc", o.schedule, func(ctx context.Context, input struct{}) (struct{}, error) {
		return struct{}{}, GC(ctx, w.conn)
	}))
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
	TaskHandlers    []*TaskHandlerInfo `json:"task_handlers"`
	StepHandlers    []*StepHandlerInfo `json:"step_handlers"`
	StartedAt       time.Time          `json:"started_at"`
	LastHeartbeatAt time.Time          `json:"last_heartbeat_at"`
}

type TaskHandlerInfo struct {
	TaskName string `json:"task_name"`
}

type StepHandlerInfo struct {
	FlowName string `json:"flow_name"`
	StepName string `json:"step_name"`
}

func CreateQueue(ctx context.Context, conn Conn, name string, opts ...QueueOpt) error {
	o := queueOpts{}
	for _, opt := range opts {
		opt.applyToQueueOpts(&o)
	}

	q := `SELECT cb_create_queue(name => $1, topics => $2, delete_at => $3, unlogged => $4);`
	_, err := conn.Exec(ctx, q, name, o.topics, o.deleteAt, o.unlogged)
	return err
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

func Send(ctx context.Context, conn Conn, queue string, payload any, opts ...SendOpt) error {
	o := sendOpts{}
	for _, opt := range opts {
		opt.applyToSendOpts(&o)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `SELECT cb_send(queue => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`
	_, err = conn.Exec(ctx, q, queue, b, o.deduplicationID, o.deliverAt)
	return err
}

func Dispatch(ctx context.Context, conn Conn, topic string, payload any, opts ...DispatchOpt) error {
	o := dispatchOpts{}
	for _, opt := range opts {
		opt.applyToDispatchOpts(&o)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`
	_, err = conn.Exec(ctx, q, topic, b, o.deduplicationID, o.deliverAt)
	return err
}

func Read(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration) ([]Message, error) {
	q := `SELECT * FROM cb_read(queue => $1, quantity => $2, hide_for => $3);`
	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleMessage)
}

func ReadPoll(ctx context.Context, conn Conn, queue string, quantity int, hideFor time.Duration, opts ...ReadPollOpt) ([]Message, error) {
	o := readPollOpts{
		duration: DefaultPollDuration,
		interval: DefaultPollInterval,
	}
	for _, opt := range opts {
		opt.applyToReadPollOpts(&o)
	}

	q := `SELECT * FROM cb_read_poll(queue => $1, quantity => $2, hide_for => $3, poll_for => $4, poll_interval => $5);`

	rows, err := conn.Query(ctx, q, queue, quantity, hideFor.Seconds(), o.duration.Seconds(), o.interval.Milliseconds())
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

func RunTask(ctx context.Context, conn Conn, name string, input any, opts ...RunTaskOpt) (string, error) {
	o := runTaskOpts{}
	for _, opt := range opts {
		opt.applyToRunTaskOpts(&o)
	}

	b, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	q := `SELECT * FROM cb_run_task(name => $1, input => $2, deduplication_id => nullif($3, ''));`
	var runID string
	err = conn.QueryRow(ctx, q, name, b, o.deduplicationID).Scan(&runID)
	if err != nil {
		return "", err
	}
	return runID, err
}

func RunTaskWait(ctx context.Context, conn Conn, name string, input any, opts ...RunTaskOpt) (*TaskRunInfo, error) {
	id, err := RunTask(ctx, conn, name, input, opts...)
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

func RunFlow(ctx context.Context, conn Conn, name string, input any, opts ...RunFlowOpt) (string, error) {
	o := runFlowOpts{}
	for _, opt := range opts {
		opt.applyToRunFlowOpts(&o)
	}

	b, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	q := `SELECT * FROM cb_run_flow(name => $1, input => $2, deduplication_id => nullif($3, ''));`
	var runID string
	err = conn.QueryRow(ctx, q, name, b, o.deduplicationID).Scan(&runID)
	if err != nil {
		return "", err
	}
	return runID, err
}

func RunFlowWait(ctx context.Context, conn Conn, name string, input any, opts ...RunFlowOpt) (*FlowRunInfo, error) {
	id, err := RunFlow(ctx, conn, name, input, opts...)
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

func EnqueueSend(batch *pgx.Batch, queue string, payload any, opts ...SendOpt) error {
	o := sendOpts{}
	for _, opt := range opts {
		opt.applyToSendOpts(&o)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	batch.Queue(
		`SELECT cb_send(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`,
		queue, b, o.deduplicationID, o.deliverAt,
	)

	return nil
}

func EnqueueDispatch(batch *pgx.Batch, topic string, payload any, opts ...DispatchOpt) error {
	o := dispatchOpts{}
	for _, opt := range opts {
		opt.applyToDispatchOpts(&o)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	batch.Queue(
		`SELECT cb_dispatch(topic => $1, payload => $2, deduplication_id => nullif($3, ''), deliver_at => $4);`,
		topic, b, o.deduplicationID, o.deliverAt,
	)

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
