package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNotFound is returned when the addressed job or output does not exist, or
// the job is not waiting for what was delivered to it.
var ErrNotFound = errors.New("catbird: not found")

// ErrAmbiguous is returned by Outputs.Get when several jobs of the type asked
// for recorded outputs. Read them with GetAll.
var ErrAmbiguous = errors.New("catbird: several outputs")

// ErrClaimLost is returned by Complete when the job's claim expired and
// another worker took it. The work of the late attempt is not the queue's any
// more: roll its transaction back.
var ErrClaimLost = errors.New("catbird: claim lost before completion")

// ErrBadPattern is returned by the stream reads when a pattern is not a topic,
// a prefix followed by ".#", or "#".
var ErrBadPattern = errors.New("catbird: invalid topic pattern")

// State is what a job is doing, or how it ended. The six states of a live job
// are derived by the statements in Status and Queues from the job's cb_jobs
// row, because three of them end by the clock and not by a write: a scheduled
// job, a job waiting to retry and a job whose claim lapsed are all queued once
// claimable_at has passed, with nothing written. The three states of a job
// that ended are stored, in cb_job_results.state, because nothing about a job
// that ended changes with time. A workflow takes StateRunning, StateFailed,
// StateCanceled or StateCompleted, where running means jobs remain — queued,
// waiting or retrying included.
type State int

const (
	StateQueued           State = iota // ready, waiting for a worker to take it
	StateScheduled                     // never claimed, the start time is still ahead
	StateRunning                       // a worker holds the claim; for a workflow: jobs remain
	StateWaitingToRetry                // an attempt failed, the retry is scheduled
	StateWaitingForSignal              // waiting for Signal to deliver a payload
	StateWaitingForJobs                // waiting for the jobs it was enqueued after
	StateFailed                        // its last attempt failed, and OnFailed runs once
	StateCanceled                      // Cancel stopped it
	StateCompleted                     // it succeeded
)

// stateNames are the words the statements return and cb_job_results.state
// holds, in constant order. The statements name the states so another client
// reads them out of the SQL.
var stateNames = [...]string{
	"queued", "scheduled", "running", "waiting to retry",
	"waiting for signal", "waiting for jobs", "failed", "canceled", "completed",
}

// String returns the word the statements return, so a log line and a database
// read of the same job agree.
func (s State) String() string {
	if s < 0 || int(s) >= len(stateNames) {
		return fmt.Sprintf("State(%d)", int(s))
	}
	return stateNames[s]
}

// Message is a published message, as a consumer reads it.
type Message struct {
	ID        int64
	Position  int64 // place in the stream, in commit order
	Topic     string
	Payload   json.RawMessage
	CreatedAt time.Time // insert time, not commit time: it does not follow position order
}

// Job is one claimed unit of work, as a handler is given it. A job has no
// position; it is not part of the stream.
type Job struct {
	ID   int64
	Type string // the job type's name
	// The workflow this job belongs to, and the value Signal, Cancel and the
	// output reads take. A job that stands alone is its own group.
	GroupID   int64
	Topic     string
	Payload   json.RawMessage
	Signal    json.RawMessage // the payload delivered by Signal; nil unless the type declared one
	Attempts  int             // 1 on the first run
	CreatedAt time.Time       // insert time of the job's message

	dependencyIDs []int64         // the jobs this one waited for, read by DependencyOutputs
	completed     bool            // Complete's delete succeeded, so the worker does not repeat it
	output        json.RawMessage // what SetOutput recorded, written by the completion
	newJobs       []newJob        // what Enqueue and EnqueueAfter recorded, written by the completion
	payloadErr    error           // a payload that could not be marshaled, returned by the completion
}

// newJob is one job a handler asked for, in the shape the completion writes.
type newJob struct {
	jobType string
	queue   string
	payload []byte
	// Whether this job depends on the others: a dependent job runs when every
	// job of the same buffer that is not dependent has completed.
	dependent bool
	signal    bool
}

// SetOutput records the job's output. Nothing is written here: the completion
// writes the output in the statement that deletes the job row, so an output
// cannot outlive an attempt that never finished. Call it and then either complete the
// job or return nil and let the worker complete it. A second call replaces the
// first.
//
// The value is marshaled here rather than in the completion: a value that
// cannot be marshaled fails the attempt that produced it, instead of failing
// every attempt once its work is already done.
func (j *Job) SetOutput(v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	j.output = body
	return nil
}

// Enqueue records a job to run when this one completes. Nothing is written
// here: the completion writes it in the statement that deletes the job row, so
// the work this job asked for and the end of this job are one commit, and a
// handler that fails or crashes halfway records nothing and retries with an
// empty buffer.
//
// The new job joins this job's workflow, so Signal, Cancel and the output reads
// address it by the same group id. A marshal failure is kept and returned by
// the completion, which fails the attempt.
func (j *Job) Enqueue(t *JobType, payload any) {
	j.record(t, payload, false)
}

// EnqueueAfter records a job to run after the jobs this handler recorded with
// Enqueue — all of them, and nothing wider: not the rest of the workflow, and
// not what another handler is adding at the same time. The count comes from the
// buffer, so nothing outside catbird holds it and it cannot be wrong.
//
// Recorded with no Enqueue beside it, the job waits for nothing and runs at
// once.
func (j *Job) EnqueueAfter(t *JobType, payload any) {
	j.record(t, payload, true)
}

func (j *Job) record(t *JobType, payload any, dependent bool) {
	body, err := json.Marshal(payload)
	if err != nil && j.payloadErr == nil {
		j.payloadErr = fmt.Errorf("catbird: job type %s: %w", t.name, err)
	}
	j.newJobs = append(j.newJobs, newJob{
		jobType:   t.name,
		queue:     t.queue.name,
		payload:   body,
		dependent: dependent,
		signal:    t.opts.Signal,
	})
}

// DependencyOutputs returns the outputs of the jobs this one waited for: the
// jobs the completion that created it recorded with Enqueue, in the order they
// were enqueued. A job that recorded no output has no element. Nil for a job
// that waited for nothing.
//
// This is the read a joining job makes. It waited for one buffer's jobs and
// this returns those, where GroupStatus carries every output in the whole
// workflow — every round of it, and the jobs another handler added. The ids
// come from the job row, so the read probes cb_job_results by primary key.
func (j *Job) DependencyOutputs(ctx context.Context, db Conn) (Outputs, error) {
	if len(j.dependencyIDs) == 0 {
		return nil, nil
	}
	rows, err := db.Query(ctx, `
		SELECT result.message_id, result.job_type, result.output
		FROM unnest($1::bigint[]) WITH ORDINALITY AS dependency (job_id, place)
		JOIN cb_job_results result ON result.message_id = dependency.job_id
		WHERE result.output IS NOT NULL
		ORDER BY dependency.place
	`, j.dependencyIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs Outputs
	for rows.Next() {
		var o Output
		if err := rows.Scan(&o.JobID, &o.JobType, &o.Value); err != nil {
			return nil, err
		}
		outputs = append(outputs, o)
	}
	return outputs, rows.Err()
}

// Conn is the part of pgx.Tx and *pgxpool.Pool this package uses: a pool, a
// connection, or a transaction.
type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// EnqueueOptions are the optional parts of Enqueue.
type EnqueueOptions struct {
	DeduplicationKey string        // when set, a second Enqueue with the same key does nothing
	Delay            time.Duration // earliest start, measured from now
	Topic            string        // what the job is about; the job type's name when empty
}

// Publish appends a message to the stream: consumers see it, no worker runs it.
// Returns the message id, or 0 when deduplicationKey already exists.
func Publish(ctx context.Context, db Conn, topic string, payload any, deduplicationKey string) (int64, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRow(ctx, `
		INSERT INTO cb_messages (topic, payload, stream, deduplication_key)
		VALUES ($1, $2, true, $3)
		ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING
		RETURNING id
	`, topic, body, nullString(deduplicationKey)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// BatchMessage is one message for PublishBatch and EnqueueBatch.
type BatchMessage struct {
	Topic            string
	Payload          any    // marshalled to JSON; a json.RawMessage is written as it is
	DeduplicationKey string // when set, a second message with the same key is not written
}

// PublishBatch appends messages to the stream in one statement, so a
// transaction that changed ten thousand records announces them in one round
// trip. Returns how many were written: a message whose DeduplicationKey
// already exists, or that repeats a key from its own batch, is skipped and not
// counted.
//
// The messages travel as three arrays that are unnested into rows, so the
// number of messages is not limited by the number of statement parameters. Like
// Publish it sends no notification: the assigner announces the messages when it
// gives them their positions.
func PublishBatch(ctx context.Context, db Conn, msgs []BatchMessage) (int, error) {
	if len(msgs) == 0 {
		return 0, nil
	}
	topics, payloads, deduplicationKeys, err := batchArrays(msgs)
	if err != nil {
		return 0, err
	}
	tag, err := db.Exec(ctx, `
		INSERT INTO cb_messages (topic, payload, stream, deduplication_key)
		SELECT topic, payload, true, deduplication_key
		FROM unnest($1::text[], $2::jsonb[], $3::text[]) AS message (topic, payload, deduplication_key)
		ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING
	`, topics, payloads, deduplicationKeys)
	return int(tag.RowsAffected()), err
}

// Cursor is a named position in the stream together with what is read from it:
// the patterns and the position belong to each other, because a position only
// says how far a reader has come through the messages its own patterns select.
// Two readers sharing a name with different patterns skip each other's
// messages — the one reading less acks past what the other has not seen — so
// build a cursor in one place and let both calls use it.
type Cursor struct {
	Name     string
	Patterns []string
}

// Read returns the next messages after the cursor, in position order. A cursor
// that has never been acked starts at position 0. Reading writes nothing and
// moves nothing; Ack is what moves the cursor.
//
// The messages Enqueue writes get no position and are never returned:
// enqueuing a job does not publish it.
func (c Cursor) Read(ctx context.Context, db Conn, limit int) ([]Message, error) {
	startSQL := "COALESCE((SELECT last_position FROM cb_cursors WHERE name = $1), 0)"
	return readMessages(ctx, db, startSQL, c.Name, c.Patterns, limit)
}

// Ack moves the cursor to position. The cursor never moves backwards, so a
// batch acked out of order cannot undo the progress of a later one. Ack takes
// no claim on the cursor: it is for a reader that runs in one place, like
// wire's poll. A consumer that several processes run is Runtime.Consume, which
// claims the cursor before it reads.
func (c Cursor) Ack(ctx context.Context, db Conn, position int64) error {
	_, err := db.Exec(ctx, `
		INSERT INTO cb_cursors (name, last_position) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET last_position = GREATEST(cb_cursors.last_position, EXCLUDED.last_position)
	`, c.Name, position)
	return err
}

// ReadAfter returns the next messages after position, in position order, for a
// caller that holds its own position instead of a cursor: a poll endpoint, or
// a connection that pushes messages to a browser.
//
// A caller that has been away longer than GC keeps messages resumes past
// deleted ones with no sign of it here. OldestPosition is what tells it: when
// that is ahead of the position it held, something was removed and the caller
// refetches its state instead of trusting the rows.
func ReadAfter(ctx context.Context, db Conn, patterns []string, after int64, limit int) ([]Message, error) {
	return readMessages(ctx, db, "$1", after, patterns, limit)
}

// LastPosition is the end of the stream: the highest position assigned so far,
// or 0 on an empty stream. A page embeds it when it renders, so a reader that
// connects afterwards starts there and misses nothing in between.
func LastPosition(ctx context.Context, db Conn) (int64, error) {
	var position int64
	err := db.QueryRow(ctx, `SELECT COALESCE(max(position), 0) FROM cb_messages`).Scan(&position)
	return position, err
}

// OldestPosition is the lowest position GC has not removed, or 0 on an empty
// stream. A reader compares it with the position it holds: when it is higher,
// the messages in between are gone.
func OldestPosition(ctx context.Context, db Conn) (int64, error) {
	var position int64
	err := db.QueryRow(ctx, `SELECT COALESCE(min(position), 0) FROM cb_messages`).Scan(&position)
	return position, err
}

// readMessages runs one stream read. startSQL is the SQL for the position to
// read after, holding $1, and limit is $2; the patterns take the parameters
// from $3.
func readMessages(ctx context.Context, db Conn, startSQL string, startArg any, patterns []string, limit int) ([]Message, error) {
	matchSQL, args, err := compilePatterns(patterns, 3)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(ctx, `
		SELECT id, position, topic, payload, created_at
		FROM cb_messages
		WHERE position > `+startSQL+` AND `+matchSQL+`
		ORDER BY position ASC
		LIMIT $2
	`, append([]any{startArg, limit}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Position, &m.Topic, &m.Payload, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// compilePatterns turns patterns into one boolean expression and the arguments
// it reads, numbered from next. Three forms: a topic on its own matches that
// topic exactly; a prefix followed by ".#" matches the prefix and every topic
// under it, so "order.#" covers "order", "order.paid" and
// "order.paid.refund"; "#" matches everything.
//
// Each pattern becomes its own comparison rather than one array test. A list
// compared with = ANY or LIKE ANY cannot be read as index arms, so it walks the
// position index and filters: 188 buffer hits against 72 for three subtrees
// read from the start of a 300k-message stream.
//
// There is no "*". A wildcard inside a topic is not a prefix range, so it can
// use no index at any position; a pattern that holds one is refused instead of
// quietly running as the slowest read in the system.
func compilePatterns(patterns []string, next int) (string, []any, error) {
	if len(patterns) == 0 {
		return "", nil, ErrBadPattern
	}
	var comparisonsSQL []string
	var args []any
	everything := false
	for _, pattern := range patterns {
		switch {
		case pattern == "#":
			everything = true
		case strings.HasSuffix(pattern, ".#"):
			prefix := strings.TrimSuffix(pattern, ".#")
			if err := checkTopic(prefix); err != nil {
				return "", nil, err
			}
			comparisonsSQL = append(comparisonsSQL, fmt.Sprintf("(topic = $%d OR topic LIKE $%d)", next, next+1))
			args = append(args, prefix, subtreeLikePattern(prefix))
			next += 2
		default:
			if err := checkTopic(pattern); err != nil {
				return "", nil, err
			}
			comparisonsSQL = append(comparisonsSQL, fmt.Sprintf("topic = $%d", next))
			args = append(args, pattern)
			next++
		}
	}
	// "#" is checked last so that a bad pattern beside it is still refused.
	if everything {
		return "true", nil, nil
	}
	return "(" + strings.Join(comparisonsSQL, " OR ") + ")", args, nil
}

func checkTopic(topic string) error {
	if topic == "" || strings.ContainsAny(topic, "#*") {
		return ErrBadPattern
	}
	return nil
}

// subtreeLikePattern builds the SQL LIKE pattern matching every topic under
// prefix — not prefix itself, which is the equality arm beside it. LIKE's own
// wildcard characters in the prefix are escaped so they match literally:
// "image_x" is not a topic under "image".
func subtreeLikePattern(prefix string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix) + ".%"
}

// Enqueue appends a message and a job row for it, and wakes the queue's workers
// unless the job cannot run yet. Returns the job's id, which is also the id of
// the workflow it starts — what Signal, Cancel and the output reads address —
// or 0 when opts.DeduplicationKey already exists.
//
// One statement does all three. The wake CTE calls pg_notify only for a job
// that is claimable now, so a delayed job and a job waiting for a signal do not
// wake workers that can do nothing with them; the final LEFT JOIN references it
// so that it runs, since an unreferenced SELECT CTE is never executed. The
// notification is delivered when the caller's transaction commits, together
// with the row.
func Enqueue(ctx context.Context, db Conn, t *JobType, payload any, opts EnqueueOptions) (int64, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	topic := opts.Topic
	if topic == "" {
		topic = t.name
	}
	var id int64
	err = db.QueryRow(ctx, `
		WITH message AS (
			INSERT INTO cb_messages (topic, payload, deduplication_key)
			VALUES ($1, $2, $3)
			ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING
			RETURNING id
		),
		job AS (
			INSERT INTO cb_jobs (message_id, queue, job_type, claimable_at, awaits_signal)
			SELECT id, $4, $5,
			       CASE WHEN $6 THEN 'infinity'::timestamptz ELSE now() + $7::interval END, $6
			FROM message
			RETURNING message_id, claimable_at
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || $4, '') FROM job WHERE claimable_at <= now()
		)
		SELECT message_id FROM job LEFT JOIN wake ON true
	`, topic, body, nullString(opts.DeduplicationKey), t.queue.name, t.name, t.opts.Signal, opts.Delay).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// EnqueueBatch appends messages and their job rows for one job type in one
// statement and wakes the queue's workers. Returns how many jobs it created: a
// message whose DeduplicationKey is already taken, or that repeats a key from
// its own batch, gets neither a message nor a job row and is not counted.
//
// This is the volume path — what a trigger uses. Every job in the batch takes
// the same options, and none of them starts a workflow or waits for anything: a
// job type declared with Signal cannot be enqueued this way, because a batch has
// no ids for a caller to signal.
//
// The job rows come from the messages that were written, so a deduplicated
// message produces no job. The wake CTE reads the job rows through LIMIT 1, which
// does two things: the join at the end sees one wake row, so it cannot multiply
// the count, and pg_notify runs once instead of once per job. It saves the calls
// rather than the wake-ups — Postgres delivers identical notifications from one
// transaction once whatever we do. Reading the job CTE through a LIMIT does
// not cut the insert short; a data-modifying CTE always runs in full.
func EnqueueBatch(ctx context.Context, db Conn, t *JobType, msgs []BatchMessage, opts EnqueueOptions) (int, error) {
	if len(msgs) == 0 {
		return 0, nil
	}
	if t.opts.Signal {
		return 0, fmt.Errorf("catbird: job type %s waits for a signal and cannot be enqueued in a batch", t.name)
	}
	topics, payloads, deduplicationKeys, err := batchArrays(msgs)
	if err != nil {
		return 0, err
	}
	var created int
	err = db.QueryRow(ctx, `
		WITH input AS (
			SELECT * FROM unnest($1::text[], $2::jsonb[], $3::text[]) AS message (topic, payload, deduplication_key)
		),
		message AS (
			INSERT INTO cb_messages (topic, payload, deduplication_key)
			SELECT topic, payload, deduplication_key FROM input
			ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING
			RETURNING id
		),
		job AS (
			INSERT INTO cb_jobs (message_id, queue, job_type, claimable_at)
			SELECT id, $4, $5, now() + $6::interval FROM message
			RETURNING message_id
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || $4, '') FROM (SELECT 1 FROM job LIMIT 1) one WHERE $7
		)
		SELECT count(*) FROM job LEFT JOIN wake ON true
	`, topics, payloads, deduplicationKeys, t.queue.name, t.name, opts.Delay, opts.Delay <= 0).Scan(&created)
	return created, err
}

// enqueuePeriodic writes one tick of a scheduled job type: the job whose
// deduplication key names the minute, unless that key is taken or a live job
// of the type exists. Two guards in one statement. The key,
// periodic:<type>:<minute> with the minute in UTC as YYYY-MM-DDTHH:MMZ,
// collapses every process ticking in the same minute into one job; the format
// is fixed because every process must produce the same key. The NOT EXISTS
// makes a tick during a live run write nothing at all — no message row, no
// key — so a run that outlives its schedule swallows the ticks it covers and
// no backlog of stale ticks can form.
//
// The guard counts every live job of the type, not only ticks, so a manual
// Enqueue of the type holds it too — "run it now" composes — but is not
// itself guarded: two manual enqueues can overlap. It repeats the ready
// index's dependencies = 0 so the probe stays on that index instead of
// scanning the heap; the cost is that a job of the type created inside a
// workflow is not counted while it still waits for other jobs.
func enqueuePeriodic(ctx context.Context, db Conn, t *JobType, minute time.Time) error {
	key := "periodic:" + t.name + ":" + minute.UTC().Format("2006-01-02T15:04") + "Z"
	_, err := db.Exec(ctx, `
		WITH message AS (
			INSERT INTO cb_messages (topic, deduplication_key)
			SELECT $1, $2
			WHERE NOT EXISTS (
				SELECT 1 FROM cb_jobs
				WHERE queue = $3 AND job_type = $1 AND dependencies = 0
			)
			ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING
			RETURNING id
		),
		job AS (
			INSERT INTO cb_jobs (message_id, queue, job_type, claimable_at)
			SELECT id, $3, $1, now()
			FROM message
			RETURNING message_id
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || $3, '') FROM job
		)
		SELECT message_id FROM job LEFT JOIN wake ON true
	`, t.name, key, t.queue.name)
	return err
}

// Complete finishes a job: it deletes the job row, writes the job's result with
// the output the handler recorded with SetOutput, counts down the jobs waiting
// for this one, and creates the jobs the handler recorded with Enqueue and
// EnqueueAfter. A worker
// does this itself when the handler returns nil, so a handler only calls it to
// put the completion in the same transaction as its own writes, which is what
// makes the job's work and the job's end one commit. The delete matches on
// attempts: if the claim expired and another worker took the job, attempts moved
// on, nothing is deleted, and this returns ErrClaimLost — the caller rolls
// back and the work of the late attempt is discarded.
//
// It is one statement, so a job costs one round trip to finish however much it
// asked for. Everything in it hangs off the row the delete returned, so an
// attempt that lost its claim writes no result, counts nothing down and creates
// no jobs.
//
// The new jobs get their ids from the sequence inside the statement, which is
// what lets one statement point the jobs that are not dependent at the ones
// that are: a job recorded with EnqueueAfter takes the count of the others as
// its dependencies and their ids in dependency_job_ids, and each of the others
// carries its id in dependent_job_ids. The count and the list come out of the
// same rows, so they cannot disagree.
// The CTE that hands out the ids is MATERIALIZED because nextval is volatile and
// an inlined CTE would be evaluated once per reference, giving a message and its
// job row different ids.
//
// A successful call marks job, so the worker does not run the completion a
// second time. The mark records that the statement succeeded, not that the
// caller's transaction committed: a handler that completes the job, rolls the
// transaction back, and then returns nil leaves a job row nothing deletes until
// the claim expires and the job runs again.
func Complete(ctx context.Context, db Conn, job *Job) error {
	if job.payloadErr != nil {
		return job.payloadErr
	}
	types, queues, payloads, dependents, signals := newJobArrays(job.newJobs)
	var completed, woken int
	err := db.QueryRow(ctx, `
		WITH completed AS (
		    DELETE FROM cb_jobs WHERE message_id = $1 AND attempts = $2
		    RETURNING message_id, group_id, queue, job_type, attempts, dependent_job_ids
		),
		result AS (
		    INSERT INTO cb_job_results (message_id, group_id, queue, job_type, attempts, state, output)
		    SELECT message_id, group_id, queue, job_type, attempts, 'completed', $3::jsonb FROM completed
		),
		dependent_job AS (
		    UPDATE cb_jobs SET dependencies = dependencies - 1
		    WHERE message_id IN (SELECT unnest(dependent_job_ids) FROM completed)
		      AND dependencies > 0
		    RETURNING queue, dependencies, claimable_at
		),
		new_job AS MATERIALIZED (
		    SELECT nextval('cb_messages_id_seq') AS id, n.*
		    FROM unnest($4::text[], $5::text[], $6::jsonb[], $7::boolean[], $8::boolean[])
		         AS n (job_type, queue, payload, dependent, signal)
		),
		new_message AS (
		    INSERT INTO cb_messages (id, topic, payload)
		    SELECT n.id, n.job_type, n.payload FROM new_job n, completed
		),
		new_job_row AS (
		    INSERT INTO cb_jobs (message_id, queue, job_type, group_id, claimable_at,
		                           dependencies, dependency_job_ids, dependent_job_ids, awaits_signal)
		    SELECT n.id, n.queue, n.job_type, coalesce(c.group_id, c.message_id),
		           CASE WHEN n.signal THEN 'infinity'::timestamptz ELSE now() END,
		           CASE WHEN n.dependent THEN (SELECT count(*) FROM new_job WHERE NOT dependent) ELSE 0 END::smallint,
		           CASE WHEN n.dependent THEN (SELECT array_agg(id ORDER BY id) FROM new_job WHERE NOT dependent) END,
		           CASE WHEN n.dependent THEN NULL ELSE (SELECT array_agg(id ORDER BY id) FROM new_job WHERE dependent) END,
		           n.signal
		    FROM new_job n, completed c
		    RETURNING queue, dependencies, claimable_at
		),
		woken AS (
		    SELECT DISTINCT queue FROM (
		        SELECT queue, dependencies, claimable_at FROM dependent_job
		        UNION ALL
		        SELECT queue, dependencies, claimable_at FROM new_job_row
		    ) ready
		    WHERE dependencies = 0 AND claimable_at <= now()
		),
		wake AS (
		    SELECT pg_notify('cb_queue_' || queue, '') FROM woken
		)
		SELECT (SELECT count(*) FROM completed), (SELECT count(*) FROM wake)
	`, job.ID, job.Attempts, job.output, types, queues, payloads, dependents, signals).Scan(&completed, &woken)
	if err != nil {
		return err
	}
	if completed == 0 {
		return ErrClaimLost
	}
	job.completed = true
	return nil
}

// Signal delivers the payload a job of this type is waiting for in the given
// workflow, and makes it claimable. groupID is what Enqueue returned for the
// job that started the workflow, which is also the id of a job that stands
// alone. Returns ErrNotFound when nothing is waiting: no such workflow, no live
// job of that type in it, or the signal was already delivered.
//
// The job is addressed by what it is rather than by its id, because a job a
// handler asked for has no id until that handler's completion runs and no
// caller can hold one. Every live job of the type in the workflow is given the
// payload; declaring one gate per type in a workflow is what keeps that to one.
func Signal(ctx context.Context, db Conn, groupID int64, t *JobType, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var delivered, woken int
	err = db.QueryRow(ctx, `
		WITH gated AS (
			UPDATE cb_jobs SET signal = $3, claimable_at = now()
			WHERE (group_id = $1 OR message_id = $1) AND job_type = $2
			  AND awaits_signal AND signal IS NULL
			RETURNING queue, dependencies
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || queue, '') FROM gated WHERE dependencies = 0
		)
		SELECT (SELECT count(*) FROM gated), (SELECT count(*) FROM wake)
	`, groupID, t.name, body).Scan(&delivered, &woken)
	if err != nil {
		return err
	}
	if delivered == 0 {
		return ErrNotFound
	}
	return nil
}

// Cancel ends every live job of the workflow as canceled, the job that started
// it included: each job row is deleted and a result is written for it. A job
// that is running when it is canceled is not interrupted, unless its queue
// renews claims, but its completion finds no row and writes nothing — no
// output, no new jobs — as after a lost claim; a handler that completes in its
// own transaction gets ErrClaimLost and rolls back. Cancel does not undo what
// a completed job did, and runs no OnFailed.
func Cancel(ctx context.Context, db Conn, groupID int64) error {
	_, err := db.Exec(ctx, `
		WITH canceled AS (
		    DELETE FROM cb_jobs WHERE group_id = $1 OR message_id = $1
		    RETURNING message_id, group_id, queue, job_type, attempts, error
		)
		INSERT INTO cb_job_results (message_id, group_id, queue, job_type, attempts, state, error)
		SELECT message_id, group_id, queue, job_type, attempts, 'canceled', error FROM canceled
	`, groupID)
	return err
}

// Output is one recorded output: which job recorded it, that job's type, and
// the value SetOutput was given. The JSON tags are the names GroupStatus's
// statement builds, so its rows decode straight into the type.
type Output struct {
	JobID   int64           `json:"job_id"`
	JobType string          `json:"job_type"`
	Value   json.RawMessage `json:"value"`
}

// Scan unmarshals the value into dest. A dest of *json.RawMessage takes the
// JSON untouched.
func (o Output) Scan(dest any) error {
	return json.Unmarshal(o.Value, dest)
}

// Outputs holds recorded outputs in the order the jobs that recorded them were
// created. A job that recorded nothing has no element.
type Outputs []Output

// Get unmarshals into dest the one output recorded by a job of this type.
// ErrNotFound when no job of the type recorded one, ErrAmbiguous when several
// did — a fan-out, which GetAll reads.
func (os Outputs) Get(t *JobType, dest any) error {
	all := os.GetAll(t)
	switch len(all) {
	case 0:
		return ErrNotFound
	case 1:
		return all[0].Scan(dest)
	default:
		return ErrAmbiguous
	}
}

// GetAll returns the outputs recorded by jobs of this type, order kept.
func (os Outputs) GetAll(t *JobType) Outputs {
	var all Outputs
	for _, o := range os {
		if o.JobType == t.name {
			all = append(all, o)
		}
	}
	return all
}

// JobStatus is what Status reports of one job: its state, what kind of job it
// is and what it was given, the attempts it has spent, what its last failed
// attempt returned, when it was created and, once it has ended, when it ended
// and what it recorded.
//
// Error is what the last failed attempt returned. It is empty while an attempt
// runs, because the claim clears it, which is what separates StateRunning from
// StateWaitingToRetry. On a failed job it is the error that ended it; a
// canceled job keeps the one it was retrying on, if any.
//
// EndedAt is zero and Output nil while the job lives. Output is nil on a
// failed or canceled job too, and on a completed job that recorded nothing.
type JobStatus struct {
	State     State
	Type      string // the job type's name
	Attempts  int
	Error     string
	CreatedAt time.Time
	EndedAt   time.Time
	Payload   json.RawMessage
	Output    json.RawMessage
}

// stateCaseSQL is the body of the CASE that derives what a live job is doing,
// over a cb_jobs row aliased job. It is one constant because Status and Queues both
// run it, and a copy that drifted — a test reordered, a comparison changed —
// would let the two reads sort the same job into different states with nothing
// failing. The words are stateNames. The three states of a job that ended are
// not derived: cb_job_results.state holds the word.
const stateCaseSQL = `
		           WHEN job.awaits_signal AND job.signal IS NULL THEN 'waiting for signal'
		           WHEN job.dependencies > 0 THEN 'waiting for jobs'
		           WHEN job.claimable_at <= now() THEN 'queued'
		           WHEN job.attempts = 0 THEN 'scheduled'
		           WHEN job.error IS NOT NULL THEN 'waiting to retry'
		           ELSE 'running'`

// Status reports what one job is doing, or how it ended. Three primary-key
// probes: a live job's state is derived from its cb_jobs row, and a job that
// ended has its state on its result row. A job is in one of the two tables,
// never both, so the coalesces below never choose.
//
// A job is inspectable for retention after it ended: GC deletes the result and
// then the message, and after that the job returns ErrNotFound, as does a
// published message's id.
func Status(ctx context.Context, db Conn, id int64) (JobStatus, error) {
	var status JobStatus
	var name, jobType, text *string
	var endedAt *time.Time
	err := db.QueryRow(ctx, `
		SELECT CASE
		           WHEN result.message_id IS NOT NULL THEN result.state
		           WHEN job.message_id IS NOT NULL THEN
		               CASE`+stateCaseSQL+`
		               END
		       END,
		       coalesce(job.job_type, result.job_type),
		       coalesce(job.attempts, result.attempts, 0),
		       coalesce(job.error, result.error),
		       message.created_at, result.ended_at, message.payload, result.output
		FROM cb_messages message
		LEFT JOIN cb_jobs job ON job.message_id = message.id
		LEFT JOIN cb_job_results result ON result.message_id = message.id
		WHERE message.id = $1 AND NOT message.stream
	`, id).Scan(&name, &jobType, &status.Attempts, &text,
		&status.CreatedAt, &endedAt, &status.Payload, &status.Output)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && name == nil) {
		// No message, or a message whose result GC has deleted and whose
		// message it is about to: the job is gone either way.
		return JobStatus{}, ErrNotFound
	}
	if err != nil {
		return JobStatus{}, err
	}
	status.Type = *jobType
	if text != nil {
		status.Error = *text
	}
	if endedAt != nil {
		status.EndedAt = *endedAt
	}
	for i, n := range stateNames {
		if n == *name {
			status.State = State(i)
			return status, nil
		}
	}
	return JobStatus{}, fmt.Errorf("catbird: unknown job state %q", *name)
}

// JobGroupStatus is what GroupStatus reports of a whole workflow: its state,
// which job to ask when it failed, and every output its jobs recorded so far,
// in the order the jobs that recorded them were created.
//
// FailedJobID is 0 unless the state is StateFailed. It is the job whose failure
// ended the workflow — a job that fails cancels the rest of its workflow, and
// the jobs the cancel took have no failure to read — so Status on it has the
// attempts and the error. When several jobs failed before the cancel reached
// them, it is the first that did.
type JobGroupStatus struct {
	State       State
	FailedJobID int64
	Outputs     Outputs
}

// GroupStatus reports what a workflow is doing: StateRunning while jobs
// remain, StateFailed once any job failed, StateCanceled when Cancel stopped
// it and no job had failed, StateCompleted when every job completed. The
// outputs ride the same statement, so polling and reading the outputs is one
// call.
//
// This is the read for the caller that holds only the id Enqueue returned:
// handlers decide the fan-out as they run, so that caller cannot name the
// workflow's jobs, and a job that failed runs nothing and publishes nothing —
// only its result shows it. A workflow is inspectable for retention after its
// last job ended: GC deletes the results as they age out, the starting job's
// first, and once every result is gone the workflow returns ErrNotFound.
func GroupStatus(ctx context.Context, db Conn, groupID int64) (JobGroupStatus, error) {
	var name *string
	var status JobGroupStatus
	var outputs []byte
	err := db.QueryRow(ctx, `
		WITH job AS (
		    SELECT message_id FROM cb_jobs WHERE group_id = $1 OR message_id = $1
		),
		result AS (
		    SELECT message_id, job_type, state, ended_at, output
		    FROM cb_job_results WHERE group_id = $1 OR message_id = $1
		)
		SELECT CASE
		           WHEN EXISTS (SELECT 1 FROM result WHERE state = 'failed') THEN 'failed'
		           WHEN EXISTS (SELECT 1 FROM result WHERE state = 'canceled') THEN 'canceled'
		           WHEN EXISTS (SELECT 1 FROM job) THEN 'running'
		           WHEN EXISTS (SELECT 1 FROM result) THEN 'completed'
		       END,
		       coalesce((SELECT message_id FROM result WHERE state = 'failed'
		                 ORDER BY ended_at, message_id LIMIT 1), 0),
		       (SELECT coalesce(jsonb_agg(jsonb_build_object(
		                   'job_id', message_id,
		                   'job_type', job_type,
		                   'value', output) ORDER BY message_id), '[]')
		        FROM result WHERE output IS NOT NULL)
	`, groupID).Scan(&name, &status.FailedJobID, &outputs)
	if err != nil {
		return JobGroupStatus{}, err
	}
	if name == nil {
		return JobGroupStatus{}, ErrNotFound
	}
	if err := json.Unmarshal(outputs, &status.Outputs); err != nil {
		return JobGroupStatus{}, err
	}
	for i, n := range stateNames {
		if n == *name {
			status.State = State(i)
			return status, nil
		}
	}
	return JobGroupStatus{}, fmt.Errorf("catbird: unknown workflow state %q", *name)
}

// QueueInfo is what Queues reports of one queue: how many of its jobs are in
// each state, and the longest any queued job has been waiting for a worker.
// The wait runs from claimable_at, so a retry counts from when its backoff ran
// out, not from when the job was created: it measures workers falling behind,
// which is what a depth number alone cannot show.
type QueueInfo struct {
	Queue            string
	Queued           int
	Scheduled        int
	Running          int
	WaitingToRetry   int
	WaitingForSignal int
	WaitingForJobs   int
	Failed           int // failed jobs GC has not collected yet
	LongestQueued    time.Duration
}

// Queues reports what every queue is doing: one QueueInfo per queue that has
// live jobs or failed ones GC has not collected, in name order. A queue whose
// every job completed or was canceled does not appear; a caller exporting a
// fixed set of queues merges in the ones it declared. It walks every job row —
// a table this design keeps small — and the failed results through their own
// index, so it is a read to poll on an interval, not a hot-path statement.
func Queues(ctx context.Context, db Conn) ([]QueueInfo, error) {
	rows, err := db.Query(ctx, `
		SELECT queue,
		       count(*) FILTER (WHERE state = 'queued'),
		       count(*) FILTER (WHERE state = 'scheduled'),
		       count(*) FILTER (WHERE state = 'running'),
		       count(*) FILTER (WHERE state = 'waiting to retry'),
		       count(*) FILTER (WHERE state = 'waiting for signal'),
		       count(*) FILTER (WHERE state = 'waiting for jobs'),
		       count(*) FILTER (WHERE state = 'failed'),
		       coalesce(extract(epoch FROM now() - min(claimable_at) FILTER (WHERE state = 'queued')), 0)::float8
		FROM (
		    SELECT job.queue, job.claimable_at,
		           CASE`+stateCaseSQL+`
		           END AS state
		    FROM cb_jobs job
		    UNION ALL
		    SELECT result.queue, NULL, result.state
		    FROM cb_job_results result WHERE result.state = 'failed'
		) job
		GROUP BY queue
		ORDER BY queue
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var queues []QueueInfo
	for rows.Next() {
		var q QueueInfo
		var age float64
		if err := rows.Scan(&q.Queue, &q.Queued, &q.Scheduled, &q.Running,
			&q.WaitingToRetry, &q.WaitingForSignal, &q.WaitingForJobs, &q.Failed, &age); err != nil {
			return nil, err
		}
		q.LongestQueued = time.Duration(age * float64(time.Second))
		queues = append(queues, q)
	}
	return queues, rows.Err()
}

// GC deletes the results of jobs that ended longer than retention ago, and
// then the messages older than retention that no job row and no result refers
// to: published messages, and the messages of the jobs whose results just
// went. A message whose job is still live is kept, however old it is.
//
// Retention runs from when a job ended, not from when it was created, so a job
// that waited a month for a signal can be inspected for the whole retention
// after it finished rather than for what was left of it. It has to outlast
// the longest wait inside a workflow: a job that waits longer than retention
// for a dependency that already ended finds no output for it.
//
// A runtime with Options.Retention set runs GC itself, hourly. An application
// that leaves it zero calls GC on its own schedule; it is safe to run from
// several places at once.
func GC(ctx context.Context, db Conn, retention time.Duration) error {
	_, err := db.Exec(ctx, `
		DELETE FROM cb_job_results
		WHERE ended_at < now() - $1::interval
	`, retention)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		DELETE FROM cb_messages message
		WHERE created_at < now() - $1::interval
		  AND NOT EXISTS (SELECT 1 FROM cb_jobs job WHERE job.message_id = message.id)
		  AND NOT EXISTS (SELECT 1 FROM cb_job_results result WHERE result.message_id = message.id)
	`, retention)
	return err
}

// batchArrays turns messages into the three parallel arrays the batch
// statements unnest, so the number of messages is not limited by the number of
// statement parameters.
func batchArrays(msgs []BatchMessage) (topics []string, payloads [][]byte, deduplicationKeys []*string, err error) {
	topics = make([]string, len(msgs))
	payloads = make([][]byte, len(msgs))
	deduplicationKeys = make([]*string, len(msgs))
	for i, msg := range msgs {
		body, err := json.Marshal(msg.Payload)
		if err != nil {
			return nil, nil, nil, err
		}
		topics[i], payloads[i], deduplicationKeys[i] = msg.Topic, body, nullString(msg.DeduplicationKey)
	}
	return topics, payloads, deduplicationKeys, nil
}

// newJobArrays turns a handler's buffer into the five parallel arrays the
// completion unnests.
func newJobArrays(jobs []newJob) (types, queues []string, payloads [][]byte, dependents, signals []bool) {
	types = make([]string, len(jobs))
	queues = make([]string, len(jobs))
	payloads = make([][]byte, len(jobs))
	dependents = make([]bool, len(jobs))
	signals = make([]bool, len(jobs))
	for i, n := range jobs {
		types[i], queues[i], payloads[i] = n.jobType, n.queue, n.payload
		dependents[i], signals[i] = n.dependent, n.signal
	}
	return types, queues, payloads, dependents, signals
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
