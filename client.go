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

// ErrNotFound is returned when the addressed job or result does not exist, or
// the job is not waiting for what was delivered to it.
var ErrNotFound = errors.New("catbird: not found")

// ErrAmbiguous is returned by Output when a workflow holds several results of
// the job type asked for. Read them with Outputs.
var ErrAmbiguous = errors.New("catbird: several results")

// ErrLeaseExpired is returned by Complete when the job's lease ran out and
// another worker claimed it. The work of the late attempt is not the queue's any
// more: roll its transaction back.
var ErrLeaseExpired = errors.New("catbird: lease expired before completion")

// ErrBadPattern is returned by the stream reads when a pattern is not a topic,
// a prefix followed by ".#", or "#".
var ErrBadPattern = errors.New("catbird: invalid topic pattern")

// Claim status values.
const (
	statusLive int16 = 0 // ready, waiting, or claimed; visible_at and dependencies tell which
	statusDead int16 = 1 // failed permanently or canceled; never claimed again
)

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
	// result reads take. A job that stands alone is its own group.
	GroupID   int64
	Topic     string
	Payload   json.RawMessage
	Signal    json.RawMessage // the payload delivered by Signal; nil unless the type declared one
	Attempts  int             // 1 on the first run
	CreatedAt time.Time       // insert time of the job's message

	completed  bool            // Complete's delete succeeded, so the worker does not repeat it
	output     json.RawMessage // what SetOutput recorded, written by the completion
	newJobs    []newJob        // what Enqueue and EnqueueAfter recorded, written by the completion
	payloadErr error           // a payload that could not be marshaled, returned by the completion
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

// SetOutput records the job's result. Nothing is written here: the completion
// writes the result in the statement that deletes the claim, so a result cannot
// outlive an attempt that never finished. Call it and then either complete the
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
// here: the completion writes it in the statement that deletes the claim, so
// the work this job asked for and the end of this job are one commit, and a
// handler that fails or crashes halfway records nothing and retries with an
// empty buffer.
//
// The new job joins this job's workflow, so Signal, Cancel and the result reads
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

// Conn is the part of pgx.Tx and *pgxpool.Pool this package uses: a pool, a
// connection, or a transaction.
type Conn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// EnqueueOptions are the optional parts of Enqueue.
type EnqueueOptions struct {
	DedupKey string        // when set, a second Enqueue with the same key does nothing
	Delay    time.Duration // earliest start, measured from now
	Topic    string        // what the job is about; the job type's name when empty
}

// Publish appends a message to the stream: consumers see it, no worker runs it.
// Returns the message id, or 0 when dedupKey already exists.
func Publish(ctx context.Context, db Conn, topic string, payload any, dedupKey string) (int64, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	var id int64
	err = db.QueryRow(ctx, `
		INSERT INTO cb_messages (topic, payload, stream, dedup_key)
		VALUES ($1, $2, true, $3)
		ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
		RETURNING id
	`, topic, body, nullString(dedupKey)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// BatchMessage is one message for PublishBatch and EnqueueBatch.
type BatchMessage struct {
	Topic    string
	Payload  any    // marshalled to JSON; a json.RawMessage is written as it is
	DedupKey string // when set, a second message with the same key is not written
}

// PublishBatch appends messages to the stream in one statement, so a
// transaction that changed ten thousand records announces them in one round
// trip. Returns how many were written: a message whose DedupKey already exists,
// or that repeats a key from its own batch, is skipped and not counted.
//
// The messages travel as three arrays that are unnested into rows, so the
// number of messages is not limited by the number of statement parameters. Like
// Publish it sends no notification: the assigner announces the messages when it
// gives them their positions.
func PublishBatch(ctx context.Context, db Conn, msgs []BatchMessage) (int, error) {
	if len(msgs) == 0 {
		return 0, nil
	}
	topics, payloads, dedupKeys, err := batchArrays(msgs)
	if err != nil {
		return 0, err
	}
	tag, err := db.Exec(ctx, `
		INSERT INTO cb_messages (topic, payload, stream, dedup_key)
		SELECT topic, payload, true, dedup_key
		FROM unnest($1::text[], $2::jsonb[], $3::text[]) AS message (topic, payload, dedup_key)
		ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
	`, topics, payloads, dedupKeys)
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
	start := "COALESCE((SELECT last_position FROM cb_cursors WHERE name = $1), 0)"
	return readMessages(ctx, db, start, c.Name, c.Patterns, limit)
}

// Ack moves the cursor to position. The cursor never moves backwards, so a
// batch acked out of order cannot undo the progress of a later one.
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

// readMessages runs one stream read. start is the SQL for the position to read
// after, holding $1, and limit is $2; the patterns take the parameters from $3.
func readMessages(ctx context.Context, db Conn, start string, startArg any, patterns []string, limit int) ([]Message, error) {
	match, args, err := compilePatterns(patterns, 3)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(ctx, `
		SELECT id, position, topic, payload, created_at
		FROM cb_messages
		WHERE position > `+start+` AND `+match+`
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
	var arms []string
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
			arms = append(arms, fmt.Sprintf("(topic = $%d OR topic LIKE $%d)", next, next+1))
			args = append(args, prefix, subtreeLikePattern(prefix))
			next += 2
		default:
			if err := checkTopic(pattern); err != nil {
				return "", nil, err
			}
			arms = append(arms, fmt.Sprintf("topic = $%d", next))
			args = append(args, pattern)
			next++
		}
	}
	// "#" is checked last so that a bad pattern beside it is still refused.
	if everything {
		return "true", nil, nil
	}
	return "(" + strings.Join(arms, " OR ") + ")", args, nil
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

// Enqueue appends a message and a claim for it, and wakes the queue's workers
// unless the job cannot run yet. Returns the job's id, which is also the id of
// the workflow it starts — what Signal, Cancel and the result reads address —
// or 0 when opts.DedupKey already exists.
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
			INSERT INTO cb_messages (topic, payload, dedup_key)
			VALUES ($1, $2, $3)
			ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
			RETURNING id
		),
		claim AS (
			INSERT INTO cb_claims (message_id, queue, job_type, visible_at, awaits_signal)
			SELECT id, $4, $5,
			       CASE WHEN $6 THEN 'infinity'::timestamptz ELSE now() + $7::interval END, $6
			FROM message
			RETURNING message_id, visible_at
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || $4, '') FROM claim WHERE visible_at <= now()
		)
		SELECT message_id FROM claim LEFT JOIN wake ON true
	`, topic, body, nullString(opts.DedupKey), t.queue.name, t.name, t.opts.Signal, opts.Delay).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

// EnqueueBatch appends messages and their claims for one job type in one
// statement and wakes the queue's workers. Returns how many jobs it created: a
// message whose DedupKey is already taken, or that repeats a key from its own
// batch, gets neither a message nor a claim and is not counted.
//
// This is the volume path — what a trigger uses. Every job in the batch takes
// the same options, and none of them starts a workflow or waits for anything: a
// job type declared with Signal cannot be enqueued this way, because a batch has
// no ids for a caller to signal.
//
// The claims come from the messages that were written, so a deduplicated
// message produces no job. The wake CTE reads the claims through LIMIT 1, which
// does two things: the join at the end sees one wake row, so it cannot multiply
// the count, and pg_notify runs once instead of once per job. It saves the calls
// rather than the wake-ups — Postgres delivers identical notifications from one
// transaction once whatever we do. Reading the claim CTE through a LIMIT does
// not cut the insert short; a data-modifying CTE always runs in full.
func EnqueueBatch(ctx context.Context, db Conn, t *JobType, msgs []BatchMessage, opts EnqueueOptions) (int, error) {
	if len(msgs) == 0 {
		return 0, nil
	}
	if t.opts.Signal {
		return 0, fmt.Errorf("catbird: job type %s waits for a signal and cannot be enqueued in a batch", t.name)
	}
	topics, payloads, dedupKeys, err := batchArrays(msgs)
	if err != nil {
		return 0, err
	}
	var created int
	err = db.QueryRow(ctx, `
		WITH input AS (
			SELECT * FROM unnest($1::text[], $2::jsonb[], $3::text[]) AS message (topic, payload, dedup_key)
		),
		message AS (
			INSERT INTO cb_messages (topic, payload, dedup_key)
			SELECT topic, payload, dedup_key FROM input
			ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
			RETURNING id
		),
		claim AS (
			INSERT INTO cb_claims (message_id, queue, job_type, visible_at)
			SELECT id, $4, $5, now() + $6::interval FROM message
			RETURNING message_id
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || $4, '') FROM (SELECT 1 FROM claim LIMIT 1) one WHERE $7
		)
		SELECT count(*) FROM claim LEFT JOIN wake ON true
	`, topics, payloads, dedupKeys, t.queue.name, t.name, opts.Delay, opts.Delay <= 0).Scan(&created)
	return created, err
}

// Complete finishes a job: it deletes the claim, writes the result the handler
// recorded with SetOutput, counts down the jobs waiting for this one, and
// creates the jobs the handler recorded with Enqueue and EnqueueAfter. A worker
// does this itself when the handler returns nil, so a handler only calls it to
// put the completion in the same transaction as its own writes, which is what
// makes the job's work and the job's end one commit. attempts is the lease
// token: if the lease ran out and another worker claimed the job, attempts moved
// on, nothing is deleted, and this returns ErrLeaseExpired — the caller rolls
// back and the work of the late attempt is discarded.
//
// It is one statement, so a job costs one round trip to finish however much it
// asked for. Everything in it hangs off the claim the delete returned, so an
// attempt that lost its lease writes no result, counts nothing down and creates no
// jobs.
//
// The new jobs get their ids from the sequence inside the statement, which is
// what lets one statement point the jobs that are not dependent at the ones
// that are: a job recorded with EnqueueAfter takes the count of the others as
// its dependencies, and each of the others carries its id in dependent_job_ids.
// The CTE that hands out the ids is MATERIALIZED because nextval is volatile and
// an inlined CTE would be evaluated once per reference, giving a message and its
// claim different ids.
//
// A successful call marks job, so the worker does not run the completion a
// second time. The mark records that the statement succeeded, not that the
// caller's transaction committed: a handler that completes the job, rolls the
// transaction back, and then returns nil leaves a claim nothing deletes until
// the lease runs out and the job runs again.
func Complete(ctx context.Context, db Conn, job *Job) error {
	if job.payloadErr != nil {
		return job.payloadErr
	}
	types, queues, payloads, dependents, signals := newJobArrays(job.newJobs)
	var completed, woken int
	err := db.QueryRow(ctx, `
		WITH claim AS (
		    DELETE FROM cb_claims WHERE message_id = $1 AND attempts = $2
		    RETURNING message_id, job_type, coalesce(group_id, message_id) AS group_id, dependent_job_ids
		),
		output AS (
		    INSERT INTO cb_outputs (message_id, group_id, job_type, output)
		    SELECT message_id, group_id, job_type, $3::jsonb FROM claim WHERE $3::jsonb IS NOT NULL
		),
		dependent_job AS (
		    UPDATE cb_claims SET dependencies = dependencies - 1
		    WHERE message_id IN (SELECT unnest(dependent_job_ids) FROM claim)
		      AND status = 0 AND dependencies > 0
		    RETURNING queue, dependencies, visible_at
		),
		new_job AS MATERIALIZED (
		    SELECT nextval('cb_messages_id_seq') AS id, n.*
		    FROM unnest($4::text[], $5::text[], $6::jsonb[], $7::boolean[], $8::boolean[])
		         AS n (job_type, queue, payload, dependent, signal)
		),
		new_message AS (
		    INSERT INTO cb_messages (id, topic, payload)
		    SELECT n.id, n.job_type, n.payload FROM new_job n, claim
		),
		new_claim AS (
		    INSERT INTO cb_claims (message_id, queue, job_type, group_id, visible_at,
		                           dependencies, dependent_job_ids, awaits_signal)
		    SELECT n.id, n.queue, n.job_type, c.group_id,
		           CASE WHEN n.signal THEN 'infinity'::timestamptz ELSE now() END,
		           CASE WHEN n.dependent THEN (SELECT count(*) FROM new_job WHERE NOT dependent) ELSE 0 END::smallint,
		           CASE WHEN n.dependent THEN NULL ELSE (SELECT array_agg(id) FROM new_job WHERE dependent) END,
		           n.signal
		    FROM new_job n, claim c
		    RETURNING queue, dependencies, visible_at
		),
		woken AS (
		    SELECT DISTINCT queue FROM (
		        SELECT queue, dependencies, visible_at FROM dependent_job
		        UNION ALL
		        SELECT queue, dependencies, visible_at FROM new_claim
		    ) ready
		    WHERE dependencies = 0 AND visible_at <= now()
		),
		wake AS (
		    SELECT pg_notify('cb_queue_' || queue, '') FROM woken
		)
		SELECT (SELECT count(*) FROM claim), (SELECT count(*) FROM wake)
	`, job.ID, job.Attempts, job.output, types, queues, payloads, dependents, signals).Scan(&completed, &woken)
	if err != nil {
		return err
	}
	if completed == 0 {
		return ErrLeaseExpired
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
			UPDATE cb_claims SET signal = $3, visible_at = now()
			WHERE (group_id = $1 OR message_id = $1) AND job_type = $2
			  AND status = $4 AND awaits_signal AND signal IS NULL
			RETURNING queue, dependencies
		),
		wake AS (
			SELECT pg_notify('cb_queue_' || queue, '') FROM gated WHERE dependencies = 0
		)
		SELECT (SELECT count(*) FROM gated), (SELECT count(*) FROM wake)
	`, groupID, t.name, body, statusLive).Scan(&delivered, &woken)
	if err != nil {
		return err
	}
	if delivered == 0 {
		return ErrNotFound
	}
	return nil
}

// Cancel marks every live job of the workflow dead, the job that started it
// included. A job that is already running finishes; cancel only stops jobs from
// starting.
func Cancel(ctx context.Context, db Conn, groupID int64) error {
	_, err := db.Exec(ctx, `
		UPDATE cb_claims SET status = $2
		WHERE (group_id = $1 OR message_id = $1) AND status = $3
	`, groupID, statusDead, statusLive)
	return err
}

// GC deletes dead claims and messages older than retention. A message that
// still has a claim is kept, however old it is. Results go with their message.
func GC(ctx context.Context, db Conn, retention time.Duration) error {
	_, err := db.Exec(ctx, `
		DELETE FROM cb_claims
		WHERE status = $2 AND visible_at < now() - $1::interval
	`, retention, statusDead)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		DELETE FROM cb_messages m
		WHERE created_at < now() - $1::interval
		  AND NOT EXISTS (SELECT 1 FROM cb_claims c WHERE c.message_id = m.id)
	`, retention)
	return err
}

// Output unmarshals into dest the result recorded by the workflow's job of this
// type. It takes a destination rather than returning the JSON because the caller
// that reads a result is the one that wrote it and knows its type; a caller that
// wants the JSON untouched passes a *json.RawMessage.
//
// Returns ErrNotFound when no job of the type recorded one, and ErrAmbiguous
// when several did — a fan-out, which Outputs reads.
func Output(ctx context.Context, db Conn, groupID int64, t *JobType, dest any) error {
	bodies, err := Outputs(ctx, db, groupID, t)
	if err != nil {
		return err
	}
	switch len(bodies) {
	case 0:
		return ErrNotFound
	case 1:
		return json.Unmarshal(bodies[0], dest)
	default:
		return ErrAmbiguous
	}
}

// Outputs returns the results recorded by the workflow's jobs of this type, in
// the order the jobs were created — the fan-out read. Any count, zero included.
func Outputs(ctx context.Context, db Conn, groupID int64, t *JobType) ([]json.RawMessage, error) {
	rows, err := db.Query(ctx, `
		SELECT output FROM cb_outputs
		WHERE group_id = $1 AND job_type = $2
		ORDER BY message_id
	`, groupID, t.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bodies []json.RawMessage
	for rows.Next() {
		var body json.RawMessage
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		bodies = append(bodies, body)
	}
	return bodies, rows.Err()
}

// batchArrays turns messages into the three parallel arrays the batch
// statements unnest, so the number of messages is not limited by the number of
// statement parameters.
func batchArrays(msgs []BatchMessage) (topics []string, payloads [][]byte, dedupKeys []*string, err error) {
	topics = make([]string, len(msgs))
	payloads = make([][]byte, len(msgs))
	dedupKeys = make([]*string, len(msgs))
	for i, msg := range msgs {
		body, err := json.Marshal(msg.Payload)
		if err != nil {
			return nil, nil, nil, err
		}
		topics[i], payloads[i], dedupKeys[i] = msg.Topic, body, nullString(msg.DedupKey)
	}
	return topics, payloads, dedupKeys, nil
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
