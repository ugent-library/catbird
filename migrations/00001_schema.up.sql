-- Every job's payload and every published message is one row here.
CREATE SEQUENCE cb_position_seq;

CREATE TABLE cb_messages (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    payload JSONB,
    deduplication_key TEXT, -- a second insert with the same key does nothing
    -- true for Publish, false for Enqueue. Only published messages get a position.
    stream BOOLEAN NOT NULL DEFAULT false,
    -- Place in the stream, set once by the assigner (see runtime.go) after the
    -- message committed, so positions follow commit order. A message from a
    -- long transaction gets its position when it commits. Readers go by
    -- position, never by id, so no message is skipped. This is the one update
    -- cb_messages rows receive.
    position BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- for GC retention
);

-- Both unique indexes are partial, over the rows that have a value. A job's
-- message has neither a position nor usually a deduplication key, and a full
-- unique index stores an entry for every NULL: 1272 kB per index per 200k job
-- messages, probed by nothing. The deduplicating inserts name the predicate:
-- ON CONFLICT (deduplication_key) WHERE deduplication_key IS NOT NULL DO NOTHING.
CREATE UNIQUE INDEX cb_messages_position_idx ON cb_messages (position) WHERE position IS NOT NULL;
CREATE UNIQUE INDEX cb_messages_deduplication_key_idx ON cb_messages (deduplication_key) WHERE deduplication_key IS NOT NULL;

-- Stream reads: topic prefix (LIKE 'a.%') in position order. A job's message
-- has no position and is not in this index.
CREATE INDEX cb_messages_topic_position_idx ON cb_messages (topic text_pattern_ops, position) WHERE position IS NOT NULL;
-- The assigner's work list: published messages that have no position yet.
CREATE INDEX cb_messages_unassigned_idx ON cb_messages (id) WHERE stream AND position IS NULL;
-- What GC deletes by. Without it every GC run reads the whole table to find
-- the rows old enough to go, 5,200 buffers per 300k messages when none are;
-- with it a run reads the rows it deletes and an empty run reads three pages.
-- One more entry per message written, and one more on the assigner's position
-- update, which already rewrites every index entry because position is
-- indexed.
CREATE INDEX cb_messages_created_at_idx ON cb_messages (created_at);

-- One row per stream consumer: the highest position it has processed.
CREATE TABLE cb_cursors (
    name TEXT PRIMARY KEY,
    last_position BIGINT NOT NULL,
    -- When another process may claim the cursor. A Consume loop's claim sets it
    -- ClaimDuration ahead, so while it is in the future one process reads and
    -- acks under the cursor and the others wait, and a batch runs in one process
    -- at a time; when it passes, another process takes the cursor over. The ack
    -- matches on the value the claim returned, so a process whose claim ran out
    -- during its handler moves nothing and releases nothing. '-infinity' means
    -- claimable now: a cursor that is only ever read and acked directly, as
    -- wire's are, stays there.
    claimable_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity'
) WITH (fillfactor = 90);

-- One row per job that still has to run. The statement that ends the job —
-- completion, the last failed attempt, or Cancel — deletes the row and writes
-- the job's result to cb_job_results below, so what a job is doing is here and
-- how it ended is there, never both. claimable_at is when a worker may next
-- take the job: the start time of a delayed job, the end of a retry's backoff,
-- and on a claimed job the claim deadline, past which any worker may claim it
-- again.
--
-- Columns run widest first so the fixed-width ones pack without padding: 72
-- bytes a row instead of 75. Grouping them by role costs those 3 bytes on every
-- claim and retry, so the comments carry the grouping.
CREATE TABLE cb_jobs (
    message_id BIGINT PRIMARY KEY REFERENCES cb_messages (id) ON DELETE CASCADE,
    -- The workflow this job belongs to: the id of the job that started it.
    -- NULL for a job that started one or stands alone, so the volume of
    -- single-shot jobs stays out of cb_jobs_group_idx below and pays nothing
    -- for it on every claim and retry. A job's own group is
    -- coalesce(group_id, message_id).
    group_id BIGINT,
    claimable_at TIMESTAMPTZ NOT NULL,
    -- Incremented on claim, and every write matches on it, so an attempt that
    -- lost its claim writes nothing.
    attempts SMALLINT NOT NULL DEFAULT 0,
    -- How many jobs this one is still waiting for; it runs when this reaches 0.
    dependencies SMALLINT NOT NULL DEFAULT 0,
    -- This job waits for a signal; the payload arrives in signal below. It sits
    -- on claimable_at = 'infinity', so the wait is a delay and needs no place in
    -- the ready index. Signal writes the payload and sets claimable_at to now().
    awaits_signal BOOLEAN NOT NULL DEFAULT false,
    queue TEXT NOT NULL, -- the claim key: which workers may take this job
    job_type TEXT NOT NULL, -- which handler runs it
    -- The key at most one live job of the type carries, from
    -- EnqueueOptions.UniqueKey. It is unique among live jobs and nothing more
    -- because this row exists only while the job is live: the statement that
    -- ends the job frees the key by deleting the row, whichever way the job
    -- ended, so no ending writes anything for it. NULL on a job enqueued
    -- without one, so the volume of single-shot jobs stays out of
    -- cb_jobs_unique_key_idx below.
    unique_key TEXT,
    -- Which jobs it waits for, in the order they were enqueued, so a handler
    -- reads the results of exactly the jobs it waited for instead of every job
    -- of their type in the workflow. NULL on a job that waited for nothing.
    dependency_job_ids BIGINT[],
    -- The other direction: the jobs waiting for this one. Completing it takes one
    -- off each of their dependencies. All three are set by the completion that
    -- created them, out of the ids one statement handed out, so nothing outside
    -- catbird holds a count and no count can disagree with its list.
    dependent_job_ids BIGINT[],
    -- The signal payload, once one arrived. It lives here and not in a table of
    -- its own because it is created, read and deleted with the job row.
    signal JSONB,
    -- What the last failed attempt returned, cut to 256 characters. The claim
    -- clears it, so text means the job is waiting to retry and no text means an
    -- attempt is running; both are otherwise a live job with claimable_at in the
    -- future. Not a run history: the next failure overwrites it, and the
    -- statement that ends the job copies the last one to cb_job_results. A job
    -- that never failed pays nothing, and 256 characters put the tuple at 332
    -- bytes against 72 empty. Past about 1.9 kB every failed attempt would
    -- compress the text and write it to a toast table.
    error TEXT
) WITH (
    -- This table is small and rewritten constantly; vacuum it when 1% of it changed.
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01
);
-- Only claimable rows are in the index; a job waiting for other jobs stays out
-- of it, and a job waiting for a signal sits at the far end on 'infinity' where
-- the claim's LIMIT never reaches it. The claim filters job_type on the heap,
-- so ready jobs of a type no running process handles are walked by every claim
-- on their queue: 1015 buffers and 7.2 ms per claim of 50 at 100k such rows,
-- against 102 and 0.03 ms with none. A job type whose consumer runs
-- occasionally, or can be down for a while, gets a queue of its own: the queue
-- leads this index, so its backlog sits in a range no other claim scans.
CREATE INDEX cb_jobs_ready_idx ON cb_jobs (queue, claimable_at) WHERE dependencies = 0;
-- What Cancel, Signal and GroupStatus probe. A worker cancels the group of
-- every job that fails in one, so without this a downstream outage runs one
-- full scan of cb_jobs per failed job: 834 buffers and 4.9 ms at 100k live
-- jobs, against 6 buffers here. Jobs outside a workflow stay out of a
-- structure that can never match them: 112 kB against 2128 kB with 1% of jobs
-- grouped.
CREATE INDEX cb_jobs_group_idx ON cb_jobs (group_id) WHERE group_id IS NOT NULL;
-- What Enqueue probes and conflicts on for a job with a unique key. Unique per
-- job type, so two types may carry the same key. Jobs without a key stay out
-- of it, as they stay out of cb_jobs_group_idx.
CREATE UNIQUE INDEX cb_jobs_unique_key_idx ON cb_jobs (job_type, unique_key) WHERE unique_key IS NOT NULL;

-- One row per job that ended, and how it ended: it completed, it failed
-- because its last attempt failed, or Cancel stopped it. The statement that
-- deletes the cb_jobs row writes it, so a result cannot outlive an attempt
-- that never finished, and a job is never in both tables. Status and
-- GroupStatus read it for a job that is no longer live, and GC deletes it
-- retention after ended_at, so how long a job took does not shorten how long
-- it can be inspected. History beyond retention is the application's own
-- table. Written once and read by inspection, so its width is not the
-- concern it is on cb_jobs; the columns still run widest first.
CREATE TABLE cb_job_results (
    message_id BIGINT PRIMARY KEY REFERENCES cb_messages (id) ON DELETE CASCADE,
    -- As on cb_jobs: NULL for a job that started a workflow or stands alone.
    group_id BIGINT,
    ended_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts SMALLINT NOT NULL, -- how many the job spent
    -- How the job ended: 'completed', 'failed' or 'canceled', the words Status
    -- reports. Stored, where a live job's state is derived, because nothing
    -- about a job that ended changes with time.
    state TEXT NOT NULL,
    queue TEXT NOT NULL,
    job_type TEXT NOT NULL,
    -- What the last failed attempt returned, cut to 256 characters, as on
    -- cb_jobs. On a failed job it is the error that ended it; a canceled job
    -- keeps the one it was retrying on, if any. NULL when no attempt failed.
    error TEXT,
    -- What the handler recorded with SetOutput. NULL on a job that recorded
    -- nothing and on every job that did not complete.
    output JSONB
);
-- What GroupStatus probes for a workflow's outcome and outputs. Jobs outside a
-- workflow stay out of it, as on cb_jobs.
CREATE INDEX cb_job_results_group_idx ON cb_job_results (group_id) WHERE group_id IS NOT NULL;
-- What Queues counts failed jobs from. Failed jobs are few; the table is not.
CREATE INDEX cb_job_results_failed_idx ON cb_job_results (queue) WHERE state = 'failed';
-- What GC deletes by, as cb_messages_created_at_idx is on cb_messages: 2,500
-- buffers per 300k results to find nothing without it, two pages with it. One
-- more entry per job that ends, on a row written once.
CREATE INDEX cb_job_results_ended_at_idx ON cb_job_results (ended_at);
