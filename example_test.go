package catbird_test

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
)

// A queue decides who competes with whom for worker slots, and how long one
// attempt may run. Extraction is slow and bursty, so it does not take slots
// from the short jobs that run once a deposit has been decided.
var (
	ingest  = catbird.NewQueue("ingest", catbird.QueueOptions{BatchSize: 8, ClaimDuration: 30 * time.Minute})
	deposit = catbird.NewQueue("deposit", catbird.QueueOptions{BatchSize: 50, ClaimDuration: time.Minute})
)

// A job type decides what kind of work a job is and how a run of it is retried.
// Review waits for a person, and the enqueue stamps that on the row, so a review
// job cannot be created by a caller that never sends a decision — and its
// handler is always given one.
var (
	submitted = catbird.NewJobType("submitted", deposit, catbird.JobTypeOptions{})
	extract   = catbird.NewJobType("extract", ingest, catbird.JobTypeOptions{MaxAttempts: 3})
	scan      = catbird.NewJobType("scan", ingest, catbird.JobTypeOptions{MaxAttempts: 3})
	review    = catbird.NewJobType("review", deposit, catbird.JobTypeOptions{Signal: true})
	publish   = catbird.NewJobType("publish", deposit, catbird.JobTypeOptions{MaxAttempts: 5})
	archive   = catbird.NewJobType("archive", deposit, catbird.JobTypeOptions{})
)

var pool *pgxpool.Pool

// Example_workflow shows a multi-step workflow: two steps in parallel, a step
// that waits for both and for a person's decision, and what follows chosen by
// that decision.
func Example_workflow() {
	ctx := context.Background()

	// The 00001_lite.sql schema must be applied first. The pool carries the
	// library's own statements — about eight — plus one connection for every
	// handler that holds a transaction, which is what BatchSize is set against.
	pool, _ = pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable&pool_max_conns=32")

	// What this process runs. A process that only enqueues registers nothing
	// and links none of these functions.
	rt := catbird.New(pool, catbird.Options{})
	rt.Handle(submitted, catbird.JobHandler(handleSubmitted))
	rt.HandleFunc(extract, handleExtract)
	rt.Handle(scan, catbird.JobHandler(handleScan))
	rt.Handle(review, catbird.JobHandler(handleReview))
	rt.HandleFunc(publish, handlePublish)
	rt.HandleFunc(archive, handleArchive)
	go rt.Start(ctx)

	// Starting one workflow. The id it returns is the workflow: signal it,
	// cancel it, read its results by it. It is the only thing the deposit row
	// has to keep.
	groupID, err := catbird.Enqueue(ctx, pool, submitted, 42, catbird.EnqueueOptions{
		DeduplicationKey: "deposit:42", // a retried POST does not start a second workflow
	})
	if err != nil {
		log.Fatal(err)
	}

	// Later, from an HTTP handler. ErrNotFound means nothing is waiting any
	// more: the workflow ended, or the decision arrived twice.
	if err := catbird.Signal(ctx, pool, groupID, review, decision{Approved: true, By: "ann"}); err != nil {
		log.Print(err)
	}
}

type decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

type scanResult struct {
	Clean   bool   `json:"clean"`
	Finding string `json:"finding"`
}

// handleSubmitted fans the workflow out. What it asks for is written by the
// statement that ends this job, so a crash in between cannot leave a submitted
// deposit with nothing started for it, and a retry starts with an empty buffer.
// It is registered through JobHandler, so the payload arrives decoded.
func handleSubmitted(ctx context.Context, depositID int, job *catbird.Job) error {
	job.Enqueue(extract, depositID)
	job.Enqueue(scan, depositID)
	job.EnqueueAfter(review, depositID) // after both of the above
	return nil
}

// handleScan records a result the review step reads later. It opens no
// transaction: the worker completes the job, and the completion writes what
// SetOutput recorded.
func handleScan(ctx context.Context, depositID int, job *catbird.Job) error {
	finding := virusCheck(depositID)
	return job.SetOutput(scanResult{Clean: finding == "", Finding: finding})
}

// handleReview runs when extraction and the scan finished and a decision
// arrived. The gate guarantees the decision is here, so there is no case for
// its absence.
func handleReview(ctx context.Context, depositID int, job *catbird.Job) error {
	var d decision
	if err := json.Unmarshal(job.Signal, &d); err != nil {
		return err
	}

	// The scan's result, read from the jobs this one waited for and addressed
	// by the job type: a job a handler asked for has no id anyone can hold.
	deps, err := job.DependencyOutputs(ctx, pool)
	if err != nil {
		return err
	}
	var scanned scanResult
	if err := deps.Get(scan, &scanned); err != nil {
		return err
	}

	switch {
	case !scanned.Clean:
		job.Enqueue(archive, depositID)
	case d.Approved:
		job.Enqueue(publish, depositID)
	}
	return nil
}

func handleExtract(ctx context.Context, job *catbird.Job) error { return nil }
func handlePublish(ctx context.Context, job *catbird.Job) error { return nil }
func handleArchive(ctx context.Context, job *catbird.Job) error { return nil }

func virusCheck(depositID int) string { return "" }
