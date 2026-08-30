package catbird_test

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
)

// Example_basicWorkflow shows a worker, a cron enqueue that is safe to run from
// several processes, and a trigger that turns stream messages into jobs.
func Example_basicWorkflow() {
	ctx := context.Background()

	// The 00001_lite.sql schema must be applied first. The pool carries the
	// library's own statements — about eight — plus one connection for every
	// handler that holds a transaction, which is what BatchSize is set against
	// below.
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable&pool_max_conns=32")
	if err != nil {
		log.Fatal(err)
	}

	// The handler is given no connection and opens one when it needs it. It
	// completes the job in its own transaction, so its writes and the end of
	// the job are one commit: the job runs again only if nothing was written.
	handler := func(ctx context.Context, job *catbird.Job) error {
		log.Printf("job %d on %s", job.ID, job.Topic)

		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// _, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance - 100 WHERE id = 1")
		if err := catbird.Complete(ctx, tx, job); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	rt := catbird.New(pool, catbird.Options{})
	catbird.NewWorker(rt, "image_processing", handler, catbird.WorkerOptions{
		// This handler holds a connection for its whole body, so the jobs that
		// run at once have to fit in the pool beside the library's own
		// statements. The default of 50 does not fit a default pool.
		BatchSize: 20,
		OnDead: func(ctx context.Context, job *catbird.Job) error {
			log.Printf("job %d failed permanently", job.ID)
			return nil
		},
	})

	// Cron without leader election: every process enqueues the same key each
	// minute; the dedup key lets exactly one insert through. The minute is
	// formatted in UTC and in this exact layout, because every process has to
	// produce the same key for the minute it is in.
	go func() {
		for {
			time.Sleep(time.Minute)
			key := "cron:heartbeat:" + time.Now().UTC().Format("2006-01-02T15:04Z")
			catbird.Enqueue(ctx, pool, "cron.minutely", "cron_workers", nil, catbird.EnqueueOptions{DedupKey: key})
		}
	}()

	// Every message on image.* becomes a job on image_processing.
	catbird.NewTrigger(rt, "img_processor", []string{"image.#"}, "image_processing", catbird.TriggerOptions{})

	go func() {
		time.Sleep(time.Second)
		catbird.Publish(ctx, pool, "image.uploaded", map[string]string{"url": "https://example.com/img.png"}, "")
	}()

	go rt.Start(ctx)
	time.Sleep(7 * time.Second)
}
