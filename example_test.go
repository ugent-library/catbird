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

	// The 00001_lite.sql schema must be applied first.
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	client := catbird.NewClient()

	// The handler receives the worker's transaction. Writes made through tx
	// commit together with the job's completion, or not at all.
	handler := func(ctx context.Context, tx catbird.Conn, msg catbird.Message) error {
		log.Printf("job %d on %s", msg.ID, msg.Topic)
		// _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance - 100 WHERE id = 1")
		return nil
	}

	rt := catbird.New(pool, catbird.Options{})
	catbird.NewWorker(rt, "image_processing", handler, catbird.WorkerOptions{
		OnDead: func(ctx context.Context, db catbird.Conn, msg catbird.Message) error {
			log.Printf("job %d failed permanently", msg.ID)
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
			client.Enqueue(ctx, pool, "cron.minutely", "cron_workers", nil, catbird.EnqueueOptions{DedupKey: key})
		}
	}()

	// Every message on image.* becomes a job on image_processing.
	catbird.NewTrigger(rt, "img_processor", "image", "image_processing", catbird.StreamOptions{})

	go func() {
		time.Sleep(time.Second)
		client.Publish(ctx, pool, "image.uploaded", map[string]string{"url": "https://example.com/img.png"}, "")
	}()

	go rt.Start(ctx)
	time.Sleep(7 * time.Second)
}
