package catbird_test

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
)

// Example_basicWorkflow demonstrates how to initialize the Catbird Lite client and worker,
// set up a leaderless cron, handle exactly-once DAG fanout, and gracefully cleanup.
func Example_basicWorkflow() {
	ctx := context.Background()

	// 1. Connect to DB
	// Note: You must run the 00001_lite.sql schema first on this testing database.
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/cb_tst?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// 2. Initialize Client
	client := catbird.NewClient()

	// Start the Background Assigner (Guarantees Stream Consumers never skip uncommitted transactions)
	catbird.StartAssigner(ctx, pool)

	// 3. Define the Handler (The user's code, absolutely no PL/pgSQL or DAG schemas needed)
	handler := func(ctx context.Context, tx catbird.DBRunner, msg catbird.Message) error {
		log.Printf("Processing Job ID: %d, Topic: %s", msg.ID, msg.Topic)

		if msg.Signals != nil {
			log.Printf("Received accumulated signals: %v", msg.Signals)
		}

		// Example exactly-once side-effect execution:
		// Because Catbird Lite automatically wraps this execution loop in a transaction,
		// any mutations we make here via `tx.Exec` are perfectly fenced!

		// If we do: tx.Exec(ctx, "UPDATE users SET money = money - 100 WHERE id = 1")
		// The engine natively commits this query and the teardown of the worker claim in a single step.
		// If the worker OOMs on the next line, the entire transaction rolls back cleanly.

		return nil
	}

	// 4. Start Managed Worker (The framework manages the complexities of leasing/backoff)
	worker := catbird.NewWorker(pool, "image_processing", handler).
		WithCleanup(func(ctx context.Context, tx catbird.DBRunner, msg catbird.Message) error {
			log.Printf("Job %d permanently failed after 5 retries. Cleanup logic executes here.", msg.ID)
			return nil
		})

	// Example Leaderless Cron
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			dedup := "cron:heartbeat:" + time.Now().Format("2006-01-02-15-04")
			client.Enqueue(ctx, catbird.WrapDBRunner(pool), "cron.minutely", "cron_workers", nil, &dedup, nil, 0)
		}
	}()

	// Example Trigger (Declarative Fan-out on Read bridging Stream messages to Job queues)
	client.RegisterTrigger(ctx, pool, "img_processor_sub", "image.%", "image_processing")

	go func() {
		time.Sleep(5 * time.Second)
		client.Enqueue(ctx, catbird.WrapDBRunner(pool), "image.uploaded", "image_processing", map[string]string{"url": "https://example.com/img.png"}, nil, nil, 0)
	}()

	log.Println("Starting Go Worker... (Press Ctrl+C to stop)")
	go worker.Start(ctx)

	// Keep alive briefly for the test runner
	time.Sleep(7 * time.Second)
}
