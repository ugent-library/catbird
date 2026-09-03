# Catbird

<p align="center">
	<img src="assets/banner.svg" alt="Catbird" width="984" />
</p>

Catbird is a PostgreSQL-backed stream for Go. Jobs, queues, and small workflows are built on the same immutable message substrate.

Published messages receive positions and form the stream. A job pairs a message with a row in `cb_jobs`, which gives it claims, retries, signals, and dependencies. PostgreSQL is the only coordinator; plain SQL migrations define the schema; workers scale by starting more processes.

## Why this exists

Catbird is for the gap between "just run it in-process" and "operate a full distributed queue stack".

- Keep infrastructure simple: PostgreSQL is enough.
- Keep behavior explicit: most operations are single SQL statements.
- Start with a durable stream, then turn messages into work where needed.
- Keep workflow logic in Go: enqueue, fan-out, join, signal, retry.
- Keep browser updates close to data with the optional wire package.

## Install

```bash
go get github.com/ugent-library/catbird
```

## Local PostgreSQL (for running examples)

```bash
docker compose up -d
```

Use this DSN in examples:

```text
postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable
```

## Quick start

This program:

1. connects to PostgreSQL,
2. applies the Catbird schema,
3. starts a runtime,
4. enqueues one job,
5. runs a typed handler.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
)

type welcomeEmail struct {
	UserID int64  `json:"user_id"`
	Email  string `json:"email"`
}

var (
	notifications = catbird.NewQueue("notifications", catbird.QueueOptions{
		BatchSize:     20,
		ClaimDuration: time.Minute,
	})

	sendWelcome = catbird.NewJobType("send-welcome-email", notifications, catbird.JobTypeOptions{
		MaxAttempts: 5,
	})
)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := catbird.MigrateUp(ctx, pool); err != nil {
		log.Fatal(err)
	}

	rt := catbird.New(pool, catbird.Options{})

	rt.Handle(sendWelcome, catbird.JobHandler(func(ctx context.Context, payload welcomeEmail, job *catbird.Job) error {
		log.Printf("send welcome email to user=%d email=%s", payload.UserID, payload.Email)
		return nil
	}))

	go rt.Start(ctx)

	workflowID, err := catbird.Enqueue(ctx, pool, sendWelcome, welcomeEmail{
		UserID: 42,
		Email:  "user@example.org",
	}, catbird.EnqueueOptions{
		DeduplicationKey: "welcome:42",
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("enqueued workflow id: %d", workflowID)

	time.Sleep(2 * time.Second)
}
```

## Workflow example: fan-out, join, and signal

This example starts one workflow, runs two jobs in parallel, waits for both, then waits for a human decision.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
)

type deposit struct {
	ID int64 `json:"id"`
}

type decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

type scanOutput struct {
	Clean bool `json:"clean"`
}

var (
	ingestQ  = catbird.NewQueue("ingest", catbird.QueueOptions{BatchSize: 8, ClaimDuration: 30 * time.Minute})
	depositQ = catbird.NewQueue("deposit", catbird.QueueOptions{BatchSize: 50, ClaimDuration: time.Minute})

	submitted = catbird.NewJobType("submitted", depositQ, catbird.JobTypeOptions{})
	extract   = catbird.NewJobType("extract", ingestQ, catbird.JobTypeOptions{MaxAttempts: 3})
	scan      = catbird.NewJobType("scan", ingestQ, catbird.JobTypeOptions{MaxAttempts: 3})
	review    = catbird.NewJobType("review", depositQ, catbird.JobTypeOptions{Signal: true})
	publish   = catbird.NewJobType("publish", depositQ, catbird.JobTypeOptions{MaxAttempts: 5})
	archive   = catbird.NewJobType("archive", depositQ, catbird.JobTypeOptions{})
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := catbird.MigrateUp(ctx, pool); err != nil {
		log.Fatal(err)
	}

	rt := catbird.New(pool, catbird.Options{})
	rt.Handle(submitted, catbird.JobHandler(handleSubmitted))
	rt.HandleFunc(extract, handleExtract)
	rt.Handle(scan, catbird.JobHandler(handleScan))
	rt.Handle(review, catbird.JobHandler(func(ctx context.Context, dep deposit, job *catbird.Job) error {
		return handleReview(ctx, pool, dep, job)
	}))
	rt.HandleFunc(publish, handlePublish)
	rt.HandleFunc(archive, handleArchive)
	go rt.Start(ctx)

	groupID, err := catbird.Enqueue(ctx, pool, submitted, deposit{ID: 42}, catbird.EnqueueOptions{
		DeduplicationKey: "deposit:42",
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := catbird.Signal(ctx, pool, groupID, review, decision{Approved: true, By: "ann"}); err != nil {
		if !errors.Is(err, catbird.ErrNotFound) {
			log.Fatal(err)
		}
	}

	time.Sleep(2 * time.Second)
}

func handleSubmitted(ctx context.Context, dep deposit, job *catbird.Job) error {
	job.Enqueue(extract, dep)
	job.Enqueue(scan, dep)
	job.EnqueueAfter(review, dep)
	return nil
}

func handleScan(ctx context.Context, dep deposit, job *catbird.Job) error {
	clean := dep.ID%2 == 0
	return job.SetOutput(scanOutput{Clean: clean})
}

func handleReview(ctx context.Context, db catbird.Conn, dep deposit, job *catbird.Job) error {
	var d decision
	if err := json.Unmarshal(job.Signal, &d); err != nil {
		return err
	}

	deps, err := job.DependencyOutputs(ctx, db)
	if err != nil {
		return err
	}
	var scanned scanOutput
	if err := deps.Get(scan, &scanned); err != nil {
		return err
	}

	switch {
	case !scanned.Clean:
		job.Enqueue(archive, dep)
	case d.Approved:
		job.Enqueue(publish, dep)
	}
	return nil
}

func handleExtract(ctx context.Context, job *catbird.Job) error { return nil }
func handlePublish(ctx context.Context, job *catbird.Job) error { return nil }
func handleArchive(ctx context.Context, job *catbird.Job) error { return nil }
```

## Transactional completion

Use this pattern when your app writes data and must complete the job in the same transaction.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
)

var (
	queue = catbird.NewQueue("tx-example", catbird.QueueOptions{ClaimDuration: time.Minute})
	task  = catbird.NewJobType("tx-task", queue, catbird.JobTypeOptions{})
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := catbird.MigrateUp(ctx, pool); err != nil {
		log.Fatal(err)
	}

	rt := catbird.New(pool, catbird.Options{})
	rt.HandleFunc(task, func(ctx context.Context, job *catbird.Job) error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if _, err := tx.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}

		if err := job.SetOutput(map[string]any{"ok": true}); err != nil {
			return err
		}

		if err := catbird.Complete(ctx, tx, job); err != nil {
			return err
		}

		return tx.Commit(ctx)
	})

	go rt.Start(ctx)

	if _, err := catbird.Enqueue(ctx, pool, task, map[string]any{"n": 1}, catbird.EnqueueOptions{}); err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)
}
```

## Stream usage

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := catbird.MigrateUp(ctx, pool); err != nil {
		log.Fatal(err)
	}

	// A consumer claims its cursor, handles a batch, and acks. Every process
	// running this registers it, and one process at a time holds the cursor.
	rt := catbird.New(pool, catbird.Options{})
	rt.Consume("indexer", []string{"record.#"}, func(ctx context.Context, msgs []catbird.Message) error {
		for _, m := range msgs {
			log.Printf("position=%d topic=%s payload=%s", m.Position, m.Topic, string(m.Payload))
		}
		return nil
	}, catbird.ConsumeOptions{})
	// Start runs the consumer and the loops that give published messages their positions.
	go rt.Start(ctx)

	if _, err := catbird.Publish(ctx, pool, "record.42.updated", map[string]any{"id": 42}, "record:42:v1"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)
}
```

## Browser delivery with wire

The wire package turns stream messages into HTML fragments and serves them over polling.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ugent-library/catbird"
	"github.com/ugent-library/catbird/wire"
)

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := catbird.MigrateUp(ctx, pool); err != nil {
		log.Fatal(err)
	}

	// Start a runtime so stream messages receive positions.
	go catbird.New(pool, catbird.Options{}).Start(ctx)

	rd := wire.NewRenderer()
	rd.HandleFunc("record.work.{id}.#", func(r *http.Request, m wire.Match, f *wire.Fragment) error {
		_, err := fmt.Fprintf(f, "<div id=\"record-%s\" hx-swap-oob=\"true\">%d updates</div>", m.Var("id"), len(m.Messages))
		return err
	})

	w := wire.New(pool, rd, wire.Options{Secret: []byte("dev-secret")})

	http.HandleFunc("/events", func(rw http.ResponseWriter, r *http.Request) {
		token := w.Token("tray:1", "record.work.#")
		w.ServePoll(rw, r, token)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

## API reference

### Core types

- Queue declaration: `NewQueue(name string, opts QueueOptions) *Queue`
- Job type declaration: `NewJobType(name string, queue *Queue, opts JobTypeOptions) *JobType`
- Runtime: `New(pool *pgxpool.Pool, opts Options) *Runtime`
- Handler adapters: `HandlerFunc`, `JobHandler`
- Claimed job: `Job`

### Runtime methods

- Register handlers: `(*Runtime).Handle`, `(*Runtime).HandleFunc`
- Register stream trigger: `(*Runtime).Trigger`
- Register stream consumer: `(*Runtime).Consume`
- Start loops: `(*Runtime).Start`

### Job and workflow operations

- Enqueue one job: `Enqueue`
- Enqueue many jobs of one type: `EnqueueBatch`
- Complete a claimed job: `Complete`
- Deliver signal payload: `Signal`
- Cancel live jobs in a workflow: `Cancel`
- Read status: `Status`, `GroupStatus`, `Queues`
- Collect old rows: `GC`, or set `Options.Retention` and the runtime runs it hourly

### Stream operations

- Publish one: `Publish`
- Publish many: `PublishBatch`
- Read with named cursor: `Cursor.Read`, `Cursor.Ack`
- Read with caller-held position: `ReadAfter`
- Stream bounds: `LastPosition`, `OldestPosition`

### Migrations

- Apply: `MigrateUp`
- Roll back to version: `MigrateDownTo`
- Read parsed migrations: `Migrations`
- Embedded migration FS for external tools: `MigrationsFS`

### wire package

- Renderer setup: `wire.NewRenderer`, `(*Renderer).Handle`, `(*Renderer).HandleFunc`
- Token flow: `(*Wire).Token`, `(*Wire).Verify`
- Poll transport: `(*Wire).ServePoll`, `(*Wire).Serve`

## Operational notes

- PostgreSQL is the only coordinator.
- Jobs are queued in `cb_jobs`; stream messages are in `cb_messages`.
- Stream readers use `position` order.
- `Signal` targets workflow + job type, not a raw job id.
- `EnqueueAfter` waits on jobs buffered by the same handler completion.

## Running tests

```bash
docker compose up -d
psql postgres://postgres:postgres@localhost:5432/postgres -c 'CREATE DATABASE cb_tst'
go test ./...
```