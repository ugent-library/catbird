package catbird

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFlows(t *testing.T) {
	pool, err := pgxpool.New(t.Context(), os.Getenv("TEST_PG_CONN"))
	if err != nil {
		t.Fatal(err)
	}

	client := New(pool)

	flow := &Flow{
		Name: "flow1",
		Steps: []Step{
			{Name: "step1"},
			{Name: "step2", DependsOn: []string{"step1"}},
		},
	}

	if err := client.CreateFlow(t.Context(), flow); err != nil {
		t.Fatal(err)
	}

	task1 := NewTask("step1", func(ctx context.Context, in string) (string, error) {
		return "processed by step 1", nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	task2 := NewTask("step2", func(ctx context.Context, in string) (string, error) {
		return "processed by step 2", nil
	}, TaskOpts{
		HideFor: 10 * time.Second,
	})

	worker, err := client.NewWorker(WorkerOpts{
		Logger: slog.Default(),
		Tasks:  []*Task{task1, task2},
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		worker.Start(t.Context())
	})

	wg.Go(func() {
		id, err := client.RunFlow(t.Context(), "flow1", "")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("flow run id: %s", id)
		time.Sleep(10 * time.Second)
		wg.Done()
	})

	wg.Wait()
}
