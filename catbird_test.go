package catbird

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var testClient *Client
var testOnce sync.Once

func getTestClient(t *testing.T) *Client {
	testOnce.Do(func() {
		dsn := os.Getenv("CB_CONN")

		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		MigrateDownTo(t.Context(), db, 0)
		MigrateUpTo(t.Context(), db, SchemaVersion)

		pool, err := pgxpool.New(t.Context(), dsn)
		if err != nil {
			t.Fatal(err)
		}

		testClient = New(pool)
	})

	return testClient
}

func TestQueueCreate(t *testing.T) {
	client := getTestClient(t)

	err := client.CreateQueue(t.Context(), "simple_queue")
	if err != nil {
		t.Fatal(err)
	}

	info, err := client.GetQueue(t.Context(), "simple_queue")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "simple_queue" {
		t.Fatalf("unexpected queue name: %s", info.Name)
	}
}

func TestQueueCreateWithTopics(t *testing.T) {
	client := getTestClient(t)

	err := client.CreateQueueWithOpts(t.Context(), "test_queue", QueueOpts{
		Topics: []string{"topic1", "topic2"},
	})
	if err != nil {
		t.Fatal(err)
	}

	info, err := client.GetQueue(t.Context(), "test_queue")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "test_queue" {
		t.Fatalf("unexpected queue name: %s", info.Name)
	}
	if len(info.Topics) != 2 || info.Topics[0] != "topic1" || info.Topics[1] != "topic2" {
		t.Fatalf("unexpected queue topics: %v", info.Topics)
	}
}

func TestQueueSendAndRead(t *testing.T) {
	client := getTestClient(t)

	queueName := "send_read_queue"
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	type TestPayload struct {
		Message string `json:"message"`
		Count   int    `json:"count"`
	}

	payload := TestPayload{Message: "hello", Count: 42}
	err = client.Send(t.Context(), queueName, payload)
	if err != nil {
		t.Fatal(err)
	}

	messages, err := client.Read(t.Context(), queueName, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	var received TestPayload
	err = json.Unmarshal(messages[0].Payload, &received)
	if err != nil {
		t.Fatal(err)
	}
	if received.Message != "hello" || received.Count != 42 {
		t.Fatalf("unexpected payload: %+v", received)
	}
}

func TestQueueDispatch(t *testing.T) {
	client := getTestClient(t)

	queueName := "dispatch_queue"
	err := client.CreateQueueWithOpts(t.Context(), queueName, QueueOpts{
		Topics: []string{"event_topic"},
	})
	if err != nil {
		t.Fatal(err)
	}

	type Event struct {
		EventType string `json:"event_type"`
		Data      string `json:"data"`
	}

	event := Event{EventType: "test_event", Data: "test_data"}
	err = client.Dispatch(t.Context(), "event_topic", event)
	if err != nil {
		t.Fatal(err)
	}

	messages, err := client.Read(t.Context(), queueName, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if messages[0].Topic != "event_topic" {
		t.Fatalf("expected topic 'event_topic', got %s", messages[0].Topic)
	}
}

func TestQueueDelete(t *testing.T) {
	client := getTestClient(t)

	queueName := "delete_queue"
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Send(t.Context(), queueName, "test")
	if err != nil {
		t.Fatal(err)
	}

	messages, err := client.Read(t.Context(), queueName, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	deleted, err := client.Delete(t.Context(), queueName, messages[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("expected message to be deleted")
	}

	messages, err = client.Read(t.Context(), queueName, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(messages))
	}
}

func TestQueueHide(t *testing.T) {
	client := getTestClient(t)

	queueName := "hide_queue"
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Send(t.Context(), queueName, "test message")
	if err != nil {
		t.Fatal(err)
	}

	messages, err := client.Read(t.Context(), queueName, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	hidden, err := client.Hide(t.Context(), queueName, messages[0].ID, 1*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !hidden {
		t.Fatal("expected message to be hidden")
	}

	// Message should be hidden now
	messages, err = client.Read(t.Context(), queueName, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages (hidden), got %d", len(messages))
	}
}

func TestQueueHideExpiry(t *testing.T) {
	client := getTestClient(t)

	queueName := "hide_expiry_queue"
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Send(t.Context(), queueName, "test message")
	if err != nil {
		t.Fatal(err)
	}

	// Read message
	messages, err := client.Read(t.Context(), queueName, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	msgID := messages[0].ID

	// Hide for a short duration (100ms)
	hidden, err := client.Hide(t.Context(), queueName, msgID, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !hidden {
		t.Fatal("expected Hide to return true for existing message")
	}

	// Try hiding the same message again with longer duration - should succeed
	hidden, err = client.Hide(t.Context(), queueName, msgID, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !hidden {
		t.Fatal("expected Hide to return true on second call")
	}

	// Try hiding non-existent message - should return false
	hidden, err = client.Hide(t.Context(), queueName, 99999, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if hidden {
		t.Fatal("expected Hide to return false for non-existent message")
	}
}

func TestQueueListQueues(t *testing.T) {
	client := getTestClient(t)

	q1 := "list_queue_1"
	q2 := "list_queue_2"

	err := client.CreateQueue(t.Context(), q1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateQueue(t.Context(), q2)
	if err != nil {
		t.Fatal(err)
	}

	queues, err := client.ListQueues(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	var foundQ1, foundQ2 bool
	for _, q := range queues {
		if q.Name == q1 {
			foundQ1 = true
		}
		if q.Name == q2 {
			foundQ2 = true
		}
	}

	if !foundQ1 || !foundQ2 {
		t.Fatalf("not all queues found in list")
	}
}

func TestFlows(t *testing.T) {
	client := getTestClient(t)

	type Task1Input struct {
		Str string `json:"str"`
	}

	task1 := NewTask("task1", func(ctx context.Context, in Task1Input) (string, error) {
		return in.Str + " processed by task 1", nil
	})

	type flow1Output struct {
		Step1 string `json:"step1"`
		Step2 string `json:"step2"`
		Step3 string `json:"step3"`
	}

	flow1 := NewFlow("flow1",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed by step 1", nil
		}),
		StepWithOneDependency("step2",
			Dependency("step1"),
			func(ctx context.Context, in string, step1Out string) (string, error) {
				return step1Out + " and by step 2", nil
			}),
		StepWithOneDependency("step3",
			Dependency("step2"),
			func(ctx context.Context, in string, step2Out string) (string, error) {
				return step2Out + " and by step 3", nil
			}),
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	worker, err := client.NewWorker(t.Context(),
		WithLogger(logger),
		WithFlow(flow1),
		WithTask(task1),
	)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		err := worker.Start(t.Context())
		if err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	func() {
		h, err := client.RunTask(t.Context(), "task1", Task1Input{Str: "input"})
		if err != nil {
			t.Fatal(err)
		}
		var out string
		if err := h.WaitForOutput(t.Context(), &out); err != nil {
			t.Fatal(err)
		}
		if out != "input processed by task 1" {
			t.Fatalf("unexpected task output: %s", out)
		}
	}()

	func() {
		h, err := client.RunFlow(t.Context(), "flow1", "input")
		if err != nil {
			t.Fatal(err)
		}
		var out flow1Output
		if err := h.WaitForOutput(t.Context(), &out); err != nil {
			t.Fatal(err)
		}
		if out.Step1 != "input processed by step 1" {
			t.Fatalf("unexpected flow output for step1: %s", out.Step1)
		}
		if out.Step2 != "input processed by step 1 and by step 2" {
			t.Fatalf("unexpected flow output for step2: %s", out.Step2)
		}
		if out.Step3 != "input processed by step 1 and by step 2 and by step 3" {
			t.Fatalf("unexpected flow output for step3: %s", out.Step3)
		}
	}()
}
