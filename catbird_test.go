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

	messages, err := client.Read(t.Context(), queueName, 1, 30*time.Second)
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

	messages, err := client.Read(t.Context(), queueName, 1, 30*time.Second)
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

	messages, err := client.Read(t.Context(), queueName, 1, 30*time.Second)
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

	messages, err = client.Read(t.Context(), queueName, 1, 30*time.Second)
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

	messages, err := client.Read(t.Context(), queueName, 1, 30*time.Second)
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
	messages, err = client.Read(t.Context(), queueName, 1, 30*time.Second)
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
	messages, err := client.Read(t.Context(), queueName, 1, 30*time.Second)
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

func TestTaskCreate(t *testing.T) {
	client := getTestClient(t)

	task := NewTask("test_task", func(ctx context.Context, in string) (string, error) {
		return in + " processed", nil
	})

	err := client.CreateTask(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}

	info, err := client.GetTask(t.Context(), "test_task")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "test_task" {
		t.Fatalf("unexpected task name: %s", info.Name)
	}
}

func TestTaskRunAndWait(t *testing.T) {
	client := getTestClient(t)

	type TaskInput struct {
		Value int `json:"value"`
	}

	task := NewTask("math_task", func(ctx context.Context, in TaskInput) (int, error) {
		return in.Value * 2, nil
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	worker, err := client.NewWorker(t.Context(),
		WithLogger(logger),
		WithTask(task),
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

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunTask(t.Context(), "math_task", TaskInput{Value: 21})
	if err != nil {
		t.Fatal(err)
	}

	var result int
	if err := h.WaitForOutput(t.Context(), &result); err != nil {
		t.Fatal(err)
	}

	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestFlowCreate(t *testing.T) {
	client := getTestClient(t)

	flow := NewFlow("test_flow",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed", nil
		}),
	)

	err := client.CreateFlow(t.Context(), flow)
	if err != nil {
		t.Fatal(err)
	}

	// Test that we can run the flow (implicit verification it was created)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	worker, err := client.NewWorker(t.Context(),
		WithLogger(logger),
		WithFlow(flow),
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

	time.Sleep(100 * time.Millisecond)

	type FlowOutput struct {
		Step1 string `json:"step1"`
	}

	h, err := client.RunFlow(t.Context(), "test_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	var out FlowOutput
	if err := h.WaitForOutput(t.Context(), &out); err != nil {
		t.Fatal(err)
	}

	if out.Step1 != "input processed" {
		t.Fatalf("unexpected output: %s", out.Step1)
	}
}

func TestFlowSingleStep(t *testing.T) {
	client := getTestClient(t)

	type FlowOutput struct {
		Step1 string `json:"step1"`
	}

	flow := NewFlow("single_step_flow",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in + " processed by step 1", nil
		}),
	)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	worker, err := client.NewWorker(t.Context(),
		WithLogger(logger),
		WithFlow(flow),
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

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "single_step_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	var out FlowOutput
	if err := h.WaitForOutput(t.Context(), &out); err != nil {
		t.Fatal(err)
	}

	if out.Step1 != "input processed by step 1" {
		t.Fatalf("unexpected output: %s", out.Step1)
	}
}

func TestFlowWithDependencies(t *testing.T) {
	client := getTestClient(t)

	// Flow structure: step1 -> step2 -> step3 (linear chain)
	type FlowOutput struct {
		Step1 string `json:"step1"`
		Step2 string `json:"step2"`
		Step3 string `json:"step3"`
	}

	flow := NewFlow("dependency_flow",
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
		WithFlow(flow),
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

	// Give worker time to start
	time.Sleep(100 * time.Millisecond)

	h, err := client.RunFlow(t.Context(), "dependency_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	var out FlowOutput
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
}

func TestFlowListFlows(t *testing.T) {
	client := getTestClient(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create multiple flows
	flows := make([]*Flow, 2)

	flow1 := NewFlow("list_flow_1",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in, nil
		}),
	)

	flow2 := NewFlow("list_flow_2",
		InitialStep("step1", func(ctx context.Context, in string) (string, error) {
			return in, nil
		}),
	)

	flows[0] = flow1
	flows[1] = flow2

	err := client.CreateFlow(t.Context(), flow1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateFlow(t.Context(), flow2)
	if err != nil {
		t.Fatal(err)
	}

	// Start worker to execute flows
	worker, err := client.NewWorker(t.Context(),
		WithLogger(logger),
		WithFlow(flow1),
		WithFlow(flow2),
	)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := worker.Start(t.Context()); err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Verify both flows execute successfully
	type FlowOutput struct {
		Step1 string `json:"step1"`
	}

	h1, err := client.RunFlow(t.Context(), "list_flow_1", "input_1")
	if err != nil {
		t.Fatal(err)
	}

	var out1 FlowOutput
	if err := h1.WaitForOutput(t.Context(), &out1); err != nil {
		t.Fatal(err)
	}

	h2, err := client.RunFlow(t.Context(), "list_flow_2", "input_2")
	if err != nil {
		t.Fatal(err)
	}

	var out2 FlowOutput
	if err := h2.WaitForOutput(t.Context(), &out2); err != nil {
		t.Fatal(err)
	}

	if out1.Step1 != "input_1" || out2.Step1 != "input_2" {
		t.Fatalf("unexpected flow outputs")
	}
}

func TestTaskListTasks(t *testing.T) {
	client := getTestClient(t)

	task1 := NewTask("list_task_1", func(ctx context.Context, in string) (string, error) {
		return in, nil
	})

	task2 := NewTask("list_task_2", func(ctx context.Context, in string) (string, error) {
		return in, nil
	})

	err := client.CreateTask(t.Context(), task1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateTask(t.Context(), task2)
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := client.ListTasks(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	var found1, found2 bool
	for _, task := range tasks {
		if task.Name == "list_task_1" {
			found1 = true
		}
		if task.Name == "list_task_2" {
			found2 = true
		}
	}

	if !found1 || !found2 {
		t.Fatalf("not all tasks found in list")
	}
}
func TestFlowComplexDependencies(t *testing.T) {
	client := getTestClient(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Flow structure:
	//        step1
	//       /     \
	//    step2    step3
	//       \     /
	//        step4
	// Create a complex flow with multiple dependencies:
	flow := NewFlow("complex_flow",
		InitialStep("step1", func(ctx context.Context, in string) (int, error) {
			return 10, nil
		}),
		StepWithOneDependency("step2",
			Dependency("step1"),
			func(ctx context.Context, in string, step1Out int) (int, error) {
				return step1Out * 2, nil // 20
			}),
		StepWithOneDependency("step3",
			Dependency("step1"),
			func(ctx context.Context, in string, step1Out int) (int, error) {
				return step1Out * 3, nil // 30
			}),
		StepWithTwoDependencies("step4",
			Dependency("step2"),
			Dependency("step3"),
			func(ctx context.Context, in string, step2Out, step3Out int) (int, error) {
				return step2Out + step3Out, nil // 20 + 30 = 50
			}),
	)

	err := client.CreateFlow(t.Context(), flow)
	if err != nil {
		t.Fatal(err)
	}

	// Start worker to execute flow
	worker, err := client.NewWorker(t.Context(),
		WithLogger(logger),
		WithFlow(flow),
	)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := worker.Start(t.Context()); err != nil {
			t.Logf("worker error: %s", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Run the flow
	h, err := client.RunFlow(t.Context(), "complex_flow", "input")
	if err != nil {
		t.Fatal(err)
	}

	// Get results
	type ComplexOutput struct {
		Step1 int `json:"step1"`
		Step2 int `json:"step2"`
		Step3 int `json:"step3"`
		Step4 int `json:"step4"`
	}

	var out ComplexOutput
	if err := h.WaitForOutput(t.Context(), &out); err != nil {
		t.Fatal(err)
	}

	// Verify the computation graph was executed correctly
	if out.Step1 != 10 {
		t.Fatalf("expected step1=10, got %d", out.Step1)
	}
	if out.Step2 != 20 {
		t.Fatalf("expected step2=20, got %d", out.Step2)
	}
	if out.Step3 != 30 {
		t.Fatalf("expected step3=30, got %d", out.Step3)
	}
	if out.Step4 != 50 {
		t.Fatalf("expected step4=50, got %d", out.Step4)
	}
}
