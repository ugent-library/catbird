package catbird

import (
	"encoding/json"
	"testing"
	"time"
)

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
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Bind(t.Context(), queueName, "event_topic"); err != nil {
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
