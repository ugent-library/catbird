package catbird

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestSendQuery(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}

	visibleAt := time.Unix(1700000000, 0).UTC()
	query, args, err := SendQuery(
		"test_queue",
		payload{Value: "hello"},
		SendOpts{
			Topic:          "topic.a",
			IdempotencyKey: "idem-1",
			VisibleAt:      visibleAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedQuery := `SELECT cb_send(queue => $1, payload => $2, topic => $3, idempotency_key => $4, visible_at => $5);`
	if query != expectedQuery {
		t.Fatalf("unexpected query: %s", query)
	}

	if len(args) != 5 {
		t.Fatalf("expected 5 args, got %d", len(args))
	}

	if gotQueue, ok := args[0].(string); !ok || gotQueue != "test_queue" {
		t.Fatalf("unexpected queue arg: %#v", args[0])
	}

	if gotPayload, ok := args[1].([]byte); !ok || string(gotPayload) != `{"value":"hello"}` {
		t.Fatalf("unexpected payload arg: %#v", args[1])
	}

	if gotTopic, ok := args[2].(*string); !ok || gotTopic == nil || *gotTopic != "topic.a" {
		t.Fatalf("unexpected topic arg: %#v", args[2])
	}

	if gotIdempotencyKey, ok := args[3].(*string); !ok || gotIdempotencyKey == nil || *gotIdempotencyKey != "idem-1" {
		t.Fatalf("unexpected idempotency key arg: %#v", args[3])
	}

	if gotVisibleAt, ok := args[4].(*time.Time); !ok || gotVisibleAt == nil || !gotVisibleAt.Equal(visibleAt) {
		t.Fatalf("unexpected visible_at arg: %#v", args[4])
	}

	_, nilArgs, err := SendQuery("test_queue", payload{Value: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nilArgs) != 5 {
		t.Fatalf("expected 5 args for nil opts, got %d", len(nilArgs))
	}
	if gotTopic, ok := nilArgs[2].(*string); !ok || gotTopic != nil {
		t.Fatalf("expected nil *string topic arg for nil opts, got %#v", nilArgs[2])
	}
	if gotIdempotencyKey, ok := nilArgs[3].(*string); !ok || gotIdempotencyKey != nil {
		t.Fatalf("expected nil *string idempotency key arg for nil opts, got %#v", nilArgs[3])
	}
	if gotVisibleAt, ok := nilArgs[4].(*time.Time); !ok || gotVisibleAt != nil {
		t.Fatalf("expected nil *time.Time visible_at arg for nil opts, got %#v", nilArgs[4])
	}
}

func TestSendManyQuery(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}

	visibleAt := time.Unix(1700000200, 0).UTC()
	query, args, err := SendManyQuery(
		"test_queue",
		[]any{payload{Value: "hello"}, payload{Value: "world"}},
		SendManyOpts{
			Topic:           "topic.a",
			IdempotencyKeys: []string{"idem-a", "idem-b"},
			VisibleAt:       visibleAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedQuery := `SELECT cb_send(queue => $1, payloads => $2, topic => $3, idempotency_keys => $4, visible_at => $5);`
	if query != expectedQuery {
		t.Fatalf("unexpected query: %s", query)
	}

	if len(args) != 5 {
		t.Fatalf("expected 5 args, got %d", len(args))
	}

	if gotQueue, ok := args[0].(string); !ok || gotQueue != "test_queue" {
		t.Fatalf("unexpected queue arg: %#v", args[0])
	}

	if gotPayloads, ok := args[1].(pgtype.FlatArray[json.RawMessage]); !ok || len(gotPayloads) != 2 || string(gotPayloads[0]) != `{"value":"hello"}` || string(gotPayloads[1]) != `{"value":"world"}` {
		t.Fatalf("unexpected payloads arg: %#v", args[1])
	}

	if gotTopic, ok := args[2].(*string); !ok || gotTopic == nil || *gotTopic != "topic.a" {
		t.Fatalf("unexpected topic arg: %#v", args[2])
	}

	if gotIdempotencyKeys, ok := args[4-1].([]string); !ok || len(gotIdempotencyKeys) != 2 || gotIdempotencyKeys[0] != "idem-a" || gotIdempotencyKeys[1] != "idem-b" {
		t.Fatalf("unexpected idempotency_keys arg: %#v", args[3])
	}

	if gotVisibleAt, ok := args[5-1].(*time.Time); !ok || gotVisibleAt == nil || !gotVisibleAt.Equal(visibleAt) {
		t.Fatalf("unexpected visible_at arg: %#v", args[4])
	}

	_, nilArgs, err := SendManyQuery("test_queue", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nilArgs) != 5 {
		t.Fatalf("expected 5 args for nil opts, got %d", len(nilArgs))
	}
	if gotPayloads, ok := nilArgs[1].(pgtype.FlatArray[json.RawMessage]); !ok || len(gotPayloads) != 0 {
		t.Fatalf("expected [] payloads for nil input, got %#v", nilArgs[1])
	}
	if gotTopic, ok := nilArgs[2].(*string); !ok || gotTopic != nil {
		t.Fatalf("expected nil *string topic arg for nil opts, got %#v", nilArgs[2])
	}
	if gotIdempotencyKeys, ok := nilArgs[3].([]string); !ok || gotIdempotencyKeys != nil {
		t.Fatalf("expected nil []string idempotency_keys arg for nil opts, got %#v", nilArgs[3])
	}
	if gotVisibleAt, ok := nilArgs[4].(*time.Time); !ok || gotVisibleAt != nil {
		t.Fatalf("expected nil *time.Time visible_at arg for nil opts, got %#v", nilArgs[4])
	}
}

func TestPublishQuery(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}

	visibleAt := time.Unix(1700000100, 0).UTC()
	query, args, err := PublishQuery(
		"topic.test",
		payload{Value: "world"},
		PublishOpts{
			IdempotencyKey: "idem-2",
			VisibleAt:      &visibleAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedQuery := `SELECT cb_publish(topic => $1, payload => $2, idempotency_key => $3, visible_at => $4);`
	if query != expectedQuery {
		t.Fatalf("unexpected query: %s", query)
	}

	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}

	if gotTopic, ok := args[0].(string); !ok || gotTopic != "topic.test" {
		t.Fatalf("unexpected topic arg: %#v", args[0])
	}

	if gotPayload, ok := args[1].([]byte); !ok || string(gotPayload) != `{"value":"world"}` {
		t.Fatalf("unexpected payload arg: %#v", args[1])
	}

	if gotIdempotencyKey, ok := args[2].(*string); !ok || gotIdempotencyKey == nil || *gotIdempotencyKey != "idem-2" {
		t.Fatalf("unexpected idempotency key arg: %#v", args[2])
	}

	if gotVisibleAt, ok := args[3].(**time.Time); !ok || gotVisibleAt == nil || *gotVisibleAt == nil || !(*gotVisibleAt).Equal(visibleAt) {
		t.Fatalf("unexpected visible_at arg: %#v", args[3])
	}

	_, nilArgs, err := PublishQuery("topic.test", payload{Value: "world"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nilArgs) != 4 {
		t.Fatalf("expected 4 args for nil opts, got %d", len(nilArgs))
	}
	if gotIdempotencyKey, ok := nilArgs[2].(*string); !ok || gotIdempotencyKey != nil {
		t.Fatalf("expected nil *string idempotency key arg for nil opts, got %#v", nilArgs[2])
	}
	if gotVisibleAt, ok := nilArgs[3].(**time.Time); !ok || gotVisibleAt != nil {
		t.Fatalf("expected nil **time.Time visible_at arg for nil opts, got %#v", nilArgs[3])
	}
}

func TestPublishManyQuery(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}

	visibleAt := time.Unix(1700000300, 0).UTC()
	query, args, err := PublishManyQuery(
		"topic.test",
		[]any{payload{Value: "alpha"}, payload{Value: "beta"}},
		PublishManyOpts{IdempotencyKeys: []string{"idem-1", "idem-2"}, VisibleAt: visibleAt},
	)
	if err != nil {
		t.Fatal(err)
	}

	expectedQuery := `SELECT cb_publish(topic => $1, payloads => $2, idempotency_keys => $3, visible_at => $4);`
	if query != expectedQuery {
		t.Fatalf("unexpected query: %s", query)
	}

	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}

	if gotTopic, ok := args[0].(string); !ok || gotTopic != "topic.test" {
		t.Fatalf("unexpected topic arg: %#v", args[0])
	}

	if gotPayloads, ok := args[1].(pgtype.FlatArray[json.RawMessage]); !ok || len(gotPayloads) != 2 || string(gotPayloads[0]) != `{"value":"alpha"}` || string(gotPayloads[1]) != `{"value":"beta"}` {
		t.Fatalf("unexpected payloads arg: %#v", args[1])
	}

	if gotIdempotencyKeys, ok := args[2].([]string); !ok || len(gotIdempotencyKeys) != 2 || gotIdempotencyKeys[0] != "idem-1" || gotIdempotencyKeys[1] != "idem-2" {
		t.Fatalf("unexpected idempotency_keys arg: %#v", args[2])
	}

	if gotVisibleAt, ok := args[3].(*time.Time); !ok || gotVisibleAt == nil || !gotVisibleAt.Equal(visibleAt) {
		t.Fatalf("unexpected visible_at arg: %#v", args[3])
	}

	_, nilArgs, err := PublishManyQuery("topic.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nilArgs) != 4 {
		t.Fatalf("expected 4 args for nil opts, got %d", len(nilArgs))
	}
	if gotPayloads, ok := nilArgs[1].(pgtype.FlatArray[json.RawMessage]); !ok || len(gotPayloads) != 0 {
		t.Fatalf("expected [] payloads for nil input, got %#v", nilArgs[1])
	}
	if gotIdempotencyKeys, ok := nilArgs[2].([]string); !ok || gotIdempotencyKeys != nil {
		t.Fatalf("expected nil []string idempotency_keys arg for nil opts, got %#v", nilArgs[2])
	}
	if gotVisibleAt, ok := nilArgs[3].(*time.Time); !ok || gotVisibleAt != nil {
		t.Fatalf("expected nil *time.Time visible_at arg for nil opts, got %#v", nilArgs[3])
	}
}

func TestQueueCreate(t *testing.T) {
	client := getTestClient(t)

	err := client.CreateQueue(t.Context(), "simple_queue", QueueOpts{Description: "A simple queue"})
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
	if info.Description != "A simple queue" {
		t.Fatalf("unexpected queue description: %s", info.Description)
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

func TestQueueSendManyAndRead(t *testing.T) {
	client := getTestClient(t)

	queueName := "send_many_read_queue"
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	type TestPayload struct {
		Message string `json:"message"`
		Count   int    `json:"count"`
	}

	err = client.SendMany(
		t.Context(),
		queueName,
		[]any{
			TestPayload{Message: "hello", Count: 1},
			TestPayload{Message: "world", Count: 2},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	messages, err := client.Read(t.Context(), queueName, 2, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	var first TestPayload
	err = json.Unmarshal(messages[0].Payload, &first)
	if err != nil {
		t.Fatal(err)
	}
	if first.Message != "hello" || first.Count != 1 {
		t.Fatalf("unexpected first payload: %+v", first)
	}

	var second TestPayload
	err = json.Unmarshal(messages[1].Payload, &second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Message != "world" || second.Count != 2 {
		t.Fatalf("unexpected second payload: %+v", second)
	}
}

func TestQueueSendManyIdempotencyLengthMismatch(t *testing.T) {
	client := getTestClient(t)

	queueName := "send_many_mismatch_queue"
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	err = client.SendMany(
		t.Context(),
		queueName,
		[]any{"a", "b"},
		SendManyOpts{IdempotencyKeys: []string{"only-one"}},
	)
	if err == nil {
		t.Fatal("expected SendMany to fail when idempotency key count does not match payload count")
	}
}

func TestQueuePublish(t *testing.T) {
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
	err = client.Publish(t.Context(), "event_topic", event)
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

func TestQueuePublishMany(t *testing.T) {
	client := getTestClient(t)

	queueName := "publish_many_queue"
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Bind(t.Context(), queueName, "event_topic_many"); err != nil {
		t.Fatal(err)
	}

	type Event struct {
		EventType string `json:"event_type"`
		Data      string `json:"data"`
	}

	err = client.PublishMany(
		t.Context(),
		"event_topic_many",
		[]any{
			Event{EventType: "evt_1", Data: "a"},
			Event{EventType: "evt_2", Data: "b"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	messages, err := client.Read(t.Context(), queueName, 2, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	if messages[0].Topic != "event_topic_many" || messages[1].Topic != "event_topic_many" {
		t.Fatalf("expected both messages to have topic event_topic_many, got %q and %q", messages[0].Topic, messages[1].Topic)
	}

	var first Event
	err = json.Unmarshal(messages[0].Payload, &first)
	if err != nil {
		t.Fatal(err)
	}
	if first.EventType != "evt_1" || first.Data != "a" {
		t.Fatalf("unexpected first payload: %+v", first)
	}

	var second Event
	err = json.Unmarshal(messages[1].Payload, &second)
	if err != nil {
		t.Fatal(err)
	}
	if second.EventType != "evt_2" || second.Data != "b" {
		t.Fatalf("unexpected second payload: %+v", second)
	}
}

func TestQueuePublishManyIdempotencyLengthMismatch(t *testing.T) {
	client := getTestClient(t)

	queueName := "publish_many_mismatch_queue"
	err := client.CreateQueue(t.Context(), queueName)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Bind(t.Context(), queueName, "event_topic_mismatch"); err != nil {
		t.Fatal(err)
	}

	err = client.PublishMany(
		t.Context(),
		"event_topic_mismatch",
		[]any{"a", "b"},
		PublishManyOpts{IdempotencyKeys: []string{"only-one"}},
	)
	if err == nil {
		t.Fatal("expected PublishMany to fail when idempotency key count does not match payload count")
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

	queueName := fmt.Sprintf("hide_queue_%d", time.Now().UnixNano())
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

	queueName := fmt.Sprintf("hide_expiry_queue_%d", time.Now().UnixNano())
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
