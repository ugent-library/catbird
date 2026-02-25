package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func uniqueQueueName(base string) string {
	return fmt.Sprintf("%s_%d", base, time.Now().UnixNano())
}

func TestBindExactTopic(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q1 := uniqueQueueName("bind_exact_q1")
	q2 := uniqueQueueName("bind_exact_q2")

	if err := client.CreateQueue(ctx, q1); err != nil {
		t.Fatal(err)
	}
	if err := client.CreateQueue(ctx, q2); err != nil {
		t.Fatal(err)
	}

	// Bind q1 to exact topic
	if err := client.Bind(ctx, q1, "events.user.created"); err != nil {
		t.Fatal(err)
	}

	// Bind q2 to different exact topic
	if err := client.Bind(ctx, q2, "events.user.deleted"); err != nil {
		t.Fatal(err)
	}

	// Dispatch to first topic
	if err := client.Publish(ctx, "events.user.created", map[string]string{"id": "123"}); err != nil {
		t.Fatal(err)
	}

	// Check q1 receives message
	msgs, err := client.Read(ctx, q1, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message in q1, got %d", len(msgs))
	}
	if msgs[0].Topic != "events.user.created" {
		t.Fatalf("expected topic 'events.user.created', got %s", msgs[0].Topic)
	}

	// Check q2 is empty
	msgs, err = client.Read(ctx, q2, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages in q2, got %d", len(msgs))
	}
}

func TestBindSingleTokenWildcard(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := uniqueQueueName("bind_single_wildcard_q")
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Bind with ? wildcard (single token)
	if err := client.Bind(ctx, q, "events.?.created"); err != nil {
		t.Fatal(err)
	}

	// Should match: events.<anything>.created
	testCases := []struct {
		topic       string
		shouldMatch bool
	}{
		{"events.user.created", true},
		{"events.order.created", true},
		{"events.product.created", true},
		{"events.user.deleted", false},      // wrong last token
		{"events.user.item.created", false}, // too many tokens
		{"events.created", false},           // too few tokens
	}

	for _, tc := range testCases {
		if err := client.Publish(ctx, tc.topic, map[string]string{"test": "data"}); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := client.Read(ctx, q, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	matchCount := 0
	for _, tc := range testCases {
		if tc.shouldMatch {
			matchCount++
		}
	}

	if len(msgs) != matchCount {
		t.Fatalf("expected %d matching messages, got %d", matchCount, len(msgs))
	}

	// Verify all received messages match the pattern
	for _, msg := range msgs {
		found := false
		for _, tc := range testCases {
			if tc.topic == msg.Topic && tc.shouldMatch {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unexpected message with topic %s", msg.Topic)
		}
	}
}

func TestBindMultiTokenWildcard(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := uniqueQueueName("bind_multi_wildcard_q")
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Bind with * wildcard (multi-token tail)
	if err := client.Bind(ctx, q, "events.user.*"); err != nil {
		t.Fatal(err)
	}

	// Should match: events.user.<anything>
	testCases := []struct {
		topic       string
		shouldMatch bool
	}{
		{"events.user.created", true},
		{"events.user.updated", true},
		{"events.user.deleted", true},
		{"events.user.profile.updated", true}, // multiple tokens after
		{"events.order.created", false},       // wrong prefix
		{"events.user", false},                // no tail (exact match fails)
	}

	for _, tc := range testCases {
		if err := client.Publish(ctx, tc.topic, map[string]string{"test": "data"}); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := client.Read(ctx, q, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	matchCount := 0
	for _, tc := range testCases {
		if tc.shouldMatch {
			matchCount++
		}
	}

	if len(msgs) != matchCount {
		t.Fatalf("expected %d matching messages, got %d", matchCount, len(msgs))
	}
}

func TestBindMultiplePatterns(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := uniqueQueueName("bind_multiple_q")
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Bind to multiple patterns
	patterns := []string{
		"events.user.created",
		"events.order.*",
		"logs.?.error",
	}

	for _, pattern := range patterns {
		if err := client.Bind(ctx, q, pattern); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		topic       string
		shouldMatch bool
	}{
		{"events.user.created", true},  // exact match
		{"events.order.created", true}, // * wildcard
		{"events.order.updated", true}, // * wildcard
		{"logs.app.error", true},       // ? wildcard
		{"logs.db.error", true},        // ? wildcard
		{"events.user.deleted", false}, // no match
		{"logs.app.info", false},       // wrong last token
		{"logs.app.db.error", false},   // too many tokens for ? pattern
	}

	for _, tc := range testCases {
		if err := client.Publish(ctx, tc.topic, map[string]string{"test": "data"}); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := client.Read(ctx, q, 20, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	matchCount := 0
	for _, tc := range testCases {
		if tc.shouldMatch {
			matchCount++
		}
	}

	if len(msgs) != matchCount {
		t.Fatalf("expected %d matching messages, got %d", matchCount, len(msgs))
	}
}

func TestBindFanout(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	// Create multiple queues
	queues := []string{uniqueQueueName("fanout_q1"), uniqueQueueName("fanout_q2"), uniqueQueueName("fanout_q3")}
	for _, q := range queues {
		if err := client.CreateQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}

	// Bind all to same exact topic
	for _, q := range queues {
		if err := client.Bind(ctx, q, "broadcast.message"); err != nil {
			t.Fatal(err)
		}
	}

	// Dispatch one message
	payload := map[string]string{"msg": "hello all"}
	if err := client.Publish(ctx, "broadcast.message", payload); err != nil {
		t.Fatal(err)
	}

	// All queues should receive the message
	for _, q := range queues {
		msgs, err := client.Read(ctx, q, 10, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 {
			t.Fatalf("queue %s: expected 1 message, got %d", q, len(msgs))
		}

		var received map[string]string
		if err := json.Unmarshal(msgs[0].Payload, &received); err != nil {
			t.Fatal(err)
		}
		if received["msg"] != "hello all" {
			t.Fatalf("queue %s: unexpected payload %v", q, received)
		}
	}
}

func TestUnbind(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := "unbind_q"
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Bind and verify
	if err := client.Bind(ctx, q, "test.topic"); err != nil {
		t.Fatal(err)
	}

	if err := client.Publish(ctx, "test.topic", "before unbind"); err != nil {
		t.Fatal(err)
	}

	msgs, err := client.Read(ctx, q, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message before unbind, got %d", len(msgs))
	}
	if _, err := client.Delete(ctx, q, msgs[0].ID); err != nil {
		t.Fatal(err)
	}

	// Unbind
	if err := client.Unbind(ctx, q, "test.topic"); err != nil {
		t.Fatal(err)
	}

	// Dispatch again - should not be received
	if err := client.Publish(ctx, "test.topic", "after unbind"); err != nil {
		t.Fatal(err)
	}

	msgs, err = client.Read(ctx, q, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages after unbind, got %d", len(msgs))
	}
}

func TestBindInvalidPatterns(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := "invalid_pattern_q"
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	invalidPatterns := []string{
		"foo.*.bar",     // * not at end
		"*.foo",         // * not after dot
		"foo*",          // * not after dot
		"foo..bar",      // double dots
		"foo.bar.",      // trailing dot
		".foo.bar",      // leading dot
		"foo bar",       // space
		"foo@bar",       // invalid char
		"foo.bar.*.baz", // * not at end
	}

	for _, pattern := range invalidPatterns {
		err := client.Bind(ctx, q, pattern)
		if err == nil {
			t.Errorf("expected error for invalid pattern %q, got nil", pattern)
		}
	}
}

func TestBindValidPatterns(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := "valid_pattern_q"
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	validPatterns := []string{
		"foo",
		"foo.bar",
		"foo.bar.baz",
		"foo.?.bar",
		"foo.?.?.bar",
		"foo.bar.*",
		"foo-bar.baz_qux",
		"FOO.Bar.123",
	}

	for _, pattern := range validPatterns {
		if err := client.Bind(ctx, q, pattern); err != nil {
			t.Errorf("unexpected error for valid pattern %q: %v", pattern, err)
		}
	}
}

func TestBindNonExistentQueue(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	err := client.Bind(ctx, "nonexistent_queue", "test.topic")
	if err == nil {
		t.Fatal("expected error binding to non-existent queue, got nil")
	}
}

func TestBindPrefixOptimization(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	// Test that prefix extraction works for performance
	q := uniqueQueueName("prefix_opt_q")
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Bind with long prefix before wildcard
	if err := client.Bind(ctx, q, "very.long.prefix.path.to.resource.*"); err != nil {
		t.Fatal(err)
	}

	// Should match
	if err := client.Publish(ctx, "very.long.prefix.path.to.resource.created", "match"); err != nil {
		t.Fatal(err)
	}

	// Should not match (different prefix)
	if err := client.Publish(ctx, "very.long.different.path.to.resource.created", "nomatch"); err != nil {
		t.Fatal(err)
	}

	msgs, err := client.Read(ctx, q, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (prefix optimization), got %d", len(msgs))
	}
}

func TestBindCaseSensitivity(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := uniqueQueueName("case_sensitive_q")
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Bind to patterns with uppercase and mixed case
	patterns := []string{
		"Events.User.Created",
		"API.?.Response",
		"LogEvents.*",
	}

	for _, pattern := range patterns {
		if err := client.Bind(ctx, q, pattern); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		topic       string
		shouldMatch bool
	}{
		{"Events.User.Created", true},       // Exact match with uppercase
		{"events.user.created", false},      // Case mismatch
		{"API.v1.Response", true},           // Wildcard with mixed case
		{"API.V1.Response", true},           // Different case in wildcard part
		{"api.v1.Response", false},          // Case mismatch in prefix
		{"LogEvents.Error.Critical", true},  // Multi-token wildcard
		{"logevents.error.critical", false}, // Case mismatch
	}

	for _, tc := range testCases {
		if err := client.Publish(ctx, tc.topic, tc.topic); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := client.Read(ctx, q, 20, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	expectedMatches := 0
	for _, tc := range testCases {
		if tc.shouldMatch {
			expectedMatches++
		}
	}

	if len(msgs) != expectedMatches {
		t.Fatalf("expected %d messages (case-sensitive), got %d", expectedMatches, len(msgs))
	}
}

func TestBindOrderIndependence(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := uniqueQueueName("order_test_q")
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Bind in one order
	patterns := []string{"a.b.c", "x.y.*", "p.?.q"}
	for _, p := range patterns {
		if err := client.Bind(ctx, q, p); err != nil {
			t.Fatal(err)
		}
	}

	// Dispatch messages
	topics := []string{"a.b.c", "x.y.z", "p.m.q"}
	for _, topic := range topics {
		if err := client.Publish(ctx, topic, "test"); err != nil {
			t.Fatal(err)
		}
	}

	msgs, err := client.Read(ctx, q, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	// Verify we got all expected topics
	receivedTopics := make([]string, len(msgs))
	for i, msg := range msgs {
		receivedTopics[i] = msg.Topic
	}
	sort.Strings(receivedTopics)
	sort.Strings(topics)

	for i := range topics {
		if receivedTopics[i] != topics[i] {
			t.Fatalf("topic mismatch at index %d: expected %s, got %s", i, topics[i], receivedTopics[i])
		}
	}
}

func TestBindRebind(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := uniqueQueueName("rebind_q")
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Bind
	if err := client.Bind(ctx, q, "test.topic"); err != nil {
		t.Fatal(err)
	}

	// Rebind same pattern (should be idempotent)
	if err := client.Bind(ctx, q, "test.topic"); err != nil {
		t.Fatal(err)
	}

	// Dispatch once
	if err := client.Publish(ctx, "test.topic", "data"); err != nil {
		t.Fatal(err)
	}

	// Should only receive one message (no duplicates from rebind)
	msgs, err := client.Read(ctx, q, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message (rebind should be idempotent), got %d", len(msgs))
	}
}

func TestBindEmptyPrefix(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	q := uniqueQueueName("empty_prefix_q")
	if err := client.CreateQueue(ctx, q); err != nil {
		t.Fatal(err)
	}

	// Pattern with no prefix before wildcard should still work
	// This tests edge case of empty prefix extraction
	if err := client.Bind(ctx, q, "?.bar"); err != nil {
		t.Fatal(err)
	}

	if err := client.Publish(ctx, "foo.bar", "test"); err != nil {
		t.Fatal(err)
	}

	msgs, err := client.Read(ctx, q, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message with empty prefix pattern, got %d", len(msgs))
	}
}

// TestBindingRaceQueueDeletion tests the race condition where a queue
// is deleted while cb_dispatch is iterating over bindings
func TestBindingRaceQueueDeletion(t *testing.T) {
	requireSlowTests(t)

	client := getTestClient(t)
	ctx := context.Background()

	// Create queue and bind
	queueName := "race_queue"
	topic := "race.topic"

	if err := client.CreateQueue(ctx, queueName); err != nil {
		t.Fatal(err)
	}

	if err := client.Bind(ctx, queueName, topic); err != nil {
		t.Fatal(err)
	}

	// Run concurrent dispatch and delete operations
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Goroutine 1: Continuously dispatch messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if err := client.Publish(ctx, topic, map[string]int{"value": i}); err != nil {
				errors <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Goroutine 2: Delete and recreate queue
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Let some dispatches happen first
		for i := 0; i < 5; i++ {
			if _, err := client.DeleteQueue(ctx, queueName); err != nil {
				errors <- err
				return
			}
			time.Sleep(5 * time.Millisecond)
			if err := client.CreateQueue(ctx, queueName); err != nil {
				errors <- err
				return
			}
			if err := client.Bind(ctx, queueName, topic); err != nil {
				errors <- err
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("race condition error: %v", err)
	}
}

// TestBindingConcurrentBindUnbind tests concurrent bind/unbind operations
func TestBindingConcurrentBindUnbind(t *testing.T) {
	requireSlowTests(t)

	client := getTestClient(t)
	ctx := context.Background()

	queueName := "concurrent_queue"
	patterns := []string{"pattern1", "pattern2", "pattern3"}

	if err := client.CreateQueue(ctx, queueName); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Concurrent binds
	for _, pattern := range patterns {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			p := pattern
			go func() {
				defer wg.Done()
				if err := client.Bind(ctx, queueName, p); err != nil {
					errors <- err
				}
			}()
		}
	}

	wg.Wait()

	// Now concurrent unbinds
	for _, pattern := range patterns {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			p := pattern
			go func() {
				defer wg.Done()
				// First unbind should succeed, rest should fail
				_ = client.Unbind(ctx, queueName, p)
			}()
		}
	}

	wg.Wait()
	close(errors)

	// Should have no errors from binds (idempotent)
	for err := range errors {
		t.Errorf("concurrent bind error: %v", err)
	}
}

// TestBindingDispatchDuringModification tests dispatch while bindings are being modified
func TestBindingDispatchDuringModification(t *testing.T) {
	requireSlowTests(t)

	client := getTestClient(t)
	ctx := context.Background()

	queueName := "modify_queue"
	topic := "modify.topic"

	if err := client.CreateQueue(ctx, queueName); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Goroutine 1: Continuously bind/unbind
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if err := client.Bind(ctx, queueName, topic); err != nil {
				errors <- err
				return
			}
			if err := client.Unbind(ctx, queueName, topic); err != nil {
				errors <- err
				return
			}
		}
	}()

	// Goroutine 2: Continuously dispatch
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			// This should never error, just may or may not find a binding
			if err := client.Publish(ctx, topic, map[string]int{"value": i}); err != nil {
				errors <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("dispatch during modification error: %v", err)
	}
}
