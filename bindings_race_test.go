package catbird

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestBindingRaceQueueDeletion tests the race condition where a queue
// is deleted while cb_dispatch is iterating over bindings
func TestBindingRaceQueueDeletion(t *testing.T) {
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
			if err := client.Dispatch(ctx, topic, map[string]int{"value": i}); err != nil {
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
			if err := client.Dispatch(ctx, topic, map[string]int{"value": i}); err != nil {
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
