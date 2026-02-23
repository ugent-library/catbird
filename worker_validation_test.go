package catbird

import (
	"context"
	"testing"
	"time"
)

func TestWorkerValidatesTaskHandlerOpts(t *testing.T) {
	client := getTestClient(t)

	// Test invalid concurrency
	t.Run("negative_concurrency", func(t *testing.T) {
		task := NewTask("invalid_task").Handler(func(_ context.Context, _ any) (any, error) {
			return nil, nil
		}, &HandlerOpts{
			Concurrency: -1,
			BatchSize:   10,
		})

		worker := client.NewWorker(t.Context(), nil).AddTask(task, nil)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for negative concurrency")
		}
		if err.Error() != `task "invalid_task" has invalid handler options: concurrency must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Test invalid batch size
	t.Run("negative_batch_size", func(t *testing.T) {
		task := NewTask("invalid_task2").Handler(func(_ context.Context, _ any) (any, error) {
			return nil, nil
		}, &HandlerOpts{
			Concurrency: 1,
			BatchSize:   -1,
		})

		worker := client.NewWorker(t.Context(), nil).AddTask(task, nil)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for negative batch size")
		}
		if err.Error() != `task "invalid_task2" has invalid handler options: batch size must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Test invalid backoff config
	t.Run("invalid_backoff", func(t *testing.T) {
		task := NewTask("invalid_task3").Handler(func(_ context.Context, _ any) (any, error) {
			return nil, nil
		}, &HandlerOpts{
			Concurrency: 1,
			BatchSize:   10,
			MaxRetries:  3,
			Backoff:     NewFullJitterBackoff(time.Second, 500*time.Millisecond), // MaxDelay < MinDelay
		})

		worker := client.NewWorker(t.Context(), nil).AddTask(task, nil)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for invalid backoff")
		}
		if err.Error() != `task "invalid_task3" has invalid handler options: backoff maximum delay must be greater than minimum delay` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestWorkerValidatesFlowStepHandlerOpts(t *testing.T) {
	client := getTestClient(t)

	// Test invalid concurrency in flow step
	t.Run("negative_concurrency", func(t *testing.T) {
		flow := NewFlow("invalid_flow").
			AddStep(NewStep("step1").Handler(func(_ context.Context, _ any) (any, error) {
				return nil, nil
			}, &HandlerOpts{
				Concurrency: -1,
				BatchSize:   10,
			}))

		worker := client.NewWorker(t.Context(), nil).AddFlow(flow, nil)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for negative concurrency")
		}
		if err.Error() != `flow "invalid_flow" step "step1" has invalid handler options: concurrency must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Test invalid batch size in flow step
	t.Run("negative_batch_size", func(t *testing.T) {
		flow := NewFlow("invalid_flow2").
			AddStep(NewStep("step1").Handler(func(_ context.Context, _ any) (any, error) {
				return nil, nil
			}, &HandlerOpts{
				Concurrency: 1,
				BatchSize:   -5, // Negative value
			}))

		worker := client.NewWorker(t.Context(), nil).AddFlow(flow, nil)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for negative batch size")
		}
		if err.Error() != `flow "invalid_flow2" step "step1" has invalid handler options: batch size must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Test invalid circuit breaker config in flow step
	t.Run("invalid_circuit_breaker", func(t *testing.T) {
		flow := NewFlow("invalid_flow3").
			AddStep(NewStep("step1").Handler(func(_ context.Context, _ any) (any, error) {
				return nil, nil
			}, &HandlerOpts{
				Concurrency:    1,
				BatchSize:      10,
				CircuitBreaker: &CircuitBreaker{failureThreshold: 0}, // Invalid threshold
			}))

		worker := client.NewWorker(t.Context(), nil).AddFlow(flow, nil)
		err := worker.Start(t.Context())
		if err == nil {
			t.Fatal("expected error for invalid circuit breaker")
		}
		// Should contain validation error for circuit breaker
		if err.Error() != `flow "invalid_flow3" step "step1" has invalid handler options: circuit breaker failure threshold must be greater than zero` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
