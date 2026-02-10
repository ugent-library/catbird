package catbird

import (
	"fmt"
	"time"
)

type handlerOpts struct {
	concurrency    int
	batchSize      int
	maxDuration    time.Duration
	maxRetries     int
	minDelay       time.Duration
	maxDelay       time.Duration
	circuitBreaker *circuitBreaker
}

// validate checks handler options for consistency.
func (h *handlerOpts) validate() error {
	if h.concurrency <= 0 {
		return fmt.Errorf("concurrency must be greater than zero")
	}
	if h.batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero")
	}
	if h.maxDuration < 0 {
		return fmt.Errorf("max duration cannot be negative")
	}
	if h.minDelay < 0 {
		return fmt.Errorf("backoff minimum delay cannot be negative")
	}
	if h.maxDelay < 0 {
		return fmt.Errorf("backoff maximum delay cannot be negative")
	}
	if h.maxDelay > 0 && h.maxDelay <= h.minDelay {
		return fmt.Errorf("backoff maximum delay must be greater than minimum delay")
	}
	if h.maxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	if h.maxRetries == 0 && h.maxDelay > 0 {
		return fmt.Errorf("backoff configured but max retries is zero")
	}
	if h.circuitBreaker != nil {
		if err := h.circuitBreaker.validate(); err != nil {
			return err
		}
	}
	return nil
}

// HandlerOpt is an option for configuring task and flow step handlers
type HandlerOpt func(*handlerOpts)

// WithConcurrency sets the number of concurrent handler executions
func WithConcurrency(n int) HandlerOpt {
	return func(h *handlerOpts) {
		h.concurrency = n
	}
}

// WithMaxDuration sets the maximum duration for handler execution
func WithMaxDuration(d time.Duration) HandlerOpt {
	return func(h *handlerOpts) {
		h.maxDuration = d
	}
}

// WithBatchSize sets the number of messages to read per batch
func WithBatchSize(n int) HandlerOpt {
	return func(h *handlerOpts) {
		h.batchSize = n
	}
}

// WithMaxRetries sets the number of retry attempts for failed handlers
func WithMaxRetries(n int) HandlerOpt {
	return func(h *handlerOpts) {
		h.maxRetries = n
	}
}

// WithBackoff sets the delay between retries, exponentially backing off from minDelay to maxDelay
func WithBackoff(minDelay, maxDelay time.Duration) HandlerOpt {
	return func(h *handlerOpts) {
		h.minDelay = minDelay
		h.maxDelay = maxDelay
	}
}

// WithCircuitBreaker configures a circuit breaker for handler execution.
// failureThreshold is the number of consecutive failures before opening.
// openTimeout controls how long the circuit stays open before trying again.
func WithCircuitBreaker(failureThreshold int, openTimeout time.Duration) HandlerOpt {
	return func(h *handlerOpts) {
		h.circuitBreaker = newCircuitBreaker(failureThreshold, openTimeout)
	}
}
