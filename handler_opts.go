package catbird

import (
	"fmt"
	"time"
)

// BackoffStrategy defines how retry delays are calculated based on delivery count.
// Implementations must be safe for concurrent use.
type BackoffStrategy interface {
	// Validate returns an error if configuration is invalid.
	Validate() error
	// NextDelay returns a delay for a zero-based delivery count (first retry = 0).
	// Implementations should always return a positive duration.
	NextDelay(deliveryCount int) time.Duration
}

// CircuitBreakerStrategy defines the interface for circuit breaker behavior.
// Implementations must be safe for concurrent use.
type CircuitBreakerStrategy interface {
	// Validate returns an error if configuration is invalid.
	Validate() error
	// Allow returns whether a call is permitted and how long to wait if not.
	Allow(now time.Time) (bool, time.Duration)
	// RecordSuccess updates breaker state after a successful call.
	RecordSuccess()
	// RecordFailure updates breaker state after a failed call.
	RecordFailure(now time.Time)
}

type HandlerOpts struct {
	Concurrency    int
	BatchSize      int
	Timeout        time.Duration
	MaxRetries     int
	Backoff        BackoffStrategy
	CircuitBreaker CircuitBreakerStrategy
}

const (
	defaultHandlerConcurrency = 4
	defaultHandlerBatchSize   = 64
	defaultHandlerTimeout     = 30 * time.Second
	defaultHandlerMaxRetries  = 2
)

var defaultHandlerBackoff = NewFullJitterBackoff(100*time.Millisecond, 2*time.Second)

// applyDefaultHandlerOpts sets default values for handler options.
func applyDefaultHandlerOpts(opts ...HandlerOpts) *HandlerOpts {
	var resolved HandlerOpts
	hasOpts := len(opts) > 0
	if hasOpts {
		resolved = opts[0]
	}

	if !hasOpts {
		resolved.Timeout = defaultHandlerTimeout
		resolved.MaxRetries = defaultHandlerMaxRetries
		resolved.Backoff = defaultHandlerBackoff
	}

	if resolved.Concurrency == 0 {
		resolved.Concurrency = defaultHandlerConcurrency
	}
	if resolved.BatchSize == 0 {
		resolved.BatchSize = defaultHandlerBatchSize
	}
	return &resolved
}

// validate checks handler options for consistency.
func (h *HandlerOpts) validate() error {
	if h.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be greater than zero")
	}
	if h.BatchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero")
	}
	if h.Timeout < 0 {
		return fmt.Errorf("timeout cannot be negative")
	}
	if h.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	if h.MaxRetries == 0 && h.Backoff != nil {
		return fmt.Errorf("backoff configured but max retries is zero")
	}
	if h.Backoff != nil {
		if err := h.Backoff.Validate(); err != nil {
			return err
		}
	}
	if h.CircuitBreaker != nil {
		if err := h.CircuitBreaker.Validate(); err != nil {
			return err
		}
	}
	return nil
}
