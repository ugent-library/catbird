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

type handlerOpts struct {
	concurrency    int
	batchSize      int
	timeout        time.Duration
	maxRetries     int
	backoff        BackoffStrategy
	circuitBreaker CircuitBreakerStrategy
}

type HandlerOpt func(*handlerOpts)

// WithConcurrency sets maximum concurrent handler executions.
func WithConcurrency(concurrency int) HandlerOpt {
	return func(opts *handlerOpts) {
		opts.concurrency = concurrency
	}
}

// WithBatchSize sets how many claims are fetched per poll.
func WithBatchSize(batchSize int) HandlerOpt {
	return func(opts *handlerOpts) {
		opts.batchSize = batchSize
	}
}

// WithTimeout sets per-handler execution timeout.
func WithTimeout(timeout time.Duration) HandlerOpt {
	return func(opts *handlerOpts) {
		opts.timeout = timeout
	}
}

// WithMaxRetries sets retry attempts for handler failures.
func WithMaxRetries(maxRetries int) HandlerOpt {
	return func(opts *handlerOpts) {
		opts.maxRetries = maxRetries
	}
}

// WithFullJitterBackoff sets full-jitter retry backoff strategy.
func WithFullJitterBackoff(minDelay, maxDelay time.Duration) HandlerOpt {
	return func(opts *handlerOpts) {
		opts.backoff = NewFullJitterBackoff(minDelay, maxDelay)
	}
}

// WithCircuitBreaker sets optional circuit breaker strategy.
func WithCircuitBreaker(failureThreshold int, openTimeout time.Duration) HandlerOpt {
	return func(opts *handlerOpts) {
		opts.circuitBreaker = NewCircuitBreaker(failureThreshold, openTimeout)
	}
}

const (
	defaultHandlerConcurrency = 4
	defaultHandlerBatchSize   = 64
	defaultHandlerTimeout     = 30 * time.Second
	defaultHandlerMaxRetries  = 2
)

var defaultHandlerBackoff = NewFullJitterBackoff(100*time.Millisecond, 2*time.Second)

// applyDefaultHandlerOpts sets default values for handler options.
func applyDefaultHandlerOpts(opts ...HandlerOpt) *handlerOpts {
	resolved := handlerOpts{
		concurrency: defaultHandlerConcurrency,
		batchSize:   defaultHandlerBatchSize,
		timeout:     defaultHandlerTimeout,
		maxRetries:  defaultHandlerMaxRetries,
		backoff:     defaultHandlerBackoff,
	}

	for _, opt := range opts {
		opt(&resolved)
	}

	if resolved.maxRetries == 0 {
		resolved.backoff = nil
	}

	return &resolved
}

// validate checks handler options for consistency.
func (h *handlerOpts) validate() error {
	if h.concurrency <= 0 {
		return fmt.Errorf("concurrency must be greater than zero")
	}
	if h.batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero")
	}
	if h.timeout < 0 {
		return fmt.Errorf("timeout cannot be negative")
	}
	if h.maxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	if h.maxRetries == 0 {
		h.backoff = nil
	}
	if h.backoff != nil {
		if err := h.backoff.Validate(); err != nil {
			return err
		}
	}
	if h.circuitBreaker != nil {
		if err := h.circuitBreaker.Validate(); err != nil {
			return err
		}
	}
	return nil
}
