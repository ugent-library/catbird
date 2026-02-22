package catbird

import (
	"fmt"
	"time"
)

type handlerOpts struct {
	concurrency    int
	batchSize      int
	timeout        time.Duration
	maxRetries     int
	minDelay       time.Duration
	maxDelay       time.Duration
	circuitBreaker *CircuitBreaker
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
