package catbird

import (
	"fmt"
	"time"
)

type HandlerOpts struct {
	Concurrency    int
	BatchSize      int
	Timeout        time.Duration
	MaxRetries     int
	MinDelay       time.Duration
	MaxDelay       time.Duration
	CircuitBreaker *CircuitBreaker
}

// applyDefaultHandlerOpts sets default values for handler options.
func applyDefaultHandlerOpts(opts *HandlerOpts) *HandlerOpts {
	if opts == nil {
		opts = &HandlerOpts{}
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 10
	}
	return opts
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
	if h.MinDelay < 0 {
		return fmt.Errorf("backoff minimum delay cannot be negative")
	}
	if h.MaxDelay < 0 {
		return fmt.Errorf("backoff maximum delay cannot be negative")
	}
	if h.MaxDelay > 0 && h.MaxDelay <= h.MinDelay {
		return fmt.Errorf("backoff maximum delay must be greater than minimum delay")
	}
	if h.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	if h.MaxRetries == 0 && h.MaxDelay > 0 {
		return fmt.Errorf("backoff configured but max retries is zero")
	}
	if h.CircuitBreaker != nil {
		if err := h.CircuitBreaker.validate(); err != nil {
			return err
		}
	}
	return nil
}
