package catbird

import (
	"fmt"
	"time"
)

// FullJitterBackoff implements exponential backoff with full jitter.
type FullJitterBackoff struct {
	MinDelay time.Duration
	MaxDelay time.Duration
}

// NewFullJitterBackoff creates a FullJitterBackoff with the provided bounds.
func NewFullJitterBackoff(minDelay, maxDelay time.Duration) *FullJitterBackoff {
	return &FullJitterBackoff{
		MinDelay: minDelay,
		MaxDelay: maxDelay,
	}
}

// Validate checks the backoff configuration for consistency.
func (b *FullJitterBackoff) Validate() error {
	if b.MinDelay < 0 {
		return fmt.Errorf("backoff minimum delay cannot be negative")
	}
	if b.MaxDelay < 0 {
		return fmt.Errorf("backoff maximum delay cannot be negative")
	}
	if b.MaxDelay > 0 && b.MaxDelay <= b.MinDelay {
		return fmt.Errorf("backoff maximum delay must be greater than minimum delay")
	}
	return nil
}

// NextDelay returns the jittered delay for the given delivery count.
// deliveryCount is expected to be zero-based for the first retry.
func (b *FullJitterBackoff) NextDelay(deliveryCount int) time.Duration {
	attempt := deliveryCount
	if attempt < 0 {
		attempt = 0
	}
	return backoffWithFullJitter(attempt, b.MinDelay, b.MaxDelay)
}
