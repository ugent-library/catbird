package catbird

import (
	"fmt"
	"sync"
	"time"
)

type circuitState int

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

type circuitBreaker struct {
	mu               sync.Mutex
	state            circuitState
	failures         int
	failureThreshold int
	openTimeout      time.Duration
	openUntil        time.Time
	halfOpenInFlight bool
}

func newCircuitBreaker(failureThreshold int, openTimeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		state:            circuitClosed,
		failureThreshold: failureThreshold,
		openTimeout:      openTimeout,
	}
}

func (c *circuitBreaker) validate() error {
	if c.failureThreshold <= 0 {
		return fmt.Errorf("circuit breaker failure threshold must be greater than zero")
	}
	if c.openTimeout <= 0 {
		return fmt.Errorf("circuit breaker open timeout must be greater than zero")
	}
	return nil
}

func (c *circuitBreaker) allow(now time.Time) (bool, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case circuitOpen:
		if now.Before(c.openUntil) {
			return false, c.openUntil.Sub(now)
		}
		c.state = circuitHalfOpen
		c.halfOpenInFlight = false
	case circuitHalfOpen:
		if c.halfOpenInFlight {
			return false, c.openTimeout
		}
		c.halfOpenInFlight = true
		return true, 0
	case circuitClosed:
		return true, 0
	}

	if c.state == circuitHalfOpen {
		if c.halfOpenInFlight {
			return false, c.openTimeout
		}
		c.halfOpenInFlight = true
		return true, 0
	}

	return true, 0
}

func (c *circuitBreaker) recordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case circuitHalfOpen:
		c.state = circuitClosed
		c.failures = 0
		c.halfOpenInFlight = false
	case circuitClosed:
		c.failures = 0
	}
}

func (c *circuitBreaker) recordFailure(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case circuitHalfOpen:
		c.state = circuitOpen
		c.openUntil = now.Add(c.openTimeout)
		c.failures = 0
		c.halfOpenInFlight = false
	case circuitClosed:
		c.failures++
		if c.failures >= c.failureThreshold {
			c.state = circuitOpen
			c.openUntil = now.Add(c.openTimeout)
			c.failures = 0
		}
	}
}
