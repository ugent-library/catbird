package notify

import (
	"sync"
	"time"
)

// Waker turns notifications into wake signals for a loop that waits on a
// channel. A signal already pending is enough — a waiting loop looks at
// everything due, so signals need no count.
type Waker struct {
	C chan struct{} // the loop's wait case; capacity one

	mu    sync.Mutex
	timer *time.Timer
	next  time.Time
}

func NewWaker() *Waker {
	return &Waker{C: make(chan struct{}, 1)}
}

// Wake signals now, without blocking.
func (w *Waker) Wake() {
	select {
	case w.C <- struct{}{}:
	default:
	}
}

// WakeAt signals when at arrives: at once when it is zero or has passed,
// otherwise through one timer kept at the earliest pending time. That is
// how a backoff-paced retry or a delayed message is picked up on time
// without polling for it.
func (w *Waker) WakeAt(at time.Time) {
	if at.IsZero() || !at.After(time.Now()) {
		w.Wake()
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.timer != nil && !at.Before(w.next) {
		return // an earlier wake is already set
	}
	if w.timer != nil {
		w.timer.Stop()
	}
	w.next = at
	w.timer = time.AfterFunc(time.Until(at), func() {
		w.Wake()
		w.mu.Lock()
		w.timer = nil
		w.next = time.Time{}
		w.mu.Unlock()
	})
}

// Stop drops a pending timed wake. Call it when the waiting loop ends.
func (w *Waker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
		w.next = time.Time{}
	}
}

// ParseTime reads a notify payload that carries an RFC 3339 timestamp —
// the format the engine's SQL uses when a notification means "due at
// this time". An empty or unparseable payload is a zero time, waking at
// once: a spurious look is cheap, a missed one is not.
func ParseTime(payload string) time.Time {
	if payload == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, payload)
	if err != nil {
		return time.Time{}
	}
	return t
}
