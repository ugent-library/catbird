package catbird

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func startTestListener(t *testing.T, listener *Listener) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)

	go func() {
		errCh <- listener.Start(ctx)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("listener.Start() failed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("listener shutdown error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Log("listener did not shutdown within timeout")
		}
	})
}

func TestListenerExactTopic(t *testing.T) {
	cb := getTestClient(t)

	topic := "test.listener." + uuid.NewString()[:8]

	var mu sync.Mutex
	var received []string

	listener := NewListener(testPool)
	listener.Handle(topic, func(topic, message string) {
		mu.Lock()
		received = append(received, topic)
		mu.Unlock()
	})
	startTestListener(t, listener)

	if err := cb.Notify(t.Context(), topic, ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0] != topic {
		t.Errorf("received = %v, want [%s]", received, topic)
	}
}

func TestListenerWildcardStar(t *testing.T) {
	cb := getTestClient(t)

	prefix := "test.wc." + uuid.NewString()[:8]

	var mu sync.Mutex
	var received []string

	listener := NewListener(testPool)
	listener.Handle(prefix+".*", func(topic, message string) {
		mu.Lock()
		received = append(received, topic)
		mu.Unlock()
	})
	startTestListener(t, listener)

	// Should match
	if err := cb.Notify(t.Context(), prefix+".created", ""); err != nil {
		t.Fatal(err)
	}
	if err := cb.Notify(t.Context(), prefix+".updated", ""); err != nil {
		t.Fatal(err)
	}
	// Should not match (two tokens after prefix)
	if err := cb.Notify(t.Context(), prefix+".sub.created", ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 {
		t.Errorf("received = %v, want 2 events", received)
	}
}

func TestListenerWildcardHash(t *testing.T) {
	cb := getTestClient(t)

	prefix := "test.hash." + uuid.NewString()[:8]

	var mu sync.Mutex
	var received []string

	listener := NewListener(testPool)
	listener.Handle(prefix+".#", func(topic, message string) {
		mu.Lock()
		received = append(received, topic)
		mu.Unlock()
	})
	startTestListener(t, listener)

	// All should match
	if err := cb.Notify(t.Context(), prefix, ""); err != nil {
		t.Fatal(err)
	}
	if err := cb.Notify(t.Context(), prefix+".one", ""); err != nil {
		t.Fatal(err)
	}
	if err := cb.Notify(t.Context(), prefix+".one.two", ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Errorf("received %d events, want 3: %v", len(received), received)
	}
}

func TestListenerWithData(t *testing.T) {
	cb := getTestClient(t)

	topic := "test.data." + uuid.NewString()[:8]

	var mu sync.Mutex
	var gotData string

	listener := NewListener(testPool)
	listener.Handle(topic, func(topic, message string) {
		mu.Lock()
		gotData = message
		mu.Unlock()
	})
	startTestListener(t, listener)

	if err := cb.Notify(t.Context(), topic, `<div>hello</div>`); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if gotData != "<div>hello</div>" {
		t.Errorf("data = %q, want %q", gotData, "<div>hello</div>")
	}
}

func TestListenerMultipleHandlers(t *testing.T) {
	cb := getTestClient(t)

	topic := "test.multi." + uuid.NewString()[:8]

	var mu sync.Mutex
	var count int

	listener := NewListener(testPool)
	listener.Handle(topic, func(topic, message string) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	listener.Handle(topic, func(topic, message string) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	startTestListener(t, listener)

	if err := cb.Notify(t.Context(), topic, ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestListenerNoMatch(t *testing.T) {
	cb := getTestClient(t)

	prefix := "test.nomatch." + uuid.NewString()[:8]

	var mu sync.Mutex
	var received bool

	listener := NewListener(testPool)
	listener.Handle(prefix+".specific", func(topic, message string) {
		mu.Lock()
		received = true
		mu.Unlock()
	})
	startTestListener(t, listener)

	if err := cb.Notify(t.Context(), prefix+".other", ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if received {
		t.Error("handler was called for non-matching topic")
	}
}

func TestListenerSkipSelf(t *testing.T) {
	cb := getTestClient(t)

	topic := "test.skipself." + uuid.NewString()[:8]

	var mu sync.Mutex
	var count int

	listener := NewListener(testPool)
	listener.Handle(topic, func(topic, message string) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	startTestListener(t, listener)

	// Notify with From: listener.ID() — should be skipped
	if err := cb.Notify(t.Context(), topic, "", NotifyOpts{SentBy: listener.ID()}); err != nil {
		t.Fatal(err)
	}

	// Notify without From — should be delivered
	if err := cb.Notify(t.Context(), topic, ""); err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Errorf("count = %d, want 1 (skip-self should have prevented one delivery)", count)
	}
}
