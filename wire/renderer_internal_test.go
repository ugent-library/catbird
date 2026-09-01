package wire

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ugent-library/catbird"
)

// batch builds messages in the order given, positions counting up, so a test
// states its stream as a list of topics.
func batch(topics ...string) []catbird.Message {
	msgs := make([]catbird.Message, len(topics))
	for i, topic := range topics {
		msgs[i] = catbird.Message{ID: int64(i + 1), Position: int64(i + 1), Topic: topic}
	}
	return msgs
}

func testRequest() *http.Request {
	return httptest.NewRequest("GET", "/events", nil)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestExactTopicRuleMatchesOnlyThatTopic(t *testing.T) {
	rd := NewRenderer()
	rd.HandleFunc("order.paid", func(r *http.Request, m Match, f *Fragment) error {
		for _, msg := range m.Messages {
			fmt.Fprintf(f, "[%s]", msg.Topic)
		}
		return nil
	})
	out := rd.dispatch(testRequest(), batch("order", "order.paid", "order.paid.refund"), quietLogger())
	if got := string(out); got != "[order.paid]" {
		t.Fatalf("got %q, want only the exact topic", got)
	}
}

func TestSubtreeRuleCoversThePrefixItself(t *testing.T) {
	// "order.#" must cover "order" and everything under it, and nothing
	// beside it — the same topics the stream read returns for that pattern.
	rd := NewRenderer()
	rd.HandleFunc("order.#", func(r *http.Request, m Match, f *Fragment) error {
		for _, msg := range m.Messages {
			fmt.Fprintf(f, "[%s]", msg.Topic)
		}
		return nil
	})
	out := rd.dispatch(testRequest(), batch("order", "order.paid", "order.paid.refund", "orders", "invoice"), quietLogger())
	if got := string(out); got != "[order][order.paid][order.paid.refund]" {
		t.Fatalf("got %q, want the prefix and its subtree", got)
	}
}

func TestHashMatchesEverything(t *testing.T) {
	rd := NewRenderer()
	calls := 0
	rd.HandleFunc("#", func(r *http.Request, m Match, f *Fragment) error {
		calls++
		if len(m.Messages) != 3 {
			t.Fatalf("got %d messages, want the whole batch", len(m.Messages))
		}
		return nil
	})
	rd.dispatch(testRequest(), batch("a", "b.c", "d.e.f"), quietLogger())
	if calls != 1 {
		t.Fatalf("handler ran %d times, want once for the whole batch", calls)
	}
}

func TestVariableGroupsTheBatchPerBinding(t *testing.T) {
	rd := NewRenderer()
	var calls []string
	rd.HandleFunc("record.work.{id}.#", func(r *http.Request, m Match, f *Fragment) error {
		var topics []string
		for _, msg := range m.Messages {
			topics = append(topics, msg.Topic)
		}
		calls = append(calls, m.Var("id")+": "+strings.Join(topics, " "))
		return nil
	})
	rd.dispatch(testRequest(), batch(
		"record.work.7.updated",
		"record.work.9.updated",
		"record.work.7.deleted",
	), quietLogger())
	want := []string{
		"7: record.work.7.updated record.work.7.deleted",
		"9: record.work.9.updated",
	}
	if len(calls) != len(want) {
		t.Fatalf("got calls %q, want %q", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("call %d is %q, want %q", i, calls[i], want[i])
		}
	}
}

func TestVarOfAnUndeclaredNameIsEmpty(t *testing.T) {
	rd := NewRenderer()
	rd.HandleFunc("user.{id}.#", func(r *http.Request, m Match, f *Fragment) error {
		if got := m.Var("nope"); got != "" {
			t.Fatalf("Var of an undeclared name is %q, want empty", got)
		}
		return nil
	})
	rd.dispatch(testRequest(), batch("user.1.mention"), quietLogger())
}

func TestAllMatchingRulesRunInRegistrationOrder(t *testing.T) {
	rd := NewRenderer()
	rd.HandleFunc("user.{id}.#", func(r *http.Request, m Match, f *Fragment) error {
		fmt.Fprintf(f, "[tray %s]", m.Var("id"))
		return nil
	})
	rd.HandleFunc("user.{id}.#", func(r *http.Request, m Match, f *Fragment) error {
		fmt.Fprintf(f, "[badge %s]", m.Var("id"))
		return nil
	})
	out := rd.dispatch(testRequest(), batch("user.1.mention"), quietLogger())
	if got := string(out); got != "[tray 1][badge 1]" {
		t.Fatalf("got %q, want both rules in registration order", got)
	}
}

func TestRuleThatMatchedNothingDoesNotRun(t *testing.T) {
	rd := NewRenderer()
	rd.HandleFunc("invoice.#", func(r *http.Request, m Match, f *Fragment) error {
		t.Fatal("handler ran on a batch its pattern does not match")
		return nil
	})
	out := rd.dispatch(testRequest(), batch("order.paid"), quietLogger())
	if len(out) != 0 {
		t.Fatalf("got %q, want nothing", out)
	}
}

func TestHandlerErrorDropsOnlyItsFragments(t *testing.T) {
	rd := NewRenderer()
	rd.HandleFunc("order.#", func(r *http.Request, m Match, f *Fragment) error {
		fmt.Fprint(f, "[half written]")
		return errors.New("render failed")
	})
	rd.HandleFunc("order.#", func(r *http.Request, m Match, f *Fragment) error {
		fmt.Fprint(f, "[intact]")
		return nil
	})
	var log strings.Builder
	logger := slog.New(slog.NewTextHandler(&log, nil))
	out := rd.dispatch(testRequest(), batch("order.paid"), logger)
	if got := string(out); got != "[intact]" {
		t.Fatalf("got %q, want the failed call's fragments dropped and the other kept", got)
	}
	if !strings.Contains(log.String(), "order.#") || !strings.Contains(log.String(), "render failed") {
		t.Fatalf("log %q names neither the pattern nor the error", log.String())
	}
}

func TestInvalidPatternsPanic(t *testing.T) {
	for _, pattern := range []string{
		"",
		".",
		"a..b",
		"a.#.b",
		"#.a",
		"a.*",
		"a.{}.b",
		"a.x{id}.b",
		"a.{id.b",
		"a.{i{d}}.b",
		"{id}.x.{id}",
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Handle(%q) did not panic", pattern)
				}
			}()
			NewRenderer().HandleFunc(pattern, func(r *http.Request, m Match, f *Fragment) error { return nil })
		}()
	}
}

func TestNilHandlerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Handle with a nil handler did not panic")
		}
	}()
	NewRenderer().Handle("order.#", nil)
}
