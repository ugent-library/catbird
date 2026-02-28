package catbird

import (
	"context"
	"errors"
	"testing"
)

func TestCompleteEarlyCreatesTypedControlError(t *testing.T) {
	err := CompleteEarly(withFlowRunScope(context.Background(), nil, "my_flow", 42, nil), map[string]any{"ok": true}, "fast-path")
	if err == nil {
		t.Fatal("expected control error")
	}

	completion, ok := asEarlyCompletion(err)
	if !ok {
		t.Fatalf("expected early completion error type, got %T", err)
	}
	if completion.reason != "fast-path" {
		t.Fatalf("unexpected reason: %q", completion.reason)
	}
	if completion.flowName != "my_flow" || completion.flowRunID != 42 {
		t.Fatalf("unexpected run scope: %s/%d", completion.flowName, completion.flowRunID)
	}

	outputMap, ok := completion.output.(map[string]any)
	if !ok || outputMap["ok"] != true {
		t.Fatalf("unexpected output payload: %#v", completion.output)
	}
}

func TestCompleteEarlyRequiresFlowContext(t *testing.T) {
	err := CompleteEarly(context.Background(), "out", "reason")
	if !errors.Is(err, ErrNoRunContext) {
		t.Fatalf("expected ErrNoRunContext, got %v", err)
	}
}
