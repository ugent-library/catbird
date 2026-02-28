package catbird

import (
	"context"
	"errors"
)

type earlyCompletionError struct {
	flowName  string
	flowRunID int64
	output    any
	reason    string
}

func (e *earlyCompletionError) Error() string {
	if e.reason == "" {
		return "early completion requested"
	}
	return "early completion requested: " + e.reason
}

// CompleteEarly requests early completion for the current flow run.
//
// Return this from a flow step handler to signal that the flow should
// complete early with the provided output and optional reason.
func CompleteEarly(ctx context.Context, output any, reason string) error {
	flowScope, _ := ctx.Value(flowRunScopeContextKey{}).(*flowRunScope)
	if flowScope == nil {
		return ErrNoRunContext
	}

	return &earlyCompletionError{
		flowName:  flowScope.name,
		flowRunID: flowScope.runID,
		output:    output,
		reason:    reason,
	}
}

func asEarlyCompletion(err error) (*earlyCompletionError, bool) {
	var completion *earlyCompletionError
	if !errors.As(err, &completion) {
		return nil, false
	}
	return completion, true
}
