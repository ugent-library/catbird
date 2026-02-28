package catbird

import "context"

// Cancel requests cancellation for the current run from inside a task or flow handler.
func Cancel(ctx context.Context, opts ...CancelOpts) error {
	if taskScope, _ := ctx.Value(taskRunScopeContextKey{}).(*taskRunScope); taskScope != nil && taskScope.conn != nil {
		if err := CancelTaskRun(ctx, taskScope.conn, taskScope.name, taskScope.runID, opts...); err != nil {
			return err
		}

		if taskScope.cancel != nil {
			taskScope.cancel()
		}

		return nil
	}

	if flowScope, _ := ctx.Value(flowRunScopeContextKey{}).(*flowRunScope); flowScope != nil && flowScope.conn != nil {
		if err := CancelFlowRun(ctx, flowScope.conn, flowScope.name, flowScope.runID, opts...); err != nil {
			return err
		}

		if flowScope.cancel != nil {
			flowScope.cancel()
		}

		return nil
	}

	return ErrNoRunContext
}
