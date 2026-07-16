package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// handlerFunc is a job handler with the types folded away: JSON in, JSON
// out. The step input goes in, the return value comes back as the step's
// output.
type handlerFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

var (
	contextType = reflect.TypeFor[context.Context]()
	errorType   = reflect.TypeFor[error]()
)

// newHandler wraps fn — func(ctx context.Context, in In) (Out, error) —
// into a handlerFunc. The signature is checked and the types extracted
// once, here; the returned function only unmarshals, calls and marshals.
func newHandler(fn any) (handlerFunc, error) {
	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

	if fnType == nil || fnType.Kind() != reflect.Func {
		return nil, fmt.Errorf("handler must be a function")
	}
	if fnType.NumIn() != 2 || fnType.In(0) != contextType {
		return nil, fmt.Errorf("handler must have signature func(context.Context, In) (Out, error)")
	}
	if fnType.NumOut() != 2 || !fnType.Out(1).Implements(errorType) {
		return nil, fmt.Errorf("handler must return (Out, error)")
	}

	inputType := fnType.In(1)

	return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		if input == nil {
			input = []byte("null") // a run's input may be SQL NULL
		}
		inputVal := reflect.New(inputType)
		if err := json.Unmarshal(input, inputVal.Interface()); err != nil {
			return nil, fmt.Errorf("unmarshal input: %w", err)
		}

		results := fnVal.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			inputVal.Elem(),
		})

		if !results[1].IsNil() {
			return nil, results[1].Interface().(error)
		}
		return json.Marshal(results[0].Interface())
	}, nil
}
