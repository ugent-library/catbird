package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// handlerFunc is a job handler with the types folded away: JSON in, JSON
// out. The step input goes in; what comes back is the step's output — the
// handler's return value, or NULL for the error-only shapes.
type handlerFunc func(ctx context.Context, p *Plan, input json.RawMessage) (json.RawMessage, error)

var (
	contextType = reflect.TypeFor[context.Context]()
	errorType   = reflect.TypeFor[error]()
	planType    = reflect.TypeFor[*Plan]()
)

// newHandler wraps fn into a handlerFunc. Four shapes are accepted:
//
//	func(ctx context.Context, in In) error
//	func(ctx context.Context, in In) (Out, error)
//	func(ctx context.Context, p *jobs.Plan, in In) error
//	func(ctx context.Context, p *jobs.Plan, in In) (Out, error)
//
// A handler that returns Out records it as its step's output; the
// error-only shapes record none. A handler that takes a Plan can also add
// steps and set the run's output through it. The signature is checked and
// the types extracted once, here; the returned function only unmarshals,
// calls and marshals.
func newHandler(fn any) (handlerFunc, error) {
	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

	if fnType == nil || fnType.Kind() != reflect.Func {
		return nil, fmt.Errorf("handler must be a function")
	}

	withPlan := fnType.NumIn() == 3 && fnType.In(1) == planType
	if !(withPlan || fnType.NumIn() == 2) || fnType.In(0) != contextType ||
		fnType.In(fnType.NumIn()-1) == planType {
		return nil, fmt.Errorf("handler must have signature func(context.Context, [*jobs.Plan,] In) ([Out,] error)")
	}
	withOutput := fnType.NumOut() == 2
	if !(withOutput || fnType.NumOut() == 1) ||
		!fnType.Out(fnType.NumOut()-1).Implements(errorType) {
		return nil, fmt.Errorf("handler must return error or (Out, error)")
	}
	inputType := fnType.In(fnType.NumIn() - 1)

	return func(ctx context.Context, p *Plan, input json.RawMessage) (json.RawMessage, error) {
		inputVal, err := unmarshalInput(input, inputType)
		if err != nil {
			return nil, err
		}
		args := []reflect.Value{reflect.ValueOf(ctx)}
		if withPlan {
			args = append(args, reflect.ValueOf(p))
		}
		args = append(args, inputVal)

		results := fnVal.Call(args)

		if errVal := results[len(results)-1]; !errVal.IsNil() {
			return nil, errVal.Interface().(error)
		}
		if !withOutput {
			return nil, nil
		}
		return json.Marshal(results[0].Interface())
	}, nil
}

func unmarshalInput(input json.RawMessage, inputType reflect.Type) (reflect.Value, error) {
	if input == nil {
		input = []byte("null") // a run's input may be SQL NULL
	}
	v := reflect.New(inputType)
	if err := json.Unmarshal(input, v.Interface()); err != nil {
		return reflect.Value{}, fmt.Errorf("unmarshal input: %w", err)
	}
	return v.Elem(), nil
}
