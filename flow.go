package catbird

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Optional wraps a dependency output that may be absent.
type Optional[T any] struct {
	IsSet bool
	Value T
}

type StepType string

const (
	StepTypeNormal    StepType = "normal"
	StepTypeMapper    StepType = "mapper"
	StepTypeGenerator StepType = "generator"
	StepTypeReducer   StepType = "reducer"
)

type Flow struct {
	name               string
	description        string
	steps              []*Step
	outputPriority     []string
	priorityConfigured bool
	retentionPeriod    time.Duration
	onFail             func(context.Context, json.RawMessage, FlowFailure) error
	onFailOpts         *handlerOpts
}

// flowOutputCache holds lazy-loaded step outputs. Stored behind a pointer so
// FlowFailure can be safely passed by value without copying a sync.Mutex.
type flowOutputCache struct {
	mu    sync.Mutex
	cache map[string]json.RawMessage
}

type FlowFailure struct {
	FlowName              string          `json:"flow_name"`
	FlowRunID             int64           `json:"flow_run_id"`
	FailedStepName        string          `json:"failed_step_name,omitempty"`
	ErrorMessage          string          `json:"error_message"`
	Attempts              int             `json:"attempts"`
	OnFailAttempts        int             `json:"on_fail_attempts"`
	StartedAt             time.Time       `json:"started_at,omitzero"`
	FailedAt              time.Time       `json:"failed_at,omitzero"`
	ConcurrencyKey        string          `json:"concurrency_key,omitempty"`
	IdempotencyKey        string          `json:"idempotency_key,omitempty"`
	FailedStepInput       json.RawMessage `json:"failed_step_input,omitempty"`
	FailedStepSignalInput json.RawMessage `json:"failed_step_signal_input,omitempty"`

	// conn and outputCache provide lazy on-demand loading of completed step outputs.
	// outputCache is a pointer so copies of FlowFailure share the same cache.
	conn        Conn
	outputCache *flowOutputCache
}

func NewFlow(name string) *Flow {
	return &Flow{name: name}
}

func newFlowStep(flow *Flow, name string, stepType StepType) *Step {
	return &Step{
		flow:                 flow,
		name:                 name,
		stepType:             stepType,
		optionalDependencies: make(map[string]bool),
	}
}

func (f *Flow) AddStep(name string) *StepBuilder {
	step := newFlowStep(f, name, StepTypeNormal)
	f.steps = append(f.steps, step)
	return &StepBuilder{Step: step}
}

func (f *Flow) AddGeneratorStep(name string) *GeneratorStepBuilder {
	step := newFlowStep(f, name, StepTypeGenerator)
	f.steps = append(f.steps, step)
	return &GeneratorStepBuilder{Step: step}
}

func (f *Flow) AddMapStep(name string) *MapStepBuilder {
	step := newFlowStep(f, name, StepTypeMapper)
	f.steps = append(f.steps, step)
	return &MapStepBuilder{Step: step}
}

func (f *Flow) AddReducerStep(name string) *ReducerStepBuilder {
	step := newFlowStep(f, name, StepTypeReducer)
	f.steps = append(f.steps, step)
	return &ReducerStepBuilder{Step: step}
}

func (f *Flow) WithDescription(description string) *Flow {
	f.description = description
	return f
}

func (f *Flow) Output(stepName string) *Flow {
	f.priorityConfigured = true
	f.outputPriority = []string{stepName}
	return f
}

func (f *Flow) OutputPriority(stepNames ...string) *Flow {
	f.priorityConfigured = true
	f.outputPriority = append([]string(nil), stepNames...)
	return f
}

// RetentionPeriod sets how long terminal runs (completed, failed, canceled)
// are retained before being deleted by the garbage collector. A zero duration disables cleanup.
func (f *Flow) RetentionPeriod(d time.Duration) *Flow {
	f.retentionPeriod = d
	return f
}

// OnFail sets a flow failure handler and execution options.
// fn must have signature (context.Context, In, FlowFailure) error.
// If opts is omitted, defaults are used (concurrency: 4, batchSize: 64, timeout: 30s, maxRetries: 2 with full-jitter backoff 100ms-2s).
func (f *Flow) OnFail(fn any, opts ...HandlerOpt) *Flow {
	handler, err := makeFlowOnFailHandler(fn)
	if err != nil {
		panic(err)
	}
	f.onFail = handler
	f.onFailOpts = applyDefaultHandlerOpts(opts...)
	return f
}

func (f FlowFailure) Output(ctx context.Context, step string) (json.RawMessage, error) {
	if f.conn == nil || f.outputCache == nil {
		return nil, ErrUnknownStepOutput
	}

	f.outputCache.mu.Lock()
	defer f.outputCache.mu.Unlock()

	if out, ok := f.outputCache.cache[step]; ok {
		return out, nil
	}

	var out json.RawMessage
	err := f.conn.QueryRow(
		ctx,
		`SELECT cb_get_flow_step_output($1, $2, $3)`,
		f.FlowName, f.FlowRunID, step,
	).Scan(&out)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownStepOutput, step)
	}
	if out == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownStepOutput, step)
	}

	if f.outputCache.cache == nil {
		f.outputCache.cache = make(map[string]json.RawMessage)
	}
	f.outputCache.cache[step] = out
	return out, nil
}

func (f FlowFailure) OutputAs(ctx context.Context, step string, out any) error {
	b, err := f.Output(ctx, step)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

func (f FlowFailure) FailedStepInputAs(out any) error {
	if len(f.FailedStepInput) == 0 {
		return ErrNoFailedStepInput
	}
	return json.Unmarshal(f.FailedStepInput, out)
}

func (f FlowFailure) FailedStepSignalAs(out any) error {
	if len(f.FailedStepSignalInput) == 0 {
		return ErrNoFailedStepSignal
	}
	return json.Unmarshal(f.FailedStepSignalInput, out)
}

type Step struct {
	flow                 *Flow
	name                 string
	stepType             StepType
	description          string
	dependencies         []string
	optionalDependencies map[string]bool // tracks which dependencies are Optional[T]
	generatorFn          any
	generatorHandler     any
	generatorDepType     reflect.Type
	generatorItemType    reflect.Type
	generatorOutputType  reflect.Type
	outputType           reflect.Type
	reducerFn            any
	reducerInit          []byte
	reducerAcc           reflect.Type
	reducerItem          reflect.Type
	mapSource            string
	reduceSourceStep     string
	condition            string
	hasSignal            bool
	handler              func(context.Context, []byte, map[string][]byte, []byte) ([]byte, error)
	handlerOpts          *handlerOpts
}

func (s *Step) withDescription(description string) {
	s.description = description
}

func (s *Step) dependsOn(deps ...string) {
	s.dependencies = append(s.dependencies, deps...)
}

func (s *Step) withCondition(condition string) {
	s.condition = condition
}

func (s *Step) withSignal() {
	s.hasSignal = true
}

func (s *Step) mapInput() {
	if s.stepType != StepTypeMapper {
		panic(fmt.Sprintf("step %s: only mapper steps support MapFlowInput()", s.name))
	}
	if s.mapSource != "" {
		panic(fmt.Sprintf("step %s: map source already set to %q", s.name, s.mapSource))
	}
	s.mapSource = ""
}

func (s *Step) mapFrom(stepName string) {
	if s.stepType != StepTypeMapper {
		panic(fmt.Sprintf("step %s: only mapper steps support MapStepOutput()", s.name))
	}
	if strings.TrimSpace(stepName) == "" {
		panic(fmt.Sprintf("step %s: map source step name must not be empty", s.name))
	}
	if s.mapSource != "" && s.mapSource != stepName {
		panic(fmt.Sprintf("step %s: map source already set to %q", s.name, s.mapSource))
	}
	s.mapSource = stepName
	for _, dep := range s.dependencies {
		if dep == stepName {
			return
		}
	}
	s.dependencies = append(s.dependencies, stepName)
}

func (s *Step) generator(fn any) {
	if s.stepType != StepTypeGenerator {
		panic(fmt.Sprintf("step %s: Generate() is only valid for AddGeneratorStep", s.name))
	}

	depType, itemType, err := parseGeneratorFn(fn, s.name)
	if err != nil {
		panic(err)
	}

	s.generatorFn = fn
	s.generatorDepType = depType
	s.generatorItemType = itemType

	if s.generatorHandler != nil {
		handlerItemType, handlerOutputType, handlerErr := parseGeneratorHandlerFn(s.generatorHandler, s.name)
		if handlerErr != nil {
			panic(handlerErr)
		}
		if handlerItemType != itemType {
			panic(fmt.Sprintf("step %s: generator item type %v does not match handler item type %v", s.name, itemType, handlerItemType))
		}
		s.generatorOutputType = handlerOutputType
		s.outputType = handlerOutputType
		if s.reducerFn != nil && s.reducerItem != handlerOutputType {
			panic(fmt.Sprintf("step %s: reducer item type %v does not match handler output type %v", s.name, s.reducerItem, handlerOutputType))
		}
	}
}

func (s *Step) reduceStep(stepName string) {
	if s.stepType != StepTypeReducer {
		panic(fmt.Sprintf("step %s: ReduceStep() is only valid for AddReducerStep", s.name))
	}
	if strings.TrimSpace(stepName) == "" {
		panic(fmt.Sprintf("step %s: reduce source step name must not be empty", s.name))
	}
	s.reduceSourceStep = stepName
	for _, dep := range s.dependencies {
		if dep == stepName {
			return
		}
	}
	s.dependencies = append(s.dependencies, stepName)
}

func (s *Step) applyHandler(fn any, opts ...HandlerOpt) {
	if s.stepType == StepTypeReducer {
		panic(fmt.Sprintf("step %s: reducer steps use Reduce(...) instead of Map/Handler", s.name))
	}

	if s.stepType == StepTypeGenerator {
		itemType, outputType, err := parseGeneratorHandlerFn(fn, s.name)
		if err != nil {
			panic(err)
		}
		s.generatorHandler = fn
		s.generatorOutputType = outputType
		s.outputType = outputType
		if s.generatorFn != nil && s.generatorItemType != nil && s.generatorItemType != itemType {
			panic(fmt.Sprintf("step %s: generator item type %v does not match handler item type %v", s.name, s.generatorItemType, itemType))
		}
		if s.reducerFn != nil && s.reducerItem != outputType {
			panic(fmt.Sprintf("step %s: reducer item type %v does not match handler output type %v", s.name, s.reducerItem, outputType))
		}
		s.handlerOpts = applyDefaultHandlerOpts(opts...)
		return
	}

	fnType := reflect.TypeOf(fn)
	if fnType == nil || fnType.Kind() != reflect.Func {
		panic(fmt.Sprintf("step %s: handler must be a function", s.name))
	}
	if fnType.NumOut() != 2 || !fnType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic(fmt.Sprintf("step %s: handler must return (Out, error)", s.name))
	}
	s.outputType = fnType.Out(0)
	if s.reducerFn != nil && s.reducerItem != s.outputType {
		panic(fmt.Sprintf("step %s: reducer item type %v does not match handler output type %v", s.name, s.reducerItem, s.outputType))
	}

	handler, optionalDeps, err := makeStepHandler(fn, s.name, s.dependencies, s.hasSignal, s.stepType == StepTypeMapper, s.mapSource)
	if err != nil {
		panic(err)
	}
	s.handler = handler
	s.optionalDependencies = optionalDeps
	s.handlerOpts = applyDefaultHandlerOpts(opts...)
}

func (s *Step) reduce(initial any, fn any) {
	if s.stepType != StepTypeReducer {
		panic(fmt.Sprintf("step %s: Reduce() is only valid for reducer steps", s.name))
	}

	accType, itemType, err := parseGeneratorReducerFn(fn, s.name)
	if err != nil {
		panic(err)
	}

	if initial == nil {
		panic(fmt.Sprintf("step %s: reducer initial value must not be nil", s.name))
	}

	initType := reflect.TypeOf(initial)
	if !initType.AssignableTo(accType) {
		panic(fmt.Sprintf("step %s: reducer initial type %v is not assignable to accumulator type %v", s.name, initType, accType))
	}

	initJSON, err := json.Marshal(initial)
	if err != nil {
		panic(fmt.Sprintf("step %s: reducer initial value is not JSON serializable: %v", s.name, err))
	}

	s.reducerFn = fn
	s.reducerInit = initJSON
	s.reducerAcc = accType
	s.reducerItem = itemType
	s.outputType = accType

	if sourceStep := s.sourceStepForReducer(); sourceStep != nil {
		sourceOutputType := sourceStep.outputType
		if sourceStep.stepType == StepTypeGenerator {
			sourceOutputType = sourceStep.generatorOutputType
		}
		if sourceOutputType != nil && sourceOutputType != itemType {
			panic(fmt.Sprintf("step %s: reducer item type %v does not match source step %q output type %v", s.name, itemType, sourceStep.name, sourceOutputType))
		}
	}

	if s.handlerOpts == nil {
		s.handlerOpts = applyDefaultHandlerOpts()
	}
}

func (s *Step) sourceStepForReducer() *Step {
	if s.flow == nil || s.reduceSourceStep == "" {
		return nil
	}
	for _, step := range s.flow.steps {
		if step.name == s.reduceSourceStep {
			return step
		}
	}
	return nil
}

type StepBuilder struct {
	*Step
}

func (b *StepBuilder) AddStep(name string) *StepBuilder {
	return b.flow.AddStep(name)
}

func (b *StepBuilder) AddGeneratorStep(name string) *GeneratorStepBuilder {
	return b.flow.AddGeneratorStep(name)
}

func (b *StepBuilder) AddMapStep(name string) *MapStepBuilder {
	return b.flow.AddMapStep(name)
}

func (b *StepBuilder) AddReducerStep(name string) *ReducerStepBuilder {
	return b.flow.AddReducerStep(name)
}

func (b *StepBuilder) WithDescription(description string) *StepBuilder {
	b.withDescription(description)
	return b
}

func (b *StepBuilder) DependsOn(deps ...string) *StepBuilder {
	b.dependsOn(deps...)
	return b
}

func (b *StepBuilder) WithCondition(condition string) *StepBuilder {
	b.withCondition(condition)
	return b
}

func (b *StepBuilder) WithSignal() *StepBuilder {
	b.withSignal()
	return b
}

func (b *StepBuilder) Do(fn any, opts ...HandlerOpt) *StepBuilder {
	b.applyHandler(fn, opts...)
	return b
}

func (b *StepBuilder) Flow() *Flow {
	return b.flow
}

type MapStepBuilder struct {
	*Step
}

func (b *MapStepBuilder) AddStep(name string) *StepBuilder {
	return b.flow.AddStep(name)
}

func (b *MapStepBuilder) AddGeneratorStep(name string) *GeneratorStepBuilder {
	return b.flow.AddGeneratorStep(name)
}

func (b *MapStepBuilder) AddMapStep(name string) *MapStepBuilder {
	return b.flow.AddMapStep(name)
}

func (b *MapStepBuilder) AddReducerStep(name string) *ReducerStepBuilder {
	return b.flow.AddReducerStep(name)
}

func (b *MapStepBuilder) WithDescription(description string) *MapStepBuilder {
	b.withDescription(description)
	return b
}

func (b *MapStepBuilder) DependsOn(deps ...string) *MapStepBuilder {
	b.dependsOn(deps...)
	return b
}

func (b *MapStepBuilder) WithCondition(condition string) *MapStepBuilder {
	b.withCondition(condition)
	return b
}

func (b *MapStepBuilder) WithSignal() *MapStepBuilder {
	b.withSignal()
	return b
}

func (b *MapStepBuilder) MapFlowInput() *MapStepBuilder {
	b.mapInput()
	return b
}

func (b *MapStepBuilder) MapStepOutput(stepName string) *MapStepBuilder {
	b.mapFrom(stepName)
	return b
}

func (b *MapStepBuilder) Map(fn any, opts ...HandlerOpt) *MapStepBuilder {
	b.applyHandler(fn, opts...)
	return b
}

func (b *MapStepBuilder) Flow() *Flow {
	return b.flow
}

type GeneratorStepBuilder struct {
	*Step
}

func (b *GeneratorStepBuilder) AddStep(name string) *StepBuilder {
	return b.flow.AddStep(name)
}

func (b *GeneratorStepBuilder) AddGeneratorStep(name string) *GeneratorStepBuilder {
	return b.flow.AddGeneratorStep(name)
}

func (b *GeneratorStepBuilder) AddMapStep(name string) *MapStepBuilder {
	return b.flow.AddMapStep(name)
}

func (b *GeneratorStepBuilder) AddReducerStep(name string) *ReducerStepBuilder {
	return b.flow.AddReducerStep(name)
}

func (b *GeneratorStepBuilder) WithDescription(description string) *GeneratorStepBuilder {
	b.withDescription(description)
	return b
}

func (b *GeneratorStepBuilder) DependsOn(deps ...string) *GeneratorStepBuilder {
	b.dependsOn(deps...)
	return b
}

func (b *GeneratorStepBuilder) WithCondition(condition string) *GeneratorStepBuilder {
	b.withCondition(condition)
	return b
}

func (b *GeneratorStepBuilder) WithSignal() *GeneratorStepBuilder {
	b.withSignal()
	return b
}

func (b *GeneratorStepBuilder) Generate(fn any) *GeneratorStepBuilder {
	b.generator(fn)
	return b
}

func (b *GeneratorStepBuilder) Map(fn any, opts ...HandlerOpt) *GeneratorStepBuilder {
	b.applyHandler(fn, opts...)
	return b
}

func (b *GeneratorStepBuilder) Flow() *Flow {
	return b.flow
}

type ReducerStepBuilder struct {
	*Step
}

func (b *ReducerStepBuilder) AddStep(name string) *StepBuilder {
	return b.flow.AddStep(name)
}

func (b *ReducerStepBuilder) AddGeneratorStep(name string) *GeneratorStepBuilder {
	return b.flow.AddGeneratorStep(name)
}

func (b *ReducerStepBuilder) AddMapStep(name string) *MapStepBuilder {
	return b.flow.AddMapStep(name)
}

func (b *ReducerStepBuilder) AddReducerStep(name string) *ReducerStepBuilder {
	return b.flow.AddReducerStep(name)
}

func (b *ReducerStepBuilder) WithDescription(description string) *ReducerStepBuilder {
	b.withDescription(description)
	return b
}

func (b *ReducerStepBuilder) DependsOn(deps ...string) *ReducerStepBuilder {
	b.dependsOn(deps...)
	return b
}

func (b *ReducerStepBuilder) WithCondition(condition string) *ReducerStepBuilder {
	b.withCondition(condition)
	return b
}

func (b *ReducerStepBuilder) WithSignal() *ReducerStepBuilder {
	b.withSignal()
	return b
}

func (b *ReducerStepBuilder) ReduceStep(stepName string) *ReducerStepBuilder {
	b.reduceStep(stepName)
	return b
}

func (b *ReducerStepBuilder) Reduce(initial any, fn any) *ReducerStepBuilder {
	b.reduce(initial, fn)
	return b
}

func (b *ReducerStepBuilder) Flow() *Flow {
	return b.flow
}

func parseGeneratorFn(fn any, stepName string) (reflect.Type, reflect.Type, error) {
	fnType := reflect.TypeOf(fn)
	if fnType == nil || fnType.Kind() != reflect.Func {
		return nil, nil, fmt.Errorf("step %s: generator must be a function", stepName)
	}

	if fnType.NumIn() < 3 {
		return nil, nil, fmt.Errorf("step %s: generator must have signature func(context.Context, In, ..., func(ItemType) error) error", stepName)
	}

	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, nil, fmt.Errorf("step %s: generator first parameter must be context.Context", stepName)
	}

	yieldType := fnType.In(fnType.NumIn() - 1)
	if yieldType.Kind() != reflect.Func || yieldType.NumIn() != 1 || yieldType.IsVariadic() {
		return nil, nil, fmt.Errorf("step %s: generator last parameter must be func(ItemType) error", stepName)
	}

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if yieldType.NumOut() != 1 || !yieldType.Out(0).Implements(errorType) {
		return nil, nil, fmt.Errorf("step %s: generator yield function must return error", stepName)
	}

	if fnType.NumOut() != 1 || !fnType.Out(0).Implements(errorType) {
		return nil, nil, fmt.Errorf("step %s: generator must return error", stepName)
	}

	return fnType.In(1), yieldType.In(0), nil
}

func validateGeneratorFnForStep(step *Step, flowName string) error {
	fnType := reflect.TypeOf(step.generatorFn)
	if fnType == nil || fnType.Kind() != reflect.Func {
		return fmt.Errorf("flow %q: step %q generator must be a function", flowName, step.name)
	}

	expectedInputs := 2 + len(step.dependencies) + 1 // context + flow input + dependencies + yield
	if step.hasSignal {
		expectedInputs++ // signal between flow input and dependencies
	}

	if fnType.NumIn() != expectedInputs {
		return fmt.Errorf("flow %q: step %q generator must have signature func(context.Context, In%s%s, func(Item) error) error, got %d inputs",
			flowName,
			step.name,
			func() string {
				if step.hasSignal {
					return ", Signal"
				}
				return ""
			}(),
			func() string {
				if len(step.dependencies) > 0 {
					return fmt.Sprintf(", %d dependencies", len(step.dependencies))
				}
				return ""
			}(),
			fnType.NumIn(),
		)
	}

	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return fmt.Errorf("flow %q: step %q generator first parameter must be context.Context", flowName, step.name)
	}

	yieldType := fnType.In(fnType.NumIn() - 1)
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if yieldType.Kind() != reflect.Func || yieldType.NumIn() != 1 || yieldType.IsVariadic() || yieldType.NumOut() != 1 || !yieldType.Out(0).Implements(errorType) {
		return fmt.Errorf("flow %q: step %q generator last parameter must be func(ItemType) error", flowName, step.name)
	}

	if step.generatorItemType != nil && yieldType.In(0) != step.generatorItemType {
		return fmt.Errorf("flow %q: step %q generator yield item type %v does not match handler item type %v", flowName, step.name, yieldType.In(0), step.generatorItemType)
	}

	if fnType.NumOut() != 1 || !fnType.Out(0).Implements(errorType) {
		return fmt.Errorf("flow %q: step %q generator must return error", flowName, step.name)
	}

	return nil
}

func parseGeneratorHandlerFn(fn any, stepName string) (reflect.Type, reflect.Type, error) {
	fnType := reflect.TypeOf(fn)
	if fnType == nil || fnType.Kind() != reflect.Func {
		return nil, nil, fmt.Errorf("step %s: generator handler must be a function", stepName)
	}

	if fnType.NumIn() != 2 {
		return nil, nil, fmt.Errorf("step %s: generator handler must have signature func(context.Context, ItemType) (Out, error)", stepName)
	}

	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, nil, fmt.Errorf("step %s: generator handler first parameter must be context.Context", stepName)
	}

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if fnType.NumOut() != 2 || !fnType.Out(1).Implements(errorType) {
		return nil, nil, fmt.Errorf("step %s: generator handler must return (Out, error)", stepName)
	}

	return fnType.In(1), fnType.Out(0), nil
}

func parseGeneratorReducerFn(fn any, stepName string) (reflect.Type, reflect.Type, error) {
	fnType := reflect.TypeOf(fn)
	if fnType == nil || fnType.Kind() != reflect.Func {
		return nil, nil, fmt.Errorf("step %s: reducer must be a function", stepName)
	}

	if fnType.NumIn() != 3 {
		return nil, nil, fmt.Errorf("step %s: reducer must have signature func(context.Context, Acc, ItemOut) (Acc, error)", stepName)
	}

	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, nil, fmt.Errorf("step %s: reducer first parameter must be context.Context", stepName)
	}

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if fnType.NumOut() != 2 || !fnType.Out(1).Implements(errorType) {
		return nil, nil, fmt.Errorf("step %s: reducer must return (Acc, error)", stepName)
	}

	accType := fnType.In(1)
	if fnType.Out(0) != accType {
		return nil, nil, fmt.Errorf("step %s: reducer output accumulator type %v must match input accumulator type %v", stepName, fnType.Out(0), accType)
	}

	return accType, fnType.In(2), nil
}

// makeStepHandler uses reflection once to extract types and create cached wrapper for step handlers.
// Step handler signature: (context.Context, In, [Signal if signal enabled], [Dep1, Dep2, ...]) (Out, error)
// Returns wrapper with signature: (context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error)
// Also returns a map indicating which dependencies are Optional[T]
func makeStepHandler(fn any, stepName string, dependencies []string, hasSignal bool, isMapStep bool, mapSource string) (func(context.Context, []byte, map[string][]byte, []byte) ([]byte, error), map[string]bool, error) {
	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

	// Validate signature at build time
	if fnType.Kind() != reflect.Func {
		return nil, nil, fmt.Errorf("step %s: handler must be a function", stepName)
	}

	// Expected signature: (context.Context, In, [Signal if signal enabled], [Dep1, Dep2, ...]) (Out, error)
	expectedInputs := 2 // ctx + flow input
	if hasSignal {
		expectedInputs++ // signal input
	}
	expectedInputs += len(dependencies) // dependency outputs

	if fnType.NumIn() != expectedInputs {
		return nil, nil, fmt.Errorf("step %s: handler must have %d inputs (context.Context, In%s%s), got %d",
			stepName, expectedInputs,
			func() string {
				if hasSignal {
					return ", Signal"
				}
				return ""
			}(),
			func() string {
				if len(dependencies) > 0 {
					return fmt.Sprintf(", %d dependencies", len(dependencies))
				}
				return ""
			}(),
			fnType.NumIn())
	}

	// Validate context.Context is first parameter
	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, nil, fmt.Errorf("step %s: first parameter must be context.Context, got %v", stepName, fnType.In(0))
	}

	// Validate error return
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if fnType.NumOut() != 2 || !fnType.Out(1).Implements(errorType) {
		return nil, nil, fmt.Errorf("step %s: handler must return (Out, error)", stepName)
	}

	// Determine parameter indices
	signalParamIdx := 2 // Signal comes after ctx and flow input (if present)
	depStartIdx := 2    // Dependencies start here
	if hasSignal {
		depStartIdx = 3 // Dependencies start after signal
	}

	// CACHE: Extract all type info once - stored in closure
	flowInputType := fnType.In(1)
	var signalType reflect.Type
	if hasSignal {
		signalType = fnType.In(signalParamIdx)
	}

	// Cache dependency types and check for Optional[T]
	depTypes := make([]reflect.Type, len(dependencies))
	depIsOptional := make([]bool, len(dependencies))
	optionalDepsMap := make(map[string]bool)
	for i := range dependencies {
		paramType := fnType.In(depStartIdx + i)
		depTypes[i] = paramType

		// Check if type is Optional[T] (struct with IsSet bool and Value T fields)
		isOptional := paramType.Kind() == reflect.Struct &&
			paramType.NumField() >= 2 &&
			paramType.Field(0).Name == "IsSet" &&
			paramType.Field(0).Type.Kind() == reflect.Bool &&
			paramType.Field(1).Name == "Value"
		depIsOptional[i] = isOptional
		optionalDepsMap[dependencies[i]] = isOptional
	}

	mapFromInput := isMapStep && mapSource == ""
	mapDepIdx := -1
	if isMapStep && !mapFromInput {
		for i, depName := range dependencies {
			if depName == mapSource {
				mapDepIdx = i
				break
			}
		}
		if mapDepIdx == -1 {
			return nil, nil, fmt.Errorf("step %s: map source dependency %q not found", stepName, mapSource)
		}
		if depIsOptional[mapDepIdx] {
			return nil, nil, fmt.Errorf("step %s: map source dependency %q cannot be Optional[T]", stepName, mapSource)
		}
	}

	// Return wrapper with all type info cached in closure
	return func(ctx context.Context, flowInputJSON []byte, depsJSON map[string][]byte, signalInputJSON []byte) ([]byte, error) {
		buildAndCall := func(flowArg reflect.Value, depValues []reflect.Value, signalValue reflect.Value) (reflect.Value, error) {
			args := []reflect.Value{reflect.ValueOf(ctx), flowArg}
			if hasSignal {
				args = append(args, signalValue)
			}
			args = append(args, depValues...)

			results := fnVal.Call(args)
			if !results[1].IsNil() {
				return reflect.Value{}, results[1].Interface().(error)
			}
			return results[0], nil
		}

		// 1. Unmarshal flow input using cached type (or []type for MapInput)
		var flowInputVal reflect.Value
		if mapFromInput {
			flowInputSliceType := reflect.SliceOf(flowInputType)
			flowInputSliceVal := reflect.New(flowInputSliceType)
			if err := json.Unmarshal(flowInputJSON, flowInputSliceVal.Interface()); err != nil {
				return nil, fmt.Errorf("unmarshal flow input: %w", err)
			}
			flowInputVal = flowInputSliceVal.Elem()
		} else {
			flowInputPtr := reflect.New(flowInputType)
			if err := json.Unmarshal(flowInputJSON, flowInputPtr.Interface()); err != nil {
				return nil, fmt.Errorf("unmarshal flow input: %w", err)
			}
			flowInputVal = flowInputPtr.Elem()
		}

		// 2. Unmarshal signal if present (uses cached signalType)
		var signalVal reflect.Value
		if hasSignal {
			signalValPtr := reflect.New(signalType)
			if len(signalInputJSON) > 0 {
				if err := json.Unmarshal(signalInputJSON, signalValPtr.Interface()); err != nil {
					return nil, fmt.Errorf("unmarshal signal: %w", err)
				}
			}
			signalVal = signalValPtr.Elem()
		}

		// 3. Unmarshal dependencies using cached depTypes
		depVals := make([]reflect.Value, len(dependencies))
		var mappedDepVals reflect.Value
		for i, depName := range dependencies {
			if i == mapDepIdx {
				depSliceType := reflect.SliceOf(depTypes[i])
				depSliceVal := reflect.New(depSliceType)
				depJSON, ok := depsJSON[depName]
				if !ok {
					return nil, fmt.Errorf("missing required dependency: %s", depName)
				}
				if err := json.Unmarshal(depJSON, depSliceVal.Interface()); err != nil {
					return nil, fmt.Errorf("unmarshal dependency %s: %w", depName, err)
				}
				mappedDepVals = depSliceVal.Elem()
				continue
			}

			if depIsOptional[i] {
				// Handle Optional[T] - construct directly
				optVal := reflect.New(depTypes[i]).Elem()
				if depOutput, exists := depsJSON[depName]; exists && len(depOutput) > 0 {
					// Set IsSet = true
					optVal.FieldByName("IsSet").SetBool(true)
					// Unmarshal into Value field
					valueField := optVal.FieldByName("Value")
					valuePtr := reflect.New(valueField.Type())
					if err := json.Unmarshal(depOutput, valuePtr.Interface()); err != nil {
						return nil, fmt.Errorf("unmarshal optional dependency %s: %w", depName, err)
					}
					valueField.Set(valuePtr.Elem())
				}
				// If not exists, optVal stays as Optional{IsSet: false}
				depVals[i] = optVal
			} else {
				// Handle required dependency
				depVal := reflect.New(depTypes[i])
				depJSON, ok := depsJSON[depName]
				if !ok {
					return nil, fmt.Errorf("missing required dependency: %s", depName)
				}
				if err := json.Unmarshal(depJSON, depVal.Interface()); err != nil {
					return nil, fmt.Errorf("unmarshal dependency %s: %w", depName, err)
				}
				depVals[i] = depVal.Elem()
			}
		}

		if !isMapStep {
			out, err := buildAndCall(flowInputVal, depVals, signalVal)
			if err != nil {
				return nil, err
			}
			return json.Marshal(out.Interface())
		}

		mapLen := 0
		if mapFromInput {
			mapLen = flowInputVal.Len()
		} else {
			mapLen = mappedDepVals.Len()
		}

		outSlice := reflect.MakeSlice(reflect.SliceOf(fnType.Out(0)), 0, mapLen)
		for i := 0; i < mapLen; i++ {
			depValsForCall := depVals
			flowArg := flowInputVal

			if mapFromInput {
				flowArg = flowInputVal.Index(i)
			} else {
				depValsForCall = make([]reflect.Value, len(depVals))
				copy(depValsForCall, depVals)
				depValsForCall[mapDepIdx] = mappedDepVals.Index(i)
			}

			out, err := buildAndCall(flowArg, depValsForCall, signalVal)
			if err != nil {
				return nil, err
			}
			outSlice = reflect.Append(outSlice, out)
		}

		return json.Marshal(outSlice.Interface())
	}, optionalDepsMap, nil
}

func makeFlowOnFailHandler(fn any) (func(context.Context, json.RawMessage, FlowFailure) error, error) {
	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

	if fnType.Kind() != reflect.Func {
		return nil, fmt.Errorf("on-fail handler must be a function")
	}
	if fnType.NumIn() != 3 || fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, fmt.Errorf("on-fail handler must have signature (context.Context, In, FlowFailure) error")
	}
	if fnType.In(2) != reflect.TypeOf(FlowFailure{}) {
		return nil, fmt.Errorf("on-fail handler third parameter must be FlowFailure")
	}
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if fnType.NumOut() != 1 || !fnType.Out(0).Implements(errorType) {
		return nil, fmt.Errorf("on-fail handler must return error")
	}

	inputType := fnType.In(1)

	return func(ctx context.Context, inputJSON json.RawMessage, failure FlowFailure) error {
		inputVal := reflect.New(inputType)
		if err := json.Unmarshal(inputJSON, inputVal.Interface()); err != nil {
			return fmt.Errorf("unmarshal input: %w", err)
		}

		results := fnVal.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			inputVal.Elem(),
			reflect.ValueOf(failure),
		})

		if !results[0].IsNil() {
			return results[0].Interface().(error)
		}

		return nil
	}, nil
}

type FlowInfo struct {
	Name            string        `json:"name"`
	Description     string        `json:"description,omitempty"`
	Steps           []StepInfo    `json:"steps"`
	OutputPriority  []string      `json:"output_priority,omitempty"`
	RetentionPeriod time.Duration `json:"retention_period,omitzero"`
	CreatedAt       time.Time     `json:"created_at"`
}

// FlowScheduleInfo contains metadata about a scheduled flow.
type FlowScheduleInfo struct {
	FlowName       string    `json:"flow_name"`
	CronSpec       string    `json:"cron_spec"`
	NextRunAt      time.Time `json:"next_run_at"`
	LastRunAt      time.Time `json:"last_run_at,omitzero"`
	LastEnqueuedAt time.Time `json:"last_enqueued_at,omitzero"`
	Enabled        bool      `json:"enabled"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type StepInfo struct {
	Name             string               `json:"name"`
	Description      string               `json:"description,omitempty"`
	StepType         StepType             `json:"step_type,omitempty"`
	MapSource        string               `json:"map_source,omitempty"`
	ReduceSourceStep string               `json:"reduce_source_step,omitempty"`
	HasSignal        bool                 `json:"has_signal,omitempty"`
	DependsOn        []StepDependencyInfo `json:"depends_on,omitempty"`
}

type StepDependencyInfo struct {
	Name string `json:"name"`
}

type stepClaim struct {
	ID          int64                      `json:"id"`
	FlowRunID   int64                      `json:"flow_run_id"`
	Attempts    int                        `json:"attempts"`
	Input       json.RawMessage            `json:"input"`
	StepOutputs map[string]json.RawMessage `json:"step_outputs"`
	SignalInput json.RawMessage            `json:"signal_input"`
}

func dependsOnStep(step *Step, depName string) bool {
	for _, name := range step.dependencies {
		if name == depName {
			return true
		}
	}
	return false
}

func validateGeneratorStepDefinition(flowName string, step *Step) error {
	if step.stepType != StepTypeGenerator {
		return nil
	}
	if step.stepType == StepTypeMapper {
		return fmt.Errorf("flow %q: step %q generator steps cannot use map mode", flowName, step.name)
	}
	if step.stepType == StepTypeReducer {
		return fmt.Errorf("flow %q: step %q generator steps cannot be reducer steps", flowName, step.name)
	}
	if step.generatorFn == nil {
		return fmt.Errorf("flow %q: step %q generator step is missing Generate(fn)", flowName, step.name)
	}
	if step.generatorHandler == nil {
		return fmt.Errorf("flow %q: step %q generator step is missing Map(fn)", flowName, step.name)
	}
	return validateGeneratorFnForStep(step, flowName)
}

func validateMapperStepDefinition(flowName string, step *Step, stepNameSet map[string]bool) error {
	if step.stepType != StepTypeMapper {
		return nil
	}
	if step.mapSource == "" {
		return nil
	}
	if step.mapSource == step.name {
		return fmt.Errorf("flow %q: step %q cannot map its own output", flowName, step.name)
	}
	if !stepNameSet[step.mapSource] {
		return fmt.Errorf("flow %q: step %q maps dependency %q which does not exist", flowName, step.name, step.mapSource)
	}
	if !dependsOnStep(step, step.mapSource) {
		return fmt.Errorf("flow %q: step %q maps %q but does not depend on it", flowName, step.name, step.mapSource)
	}
	if step.optionalDependencies[step.mapSource] {
		return fmt.Errorf("flow %q: step %q cannot map optional dependency %q", flowName, step.name, step.mapSource)
	}
	return nil
}

func validateReducerStepDefinition(flowName string, step *Step, stepNameSet map[string]bool, stepByName map[string]*Step) error {
	if step.stepType != StepTypeReducer {
		return nil
	}
	if step.reduceSourceStep == "" {
		return fmt.Errorf("flow %q: step %q reducer step is missing ReduceStep(source)", flowName, step.name)
	}
	if step.reduceSourceStep == step.name {
		return fmt.Errorf("flow %q: step %q cannot reduce its own output", flowName, step.name)
	}
	if !stepNameSet[step.reduceSourceStep] {
		return fmt.Errorf("flow %q: step %q reduces unknown source step %q", flowName, step.name, step.reduceSourceStep)
	}
	sourceStep := stepByName[step.reduceSourceStep]
	if sourceStep == nil || (sourceStep.stepType != StepTypeMapper && sourceStep.stepType != StepTypeGenerator) {
		return fmt.Errorf("flow %q: step %q can only reduce mapper/generator source step %q", flowName, step.name, step.reduceSourceStep)
	}
	if !dependsOnStep(step, step.reduceSourceStep) {
		return fmt.Errorf("flow %q: step %q reduces %q but does not depend on it", flowName, step.name, step.reduceSourceStep)
	}
	if step.reducerFn == nil || step.reducerAcc == nil || len(step.reducerInit) == 0 || step.reducerItem == nil {
		return fmt.Errorf("flow %q: step %q reducer definition is invalid", flowName, step.name)
	}
	return nil
}

func validateFlowDependencies(flow *Flow) error {
	steps := flow.steps
	if len(steps) == 0 {
		return fmt.Errorf("flow %q must have at least one step", flow.name)
	}

	stepNameSet := make(map[string]bool)
	stepByName := make(map[string]*Step, len(steps))
	for _, step := range steps {
		stepNameSet[step.name] = true
		stepByName[step.name] = step
	}

	for _, step := range steps {
		if err := validateGeneratorStepDefinition(flow.name, step); err != nil {
			return err
		}
		if err := validateMapperStepDefinition(flow.name, step, stepNameSet); err != nil {
			return err
		}
		if err := validateReducerStepDefinition(flow.name, step, stepNameSet, stepByName); err != nil {
			return err
		}
	}

	// Build set of conditional steps (steps that have a condition and may be skipped)
	conditionalSteps := make(map[string]bool)
	for _, step := range steps {
		if strings.TrimSpace(step.condition) != "" {
			conditionalSteps[step.name] = true
		}
	}

	// Build set of all steps that have dependents
	dependencySet := make(map[string]bool)
	for _, step := range steps {
		for _, depName := range step.dependencies {
			dependencySet[depName] = true
		}
	}

	// Find all steps with no dependents (final steps)
	var finalSteps []string
	for _, step := range steps {
		if !dependencySet[step.name] {
			finalSteps = append(finalSteps, step.name)
		}
	}

	// Validate structural terminal steps
	if len(finalSteps) == 0 {
		return fmt.Errorf("flow %q has no final step (circular dependency?)", flow.name)
	}

	if flow.priorityConfigured {
		if len(flow.outputPriority) == 0 {
			return fmt.Errorf("flow %q: output priority must not be empty", flow.name)
		}

		seenOutputSteps := make(map[string]bool, len(flow.outputPriority))
		for i, outputStepName := range flow.outputPriority {
			if strings.TrimSpace(outputStepName) == "" {
				return fmt.Errorf("flow %q: output priority at index %d must not be empty", flow.name, i)
			}
			if !stepNameSet[outputStepName] {
				return fmt.Errorf("flow %q: output priority references unknown step %q", flow.name, outputStepName)
			}
			if seenOutputSteps[outputStepName] {
				return fmt.Errorf("flow %q: output priority contains duplicate step %q", flow.name, outputStepName)
			}
			seenOutputSteps[outputStepName] = true
		}

		for _, terminalStepName := range finalSteps {
			if !seenOutputSteps[terminalStepName] {
				return fmt.Errorf("flow %q: output priority must include structural terminal step %q", flow.name, terminalStepName)
			}
		}
	}

	// Validate that dependencies on conditional steps use Optional[T]
	for _, step := range steps {
		if step.stepType == StepTypeReducer {
			continue
		}
		for _, depName := range step.dependencies {
			if conditionalSteps[depName] {
				// This is a dependency on a conditional step
				// Check if the dependency is marked as optional
				if !step.optionalDependencies[depName] {
					return fmt.Errorf("flow %q: step %q depends on conditional step %q but the handler does not use Optional[T] parameter - use Optional[%T] in the handler signature", flow.name, step.name, depName, depName)
				}
			}
		}
	}

	return nil
}

// CreateFlow creates one or more flow definitions.
func CreateFlow(ctx context.Context, conn Conn, flows ...*Flow) error {
	q := `SELECT * FROM cb_create_flow(name => $1, description => $2, steps => $3, output_priority => $4, retention_period => $5);`
	for _, flow := range flows {
		if err := validateFlowDependencies(flow); err != nil {
			return err
		}

		// Need to marshal the steps with their public fields for JSON
		// Convert to a serializable structure
		type stepDependency struct {
			Name string `json:"name"`
		}
		type serializableStep struct {
			Name             string            `json:"name"`
			Description      string            `json:"description,omitempty"`
			Condition        string            `json:"condition,omitempty"`
			StepType         StepType          `json:"step_type,omitempty"`
			MapSource        string            `json:"map_source,omitempty"`
			ReduceSourceStep string            `json:"reduce_source_step,omitempty"`
			HasSignal        bool              `json:"has_signal"`
			DependsOn        []*stepDependency `json:"depends_on,omitempty"`
		}

		steps := flow.steps
		serSteps := make([]serializableStep, len(steps))
		for i, s := range steps {
			// Convert dependency names to stepDependency objects
			depNames := s.dependencies
			deps := make([]*stepDependency, len(depNames))
			for j, depName := range depNames {
				deps[j] = &stepDependency{Name: depName}
			}

			serStep := serializableStep{
				Name:             s.name,
				Description:      s.description,
				StepType:         s.stepType,
				MapSource:        s.mapSource,
				ReduceSourceStep: s.reduceSourceStep,
				HasSignal:        s.hasSignal,
				DependsOn:        deps,
			}
			serStep.Condition = s.condition
			serSteps[i] = serStep
		}

		b, err := json.Marshal(serSteps)
		if err != nil {
			return err
		}

		priority := flow.outputPriority
		if !flow.priorityConfigured {
			priority = defaultFlowOutputPriority(flow)
		}

		var retentionPeriod *time.Duration
		if flow.retentionPeriod > 0 {
			retentionPeriod = &flow.retentionPeriod
		}
		_, err = conn.Exec(ctx, q, flow.name, ptrOrNil(flow.description), b, priority, retentionPeriod)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetFlow retrieves flow metadata by name.
func GetFlow(ctx context.Context, conn Conn, flowName string) (*FlowInfo, error) {
	q := `SELECT * FROM cb_flow_info WHERE name = $1;`
	return scanFlow(conn.QueryRow(ctx, q, flowName))
}

// ListFlows returns all flows
func ListFlows(ctx context.Context, conn Conn) ([]*FlowInfo, error) {
	q := `SELECT * FROM cb_flow_info ORDER BY name;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleFlow)
}

type RunFlowOpts struct {
	ConcurrencyKey string // Prevents overlapping runs; allows reruns after completion
	IdempotencyKey string // Prevents all duplicate runs; permanent across all statuses
	Headers        map[string]any
	VisibleAt      time.Time
}

// FlowRunInfo represents the details of a flow execution.
type FlowRunInfo struct {
	ID                int64           `json:"id"`
	ConcurrencyKey    string          `json:"concurrency_key,omitempty"`
	IdempotencyKey    string          `json:"idempotency_key,omitempty"`
	Status            string          `json:"status"`
	Input             json.RawMessage `json:"input,omitempty"`
	Headers           json.RawMessage `json:"headers,omitempty"`
	Output            json.RawMessage `json:"output,omitempty"`
	ErrorMessage      string          `json:"error_message,omitempty"`
	CancelReason      string          `json:"cancel_reason,omitempty"`
	CancelRequestedAt time.Time       `json:"cancel_requested_at,omitzero"`
	CanceledAt        time.Time       `json:"canceled_at,omitzero"`
	StartedAt         time.Time       `json:"started_at,omitzero"`
	CompletedAt       time.Time       `json:"completed_at,omitzero"`
	FailedAt          time.Time       `json:"failed_at,omitzero"`
}

// IsDone reports whether the flow run reached a terminal state.
func (r *FlowRunInfo) IsDone() bool {
	switch r.Status {
	case StatusCompleted, StatusFailed, StatusCanceled:
		return true
	default:
		return false
	}
}

// IsCompleted reports whether the flow run completed successfully.
func (r *FlowRunInfo) IsCompleted() bool {
	return r.Status == StatusCompleted
}

// OutputAs unmarshals the output of a completed flow run.
// Returns an error if the flow run has failed or is not completed yet.
func (r *FlowRunInfo) OutputAs(out any) error {
	if r.Status == StatusFailed {
		return fmt.Errorf("%w: %s", ErrRunFailed, r.ErrorMessage)
	}
	if r.Status == StatusCanceled {
		return canceledRunError(r.CancelReason)
	}
	if r.Status != StatusCompleted {
		return fmt.Errorf("run not completed: current status is %s", r.Status)
	}
	return json.Unmarshal(r.Output, out)
}

// FlowHandle is a handle to a flow execution.
type FlowHandle struct {
	conn Conn
	Name string
	ID   int64
}

// WaitForOutput blocks until the flow execution completes and unmarshals the output.
// Pass optional WaitOpts to customize polling behavior; defaults are used when omitted.
func (h *FlowHandle) WaitForOutput(ctx context.Context, out any, opts ...WaitOpts) error {
	var pollFor time.Duration
	var pollInterval time.Duration

	if len(opts) > 0 {
		pollFor = opts[0].PollFor
		pollInterval = opts[0].PollInterval
	}

	pollForMs, pollIntervalMs := resolvePollDurations(defaultPollFor, defaultPollInterval, pollFor, pollInterval)

	q := `
		SELECT status, output, error_message
		FROM cb_wait_flow_output(flow_name => $1, run_id => $2, poll_for => $3, poll_interval => $4);
	`

	for {
		var status string
		var output json.RawMessage
		var errorMessage *string

		err := h.conn.QueryRow(ctx, q, h.Name, h.ID, pollForMs, pollIntervalMs).Scan(&status, &output, &errorMessage)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			return err
		}

		switch status {
		case StatusCompleted:
			return json.Unmarshal(output, out)
		case StatusFailed:
			if errorMessage != nil {
				return fmt.Errorf("%w: %s", ErrRunFailed, *errorMessage)
			}
			return ErrRunFailed
		case StatusCanceled:
			if errorMessage != nil {
				return canceledRunError(*errorMessage)
			}
			return ErrRunCanceled
		}
	}
}

// CancelFlowRun requests cancellation for a flow run.
// Returns nil for idempotent no-op when the run is already terminal.
func CancelFlowRun(ctx context.Context, conn Conn, flowName string, runID int64, opts ...CancelOpts) error {
	q := `SELECT changed, final_status FROM cb_request_flow_cancellation(name => $1, run_id => $2, reason => $3);`
	var changed bool
	var finalStatus string
	err := conn.QueryRow(ctx, q, flowName, runID, resolveCancelReason(opts...)).Scan(&changed, &finalStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	_ = changed
	_ = finalStatus
	return nil
}

// GetStep retrieves status details for a step in the current flow run.
// Intended for use inside flow step handlers.
func GetStep(ctx context.Context, stepName string) (*StepRunInfo, error) {
	scope, _ := ctx.Value(flowRunScopeContextKey{}).(*flowRunScope)
	if scope == nil || scope.conn == nil {
		return nil, ErrNoRunContext
	}
	return getStepStatus(ctx, scope.conn, scope.name, scope.runID, stepName)
}

// WaitForStep blocks until the given step reaches a terminal state in the current flow run.
// Pass optional WaitOpts to customize polling behavior; defaults are used when omitted.
func WaitForStep(ctx context.Context, stepName string, opts ...WaitOpts) (*StepRunInfo, error) {
	var pollFor time.Duration
	var pollInterval time.Duration

	if len(opts) > 0 {
		pollFor = opts[0].PollFor
		pollInterval = opts[0].PollInterval
	}

	_, pollIntervalMs := resolvePollDurations(defaultPollFor, defaultPollInterval, pollFor, pollInterval)
	interval := time.Duration(pollIntervalMs) * time.Millisecond

	for {
		step, err := GetStep(ctx, stepName)
		if err != nil {
			return nil, err
		}
		if step.IsDone() {
			return step, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

// RunFlow enqueues a flow execution and returns a handle for monitoring.
func RunFlow(ctx context.Context, conn Conn, flowName string, input any, opts ...RunFlowOpts) (*FlowHandle, error) {
	q, args, err := RunFlowQuery(flowName, input, opts...)
	if err != nil {
		return nil, err
	}

	var id int64
	err = conn.QueryRow(ctx, q, args...).Scan(&id)
	if err != nil {
		return nil, err
	}
	return &FlowHandle{conn: conn, Name: flowName, ID: id}, nil
}

// RunFlowQuery builds the SQL query and args for a RunFlow operation.
// Pass no opts to use defaults.
func RunFlowQuery(flowName string, input any, opts ...RunFlowOpts) (string, []any, error) {
	var resolved RunFlowOpts
	if len(opts) > 0 {
		resolved = opts[0]
	}

	b, err := json.Marshal(input)
	if err != nil {
		return "", nil, err
	}
	headers, err := marshalOptionalHeaders(resolved.Headers)
	if err != nil {
		return "", nil, err
	}

	q := `SELECT * FROM cb_run_flow(name => $1, input => $2, concurrency_key => $3, idempotency_key => $4, headers => $5, visible_at => $6);`
	args := []any{flowName, b, ptrOrNil(resolved.ConcurrencyKey), ptrOrNil(resolved.IdempotencyKey), headers, ptrOrNil(resolved.VisibleAt)}

	return q, args, nil
}

// GetFlowRun retrieves a specific flow run result by ID.
func GetFlowRun(ctx context.Context, conn Conn, flowName string, flowRunID int64) (*FlowRunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(flowName))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, headers, output, error_message, cancel_reason, cancel_requested_at, canceled_at, started_at, completed_at, failed_at FROM %s WHERE id = $1;`, pgx.Identifier{tableName}.Sanitize())
	return scanFlowRun(conn.QueryRow(ctx, query, flowRunID))
}

// ListFlowRuns returns recent flow runs for the specified flow.
func ListFlowRuns(ctx context.Context, conn Conn, flowName string) ([]*FlowRunInfo, error) {
	tableName := fmt.Sprintf("cb_f_%s", strings.ToLower(flowName))
	query := fmt.Sprintf(`SELECT id, concurrency_key, idempotency_key, status, input, headers, output, error_message, cancel_reason, cancel_requested_at, canceled_at, started_at, completed_at, failed_at FROM %s ORDER BY started_at DESC LIMIT 20;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanCollectibleFlowRun)
}

func scanCollectibleFlowRun(row pgx.CollectableRow) (*FlowRunInfo, error) {
	return scanFlowRun(row)
}

func scanFlowRun(row pgx.Row) (*FlowRunInfo, error) {
	rec := FlowRunInfo{}

	var concurrencyKey *string
	var idempotencyKey *string
	var input *json.RawMessage
	var headers *json.RawMessage
	var output *json.RawMessage
	var errorMessage *string
	var cancelReason *string
	var cancelRequestedAt *time.Time
	var canceledAt *time.Time
	var completedAt *time.Time
	var failedAt *time.Time

	if err := row.Scan(
		&rec.ID,
		&concurrencyKey,
		&idempotencyKey,
		&rec.Status,
		&input,
		&headers,
		&output,
		&errorMessage,
		&cancelReason,
		&cancelRequestedAt,
		&canceledAt,
		&rec.StartedAt,
		&completedAt,
		&failedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if concurrencyKey != nil {
		rec.ConcurrencyKey = *concurrencyKey
	}
	if idempotencyKey != nil {
		rec.IdempotencyKey = *idempotencyKey
	}
	if input != nil {
		rec.Input = *input
	}
	if headers != nil {
		rec.Headers = *headers
	}
	if output != nil {
		rec.Output = *output
	}
	if errorMessage != nil {
		rec.ErrorMessage = *errorMessage
	}
	if cancelReason != nil {
		rec.CancelReason = *cancelReason
	}
	if cancelRequestedAt != nil {
		rec.CancelRequestedAt = *cancelRequestedAt
	}
	if canceledAt != nil {
		rec.CanceledAt = *canceledAt
	}
	if completedAt != nil {
		rec.CompletedAt = *completedAt
	}
	if failedAt != nil {
		rec.FailedAt = *failedAt
	}

	return &rec, nil
}

// StepRunInfo represents the execution state of a single step within a flow run.
type StepRunInfo struct {
	ID           int64           `json:"id"`
	StepName     string          `json:"step_name"`
	Status       string          `json:"status"`
	Attempts     int             `json:"attempts"`
	Output       json.RawMessage `json:"output,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	CreatedAt    time.Time       `json:"created_at,omitzero"`
	VisibleAt    time.Time       `json:"visible_at,omitzero"`
	StartedAt    time.Time       `json:"started_at,omitzero"`
	CompletedAt  time.Time       `json:"completed_at,omitzero"`
	FailedAt     time.Time       `json:"failed_at,omitzero"`
	SkippedAt    time.Time       `json:"skipped_at,omitzero"`
	CanceledAt   time.Time       `json:"canceled_at,omitzero"`
}

// IsDone reports whether the step run reached a terminal state.
func (r *StepRunInfo) IsDone() bool {
	switch r.Status {
	case StatusCompleted, StatusFailed, StatusSkipped, StatusCanceled:
		return true
	default:
		return false
	}
}

// IsCompleted reports whether the step run completed successfully.
func (r *StepRunInfo) IsCompleted() bool {
	return r.Status == StatusCompleted
}

func getStepStatus(ctx context.Context, conn Conn, flowName string, flowRunID int64, stepName string) (*StepRunInfo, error) {
	q := `
		SELECT id, status, attempts, output, error_message, created_at, visible_at, started_at, completed_at, failed_at, skipped_at, canceled_at
		FROM cb_get_flow_step_status(flow_name => $1, run_id => $2, step_name => $3);
	`

	var info StepRunInfo
	info.StepName = stepName
	var output *json.RawMessage
	var errorMessage *string
	var startedAt, completedAt, failedAt, skippedAt, canceledAt *time.Time

	err := conn.QueryRow(ctx, q, flowName, flowRunID, stepName).Scan(
		&info.ID,
		&info.Status,
		&info.Attempts,
		&output,
		&errorMessage,
		&info.CreatedAt,
		&info.VisibleAt,
		&startedAt,
		&completedAt,
		&failedAt,
		&skippedAt,
		&canceledAt,
	)
	if err != nil {
		return nil, err
	}

	if output != nil {
		info.Output = *output
	}
	if errorMessage != nil {
		info.ErrorMessage = *errorMessage
	}
	if startedAt != nil {
		info.StartedAt = *startedAt
	}
	if completedAt != nil {
		info.CompletedAt = *completedAt
	}
	if failedAt != nil {
		info.FailedAt = *failedAt
	}
	if skippedAt != nil {
		info.SkippedAt = *skippedAt
	}
	if canceledAt != nil {
		info.CanceledAt = *canceledAt
	}

	return &info, nil
}

// GetFlowRunSteps retrieves all step runs for a specific flow run.
func GetFlowRunSteps(ctx context.Context, conn Conn, flowName string, flowRunID int64) ([]*StepRunInfo, error) {
	tableName := fmt.Sprintf("cb_s_%s", strings.ToLower(flowName))
	query := fmt.Sprintf(`
		SELECT id, step_name, status, attempts, output, error_message, created_at, visible_at, started_at, completed_at, failed_at, skipped_at, canceled_at
		FROM %s
		WHERE flow_run_id = $1
		ORDER BY id;`, pgx.Identifier{tableName}.Sanitize())
	rows, err := conn.Query(ctx, query, flowRunID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*StepRunInfo, error) {
		var s StepRunInfo
		var output *json.RawMessage
		var errorMessage *string
		var startedAt, completedAt, failedAt, skippedAt, canceledAt *time.Time

		err := row.Scan(
			&s.ID,
			&s.StepName,
			&s.Status,
			&s.Attempts,
			&output,
			&errorMessage,
			&s.CreatedAt,
			&s.VisibleAt,
			&startedAt,
			&completedAt,
			&failedAt,
			&skippedAt,
			&canceledAt,
		)
		if err != nil {
			return nil, err
		}

		// Copy nullable fields
		if output != nil {
			s.Output = *output
		}
		if errorMessage != nil {
			s.ErrorMessage = *errorMessage
		}
		if startedAt != nil {
			s.StartedAt = *startedAt
		}
		if completedAt != nil {
			s.CompletedAt = *completedAt
		}
		if failedAt != nil {
			s.FailedAt = *failedAt
		}
		if skippedAt != nil {
			s.SkippedAt = *skippedAt
		}
		if canceledAt != nil {
			s.CanceledAt = *canceledAt
		}

		return &s, nil
	})
}

// SignalFlow delivers a signal to a waiting step in a flow run.
// The step must have been defined with `.WithSignal()`.
// Signals enable human-in-the-loop workflows where a step waits for external input before executing.
// Returns an error if the signal was already delivered or the step doesn't require a signal.
func SignalFlow(ctx context.Context, conn Conn, flowName string, flowRunID int64, stepName string, input any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}
	q := `SELECT cb_signal_flow($1, $2, $3, $4);`
	var delivered bool
	err = conn.QueryRow(ctx, q, flowName, flowRunID, stepName, b).Scan(&delivered)
	if err != nil {
		return err
	}
	if !delivered {
		return fmt.Errorf("signal not delivered: step may not require signal or signal already delivered")
	}
	return nil
}

func scanCollectibleFlow(row pgx.CollectableRow) (*FlowInfo, error) {
	return scanFlow(row)
}

func scanFlow(row pgx.Row) (*FlowInfo, error) {
	rec := FlowInfo{}

	var description *string
	var steps json.RawMessage
	var outputPriority []string
	var retentionPeriod *time.Duration

	if err := row.Scan(
		&rec.Name,
		&description,
		&steps,
		&outputPriority,
		&retentionPeriod,
		&rec.CreatedAt,
	); err != nil {
		return nil, err
	}

	if description != nil {
		rec.Description = *description
	}

	if err := json.Unmarshal(steps, &rec.Steps); err != nil {
		return nil, err
	}

	rec.OutputPriority = outputPriority

	if retentionPeriod != nil {
		rec.RetentionPeriod = *retentionPeriod
	}

	return &rec, nil
}

func defaultFlowOutputPriority(flow *Flow) []string {
	if len(flow.steps) == 0 {
		return nil
	}

	hasDependent := make(map[string]bool, len(flow.steps))
	for _, step := range flow.steps {
		for _, depName := range step.dependencies {
			hasDependent[depName] = true
		}
	}

	priority := make([]string, 0, len(flow.steps))
	for _, step := range flow.steps {
		if !hasDependent[step.name] {
			priority = append(priority, step.name)
		}
	}

	return priority
}

// CreateFlowSchedule creates a cron-based schedule for a flow.
func CreateFlowSchedule(ctx context.Context, conn Conn, flowName, cronSpec string, opts ...ScheduleOpt) error {
	var inputJSON []byte
	var err error

	resolved := applyDefaultScheduleOpts(opts...)

	if resolved.input == nil {
		inputJSON = []byte("{}")
	} else {
		inputJSON, err = json.Marshal(resolved.input)
		if err != nil {
			return fmt.Errorf("failed to marshal flow schedule input: %w", err)
		}
	}

	_, err = conn.Exec(ctx, `SELECT cb_create_flow_schedule($1, $2, $3);`, flowName, cronSpec, inputJSON)
	if err != nil {
		return fmt.Errorf("failed to create flow schedule %q: %w", flowName, err)
	}
	return nil
}

// ListFlowSchedules returns all flow schedules ordered by next_run_at.
func ListFlowSchedules(ctx context.Context, conn Conn) ([]*FlowScheduleInfo, error) {
	q := `SELECT flow_name, cron_spec, next_run_at, last_run_at, last_enqueued_at, enabled, created_at, updated_at
		FROM cb_flow_schedules
		ORDER BY next_run_at ASC;`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*FlowScheduleInfo, error) {
		var s FlowScheduleInfo
		var lastRunAt *time.Time
		var lastEnqueuedAt *time.Time
		err := row.Scan(&s.FlowName, &s.CronSpec, &s.NextRunAt, &lastRunAt, &lastEnqueuedAt, &s.Enabled, &s.CreatedAt, &s.UpdatedAt)
		if lastRunAt != nil {
			s.LastRunAt = *lastRunAt
		}
		if lastEnqueuedAt != nil {
			s.LastEnqueuedAt = *lastEnqueuedAt
		}
		return &s, err
	})
}
