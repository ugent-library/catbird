package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Plan is a handler's buffer of follow-on work: the steps it adds and the
// run output it sets. Every method buffers; nothing blocks. The buffer
// commits with the step's completion, in one transaction — a handler that
// fails or crashes midway commits nothing, and its retry starts with an
// empty buffer.
type Plan struct {
	ctx         context.Context
	conn        Conn
	runID       int64
	signalInput json.RawMessage
	defined     func(ctx context.Context, name string) bool

	steps     []step
	runOutput json.RawMessage
}

// step is one buffered entry, in exactly the shape cb_job_complete's
// steps parameter takes.
type step struct {
	Name           string          `json:"name"`
	Input          json.RawMessage `json:"input"`
	WaitsForSteps  bool            `json:"waits_for_steps"`
	WaitsForSignal bool            `json:"waits_for_signal"`
}

// StepOpt tunes a step added to a Plan.
type StepOpt func(*step)

// WaitsForSignal makes the step wait for a payload sent to its name
// (Signal) before it can run. On a p.After().Step the step waits for the
// run's other steps first, then for the signal. Among a run's unresolved
// steps the name must be unique: adding a second signal-waiting step with
// a name still in use fails the completion.
func WaitsForSignal() StepOpt {
	return func(s *step) { s.WaitsForSignal = true }
}

// Step adds a step running the named job. It becomes claimable the moment
// this handler's completion commits, unless an option or After says what
// it waits for. The input is marshaled here and carried on the step row;
// a parent passes its results forward in it — the parent is finished by
// the time its steps run. Step panics when the job is not defined or the
// input cannot be marshaled: the panic fails this attempt, putting the
// bug on the attempt row with this call in the stack.
func (p *Plan) Step(name string, input any, opts ...StepOpt) {
	p.add(name, input, false, opts)
}

// After scopes what an added step waits for: everything the run owes
// right now, this handler's other buffered steps included. The returned
// value's only method is Step, so a half-built chain buffers nothing.
func (p *Plan) After() After {
	return After{plan: p}
}

// After is the scope p.After() opens. A step added through it waits until
// everything the run owes has completed successfully; when the run is
// given up on instead, waiting steps are canceled — cleanup that must run
// despite failure belongs to on_fail.
type After struct {
	plan *Plan
}

// Step adds a step like Plan.Step, waiting first for everything the run
// owes.
func (a After) Step(name string, input any, opts ...StepOpt) {
	a.plan.add(name, input, true, opts)
}

func (p *Plan) add(name string, input any, waitsForSteps bool, opts []StepOpt) {
	if !p.defined(p.ctx, name) {
		panic(fmt.Sprintf("catbird: job %s not defined", name))
	}
	s := step{Name: name, WaitsForSteps: waitsForSteps}
	if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			panic(fmt.Sprintf("catbird: step %s: marshal input: %v", name, err))
		}
		s.Input = b
	}
	for _, opt := range opts {
		opt(&s)
	}
	p.steps = append(p.steps, s)
}

// SetRunOutput sets the run's output — the run's only output: a handler's
// return value is its step's output, and the engine never promotes one to
// the run. Any step may set it; when several do, the last completion
// wins. A run whose steps never set one finishes with no output. Panics
// when v cannot be marshaled, failing the attempt.
func (p *Plan) SetRunOutput(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("catbird: marshal run output: %v", err))
	}
	p.runOutput = b
}

// RunInput reads the run's input — what the run was created with, kept on
// the run row. A step's own input is the handler's argument; this is the
// birth input, the same for every step of the run.
func RunInput[T any](p *Plan) (T, error) {
	var v T
	var raw json.RawMessage
	err := p.conn.QueryRow(p.ctx,
		`SELECT input FROM cb_job_runs WHERE id = $1`, p.runID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, ErrNotFound
	}
	if err != nil || raw == nil {
		return v, err
	}
	err = json.Unmarshal(raw, &v)
	return v, err
}

// SignalInput reads the payload that satisfied this step's signal wait —
// delivered by Signal, stamped into the step's signal_input column and
// handed to the handler by cb_job_start. The zero value when the step did
// not wait for a signal, or the payload was empty.
func SignalInput[T any](p *Plan) (T, error) {
	var v T
	if p.signalInput == nil {
		return v, nil
	}
	err := json.Unmarshal(p.signalInput, &v)
	return v, err
}

// StepOutput reads the output of the run's one completed step named name —
// how a barrier reads a step that did not exist yet when the barrier was
// added. Exactly one completed step may hold the name: none is
// ErrNotFound, several — a fan-out — are read with StepOutputs.
func StepOutput[T any](p *Plan, name string) (T, error) {
	var v T
	outputs, err := p.completedOutputs(name)
	if err != nil {
		return v, err
	}
	if len(outputs) == 0 {
		return v, fmt.Errorf("%w: no completed step %s", ErrNotFound, name)
	}
	if len(outputs) > 1 {
		return v, fmt.Errorf("catbird: %d completed steps named %s; read them with StepOutputs", len(outputs), name)
	}
	if outputs[0] == nil {
		return v, nil
	}
	err = json.Unmarshal(outputs[0], &v)
	return v, err
}

// StepOutputs reads the outputs of the run's completed steps named name,
// one element per step in the order the steps were added — the fan-out
// reader. Any count, zero included.
func StepOutputs[T any](p *Plan, name string) ([]T, error) {
	outputs, err := p.completedOutputs(name)
	if err != nil {
		return nil, err
	}
	vs := make([]T, len(outputs))
	for i, raw := range outputs {
		if raw == nil {
			continue
		}
		if err := json.Unmarshal(raw, &vs[i]); err != nil {
			return nil, fmt.Errorf("step %s: unmarshal output %d: %w", name, i, err)
		}
	}
	return vs, nil
}

func (p *Plan) completedOutputs(name string) ([]json.RawMessage, error) {
	rows, err := p.conn.Query(p.ctx,
		`SELECT s.output FROM cb_job_steps s
		 WHERE s.run_id = $1 AND s.name = $2 AND s.status = 'completed'
		 ORDER BY s.id`, p.runID, name)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[json.RawMessage])
}
