// Package wire delivers stream messages to browsers as HTML fragments. A
// process-wide Renderer says how the messages under a topic pattern become
// fragments; the transports that carry them to a page are built on it.
package wire

import (
	"bytes"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/ugent-library/catbird"
)

// Handler turns the messages one rule matched into HTML fragments. It is
// called once per distinct binding of the pattern's variables, with that
// binding's messages oldest first, and never with none. An error drops what
// this call wrote and is logged; the other handlers' fragments still go out.
//
// It is an interface for the reason http.Handler is: a handler with
// dependencies is a struct implementing it, and logging or panic recovery
// wrap every handler as a func(Handler) Handler. A plain function registers
// with Renderer.HandleFunc, or converts with HandlerFunc.
type Handler interface {
	ServeMessages(r *http.Request, m Match, f *Fragment) error
}

// HandlerFunc makes a plain function a Handler, like http.HandlerFunc.
type HandlerFunc func(r *http.Request, m Match, f *Fragment) error

// ServeMessages calls fn.
func (fn HandlerFunc) ServeMessages(r *http.Request, m Match, f *Fragment) error {
	return fn(r, m, f)
}

// Match is what one rule matched in one dispatch: the messages, oldest first,
// and the values the pattern's variables took.
type Match struct {
	Messages []catbird.Message

	vars map[string]string
}

// Var returns the topic segment the pattern's {name} matched, or "" for a
// name the pattern does not declare.
func (m Match) Var(name string) string { return m.vars[name] }

// Fragment collects the HTML a handler writes. It is an io.Writer, so a
// template component renders straight into it. Every fragment should address
// its own element — hx-swap-oob, or whatever the page's library reads —
// because one response carries the fragments of every handler that ran.
type Fragment struct {
	buf bytes.Buffer
}

// Write appends to the fragment.
func (f *Fragment) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}

// Renderer maps topic patterns to handlers, the way an http.ServeMux maps
// paths: on this pattern, this handler. One per process, built at startup;
// register every rule before the first request, because dispatch reads the
// rules without locking.
type Renderer struct {
	rules []rule
}

// rule is one registered pattern and its handler.
type rule struct {
	pattern string
	handler Handler

	all      bool      // the pattern is "#"
	subtree  bool      // the pattern ends in ".#"
	segments []segment // the segments before any "#"
	names    []string  // the variables' names, in pattern order
}

// segment is one dot-separated part of a pattern: a literal a topic segment
// must equal, or a {name} variable that matches any one topic segment.
type segment struct {
	name     string
	variable bool
}

// NewRenderer returns an empty Renderer.
func NewRenderer() *Renderer {
	return &Renderer{}
}

// Handle registers h under pattern. The pattern is the stream grammar plus
// variables: a topic matches that topic exactly, a prefix followed by ".#"
// matches the prefix and every topic under it, "#" matches everything, and a
// segment written {name} matches any one topic segment and hands its value to
// the handler as m.Var(name) — so "record.work.{id}.#" runs once per record.
// Variables match in Go, over messages already read; they never reach the
// database.
//
// Several rules may match one message, and each of them runs, in registration
// order — one rule writing a tray item per notification while another
// rewrites the unread badge once is the ordinary case, and the two may share
// a pattern. Handle panics on a pattern outside the grammar, because a rule
// is startup configuration and a bad one should stop the process, not sit
// silently matching nothing.
func (rd *Renderer) Handle(pattern string, h Handler) {
	if h == nil {
		panic("wire: pattern " + pattern + " registered with no handler")
	}
	rl, reason := parsePattern(pattern)
	if reason != "" {
		panic("wire: invalid pattern " + pattern + ": " + reason)
	}
	rl.handler = h
	rd.rules = append(rd.rules, rl)
}

// HandleFunc registers a plain function under pattern.
func (rd *Renderer) HandleFunc(pattern string, fn func(*http.Request, Match, *Fragment) error) {
	rd.Handle(pattern, HandlerFunc(fn))
}

// parsePattern reads a pattern into a rule, or says what is wrong with it.
func parsePattern(pattern string) (rule, string) {
	rl := rule{pattern: pattern}
	if pattern == "#" {
		rl.all = true
		return rl, ""
	}
	rest := pattern
	if prefix, ok := strings.CutSuffix(pattern, ".#"); ok {
		rl.subtree = true
		rest = prefix
	}
	for part := range strings.SplitSeq(rest, ".") {
		switch {
		case part == "":
			return rule{}, "empty segment"
		case strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}"):
			name := part[1 : len(part)-1]
			if name == "" || strings.ContainsAny(name, "{}") {
				return rule{}, "malformed variable " + part
			}
			if slices.Contains(rl.names, name) {
				return rule{}, "variable {" + name + "} appears twice"
			}
			rl.segments = append(rl.segments, segment{name: name, variable: true})
			rl.names = append(rl.names, name)
		case strings.ContainsAny(part, "#*{}"):
			return rule{}, "segment " + part + " is not a literal or a {name}"
		default:
			rl.segments = append(rl.segments, segment{name: part})
		}
	}
	return rl, ""
}

// match reports whether a topic, already split on ".", falls under the rule,
// and the values its variables took, in pattern order. A subtree pattern
// matches its prefix itself as well as everything under it, exactly as the
// stream read does — the two grammars must agree, or a rule and the token
// naming the same subtree would cover different topics.
func (rl *rule) match(parts []string) ([]string, bool) {
	if rl.all {
		return nil, true
	}
	if rl.subtree {
		if len(parts) < len(rl.segments) {
			return nil, false
		}
	} else if len(parts) != len(rl.segments) {
		return nil, false
	}
	var values []string
	for i, seg := range rl.segments {
		if seg.variable {
			values = append(values, parts[i])
		} else if parts[i] != seg.name {
			return nil, false
		}
	}
	return values, true
}

// dispatch runs one batch through every rule and returns the HTML the
// handlers wrote. A rule's handler runs once per distinct binding of its
// variables, with that binding's messages in batch order — so a batch holding
// fifty edits of one record renders the record once — and a rule that matched
// nothing does not run, so an empty poll rewrites no region. A handler error
// is logged and drops that call's fragments while the rest still return: the
// alternative, failing the whole response, lets one message a handler cannot
// render stop a page's polling for good, while dropping the call costs one
// region until its next message.
func (rd *Renderer) dispatch(r *http.Request, messages []catbird.Message, logger *slog.Logger) []byte {
	topics := make([][]string, len(messages))
	for i, m := range messages {
		topics[i] = strings.Split(m.Topic, ".")
	}
	var out bytes.Buffer
	for _, rl := range rd.rules {
		// Group the rule's matches per binding. The key joins the values with
		// ".", which cannot collide: a value is one topic segment and holds no
		// dot. Bindings keep the order their first message arrived in.
		var keys []string
		messagesByKey := map[string][]catbird.Message{}
		valuesByKey := map[string][]string{}
		for i, m := range messages {
			values, ok := rl.match(topics[i])
			if !ok {
				continue
			}
			key := strings.Join(values, ".")
			if _, seen := messagesByKey[key]; !seen {
				keys = append(keys, key)
				valuesByKey[key] = values
			}
			messagesByKey[key] = append(messagesByKey[key], m)
		}
		for _, key := range keys {
			vars := make(map[string]string, len(rl.names))
			for i, name := range rl.names {
				vars[name] = valuesByKey[key][i]
			}
			f := &Fragment{}
			m := Match{Messages: messagesByKey[key], vars: vars}
			if err := rl.handler.ServeMessages(r, m, f); err != nil {
				logger.Error("wire: handler failed, fragments dropped", "pattern", rl.pattern, "err", err)
				continue
			}
			out.Write(f.buf.Bytes())
		}
	}
	return out.Bytes()
}
