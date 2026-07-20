package wire

import "strings"

// This trie dispatches topics to in-process subscribers: SSE connections,
// Listen handlers and renderers. It is wire's own copy — the engine's
// topic matcher is the stream module's SQL grammar, and the two must be
// free to evolve apart.

// topicTrie matches hierarchical dot-separated topics with wildcard
// support (* = single token, # = zero or more trailing tokens).
type topicTrie[T any] struct {
	exact map[string]*topicTrie[T]
	wild  *topicTrie[T] // * — matches exactly one token
	hash  []T           // # — matches zero or more trailing tokens
	subs  []T           // exact match terminates here
}

func newTopicTrie[T any]() *topicTrie[T] {
	return &topicTrie[T]{}
}

// add inserts a subscriber under the given pattern.
func (t *topicTrie[T]) add(pattern string, sub T) {
	node := t
	for _, token := range strings.Split(pattern, ".") {
		if token == "#" {
			node.hash = append(node.hash, sub)
			return
		}
		if token == "*" {
			if node.wild == nil {
				node.wild = &topicTrie[T]{}
			}
			node = node.wild
			continue
		}
		if node.exact == nil {
			node.exact = make(map[string]*topicTrie[T])
		}
		child, ok := node.exact[token]
		if !ok {
			child = &topicTrie[T]{}
			node.exact[token] = child
		}
		node = child
	}
	node.subs = append(node.subs, sub)
}

// remove removes a subscriber from the given pattern using the eq function for identity.
func (t *topicTrie[T]) remove(pattern string, eq func(T) bool) {
	node := t
	for _, token := range strings.Split(pattern, ".") {
		if token == "#" {
			node.hash = removeFunc(node.hash, eq)
			return
		}
		if token == "*" {
			if node.wild == nil {
				return
			}
			node = node.wild
			continue
		}
		child, ok := node.exact[token]
		if !ok {
			return
		}
		node = child
	}
	node.subs = removeFunc(node.subs, eq)
}

// match collects all subscribers matching the given concrete topic.
func (t *topicTrie[T]) match(topic string, out []T) []T {
	tokens := strings.Split(topic, ".")
	return t.matchTokens(tokens, out)
}

func (t *topicTrie[T]) matchTokens(tokens []string, out []T) []T {
	// # at this level matches zero or more remaining tokens
	out = append(out, t.hash...)

	if len(tokens) == 0 {
		// exact match: all tokens consumed
		return append(out, t.subs...)
	}

	token := tokens[0]
	rest := tokens[1:]

	// exact child
	if child, ok := t.exact[token]; ok {
		out = child.matchTokens(rest, out)
	}

	// * matches this single token
	if t.wild != nil {
		out = t.wild.matchTokens(rest, out)
	}

	return out
}

func removeFunc[T any](s []T, eq func(T) bool) []T {
	for i, v := range s {
		if eq(v) {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

// matchTopic reports whether topic matches the given pattern.
//   - "." separates tokens
//   - "*" matches exactly one token
//   - "#" matches zero or more tokens (must be the final token)
//
// Both pattern and topic must be non-empty.
func matchTopic(pattern, topic string) bool {
	if pattern == "#" {
		return true
	}

	patternTokens := strings.Split(pattern, ".")
	topicTokens := strings.Split(topic, ".")

	for i, pt := range patternTokens {
		if pt == "#" {
			// # must be last token (validated at registration)
			return true
		}
		if i >= len(topicTokens) {
			return false
		}
		if pt != "*" && pt != topicTokens[i] {
			return false
		}
	}

	return len(patternTokens) == len(topicTokens)
}
