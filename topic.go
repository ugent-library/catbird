package catbird

import "strings"

// matchTopic reports whether topic matches the given pattern.
// Patterns use the same syntax as Bind:
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
