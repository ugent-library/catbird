package catbird

import "testing"

func TestMatchTopic(t *testing.T) {
	tests := []struct {
		pattern string
		topic   string
		want    bool
	}{
		// Exact match
		{"events.user.created", "events.user.created", true},
		{"events.user.created", "events.user.updated", false},
		{"events.user.created", "events.user", false},
		{"events.user", "events.user.created", false},
		{"a", "a", true},
		{"a", "b", false},

		// Single-token wildcard (*)
		{"events.*.created", "events.user.created", true},
		{"events.*.created", "events.order.created", true},
		{"events.*.created", "events.user.updated", false},
		{"events.*.created", "events.user.order.created", false},
		{"*", "anything", true},
		{"*.*", "a.b", true},
		{"*.*", "a", false},

		// Multi-token wildcard (#)
		{"events.#", "events", true},
		{"events.#", "events.user", true},
		{"events.#", "events.user.created", true},
		{"events.#", "other.user", false},
		{"#", "anything", true},
		{"#", "a.b.c.d", true},

		// Combined
		{"events.*.#", "events.user", true},
		{"events.*.#", "events.user.created", true},
		{"events.*.#", "events.user.created.v2", true},
		{"events.*.#", "events", false},
		{"catbird.event.*.failed", "catbird.event.task.failed", true},
		{"catbird.event.*.failed", "catbird.event.flow.failed", true},
		{"catbird.event.*.failed", "catbird.event.task.completed", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.topic, func(t *testing.T) {
			got := matchTopic(tt.pattern, tt.topic)
			if got != tt.want {
				t.Errorf("matchTopic(%q, %q) = %v, want %v", tt.pattern, tt.topic, got, tt.want)
			}
		})
	}
}
