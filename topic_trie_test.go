package catbird

import (
	"fmt"
	"strings"
	"testing"
)

func TestTopicTrieExactMatch(t *testing.T) {
	trie := newTopicTrie[string]()
	trie.add("user.123", "sub1")
	trie.add("user.456", "sub2")

	got := trie.match("user.123", nil)
	if len(got) != 1 || got[0] != "sub1" {
		t.Fatalf("expected [sub1], got %v", got)
	}
}

func TestTopicTrieHashWildcard(t *testing.T) {
	trie := newTopicTrie[string]()
	trie.add("user.123.#", "sub1")

	got := trie.match("user.123.batch_edit.done", nil)
	if len(got) != 1 || got[0] != "sub1" {
		t.Fatalf("expected [sub1], got %v", got)
	}

	// # matches zero tokens too
	got = trie.match("user.123", nil)
	if len(got) != 1 || got[0] != "sub1" {
		t.Fatalf("expected [sub1] for zero-tail match, got %v", got)
	}
}

func TestTopicTrieStarWildcard(t *testing.T) {
	trie := newTopicTrie[string]()
	trie.add("user.*.alert", "sub1")

	got := trie.match("user.123.alert", nil)
	if len(got) != 1 || got[0] != "sub1" {
		t.Fatalf("expected [sub1], got %v", got)
	}

	// * does not match multiple tokens
	got = trie.match("user.123.456.alert", nil)
	if len(got) != 0 {
		t.Fatalf("expected no matches, got %v", got)
	}
}

func TestTopicTrieMultipleMatches(t *testing.T) {
	trie := newTopicTrie[string]()
	trie.add("user.123.#", "sub1")
	trie.add("user.*.notification", "sub2")
	trie.add("user.123.notification", "sub3")

	got := trie.match("user.123.notification", nil)
	if len(got) != 3 {
		t.Fatalf("expected 3 matches, got %v", got)
	}
}

func TestTopicTrieRemove(t *testing.T) {
	trie := newTopicTrie[string]()
	trie.add("user.123.#", "sub1")
	trie.add("user.123.#", "sub2")

	trie.remove("user.123.#", func(s string) bool { return s == "sub1" })

	got := trie.match("user.123.anything", nil)
	if len(got) != 1 || got[0] != "sub2" {
		t.Fatalf("expected [sub2], got %v", got)
	}
}

func TestTopicTrieNoMatch(t *testing.T) {
	trie := newTopicTrie[string]()
	trie.add("user.123", "sub1")

	got := trie.match("user.456", nil)
	if len(got) != 0 {
		t.Fatalf("expected no matches, got %v", got)
	}
}

func TestTopicTrieRootHash(t *testing.T) {
	trie := newTopicTrie[string]()
	trie.add("#", "firehose")

	got := trie.match("anything.at.all", nil)
	if len(got) != 1 || got[0] != "firehose" {
		t.Fatalf("expected [firehose], got %v", got)
	}
}

// --- Benchmarks ---

// BenchmarkMapLookup simulates the current exact-match map approach.
func BenchmarkMapLookup(b *testing.B) {
	for _, numSubs := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			topics := make(map[string][]string)
			for i := range numSubs {
				pattern := fmt.Sprintf("user.%d", i)
				topics[pattern] = append(topics[pattern], fmt.Sprintf("sub%d", i))
			}

			// Publish to a topic that exists — best case for map
			target := fmt.Sprintf("user.%d", numSubs/2)
			b.ResetTimer()
			for range b.N {
				_ = topics[target]
			}
		})
	}
}

// BenchmarkMapLookupWildcardSim simulates wildcard matching with the current
// map approach — requires iterating all patterns.
func BenchmarkMapLookupWildcardSim(b *testing.B) {
	for _, numSubs := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			patterns := make([]string, numSubs)
			for i := range numSubs {
				patterns[i] = fmt.Sprintf("user.%d.#", i)
			}

			target := fmt.Sprintf("user.%d.batch_edit.done", numSubs/2)
			b.ResetTimer()
			for range b.N {
				var matched []string
				for _, p := range patterns {
					if matchTopic(p, target) {
						matched = append(matched, p)
					}
				}
			}
		})
	}
}

// BenchmarkTrieMatch benchmarks the trie approach with wildcard patterns.
func BenchmarkTrieMatch(b *testing.B) {
	for _, numSubs := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			trie := newTopicTrie[string]()
			for i := range numSubs {
				trie.add(fmt.Sprintf("user.%d.#", i), fmt.Sprintf("sub%d", i))
			}

			target := fmt.Sprintf("user.%d.batch_edit.done", numSubs/2)
			b.ResetTimer()
			for range b.N {
				trie.match(target, nil)
			}
		})
	}
}

// BenchmarkTrieMatchMixed benchmarks the trie with a mix of exact and wildcard patterns.
func BenchmarkTrieMatchMixed(b *testing.B) {
	for _, numSubs := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("subs=%d", numSubs), func(b *testing.B) {
			trie := newTopicTrie[string]()
			for i := range numSubs {
				if i%10 == 0 {
					// 10% wildcard subscribers
					trie.add(fmt.Sprintf("user.%d.#", i), fmt.Sprintf("wild%d", i))
				} else {
					// 90% exact subscribers
					trie.add(fmt.Sprintf("user.%d.notification", i), fmt.Sprintf("exact%d", i))
				}
			}

			target := fmt.Sprintf("user.%d.notification", numSubs/2)
			b.ResetTimer()
			for range b.N {
				trie.match(target, nil)
			}
		})
	}
}

// BenchmarkTrieMatchDeepTopic benchmarks with deeper topic hierarchies.
func BenchmarkTrieMatchDeepTopic(b *testing.B) {
	trie := newTopicTrie[string]()
	for i := range 1000 {
		trie.add(fmt.Sprintf("org.acme.user.%d.#", i), fmt.Sprintf("sub%d", i))
	}

	target := "org.acme.user.500.batch_edit.list.42.done"
	b.ResetTimer()
	for range b.N {
		trie.match(target, nil)
	}
}

// BenchmarkTopicSplit measures the overhead of strings.Split on topics.
func BenchmarkTopicSplit(b *testing.B) {
	topic := "user.01JXYZ.batch_edit.done"
	b.ResetTimer()
	for range b.N {
		strings.Split(topic, ".")
	}
}
