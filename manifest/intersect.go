package manifest

import "strings"

// PatternsIntersect reports whether two key patterns could match a common concrete key (rule 23).
func PatternsIntersect(a, b string) bool {
	return intersect(strings.Split(a, "."), strings.Split(b, "."))
}

// intersect compares two token lists position by position, conservatively (it never under-approximates).
func intersect(a, b []string) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		// Inside the loop the other side has >= 1 remaining token, so ">" is satisfied.
		if a[i] == ">" || b[j] == ">" {
			return true
		}
		if a[i] != "*" && b[j] != "*" && a[i] != b[j] {
			return false
		}
		i++
		j++
	}
	// Both exhausted at the same length means a common key exists. Any leftover token — including a
	// lone trailing ">", which needs >= 1 more segment — against an exhausted side means disjoint.
	return i == len(a) && j == len(b)
}

// patternCovers reports whether a read pattern matches a concrete key ("*" one token, trailing ">" the rest).
func patternCovers(pattern, key string) bool {
	tokens := strings.Split(pattern, ".")
	segments := strings.Split(key, ".")
	i := 0
	for _, token := range tokens {
		if token == ">" {
			// ">" matches one-or-more remaining tokens, never zero (NATS: "a.>" excludes "a").
			return i < len(segments)
		}
		if i >= len(segments) {
			return false
		}
		if token != "*" && token != segments[i] {
			return false
		}
		i++
	}
	return i == len(segments)
}
