package harness

import (
	"errors"
	"strings"
)

// ErrUndeclaredWrite reports a component write outside its manifest grants, to meta.*, or to a
// claim key outside meta.claim.* — the data-plane 403 where the developer can see it (rules H7, H12).
var ErrUndeclaredWrite = errors.New("harness: the component wrote a key it did not declare")

// ErrUndeclaredRead reports an activation read outside the manifest reads under StrictReads (rule H13).
var ErrUndeclaredRead = errors.New("harness: the component read a key it did not declare")

// ErrHarnessState reports a call made in the wrong lifecycle state (rules H10, H13).
var ErrHarnessState = errors.New("harness: the call does not fit the harness state")

// The invoke fake's verdicts, in the sidecar's own error-code vocabulary (rule H14).
const (
	codeInvokeNotDeclared  = "INVOKE_NOT_DECLARED"
	codeInvokeNoResponders = "INVOKE_NO_RESPONDERS"
	codeInvokeServiceError = "INVOKE_SERVICE_ERROR"
)

// metaPrefix is the platform-reserved key space no component write may reach (rules H7, H12).
const metaPrefix = "meta."

// claimPrefix is the claim sub-namespace of meta.*, the only keys a claim call may name (rule H12).
const claimPrefix = "meta.claim."

// reserved reports whether a key is in the platform-reserved meta.* space (sidecar.md rule A12).
func reserved(key string) bool {
	return key == "meta" || strings.HasPrefix(key, metaPrefix)
}

// writeAuthorized reports whether a key matches a declared writes pattern: a literal, or prefix.>
// matching the prefix plus one or more further tokens (sidecar.md rule A8, reproduced by rule H7).
func writeAuthorized(key string, patterns []string) bool {
	for _, pattern := range patterns {
		if prefix, ok := strings.CutSuffix(pattern, ".>"); ok {
			if strings.HasPrefix(key, prefix+".") {
				return true
			}
			continue
		}
		if key == pattern {
			return true
		}
	}
	return false
}

// matchesAny reports whether a key matches any of the read-grammar patterns.
func matchesAny(key string, patterns []string) bool {
	for _, pattern := range patterns {
		if matchesPattern(key, pattern) {
			return true
		}
	}
	return false
}

// matchesPattern matches a key against the watch grammar: * one token, a trailing > the rest.
func matchesPattern(key, pattern string) bool {
	keyTokens := strings.Split(key, ".")
	patternTokens := strings.Split(pattern, ".")
	for i, token := range patternTokens {
		if token == ">" {
			return i < len(keyTokens)
		}
		if i >= len(keyTokens) {
			return false
		}
		if token != "*" && token != keyTokens[i] {
			return false
		}
	}
	return len(keyTokens) == len(patternTokens)
}
