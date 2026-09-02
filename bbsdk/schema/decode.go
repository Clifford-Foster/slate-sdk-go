package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

// DecodeError reports a value that does not satisfy the derived schema (rule D3).
type DecodeError struct {
	// Unsatisfied names the failed members in the sidecar.md Validation Verdict vocabulary.
	Unsatisfied []string
}

// Error names every unsatisfied member, in the order the verdict records them.
func (e *DecodeError) Error() string {
	return "bbsdk/schema: the value does not satisfy the type's schema: " + strings.Join(e.Unsatisfied, "; ")
}

// Decode validates a value against Derive[T]() and unmarshals the typed value (rule D3).
func Decode[T any](value map[string]any) (T, error) {
	var decoded T
	document, err := Derive[T]()
	if err != nil {
		return decoded, err
	}
	compiled, err := prepare(document)
	if err != nil {
		return decoded, err
	}
	unsatisfied, err := compiled.unsatisfied(value)
	if err != nil {
		return decoded, err
	}
	if len(unsatisfied) > 0 {
		return decoded, &DecodeError{Unsatisfied: unsatisfied}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return decoded, fmt.Errorf("bbsdk/schema: the value is not JSON-encodable: %w", err)
	}
	// Extra members are ignored rather than refused: a producer may always carry more than the
	// consumer's type names, which is what encoding/json does by default (rule D3).
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return decoded, fmt.Errorf("bbsdk/schema: the value does not unmarshal into the type: %w", err)
	}
	return decoded, nil
}
