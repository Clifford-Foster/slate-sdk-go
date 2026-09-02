// Contract: contracts/bb_sdk_go.md — rule H20's seam.
//
// The two calls src/bb_sdk_go/harness prepares and applies a manifest's declared shapes through. The
// harness's board-side gates are the sidecar's gates reproduced in process, so they must judge a
// value with the very same compiled document and the very same verdict vocabulary Decode does —
// which is why this subpackage owns the construction and the harness borrows it rather than
// standing up a second, drifting validator.
//
// This is not component-facing API: it is the narrow equivalent of the repo's testexport.go
// convention (.claude/rules/conventions.md), used by this SDK's own harness subpackage and by
// nothing else.

package schema

// HarnessSchema is one prepared schema document the harness's board-side gates judge against (rule H20).
type HarnessSchema struct {
	compiled *prepared
}

// HarnessPrepare compiles one declared schema document under the no-fetch construction (rule H20).
func HarnessPrepare(document any) (*HarnessSchema, error) {
	compiled, err := prepare(document)
	if err != nil {
		return nil, err
	}
	return &HarnessSchema{compiled: compiled}, nil
}

// HarnessUnsatisfied names the members a value fails, empty when it conforms (rule H20).
func (s *HarnessSchema) HarnessUnsatisfied(value any) ([]string, error) {
	return s.compiled.unsatisfied(value)
}
