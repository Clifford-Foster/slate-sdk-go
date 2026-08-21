// Contract: contracts/bb_sdk_go.md — rule H18's seam.
//
// The one construction seam src/bb_sdk_go/harness builds its chain driver through. Rule H18 pins the
// driver to a runner whose retry backoff is not waited, which is a property of the runner rather than
// of the chain, so it cannot be expressed through Bind or the document.
//
// This is not public API: it is the narrow equivalent of the repo's testexport.go convention
// (.claude/rules/conventions.md), used by this SDK's own harness subpackage and by nothing else.

package steps

// HarnessRunner builds a runner whose retry backoff is not waited, for the rule-H18 chain driver.
func HarnessRunner(b *Bound, board Board) *Runner {
	return newRunner(b, board, 0)
}
