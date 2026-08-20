package steps

import "context"

// Step is one step blackbox: a JSON-serializable dict in, a JSON-serializable dict out (rule T2).
type Step interface {
	Run(ctx context.Context, input map[string]any) (map[string]any, error)
}

// Setuper is the optional component-start half of the protocol; a step without one is a no-op (rule T2).
type Setuper interface {
	Setup(ctx context.Context, config map[string]any) error
}

// Teardowner is the optional component-stop half of the protocol; a step without one is a no-op (rule T2).
type Teardowner interface {
	Teardown(ctx context.Context) error
}

// Provider is a step package's New: the runner calls it once per alias (rule T3).
type Provider func() Step

// The rule-3 failure taxonomy: every StepError carries exactly one of these four causes.
const (
	CauseValidation = "validation"
	CauseTimeout    = "timeout"
	CauseRemote     = "remote"
	CauseInternal   = "internal"
)

// The two bindings a chain entry can resolve to, and their permanent parse aliases
// (steps_runtime.md rule 10).
const (
	bindingMemory      = "memory"
	bindingNATS        = "nats"
	bindingAliasMemory = "embedded"
	bindingAliasNATS   = "service"
)

// foldBinding folds a permanent parse alias to its canonical name, leaving anything else alone: the
// membership test of rule T7 runs over folded operands, and an unknown value stays unknown so
// ErrBindingUnsupported keeps its condition.
func foldBinding(value string) string {
	switch value {
	case bindingAliasMemory:
		return bindingMemory
	case bindingAliasNATS:
		return bindingNATS
	default:
		return value
	}
}

// The status-record states of steps_runtime.md's status-key convention.
const (
	stateRunning = "running"
	stateOK      = "ok"
	stateError   = "error"
)

// gatherInputKey is rule 14's gather member for a parallel stage's input. No alias can spell it: the
// alias grammar admits no leading underscore, which is why no alias needs reserving.
const gatherInputKey = "_input"
