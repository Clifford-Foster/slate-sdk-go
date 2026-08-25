package components

import "context"

// Component is one component blackbox: a JSON-serializable dict in, a JSON-serializable dict out (rule T2).
type Component interface {
	Run(ctx context.Context, input map[string]any) (map[string]any, error)
}

// Setuper is the optional component-start half of the protocol; a component without one is a no-op (rule T2).
type Setuper interface {
	Setup(ctx context.Context, config map[string]any) error
}

// Teardowner is the optional component-stop half of the protocol; a component without one is a no-op (rule T2).
type Teardowner interface {
	Teardown(ctx context.Context) error
}

// Components is the call handle: one wired slot per call, dispatched by the runner (rule T2).
type Components interface {
	Call(ctx context.Context, slot string, payload map[string]any) (map[string]any, error)
}

// ComponentUser is the optional call half of the protocol; a component without one is a no-op (rule T2).
type ComponentUser interface {
	UseComponents(c Components)
}

// Provider is a component package's New: the runner calls it once per alias (rule T3).
type Provider func() Component

// The rule-3 failure taxonomy: every ComponentError carries exactly one of these four causes.
const (
	CauseValidation = "validation"
	CauseTimeout    = "timeout"
	CauseRemote     = "remote"
	CauseInternal   = "internal"
)

// The two communications a chain entry can resolve to, and their permanent parse aliases
// (components_runtime.md rule 10).
const (
	communicationMemory      = "memory"
	communicationNATS        = "nats"
	communicationAliasMemory = "embedded"
	communicationAliasNATS   = "service"
)

// foldCommunication folds a permanent parse alias to its canonical name, leaving anything else alone: the
// membership test of rule T7 runs over folded operands, and an unknown value stays unknown so
// ErrCommunicationUnsupported keeps its condition.
func foldCommunication(value string) string {
	switch value {
	case communicationAliasMemory:
		return communicationMemory
	case communicationAliasNATS:
		return communicationNATS
	default:
		return value
	}
}

// The reserved instance-config prefix rule 10 namespaces an alias's fields under, and its PERMANENT
// read-side alias — the spelling a unit deployed before the unification carries. Only the first is
// ever written.
const (
	componentConfigPrefix = "component_"
	legacyConfigPrefix    = "step_"
)

// The status-record states of components_runtime.md's status-key convention.
const (
	stateRunning = "running"
	stateOK      = "ok"
	stateError   = "error"
)

// gatherInputKey is rule 14's gather member for a parallel stage's input. No alias can spell it: the
// alias grammar admits no leading underscore, which is why no alias needs reserving.
const gatherInputKey = "_input"
