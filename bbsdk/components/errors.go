package components

import (
	"errors"
	"fmt"
)

// ErrChainDocumentInvalid reports a chain document that does not decode or carries an unknown version (rule T5).
var ErrChainDocumentInvalid = errors.New("bbsdk/components: the chain document is invalid")

// ErrChainInputMissing reports the chain's input_key absent at traversal start — CHAIN_INPUT_MISSING (rule T5).
var ErrChainInputMissing = errors.New("bbsdk/components: the chain input key is not set")

// ErrCommunicationUnsupported reports a resolved communication outside the entry's legal set — COMPONENT_COMMUNICATION_UNSUPPORTED (rule T7).
var ErrCommunicationUnsupported = errors.New("bbsdk/components: the resolved communication is not legal for this component")

// ErrChainCommunicationRetired reports an entry that resolves to nats — CHAIN_COMMUNICATION_RETIRED
// (components_runtime.md rule 18, rule T7). A chain never binds nats: the target is a hardwired peer
// declaring serves, invoked from inside the calling component's own Run under its own invokes.
var ErrChainCommunicationRetired = errors.New(
	"bbsdk/components: a chain entry never binds 'nats' — declare the target as a hardwired peer with " +
		"'serves' and invoke it from inside the calling component's own Run(), naming it in that " +
		"component's bb.toml 'invokes'")

// ErrSeamSchemaInvalid reports a declared seam schema that will not compile (rule T12).
var ErrSeamSchemaInvalid = errors.New("bbsdk/components: the declared seam schema will not compile")

// ErrComponentUnknown reports a Call on a slot the entry's wiring does not resolve — COMPONENT_UNKNOWN (rules T2, T7).
var ErrComponentUnknown = errors.New("bbsdk/components: the entry's wiring does not resolve this slot")

// ComponentError is the one runtime failure value: the rule-3 taxonomy plus the original error (rule T4).
type ComponentError struct {
	// Message names the component and what went wrong; it is human-readable and not a compatibility surface.
	Message string
	// Cause is one of CauseValidation, CauseTimeout, CauseRemote, CauseInternal.
	Cause string
	// Retryable reports whether the runner may spend a retry on this failure (rule 8.4).
	Retryable bool
	// Err is the original failure, reachable through Unwrap so errors.Is and errors.As find it.
	Err error
}

// Error renders the message and the cause the runner classified the failure under.
func (e *ComponentError) Error() string {
	return fmt.Sprintf("bbsdk/components: %s (%s)", e.Message, e.Cause)
}

// Unwrap reports the original failure, nil when the runner raised the verdict itself.
func (e *ComponentError) Unwrap() error {
	return e.Err
}

// stepError builds one runner verdict; every rule-3 normalization funnels through it.
func stepError(cause string, retryable bool, err error, format string, args ...any) *ComponentError {
	return &ComponentError{Message: fmt.Sprintf(format, args...), Cause: cause, Retryable: retryable, Err: err}
}

// normalize applies rule 3: a component's own ComponentError passes through, anything else becomes an internal,
// non-retryable one wrapping the original — so a caller and the status key never see a foreign error.
func normalize(alias string, err error) *ComponentError {
	var verdict *ComponentError
	if errors.As(err, &verdict) {
		return verdict
	}
	return stepError(CauseInternal, false, err, "component %q failed: %s", alias, err)
}

// recovered is rule T4's panic clause: a panic escaping Run on the goroutine the runner called it on
// is recovered at the branch boundary and normalized like any other foreign failure. It reaches no
// further — a panic on a goroutine the component itself started is the component's, and ends the process.
func recovered(alias string, value any) *ComponentError {
	err, isError := value.(error)
	if !isError {
		err = fmt.Errorf("%v", value)
	}
	return stepError(CauseInternal, false, err, "component %q panicked: %v", alias, value)
}
