package steps

import (
	"errors"
	"fmt"
)

// ErrChainDocumentInvalid reports a chain document that does not decode or carries an unknown version (rule T5).
var ErrChainDocumentInvalid = errors.New("bbsdk/steps: the chain document is invalid")

// ErrChainInputMissing reports the chain's input_key absent at traversal start — CHAIN_INPUT_MISSING (rule T5).
var ErrChainInputMissing = errors.New("bbsdk/steps: the chain input key is not set")

// ErrBindingUnsupported reports a resolved binding outside the entry's legal set — STEP_BINDING_UNSUPPORTED (rule T7).
var ErrBindingUnsupported = errors.New("bbsdk/steps: the resolved binding is not legal for this step")

// ErrSeamSchemaInvalid reports a declared seam schema that will not compile (rule T12).
var ErrSeamSchemaInvalid = errors.New("bbsdk/steps: the declared seam schema will not compile")

// StepError is the one runtime failure value: the rule-3 taxonomy plus the original error (rule T4).
type StepError struct {
	// Message names the step and what went wrong; it is human-readable and not a compatibility surface.
	Message string
	// Cause is one of CauseValidation, CauseTimeout, CauseRemote, CauseInternal.
	Cause string
	// Retryable reports whether the runner may spend a retry on this failure (rule 8.4).
	Retryable bool
	// Err is the original failure, reachable through Unwrap so errors.Is and errors.As find it.
	Err error
}

// Error renders the message and the cause the runner classified the failure under.
func (e *StepError) Error() string {
	return fmt.Sprintf("bbsdk/steps: %s (%s)", e.Message, e.Cause)
}

// Unwrap reports the original failure, nil when the runner raised the verdict itself.
func (e *StepError) Unwrap() error {
	return e.Err
}

// stepError builds one runner verdict; every rule-3 normalization funnels through it.
func stepError(cause string, retryable bool, err error, format string, args ...any) *StepError {
	return &StepError{Message: fmt.Sprintf(format, args...), Cause: cause, Retryable: retryable, Err: err}
}

// normalize applies rule 3: a step's own StepError passes through, anything else becomes an internal,
// non-retryable one wrapping the original — so a caller and the status key never see a foreign error.
func normalize(alias string, err error) *StepError {
	var verdict *StepError
	if errors.As(err, &verdict) {
		return verdict
	}
	return stepError(CauseInternal, false, err, "step %q failed: %s", alias, err)
}

// recovered is rule T4's panic clause: a panic escaping Run on the goroutine the runner called it on
// is recovered at the branch boundary and normalized like any other foreign failure. It reaches no
// further — a panic on a goroutine the step itself started is the step's, and ends the process.
func recovered(alias string, value any) *StepError {
	err, isError := value.(error)
	if !isError {
		err = fmt.Errorf("%v", value)
	}
	return stepError(CauseInternal, false, err, "step %q panicked: %v", alias, value)
}
