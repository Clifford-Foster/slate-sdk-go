package bbsdk

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrSidecar is the base of every data-plane failure; every returned boundary error wraps it (rule E2).
var ErrSidecar = errors.New("bbsdk: sidecar error")

// ErrWriteNotAuthorized reports a write the boundary refused: an undeclared key, a policy denial, or meta.*.
var ErrWriteNotAuthorized = fmt.Errorf("%w: write not authorized", ErrSidecar)

// ErrReadNotAuthorized reports a read outside the component's declared reads.
var ErrReadNotAuthorized = fmt.Errorf("%w: read not authorized", ErrSidecar)

// ErrRevisionConflict reports a compare-and-swap write or delete that lost the revision race.
var ErrRevisionConflict = fmt.Errorf("%w: revision conflict", ErrSidecar)

// ErrValueInvalid reports a malformed request: a bad value, key, body, watch pattern, claim key, or size.
var ErrValueInvalid = fmt.Errorf("%w: invalid value", ErrSidecar)

// ErrBlackboardUnavailable reports the sidecar's upstream blackboard being unreachable.
var ErrBlackboardUnavailable = fmt.Errorf("%w: blackboard unavailable", ErrSidecar)

// ErrInvoke reports every outbound invoke failure, wire-reported or locally raised (rule E5).
var ErrInvoke = fmt.Errorf("%w: invoke failed", ErrSidecar)

// ErrActivationFailed reports a write, or a second Fail, after Fail ended the activation (rule K9).
// It is a local lifecycle verdict, not a boundary answer, so it stands outside the ErrSidecar tree.
var ErrActivationFailed = errors.New("bbsdk: the activation already failed")

// The two codes the SDK reads rather than merely relays, and the code a body it cannot parse yields.
const (
	codeKeyNotFound        = "KEY_NOT_FOUND"
	codeInvokeServiceError = "INVOKE_SERVICE_ERROR"
	codeUnknown            = "UNKNOWN"
)

// errorClasses is the normative code-to-sentinel mapping of rule E2. The codes are the sidecar's
// compatibility surface; the grouping is this SDK's, pinned so the two SDKs cannot drift. A code
// absent from the table carries no class sentinel and reaches the component verbatim (rule E4).
var errorClasses = map[string]error{
	"WRITE_NOT_AUTHORIZED":   ErrWriteNotAuthorized,
	"META_KEY_BLOCKED":       ErrWriteNotAuthorized,
	"READ_NOT_AUTHORIZED":    ErrReadNotAuthorized,
	"REVISION_CONFLICT":      ErrRevisionConflict,
	"VALUE_NOT_DICT":         ErrValueInvalid,
	"VALUE_TOO_LARGE":        ErrValueInvalid,
	"BODY_INVALID":           ErrValueInvalid,
	"KEY_INVALID":            ErrValueInvalid,
	"WATCH_PATTERN_INVALID":  ErrValueInvalid,
	"CLAIM_KEY_INVALID":      ErrValueInvalid,
	"REQUEST_TOO_LARGE":      ErrValueInvalid,
	"BLACKBOARD_UNAVAILABLE": ErrBlackboardUnavailable,
}

// SidecarError is the boundary's error envelope, parsed (rule E1).
type SidecarError struct {
	// Code is the sidecar's stable machine code, preserved verbatim even when unrecognized (rule E4).
	Code string
	// Message is human-readable and free-form; it is not part of the compatibility surface.
	Message string
	// Key is the key the failure is scoped to, empty when it is not key-scoped.
	Key string
	// Rule carries the matched workspace-policy pattern, present only on a policy denial (rule E3).
	Rule string
	// Status is the HTTP status, or 0 for a verdict the SDK raised locally (rule E6).
	Status int
	// invoke marks a failure from the invoke route, whose every code is the one class (rule E5).
	invoke bool
}

// Error renders the machine code, the HTTP status where there is one, and the boundary's message.
func (e *SidecarError) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("bbsdk: %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("bbsdk: %s (HTTP %d): %s", e.Code, e.Status, e.Message)
}

// Unwrap reports the base sentinel and the one class sentinel this code maps to (rule E2).
func (e *SidecarError) Unwrap() []error {
	if e.invoke {
		return []error{ErrSidecar, ErrInvoke}
	}
	if class, ok := errorClasses[e.Code]; ok {
		return []error{ErrSidecar, class}
	}
	return []error{ErrSidecar}
}

// errorEnvelope is the non-2xx body every boundary route carries (sidecar.md Part A, Error Envelope).
type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Key     string `json:"key"`
		Rule    string `json:"rule"`
	} `json:"error"`
}

// parseError reads a non-2xx body as the error envelope; an absent, unparseable or differently
// shaped body degrades to UNKNOWN with an empty message rather than a swallowed failure (rule E1).
func parseError(status int, body []byte, invoke bool) *SidecarError {
	failure := &SidecarError{Code: codeUnknown, Status: status, invoke: invoke}
	var envelope errorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Error.Code == "" {
		return failure
	}
	failure.Code = envelope.Error.Code
	failure.Message = envelope.Error.Message
	failure.Key = envelope.Error.Key
	failure.Rule = envelope.Error.Rule
	return failure
}

// localInvokeError is the target-fault verdict the SDK raises itself; a zero Status is how a
// component tells a local verdict from a relayed one (rules S12, E6).
func localInvokeError(message string) *SidecarError {
	return &SidecarError{Code: codeInvokeServiceError, Message: message, invoke: true}
}

// report hands an error to a caller-supplied hook, which the zero WatchOptions leaves unset (rule S9).
func report(hook func(error), err error) {
	if hook == nil {
		return
	}
	hook(err)
}
