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

// ErrSchemaViolation reports a write the boundary refused against the component's own declared write
// schema, so drift between a component's code and its declared shape is caught at the component that
// drifted (rules E2, E8).
var ErrSchemaViolation = fmt.Errorf("%w: schema violation", ErrSidecar)

// ErrBlackboardUnavailable reports the sidecar's upstream blackboard being unreachable.
var ErrBlackboardUnavailable = fmt.Errorf("%w: blackboard unavailable", ErrSidecar)

// ErrInvoke reports every outbound invoke failure, wire-reported or locally raised (rule E5).
var ErrInvoke = fmt.Errorf("%w: invoke failed", ErrSidecar)

// ErrActivationFailed reports a write, or a second Fail, after Fail ended the activation (rule K9).
// It is a local lifecycle verdict, not a boundary answer, so it stands outside the ErrSidecar tree.
var ErrActivationFailed = errors.New("bbsdk: the activation already failed")

// The codes the SDK reads or raises rather than merely relays, and the code a body it cannot parse
// yields. SCHEMA_VIOLATION is raised locally by the harness's board-side gate (rule H20).
const (
	codeKeyNotFound        = "KEY_NOT_FOUND"
	codeInvokeServiceError = "INVOKE_SERVICE_ERROR"
	codeSchemaViolation    = "SCHEMA_VIOLATION"
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
	codeSchemaViolation:      ErrSchemaViolation,
	"BLACKBOARD_UNAVAILABLE": ErrBlackboardUnavailable,
}

// ValidationVerdict is one sidecar.md Validation Verdict: what judged a value, the declared pattern
// it was attached to, the concrete key, and the members the value did not satisfy (rule E8).
type ValidationVerdict struct {
	// Source is what judged the value: write_schema, read_expectation, or a value grown later.
	Source string `json:"source"`
	// Pattern is the declared write_schemas/read_expectations key the schema is attached to.
	Pattern string `json:"pattern"`
	// Key is the concrete key whose value was judged.
	Key string `json:"key"`
	// Unsatisfied names the unsatisfied members, in the validator's deterministic order.
	Unsatisfied []string `json:"unsatisfied"`
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
	// Validation carries the envelope's verdicts, empty except on SCHEMA_VIOLATION (rule E8).
	Validation []ValidationVerdict
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
		// Validation is read raw so a malformed member costs the verdicts and nothing else (rule E8).
		Validation json.RawMessage `json:"validation"`
	} `json:"error"`
}

// parseError reads a non-2xx body as the error envelope; an absent, unparseable or differently
// shaped body degrades to UNKNOWN with an empty message rather than a swallowed failure (rule E1).
func parseError(status int, body []byte, invoke bool) *SidecarError {
	failure := &SidecarError{Code: codeUnknown, Status: status, invoke: invoke, Validation: []ValidationVerdict{}}
	var envelope errorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Error.Code == "" {
		return failure
	}
	failure.Code = envelope.Error.Code
	failure.Message = envelope.Error.Message
	failure.Key = envelope.Error.Key
	failure.Rule = envelope.Error.Rule
	failure.Validation = parseVerdicts(envelope.Error.Validation)
	return failure
}

// parseVerdicts reads error.validation defensively: a missing or malformed member — the list itself,
// one entry of it, or one member of an entry — yields an empty slice or a zero value, never an error
// of its own, so a component still sees the failure the envelope reports (rule E8).
func parseVerdicts(raw json.RawMessage) []ValidationVerdict {
	verdicts := []ValidationVerdict{}
	if len(raw) == 0 {
		return verdicts
	}
	var entries []map[string]any
	if err := json.Unmarshal(raw, &entries); err != nil {
		return verdicts
	}
	for _, entry := range entries {
		verdict := ValidationVerdict{
			Source:      text(entry["source"]),
			Pattern:     text(entry["pattern"]),
			Key:         text(entry["key"]),
			Unsatisfied: texts(entry["unsatisfied"]),
		}
		verdicts = append(verdicts, verdict)
	}
	return verdicts
}

// text reads one string member, yielding empty for a member of any other type.
func text(value any) string {
	rendered, isText := value.(string)
	if !isText {
		return ""
	}
	return rendered
}

// texts reads one string list member, skipping entries that are not strings.
func texts(value any) []string {
	items, isList := value.([]any)
	if !isList {
		return []string{}
	}
	rendered := make([]string, 0, len(items))
	for _, item := range items {
		if member, isText := item.(string); isText {
			rendered = append(rendered, member)
		}
	}
	return rendered
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
