package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
)

// ErrGatewayAuth reports a 401/403 from the control plane; a credential does not heal, so no call
// and no watch retries one (rules W2, W3).
var ErrGatewayAuth = errors.New("bbsdk/gateway: the control plane refused the credential")

// ErrRateLimited reports a 429 RATE_LIMITED; the wrapped *bbsdk.SidecarError carries RetryAfter.
var ErrRateLimited = errors.New("bbsdk/gateway: rate limited")

// The control plane's spellings for two codes rule E2's table names under the sidecar's (rule W2): a
// blocked reserved-key write is the write-not-authorized class, and the facade's single value
// verdict is the value-invalid class. Consulted only where rule E2's own grouping claims nothing.
var gatewayClasses = map[string]error{
	"META_WRITE_FORBIDDEN": bbsdk.ErrWriteNotAuthorized,
	"VALUE_INVALID":        bbsdk.ErrValueInvalid,
}

// The codes a WebSocket close carries no envelope for: the control plane's own spelling for each of
// the three closes that do not heal (rule W3).
const (
	codeAuthRequired      = "AUTH_REQUIRED"
	codeForbidden         = "FORBIDDEN"
	codeWorkspaceNotFound = "WORKSPACE_NOT_FOUND"
	codeKeyNotFound       = "KEY_NOT_FOUND"
	codeUnknown           = "UNKNOWN"
)

// classedError carries one boundary failure under a sentinel this package adds, so a caller reaches
// the parsed envelope with errors.As and the class with errors.Is (rule W2).
type classedError struct {
	failure *bbsdk.SidecarError
	class   error
}

// Error renders the boundary's own message; the API key appears in none of it (rule W1).
func (e *classedError) Error() string { return e.failure.Error() }

// Unwrap reports the parsed envelope — itself wrapping ErrSidecar — and this package's class.
func (e *classedError) Unwrap() []error { return []error{e.failure, e.class} }

// errorEnvelope is the shared control-plane error body (control_plane.md §Schemas).
type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Key     string `json:"key"`
		Rule    string `json:"rule"`
	} `json:"error"`
}

// parseEnvelope reads a non-2xx body as the error envelope; an absent, unparseable or differently
// shaped body degrades to UNKNOWN rather than a swallowed failure (rule E1's posture).
func parseEnvelope(status int, body []byte) *bbsdk.SidecarError {
	failure := &bbsdk.SidecarError{Code: codeUnknown, Status: status, Validation: []bbsdk.ValidationVerdict{}}
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

// classify types one non-2xx response: rule E2's shared grouping where it claims the code, then this
// package's two additions, then ErrGatewayAuth for 401/403 and ErrRateLimited for 429 (rule W2).
func classify(status int, body []byte, header http.Header) error {
	failure := parseEnvelope(status, body)
	if len(failure.Unwrap()) > 1 {
		return failure // rule E2's table claimed the code; the shared grouping wins
	}
	if class, claimed := gatewayClasses[failure.Code]; claimed {
		return &classedError{failure: failure, class: class}
	}
	if status == http.StatusTooManyRequests {
		failure.RetryAfter = retryAfter(header)
		return &classedError{failure: failure, class: ErrRateLimited}
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return &classedError{failure: failure, class: ErrGatewayAuth}
	}
	return failure // an unrecognized code reaches the caller verbatim on the base class (rule E4)
}

// retryAfter reads Retry-After in whole seconds (control_plane.md rule H3); a header that is absent
// or is not a number yields zero, so a caller always has a duration to back off by.
func retryAfter(header http.Header) time.Duration {
	raw := header.Get("Retry-After")
	if raw == "" {
		return 0
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

// closeFailure is the typed error for a WebSocket close that does not heal. A close carries no HTTP
// status, so the error carries none either rather than inventing one (rule W3).
func closeFailure(code string, message string) error {
	failure := &bbsdk.SidecarError{Code: code, Message: message, Validation: []bbsdk.ValidationVerdict{}}
	if code == codeWorkspaceNotFound {
		return failure
	}
	return &classedError{failure: failure, class: ErrGatewayAuth}
}

// report hands an error to a caller-supplied hook, which the zero WatchOptions leaves unset.
func report(hook func(error), err error) {
	if hook == nil {
		return
	}
	hook(err)
}
