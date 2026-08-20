// Contract: contracts/sidecar.md — Part A rules A28 to A30, outbound service invocation.

package bbsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// The invoke budget of rule S11. The 30 s default is the sidecar's own BB_INVOKE_TIMEOUT_S default
// and cap, duplicated here by necessity because the SDK reads none of the sidecar's environment
// (rule C4) — a change to that default is a cross-cutting amendment that must sweep this constant.
// The grace is load-bearing: without it an overdue target surfaces as a client-side deadline
// instead of the contract's 504 INVOKE_TIMEOUT, and the component loses the ability to tell a slow
// target from a broken one.
const (
	defaultInvokeTimeout = 30 * time.Second
	invokeGrace          = 5 * time.Second
)

// invokeOptions carries one call's invoke budget.
type invokeOptions struct {
	timeout    time.Duration
	timeoutSet bool
}

// InvokeOption configures a single Invoke call (rule S11).
type InvokeOption func(*invokeOptions)

// WithInvokeTimeout sets the invoke's timeout_s and, with the grace, the call's own budget (rule S11).
func WithInvokeTimeout(timeout time.Duration) InvokeOption {
	return func(o *invokeOptions) {
		o.timeout = timeout
		o.timeoutSet = true
	}
}

// Invoke performs one outbound service call through the sidecar and relays the reply (rules S11, S12).
func (c *Client) Invoke(
	ctx context.Context,
	service, endpoint string,
	payload map[string]any,
	opts ...InvokeOption,
) (map[string]any, error) {
	settings := invokeOptions{timeout: defaultInvokeTimeout}
	for _, opt := range opts {
		if opt != nil {
			opt(&settings)
		}
	}
	body := map[string]any{"service": service, "endpoint": endpoint, "payload": payload}
	if settings.timeoutSet {
		body["timeout_s"] = settings.timeout.Seconds()
	}
	encoded, err := encodeBody(body)
	if err != nil {
		return nil, err
	}
	status, response, err := c.transport.call(
		ctx, http.MethodPost, c.transport.baseURL+"/v1/invoke", encoded, settings.timeout+invokeGrace)
	if err != nil {
		return nil, fmt.Errorf("bbsdk: invoke %s.%s: %w", service, endpoint, err)
	}
	if !succeeded(status) {
		// Every failure on this route is the one class; the code is what a component branches on,
		// and no-responders and timeout stay distinct there (rule E5).
		return nil, parseError(status, response, true)
	}
	if len(bytes.TrimSpace(response)) == 0 {
		// The sidecar relays an empty reply as empty rather than synthesizing an object.
		return map[string]any{}, nil
	}
	var document any
	if err := json.Unmarshal(response, &document); err != nil {
		return nil, localInvokeError(fmt.Sprintf("the target's reply is not a JSON object: %s", err))
	}
	reply, isObject := document.(map[string]any)
	if !isObject {
		// A reply that is not an object is a target fault, the same class the sidecar reports for
		// an oversize reply — never a fabricated transport code (rule S12).
		return nil, localInvokeError("the target's reply is not a JSON object")
	}
	return reply, nil
}
