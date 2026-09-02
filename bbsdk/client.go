// Contract: contracts/sidecar.md — Part A, the loopback data plane this client presents.

package bbsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Entry is one key's value and revision, as the data plane reports it.
type Entry struct {
	Key      string
	Value    map[string]any
	Revision uint64
}

// Client is the component's data-plane client; it is safe for concurrent use (rule S15).
type Client struct {
	transport *transport
}

// NewClient builds a client against the loopback data plane (rules C1, S1).
func NewClient(opts ...Option) (*Client, error) {
	// A malformed BB_CONFIG fails at construction rather than mid-activation, so a component with a
	// broken config never reaches a first delivery (rule C3a).
	if _, err := ComponentConfig(); err != nil {
		return nil, err
	}
	resolved, err := newTransport(opts)
	if err != nil {
		return nil, err
	}
	return &Client{transport: resolved}, nil
}

// Get reads one key, returning (nil, nil) when it is absent (rule S3).
func (c *Client) Get(ctx context.Context, key string) (*Entry, error) {
	status, body, err := c.transport.call(ctx, http.MethodGet, c.transport.stateURL(key), nil, c.transport.budget)
	if err != nil {
		return nil, fmt.Errorf("bbsdk: get %q: %w", key, err)
	}
	if status == http.StatusNotFound {
		// The absent-key convention is a 404 carrying KEY_NOT_FOUND, and only that: any other 404,
		// a missing route among them, stays an error.
		failure := parseError(status, body, false)
		if failure.Code == codeKeyNotFound {
			return nil, nil
		}
		return nil, failure
	}
	if !succeeded(status) {
		return nil, parseError(status, body, false)
	}
	var payload struct {
		Key      string         `json:"key"`
		Value    map[string]any `json:"value"`
		Revision uint64         `json:"revision"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("bbsdk: decoding the get response for %q: %w", key, err)
	}
	return &Entry{Key: payload.Key, Value: payload.Value, Revision: payload.Revision}, nil
}

// Put writes a value unconditionally and returns the new revision — or 0 when the sidecar published
// it instead of writing it, under a manifest declaring communication: nats (rule S4).
func (c *Client) Put(ctx context.Context, key string, value map[string]any) (uint64, error) {
	return c.put(ctx, key, map[string]any{"value": value})
}

// PutCAS writes a value only while the key stands at revision, returning the new revision (rule S4).
// Under communication: nats the sidecar answers 422 BODY_INVALID — a CAS has no meaning on a
// publication — which reaches the caller as the rule-E2 ErrValueInvalid class like any other.
func (c *Client) PutCAS(ctx context.Context, key string, value map[string]any, revision uint64) (uint64, error) {
	return c.put(ctx, key, map[string]any{"value": value, "revision": revision})
}

// put sends one PUT body and reads the new revision off the 200 response. Under communication: nats
// the sidecar publishes instead of writing and answers `{"published": "<key>"}` with no revision at
// all, so the decode leaves the revision at 0 and that zero reaches the caller as rule S4's signal
// that nothing was written to the board — a JetStream revision is never 0.
func (c *Client) put(ctx context.Context, key string, body map[string]any) (uint64, error) {
	encoded, err := encodeBody(body)
	if err != nil {
		return 0, err
	}
	status, response, err := c.transport.call(ctx, http.MethodPut, c.transport.stateURL(key), encoded, c.transport.budget)
	if err != nil {
		return 0, fmt.Errorf("bbsdk: put %q: %w", key, err)
	}
	if !succeeded(status) {
		return 0, parseError(status, response, false)
	}
	var payload struct {
		Revision uint64 `json:"revision"`
	}
	if err := json.Unmarshal(response, &payload); err != nil {
		return 0, fmt.Errorf("bbsdk: decoding the put response for %q: %w", key, err)
	}
	return payload.Revision, nil
}

// Delete removes a key unconditionally; deleting a missing key is a no-op (rule S5). Under
// communication: nats every delete is the sidecar's 403 WRITE_NOT_AUTHORIZED — there is no key to
// delete and nothing to publish — reaching the caller as ErrWriteNotAuthorized (rules S5, E2).
func (c *Client) Delete(ctx context.Context, key string) error {
	// An unconditional delete sends no body at all — the CAS form is the only one that carries one.
	return c.delete(ctx, key, nil)
}

// DeleteCAS removes a key only while it stands at revision (rule S5).
func (c *Client) DeleteCAS(ctx context.Context, key string, revision uint64) error {
	encoded, err := encodeBody(map[string]any{"revision": revision})
	if err != nil {
		return err
	}
	return c.delete(ctx, key, encoded)
}

// delete sends one DELETE, with or without a CAS body.
func (c *Client) delete(ctx context.Context, key string, body []byte) error {
	status, response, err := c.transport.call(ctx, http.MethodDelete, c.transport.stateURL(key), body, c.transport.budget)
	if err != nil {
		return fmt.Errorf("bbsdk: delete %q: %w", key, err)
	}
	if !succeeded(status) {
		return parseError(status, response, false)
	}
	return nil
}

// Keys lists every readable key name, empty when the bucket is (rule S6).
func (c *Client) Keys(ctx context.Context) ([]string, error) {
	status, body, err := c.transport.call(ctx, http.MethodGet, c.transport.baseURL+"/v1/state", nil, c.transport.budget)
	if err != nil {
		return nil, fmt.Errorf("bbsdk: keys: %w", err)
	}
	if !succeeded(status) {
		return nil, parseError(status, body, false)
	}
	var payload struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("bbsdk: decoding the keys response: %w", err)
	}
	if payload.Keys == nil {
		return []string{}, nil
	}
	// The list the sidecar returns is already read-scoped; the SDK filters nothing.
	return payload.Keys, nil
}

// Claim acquires a meta.claim.* lease for ttl; false is a normal outcome, never an error (rule S14).
func (c *Client) Claim(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	encoded, err := encodeBody(map[string]any{"key": key, "ttl_s": ttl.Seconds()})
	if err != nil {
		return false, err
	}
	status, response, err := c.transport.call(ctx, http.MethodPost, c.transport.baseURL+"/v1/claims", encoded, c.transport.budget)
	if err != nil {
		return false, fmt.Errorf("bbsdk: claim %q: %w", key, err)
	}
	if !succeeded(status) {
		return false, parseError(status, response, false)
	}
	var payload struct {
		Acquired bool `json:"acquired"`
	}
	if err := json.Unmarshal(response, &payload); err != nil {
		return false, fmt.Errorf("bbsdk: decoding the claim response for %q: %w", key, err)
	}
	return payload.Acquired, nil
}

// ReleaseClaim releases a lease; releasing one this instance does not hold is a no-op (rule S14).
func (c *Client) ReleaseClaim(ctx context.Context, key string) error {
	status, response, err := c.transport.call(ctx, http.MethodDelete, c.transport.claimURL(key), nil, c.transport.budget)
	if err != nil {
		return fmt.Errorf("bbsdk: release claim %q: %w", key, err)
	}
	if !succeeded(status) {
		return parseError(status, response, false)
	}
	return nil
}

// Close releases the transport resources the client owns; it is idempotent (rule S16).
func (c *Client) Close() error {
	return c.transport.close()
}
