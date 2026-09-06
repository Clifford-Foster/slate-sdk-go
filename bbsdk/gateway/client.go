package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
)

// apiPrefix is the control plane's public base path (control_plane.md Part A §HTTP Interface).
const apiPrefix = "/api/v1"

// defaultBudget is the per-call context budget every state call runs under (rule W1, rule S1's posture).
const defaultBudget = 5 * time.Second

// options is the whole of the client's configuration; rule W1 offers no other knob.
type options struct {
	httpClient *http.Client
	budget     time.Duration
	tlsConfig  *tls.Config
	sleep      func(context.Context, time.Duration)
	jitter     func() float64
}

// Option configures a gateway client (rules W1, W3).
type Option func(*options) error

// WithHTTPClient supplies the transport; the client is copied with its Timeout cleared.
func WithHTTPClient(client *http.Client) Option {
	return func(o *options) error {
		if client == nil {
			return errors.New("bbsdk/gateway: WithHTTPClient needs a client")
		}
		o.httpClient = client
		return nil
	}
}

// WithTimeout sets the per-call budget of rule W1, whose default is five seconds.
func WithTimeout(budget time.Duration) Option {
	return func(o *options) error {
		if budget <= 0 {
			return fmt.Errorf("bbsdk/gateway: the call budget must be positive, got %s", budget)
		}
		o.budget = budget
		return nil
	}
}

// WithTLSConfig sets the TLS configuration — the one TLS knob (rule W1); a supplied client's wins.
func WithTLSConfig(config *tls.Config) Option {
	return func(o *options) error {
		if config == nil {
			return errors.New("bbsdk/gateway: WithTLSConfig needs a configuration")
		}
		o.tlsConfig = config
		return nil
	}
}

// WithBackoff injects the reconnect ladder's sleeper and jitter source — the Go form of the Python
// module's rng/sleep seams (rule W3); jitter returns a float in [0, 1).
func WithBackoff(sleep func(ctx context.Context, delay time.Duration), jitter func() float64) Option {
	return func(o *options) error {
		if sleep == nil || jitter == nil {
			return errors.New("bbsdk/gateway: WithBackoff needs both a sleeper and a jitter source")
		}
		o.sleep = sleep
		o.jitter = jitter
		return nil
	}
}

// Client is the one credentialed client: a bearer key over TLS to the control plane's public API.
// It is safe for concurrent use, which is what makes rule W4's fan-out one client (rule W1).
type Client struct {
	baseURL string
	// apiKey is held here and nowhere else: it is never logged and never placed in an error (rule W1).
	apiKey string
	http   *http.Client
	budget time.Duration
	sleep  func(context.Context, time.Duration)
	jitter func() float64
	// owned is the transport this package built and therefore closes; nil when the caller supplied
	// a client, whose idle connections are not this package's to release.
	owned     *http.Transport
	closeOnce sync.Once
}

// New builds a gateway client against the control plane's public API under an API key (rule W1).
func New(baseURL, apiKey string, opts ...Option) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("bbsdk/gateway: base URL %q: %w", baseURL, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("bbsdk/gateway: base URL %q needs a scheme and a host", baseURL)
	}
	settings := options{budget: defaultBudget, sleep: waitOut, jitter: rand.Float64}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&settings); err != nil {
			return nil, err
		}
	}
	client := &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		apiKey:  apiKey,
		budget:  settings.budget,
		sleep:   settings.sleep,
		jitter:  settings.jitter,
	}
	if settings.httpClient != nil {
		// The supplied client is used with its Timeout cleared on a copy: a response deadline would
		// reach the watch socket as well, which is not what a per-call budget means.
		copied := *settings.httpClient
		copied.Timeout = 0
		client.http = &copied
		return client, nil
	}
	base, isTransport := http.DefaultTransport.(*http.Transport)
	if !isTransport {
		return nil, errors.New("bbsdk/gateway: the standard library's default transport is not an *http.Transport")
	}
	owned := base.Clone()
	if settings.tlsConfig != nil {
		owned.TLSClientConfig = settings.tlsConfig
	}
	client.owned = owned
	// No cookie jar is ever installed: net/http attaches no cookie without one, so an upstream
	// Set-Cookie is never stored and never replayed (rule W1).
	client.http = &http.Client{Transport: owned}
	return client, nil
}

// Get reads one key from a workspace, returning (nil, nil) when it is absent (rule W2).
func (c *Client) Get(ctx context.Context, workspace, key string) (*bbsdk.Entry, error) {
	status, body, header, err := c.call(ctx, http.MethodGet, c.stateURL(workspace, key), nil)
	if err != nil {
		return nil, fmt.Errorf("bbsdk/gateway: get %q: %w", key, err)
	}
	if status == http.StatusNotFound {
		failure := parseEnvelope(status, body)
		if failure.Code == codeKeyNotFound {
			return nil, nil
		}
		return nil, classify(status, body, header)
	}
	if !succeeded(status) {
		return nil, classify(status, body, header)
	}
	var payload struct {
		Key      string         `json:"key"`
		Value    map[string]any `json:"value"`
		Revision uint64         `json:"revision"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("bbsdk/gateway: decoding the get response for %q: %w", key, err)
	}
	return &bbsdk.Entry{Key: payload.Key, Value: payload.Value, Revision: payload.Revision}, nil
}

// Put writes a value into a workspace unconditionally and returns the new revision (rule W2).
func (c *Client) Put(ctx context.Context, workspace, key string, value map[string]any) (uint64, error) {
	return c.put(ctx, workspace, key, map[string]any{"value": value})
}

// PutCAS writes a value only while the key stands at revision, returning the new revision (rule W2).
func (c *Client) PutCAS(
	ctx context.Context, workspace, key string, value map[string]any, revision uint64,
) (uint64, error) {
	return c.put(ctx, workspace, key, map[string]any{"value": value, "revision": revision})
}

// put sends one PUT body and reads the new revision off the 200 response (control_plane.md rules A8, A9).
func (c *Client) put(ctx context.Context, workspace, key string, body map[string]any) (uint64, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("bbsdk/gateway: encoding the put body for %q: %w", key, err)
	}
	status, response, header, err := c.call(ctx, http.MethodPut, c.stateURL(workspace, key), encoded)
	if err != nil {
		return 0, fmt.Errorf("bbsdk/gateway: put %q: %w", key, err)
	}
	if !succeeded(status) {
		return 0, classify(status, response, header)
	}
	var payload struct {
		Revision uint64 `json:"revision"`
	}
	if err := json.Unmarshal(response, &payload); err != nil {
		return 0, fmt.Errorf("bbsdk/gateway: decoding the put response for %q: %w", key, err)
	}
	return payload.Revision, nil
}

// Delete removes a key from a workspace; a missing key is a no-op (control_plane.md rule A10).
func (c *Client) Delete(ctx context.Context, workspace, key string) error {
	return c.delete(ctx, c.stateURL(workspace, key))
}

// DeleteCAS removes a key only while it stands at revision, sent as the query parameter (rule W2).
func (c *Client) DeleteCAS(ctx context.Context, workspace, key string, revision uint64) error {
	return c.delete(ctx, c.stateURL(workspace, key)+"?revision="+strconv.FormatUint(revision, 10))
}

// delete sends one DELETE, with or without the CAS query parameter.
func (c *Client) delete(ctx context.Context, target string) error {
	status, body, header, err := c.call(ctx, http.MethodDelete, target, nil)
	if err != nil {
		return fmt.Errorf("bbsdk/gateway: delete: %w", err)
	}
	if !succeeded(status) {
		return classify(status, body, header)
	}
	return nil
}

// Keys lists every key name the workspace holds (control_plane.md rule A11).
func (c *Client) Keys(ctx context.Context, workspace string) ([]string, error) {
	status, body, header, err := c.call(ctx, http.MethodGet, c.workspaceURL(workspace)+"/state", nil)
	if err != nil {
		return nil, fmt.Errorf("bbsdk/gateway: keys: %w", err)
	}
	if !succeeded(status) {
		return nil, classify(status, body, header)
	}
	var payload struct {
		Keys []string `json:"keys"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("bbsdk/gateway: decoding the keys response: %w", err)
	}
	if payload.Keys == nil {
		return []string{}, nil
	}
	return payload.Keys, nil
}

// History reads the key's retained revision window, newest first (control_plane.md rule A29).
func (c *Client) History(ctx context.Context, workspace, key string) ([]bbsdk.Entry, error) {
	status, body, header, err := c.call(ctx, http.MethodGet, c.stateURL(workspace, key)+"/history", nil)
	if err != nil {
		return nil, fmt.Errorf("bbsdk/gateway: history %q: %w", key, err)
	}
	if !succeeded(status) {
		return nil, classify(status, body, header)
	}
	var payload struct {
		Entries []struct {
			Revision uint64         `json:"revision"`
			Value    map[string]any `json:"value"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("bbsdk/gateway: decoding the history response for %q: %w", key, err)
	}
	entries := make([]bbsdk.Entry, 0, len(payload.Entries))
	for _, retained := range payload.Entries {
		// Rule W2: a tombstone revision carries Value nil, never an empty map — a delete must stay
		// distinguishable from an empty write, which an empty map would silently impersonate.
		entries = append(entries, bbsdk.Entry{Key: key, Value: retained.Value, Revision: retained.Revision})
	}
	return entries, nil
}

// Close releases the transport resources this client owns; it is idempotent.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		if c.owned != nil {
			c.owned.CloseIdleConnections()
		}
	})
	return nil
}

// call issues one request under the per-call budget, carrying the bearer (rule W1).
func (c *Client) call(
	ctx context.Context, method, target string, body []byte,
) (int, []byte, http.Header, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.budget)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(callCtx, method, target, reader)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("building the %s request: %w", method, err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	response, err := c.http.Do(request)
	if err != nil {
		// A dial failure, a broken connection or a deadline stays what it is (rule E7's posture).
		return 0, nil, nil, err
	}
	payload, readErr := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); closeErr != nil && readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		return 0, nil, nil, readErr
	}
	return response.StatusCode, payload, response.Header, nil
}

// workspaceURL renders the workspace's public base, one percent-encoded segment (rule S7's posture).
func (c *Client) workspaceURL(workspace string) string {
	return c.baseURL + apiPrefix + "/workspaces/" + url.PathEscape(workspace)
}

// stateURL renders a key as one percent-encoded path segment, so no character — a slash included —
// can split it (rules W2, S7).
func (c *Client) stateURL(workspace, key string) string {
	return c.workspaceURL(workspace) + "/state/" + url.PathEscape(key)
}

// succeeded reports whether a status is a 2xx, the only shape carrying a result body.
func succeeded(status int) bool {
	return status >= 200 && status < 300
}

// waitOut is the default reconnect sleeper: it waits the delay out, or returns when ctx is done.
func waitOut(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
