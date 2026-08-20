// Contract: contracts/sidecar.md — Part A, the loopback data plane this transport speaks.

package bbsdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// defaultCallBudget is the per-call context budget every non-streaming call runs under (rule S2).
const defaultCallBudget = 5 * time.Second

// clientOptions is the whole of the client's configuration. Rule S1 offers no other knob: there is
// no retry policy, no header hook and no middleware chain, because the boundary needs none.
type clientOptions struct {
	baseURL    string
	httpClient *http.Client
	budget     time.Duration
}

// Option configures a client against the loopback data plane (rule S1).
type Option func(*clientOptions) error

// WithBaseURL overrides the loopback data-plane address rule C1 resolves from the environment.
func WithBaseURL(baseURL string) Option {
	return func(o *clientOptions) error {
		parsed, err := url.Parse(baseURL)
		if err != nil {
			return fmt.Errorf("bbsdk: base URL %q: %w", baseURL, err)
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("bbsdk: base URL %q needs a scheme and a host", baseURL)
		}
		o.baseURL = strings.TrimSuffix(baseURL, "/")
		return nil
	}
}

// WithHTTPClient supplies the transport; the client is copied with its Timeout cleared (rule S2).
func WithHTTPClient(client *http.Client) Option {
	return func(o *clientOptions) error {
		if client == nil {
			return errors.New("bbsdk: WithHTTPClient needs a client")
		}
		o.httpClient = client
		return nil
	}
}

// WithTimeout sets the per-call context budget of rule S2, whose default is five seconds.
func WithTimeout(budget time.Duration) Option {
	return func(o *clientOptions) error {
		if budget <= 0 {
			return fmt.Errorf("bbsdk: the call budget must be positive, got %s", budget)
		}
		o.budget = budget
		return nil
	}
}

// transport is the HTTP plumbing the data-plane surfaces share: the resolved base URL, an
// http.Client that never carries a Timeout, and the per-call budget every non-streaming call
// spends as a context deadline.
type transport struct {
	baseURL string
	http    *http.Client
	budget  time.Duration
	// owned is the transport this SDK built and therefore closes; nil when the caller supplied a
	// client, whose idle connections are not the SDK's to release (rule S16).
	owned     *http.Transport
	closeOnce sync.Once
}

// newTransport resolves the options into the deadline-free plumbing of rules C1, S1 and S2.
func newTransport(opts []Option) (*transport, error) {
	settings := clientOptions{baseURL: DataPlaneBaseURL(), budget: defaultCallBudget}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&settings); err != nil {
			return nil, err
		}
	}
	resolved := &transport{baseURL: settings.baseURL, budget: settings.budget}
	if settings.httpClient != nil {
		// The supplied client is used with its Timeout cleared on a copy. A response-read deadline
		// would reach the streams as well, which is the defect class rule S2 removes by
		// construction rather than by a per-call override.
		copied := *settings.httpClient
		copied.Timeout = 0
		resolved.http = &copied
		return resolved, nil
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("bbsdk: the standard library's default transport is not an *http.Transport")
	}
	// Cloning keeps the standard dial and TLS-handshake budgets, which rule S2 leaves standing; the
	// http.Client itself is built without a Timeout, so no stream can inherit a request deadline.
	owned := base.Clone()
	resolved.owned = owned
	resolved.http = &http.Client{Transport: owned}
	return resolved, nil
}

// stateURL renders a key as one percent-encoded path segment, so no character — a slash included —
// can split it. The sidecar's own key-grammar check then applies to the decoded key (rule S7).
func (t *transport) stateURL(key string) string {
	return t.baseURL + "/v1/state/" + url.PathEscape(key)
}

// claimURL renders a claim key as one percent-encoded path segment (rules S7, S14).
func (t *transport) claimURL(key string) string {
	return t.baseURL + "/v1/claims/" + url.PathEscape(key)
}

// call issues one non-streaming request under budget as a context deadline, returning the response
// status and body. The caller's own deadline still wins when it is sooner (rule S2).
func (t *transport) call(ctx context.Context, method, target string, body []byte, budget time.Duration) (int, []byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(callCtx, method, target, reader)
	if err != nil {
		return 0, nil, fmt.Errorf("building the %s request: %w", method, err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	// No credential, no client certificate and no cookie rides the boundary: it is unauthenticated
	// loopback, and net/http attaches no cookie without an explicit Jar (rule G5).
	response, err := t.http.Do(request)
	if err != nil {
		// A dial failure, a broken connection or a context deadline stays what it is; it is never
		// dressed up as a boundary error with an invented code (rule E7).
		return 0, nil, err
	}
	payload, readErr := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); closeErr != nil && readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		return 0, nil, readErr
	}
	return response.StatusCode, payload, nil
}

// close releases the idle connections of a transport the SDK built; a supplied client is left
// alone. Closing is idempotent (rule S16).
func (t *transport) close() error {
	t.closeOnce.Do(func() {
		if t.owned != nil {
			t.owned.CloseIdleConnections()
		}
	})
	return nil
}

// succeeded reports whether a status is a 2xx, the only shape carrying a result body.
func succeeded(status int) bool {
	return status >= 200 && status < 300
}

// encodeBody marshals a request body; a value the component cannot serialize fails before the wire.
func encodeBody(body map[string]any) ([]byte, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("bbsdk: encoding the request body: %w", err)
	}
	return encoded, nil
}
