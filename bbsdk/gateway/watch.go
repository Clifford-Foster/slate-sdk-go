package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
)

// Rule K19's ladder, the numbers the pull loop implements: base 0.5 s, doubling per consecutive
// failure, cap 15 s, jitter ±20 %, applied before the failure counter increments.
const (
	reconnectBase   = 500 * time.Millisecond
	reconnectCap    = 15 * time.Second
	reconnectJitter = 0.2
)

// allKeys is the whole-board wildcard. The subscribe message takes 1-32 patterns
// (control_plane.md rule A14), so a nil pattern list becomes this rather than an omitted parameter.
const allKeys = ">"

// maxFrameBytes bounds one entry message. A blackboard value is bounded at 1 MiB
// (blackboard_platform.md §1) and the message wraps it in an envelope carrying the key, the
// revision and the stamp — so the read bound sits above the value bound, never at it.
const maxFrameBytes = 2 << 20

// The server → client message types this client reads (control_plane.md rules A16, A17).
const (
	messageEntry = "entry"
	messageSync  = "sync"
)

// The close codes that do not heal, and the control-plane code each one means (rule W3).
var fatalCloseCodes = map[websocket.StatusCode]string{
	4401: codeAuthRequired,
	4403: codeForbidden,
	4404: codeWorkspaceNotFound,
}

// WatchOptions carries the watch's optional callbacks; the zero value registers none.
type WatchOptions struct {
	// OnSynced fires on each sync message, the end of a replay (control_plane.md rule A16).
	OnSynced func()
	// OnResynced fires before every POST-RECONNECT replay: every key is about to arrive at its
	// latest revision and the revisions between may have been collapsed (rule W3).
	OnResynced func()
	// OnDelete receives a tombstone; the main callback never sees a delete.
	OnDelete func(key string, revision uint64)
	// OnError receives a message the client cannot decode, and each transport failure it retries.
	OnError func(error)
}

// Watch opens one watch socket per attempt and blocks, reconnecting on the rule-K19 ladder until
// ctx is done or a close that does not heal ends it (rule W3).
func (c *Client) Watch(
	ctx context.Context, workspace string, patterns []string, callback func(bbsdk.Entry), opts WatchOptions,
) error {
	keys := patterns
	if len(keys) == 0 {
		keys = []string{allKeys}
	}
	subscribe, err := encodeSubscribe(keys)
	if err != nil {
		return fmt.Errorf("bbsdk/gateway: encoding the subscribe message: %w", err)
	}
	target := c.watchURL(workspace)
	failures := 0
	resyncing := false
	for {
		if ctx.Err() != nil {
			return nil
		}
		delivered, fatal := c.session(ctx, target, subscribe, callback, opts, resyncing)
		if fatal != nil {
			return fatal
		}
		if ctx.Err() != nil {
			return nil
		}
		if delivered {
			failures = 0 // a delivered message proves the connection healed
		}
		c.sleep(ctx, reconnectDelay(failures, c.jitter))
		failures++
		resyncing = true
	}
}

// encodeSubscribe renders control_plane.md rule A15's subscribe message. The members are ordered and
// the HTML escaping disabled so the bytes match the Python module's for the same patterns: a `>`
// wildcard must reach the server as `>`, not as the standard encoder's `>` (rules G4, W5).
func encodeSubscribe(keys []string) ([]byte, error) {
	message := struct {
		Type string   `json:"type"`
		Keys []string `json:"keys"`
	}{Type: "subscribe", Keys: keys}
	var rendered bytes.Buffer
	encoder := json.NewEncoder(&rendered)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(message); err != nil {
		return nil, err
	}
	return bytes.TrimRight(rendered.Bytes(), "\n"), nil
}

// session runs one connection: dial, subscribe, then deliver until the socket ends. It reports
// whether anything was delivered, and the error that must not be retried when there is one.
func (c *Client) session(
	ctx context.Context,
	target string,
	subscribe []byte,
	callback func(bbsdk.Entry),
	opts WatchOptions,
	resyncing bool,
) (bool, error) {
	header := http.Header{}
	// The bearer rides the upgrade exactly as it rides every other request (rule W1).
	header.Set("Authorization", "Bearer "+c.apiKey)
	conn, response, err := websocket.Dial(ctx, target, &websocket.DialOptions{HTTPClient: c.http, HTTPHeader: header})
	if err != nil {
		if refused := refusedUpgrade(response); refused != nil {
			return false, refused
		}
		report(opts.OnError, fmt.Errorf("bbsdk/gateway: opening the watch socket: %w", err))
		return false, nil
	}
	defer func() {
		if closeErr := conn.CloseNow(); closeErr != nil {
			report(opts.OnError, fmt.Errorf("bbsdk/gateway: closing the watch socket: %w", closeErr))
		}
	}()
	conn.SetReadLimit(maxFrameBytes)
	if err := conn.Write(ctx, websocket.MessageText, subscribe); err != nil {
		report(opts.OnError, fmt.Errorf("bbsdk/gateway: sending the subscribe message: %w", err))
		return false, nil
	}
	if resyncing && opts.OnResynced != nil {
		opts.OnResynced()
	}
	delivered := false
	for {
		kind, data, err := conn.Read(ctx)
		if err != nil {
			if code, fatal := fatalCloseCodes[websocket.CloseStatus(err)]; fatal {
				return delivered, closeFailure(code, "the watch socket closed "+
					fmt.Sprint(int(websocket.CloseStatus(err))))
			}
			if ctx.Err() == nil {
				report(opts.OnError, fmt.Errorf("bbsdk/gateway: reading the watch socket: %w", err))
			}
			return delivered, nil
		}
		if kind != websocket.MessageText {
			continue
		}
		if deliverMessage(data, callback, opts) {
			delivered = true
		}
	}
}

// deliverMessage routes one server message; an error notice or an unknown type is skipped, and a
// message that will not decode is reported and skipped rather than ending the watch (rule S9's Go form).
func deliverMessage(data []byte, callback func(bbsdk.Entry), opts WatchOptions) bool {
	var message struct {
		Type     string         `json:"type"`
		Key      string         `json:"key"`
		Value    map[string]any `json:"value"`
		Revision uint64         `json:"revision"`
		Deleted  bool           `json:"deleted"`
	}
	if err := json.Unmarshal(data, &message); err != nil {
		report(opts.OnError, fmt.Errorf("bbsdk/gateway: decoding a watch message: %w", err))
		return false
	}
	switch message.Type {
	case messageEntry:
		if message.Deleted {
			if opts.OnDelete != nil {
				opts.OnDelete(message.Key, message.Revision)
			}
			return true
		}
		if callback != nil {
			callback(bbsdk.Entry{Key: message.Key, Value: message.Value, Revision: message.Revision})
		}
		return true
	case messageSync:
		if opts.OnSynced != nil {
			opts.OnSynced()
		}
		return true
	}
	return false
}

// refusedUpgrade types an upgrade the control plane refused with 401/403, or nil to reconnect.
func refusedUpgrade(response *http.Response) error {
	if response == nil {
		return nil
	}
	switch response.StatusCode {
	case http.StatusUnauthorized:
		return closeFailure(codeAuthRequired, "the watch upgrade was refused")
	case http.StatusForbidden:
		return closeFailure(codeForbidden, "the watch upgrade was refused")
	}
	return nil
}

// watchURL renders the watch socket's address: the base URL's own scheme, and a path outside the
// /api/v1 prefix (control_plane.md Part A §WebSocket).
func (c *Client) watchURL(workspace string) string {
	parsed, err := url.Parse(c.baseURL)
	if err != nil {
		return c.baseURL
	}
	scheme := "ws"
	if parsed.Scheme == "https" || parsed.Scheme == "wss" {
		scheme = "wss"
	}
	parsed.Scheme = scheme
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + "/ws/workspaces/" + url.PathEscape(workspace) + "/watch"
	return parsed.String()
}

// reconnectDelay is capped exponential backoff with ±jitter for the Nth consecutive reconnect
// (rule K19's numbers; jitter returns a float in [0, 1)).
func reconnectDelay(failures int, jitter func() float64) time.Duration {
	delay := float64(reconnectBase) * math.Pow(2, float64(failures))
	delay = math.Min(delay, float64(reconnectCap))
	delay += delay * reconnectJitter * (2*jitter() - 1)
	return time.Duration(math.Max(0, delay))
}
