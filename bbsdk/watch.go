// Contract: contracts/sidecar.md — Part A rules A24 and A25, the data-plane watch stream.

package bbsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// The frames the watch stream carries (sidecar.md rule A24).
const (
	watchEventPut    = "put"
	watchEventDelete = "delete"
	watchEventSynced = "synced"
)

// WatchOptions carries the watch's optional callbacks; the zero value registers none (rule S9).
type WatchOptions struct {
	// OnSynced fires on the one synced frame, the replay-complete marker.
	OnSynced func()
	// OnDelete receives a tombstone; the main callback never sees a delete.
	OnDelete func(key string, revision uint64)
	// OnError receives a frame the SDK cannot decode, which is then skipped.
	OnError func(error)
}

// Watch opens one watch stream and blocks, delivering events until ctx is done or the stream ends (rule S8).
func (c *Client) Watch(ctx context.Context, patterns []string, callback func(Entry), opts WatchOptions) error {
	target := c.transport.baseURL + "/v1/state/watch"
	if len(patterns) > 0 {
		// Omitting the parameter entirely for a nil or empty slice is what lets the sidecar's own
		// whole-workspace default apply.
		target += "?" + url.Values{"patterns": {strings.Join(patterns, ",")}}.Encode()
	}
	// The stream runs on the caller's context alone: no per-call budget is attached, so no response
	// read deadline can end a connection the sidecar is keeping alive with comments (rule S2).
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("bbsdk: building the watch request: %w", err)
	}
	response, err := c.transport.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("bbsdk: opening the watch stream: %w", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			report(opts.OnError, fmt.Errorf("bbsdk: closing the watch stream: %w", closeErr))
		}
	}()
	if !succeeded(response.StatusCode) {
		payload, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return fmt.Errorf("bbsdk: reading the watch stream's error body: %w", readErr)
		}
		return parseError(response.StatusCode, payload, false)
	}

	// There is no automatic reconnect: the sidecar puts resume on the client, and a reconnecting
	// client re-receives the initial replay and dedups by (key, revision) — behavior a component
	// must be able to see, so the SDK does not hide it (rule S8).
	reader := newSSEReader(response.Body)
	for {
		frame, err := reader.next()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, io.EOF) {
				return errors.New("bbsdk: the watch stream ended")
			}
			return fmt.Errorf("bbsdk: reading the watch stream: %w", err)
		}
		deliverWatchFrame(frame, callback, opts)
	}
}

// deliverWatchFrame routes one frame to its callback; an unknown event name is skipped (rule G8).
func deliverWatchFrame(frame sseFrame, callback func(Entry), opts WatchOptions) {
	switch frame.event {
	case watchEventPut:
		// The initial replay of every matching key and every subsequent write both arrive here.
		var payload struct {
			Key      string         `json:"key"`
			Value    map[string]any `json:"value"`
			Revision uint64         `json:"revision"`
		}
		if err := json.Unmarshal([]byte(frame.data), &payload); err != nil {
			report(opts.OnError, fmt.Errorf("bbsdk: decoding a watch put frame: %w", err))
			return
		}
		if callback != nil {
			callback(Entry{Key: payload.Key, Value: payload.Value, Revision: payload.Revision})
		}
	case watchEventDelete:
		var payload struct {
			Key      string `json:"key"`
			Revision uint64 `json:"revision"`
		}
		if err := json.Unmarshal([]byte(frame.data), &payload); err != nil {
			report(opts.OnError, fmt.Errorf("bbsdk: decoding a watch delete frame: %w", err))
			return
		}
		if opts.OnDelete != nil {
			opts.OnDelete(payload.Key, payload.Revision)
		}
	case watchEventSynced:
		if opts.OnSynced != nil {
			opts.OnSynced()
		}
	}
}
