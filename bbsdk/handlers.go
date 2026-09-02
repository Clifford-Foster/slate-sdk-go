// Contract: contracts/sidecar.md — Part B rules B9 and B16, the two webhooks a push component
// serves; the event webhook is retired with rule B15 (bb_sdk_go.md rule K12).

package bbsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// readHeaderTimeout bounds a request's header read, matching the repo's other listeners.
const readHeaderTimeout = 10 * time.Second

// ActivateHandler serves the push activation webhook at the manifest's activate_url path (rule K2).
func ActivateHandler(c *Client, h ActivationFunc) http.Handler {
	// A malformed BB_CONFIG is a startup failure the constructors propagate (rule C3a); resolving it
	// here is the earliest a constructor with no error channel can, and a component that built its
	// client already saw it.
	config, configErr := ComponentConfig()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bbsdk: the activation webhook accepts POST", http.StatusMethodNotAllowed)
			return
		}
		if configErr != nil {
			http.Error(w, "bbsdk: BB_CONFIG is not valid JSON", http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bbsdk: the activation body could not be read", http.StatusInternalServerError)
			return
		}
		activation := newActivation(decodeActivation(body), c, config)
		writes, err := h(r.Context(), activation)
		if err != nil {
			// A handler error is the infrastructure failure signal: the sidecar records NON_2XX and
			// never retries it. A component reporting that its own work failed uses Fail (rules K3, K8).
			http.Error(w, "bbsdk: the activation handler failed", http.StatusInternalServerError)
			return
		}
		result, err := activation.Result(writes)
		if err != nil {
			http.Error(w, "bbsdk: the activation result is not a valid envelope", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
}

// The event webhook is retired with sidecar.md rule B15 and has no replacement (rule K12): a message
// on a declared subscribes subject reaches ActivateHandler as an ordinary activation (rule K21).

// RPCHandler serves the bridged Micro endpoints under the manifest's rpc_url mount point (rule K13).
func RPCHandler(c *Client, handlers map[string]RPCFunc) http.Handler {
	config, configErr := ComponentConfig()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bbsdk: the RPC bridge accepts POST", http.StatusMethodNotAllowed)
			return
		}
		if configErr != nil {
			http.Error(w, "bbsdk: BB_CONFIG is not valid JSON", http.StatusInternalServerError)
			return
		}
		endpoint := endpointOf(r.URL.Path)
		handler, known := handlers[endpoint]
		if !known {
			writeJSON(w, http.StatusNotFound,
				map[string]any{"error": fmt.Sprintf("bbsdk: no handler for RPC endpoint %q", endpoint)})
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bbsdk: the RPC body could not be read", http.StatusInternalServerError)
			return
		}
		request := &RPCRequest{
			Endpoint:  endpoint,
			Payload:   body,
			Config:    config,
			dataPlane: c,
		}
		reply, err := handler(r.Context(), request)
		if err != nil {
			// The sidecar turns a non-2xx into a Micro error reply.
			http.Error(w, "bbsdk: the RPC handler failed", http.StatusInternalServerError)
			return
		}
		if reply == nil {
			reply = map[string]any{}
		}
		writeJSON(w, http.StatusOK, reply)
	})
}

// endpointOf reads the endpoint name off a bridged request path. The sidecar posts to
// {rpc_url}/{endpoint}, so the last path segment names it whether or not the component stripped its
// own mount prefix before handing the request over (rule K13).
func endpointOf(requestPath string) string {
	if index := strings.LastIndexByte(requestPath, '/'); index >= 0 {
		return requestPath[index+1:]
	}
	return requestPath
}

// Serve binds the activation address, serves h until ctx is done, then drains (rule K14).
func Serve(ctx context.Context, h http.Handler) error {
	host, port, err := ActivateBind()
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              net.JoinHostPort(host, strconv.Itoa(port)),
		Handler:           h,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	// Serve routes nothing: the component composes the handler constructors onto its own mux at the
	// paths its manifest declares, and hands the result here.
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()
	select {
	case err := <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("bbsdk: serving %s: %w", server.Addr, err)
	case <-ctx.Done():
	}
	// Shutdown waits for in-flight handlers with no deadline of its own: the supervisor's SIGTERM
	// grace window and its SIGKILL are what bound the drain (build_service.md rule B5).
	if err := server.Shutdown(context.Background()); err != nil {
		return fmt.Errorf("bbsdk: draining %s: %w", server.Addr, err)
	}
	<-served
	return nil
}

// SignalContext turns SIGINT and SIGTERM into cancellation of the returned context (rule K14).
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	// The SDK installs a signal handler only when asked; this is the one line a component's main
	// needs to satisfy the entrypoint protocol's drain-and-exit requirement (build_service.md B5).
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

// writeJSON renders a handler response body; a body the SDK cannot marshal degrades to a 500 rather
// than a panic, since no exported symbol panics on any input (rule G3).
func writeJSON(w http.ResponseWriter, status int, body any) {
	encoded, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "bbsdk: the response body could not be encoded", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// A write failure means the sidecar is gone; there is nothing left to report it through.
	if _, err := w.Write(encoded); err != nil {
		return
	}
}
