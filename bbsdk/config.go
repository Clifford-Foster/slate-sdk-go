package bbsdk

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
)

// The supervisor-injected environment the SDK reads. No other variable is consulted: every timeout,
// NATS and scoping variable belongs to the sidecar's own container, never the component's (rule C4).
const (
	envDataPlanePort = "BB_DATA_PLANE_PORT"
	envActivateHost  = "BB_ACTIVATE_HOST"
	envActivatePort  = "BB_ACTIVATE_PORT"
	envConfig        = "BB_CONFIG"
)

// The boundary's defaults: the loopback data-plane port, and the activation bind the manifest
// convention and the built image's entrypoint agree on (rules C1, C2).
const (
	defaultDataPlanePort = "8722"
	defaultActivateHost  = "0.0.0.0"
	defaultActivatePort  = 8080
)

// DataPlaneBaseURL returns the loopback data-plane base URL, reading the environment on each call (rule C1).
func DataPlaneBaseURL() string {
	port := os.Getenv(envDataPlanePort)
	if port == "" {
		port = defaultDataPlanePort
	}
	// The host is never derived from the environment: loopback reachability is the boundary's whole
	// trust assumption, so no variable can move the data plane off it (sidecar.md rules A1, A2).
	return "http://127.0.0.1:" + port
}

// ActivateBind returns the host and port a push component's activation listener binds (rule C2).
func ActivateBind() (string, int, error) {
	host := os.Getenv(envActivateHost)
	if host == "" {
		// 0.0.0.0 deliberately: the component must be reachable by its paired sidecar inside the
		// shared network namespace.
		host = defaultActivateHost
	}
	raw := os.Getenv(envActivatePort)
	if raw == "" {
		return host, defaultActivatePort, nil
	}
	port, err := strconv.Atoi(raw)
	if err != nil {
		// A non-numeric port is an error, never a silent default.
		return "", 0, fmt.Errorf("bbsdk: %s=%q is not a port number: %w", envActivatePort, raw, err)
	}
	return host, port, nil
}

// componentConfig memoizes the parse of BB_CONFIG so the environment is read once per process and
// every activation, event and RPC context receives the same map (rule C3). Unlike rule C1's
// DataPlaneBaseURL, which reads the environment on each call, a later rewrite of the variable never
// moves the value an in-flight activation sees.
var componentConfig struct {
	once  sync.Once
	value map[string]any
	err   error
}

// ComponentConfig returns the parsed BB_CONFIG object, parsed once per process (rules C3, C3a).
func ComponentConfig() (map[string]any, error) {
	componentConfig.once.Do(func() {
		componentConfig.value, componentConfig.err = parseComponentConfig(os.Getenv(envConfig))
	})
	return componentConfig.value, componentConfig.err
}

// parseComponentConfig reads the {name: value} object BB_CONFIG carries. Unset and empty yield an
// empty map; valid JSON that is not an object yields an empty map too; anything else is an error a
// constructor propagates, so a component with a broken config never reaches a first delivery (rule C3a).
func parseComponentConfig(raw string) (map[string]any, error) {
	if raw == "" {
		return map[string]any{}, nil
	}
	var document any
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		return nil, fmt.Errorf("bbsdk: %s is not valid JSON: %w", envConfig, err)
	}
	object, ok := document.(map[string]any)
	if !ok {
		return map[string]any{}, nil
	}
	return object, nil
}
