// Contract: contracts/bb_sdk_go.md — rule H2's seam.
//
// The four constructors src/bb_sdk_go/harness builds its in-process deliveries through. A harness
// delivery is the very same Activation, Event and RPCRequest a sidecar delivery is — one mint, one
// Fail guard, one result envelope (rules K5 to K9), which is what makes rule H3's parity promise
// mechanical rather than re-implemented — over a data plane that is the harness's in-memory board
// instead of the loopback client. Only the data plane is unexported, so only these constructors can
// attach one.
//
// This is not public API: it is the narrow equivalent of the repo's testexport.go convention
// (.claude/rules/conventions.md), used by this SDK's own harness subpackage and by nothing else.

package bbsdk

// HarnessActivation builds one activation over a harness-supplied data plane (rule H2).
func HarnessActivation(
	plane dataPlane,
	blackboardID, activationID string,
	snapshot map[string]map[string]any,
	changedKeys []string,
	config map[string]any,
) *Activation {
	return newActivation(activationPayload{
		BlackboardID: blackboardID,
		ActivationID: activationID,
		Snapshot:     snapshot,
		ChangedKeys:  changedKeys,
	}, plane, config)
}

// HarnessEvent builds one event over a harness-supplied data plane (rules H2, H13).
func HarnessEvent(plane dataPlane, subject string, payload map[string]any, config map[string]any) *Event {
	// PayloadB64 is left empty: the harness delivers structured payloads, never raw bytes (rule H13).
	return &Event{Subject: subject, Payload: payload, Config: config, dataPlane: plane}
}

// HarnessRPCRequest builds one bridged request over a harness-supplied data plane (rules H2, H13).
func HarnessRPCRequest(plane dataPlane, endpoint string, payload []byte, config map[string]any) *RPCRequest {
	return &RPCRequest{Endpoint: endpoint, Payload: payload, Config: config, dataPlane: plane}
}

// HarnessInvokeError builds the locally raised invoke verdict of rule E6, carrying no HTTP status (rule H14).
func HarnessInvokeError(code, message string) *SidecarError {
	return &SidecarError{Code: code, Message: message, invoke: true}
}
