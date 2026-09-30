// Contract: contracts/bb_sdk_go.md — rule H2's seam.
//
// The constructors src/bb_sdk_go/harness builds its in-process deliveries through. A harness
// delivery is the very same Activation and RPCRequest a sidecar delivery is — one mint, one Fail
// guard, one result envelope (rules K5 to K9), which is what makes rule H3's parity promise
// mechanical rather than re-implemented — over a data plane that is the harness's in-memory board
// instead of the loopback client. Only the data plane is unexported, so only these constructors can
// attach one. The event constructor is retired with the event-handler path (rule K12).
//
// This is not public API: it is the narrow equivalent of the repo's testexport.go convention
// (.claude/rules/conventions.md), used by this SDK's own harness subpackage and by nothing else.

package bbsdk

// HarnessActivation builds one activation over a harness-supplied data plane (rules H2, H21). A
// board delivery passes ActivationSource{Kind: "board"} and a nil input; a subject delivery passes
// the subscription's own source and the message body, exactly the members rule K21 states.
func HarnessActivation(
	plane dataPlane,
	blackboardID, activationID string,
	source ActivationSource,
	input map[string]any,
	snapshot map[string]map[string]any,
	changedKeys []string,
	revisions map[string]uint64,
	config map[string]any,
) *Activation {
	return newActivation(activationPayload{
		BlackboardID: blackboardID,
		ActivationID: activationID,
		Source:       &source,
		// InputB64 is left empty: the harness delivers structured payloads, never raw bytes (rule H21).
		Input:       input,
		Snapshot:    snapshot,
		ChangedKeys: changedKeys,
		// The delivery identity the harness's own board assigned, so a component's dedup runs here
		// exactly as it runs behind the sidecar (rule B3; test_harness.md 0.22.0's parity).
		Revisions: revisions,
	}, plane, config)
}

// HarnessBoardSource is the source every board activation carries, harness and sidecar alike (rule K21).
func HarnessBoardSource() ActivationSource { return ActivationSource{Kind: sourceKindBoard} }

// HarnessRPCRequest builds one bridged request over a harness-supplied data plane (rules H2, H13).
func HarnessRPCRequest(plane dataPlane, endpoint string, payload []byte, config map[string]any) *RPCRequest {
	return &RPCRequest{Endpoint: endpoint, Payload: payload, Config: config, dataPlane: plane}
}

// HarnessInvokeError builds the locally raised invoke verdict of rule E6, carrying no HTTP status (rule H14).
func HarnessInvokeError(code, message string) *SidecarError {
	return &SidecarError{Code: code, Message: message, invoke: true}
}

// HarnessSchemaViolation builds the locally raised write-schema verdict of rule H20: the same
// SCHEMA_VIOLATION value the data plane returns, carrying the Verdicts and no HTTP status (rules E6, E8).
func HarnessSchemaViolation(message string, verdicts []ValidationVerdict) *SidecarError {
	return &SidecarError{Code: codeSchemaViolation, Message: message, Validation: verdicts}
}
