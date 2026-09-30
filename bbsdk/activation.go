// Contract: contracts/sidecar.md — Part B, the activation the boundary delivers and the one result
// envelope it expects back.

package bbsdk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Clifford-Foster/slate-sdk-go/blackboard"
)

// Writes is the returned-writes mapping the sidecar applies on the component's behalf (rule B8).
type Writes map[string]map[string]any

// ActivationFunc is the component's activation entry point, identical in both delivery modes (rule K1).
type ActivationFunc func(ctx context.Context, a *Activation) (Writes, error)

// RPCFunc handles one bridged Micro request (rule K13).
type RPCFunc func(ctx context.Context, r *RPCRequest) (map[string]any, error)

// AppError is the application-failure object Fail records (sidecar.md rule B28).
type AppError struct {
	// Code is the component's own vocabulary; the transport enum is the sidecar's, never a component's.
	Code string `json:"code"`
	// Message is the free-form failure text.
	Message string `json:"message"`
	// Detail is the optional structured detail, omitted from the envelope when absent.
	Detail map[string]any `json:"detail,omitempty"`
}

// Result is the one result envelope: the push response body and the pull ack body (rule K5).
type Result struct {
	// Writes is the returned-writes mapping, absent when the activation returned none.
	Writes Writes `json:"writes,omitempty"`
	// CorrelationID is the boundary mint, absent when the activation minted none.
	CorrelationID string `json:"correlation_id,omitempty"`
	// Error is the application failure, mutually exclusive with Writes.
	Error *AppError `json:"error,omitempty"`
}

// dataPlane is the Part A surface an Activation, an Event and an RPCRequest reach. In production it
// is the very client the component built — no second transport, no second budget — and *Client is
// its only implementation on that path; the in-process board the harness supplies is the other
// (rule H2). An Activation overrides the four write calls to add rules K7 and K9; everything else is
// a straight hand-off, because the SDK owns no platform semantics of its own (rule G6).
type dataPlane interface {
	Get(ctx context.Context, key string) (*Entry, error)
	Keys(ctx context.Context) ([]string, error)
	Put(ctx context.Context, key string, value map[string]any) (uint64, error)
	PutCAS(ctx context.Context, key string, value map[string]any, revision uint64) (uint64, error)
	Delete(ctx context.Context, key string) error
	DeleteCAS(ctx context.Context, key string, revision uint64) error
	Invoke(ctx context.Context, service, endpoint string, payload map[string]any, opts ...InvokeOption) (map[string]any, error)
	Claim(ctx context.Context, key string, ttl time.Duration) (bool, error)
	ReleaseClaim(ctx context.Context, key string) error
}

var _ dataPlane = (*Client)(nil)

// sourceKindBoard is the source kind every precondition-driven activation carries, and the value a
// payload with no source member at all defaults to — a sidecar older than B36 (rule K21).
const sourceKindBoard = "board"

// ActivationSource is the ActivationPayload's source member: what produced this activation (rule K21).
type ActivationSource struct {
	// Kind is "board" on a precondition-driven activation and "subject" on a hardwired one.
	Kind string `json:"kind"`
	// Subject is the declared subscription the message arrived on, in the manifest's own spelling.
	Subject string `json:"subject"`
	// Durable is that subscription's declared delivery arm.
	Durable bool `json:"durable"`
	// Redelivered reports whether the broker re-delivered the message.
	Redelivered bool `json:"redelivered"`
}

// Activation is one delivered activation: the payload fields, the data plane, and the mint and fail
// affordances the boundary requires of a component (rules K1, K6 to K9).
type Activation struct {
	// BlackboardID is the blackboard the sidecar is attached to.
	BlackboardID string
	// ActivationID is the component's idempotency key, stable across redeliveries (rules K4, B3).
	ActivationID string
	// Source is this activation's source; Kind is "board" when the payload carries none (rule K21).
	Source ActivationSource
	// Input is the subject message body parsed as an object, nil on a board activation (rule K21).
	Input map[string]any
	// InputB64 is base64 of the raw message bytes, carried only when Input is null (rule K21).
	InputB64 string
	// Snapshot is every currently-set watched key's value at evaluation time.
	Snapshot map[string]map[string]any
	// ChangedKeys is the sorted, de-duplicated set of keys whose change produced this evaluation.
	ChangedKeys []string
	// Revisions is the delivery identity: each changed key's revision, empty when the payload carries none (rule B3).
	Revisions map[string]uint64
	// Config is the parsed BB_CONFIG: the same map on every activation, read-only by convention (rule C3).
	Config map[string]any

	dataPlane

	mu            sync.Mutex
	correlationID string
	failure       *AppError
}

// activationPayload is the ActivationPayload body both delivery legs carry (sidecar.md Part B).
type activationPayload struct {
	BlackboardID string                    `json:"blackboard_id"`
	Snapshot     map[string]map[string]any `json:"snapshot"`
	ChangedKeys  []string                  `json:"changed_keys"`
	Revisions    map[string]uint64         `json:"revisions"`
	ActivationID string                    `json:"activation_id"`
	// Source is a pointer so its absence stays distinguishable from a zero value: a payload carrying
	// no source member at all is an older sidecar's, and defaults to the board kind (rule K21).
	Source   *ActivationSource `json:"source"`
	Input    map[string]any    `json:"input"`
	InputB64 string            `json:"input_b64"`
}

// source reads the payload's source member, defaulting a payload that carries none to the board
// kind with every other field zero — so a component written against rule K21 runs unchanged behind
// a sidecar older than B36 and never branches on absence.
func (p activationPayload) source() ActivationSource {
	if p.Source == nil {
		return ActivationSource{Kind: sourceKindBoard}
	}
	return *p.Source
}

// revisions reads the payload's revisions member — the delivery identity of sidecar.md rule B3,
// the key a component dedups a side effect on. A payload carrying none (a sidecar older than
// 0.34.0) yields an empty map rather than nil, so a component indexes one shape on every
// activation and never branches on absence.
func (p activationPayload) revisions() map[string]uint64 {
	if p.Revisions == nil {
		return map[string]uint64{}
	}
	return p.Revisions
}

// decodeActivation decodes an ActivationPayload, leaving the fields a body lacks at their zero
// values. A body that is not a JSON object decodes to nothing at all and is not reported: the SDK
// does not second-guess its own sidecar (rule K2, Python-SDK parity).
func decodeActivation(body []byte) activationPayload {
	var payload activationPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return payload
	}
	return payload
}

// newActivation builds one activation from a decoded payload, the component's data plane, and the
// process-wide config both delivery modes share.
func newActivation(payload activationPayload, plane dataPlane, config map[string]any) *Activation {
	return &Activation{
		BlackboardID: payload.BlackboardID,
		ActivationID: payload.ActivationID,
		Source:       payload.source(),
		Input:        payload.Input,
		InputB64:     payload.InputB64,
		Snapshot:     payload.Snapshot,
		ChangedKeys:  payload.ChangedKeys,
		Revisions:    payload.revisions(),
		Config:       config,
		dataPlane:    plane,
	}
}

// NewCorrelation mints this activation's correlation id once and returns that id thereafter (rule K6).
func (a *Activation) NewCorrelation() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.correlationID == "" {
		// Nothing is minted implicitly: an activation that never calls this leaves its writes
		// unstamped, exactly as the boundary's own precedence requires (sidecar.md rule B22).
		a.correlationID = mintCorrelation()
	}
	return a.correlationID
}

// Fail ends the activation as an application failure; it is terminal and single-shot (rules K8, K9).
func (a *Activation) Fail(code, message string, detail map[string]any) error {
	if code == "" {
		return errors.New("bbsdk: Fail needs a non-empty code")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failure != nil {
		return fmt.Errorf("%w: Fail is terminal and single-shot", ErrActivationFailed)
	}
	a.failure = &AppError{Code: code, Message: message, Detail: detail}
	return nil
}

// Result builds the one result envelope, for the push response body and the pull ack alike (rule K5).
func (a *Activation) Result(writes Writes) (Result, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failure != nil && len(writes) > 0 {
		// Writes and error together are the boundary's BAD_RESPONSE, so the envelope is refused
		// here: failing locally is strictly better than posting one the sidecar must reject, and the
		// wire outcome is a component-side failure either way.
		return Result{}, fmt.Errorf(
			"bbsdk: the activation both failed with code %q and returned %d writes; the result envelope carries one or the other",
			a.failure.Code, len(writes))
	}
	result := Result{CorrelationID: a.correlationID}
	if a.failure != nil {
		result.Error = a.failure
		return result, nil
	}
	if len(writes) > 0 {
		result.Writes = writes
	}
	return result, nil
}

// Put writes a value unconditionally, carrying this activation's mint (rules K7, K9, S4).
func (a *Activation) Put(ctx context.Context, key string, value map[string]any) (uint64, error) {
	stamped, err := a.stamp(value)
	if err != nil {
		return 0, err
	}
	return a.dataPlane.Put(ctx, key, stamped)
}

// PutCAS writes a value only while the key stands at revision, carrying the mint (rules K7, K9, S4).
func (a *Activation) PutCAS(ctx context.Context, key string, value map[string]any, revision uint64) (uint64, error) {
	stamped, err := a.stamp(value)
	if err != nil {
		return 0, err
	}
	return a.dataPlane.PutCAS(ctx, key, stamped, revision)
}

// Delete removes a key unconditionally; no write is accepted after Fail (rules K9, S5).
func (a *Activation) Delete(ctx context.Context, key string) error {
	if err := a.writable(); err != nil {
		return err
	}
	return a.dataPlane.Delete(ctx, key)
}

// DeleteCAS removes a key only while it stands at revision (rules K9, S5).
func (a *Activation) DeleteCAS(ctx context.Context, key string, revision uint64) error {
	if err := a.writable(); err != nil {
		return err
	}
	return a.dataPlane.DeleteCAS(ctx, key, revision)
}

// writable reports the terminal-failure guard: a write after Fail is refused (rule K9).
func (a *Activation) writable() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failure != nil {
		return fmt.Errorf("%w: no write is accepted after Fail", ErrActivationFailed)
	}
	return nil
}

// stamp applies rule K9's guard and rule K7's mint: a minted activation stamps its id into the
// value's reserved _meta unless the value already carries one, the per-write override winning. The
// sidecar learns the mint only from the response, so a put issued after NewCorrelation must carry
// the id itself to reach the boundary as that override (sidecar.md rule B23).
func (a *Activation) stamp(value map[string]any) (map[string]any, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failure != nil {
		return nil, fmt.Errorf("%w: no write is accepted after Fail", ErrActivationFailed)
	}
	if a.correlationID == "" || blackboard.CorrelationOf(value) != "" {
		return value, nil
	}
	return blackboard.WithCorrelation(value, a.correlationID), nil
}

// The event-handler path is retired outright (rule K12, Clifford's D2 ruling, 2026-09-01): EventFunc,
// Event and the EventPayload decode are gone with sidecar.md's event_url, EventPayload and rule B15,
// and have no replacement API. A message on a declared subscribes subject is an ordinary rule-K1
// activation whose Source.Kind is "subject" and whose Input is the message body (rule K21).

// RPCRequest is one bridged Micro request (rule K13).
type RPCRequest struct {
	// Endpoint is the manifest endpoint name the request was routed to.
	Endpoint string
	// Payload is the raw Micro request body, lossless (sidecar.md rule B16).
	Payload []byte
	// Config is the parsed BB_CONFIG (rule C3).
	Config map[string]any

	dataPlane
}

// mintCorrelation returns a fresh UUIDv4 rendered as 32 lowercase hex digits (rule K6).
// crypto/rand.Read is documented never to fail and always to fill its buffer, which is what lets
// rule K6's signature carry no error channel; the branch stands only because every error is handled
// at its call site here, and an unreachable failure mints nothing rather than a weak id.
func mintCorrelation() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return ""
	}
	buffer[6] = (buffer[6] & 0x0f) | 0x40 // version 4
	buffer[8] = (buffer[8] & 0x3f) | 0x80 // RFC 4122 variant
	return hex.EncodeToString(buffer)
}
