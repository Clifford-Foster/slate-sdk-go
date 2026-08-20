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

// EventFunc handles one fire-and-forget event (rule K12).
type EventFunc func(ctx context.Context, e *Event) error

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

// Activation is one delivered activation: the payload fields, the data plane, and the mint and fail
// affordances the boundary requires of a component (rules K1, K6 to K9).
type Activation struct {
	// BlackboardID is the blackboard the sidecar is attached to.
	BlackboardID string
	// ActivationID is the component's idempotency key, stable across redeliveries (rules K4, B3).
	ActivationID string
	// Snapshot is every currently-set watched key's value at evaluation time.
	Snapshot map[string]map[string]any
	// ChangedKeys is the sorted, de-duplicated set of keys whose change produced this evaluation.
	ChangedKeys []string
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
	ActivationID string                    `json:"activation_id"`
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
		Snapshot:     payload.Snapshot,
		ChangedKeys:  payload.ChangedKeys,
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

// Event is one delivered event, mirroring the sidecar's EventPayload (rule K12).
type Event struct {
	// Subject is the concrete NATS subject the message arrived on.
	Subject string
	// Payload is the body parsed as a JSON object, nil when the wire value is null.
	Payload map[string]any
	// PayloadB64 is base64 of the raw bytes, present only when Payload is nil.
	PayloadB64 string
	// Config is the parsed BB_CONFIG (rule C3).
	Config map[string]any

	dataPlane
}

// eventPayload is the EventPayload body the event webhook and the pull stream both carry
// (sidecar.md rule B15).
type eventPayload struct {
	Subject    string         `json:"subject"`
	Payload    map[string]any `json:"payload"`
	PayloadB64 string         `json:"payload_b64"`
}

// decodeEvent decodes an EventPayload. The wire is mirrored exactly, the lossless non-JSON case
// included: an event whose body is not an object arrives as a nil Payload plus PayloadB64
// (sidecar.md rule B15).
func decodeEvent(body []byte) eventPayload {
	var payload eventPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return eventPayload{}
	}
	return payload
}

// newEvent builds one event from a decoded payload, the component's data plane, and the
// process-wide config. The push webhook and the pull stream build the same Event from the same
// payload, so one handler serves a component in either delivery mode (rules K12, K15a).
func newEvent(payload eventPayload, plane dataPlane, config map[string]any) *Event {
	return &Event{
		Subject:    payload.Subject,
		Payload:    payload.Payload,
		PayloadB64: payload.PayloadB64,
		Config:     config,
		dataPlane:  plane,
	}
}

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
