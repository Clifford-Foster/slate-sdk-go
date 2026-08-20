package harness

import (
	"context"
	"fmt"
	"maps"
	"slices"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
	"github.com/Clifford-Foster/slate-sdk-go/blackboard"
)

// defaultInstance is the single-instance error-record segment (blackboard_platform.md §7).
const defaultInstance = "default"

// isoMicros is the error record's ts format, the core library's own (blackboard_platform.md §7).
const isoMicros = "2006-01-02T15:04:05.000000Z07:00"

// Record is one completed activation, in completion order (rule H11).
type Record struct {
	// ActivationID is the harness's deterministic per-run sequence: act-1, act-2, … (rule H6).
	ActivationID string
	// Snapshot is the watched-key state the precondition was evaluated against.
	Snapshot map[string]map[string]any
	// ChangedKeys is the sorted set of watched keys changed since the previous evaluation.
	ChangedKeys []string
	// Writes are the writes that succeeded, in application order.
	Writes []Write
	// Err is the error the component returned, or the harness's own verdict on its result (rule H9).
	Err error
	// AppError is the failure a Fail declared, cleared by the next successful activation (rule H13).
	AppError *bbsdk.AppError
	// Consumed is the sorted set of triggering keys a consumes pattern took off the board (rule H13).
	Consumed []string
}

// Write is one applied write; a nil Value records a delete.
type Write struct {
	Key      string
	Value    map[string]any
	Revision uint64
}

// evaluated observes every completed evaluation, firing or not, at the engine's commit point — the
// seam that lets the harness report an activation's snapshot, changed keys and snapshot revisions
// without re-implementing any of them (blackboard_platform.md §3.5; rules H2, H11).
func (h *Harness) evaluated(evaluation blackboard.Evaluation) {
	// The AdvanceTime poke is the harness's own; no component and no record ever sees it (rule H5).
	evaluation.Changed = slices.DeleteFunc(slices.Clone(evaluation.Changed), func(key string) bool {
		return key == tickKey
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.evaluations++
	h.delivered = evaluation.Revisions
	h.pending = evaluation
	if evaluation.Activated {
		h.inFlight = true
	}
	h.wake()
}

// activate runs one activation: it is the engine's activate() and therefore already single-flighted,
// so a change arriving under it coalesces into one re-evaluation afterwards (§3.4 rule 2).
func (h *Harness) activate(ctx context.Context, _ *blackboard.Blackboard) error {
	h.mu.Lock()
	evaluation := h.pending
	h.sequence++
	record := Record{
		ActivationID: fmt.Sprintf("act-%d", h.sequence),
		Snapshot:     snapshotOf(evaluation.State.Snapshot),
		ChangedKeys:  evaluation.Changed,
		Consumed:     []string{},
	}
	h.mu.Unlock()
	if record.ChangedKeys == nil {
		record.ChangedKeys = []string{}
	}
	delivery := &plane{
		harness:   h,
		holder:    record.ActivationID,
		inherited: effectiveInherited(record.Snapshot, record.ChangedKeys),
		strict:    h.options.StrictReads,
		recording: true,
	}
	defer func() {
		record.Writes = delivery.applied()
		if recovered := recover(); recovered != nil {
			// A panicking component is a failed activation, never a dead harness (rule H9).
			record.Err = fmt.Errorf("harness: the component panicked: %v", recovered)
		}
		h.complete(record)
	}()
	h.deliver(ctx, &record, delivery, evaluation.Revisions)
	return nil
}

// deliver hands the activation to the component and applies what it answers with: the returned
// writes as one authorized batch, then the error-record clear, then consumption — the sidecar's own
// order of effects (rules H8, H13).
func (h *Harness) deliver(ctx context.Context, record *Record, delivery *plane, revisions map[string]uint64) {
	activation := bbsdk.HarnessActivation(
		delivery, blackboardID, record.ActivationID, record.Snapshot, record.ChangedKeys, h.config)
	writes, err := h.component(ctx, activation)
	if err != nil {
		// A component failure is recorded, never propagated: earlier successful writes stand (rule H9).
		record.Err = err
		return
	}
	result, err := activation.Result(writes)
	if err != nil {
		// Fail together with a returned mapping: nothing is applied and no error record is written.
		record.Err = err
		return
	}
	effective := result.CorrelationID
	if effective == "" {
		effective = delivery.inherited
	}
	if result.Error != nil {
		record.AppError = result.Error
		record.Err = h.writeErrorRecord(ctx, record.ActivationID, result.Error, effective)
		return
	}
	if err := h.applyReturned(ctx, delivery, result.Writes, effective); err != nil {
		record.Err = err
		return
	}
	// The next successful activation clears the error record, so the key always reflects the latest
	// outcome; consumption is the last effect of all (rule H13).
	h.store.deleteAt(h.errorKey())
	record.Consumed = h.consume(record.ChangedKeys, revisions)
}

// applyReturned authorizes the returned mapping as a batch and then applies it in sorted key order,
// which is the order Go serializes a map in and therefore the order the sidecar would apply
// (rules H8, K10). One undeclared entry costs the whole mapping.
func (h *Harness) applyReturned(ctx context.Context, delivery *plane, writes bbsdk.Writes, correlationID string) error {
	if len(writes) == 0 {
		return nil
	}
	keys := slices.Sorted(maps.Keys(writes))
	for _, key := range keys {
		if reserved(key) {
			return fmt.Errorf("%w: the returned mapping names %q, in the platform-reserved meta.* key space; none of it was applied",
				ErrUndeclaredWrite, key)
		}
		if !writeAuthorized(key, h.loaded.Writes) {
			return fmt.Errorf("%w: the returned mapping names %q, which matches no writes pattern; none of it was applied",
				ErrUndeclaredWrite, key)
		}
	}
	for _, key := range keys {
		value := stampCorrelation(writes[key], correlationID)
		revision, err := h.board.Put(ctx, key, value)
		if err != nil {
			// KV has no transactions: a value-rule failure mid-apply keeps what already landed.
			return fmt.Errorf("harness: applying the returned write %q: %w", key, err)
		}
		delivery.record(Write{Key: key, Value: value, Revision: revision})
	}
	return nil
}

// consume CAS-deletes the triggering keys a consumes pattern matches, at their snapshot revisions: a
// key that moved since the snapshot is left silently, and a deletion trigger — which carries no
// snapshot revision — is never consumable (rule H13).
func (h *Harness) consume(changed []string, revisions map[string]uint64) []string {
	consumed := []string{}
	if len(h.loaded.Consumes) == 0 {
		return consumed
	}
	for _, key := range changed {
		if !matchesAny(key, h.loaded.Consumes) {
			continue
		}
		revision, known := revisions[key]
		if !known {
			continue
		}
		deleted, err := h.store.deleteCAS(key, revision)
		if err != nil || deleted == 0 {
			continue
		}
		consumed = append(consumed, key)
	}
	return consumed
}

// writeErrorRecord parks the §7 error record a Fail produces as an external write: error.> is an
// ordinary namespace the platform writes, never gated by the component's own grants (rule H13).
func (h *Harness) writeErrorRecord(ctx context.Context, activationID string, failure *bbsdk.AppError, correlationID string) error {
	record := blackboard.ErrorRecord(failure.Code, failure.Message, blackboard.ErrorRecordOptions{
		Component:     h.loaded.Name,
		Instance:      defaultInstance,
		ActivationID:  activationID,
		CorrelationID: correlationID,
		Detail:        failure.Detail,
		TS:            h.clock.now().UTC().Format(isoMicros),
	})
	if _, err := h.board.Put(ctx, h.errorKey(), record); err != nil {
		return fmt.Errorf("harness: writing the application error record: %w", err)
	}
	return nil
}

// errorKey is where this component's application failures land (blackboard_platform.md §7).
func (h *Harness) errorKey() string {
	return "error.activation." + h.loaded.Name + "." + defaultInstance
}

// complete files a finished activation and releases everything waiting on one.
func (h *Harness) complete(record Record) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record)
	h.inFlight = false
	h.wake()
}
