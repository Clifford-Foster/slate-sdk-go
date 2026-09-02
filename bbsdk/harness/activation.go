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
	// Source is what produced this activation: the board, or a subject message (rules H21, K21).
	Source bbsdk.ActivationSource
	// Snapshot is the watched-key state the precondition was evaluated against.
	Snapshot map[string]map[string]any
	// ChangedKeys is the sorted set of watched keys changed since the previous evaluation.
	ChangedKeys []string
	// Writes are the writes that succeeded, in application order.
	Writes []Write
	// Publishes are the returned names routed to a declared publishes subject, never put on the
	// board, in the mapping's sorted order after its board writes (rule H21).
	Publishes []Publish
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

// activate runs one board activation: it is the engine's activate() and therefore already
// single-flighted among board evaluations, so a change arriving under it coalesces into one
// re-evaluation afterwards (§3.4 rule 2).
func (h *Harness) activate(ctx context.Context, _ *blackboard.Blackboard) error {
	// Under a declared single_flight this is the one guard both sources share, so a message arriving
	// while a board activation runs waits behind it in arrival order; under false the board source
	// keeps the core engine's own guard among board evaluations and takes none here (rule B39).
	ticket := h.guard.enter(h.singleFlight())
	ticket.wait()
	defer ticket.release()
	h.mu.Lock()
	evaluation := h.pending
	h.mu.Unlock()
	snapshot := snapshotOf(evaluation.State.Snapshot)
	changed := evaluation.Changed
	if changed == nil {
		changed = []string{}
	}
	// The strict-read gate runs before the component function and before any id is minted: a
	// declined evaluation produces no Record, consumes nothing, and lands on ValidationSkips
	// instead — never a silent non-activation (rule H20).
	if verdicts := h.shapes.readVerdicts(snapshot); len(verdicts) > 0 {
		h.decline(ValidationSkip{Snapshot: snapshot, ChangedKeys: changed, Verdicts: verdicts})
		return nil
	}
	h.runActivation(ctx, bbsdk.HarnessBoardSource(), nil, snapshot, changed, evaluation.Revisions)
	return nil
}

// runActivation is the one delivery both sources share: the board's firing evaluation and rule
// H21's subject message reach the same component function, through the same data plane, and are
// recorded as the same Record — only Source tells them apart (rules H21, K21).
func (h *Harness) runActivation(
	ctx context.Context,
	source bbsdk.ActivationSource,
	input map[string]any,
	snapshot map[string]map[string]any,
	changed []string,
	revisions map[string]uint64,
) {
	h.mu.Lock()
	h.sequence++
	record := Record{
		ActivationID: fmt.Sprintf("act-%d", h.sequence),
		Source:       source,
		Snapshot:     snapshot,
		ChangedKeys:  changed,
		Consumed:     []string{},
	}
	h.mu.Unlock()
	delivery := &plane{
		harness:   h,
		holder:    record.ActivationID,
		inherited: effectiveInherited(record.Snapshot, record.ChangedKeys),
		strict:    h.options.StrictReads,
		recording: true,
	}
	board := source.Kind != sourceKindSubject
	defer func() {
		record.Writes = delivery.applied()
		record.Publishes = delivery.published()
		if recovered := recover(); recovered != nil {
			// A panicking component is a failed activation, never a dead harness (rule H9).
			record.Err = fmt.Errorf("harness: the component panicked: %v", recovered)
		}
		h.complete(record, board)
	}()
	h.deliver(ctx, &record, delivery, input, revisions)
}

// deliver hands the activation to the component and applies what it answers with: the returned
// mapping as one authorized batch routed by declaration, then the error-record clear, then
// consumption — the sidecar's own order of effects (rules H8, H13, H21).
func (h *Harness) deliver(
	ctx context.Context, record *Record, delivery *plane, input map[string]any, revisions map[string]uint64,
) {
	activation := bbsdk.HarnessActivation(delivery, blackboardID, record.ActivationID,
		record.Source, input, record.Snapshot, record.ChangedKeys, h.config)
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

// applyReturned routes the returned mapping by declaration and applies it as one batch: a name
// matching a writes pattern is a board write, a name equal to a declared publishes subject is a
// publish, and a name matching neither costs the whole mapping — nothing applied, nothing published
// (rules H8, H21, the harness twin of the sidecar's rule-B37 routing and WRITE_REJECTED). The board
// writes apply first, then the publishes, both in sorted key order, which is the order Go
// serializes a map in and therefore the order the sidecar would apply (rule K10).
func (h *Harness) applyReturned(ctx context.Context, delivery *plane, writes bbsdk.Writes, correlationID string) error {
	if len(writes) == 0 {
		return nil
	}
	keys := slices.Sorted(maps.Keys(writes))
	var board, publish []string
	for _, key := range keys {
		switch {
		case reserved(key):
			return fmt.Errorf("%w: the returned mapping names %q, in the platform-reserved meta.* key space; none of it was applied",
				ErrUndeclaredWrite, key)
		case h.boardWritable(key):
			board = append(board, key)
		case h.publishable(key):
			publish = append(publish, key)
		default:
			return fmt.Errorf("%w: the returned mapping names %q, which matches no writes pattern and no publishes subject; none of it was applied",
				ErrUndeclaredWrite, key)
		}
	}
	// The declared shapes judge the batch on rule H8's terms: one violating entry costs the whole
	// mapping, and the correlation stamp below is applied only to what survived (rules H20, H21).
	var verdicts []bbsdk.ValidationVerdict
	for _, key := range keys {
		verdicts = append(verdicts, h.outboundVerdicts(key, writes[key])...)
	}
	if len(verdicts) > 0 {
		return schemaViolation("the returned mapping", verdicts)
	}
	for _, key := range board {
		value := stampCorrelation(writes[key], correlationID)
		revision, err := h.board.Put(ctx, key, value)
		if err != nil {
			// KV has no transactions: a value-rule failure mid-apply keeps what already landed.
			return fmt.Errorf("harness: applying the returned write %q: %w", key, err)
		}
		delivery.record(Write{Key: key, Value: value, Revision: revision})
	}
	for _, subject := range publish {
		delivery.publish(Publish{Subject: subject, Payload: stampCorrelation(writes[subject], correlationID)})
	}
	return nil
}

// outboundVerdicts judges one outbound name against whichever shape it declared: the write schemas
// matching it as a key, and the publishes entry's own schema where it names one (rules H20, H21).
func (h *Harness) outboundVerdicts(name string, value map[string]any) []bbsdk.ValidationVerdict {
	return append(h.shapes.writeVerdicts(name, value), h.shapes.publishVerdicts(name, value)...)
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

// complete files a finished activation and releases everything waiting on one. Only a board
// activation clears the engine's in-flight mark: under single_flight: false a subject activation
// overlaps one, and clearing it there would let Settle return with the board work still running.
func (h *Harness) complete(record Record, board bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record)
	if board {
		h.inFlight = false
	}
	h.wake()
}

// decline files a firing evaluation the strict-read gate refused and releases the guard: Settle
// treats it as a completed evaluation, because nothing about it remains pending (rule H20).
func (h *Harness) decline(skip ValidationSkip) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.skips = append(h.skips, skip)
	h.inFlight = false
	h.wake()
}
