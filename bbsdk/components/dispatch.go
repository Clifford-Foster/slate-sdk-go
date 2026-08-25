// Contract: contracts/components_runtime.md — rule 8b, the component-call dispatcher a calling component's
// Call arrives through, and the per-slot aggregate of §Status key.

package components

import (
	"context"
	"errors"
	"sync"
	"time"
)

// defaultMaxComponentCalls is MAX_COMPONENT_CALLS: the per-activation ceiling an entry whose document
// carries no max_component_calls takes (components_runtime.md §Chain definition, rule 8b).
const defaultMaxComponentCalls = 1000

// abandonWait bounds the cancel-and-wait of rule 8b for a call whose wired entry declares no
// timeout_s: the wait must be bounded, and such an entry supplies no budget of its own to bound it by.
const abandonWait = 5 * time.Second

// The three conditions rule 8b refuses a Call at its boundary under. No second error taxonomy is
// minted for them: each is carried by the message of an internal, non-retryable ComponentError.
const (
	codeComponentUnknown       = "COMPONENT_UNKNOWN"
	codeComponentCallAbandoned = "COMPONENT_CALL_ABANDONED"
	codeComponentCallLimit     = "COMPONENT_CALL_LIMIT"
)

// slotTotals is one slot's per-activation aggregate: the counters §Status key puts on the calling
// entry's terminal record.
type slotTotals struct {
	calls     int
	errors    int
	duration  time.Duration
	target    string
	abandoned bool
}

// inflightCall is one dispatched call the dispatcher may still have to wait for at traversal end.
type inflightCall struct {
	slot     string
	started  time.Time
	timeoutS int
	// finished closes when the called component's goroutine has returned, which is what "waits for it"
	// means: the call is over, not merely no longer awaited.
	finished chan struct{}
}

// dispatcher is one calling entry's component-call handle — the named type rule 8b's Call arrives
// through. It is handed to the component once at Bind and re-armed per activation by the runner.
type dispatcher struct {
	// alias names the calling entry in the failures the dispatcher raises itself.
	alias string
	// wiring is the entry's components table, already bound; a slot it does not carry resolves to nothing.
	wiring   map[string]*boundComponent
	maxCalls int
	backoff  time.Duration

	mu sync.Mutex
	// calls is the running per-activation total, summed across every slot (rule 8b's ceiling).
	calls  int
	ended  bool
	totals map[string]*slotTotals
	// callsCtx descends from the traversal's own context, so the dispatcher enforces the traversal's
	// deadline whatever context the calling code hands Call.
	callsCtx context.Context
	cancel   context.CancelFunc
	inflight []*inflightCall
}

// newDispatcher builds one calling entry's handle over its bound wiring.
func newDispatcher(alias string, wiring map[string]*boundComponent, maxCalls int) *dispatcher {
	if maxCalls <= 0 {
		maxCalls = defaultMaxComponentCalls
	}
	return &dispatcher{alias: alias, wiring: wiring, maxCalls: maxCalls, backoff: retryBackoff}
}

// begin arms the dispatcher for one activation: a fresh ceiling count, a fresh aggregate, and a
// context descending from the traversal's own.
func (d *dispatcher) begin(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancel != nil {
		d.cancel()
	}
	d.callsCtx, d.cancel = context.WithCancel(ctx)
	d.calls, d.ended = 0, false
	d.totals = map[string]*slotTotals{}
	d.inflight = nil
}

// Call dispatches one component call: resolution, the boundary refusals, per-call seam validation at
// both seams, the wired retries, and the aggregate — all of it the runner's, never the calling component's.
func (d *dispatcher) Call(ctx context.Context, slot string, payload map[string]any) (map[string]any, error) {
	wired, failure := d.admit(slot)
	if failure != nil {
		return nil, failure
	}
	started := time.Now()
	output, failure := d.attempts(ctx, slot, wired, payload)
	d.record(slot, time.Since(started), failure != nil)
	if failure != nil {
		return nil, failure
	}
	return output, nil
}

// admit performs the three refusals rule 8b makes at the Call boundary, before anything is
// dispatched, and takes the call's place under the ceiling when none of them fires.
func (d *dispatcher) admit(slot string) (*boundComponent, *ComponentError) {
	d.mu.Lock()
	defer d.mu.Unlock()
	wired, resolved := d.wiring[slot]
	if !resolved {
		// Compose makes this unreachable for a chain it accepted; the runtime states it because a
		// runtime handed a hand-written document must still fail honestly.
		return nil, stepError(CauseInternal, false, ErrComponentUnknown,
			"%s: component %q called slot %q, which its wiring does not resolve", codeComponentUnknown, d.alias, slot)
	}
	if d.ended {
		return nil, stepError(CauseInternal, false, nil,
			"%s: component %q called slot %q after its traversal had ended",
			codeComponentCallAbandoned, d.alias, slot)
	}
	if d.calls >= d.maxCalls {
		return nil, stepError(CauseInternal, false, nil,
			"%s: component %q reached its ceiling of %d component calls per activation",
			codeComponentCallLimit, d.alias, d.maxCalls)
	}
	d.calls++
	d.slotTotals(slot, wired).calls++
	return wired, nil
}

// attempts runs the wired entry's retry budget inside the dispatcher, so only the exhausted outcome
// reaches the calling code: the wiring owns transport policy and the code owns the business decision.
func (d *dispatcher) attempts(
	ctx context.Context,
	slot string,
	wired *boundComponent,
	payload map[string]any,
) (map[string]any, *ComponentError) {
	attempt := 0
	for {
		output, failure := d.validated(ctx, slot, wired, payload)
		if failure == nil {
			return output, nil
		}
		// mayRetry carries rule 6's exclusion: a validation failure is never retried, at either seam
		// and whatever the wired entry's retries says.
		if mayRetry(failure) && attempt < wired.spec.Retries && d.wait() {
			attempt++
			continue
		}
		return nil, failure
	}
}

// validated is one attempt: the rule-2 discipline and rule-6 validation at both seams around the
// call, on every call, naming the wired component's own alias.
func (d *dispatcher) validated(
	ctx context.Context,
	slot string,
	wired *boundComponent,
	payload map[string]any,
) (map[string]any, *ComponentError) {
	alias := wired.spec.Alias
	encoded, failure := jsonDict(payload, alias, "input")
	if failure != nil {
		return nil, failure
	}
	if failure := validateSeam(wired.inputSchema, encoded, alias, "input"); failure != nil {
		return nil, failure
	}
	output, failure := d.dispatch(ctx, slot, wired, payload)
	if failure != nil {
		return nil, failure
	}
	encoded, failure = jsonDict(output, alias, "output")
	if failure != nil {
		return nil, failure
	}
	if failure := validateSeam(wired.outputSchema, encoded, alias, "output"); failure != nil {
		return nil, failure
	}
	return output, nil
}

// dispatch performs the one call the wired entry's communication names. The call runs on its own recovering
// goroutine — which is why a panic escaping a CALLED component's Run is normalized whatever goroutine
// Call was made from (rule T4) — and cancellation cancels it and then WAITS for it, bounded by that
// call's own budget: a deliberate divergence from the stage path, which abandons rather than waits.
func (d *dispatcher) dispatch(
	ctx context.Context,
	slot string,
	wired *boundComponent,
	payload map[string]any,
) (map[string]any, *ComponentError) {
	parent := d.traversalContext()
	callCtx, cancel := context.WithCancel(parent)
	defer cancel()
	if wired.spec.TimeoutS > 0 {
		var expire context.CancelFunc
		callCtx, expire = context.WithTimeout(callCtx, time.Duration(wired.spec.TimeoutS)*time.Second)
		defer expire()
	}
	if ctx != nil {
		// Call must be given a context descending from run()'s; the dispatcher enforces the traversal's
		// own deadline regardless, so a detached one is bounded all the same and whichever ends first wins.
		defer context.AfterFunc(ctx, cancel)()
	}

	call := &inflightCall{slot: slot, started: time.Now(), timeoutS: wired.spec.TimeoutS,
		finished: make(chan struct{})}
	d.enroll(call)
	defer d.retire(call)
	done := make(chan outcome, 1)
	go func() {
		// finished closes last, after the outcome has been delivered or the panic normalized.
		defer close(call.finished)
		defer func() {
			if value := recover(); value != nil {
				done <- outcome{err: recovered(wired.spec.Alias, value)}
			}
		}()
		if wired.communication == communicationNATS {
			output, failure := invokeStep(callCtx, wired, payload)
			done <- outcome{output: output, err: failure}
			return
		}
		output, err := wired.component.Run(callCtx, payload)
		if err != nil {
			done <- outcome{err: normalize(wired.spec.Alias, err)}
			return
		}
		done <- outcome{output: output}
	}()

	select {
	case result := <-done:
		return d.classify(result, parent, callCtx, wired)
	case <-callCtx.Done():
		wait := time.NewTimer(waitBound(call))
		defer wait.Stop()
		select {
		case result := <-done:
			return d.classify(result, parent, callCtx, wired)
		case <-wait.C:
			// The bounded wait expired — a component that ignores its context ignores its budget too.
			// The runner stops waiting, marks the slot so the terminal record names the call left
			// running, and proceeds to its terminal write.
			d.markAbandoned(slot)
			return nil, stepError(CauseInternal, false, callCtx.Err(),
				"%s: component %q left its call on slot %q running: %s",
				codeComponentCallAbandoned, d.alias, slot, callCtx.Err())
		}
	}
}

// classify applies the wired entry's own budget to the outcome: an expiry is rule 8.3's retryable
// timeout however the called component spelled the failure it returned.
func (d *dispatcher) classify(
	result outcome,
	parent, callCtx context.Context,
	wired *boundComponent,
) (map[string]any, *ComponentError) {
	if wired.spec.TimeoutS > 0 && errors.Is(callCtx.Err(), context.DeadlineExceeded) && parent.Err() == nil {
		return nil, timedOut(wired)
	}
	return result.output, result.err
}

// finish ends this entry's calls for the activation and folds its aggregate for the terminal record
// about to be written: no further call is admitted, every in-flight one is cancelled and waited for
// bounded by its own budget, and a slot still running when the wait expires is marked abandoned.
func (d *dispatcher) finish() map[string]any {
	d.mu.Lock()
	if d.ended {
		defer d.mu.Unlock()
		return d.fold()
	}
	d.ended = true
	cancel, pending := d.cancel, d.inflight
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, call := range pending {
		wait := time.NewTimer(waitBound(call))
		select {
		case <-call.finished:
		case <-wait.C:
			d.markAbandoned(call.slot)
		}
		wait.Stop()
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.fold()
}

// fold renders the per-slot aggregate §Status key puts on the terminal record: one member per slot
// the entry called, and nothing at all for an entry that called none.
func (d *dispatcher) fold() map[string]any {
	if len(d.totals) == 0 {
		return nil
	}
	aggregate := make(map[string]any, len(d.totals))
	for slot, totals := range d.totals {
		member := map[string]any{
			"calls":  totals.calls,
			"errors": totals.errors,
			"ms":     int(totals.duration.Milliseconds()),
		}
		if totals.target != "" {
			member["target"] = totals.target
		}
		if totals.abandoned {
			member["abandoned"] = true
		}
		aggregate[slot] = member
	}
	return aggregate
}

// record folds one finished call into its slot's totals, under the dispatcher's own exclusion: a component
// may call several slots concurrently, and the single writer reads the result at the terminal write.
func (d *dispatcher) record(slot string, spent time.Duration, failed bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	totals, counted := d.totals[slot]
	if !counted {
		return
	}
	totals.duration += spent
	if failed {
		// A caught failure counts too: the aggregate is what keeps a handled one visible.
		totals.errors++
	}
}

// markAbandoned records that a slot's in-flight call was still running when the runner stopped
// waiting for it.
func (d *dispatcher) markAbandoned(slot string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if totals, counted := d.totals[slot]; counted {
		totals.abandoned = true
	}
}

// slotTotals is the slot's aggregate member, minted on its first call. The caller holds the lock.
func (d *dispatcher) slotTotals(slot string, wired *boundComponent) *slotTotals {
	totals, present := d.totals[slot]
	if !present {
		totals = &slotTotals{}
		if wired.communication == communicationNATS && wired.spec.Service != nil {
			// The target records which service was called, never which build answered it (rule 15).
			totals.target = wired.spec.Service.Name + "." + wired.spec.Service.Endpoint
		}
		d.totals[slot] = totals
	}
	return totals
}

// enroll registers a dispatched call as in flight.
func (d *dispatcher) enroll(call *inflightCall) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inflight = append(d.inflight, call)
}

// retire drops a call that has returned.
func (d *dispatcher) retire(call *inflightCall) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, enrolled := range d.inflight {
		if enrolled == call {
			d.inflight = append(d.inflight[:i], d.inflight[i+1:]...)
			return
		}
	}
}

// traversalContext is the context calls descend from — the traversal's own, or a dead one when the
// dispatcher was never armed.
func (d *dispatcher) traversalContext() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.callsCtx == nil {
		return context.Background()
	}
	return d.callsCtx
}

// wait spends rule 8.4's fixed backoff between attempts, reporting false when the traversal ended first.
func (d *dispatcher) wait() bool {
	ctx := d.traversalContext()
	if d.backoff <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d.backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// waitBound is what remains of a call's own budget, which is what rule 8b bounds the cancel-and-wait
// by. A call whose entry declares no timeout_s has no budget of its own and takes abandonWait.
func waitBound(call *inflightCall) time.Duration {
	if call.timeoutS <= 0 {
		return abandonWait
	}
	remaining := time.Duration(call.timeoutS)*time.Second - time.Since(call.started)
	if remaining < 0 {
		return 0
	}
	return remaining
}
