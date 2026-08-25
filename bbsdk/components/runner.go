package components

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"
)

// retryBackoff is rule 8.4's fixed wait between attempts on a retryable failure.
const retryBackoff = time.Second

// errTraversalEnded marks the verdict the runner mints for a branch the traversal's end took before
// it reached one of its own. Rule 8a classifies that branch as un-terminal at the stage's
// resolution, and its record as cancellation-derived — the rank the writer flushes below a genuine
// failure's.
var errTraversalEnded = errors.New("bbsdk/components: the traversal ended before the component reached a verdict")

// Runner executes one traversal of a bound chain per activation (rules 8, 8a).
type Runner struct {
	bound   *Bound
	board   Board
	backoff time.Duration
	started []*boundComponent
}

// NewRunner builds a runner over the data plane; the three keys come from the chain document.
func NewRunner(b *Bound, board Board) *Runner {
	return newRunner(b, board, retryBackoff)
}

// newRunner builds a runner whose retry backoff is the supplied one, on the stage path and inside the
// component-call dispatcher alike — rule 8b puts the wired retries there, so the two must agree.
func newRunner(b *Bound, board Board, backoff time.Duration) *Runner {
	runner := &Runner{bound: b, board: board, backoff: backoff}
	discardWalk(b.entries(func(entry *boundComponent) error {
		if entry.calls != nil {
			entry.calls.backoff = backoff
		}
		return nil
	}))
	return runner
}

// discardWalk drops the error of a walk whose visitor cannot fail.
func discardWalk(error) {}

// Start runs Setup on every memory-bound component in chain order — stage by stage, declaration order
// inside a stage, and a wired component immediately after its calling entry's in sorted slot order —
// returning the first failure unwrapped, since a setup failure is a startup failure and rule 3's
// normalization is Run-scoped (rule T6).
func (r *Runner) Start(ctx context.Context) error {
	return r.bound.entries(func(entry *boundComponent) error {
		setuper, hasSetup := entry.component.(Setuper)
		if hasSetup {
			if err := setuper.Setup(ctx, maps.Clone(entry.config)); err != nil {
				return err
			}
		}
		r.started = append(r.started, entry)
		return nil
	})
}

// Stop runs Teardown in reverse chain order, best effort: no failure stops the sweep and all of them
// are returned joined, which is how a Go library declines to raise (rule T6).
func (r *Runner) Stop(ctx context.Context) error {
	var failures []error
	for i := len(r.started) - 1; i >= 0; i-- {
		entry := r.started[i]
		teardowner, hasTeardown := entry.component.(Teardowner)
		if !hasTeardown {
			continue
		}
		if err := teardowner.Teardown(ctx); err != nil {
			failures = append(failures, fmt.Errorf("bbsdk/components: teardown of component %q: %w", entry.spec.Alias, err))
		}
	}
	r.started = nil
	return errors.Join(failures...)
}

// Run executes one traversal: read input_key, run the stages in order, write output_key once on
// success and never on failure, and keep the status key current (rules 8, 8a).
func (r *Runner) Run(ctx context.Context, activationID string) error {
	// Rule 8b's ceiling and aggregate are per activation, so every dispatcher is armed here and closed
	// on the way out — an entry the traversal never reached included, whose calls context would
	// otherwise outlive it.
	r.armCalls(ctx)
	defer r.closeCalls()
	entry, err := r.board.Get(ctx, r.bound.chain.InputKey)
	if err != nil {
		return fmt.Errorf("bbsdk/components: reading the chain input key %q: %w", r.bound.chain.InputKey, err)
	}
	if entry == nil {
		// Rule 8.1: the one runner-level failure that writes no status record at all.
		return fmt.Errorf("%w: %q", ErrChainInputMissing, r.bound.chain.InputKey)
	}
	data := entry.Value
	total := len(r.bound.stages)
	for index, stage := range r.bound.stages {
		data, err = r.runStage(ctx, stage, data, index+1, total, activationID, index+1 == total)
		if err != nil {
			// Fail fast across stages: nothing reaches output_key and no later stage runs (rule 8.5).
			return err
		}
	}
	return nil
}

// RunEntry runs one bound entry once, with its dispatcher armed for the call (rule T13).
//
// This is the hosting a component-declaring component gets over RPC: there is no traversal, so no
// input_key is read, no output_key or status key is written and the Board is not touched at all —
// the input arrives from the caller and the output returns to it. The entry's own retries are not
// applied, a rule-8.4 retry being a fresh `running` status write this hosting has no key for; a wired
// component's retries are the dispatcher's and are untouched. At most one call per entry may be in
// flight, exactly as Run assumes at most one traversal; no guard is added here.
func (r *Runner) RunEntry(ctx context.Context, alias string, input map[string]any) (map[string]any, error) {
	entry := r.bound.entry(alias)
	if entry == nil {
		return nil, fmt.Errorf("bbsdk/components: the bound chain carries no entry aliased %q", alias)
	}
	if entry.calls != nil {
		// Rule 8b's ceiling and aggregate are per activation, and this hosting's activation IS the one
		// call: the dispatcher is armed for it and closed when it returns. What finish folds is dropped —
		// the aggregate rides a terminal status record, and this hosting writes none.
		entry.calls.begin(ctx)
		defer func() { _ = entry.calls.finish() }()
	}
	output, _, failure := r.execute(ctx, entry, input)
	if failure != nil {
		return nil, failure
	}
	return output, nil
}

// armCalls arms every calling entry's dispatcher for this activation (rule 8b).
func (r *Runner) armCalls(ctx context.Context) {
	discardWalk(r.bound.entries(func(entry *boundComponent) error {
		if entry.calls != nil {
			entry.calls.begin(ctx)
		}
		return nil
	}))
}

// closeCalls ends every dispatcher still open when the traversal returns.
func (r *Runner) closeCalls() {
	discardWalk(r.bound.entries(func(entry *boundComponent) error {
		if entry.calls != nil {
			entry.calls.finish()
		}
		return nil
	}))
}

// finishCalls ends one entry's calls and folds its per-slot aggregate for the terminal record about
// to be written; an entry that calls nothing has none (components_runtime.md §Status key).
func finishCalls(entry *boundComponent) map[string]any {
	if entry.calls == nil {
		return nil
	}
	return entry.calls.finish()
}

// branchResult is one branch's outcome: its output dict, or the failure its own budget ended on.
type branchResult struct {
	output map[string]any
	err    *ComponentError
}

// runStage runs one stage — a single component, or every branch of a parallel group concurrently — and
// performs every status write of it through one writer on this goroutine (rules 8, 8a).
func (r *Runner) runStage(
	ctx context.Context,
	stage []*boundComponent,
	input map[string]any,
	index, total int,
	activationID string,
	final bool,
) (map[string]any, error) {
	writer := &statusWriter{
		board:        r.board,
		key:          r.bound.chain.StatusKey,
		activationID: activationID,
		index:        index,
		of:           total,
		holdLastOK:   final,
	}
	// The channel holds every transition a stage can produce — one running per attempt plus one
	// terminal per branch — so a branch never blocks on the writer and wait-all cannot deadlock.
	events := make(chan transition, stageCapacity(stage))
	results := make([]branchResult, len(stage))
	var branches sync.WaitGroup
	for position, entry := range stage {
		branches.Add(1)
		go func() {
			defer branches.Done()
			results[position] = r.runBranch(ctx, entry, input, position, events)
		}()
	}
	go func() {
		branches.Wait()
		close(events)
	}()
	for event := range events {
		writer.accept(ctx, event)
	}
	// Every branch has run to its own completion: the stage has resolved (rule 8a's wait-all).
	if failure := stageFailure(stage, results, index); failure != nil {
		writer.close(ctx)
		return nil, failure
	}
	output := gather(stage, input, results)
	if final {
		// Rule 8.6: the output_key PUT precedes the traversal's last status write, in every topology.
		if _, err := r.board.Put(ctx, r.bound.chain.OutputKey, output); err != nil {
			writer.close(ctx)
			return nil, fmt.Errorf("bbsdk/components: writing the chain output key %q: %w", r.bound.chain.OutputKey, err)
		}
	}
	writer.close(ctx)
	if writer.err != nil {
		return nil, writer.err
	}
	return output, nil
}

// runBranch runs one branch's whole per-component lifecycle — seam validation, its own timeout, its own
// retries with the fixed backoff, rule-3 normalization — emitting transitions instead of writing them.
func (r *Runner) runBranch(
	ctx context.Context,
	entry *boundComponent,
	input map[string]any,
	position int,
	events chan<- transition,
) branchResult {
	alias := entry.spec.Alias
	attempt := 0
	for {
		events <- transition{position: position, alias: alias, state: stateRunning}
		output, duration, failure := r.execute(ctx, entry, input)
		if failure == nil {
			// The aggregate rides every terminal record, and the calls are ended before it is folded —
			// so an abandoned call is named on the record rather than lost after it (rules 8, 8b).
			events <- transition{position: position, alias: alias, state: stateOK,
				durationMS: duration, components: finishCalls(entry)}
			return branchResult{output: output}
		}
		if mayRetry(failure) && attempt < entry.spec.Retries && r.wait(ctx) {
			attempt++
			continue
		}
		events <- transition{position: position, alias: alias, state: stateError,
			err: failure, cancelled: errors.Is(failure, errTraversalEnded), components: finishCalls(entry)}
		return branchResult{err: failure}
	}
}

// wait spends the fixed retry backoff, reporting false when the traversal ended first.
func (r *Runner) wait(ctx context.Context) bool {
	if r.backoff <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(r.backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// execute runs one attempt: the rule-2 discipline and rule-6 validation at both seams, with the
// component's own timeout around the call itself. duration_ms measures the successful call alone.
func (r *Runner) execute(
	ctx context.Context,
	entry *boundComponent,
	input map[string]any,
) (map[string]any, int, *ComponentError) {
	alias := entry.spec.Alias
	encoded, failure := jsonDict(input, alias, "input")
	if failure != nil {
		return nil, 0, failure
	}
	if failure := validateSeam(entry.inputSchema, encoded, alias, "input"); failure != nil {
		return nil, 0, failure
	}
	started := time.Now()
	output, failure := r.dispatch(ctx, entry, input)
	if failure != nil {
		return nil, 0, failure
	}
	duration := int(time.Since(started).Milliseconds())
	encoded, failure = jsonDict(output, alias, "output")
	if failure != nil {
		return nil, 0, failure
	}
	if failure := validateSeam(entry.outputSchema, encoded, alias, "output"); failure != nil {
		return nil, 0, failure
	}
	return output, duration, nil
}

// outcome is one component call's return, carried off the goroutine the component ran on.
type outcome struct {
	output map[string]any
	err    *ComponentError
}

// dispatch performs the one call the communication names, bounded by the component's own timeout_s. The call
// runs on its own goroutine so an expired budget ends the attempt rather than waiting on a component that
// ignores its context, and a panic escaping Run there is recovered and normalized (rule T4).
func (r *Runner) dispatch(
	ctx context.Context,
	entry *boundComponent,
	input map[string]any,
) (map[string]any, *ComponentError) {
	alias := entry.spec.Alias
	callCtx := ctx
	if entry.spec.TimeoutS > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, time.Duration(entry.spec.TimeoutS)*time.Second)
		defer cancel()
	}
	done := make(chan outcome, 1)
	go func() {
		defer func() {
			if value := recover(); value != nil {
				done <- outcome{err: recovered(alias, value)}
			}
		}()
		if entry.communication == communicationNATS {
			output, failure := invokeStep(callCtx, entry, input)
			done <- outcome{output: output, err: failure}
			return
		}
		output, err := entry.component.Run(callCtx, input)
		if err != nil {
			done <- outcome{err: normalize(alias, err)}
			return
		}
		done <- outcome{output: output}
	}()
	select {
	case result := <-done:
		if result.err != nil && expired(ctx, callCtx, entry) {
			// A failure returned after the component's own budget expired is that expiry, however the component
			// spelled it — the classification cannot depend on which of the two the scheduler saw first.
			return nil, timedOut(entry)
		}
		return result.output, result.err
	case <-callCtx.Done():
		if expired(ctx, callCtx, entry) {
			return nil, timedOut(entry)
		}
		// Rule 8a's classification: the component reached no verdict of its own, so this branch is
		// un-terminal at the stage's resolution and its record is cancellation-derived.
		return nil, stepError(CauseInternal, false, fmt.Errorf("%w: %w", errTraversalEnded, callCtx.Err()),
			"component %q did not finish before the traversal ended: %s", alias, callCtx.Err())
	}
}

// expired reports whether the component's own timeout_s ended the attempt, rather than the caller's context.
func expired(ctx, callCtx context.Context, entry *boundComponent) bool {
	return entry.spec.TimeoutS > 0 && errors.Is(callCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
}

// timedOut is rule 8.3's verdict: an expiry is a retryable timeout ComponentError.
func timedOut(entry *boundComponent) *ComponentError {
	return stepError(CauseTimeout, true, context.DeadlineExceeded,
		"component %q exceeded its %ds timeout", entry.spec.Alias, entry.spec.TimeoutS)
}

// mayRetry applies rule 6: a validation failure is never retried, whatever the budget says.
func mayRetry(failure *ComponentError) bool {
	return failure.Retryable && failure.Cause != CauseValidation
}

// jsonDict enforces rule 2's JSON-dict discipline at one seam and returns the bytes a declared seam
// schema validates, so an embedded component is held to exactly what a remote one would put on the wire.
func jsonDict(value map[string]any, alias, seam string) ([]byte, *ComponentError) {
	if value == nil {
		return nil, stepError(CauseInternal, false, nil, "component %q %s is not a dict", alias, seam)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, stepError(CauseInternal, false, err,
			"component %q %s is not JSON-serializable: %s", alias, seam, err)
	}
	return encoded, nil
}

// stageCapacity is the number of transitions a stage can produce: one running per attempt plus one
// terminal record per branch.
func stageCapacity(stage []*boundComponent) int {
	capacity := 0
	for _, entry := range stage {
		capacity += entry.spec.Retries + 2
	}
	return capacity
}

// gather applies rule 14's IO mapping: a single-component stage produces its component's output dict, and a
// parallel stage produces the stage input under _input plus one member per branch, in declaration
// order — collision-free by construction, independent of completion order.
func gather(stage []*boundComponent, input map[string]any, results []branchResult) map[string]any {
	if len(stage) == 1 {
		return results[0].output
	}
	gathered := make(map[string]any, len(stage)+1)
	gathered[gatherInputKey] = input
	for position, entry := range stage {
		gathered[entry.spec.Alias] = results[position].output
	}
	return gathered
}

// stageFailure is the stage's verdict: a single component's own failure, or rule 8a's aggregate naming
// every failed branch with that branch's own cause.
func stageFailure(stage []*boundComponent, results []branchResult, index int) *ComponentError {
	if len(stage) == 1 {
		return results[0].err
	}
	details := make([]string, 0, len(stage))
	wrapped := make([]error, 0, len(stage))
	causes := map[string]struct{}{}
	for position, entry := range stage {
		failure := results[position].err
		if failure == nil {
			continue
		}
		details = append(details, fmt.Sprintf("component %q (%s): %s", entry.spec.Alias, failure.Cause, failure.Message))
		wrapped = append(wrapped, failure)
		causes[failure.Cause] = struct{}{}
	}
	if len(details) == 0 {
		return nil
	}
	// Never retryable — every branch's budget is already spent — and the shared cause only when the
	// failed branches really do share one.
	cause := CauseInternal
	if len(causes) == 1 {
		for only := range causes {
			cause = only
		}
	}
	return stepError(cause, false, errors.Join(wrapped...),
		"parallel stage %d failed in %d branch(es) — %s", index, len(details), strings.Join(details, "; "))
}
