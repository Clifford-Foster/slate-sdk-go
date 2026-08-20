package steps

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

// Runner executes one traversal of a bound chain per activation (rules 8, 8a).
type Runner struct {
	bound   *Bound
	board   Board
	backoff time.Duration
	started []*boundStep
}

// NewRunner builds a runner over the data plane; the three keys come from the chain document.
func NewRunner(b *Bound, board Board) *Runner {
	return &Runner{bound: b, board: board, backoff: retryBackoff}
}

// Start runs Setup on every memory-bound step in chain order, returning the first failure
// unwrapped — a setup failure is a startup failure, and rule 3's normalization is Run-scoped (rule T6).
func (r *Runner) Start(ctx context.Context) error {
	for _, stage := range r.bound.stages {
		for _, entry := range stage {
			setuper, hasSetup := entry.step.(Setuper)
			if hasSetup {
				if err := setuper.Setup(ctx, maps.Clone(entry.config)); err != nil {
					return err
				}
			}
			r.started = append(r.started, entry)
		}
	}
	return nil
}

// Stop runs Teardown in reverse chain order, best effort: no failure stops the sweep and all of them
// are returned joined, which is how a Go library declines to raise (rule T6).
func (r *Runner) Stop(ctx context.Context) error {
	var failures []error
	for i := len(r.started) - 1; i >= 0; i-- {
		entry := r.started[i]
		teardowner, hasTeardown := entry.step.(Teardowner)
		if !hasTeardown {
			continue
		}
		if err := teardowner.Teardown(ctx); err != nil {
			failures = append(failures, fmt.Errorf("bbsdk/steps: teardown of step %q: %w", entry.spec.Alias, err))
		}
	}
	r.started = nil
	return errors.Join(failures...)
}

// Run executes one traversal: read input_key, run the stages in order, write output_key once on
// success and never on failure, and keep the status key current (rules 8, 8a).
func (r *Runner) Run(ctx context.Context, activationID string) error {
	entry, err := r.board.Get(ctx, r.bound.chain.InputKey)
	if err != nil {
		return fmt.Errorf("bbsdk/steps: reading the chain input key %q: %w", r.bound.chain.InputKey, err)
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

// branchResult is one branch's outcome: its output dict, or the failure its own budget ended on.
type branchResult struct {
	output map[string]any
	err    *StepError
}

// runStage runs one stage — a single step, or every branch of a parallel group concurrently — and
// performs every status write of it through one writer on this goroutine (rules 8, 8a).
func (r *Runner) runStage(
	ctx context.Context,
	stage []*boundStep,
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
			return nil, fmt.Errorf("bbsdk/steps: writing the chain output key %q: %w", r.bound.chain.OutputKey, err)
		}
	}
	writer.close(ctx)
	if writer.err != nil {
		return nil, writer.err
	}
	return output, nil
}

// runBranch runs one branch's whole per-step lifecycle — seam validation, its own timeout, its own
// retries with the fixed backoff, rule-3 normalization — emitting transitions instead of writing them.
func (r *Runner) runBranch(
	ctx context.Context,
	entry *boundStep,
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
			events <- transition{position: position, alias: alias, state: stateOK, durationMS: duration}
			return branchResult{output: output}
		}
		if mayRetry(failure) && attempt < entry.spec.Retries && r.wait(ctx) {
			attempt++
			continue
		}
		events <- transition{position: position, alias: alias, state: stateError, err: failure}
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
// step's own timeout around the call itself. duration_ms measures the successful call alone.
func (r *Runner) execute(
	ctx context.Context,
	entry *boundStep,
	input map[string]any,
) (map[string]any, int, *StepError) {
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

// outcome is one step call's return, carried off the goroutine the step ran on.
type outcome struct {
	output map[string]any
	err    *StepError
}

// dispatch performs the one call the binding names, bounded by the step's own timeout_s. The call
// runs on its own goroutine so an expired budget ends the attempt rather than waiting on a step that
// ignores its context, and a panic escaping Run there is recovered and normalized (rule T4).
func (r *Runner) dispatch(
	ctx context.Context,
	entry *boundStep,
	input map[string]any,
) (map[string]any, *StepError) {
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
		if entry.binding == bindingNATS {
			output, failure := invokeStep(callCtx, entry, input)
			done <- outcome{output: output, err: failure}
			return
		}
		output, err := entry.step.Run(callCtx, input)
		if err != nil {
			done <- outcome{err: normalize(alias, err)}
			return
		}
		done <- outcome{output: output}
	}()
	select {
	case result := <-done:
		if result.err != nil && expired(ctx, callCtx, entry) {
			// A failure returned after the step's own budget expired is that expiry, however the step
			// spelled it — the classification cannot depend on which of the two the scheduler saw first.
			return nil, timedOut(entry)
		}
		return result.output, result.err
	case <-callCtx.Done():
		if expired(ctx, callCtx, entry) {
			return nil, timedOut(entry)
		}
		return nil, stepError(CauseInternal, false, callCtx.Err(),
			"step %q did not finish before the traversal ended: %s", alias, callCtx.Err())
	}
}

// expired reports whether the step's own timeout_s ended the attempt, rather than the caller's context.
func expired(ctx, callCtx context.Context, entry *boundStep) bool {
	return entry.spec.TimeoutS > 0 && errors.Is(callCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
}

// timedOut is rule 8.3's verdict: an expiry is a retryable timeout StepError.
func timedOut(entry *boundStep) *StepError {
	return stepError(CauseTimeout, true, context.DeadlineExceeded,
		"step %q exceeded its %ds timeout", entry.spec.Alias, entry.spec.TimeoutS)
}

// mayRetry applies rule 6: a validation failure is never retried, whatever the budget says.
func mayRetry(failure *StepError) bool {
	return failure.Retryable && failure.Cause != CauseValidation
}

// jsonDict enforces rule 2's JSON-dict discipline at one seam and returns the bytes a declared seam
// schema validates, so an embedded step is held to exactly what a remote one would put on the wire.
func jsonDict(value map[string]any, alias, seam string) ([]byte, *StepError) {
	if value == nil {
		return nil, stepError(CauseInternal, false, nil, "step %q %s is not a dict", alias, seam)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, stepError(CauseInternal, false, err,
			"step %q %s is not JSON-serializable: %s", alias, seam, err)
	}
	return encoded, nil
}

// stageCapacity is the number of transitions a stage can produce: one running per attempt plus one
// terminal record per branch.
func stageCapacity(stage []*boundStep) int {
	capacity := 0
	for _, entry := range stage {
		capacity += entry.spec.Retries + 2
	}
	return capacity
}

// gather applies rule 14's IO mapping: a single-step stage produces its step's output dict, and a
// parallel stage produces the stage input under _input plus one member per branch, in declaration
// order — collision-free by construction, independent of completion order.
func gather(stage []*boundStep, input map[string]any, results []branchResult) map[string]any {
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

// stageFailure is the stage's verdict: a single step's own failure, or rule 8a's aggregate naming
// every failed branch with that branch's own cause.
func stageFailure(stage []*boundStep, results []branchResult, index int) *StepError {
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
		details = append(details, fmt.Sprintf("step %q (%s): %s", entry.spec.Alias, failure.Cause, failure.Message))
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
