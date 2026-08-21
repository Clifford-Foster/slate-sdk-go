package steps

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// terminalWriteBudget bounds the writer's detached terminal writes (rule T8). It is deliberately
// short: a traversal that has already been cancelled is draining, not working.
const terminalWriteBudget = 5 * time.Second

// transition is one branch's status transition, queued for its stage's single writer (rule 8a).
type transition struct {
	// position is the branch's declaration index, which fixes the order withheld errors are written in.
	position   int
	alias      string
	state      string
	durationMS int
	err        *StepError
	// components is the entry's folded per-slot call aggregate, present on a terminal record of an
	// entry that called components and nil everywhere else (steps_runtime.md §Status key).
	components map[string]any
}

// statusWriter is one stage's single status writer: running and ok records as it receives them, the
// error records withheld until the stage resolves and then written in declaration order (rule 8a).
type statusWriter struct {
	board        Board
	key          string
	activationID string
	index        int
	of           int
	// holdLastOK withholds the stage's last terminal ok so rule 8.6's output_key PUT precedes it.
	holdLastOK bool
	held       *transition
	failures   []transition
	// err keeps the first board failure; the traversal reports it once the stage has resolved.
	err error
}

// accept takes one transition: it is written now, or withheld until the stage resolves.
func (w *statusWriter) accept(ctx context.Context, event transition) {
	if event.state == stateError {
		// Withheld to stage end and written in declaration order, so a failed stage rests on an error
		// record however fast the failing branch was — the last-declared failure's.
		w.failures = append(w.failures, event)
		return
	}
	if event.state == stateOK && w.holdLastOK {
		// Which ok is the stage's last is only known once the stage resolves, so the writer lags the
		// ok stream by one and close flushes the survivor after the output_key PUT.
		previous := w.held
		w.held = &event
		if previous != nil {
			w.put(ctx, *previous)
		}
		return
	}
	w.put(ctx, event)
}

// close flushes everything withheld: the held ok first, then the error records in declaration order.
func (w *statusWriter) close(ctx context.Context) {
	if w.held != nil {
		held := *w.held
		w.held = nil
		w.put(ctx, held)
	}
	failures := w.failures
	w.failures = nil
	slices.SortStableFunc(failures, func(a, b transition) int { return a.position - b.position })
	for _, event := range failures {
		w.put(ctx, event)
	}
}

// put performs one status PUT. A terminal record is written under a context detached from the
// traversal's cancellation (rule T8), so a traversal cancelled mid-stage still leaves the status key
// on a terminal record rather than a stale running.
func (w *statusWriter) put(ctx context.Context, event transition) {
	writeCtx := ctx
	if event.state != stateRunning {
		detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalWriteBudget)
		defer cancel()
		writeCtx = detached
	}
	if _, err := w.board.Put(writeCtx, w.key, w.record(event)); err != nil && w.err == nil {
		w.err = fmt.Errorf("bbsdk/steps: writing the status key %q: %w", w.key, err)
	}
}

// record builds the status value: one dict per transition, overwritten in place (steps_runtime.md
// §Status key).
func (w *statusWriter) record(event transition) map[string]any {
	value := map[string]any{
		"step":          event.alias,
		"index":         w.index,
		"of":            w.of,
		"state":         event.state,
		"activation_id": w.activationID,
	}
	if event.state == stateOK {
		value["duration_ms"] = event.durationMS
	}
	if event.components != nil {
		// One member on a record that was going to be written anyway: however many calls a traversal
		// makes, it performs exactly the status writes the same chain performs without them.
		value["components"] = event.components
	}
	if event.err != nil {
		value["error"] = map[string]any{
			"message":   event.err.Message,
			"cause":     event.err.Cause,
			"retryable": event.err.Retryable,
		}
	}
	return value
}
