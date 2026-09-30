package blackboard

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// EvalState is the state a precondition is evaluated against (§4 evaluate).
type EvalState struct {
	// Snapshot is the current key-value state.
	Snapshot map[string]any
	// Previous is the snapshot CHANGED compares against; nil means there is none, which
	// makes every CHANGED clause false (§4 rule 3).
	Previous map[string]any
	// Timestamps are per-key write times for OLDER_THAN; a key without one makes its
	// clause false (§4 rule 4).
	Timestamps map[string]time.Time
	// Now is the evaluation time; the zero value means the current UTC time.
	Now time.Time
}

// context resolves the evaluation inputs once, defaulting Now to the current UTC time.
func (s EvalState) context() evalContext {
	now := s.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return evalContext{
		snapshot:    s.Snapshot,
		previous:    s.Previous,
		hasPrevious: s.Previous != nil,
		timestamps:  s.Timestamps,
		now:         now,
	}
}

// ClauseVerdict is one top-level clause's verdict: its canonical DSL text, result, and a short detail.
type ClauseVerdict struct {
	// Clause is the operand's canonical DSL source text, re-serialized from the parsed
	// expression: re-parseable and precedence-preserving.
	Clause string
	// Result is the operand's own evaluation verdict against the snapshot.
	Result bool
	// Detail is a short human explanation of the result.
	Detail string
}

// Precondition is a parsed precondition expression, reusable across evaluations (§4 rule 1).
type Precondition struct {
	root node
}

// ParsePrecondition parses a precondition expression into a reusable Precondition.
func ParsePrecondition(expression string) (*Precondition, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, fmt.Errorf("%w: empty expression", ErrPreconditionParse)
	}
	tokens, err := tokenize(expression)
	if err != nil {
		return nil, err
	}
	root, err := (&parser{tokens: tokens}).parse()
	if err != nil {
		return nil, err
	}
	return &Precondition{root: root}, nil
}

// Evaluate reports whether the precondition holds against the given state; it is pure (§4 rule 5).
func (p *Precondition) Evaluate(state EvalState) bool {
	return p.root.eval(state.context())
}

// ReferencedKeys returns every KV key and key pattern the expression references, deduplicated and sorted (§4 rules 8, 11).
func (p *Precondition) ReferencedKeys() []string {
	acc := make(map[string]struct{})
	p.root.collectKeys(acc)
	return slices.Sorted(maps.Keys(acc))
}

// Explain returns one verdict per top-level AND/OR operand, in source order (§4 rule 9).
func (p *Precondition) Explain(state EvalState) []ClauseVerdict {
	ctx := state.context()
	operands := topLevelOperands(p.root)
	verdicts := make([]ClauseVerdict, 0, len(operands))
	for _, operand := range operands {
		result := operand.eval(ctx)
		clause := operand.unparse(precRoot)
		verdicts = append(verdicts, ClauseVerdict{
			Clause: clause,
			Result: result,
			Detail: clause + " → " + operand.reason(ctx, result),
		})
	}
	return verdicts
}
