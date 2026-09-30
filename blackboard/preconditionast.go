package blackboard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Operator precedence, lowest to highest, used to parenthesize re-serialized clause text (§4 Grammar).
const (
	precRoot = iota
	precOr
	precAnd
	precNot
)

// detailMaxRunes bounds a value rendered into a clause detail, keeping the explanation short (§4 explain).
const detailMaxRunes = 40

// evalContext is one evaluation's resolved inputs: the current snapshot plus the optional
// previous snapshot, key timestamps, and evaluation time.
type evalContext struct {
	snapshot map[string]any
	previous map[string]any
	// hasPrevious distinguishes "no previous snapshot" (CHANGED is always false, §4 rule 3)
	// from an empty one (every key resolves as absent).
	hasPrevious bool
	timestamps  map[string]time.Time
	now         time.Time
}

// node is one parsed expression node: it evaluates, reports its keys, and re-serializes itself.
type node interface {
	eval(ctx evalContext) bool
	collectKeys(acc map[string]struct{})
	unparse(parentPrec int) string
	reason(ctx evalContext, result bool) string
}

// ref is a precondition reference: a whole key, or a dotted field path into that key's value.
type ref struct {
	key string
	// field is the dotted path into the value, empty when the reference is the bare key.
	field string
}

// resolve looks the key up in a snapshot, then walks the dotted field path (§4 rule 6).
func (r ref) resolve(snapshot map[string]any) (any, bool) {
	value, present := snapshot[r.key]
	if !present {
		return nil, false
	}
	if r.field == "" {
		return value, true
	}
	for _, part := range strings.Split(r.field, ".") {
		object, isObject := value.(map[string]any)
		if !isObject {
			return nil, false
		}
		if value, present = object[part]; !present {
			return nil, false
		}
	}
	return value, true
}

// text renders the reference back to its DSL source form.
func (r ref) text() string {
	if r.field == "" {
		return r.key
	}
	return r.key + ":" + r.field
}

// absent names which half of a reference failed to resolve, as far as the source text tells.
func (r ref) absent() string {
	if r.field != "" {
		return "field absent"
	}
	return "key absent"
}

type eqNode struct {
	ref     ref
	literal string
}

func (n eqNode) eval(ctx evalContext) bool {
	value, present := n.ref.resolve(ctx.snapshot)
	return present && equalsLiteral(value, n.literal)
}

func (n eqNode) collectKeys(acc map[string]struct{}) { acc[n.ref.key] = struct{}{} }

func (n eqNode) unparse(int) string { return n.ref.text() + " == " + quoteLiteral(n.literal) }

func (n eqNode) reason(ctx evalContext, _ bool) string {
	value, present := n.ref.resolve(ctx.snapshot)
	switch {
	case !present:
		return n.ref.absent()
	case equalsLiteral(value, n.literal):
		return "equals " + quoteLiteral(n.literal)
	default:
		return "is " + shortValue(value, true) + ", not " + quoteLiteral(n.literal)
	}
}

type neqNode struct {
	ref     ref
	literal string
}

func (n neqNode) eval(ctx evalContext) bool {
	value, present := n.ref.resolve(ctx.snapshot)
	return !present || !equalsLiteral(value, n.literal)
}

func (n neqNode) collectKeys(acc map[string]struct{}) { acc[n.ref.key] = struct{}{} }

func (n neqNode) unparse(int) string { return n.ref.text() + " != " + quoteLiteral(n.literal) }

func (n neqNode) reason(ctx evalContext, _ bool) string {
	value, present := n.ref.resolve(ctx.snapshot)
	switch {
	case !present:
		return n.ref.absent() + " (unset ≠ literal)"
	case equalsLiteral(value, n.literal):
		return "equals " + quoteLiteral(n.literal)
	default:
		return "is " + shortValue(value, true)
	}
}

type isSetNode struct {
	ref ref
}

func (n isSetNode) eval(ctx evalContext) bool {
	value, present := n.ref.resolve(ctx.snapshot)
	return present && value != nil
}

func (n isSetNode) collectKeys(acc map[string]struct{}) { acc[n.ref.key] = struct{}{} }

func (n isSetNode) unparse(int) string { return n.ref.text() + " IS SET" }

func (n isSetNode) reason(ctx evalContext, _ bool) string {
	value, present := n.ref.resolve(ctx.snapshot)
	switch {
	case !present:
		return n.ref.absent()
	case value == nil:
		return "is null"
	default:
		return "is set"
	}
}

type isNotSetNode struct {
	ref ref
}

func (n isNotSetNode) eval(ctx evalContext) bool {
	value, present := n.ref.resolve(ctx.snapshot)
	return !present || value == nil
}

func (n isNotSetNode) collectKeys(acc map[string]struct{}) { acc[n.ref.key] = struct{}{} }

func (n isNotSetNode) unparse(int) string { return n.ref.text() + " IS NOT SET" }

func (n isNotSetNode) reason(ctx evalContext, _ bool) string {
	value, present := n.ref.resolve(ctx.snapshot)
	switch {
	case !present:
		return n.ref.absent()
	case value == nil:
		return "is null"
	default:
		return "is set (" + shortValue(value, true) + ")"
	}
}

type changedNode struct {
	ref ref
}

func (n changedNode) eval(ctx evalContext) bool {
	if !ctx.hasPrevious {
		return false
	}
	current, currentPresent := n.ref.resolve(ctx.snapshot)
	prior, priorPresent := n.ref.resolve(ctx.previous)
	if currentPresent != priorPresent {
		return true
	}
	return currentPresent && !reflect.DeepEqual(current, prior)
}

func (n changedNode) collectKeys(acc map[string]struct{}) { acc[n.ref.key] = struct{}{} }

func (n changedNode) unparse(int) string { return n.ref.text() + " CHANGED" }

func (n changedNode) reason(ctx evalContext, result bool) string {
	if !ctx.hasPrevious {
		return "no previous snapshot"
	}
	current, currentPresent := n.ref.resolve(ctx.snapshot)
	prior, priorPresent := n.ref.resolve(ctx.previous)
	if result {
		return "changed (" + shortValue(prior, priorPresent) + " → " + shortValue(current, currentPresent) + ")"
	}
	return "unchanged (" + shortValue(current, currentPresent) + ")"
}

// keyPattern is a pattern atom's operand, a key family in the read-pattern grammar (§4 rule 10).
type keyPattern string

// matches reports whether a key is in the family: "*" exactly one token, a final ">" one or more (§1 watch).
func (k keyPattern) matches(key string) bool {
	keyTokens := strings.Split(key, ".")
	patternTokens := strings.Split(string(k), ".")
	for i, part := range patternTokens {
		if part == ">" {
			return i < len(keyTokens)
		}
		if i >= len(keyTokens) {
			return false
		}
		if part != "*" && part != keyTokens[i] {
			return false
		}
	}
	return len(keyTokens) == len(patternTokens)
}

// matching returns the snapshot's keys the pattern matches, sorted.
func (k keyPattern) matching(snapshot map[string]any) []string {
	keys := []string{}
	for key := range snapshot {
		if k.matches(key) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// setMatches returns the matching keys whose value is set — present and non-None (§4 rule 2).
func (k keyPattern) setMatches(snapshot map[string]any) []string {
	keys := []string{}
	for _, key := range k.matching(snapshot) {
		if snapshot[key] != nil {
			keys = append(keys, key)
		}
	}
	return keys
}

// changedMatches returns the matching keys added, deleted or rewritten between the previous snapshot and this one.
func (k keyPattern) changedMatches(ctx evalContext) []string {
	keys := []string{}
	for _, key := range k.matching(ctx.snapshot) {
		prior, priorPresent := ctx.previous[key]
		if !priorPresent || !reflect.DeepEqual(ctx.snapshot[key], prior) {
			keys = append(keys, key)
		}
	}
	for _, key := range k.matching(ctx.previous) {
		if _, present := ctx.snapshot[key]; !present {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// setReason names a pattern's set matching keys, or the unset ones when none is set (§4 rule 12).
func (k keyPattern) setReason(ctx evalContext) string {
	if set := k.setMatches(ctx.snapshot); len(set) > 0 {
		return namedKeys(set, "set")
	}
	if matching := k.matching(ctx.snapshot); len(matching) > 0 {
		return namedKeys(matching, "null")
	}
	return "no matching key"
}

type patternIsSetNode struct {
	pattern keyPattern
}

func (n patternIsSetNode) eval(ctx evalContext) bool {
	return len(n.pattern.setMatches(ctx.snapshot)) > 0
}

func (n patternIsSetNode) collectKeys(acc map[string]struct{}) { acc[string(n.pattern)] = struct{}{} }

func (n patternIsSetNode) unparse(int) string { return string(n.pattern) + " IS SET" }

func (n patternIsSetNode) reason(ctx evalContext, _ bool) string { return n.pattern.setReason(ctx) }

type patternIsNotSetNode struct {
	pattern keyPattern
}

func (n patternIsNotSetNode) eval(ctx evalContext) bool {
	return len(n.pattern.setMatches(ctx.snapshot)) == 0
}

func (n patternIsNotSetNode) collectKeys(acc map[string]struct{}) {
	acc[string(n.pattern)] = struct{}{}
}

func (n patternIsNotSetNode) unparse(int) string { return string(n.pattern) + " IS NOT SET" }

func (n patternIsNotSetNode) reason(ctx evalContext, _ bool) string { return n.pattern.setReason(ctx) }

type patternChangedNode struct {
	pattern keyPattern
}

func (n patternChangedNode) eval(ctx evalContext) bool {
	return ctx.hasPrevious && len(n.pattern.changedMatches(ctx)) > 0
}

func (n patternChangedNode) collectKeys(acc map[string]struct{}) { acc[string(n.pattern)] = struct{}{} }

func (n patternChangedNode) unparse(int) string { return string(n.pattern) + " CHANGED" }

func (n patternChangedNode) reason(ctx evalContext, _ bool) string {
	if !ctx.hasPrevious {
		return "no previous snapshot"
	}
	if changed := n.pattern.changedMatches(ctx); len(changed) > 0 {
		return namedKeys(changed, "changed")
	}
	if matching := n.pattern.matching(ctx.snapshot); len(matching) > 0 {
		return namedKeys(matching, "unchanged")
	}
	return "no matching key, unchanged"
}

// patternDetailKeys is how many matching keys a pattern atom's detail names before eliding the rest (§4 rule 12).
const patternDetailKeys = 3

// namedKeys renders a count of matching keys, their state, and the first few of them, e.g. "2 matching keys set (a, b)".
func namedKeys(keys []string, state string) string {
	noun := "matching keys"
	if len(keys) == 1 {
		noun = "matching key"
	}
	named := strings.Join(keys[:min(len(keys), patternDetailKeys)], ", ")
	if len(keys) > patternDetailKeys {
		named += ", …"
	}
	return fmt.Sprintf("%d %s %s (%s)", len(keys), noun, state, named)
}

type olderThanNode struct {
	key string
	// seconds is the parsed duration, in whole seconds as the grammar writes it.
	seconds int64
}

func (n olderThanNode) eval(ctx evalContext) bool {
	timestamp, present := ctx.timestamps[n.key]
	if !present {
		return false
	}
	return ctx.now.Sub(timestamp).Seconds() > float64(n.seconds)
}

func (n olderThanNode) collectKeys(acc map[string]struct{}) { acc[n.key] = struct{}{} }

func (n olderThanNode) unparse(int) string {
	return n.key + " OLDER_THAN " + formatDuration(n.seconds)
}

func (n olderThanNode) reason(ctx evalContext, result bool) string {
	timestamp, present := ctx.timestamps[n.key]
	if !present {
		return "no timestamp"
	}
	age := ctx.now.Sub(timestamp).Seconds()
	comparison := " ≤ "
	if result {
		comparison = " > "
	}
	return fmt.Sprintf("age %.0fs%s%.0fs", age, comparison, float64(n.seconds))
}

type andNode struct {
	left  node
	right node
}

func (n andNode) eval(ctx evalContext) bool {
	return n.left.eval(ctx) && n.right.eval(ctx)
}

func (n andNode) collectKeys(acc map[string]struct{}) {
	n.left.collectKeys(acc)
	n.right.collectKeys(acc)
}

func (n andNode) unparse(parentPrec int) string {
	return wrapClause(n.left.unparse(precAnd)+" AND "+n.right.unparse(precAnd), precAnd, parentPrec)
}

func (n andNode) reason(_ evalContext, result bool) string { return subExpressionReason(result) }

type orNode struct {
	left  node
	right node
}

func (n orNode) eval(ctx evalContext) bool {
	return n.left.eval(ctx) || n.right.eval(ctx)
}

func (n orNode) collectKeys(acc map[string]struct{}) {
	n.left.collectKeys(acc)
	n.right.collectKeys(acc)
}

func (n orNode) unparse(parentPrec int) string {
	return wrapClause(n.left.unparse(precOr)+" OR "+n.right.unparse(precOr), precOr, parentPrec)
}

func (n orNode) reason(_ evalContext, result bool) string { return subExpressionReason(result) }

type notNode struct {
	operand node
}

func (n notNode) eval(ctx evalContext) bool { return !n.operand.eval(ctx) }

func (n notNode) collectKeys(acc map[string]struct{}) { n.operand.collectKeys(acc) }

func (n notNode) unparse(parentPrec int) string {
	return wrapClause("NOT "+n.operand.unparse(precNot), precNot, parentPrec)
}

func (n notNode) reason(_ evalContext, result bool) string { return subExpressionReason(result) }

// topLevelOperands splits a root into its top-level AND/OR operands; any other root is one operand (§4 rule 9).
func topLevelOperands(root node) []node {
	switch typed := root.(type) {
	case andNode:
		return flattenAnd(typed, nil)
	case orNode:
		return flattenOr(typed, nil)
	default:
		return []node{root}
	}
}

// flattenAnd appends the conjuncts of a left-nested AND chain in source order.
func flattenAnd(n andNode, acc []node) []node {
	for _, operand := range []node{n.left, n.right} {
		if nested, isAnd := operand.(andNode); isAnd {
			acc = flattenAnd(nested, acc)
			continue
		}
		acc = append(acc, operand)
	}
	return acc
}

// flattenOr appends the disjuncts of a left-nested OR chain in source order.
func flattenOr(n orNode, acc []node) []node {
	for _, operand := range []node{n.left, n.right} {
		if nested, isOr := operand.(orNode); isOr {
			acc = flattenOr(nested, acc)
			continue
		}
		acc = append(acc, operand)
	}
	return acc
}

// equalsLiteral reports whether a resolved value equals a string literal; a non-string never does (§4 rule 7).
func equalsLiteral(value any, literal string) bool {
	text, isString := value.(string)
	return isString && text == literal
}

// quoteLiteral renders a string literal in DSL quoting, keeping re-serialized clause text re-parseable.
func quoteLiteral(literal string) string {
	if strings.Contains(literal, `"`) {
		return "'" + literal + "'"
	}
	return `"` + literal + `"`
}

// formatDuration renders whole seconds in the largest exact unit the grammar spells.
func formatDuration(seconds int64) string {
	switch {
	case seconds != 0 && seconds%3600 == 0:
		return strconv.FormatInt(seconds/3600, 10) + "h"
	case seconds != 0 && seconds%60 == 0:
		return strconv.FormatInt(seconds/60, 10) + "m"
	default:
		return strconv.FormatInt(seconds, 10) + "s"
	}
}

// wrapClause parenthesizes a sub-expression only where the parent binds tighter than it does.
func wrapClause(text string, prec, parentPrec int) string {
	if prec < parentPrec {
		return "(" + text + ")"
	}
	return text
}

// subExpressionReason is the detail a compound operand carries: explain does not recurse into it (§4 rule 9).
func subExpressionReason(result bool) string {
	if result {
		return "sub-expression true"
	}
	return "sub-expression false"
}

// shortValue renders a resolved value for a clause detail, truncated to keep the explanation short.
func shortValue(value any, present bool) string {
	if !present {
		return "absent"
	}
	text := compactJSON(value)
	if utf8.RuneCountInString(text) <= detailMaxRunes {
		return text
	}
	return string([]rune(text)[:detailMaxRunes-1]) + "…"
}

// compactJSON renders a value as compact JSON, falling back to Go formatting for the unserializable.
func compactJSON(value any) string {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Sprintf("%v", value)
	}
	return strings.TrimSuffix(buffer.String(), "\n")
}
