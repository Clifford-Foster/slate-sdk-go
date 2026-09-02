package harness

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
	"github.com/Clifford-Foster/slate-sdk-go/bbsdk/schema"
	"github.com/Clifford-Foster/slate-sdk-go/blackboard"
	"github.com/Clifford-Foster/slate-sdk-go/manifest"
)

// The Validation Verdict sources these gates produce (sidecar.md Part B §Schemas).
const (
	sourceWriteSchema     = "write_schema"
	sourceReadExpectation = "read_expectation"
	sourcePublishSchema   = "publish_schema"
	sourceSubscribeSchema = "subscribe_schema"
)

// ValidationSkip is one firing evaluation the strict-read gate declined to deliver (rule H20).
type ValidationSkip struct {
	// Snapshot is the watched-key state the declined evaluation was judged against.
	Snapshot map[string]map[string]any
	// ChangedKeys is the sorted set of watched keys that triggered the declined evaluation.
	ChangedKeys []string
	// Verdicts carries one verdict per non-conforming snapshot key, in occurrence order.
	Verdicts []bbsdk.ValidationVerdict
}

// ValidationSkips returns the firing evaluations the strict-read gate declined, in occurrence
// order — the harness's "why didn't it fire" surface, never silent (rule H20).
func (h *Harness) ValidationSkips() []ValidationSkip {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.skips)
}

// declaredShape is one prepared declaration: the pattern it is attached to and its compiled document.
type declaredShape struct {
	pattern string
	shape   *schema.HarnessSchema
}

// shapes are the manifest's declared shapes, prepared once at construction (rules H20, H21).
type shapes struct {
	writes []declaredShape
	// strictReads carries only the expectations marked strict; a strict:false entry is prepared like
	// any other — a broken document fails construction either way — but never gates.
	strictReads []declaredShape
	// publishes are the publishes entries' own schemas, judged on a returned or published name.
	publishes []declaredShape
	// strictSubscribes are the strict subscriptions' schemas, keyed by the declared subject; a
	// strict:false subscription's schema is prepared like any other but never gates (rule H21).
	strictSubscribes map[string]*schema.HarnessSchema
}

// prepareShapes prepares every write_schemas and read_expectations document the manifest declares,
// resolving a path-form source against dir. Every failure is a construction failure naming the
// source: the sidecar's rule-A31 startup-refusal posture, fail-at-load (rule H20).
func prepareShapes(m *manifest.Manifest, dir string) (*shapes, error) {
	prepared := &shapes{strictSubscribes: map[string]*schema.HarnessSchema{}}
	// The subject side is prepared on the same fail-at-load terms as the board side (rule H21).
	for _, entry := range m.Subscribes {
		if entry.Schema == nil {
			continue
		}
		shape, err := prepareShape(entry.Schema, dir, "subscribes["+entry.Subject+"]")
		if err != nil {
			return nil, err
		}
		if entry.Strict {
			prepared.strictSubscribes[entry.Subject] = shape
		}
	}
	for _, entry := range m.Publishes {
		if entry.Schema == nil {
			continue
		}
		shape, err := prepareShape(entry.Schema, dir, "publishes["+entry.Subject+"]")
		if err != nil {
			return nil, err
		}
		prepared.publishes = append(prepared.publishes, declaredShape{pattern: entry.Subject, shape: shape})
	}
	for _, pattern := range slices.Sorted(maps.Keys(m.WriteSchemas)) {
		shape, err := prepareShape(m.WriteSchemas[pattern], dir, "write_schemas["+pattern+"]")
		if err != nil {
			return nil, err
		}
		prepared.writes = append(prepared.writes, declaredShape{pattern: pattern, shape: shape})
	}
	for _, pattern := range slices.Sorted(maps.Keys(m.ReadExpectations)) {
		expectation := m.ReadExpectations[pattern]
		shape, err := prepareShape(expectation.Schema, dir, "read_expectations["+pattern+"]")
		if err != nil {
			return nil, err
		}
		if expectation.Strict {
			prepared.strictReads = append(prepared.strictReads, declaredShape{pattern: pattern, shape: shape})
		}
	}
	// A manifest declaring no shapes prepares nothing, validates nothing, and behaves exactly as it
	// did before this rule existed.
	return prepared, nil
}

// prepareShape resolves and compiles one declared source: an inline document directly, a path-form
// source only where a directory to resolve against exists — which is Open's, never New's, since
// rule H16 forbids this harness a read New was given no path for (rule H20).
func prepareShape(source any, dir, declared string) (*schema.HarnessSchema, error) {
	document, err := resolveSource(source, dir, declared)
	if err != nil {
		return nil, err
	}
	shape, err := schema.HarnessPrepare(document)
	if err != nil {
		return nil, fmt.Errorf("harness: the schema source %s: %w", describeSource(source, declared), err)
	}
	return shape, nil
}

// describeSource names one source the way a construction failure must: the declaration it is
// attached to, and the file it came from where it came from one (rule H20).
func describeSource(source any, declared string) string {
	if path, isPath := source.(string); isPath {
		return fmt.Sprintf("%s (%q)", declared, path)
	}
	return declared
}

// resolveSource reads one declared schema source into a document.
func resolveSource(source any, dir, declared string) (any, error) {
	switch typed := source.(type) {
	case map[string]any:
		return typed, nil
	case string:
		if dir == "" {
			return nil, fmt.Errorf(
				"harness: the schema source %s is the path %q, and New has no manifest directory to resolve it against — use Open, or declare the document inline",
				declared, typed)
		}
		data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(typed)))
		if err != nil {
			return nil, fmt.Errorf("harness: the schema source %s (%q) could not be read: %w", declared, typed, err)
		}
		var document any
		if err := json.Unmarshal(data, &document); err != nil {
			return nil, fmt.Errorf("harness: the schema source %s (%q) is not valid JSON: %w", declared, typed, err)
		}
		return document, nil
	default:
		return nil, fmt.Errorf("harness: the schema source %s is neither an inline document nor a path", declared)
	}
}

// writeVerdicts judges one component write against every declared write schema whose pattern matches
// the key — a key matching several declared patterns must satisfy all of them, composition only
// narrowing — and returns one Verdict per failed schema (rule H20).
func (s *shapes) writeVerdicts(key string, value map[string]any) []bbsdk.ValidationVerdict {
	if s == nil {
		return nil
	}
	var verdicts []bbsdk.ValidationVerdict
	for _, declared := range s.writes {
		if !writeMatches(key, declared.pattern) {
			continue
		}
		if unsatisfied := judge(declared.shape, value); len(unsatisfied) > 0 {
			verdicts = append(verdicts, bbsdk.ValidationVerdict{
				Source: sourceWriteSchema, Pattern: declared.pattern, Key: key, Unsatisfied: unsatisfied,
			})
		}
	}
	return verdicts
}

// publishVerdicts judges one outbound name against the schema its publishes entry declares, when it
// names one; a name that is no declared publishes subject is judged by nothing here (rule H21).
func (s *shapes) publishVerdicts(subject string, value map[string]any) []bbsdk.ValidationVerdict {
	if s == nil {
		return nil
	}
	var verdicts []bbsdk.ValidationVerdict
	for _, declared := range s.publishes {
		if declared.pattern != subject {
			continue
		}
		if unsatisfied := judge(declared.shape, value); len(unsatisfied) > 0 {
			verdicts = append(verdicts, bbsdk.ValidationVerdict{
				Source: sourcePublishSchema, Pattern: declared.pattern, Key: subject, Unsatisfied: unsatisfied,
			})
		}
	}
	return verdicts
}

// subscribeVerdicts judges one received payload against a strict subscription's declared schema; a
// strict:false or schema-less entry never gates. Pattern is the declared subject and Key the concrete
// one, the shape sidecar.md rule B38 assigns this verdict (rule H21).
func (s *shapes) subscribeVerdicts(
	entry manifest.Subscription, subject string, payload map[string]any,
) []bbsdk.ValidationVerdict {
	if s == nil {
		return nil
	}
	shape, gated := s.strictSubscribes[entry.Subject]
	if !gated {
		return nil
	}
	unsatisfied := judge(shape, payload)
	if len(unsatisfied) == 0 {
		return nil
	}
	return []bbsdk.ValidationVerdict{{
		Source: sourceSubscribeSchema, Pattern: entry.Subject, Key: subject, Unsatisfied: unsatisfied,
	}}
}

// readVerdicts judges the whole snapshot against every strict expectation: every key present that
// matches a strict pattern, in deterministic order. An absent key is never a violation — presence is
// the precondition's concern, and this gate judges only values that are there (rule H20).
func (s *shapes) readVerdicts(snapshot map[string]map[string]any) []bbsdk.ValidationVerdict {
	if s == nil || len(s.strictReads) == 0 {
		return nil
	}
	keys := slices.Sorted(maps.Keys(snapshot))
	var verdicts []bbsdk.ValidationVerdict
	for _, declared := range s.strictReads {
		for _, key := range keys {
			value := snapshot[key]
			if value == nil || !matchesPattern(key, declared.pattern) {
				continue
			}
			if unsatisfied := judge(declared.shape, value); len(unsatisfied) > 0 {
				verdicts = append(verdicts, bbsdk.ValidationVerdict{
					Source: sourceReadExpectation, Pattern: declared.pattern, Key: key, Unsatisfied: unsatisfied,
				})
			}
		}
	}
	return verdicts
}

// judge names what one value fails. A value that will not encode as JSON is left unjudged rather
// than reported as a shape failure: the schema stage runs on the parsed value, so the core's own
// value contract is what refuses it, exactly as the boundary's stage order has it.
func judge(shape *schema.HarnessSchema, value map[string]any) []string {
	unsatisfied, err := shape.HarnessUnsatisfied(withoutMeta(value))
	if err != nil {
		return nil
	}
	return unsatisfied
}

// withoutMeta is what a declared schema judges: the value with the reserved _meta member excluded, on
// a copy so the stored value is never mutated. A declared shape describes component payload, never
// the platform envelope, so neither the correlation stamp nor a component-carried _meta override can
// trip it (rule H20, mirroring sidecar.md rule A31's exclusion).
func withoutMeta(value map[string]any) map[string]any {
	if _, carried := value[blackboard.MetaField]; !carried {
		return value
	}
	judged := maps.Clone(value)
	delete(judged, blackboard.MetaField)
	return judged
}

// schemaViolation renders the verdicts as the same SCHEMA_VIOLATION value the data plane returns, so
// a component discriminates a shape failure with errors.Is and reads the members off .Validation
// without parsing a message (rules E8, H20).
func schemaViolation(what string, verdicts []bbsdk.ValidationVerdict) error {
	named := make([]string, 0, len(verdicts))
	for _, verdict := range verdicts {
		named = append(named, fmt.Sprintf("%s against %q: %s",
			verdict.Key, verdict.Pattern, strings.Join(verdict.Unsatisfied, ", ")))
	}
	return bbsdk.HarnessSchemaViolation(
		fmt.Sprintf("%s violates the component's own declared write schema — %s", what, strings.Join(named, "; ")),
		verdicts)
}
