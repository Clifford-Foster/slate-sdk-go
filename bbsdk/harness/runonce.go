package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"testing"
	"time"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
	"github.com/Clifford-Foster/slate-sdk-go/blackboard"
)

// runSpecEnv names the spec file the CLI writes; without it the runner test skips (rule H19).
const runSpecEnv = "BB_RUN_SPEC"

// runSentinel is the handshake line the CLI finds the report behind (rule H19).
const runSentinel = "BB_RUN_REPORT/1"

// runSettleTimeout bounds one run's settle; what completed is reported either way (rule H19).
const runSettleTimeout = 5 * time.Second

// runSpec is the CLI's spec document: the manifest to open, the state to seed, the trigger or none,
// and the instance configuration (rule H19).
type runSpec struct {
	Manifest string                    `json:"manifest"`
	Snapshot map[string]map[string]any `json:"snapshot"`
	Trigger  *runTrigger               `json:"trigger"`
	Config   map[string]any            `json:"config"`
}

// runTrigger is the one key and value the run puts after start (rule H19).
type runTrigger struct {
	Key     string         `json:"key"`
	Payload map[string]any `json:"payload"`
}

// reportClause is one top-level clause verdict, the core evaluator's own (rule H19).
type reportClause struct {
	Clause string `json:"clause"`
	Result bool   `json:"result"`
	Detail string `json:"detail"`
}

// reportWrite is one applied write; a null value records a delete (rule H19).
type reportWrite struct {
	Key   string         `json:"key"`
	Value map[string]any `json:"value"`
}

// reportActivation is one completed activation as the CLI renders it (rule H19).
type reportActivation struct {
	ID       string        `json:"id"`
	Writes   []reportWrite `json:"writes"`
	Consumed []string      `json:"consumed"`
	Err      *string       `json:"err"`
	AppError *string       `json:"app_error"`
}

// RunOnceFromEnv executes the `bb run --once` spec named by BB_RUN_SPEC and reports it on stdout (rule H19).
func RunOnceFromEnv(t *testing.T, component bbsdk.ActivationFunc) {
	t.Helper()
	path := os.Getenv(runSpecEnv)
	if path == "" {
		// Inert outside the CLI seam: a plain `go test ./...` never executes the run (rule H19).
		t.Skipf("%s is unset: this runner test executes only under `bb run --once`", runSpecEnv)
		return
	}
	spec, err := readRunSpec(path)
	if err != nil {
		t.Fatalf("bb run --once: %v", err)
	}
	// The manifest is the CLI's generated document; the harness validates none of its own, so a
	// document rejected here is the one the platform would reject (rules H1, H19).
	h, err := Open(spec.Manifest, component, Options{Config: spec.Config})
	if err != nil {
		t.Fatalf("bb run --once: the manifest at %s was rejected: %v", spec.Manifest, err)
	}
	for _, key := range slices.Sorted(maps.Keys(spec.Snapshot)) {
		if err := h.Seed(key, spec.Snapshot[key]); err != nil {
			t.Fatalf("bb run --once: seeding %q from the workspace snapshot: %v", key, err)
		}
	}
	report := map[string]any{}
	if spec.Trigger == nil {
		probeRun(t, h, spec, report)
	} else {
		triggerRun(t, h, spec, report)
	}
	emitRunReport(t, report)
}

// readRunSpec reads and decodes the spec file; an unreadable or unparseable one names its reason (rule H19).
func readRunSpec(path string) (runSpec, error) {
	document, err := os.ReadFile(path) //nolint:gosec // the path is the CLI's own spec file, named in the environment
	if err != nil {
		return runSpec{}, fmt.Errorf("reading the %s spec: %w", runSpecEnv, err)
	}
	var spec runSpec
	if err := json.Unmarshal(document, &spec); err != nil {
		return runSpec{}, fmt.Errorf("the %s spec at %s does not parse: %w", runSpecEnv, path, err)
	}
	if spec.Manifest == "" {
		return runSpec{}, fmt.Errorf("the %s spec at %s names no manifest", runSpecEnv, path)
	}
	return spec, nil
}

// probeRun fills the probe arm's report: the component never runs, and the precondition is evaluated
// against the bare snapshot with previous={} — the engine's own first evaluation (rule H19).
func probeRun(t *testing.T, h *Harness, spec runSpec, report map[string]any) {
	t.Helper()
	verdict, clauses := explainRun(t, h.loaded.Precondition, spec.Snapshot)
	report["verdict"], report["clauses"] = verdict, clauses
	report["watched"] = watchedPatterns(h.loaded.Reads)
	report["activations"] = []reportActivation{}
	report["duration_ms"] = 0.0
}

// triggerRun drives one trigger through the harness and fills its report: every resulting activation,
// cascades included, and the trigger-to-settle span (rule H19).
func triggerRun(t *testing.T, h *Harness, spec runSpec, report map[string]any) {
	t.Helper()
	ctx := context.Background()
	if err := h.Start(ctx); err != nil {
		t.Fatalf("bb run --once: starting the harness: %v", err)
	}
	started := time.Now()
	if _, err := h.Put(ctx, spec.Trigger.Key, spec.Trigger.Payload); err != nil {
		t.Fatalf("bb run --once: putting the trigger %q: %v (stopping the harness: %v)",
			spec.Trigger.Key, err, h.Stop(ctx))
	}
	settle, cancel := context.WithTimeout(ctx, runSettleTimeout)
	// A settle that runs out of time reports what completed, exactly as the Python arm does; the
	// deadline is the only error it returns (rule H11).
	if err := h.Settle(settle); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("bb run --once: settling the run: %v", err)
	}
	cancel()
	elapsed := time.Since(started)
	if err := h.Stop(ctx); err != nil {
		t.Fatalf("bb run --once: stopping the harness: %v", err)
	}
	records := h.Activations()
	// The rendered verdict is the engine's own: an activation's read-scoped snapshot where there was
	// one, and the post-trigger snapshot where nothing fired (`cli.md` rule 10's scoping note).
	evaluated := triggerSnapshot(spec)
	if len(records) > 0 {
		evaluated = records[0].Snapshot
	}
	verdict, clauses := explainRun(t, h.loaded.Precondition, evaluated)
	report["verdict"], report["clauses"] = verdict, clauses
	report["activations"] = reportActivations(records)
	report["duration_ms"] = float64(elapsed.Microseconds()) / 1000
}

// triggerSnapshot is the state the trigger leaves behind, which is what a run that never fired is
// explained against.
func triggerSnapshot(spec runSpec) map[string]map[string]any {
	snapshot := make(map[string]map[string]any, len(spec.Snapshot)+1)
	maps.Copy(snapshot, spec.Snapshot)
	snapshot[spec.Trigger.Key] = spec.Trigger.Payload
	return snapshot
}

// explainRun evaluates the precondition through the core evaluator with previous={} — first-evaluation
// parity with the engine — and returns its verdict and top-level clause list (rule H19).
func explainRun(t *testing.T, precondition string, snapshot map[string]map[string]any) (bool, []reportClause) {
	t.Helper()
	clauses := []reportClause{}
	if precondition == "" {
		// No precondition: any change to a watched key activates (blackboard_platform.md §3.4).
		return true, clauses
	}
	parsed, err := blackboard.ParsePrecondition(precondition)
	if err != nil {
		t.Fatalf("bb run --once: the manifest's precondition does not parse: %v", err)
	}
	state := blackboard.EvalState{Snapshot: evalSnapshot(snapshot), Previous: map[string]any{}}
	for _, clause := range parsed.Explain(state) {
		clauses = append(clauses, reportClause{Clause: clause.Clause, Result: clause.Result, Detail: clause.Detail})
	}
	return parsed.Evaluate(state), clauses
}

// evalSnapshot is the evaluator's view of a snapshot: every value is a JSON object (rule K1).
func evalSnapshot(snapshot map[string]map[string]any) map[string]any {
	state := make(map[string]any, len(snapshot))
	for key, value := range snapshot {
		state[key] = value
	}
	return state
}

// watchedPatterns is the manifest's watched patterns, always an array.
func watchedPatterns(reads []string) []string {
	if reads == nil {
		return []string{}
	}
	return slices.Clone(reads)
}

// reportActivations is every completed activation in completion order, cascades included (rule H19).
func reportActivations(records []Record) []reportActivation {
	activations := make([]reportActivation, 0, len(records))
	for _, record := range records {
		writes := make([]reportWrite, 0, len(record.Writes))
		for _, write := range record.Writes {
			// A nil value records a delete, which the report carries as a null (rule H19).
			writes = append(writes, reportWrite{Key: write.Key, Value: write.Value})
		}
		consumed := record.Consumed
		if consumed == nil {
			consumed = []string{}
		}
		activations = append(activations, reportActivation{
			ID:       record.ActivationID,
			Writes:   writes,
			Consumed: consumed,
			Err:      recordErrText(record.Err),
			AppError: appErrorText(record.AppError),
		})
	}
	return activations
}

// recordErrText is the activation's error verbatim, or nil (rule H19).
func recordErrText(err error) *string {
	if err == nil {
		return nil
	}
	text := err.Error()
	return &text
}

// appErrorText is the declared application failure as its own envelope, or nil (rule H19).
func appErrorText(failure *bbsdk.AppError) *string {
	if failure == nil {
		return nil
	}
	encoded, err := json.Marshal(failure)
	if err != nil {
		text := failure.Code + ": " + failure.Message
		return &text
	}
	text := string(encoded)
	return &text
}

// emitRunReport writes the sentinel line and the one report object to stdout — the seam's whole
// surface, so framework noise around them is inert by construction (rule H19).
func emitRunReport(t *testing.T, report map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("bb run --once: encoding the report: %v", err)
	}
	if _, err := fmt.Fprintf(os.Stdout, "%s\n%s\n", runSentinel, encoded); err != nil {
		t.Fatalf("bb run --once: writing the report: %v", err)
	}
}
