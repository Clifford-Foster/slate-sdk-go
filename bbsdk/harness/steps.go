// Contract: contracts/bb_sdk_go.md — rules H17 and H18, the step and chain drivers.
//
// Both drive the real bbsdk/steps runner over an in-memory board: setup, seam validation, rule-3
// normalization, the rule-8 and rule-8a traversal — including a parallel stage's real goroutine
// concurrency — all come from the runner itself, never from a driver-local re-implementation.
// Nothing leaves the process: the status key is an in-memory sink and no file is ever read (rule H16).

package harness

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"sync"
	"testing/fstest"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
	"github.com/Clifford-Foster/slate-sdk-go/bbsdk/steps"
)

// The one-stage chain RunStep drives its step over, and the activation ids both drivers pin.
const (
	stepAlias         = "step"
	stepInputKey      = "harness.step.input"
	stepOutputKey     = "harness.step.output"
	stepStatusKey     = "harness.step.status"
	stepActivationID  = "step-1"
	chainActivationID = "chain-1"
)

// The paths the supplied seam schemas are mounted at inside the driver's in-memory schema tree.
const (
	stepInputSchemaPath  = "steps/step/input.json"
	stepOutputSchemaPath = "steps/step/output.json"
)

// bindingMemory is the only binding either driver runs: the harness is in-memory by invariant. It is
// the canonical name, which is what the driver WRITES wherever it lays a binding value down; on the
// read side both spellings are accepted (steps_runtime.md rule 10, test_harness.md rule 38).
const bindingMemory = "memory"

// bindingAliasMemory is `memory`'s permanent parse alias, folded on read and never written.
const bindingAliasMemory = "embedded"

// memoryBindable reports whether a document's legal set admits the memory binding, folding both
// operands of the membership test so an authored chain in the alias spelling gates identically to
// its canonical twin (test_harness.md rule 38).
func memoryBindable(legal []string) bool {
	for _, value := range legal {
		if value == bindingMemory || value == bindingAliasMemory {
			return true
		}
	}
	return false
}

// StepOptions configures RunStep: the step's config, its declared defaults, and its seam schemas.
type StepOptions struct {
	// Config is the config the caller supplies; it is merged over Defaults by presence (rule H17).
	Config map[string]any
	// Defaults stands in for the step's step.toml-declared defaults, which rule H16 forbids reading.
	Defaults map[string]any
	// InputSchema and OutputSchema are draft 2020-12 documents; an empty one declares no seam.
	InputSchema  []byte
	OutputSchema []byte
	// Validate disables both seam validations when it points at false; nil validates (rule H17).
	Validate *bool
}

// ChainOptions configures RunChain: the instance config, and the tree the seam schemas live in.
type ChainOptions struct {
	// Config is the instance config; its rule-10 binding fields are ignored, the rest reaches the steps.
	Config map[string]any
	// Schemas is the tree the document's declared seam-schema paths resolve against.
	Schemas fs.FS
}

// ChainRecord is one in-memory traversal: its output, every status write in the writer's order, and
// the terminal failure.
type ChainRecord struct {
	// Output is the chain's final dict, nil when the traversal failed.
	Output map[string]any
	// Statuses is every status write, in the order the single writer performed it.
	Statuses []map[string]any
	// Err is the terminal StepError, nil when the traversal succeeded.
	Err error
}

// RunStep drives one step through the real runner over a one-stage chain (rule H17).
func RunStep(ctx context.Context, step steps.Step, input map[string]any, opts StepOptions) (map[string]any, error) {
	if step == nil {
		return nil, fmt.Errorf("%w: RunStep needs a step to run", ErrHarnessState)
	}
	spec := steps.StepSpec{
		Alias:         stepAlias,
		Step:          stepAlias,
		Binding:       bindingMemory,
		LegalBindings: []string{bindingMemory},
		Config:        resolvedStepConfig(opts),
	}
	schemas := fstest.MapFS{}
	if opts.Validate == nil || *opts.Validate {
		if len(opts.InputSchema) > 0 {
			schemas[stepInputSchemaPath] = &fstest.MapFile{Data: opts.InputSchema}
			spec.InputSchema = stepInputSchemaPath
		}
		if len(opts.OutputSchema) > 0 {
			schemas[stepOutputSchemaPath] = &fstest.MapFile{Data: opts.OutputSchema}
			spec.OutputSchema = stepOutputSchemaPath
		}
	}
	chain := steps.Chain{
		Name:      stepAlias,
		InputKey:  stepInputKey,
		OutputKey: stepOutputKey,
		StatusKey: stepStatusKey,
		Stages:    []steps.Stage{{Steps: []steps.StepSpec{spec}}},
	}
	providers := map[string]steps.Provider{stepAlias: func() steps.Step { return step }}
	board := newChainBoard(chain.StatusKey, chain.InputKey, input)
	bound, err := steps.Bind(chain, providers, schemas, nil, nil)
	if err != nil {
		return nil, err
	}
	runner := steps.HarnessRunner(bound, board)
	if err := runner.Start(ctx); err != nil {
		// A Setup failure propagates raw: it is a startup failure, not a step failure (rule T6).
		return nil, err
	}
	defer func() { discard(runner.Stop(ctx)) }()
	if err := runner.Run(ctx, stepActivationID); err != nil {
		return nil, err
	}
	return board.value(chain.OutputKey), nil
}

// RunChain drives a whole chain document in memory, binding every step memory (rule H18).
func RunChain(
	ctx context.Context,
	document []byte,
	providers map[string]steps.Provider,
	input map[string]any,
	opts ChainOptions,
) (ChainRecord, error) {
	chain, err := steps.ParseChain(document)
	if err != nil {
		return ChainRecord{}, err
	}
	config := maps.Clone(opts.Config)
	if config == nil {
		config = map[string]any{}
	}
	for _, stage := range chain.Stages {
		for _, spec := range stage.Steps {
			// The rule-5 embed gate is enforced from the document: gate soundness wins over in-memory
			// convenience, so a step forbidding embedding is a state error rather than a silent bind.
			if !memoryBindable(spec.LegalBindings) {
				return ChainRecord{}, fmt.Errorf(
					"%w: step %q may not be bound %s (STEP_EMBED_FORBIDDEN)", ErrHarnessState, spec.Alias, bindingMemory)
			}
			config["step_"+spec.Alias+"_binding"] = bindingMemory
		}
	}
	board := newChainBoard(chain.StatusKey, chain.InputKey, input)
	bound, err := steps.Bind(chain, providers, opts.Schemas, config, nil)
	if err != nil {
		return ChainRecord{}, err
	}
	runner := steps.HarnessRunner(bound, board)
	if err := runner.Start(ctx); err != nil {
		return ChainRecord{}, err
	}
	defer func() { discard(runner.Stop(ctx)) }()
	failure := runner.Run(ctx, chainActivationID)
	record := ChainRecord{Statuses: board.statusWrites()}
	if failure != nil {
		var verdict *steps.StepError
		if !errors.As(failure, &verdict) {
			// A driver-level failure — an unbindable chain, a board fault — is the call's own error;
			// only a step failure lands on the record.
			return record, failure
		}
		record.Err = verdict
		return record, nil
	}
	record.Output = board.value(chain.OutputKey)
	return record, nil
}

// resolvedStepConfig applies rule H17's merge by presence: the declared defaults are laid down first
// and the supplied config is merged over them, an explicitly supplied nil included, with undeclared
// supplied keys passed through verbatim and no rule-10 namespacing.
func resolvedStepConfig(opts StepOptions) map[string]any {
	resolved := map[string]any{}
	maps.Copy(resolved, opts.Defaults)
	maps.Copy(resolved, opts.Config)
	return resolved
}

// chainBoard is the drivers' in-memory board: the seeded input key, the status sink in the writer's
// order, and whatever the runner writes to output_key.
type chainBoard struct {
	mu        sync.Mutex
	statusKey string
	values    map[string]map[string]any
	statuses  []map[string]any
	revision  uint64
}

// newChainBoard builds a board holding one seeded input key.
func newChainBoard(statusKey, inputKey string, input map[string]any) *chainBoard {
	return &chainBoard{statusKey: statusKey, values: map[string]map[string]any{inputKey: input}}
}

// Get reads one key, reporting an absent key the way the data plane does (rule S3).
func (b *chainBoard) Get(_ context.Context, key string) (*bbsdk.Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	value, present := b.values[key]
	if !present {
		return nil, nil
	}
	return &bbsdk.Entry{Key: key, Value: value, Revision: b.revision}, nil
}

// Put writes one key, recording it when it is the status key.
func (b *chainBoard) Put(_ context.Context, key string, value map[string]any) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.revision++
	b.values[key] = value
	if key == b.statusKey {
		b.statuses = append(b.statuses, value)
	}
	return b.revision, nil
}

// value is the value at one key, nil when it was never written.
func (b *chainBoard) value(key string) map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.values[key]
}

// statusWrites is every status write, in the order the single writer performed them.
func (b *chainBoard) statusWrites() []map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.statuses)
}

// discard swallows a failure the contract says is best-effort and never raised (steps_runtime.md
// rule 8's teardown sweep).
func discard(error) {}
