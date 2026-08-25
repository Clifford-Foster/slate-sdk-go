// Contract: contracts/bb_sdk_go.md — rules H17 and H18, the component and chain drivers.
//
// Both drive the real bbsdk/components runner over an in-memory board: setup, seam validation, rule-3
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
	"github.com/Clifford-Foster/slate-sdk-go/bbsdk/components"
)

// The one-stage chain RunComponent drives its component over, and the activation ids both drivers pin.
const (
	stepAlias         = "component"
	stepInputKey      = "harness.component.input"
	stepOutputKey     = "harness.component.output"
	stepStatusKey     = "harness.component.status"
	stepActivationID  = "component-1"
	chainActivationID = "chain-1"
)

// The paths the supplied seam schemas are mounted at inside the driver's in-memory schema tree.
const (
	stepInputSchemaPath  = "components/component/input.json"
	stepOutputSchemaPath = "components/component/output.json"
)

// communicationMemory is the communication either driver runs wherever memory is legal: the harness is in-memory
// by invariant. It is the canonical name, which is what the driver WRITES wherever it lays a communication
// value down; on the read side both spellings are accepted (components_runtime.md rule 10,
// test_harness.md rule 38).
const communicationMemory = "memory"

// communicationNATS is what an entry memory is not legal for is pinned to, dispatched to a registered
// in-process responder rather than to transport (rule H18).
const communicationNATS = "nats"

// communicationAliasMemory is `memory`'s permanent parse alias, folded on read and never written.
const communicationAliasMemory = "embedded"

// memoryBindable reports whether a document's legal set admits the memory communication, folding both
// operands of the membership test so an authored chain in the alias spelling gates identically to
// its canonical twin (test_harness.md rule 38).
func memoryBindable(legal []string) bool {
	for _, value := range legal {
		if value == communicationMemory || value == communicationAliasMemory {
			return true
		}
	}
	return false
}

// ComponentOptions configures RunComponent: the component's config, its declared defaults, and its seam schemas.
type ComponentOptions struct {
	// Config is the config the caller supplies; it is merged over Defaults by presence (rule H17).
	Config map[string]any
	// Defaults stands in for the component's bb.toml-declared defaults, which rule H16 forbids reading.
	Defaults map[string]any
	// InputSchema and OutputSchema are draft 2020-12 documents; an empty one declares no seam.
	InputSchema  []byte
	OutputSchema []byte
	// Validate disables both seam validations when it points at false; nil validates (rule H17).
	Validate *bool
	// Components drives a calling component's slots with fakes: slot name to responder. It is a fake and
	// not a dispatch — no wired entry, so no wired schemas, no wired retries and no ceiling (rule H17).
	Components map[string]func(ctx context.Context, payload map[string]any) (map[string]any, error)
}

// ChainOptions configures RunChain: the instance config, and the tree the seam schemas live in.
type ChainOptions struct {
	// Config is the instance config; its rule-10 communication fields are ignored, the rest reaches the components.
	Config map[string]any
	// Schemas is the tree the document's declared seam-schema paths resolve against.
	Schemas fs.FS
	// Responders answer an entry memory is not legal for, keyed by service then endpoint exactly as
	// Options.InvokeResponders is. A registered in-process responder is not transport (rule H18).
	Responders map[string]map[string]InvokeFunc
}

// ChainRecord is one in-memory traversal: its output, every status write in the writer's order, and
// the terminal failure.
type ChainRecord struct {
	// Output is the chain's final dict, nil when the traversal failed.
	Output map[string]any
	// Statuses is every status write, in the order the single writer performed it.
	Statuses []map[string]any
	// Err is the terminal ComponentError, nil when the traversal succeeded.
	Err error
}

// RunComponent drives one component through the real runner over a one-stage chain (rule H17).
func RunComponent(ctx context.Context, component components.Component, input map[string]any, opts ComponentOptions) (map[string]any, error) {
	if component == nil {
		return nil, fmt.Errorf("%w: RunComponent needs a component to run", ErrHarnessState)
	}
	spec := components.ComponentSpec{
		Alias:               stepAlias,
		Component:           stepAlias,
		Communication:       communicationMemory,
		LegalCommunications: []string{communicationMemory},
		Config:              resolvedStepConfig(opts),
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
	chain := components.Chain{
		Name:      stepAlias,
		InputKey:  stepInputKey,
		OutputKey: stepOutputKey,
		StatusKey: stepStatusKey,
		Stages:    []components.Stage{{Components: []components.ComponentSpec{spec}}},
	}
	providers := map[string]components.Provider{stepAlias: func() components.Component { return component }}
	board := newChainBoard(chain.StatusKey, chain.InputKey, input)
	bound, err := components.Bind(chain, providers, schemas, nil, nil)
	if err != nil {
		return nil, err
	}
	// The fake handle is installed after Bind, which is where the real dispatcher would have been
	// handed over: RunComponent drives one component with no chain and no wiring behind it, so the fakes win.
	if user, callsComponents := component.(components.ComponentUser); callsComponents {
		user.UseComponents(componentFakes(opts.Components))
	}
	runner := components.HarnessRunner(bound, board)
	if err := runner.Start(ctx); err != nil {
		// A Setup failure propagates raw: it is a startup failure, not a component failure (rule T6).
		return nil, err
	}
	defer func() { discard(runner.Stop(ctx)) }()
	if err := runner.Run(ctx, stepActivationID); err != nil {
		return nil, err
	}
	return board.value(chain.OutputKey), nil
}

// RunChain drives a whole chain document in memory, communication every component memory (rule H18).
func RunChain(
	ctx context.Context,
	document []byte,
	providers map[string]components.Provider,
	input map[string]any,
	opts ChainOptions,
) (ChainRecord, error) {
	chain, err := components.ParseChain(document)
	if err != nil {
		return ChainRecord{}, err
	}
	config := maps.Clone(opts.Config)
	if config == nil {
		config = map[string]any{}
	}
	for _, stage := range chain.Stages {
		for _, spec := range stage.Components {
			pinBinding(spec, config)
		}
	}
	board := newChainBoard(chain.StatusKey, chain.InputKey, input)
	bound, err := components.Bind(chain, providers, opts.Schemas, config, responderInvoker{responders: opts.Responders})
	if err != nil {
		return ChainRecord{}, err
	}
	runner := components.HarnessRunner(bound, board)
	if err := runner.Start(ctx); err != nil {
		return ChainRecord{}, err
	}
	defer func() { discard(runner.Stop(ctx)) }()
	failure := runner.Run(ctx, chainActivationID)
	record := ChainRecord{Statuses: board.statusWrites()}
	if failure != nil {
		var verdict *components.ComponentError
		if !errors.As(failure, &verdict) {
			// A driver-level failure — an unbindable chain, a board fault — is the call's own error;
			// only a component failure lands on the record.
			return record, failure
		}
		record.Err = verdict
		return record, nil
	}
	record.Output = board.value(chain.OutputKey)
	return record, nil
}

// pinBinding pins one entry's communication, and every entry its wiring carries: memory wherever memory is
// legal for it, and otherwise the nats dispatch to a registered responder. The rule-5 embed gate is
// enforced from the document — the driver never binds an entry memory against a legal set that
// excludes it, so gate soundness survives the pin's move (rule H18).
func pinBinding(spec components.ComponentSpec, config map[string]any) {
	communication := communicationNATS
	if memoryBindable(spec.LegalCommunications) {
		communication = communicationMemory
	}
	config["component_"+spec.Alias+"_communication"] = communication
	for _, wired := range spec.Calls {
		// A wired component is pinned on the same terms as a stage entry; without it a chain wiring an
		// existing deployed component could not be driven here at all.
		pinBinding(wired, config)
	}
}

// componentFakes is rule H17's fake call handle: a mapping of slot to responder with nothing behind
// it. It is a named type, as the handle is in both languages.
type componentFakes map[string]func(ctx context.Context, payload map[string]any) (map[string]any, error)

// Call answers from the mapping; a slot it does not carry is the ordinary *ComponentError wrapping
// ErrComponentUnknown, exactly as an unresolved slot is at bind.
func (c componentFakes) Call(ctx context.Context, slot string, payload map[string]any) (map[string]any, error) {
	responder, wired := c[slot]
	if !wired {
		return nil, &components.ComponentError{
			Message:   fmt.Sprintf("COMPONENT_UNKNOWN: no fake is registered for slot %q", slot),
			Cause:     components.CauseInternal,
			Retryable: false,
			Err:       components.ErrComponentUnknown,
		}
	}
	return responder(ctx, payload)
}

// responderInvoker dispatches a nats-bound entry to a registered in-process responder (rule H18).
type responderInvoker struct {
	responders map[string]map[string]InvokeFunc
}

// Invoke answers from the registry; an unanswered target is INVOKE_NO_RESPONDERS on rule H14's terms,
// which is what proves the registry live rather than the entry having been bound memory behind the
// assertion's back.
func (r responderInvoker) Invoke(
	ctx context.Context,
	service, endpoint string,
	payload map[string]any,
	_ ...bbsdk.InvokeOption,
) (map[string]any, error) {
	target := service + "." + endpoint
	responder := r.responders[service][endpoint]
	if responder == nil {
		return nil, bbsdk.HarnessInvokeError(codeInvokeNoResponders,
			fmt.Sprintf("no responder is registered for %q", target))
	}
	reply, err := responder(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %w",
			bbsdk.HarnessInvokeError(codeInvokeServiceError, fmt.Sprintf("the %q responder failed", target)), err)
	}
	if reply == nil {
		return map[string]any{}, nil
	}
	return reply, nil
}

// resolvedStepConfig applies rule H17's merge by presence: the declared defaults are laid down first
// and the supplied config is merged over them, an explicitly supplied nil included, with undeclared
// supplied keys passed through verbatim and no rule-10 namespacing.
func resolvedStepConfig(opts ComponentOptions) map[string]any {
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

// discard swallows a failure the contract says is best-effort and never raised (components_runtime.md
// rule 8's teardown sweep).
func discard(error) {}
