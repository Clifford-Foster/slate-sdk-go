// Contract: contracts/sidecar.md — rule A28, the invoke a service-bound component is dispatched through.

package components

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
)

// Invoker is the rule-13 dispatch seam for a service-bound component; *bbsdk.Client satisfies it.
type Invoker interface {
	Invoke(
		ctx context.Context,
		service, endpoint string,
		payload map[string]any,
		opts ...bbsdk.InvokeOption,
	) (map[string]any, error)
}

// Board is the boundary-IO seam the runner reads and writes its three keys through; *bbsdk.Client
// satisfies it.
type Board interface {
	Get(ctx context.Context, key string) (*bbsdk.Entry, error)
	Put(ctx context.Context, key string, value map[string]any) (uint64, error)
}

// Bound is a chain resolved for execution: every entry's communication, object, config and seam schemas.
type Bound struct {
	chain  Chain
	stages [][]*boundComponent
}

// boundComponent is one chain entry resolved against the instance config.
type boundComponent struct {
	spec          ComponentSpec
	component     Component
	communication string
	config        map[string]any
	inputSchema   *jsonschema.Schema
	outputSchema  *jsonschema.Schema
	invoker       Invoker
	// wired is the entry's bound components table in sorted slot order, which is the order rule 8's
	// setup sweep visits them in — immediately after this entry's own.
	wired []*boundComponent
	// calls is the entry's dispatcher, present only where the component implements the call half (rule T7).
	calls *dispatcher
}

// Bind resolves every chain entry against the instance config and the supplied providers (rule T7).
func Bind(
	c Chain,
	providers map[string]Provider,
	schemas fs.FS,
	config map[string]any,
	invoker Invoker,
) (*Bound, error) {
	bound := &Bound{chain: c, stages: make([][]*boundComponent, 0, len(c.Stages))}
	for _, stage := range c.Stages {
		resolved := make([]*boundComponent, 0, len(stage.Components))
		for _, spec := range stage.Components {
			entry, err := bindOne(spec, providers, schemas, config, invoker)
			if err != nil {
				return nil, err
			}
			resolved = append(resolved, entry)
		}
		bound.stages = append(bound.stages, resolved)
	}
	return bound, nil
}

// entries visits every bound entry in rule 8's sweep order: chain order — stage by stage, declaration
// order inside a stage — with a wired component immediately after its calling entry's, in sorted slot
// order, so the sweep visits an entry and then everything that entry calls before moving on.
func (b *Bound) entries(visit func(*boundComponent) error) error {
	for _, stage := range b.stages {
		for _, entry := range stage {
			if err := visit(entry); err != nil {
				return err
			}
			for _, wired := range entry.wired {
				if err := visit(wired); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// entry finds one bound chain entry by alias. A wired component is reached through the entry that
// calls it and is not addressable here, which is what makes the lookup the chain's own topology.
func (b *Bound) entry(alias string) *boundComponent {
	for _, stage := range b.stages {
		for _, entry := range stage {
			if entry.spec.Alias == alias {
				return entry
			}
		}
	}
	return nil
}

// bindOne resolves one entry: its communication from the instance config, then its object or its target.
func bindOne(
	spec ComponentSpec,
	providers map[string]Provider,
	schemas fs.FS,
	config map[string]any,
	invoker Invoker,
) (*boundComponent, error) {
	communication, err := resolveCommunication(spec, config)
	if err != nil {
		return nil, err
	}
	inputSchema, err := compileSeamSchema(schemas, spec.Alias, spec.InputSchema)
	if err != nil {
		return nil, err
	}
	outputSchema, err := compileSeamSchema(schemas, spec.Alias, spec.OutputSchema)
	if err != nil {
		return nil, err
	}
	entry := &boundComponent{
		spec:          spec,
		communication: communication,
		inputSchema:   inputSchema,
		outputSchema:  outputSchema,
	}
	if communication == communicationNATS {
		// Rule 13: the local object is never constructed — no Setup, no Teardown — and the component's own
		// config fields are not transmitted; the remote component owns its lifecycle and its config.
		entry.config = map[string]any{}
		entry.invoker = invoker
		return entry, nil
	}
	provider, declared := providers[spec.Alias]
	if !declared || provider == nil {
		return nil, fmt.Errorf("bbsdk/components: component %q is bound %q but no provider was supplied for it",
			spec.Alias, communication)
	}
	// Rule T3: New is called once per alias, so each alias holds whatever the package hands back — a
	// fresh object makes it a factory, a package-level singleton makes it shared.
	object := provider()
	if object == nil {
		return nil, fmt.Errorf("bbsdk/components: the provider for component %q returned no component", spec.Alias)
	}
	entry.component = object
	entry.config = componentConfig(spec, config)
	if err := bindWiring(entry, providers, schemas, config, invoker); err != nil {
		return nil, err
	}
	return entry, nil
}

// bindWiring resolves the entry's components table exactly as a stage entry is resolved — its own
// communication against its own legal set, its memory object from providers once per alias, its nats
// dispatch to the invoker with no local object — and then hands the calling component's ComponentUser half
// a Components over that result, once, before any traversal (rule T7).
func bindWiring(
	entry *boundComponent,
	providers map[string]Provider,
	schemas fs.FS,
	config map[string]any,
	invoker Invoker,
) error {
	wiring := make(map[string]*boundComponent, len(entry.spec.Calls))
	for _, slot := range slices.Sorted(maps.Keys(entry.spec.Calls)) {
		// The nesting is one level: a wired entry carries no components of its own (rule 14), so this
		// resolution recurses no further than compose's document can.
		wired, err := bindOne(entry.spec.Calls[slot], providers, schemas, config, invoker)
		if err != nil {
			return err
		}
		wiring[slot] = wired
		entry.wired = append(entry.wired, wired)
	}
	user, callsComponents := entry.component.(ComponentUser)
	if !callsComponents {
		// The half is probed once at bind by interface assertion, and its absence is a no-op — so no
		// existing component changes.
		return nil
	}
	entry.calls = newDispatcher(entry.spec.Alias, wiring, entry.spec.MaxComponentCalls)
	user.UseComponents(entry.calls)
	return nil
}

// resolveCommunication reads component_<alias>_communication from the instance config — or its permanent
// alias step_<alias>_binding, the canonical name winning where a configuration carries both — falling
// back to the document's compose-time default, and refuses a value outside the legal set compose
// derived (rules 10, T7).
func resolveCommunication(spec ComponentSpec, config map[string]any) (string, error) {
	declared := spec.Communication
	supplied, present := config[communicationField(spec.Alias)]
	if !present {
		supplied, present = config[legacyCommunicationField(spec.Alias)]
	}
	if present {
		text, isText := supplied.(string)
		if !isText {
			return "", fmt.Errorf("%w: component %q was given communication %v, which is not a string",
				ErrCommunicationUnsupported, spec.Alias, supplied)
		}
		declared = text
	}
	// Rule T7: an alias folds to its canonical name BEFORE the membership test, and BOTH operands
	// fold — the resolved value and every member of the document's legal set — so a pre-flip document
	// and a post-flip config bind against each other in either direction. Nothing is rewritten: the
	// document keeps the spelling it was read with.
	communication := foldCommunication(declared)
	legal := foldLegalCommunications(spec.LegalCommunications)
	if !slices.Contains(legal, communication) {
		return "", fmt.Errorf("%w: component %q resolved to %q, and its legal set is [%s]",
			ErrCommunicationUnsupported, spec.Alias, declared, strings.Join(legal, " "))
	}
	return communication, nil
}

// foldLegalCommunications folds a document's legal set to canonical names, deduplicating: the fold never
// enlarges a set, so a single-member set stays single-member (rule T7).
func foldLegalCommunications(declared []string) []string {
	folded := make([]string, 0, len(declared))
	for _, value := range declared {
		if canonical := foldCommunication(value); !slices.Contains(folded, canonical) {
			folded = append(folded, canonical)
		}
	}
	return folded
}

// communicationField is the reserved config field rule 10 namespaces a component's deploy-time communication under.
func communicationField(alias string) string {
	return componentConfigPrefix + alias + "_communication"
}

// legacyCommunicationField is that field's PERMANENT read-side alias: a unit deployed before the
// unification carries this spelling in its configuration and must keep binding after the caller is
// rebuilt (rule 10). It is read, never written.
func legacyCommunicationField(alias string) string {
	return legacyConfigPrefix + alias + "_binding"
}

// componentConfig applies rule 10's run-time precedence: an instance component_<alias>_<field> beats the
// chain definition's value, which compose already resolved into the document — the component's declared
// defaults having been folded into the namespaced manifest fields at compose time, since a Go runtime
// never reads bb.toml. The reserved communication field is not a config value and never reaches the
// component. The pre-unification prefix step_<alias>_ is read on the same terms as every other permanent
// alias (rule 10): it is folded second, so the canonical spelling wins wherever a configuration carries
// both, and nothing is rewritten.
func componentConfig(spec ComponentSpec, config map[string]any) map[string]any {
	values := make(map[string]any, len(spec.Config))
	for name, value := range spec.Config {
		values[name] = value
	}
	for _, prefix := range []string{legacyConfigPrefix + spec.Alias + "_", componentConfigPrefix + spec.Alias + "_"} {
		for name, value := range config {
			field, namespaced := strings.CutPrefix(name, prefix)
			if !namespaced || field == "communication" || field == "binding" {
				continue
			}
			values[field] = value
		}
	}
	return values
}
