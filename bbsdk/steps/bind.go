// Contract: contracts/sidecar.md — rule A28, the invoke a service-bound step is dispatched through.

package steps

import (
	"context"
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
)

// Invoker is the rule-13 dispatch seam for a service-bound step; *bbsdk.Client satisfies it.
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

// Bound is a chain resolved for execution: every entry's binding, object, config and seam schemas.
type Bound struct {
	chain  Chain
	stages [][]*boundStep
}

// boundStep is one chain entry resolved against the instance config.
type boundStep struct {
	spec         StepSpec
	step         Step
	binding      string
	config       map[string]any
	inputSchema  *jsonschema.Schema
	outputSchema *jsonschema.Schema
	invoker      Invoker
}

// Bind resolves every chain entry against the instance config and the supplied providers (rule T7).
func Bind(
	c Chain,
	providers map[string]Provider,
	schemas fs.FS,
	config map[string]any,
	invoker Invoker,
) (*Bound, error) {
	bound := &Bound{chain: c, stages: make([][]*boundStep, 0, len(c.Stages))}
	for _, stage := range c.Stages {
		resolved := make([]*boundStep, 0, len(stage.Steps))
		for _, spec := range stage.Steps {
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

// bindOne resolves one entry: its binding from the instance config, then its object or its target.
func bindOne(
	spec StepSpec,
	providers map[string]Provider,
	schemas fs.FS,
	config map[string]any,
	invoker Invoker,
) (*boundStep, error) {
	binding, err := resolveBinding(spec, config)
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
	entry := &boundStep{
		spec:         spec,
		binding:      binding,
		inputSchema:  inputSchema,
		outputSchema: outputSchema,
	}
	if binding == bindingNATS {
		// Rule 13: the local object is never constructed — no Setup, no Teardown — and the step's own
		// config fields are not transmitted; the remote component owns its lifecycle and its config.
		entry.config = map[string]any{}
		entry.invoker = invoker
		return entry, nil
	}
	provider, declared := providers[spec.Alias]
	if !declared || provider == nil {
		return nil, fmt.Errorf("bbsdk/steps: step %q is bound %q but no provider was supplied for it",
			spec.Alias, binding)
	}
	// Rule T3: New is called once per alias, so each alias holds whatever the package hands back — a
	// fresh object makes it a factory, a package-level singleton makes it shared.
	object := provider()
	if object == nil {
		return nil, fmt.Errorf("bbsdk/steps: the provider for step %q returned no step", spec.Alias)
	}
	entry.step = object
	entry.config = stepConfig(spec, config)
	return entry, nil
}

// resolveBinding reads step_<alias>_binding from the instance config, falling back to the document's
// compose-time default, and refuses a value outside the legal set compose derived (rules 10, T7).
func resolveBinding(spec StepSpec, config map[string]any) (string, error) {
	declared := spec.Binding
	if supplied, present := config[bindingField(spec.Alias)]; present {
		text, isText := supplied.(string)
		if !isText {
			return "", fmt.Errorf("%w: step %q was given binding %v, which is not a string",
				ErrBindingUnsupported, spec.Alias, supplied)
		}
		declared = text
	}
	// Rule T7: an alias folds to its canonical name BEFORE the membership test, and BOTH operands
	// fold — the resolved value and every member of the document's legal set — so a pre-flip document
	// and a post-flip config bind against each other in either direction. Nothing is rewritten: the
	// document keeps the spelling it was read with.
	binding := foldBinding(declared)
	legal := foldLegalBindings(spec.LegalBindings)
	if !slices.Contains(legal, binding) {
		return "", fmt.Errorf("%w: step %q resolved to %q, and its legal set is [%s]",
			ErrBindingUnsupported, spec.Alias, declared, strings.Join(legal, " "))
	}
	return binding, nil
}

// foldLegalBindings folds a document's legal set to canonical names, deduplicating: the fold never
// enlarges a set, so a single-member set stays single-member (rule T7).
func foldLegalBindings(declared []string) []string {
	folded := make([]string, 0, len(declared))
	for _, value := range declared {
		if canonical := foldBinding(value); !slices.Contains(folded, canonical) {
			folded = append(folded, canonical)
		}
	}
	return folded
}

// bindingField is the reserved config field rule 10 namespaces a step's deploy-time binding under.
func bindingField(alias string) string {
	return "step_" + alias + "_binding"
}

// stepConfig applies rule 10's run-time precedence: an instance step_<alias>_<field> beats the chain
// definition's value, which compose already resolved into the document — the step's declared defaults
// having been folded into the namespaced manifest fields at compose time, since a Go runtime never
// reads step.toml. The reserved binding field is not a config value and never reaches the step.
func stepConfig(spec StepSpec, config map[string]any) map[string]any {
	values := make(map[string]any, len(spec.Config))
	for name, value := range spec.Config {
		values[name] = value
	}
	prefix := "step_" + spec.Alias + "_"
	for name, value := range config {
		field, namespaced := strings.CutPrefix(name, prefix)
		if !namespaced || field == "binding" {
			continue
		}
		values[field] = value
	}
	return values
}
