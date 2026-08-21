package steps

import (
	"encoding/json"
	"fmt"
	"io/fs"
)

// documentVersion is the one canonical-chain-document version this runtime knows. A document carrying
// any other value is refused rather than guessed at — a 1 an older bb compose generate emitted
// included, since a runtime that cannot read an entry's wiring must not run the chain wiring-less
// (steps_runtime.md §Logical, rule T5).
const documentVersion = 2

// chainDocumentPath is where compose puts the document inside the embedded steps tree (rule 11a).
const chainDocumentPath = "steps/chain.json"

// ServiceTarget is a service-bound step's remote target: a deployed component and one of its endpoints.
type ServiceTarget struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
}

// StepSpec is one chain entry exactly as compose derived it; the runtime re-derives none of it.
type StepSpec struct {
	Alias             string         `json:"alias"`
	Step              string         `json:"step"`
	Version           string         `json:"version"`
	SourceDir         string         `json:"source_dir"`
	Binding           string         `json:"binding"`
	LegalBindings     []string       `json:"legal_bindings"`
	MultiInstanceSafe bool           `json:"multi_instance_safe"`
	Config            map[string]any `json:"config"`
	Retries           int            `json:"retries"`
	TimeoutS          int            `json:"timeout_s"`
	Service           *ServiceTarget `json:"service"`
	InputSchema       string         `json:"input_schema"`
	OutputSchema      string         `json:"output_schema"`
	// Components is the entry's wiring table, keyed by slot and one level deep; MaxComponentCalls is
	// its effective per-activation ceiling. Both are absent from an entry that wires none, and both are
	// compose's derived output — the decode re-derives neither and range-checks neither (rule T5).
	Components        map[string]StepSpec `json:"components,omitempty"`
	MaxComponentCalls int                 `json:"max_component_calls,omitempty"`
}

// Stage is one stage of the chain: a single step, or the 2 to 8 branches of a parallel group.
type Stage struct {
	Steps []StepSpec `json:"steps"`
}

// Chain is the decoded chain document: the composite's name, its three boundary keys, its
// precondition, and its ordered stages.
type Chain struct {
	Name         string  `json:"name"`
	InputKey     string  `json:"input_key"`
	OutputKey    string  `json:"output_key"`
	StatusKey    string  `json:"status_key"`
	Precondition string  `json:"precondition"`
	Stages       []Stage `json:"stages"`
}

// document is the whole artifact: the chain members plus the reserved _bb block compose writes last.
// The struct tags here are the document's schema — the two compose cores spell exactly these members.
type document struct {
	Chain
	Meta struct {
		DocumentVersion int `json:"document_version"`
	} `json:"_bb"`
}

// LoadChain decodes steps/chain.json from the embedded steps tree (rule T5).
func LoadChain(fsys fs.FS) (Chain, error) {
	if fsys == nil {
		return Chain{}, fmt.Errorf("%w: no embedded tree to read %s from", ErrChainDocumentInvalid, chainDocumentPath)
	}
	data, err := fs.ReadFile(fsys, chainDocumentPath)
	if err != nil {
		return Chain{}, fmt.Errorf("%w: reading %s: %w", ErrChainDocumentInvalid, chainDocumentPath, err)
	}
	return ParseChain(data)
}

// ParseChain decodes a chain document held in memory (rule T5).
func ParseChain(data []byte) (Chain, error) {
	// A decode, not a parse of the chain grammar: the document is compose's own validated output, so
	// no alias, topology, budget or satisfiability check belongs here — each is a compose finding that
	// exists before the document does.
	var decoded document
	if err := json.Unmarshal(data, &decoded); err != nil {
		return Chain{}, fmt.Errorf("%w: %w", ErrChainDocumentInvalid, err)
	}
	if decoded.Meta.DocumentVersion != documentVersion {
		// The compatibility gate: a runtime that does not know the value refuses to start.
		return Chain{}, fmt.Errorf("%w: document_version %d is not %d",
			ErrChainDocumentInvalid, decoded.Meta.DocumentVersion, documentVersion)
	}
	return decoded.Chain, nil
}
