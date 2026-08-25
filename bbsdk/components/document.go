package components

import (
	"encoding/json"
	"fmt"
	"io/fs"
)

// documentVersion is the canonical-chain-document version compose emits and this runtime reads
// (components_runtime.md §Logical, rule T5).
const documentVersion = 3

// legacyDocumentVersion is the pre-unification version, accepted on read PERMANENTLY: it carries this
// document's members under their old spellings and is read as exactly this document with those five
// members renamed. A version this runtime does not know — any value above the current one — is still
// refused rather than guessed at, because an image built before a flip must keep running until it is
// rebuilt but a document a later compose wrote must not be run wiring-less.
const legacyDocumentVersion = 2

// chainDocumentPath is where compose puts the document inside the embedded components tree (rule 11a).
const chainDocumentPath = "components/chain.json"

// ServiceTarget is a service-bound component's remote target: a deployed component and one of its endpoints.
type ServiceTarget struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
}

// ComponentSpec is one chain entry exactly as compose derived it; the runtime re-derives none of it.
type ComponentSpec struct {
	Alias               string         `json:"alias"`
	Component           string         `json:"component"`
	Version             string         `json:"version"`
	SourceDir           string         `json:"source_dir"`
	Communication       string         `json:"communication"`
	LegalCommunications []string       `json:"legal_communications"`
	MultiInstanceSafe   bool           `json:"multi_instance_safe"`
	Config              map[string]any `json:"config"`
	Retries             int            `json:"retries"`
	TimeoutS            int            `json:"timeout_s"`
	Service             *ServiceTarget `json:"service"`
	InputSchema         string         `json:"input_schema"`
	OutputSchema        string         `json:"output_schema"`
	// Calls is the entry's wiring table, keyed by slot and one level deep; MaxComponentCalls is
	// its effective per-activation ceiling. Both are absent from an entry that wires none, and both are
	// compose's derived output — the decode re-derives neither and range-checks neither (rule T5).
	Calls             map[string]ComponentSpec `json:"calls,omitempty"`
	MaxComponentCalls int                      `json:"max_component_calls,omitempty"`
}

// Stage is one stage of the chain: a single component, or the 2 to 8 branches of a parallel group.
type Stage struct {
	Components []ComponentSpec `json:"components"`
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

// legacyStage is a version-2 stage: its entry list was spelled `steps`.
type legacyStage struct {
	Steps []legacySpec `json:"steps"`
}

// legacySpec is a version-2 chain entry: its reference was `step`, its communication `binding`, its
// legal set `legal_bindings` and its wiring table `components`. Every other member and every value is
// identical, so the fold below is a rename and nothing more.
type legacySpec struct {
	ComponentSpec
	Step          string                `json:"step"`
	Binding       string                `json:"binding"`
	LegalBindings []string              `json:"legal_bindings"`
	Wiring        map[string]legacySpec `json:"components,omitempty"`
}

// legacyDocument is the whole version-2 artifact.
type legacyDocument struct {
	Chain
	Stages []legacyStage `json:"stages"`
	Meta   struct {
		DocumentVersion int `json:"document_version"`
	} `json:"_bb"`
}

// LoadChain decodes components/chain.json from the embedded components tree (rule T5).
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
	var probe struct {
		Meta struct {
			DocumentVersion int `json:"document_version"`
		} `json:"_bb"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return Chain{}, fmt.Errorf("%w: %w", ErrChainDocumentInvalid, err)
	}
	switch probe.Meta.DocumentVersion {
	case documentVersion:
		var decoded document
		if err := json.Unmarshal(data, &decoded); err != nil {
			return Chain{}, fmt.Errorf("%w: %w", ErrChainDocumentInvalid, err)
		}
		return decoded.Chain, nil
	case legacyDocumentVersion:
		var decoded legacyDocument
		if err := json.Unmarshal(data, &decoded); err != nil {
			return Chain{}, fmt.Errorf("%w: %w", ErrChainDocumentInvalid, err)
		}
		chain := decoded.Chain
		chain.Stages = make([]Stage, 0, len(decoded.Stages))
		for _, stage := range decoded.Stages {
			entries := make([]ComponentSpec, 0, len(stage.Steps))
			for _, entry := range stage.Steps {
				entries = append(entries, foldLegacySpec(entry))
			}
			chain.Stages = append(chain.Stages, Stage{Components: entries})
		}
		return chain, nil
	default:
		// The compatibility gate: a runtime that does not know the value refuses to start.
		return Chain{}, fmt.Errorf("%w: document_version %d is neither %d nor %d",
			ErrChainDocumentInvalid, probe.Meta.DocumentVersion, documentVersion, legacyDocumentVersion)
	}
}

// foldLegacySpec reads a version-2 entry as this version's entry: the five renamed members resolve to
// their canonical homes, and the canonical member wins wherever a document carries both.
func foldLegacySpec(entry legacySpec) ComponentSpec {
	spec := entry.ComponentSpec
	if spec.Component == "" {
		spec.Component = entry.Step
	}
	if spec.Communication == "" {
		spec.Communication = entry.Binding
	}
	if spec.LegalCommunications == nil {
		spec.LegalCommunications = entry.LegalBindings
	}
	if spec.Calls == nil && entry.Wiring != nil {
		wiring := make(map[string]ComponentSpec, len(entry.Wiring))
		for slot, wired := range entry.Wiring {
			wiring[slot] = foldLegacySpec(wired)
		}
		spec.Calls = wiring
	}
	return spec
}
