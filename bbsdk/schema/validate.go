package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// documentScheme keeps a prepared document's identity out of any namespace a compiler could go and
// fetch from: the location is an opaque name, never a URL.
const documentScheme = "bbsdk:///schema"

// refusingLoader answers nothing, so the compiler fetches nothing however a document is written —
// the established no-fetch construction, shared with the seam validator (rules D3, H20).
type refusingLoader struct{}

// Load refuses every external reference.
func (refusingLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("bbsdk/schema: a declared schema may not reference %s — it must be self-contained", url)
}

// prepared is one compiled schema document: parsed and compiled once, validated against many times.
type prepared struct {
	compiled *jsonschema.Schema
}

// prepare compiles one schema document under the no-fetch construction (rules D3, H20).
func prepare(document any) (*prepared, error) {
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("bbsdk/schema: the document is not JSON-encodable: %w", err)
	}
	decoded, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("bbsdk/schema: the document is not valid JSON: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	// A document carrying no $schema member is draft 2020-12, the one draft these shapes speak.
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(refusingLoader{})
	if err := compiler.AddResource(documentScheme, decoded); err != nil {
		return nil, fmt.Errorf("bbsdk/schema: the document is not a valid JSON Schema: %w", err)
	}
	compiled, err := compiler.Compile(documentScheme)
	if err != nil {
		return nil, fmt.Errorf("bbsdk/schema: the document is not a valid JSON Schema: %w", err)
	}
	return &prepared{compiled: compiled}, nil
}

// unsatisfied validates one value and names what it failed, empty when the value conforms. The
// instance is validated from the bytes the value encodes to, so the schema judges exactly what the
// board would carry (rules D3, H20).
func (p *prepared) unsatisfied(value any) ([]string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("bbsdk/schema: the value is not JSON-encodable: %w", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("bbsdk/schema: the value is not valid JSON: %w", err)
	}
	if err := p.compiled.Validate(instance); err != nil {
		return verdictMembers(err), nil
	}
	return nil, nil
}

// verdictMembers renders a validation failure in the sidecar.md Validation Verdict vocabulary: one
// entry per unsatisfied member or constraint, deduplicated and ordered deterministically so two
// runs — and the two SDKs judging one corpus — name the same members in the same order (rule D3).
func verdictMembers(err error) []string {
	var failure *jsonschema.ValidationError
	if !errors.As(err, &failure) {
		return []string{"schema: " + err.Error()}
	}
	members := []string{}
	collectMembers(failure, &members)
	if len(members) == 0 {
		members = append(members, "schema")
	}
	slices.Sort(members)
	return slices.Compact(members)
}

// collectMembers walks the validation tree to its leaves, which are the units naming a keyword.
func collectMembers(failure *jsonschema.ValidationError, into *[]string) {
	if len(failure.Causes) > 0 {
		for _, cause := range failure.Causes {
			collectMembers(cause, into)
		}
		return
	}
	location := pointer(failure.InstanceLocation)
	if missing, ok := failure.ErrorKind.(*kind.Required); ok {
		// A missing member is named, not pointed at — the "required: total" spelling.
		for _, name := range missing.Missing {
			*into = append(*into, "required: "+member(location, name))
		}
		return
	}
	*into = append(*into, at(keywordOf(failure.ErrorKind), location))
}

// keywordOf names the keyword a unit failed, falling back where the unit names a schema rather
// than a keyword.
func keywordOf(errorKind jsonschema.ErrorKind) string {
	path := errorKind.KeywordPath()
	if len(path) == 0 {
		return "schema"
	}
	return path[len(path)-1]
}

// member qualifies a missing member name by its container, which is bare at the root.
func member(location, name string) string {
	if location == "" {
		return name
	}
	return location + "/" + escapeToken(name)
}

// at renders one keyword against the value it judged: "<keyword>: <pointer>", and the bare keyword
// at the root, where the pointer is empty.
func at(keyword, location string) string {
	if location == "" {
		return keyword
	}
	return keyword + ": " + location
}

// pointer renders an instance location as an RFC 6901 JSON pointer.
func pointer(tokens []string) string {
	var rendered strings.Builder
	for _, token := range tokens {
		rendered.WriteByte('/')
		rendered.WriteString(escapeToken(token))
	}
	return rendered.String()
}

// escapeToken renders one JSON-pointer token, escaping RFC 6901's two reserved characters.
func escapeToken(token string) string {
	return strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}
