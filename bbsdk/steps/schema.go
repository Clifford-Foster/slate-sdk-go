package steps

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// seamSchemaScheme keeps a compiled seam schema's identity out of the filesystem's namespace: the
// document's path is a location inside the embedded tree, never a URL the compiler may go and fetch.
const seamSchemaScheme = "bbsdk:///"

// refusingLoader answers nothing. The refusal itself is checkSelfContained's, decided structurally
// before the compiler runs; this is the additional guard rule T12 requires so rule G1's reads-no-file
// posture survives contact with an implementation that would otherwise fetch.
type refusingLoader struct{}

// Load refuses every external reference.
func (refusingLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("bbsdk/steps: a seam schema may not reference %s — it must be self-contained", url)
}

// compileSeamSchema compiles one declared seam schema out of the embedded tree; an undeclared seam
// (an empty path) compiles to nil, which is how the runner knows not to validate it (rule 6).
func compileSeamSchema(schemas fs.FS, alias, path string) (*jsonschema.Schema, error) {
	if path == "" {
		return nil, nil
	}
	if schemas == nil {
		return nil, seamSchemaError(alias, path, errors.New("no schema tree was supplied"))
	}
	data, err := fs.ReadFile(schemas, path)
	if err != nil {
		return nil, seamSchemaError(alias, path, fmt.Errorf("it could not be read: %w", err))
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return nil, seamSchemaError(alias, path, fmt.Errorf("it is not valid JSON: %w", err))
	}
	// Rule T12: self-containment is decided structurally, over the references the document itself
	// carries, before the compiler sees it — an implementation that resolves the meta-schema's own
	// URL from a built-in, or never looks inside an unreached $defs, would otherwise let one through.
	if err := checkSelfContained(document); err != nil {
		return nil, seamSchemaError(alias, path, err)
	}
	compiler := jsonschema.NewCompiler()
	// A schema with no $schema member is draft 2020-12, the one draft this contract's seams speak.
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(refusingLoader{})
	location := seamSchemaScheme + path
	if err := compiler.AddResource(location, document); err != nil {
		return nil, seamSchemaError(alias, path, err)
	}
	compiled, err := compiler.Compile(location)
	if err != nil {
		return nil, seamSchemaError(alias, path, fmt.Errorf("it is not a valid JSON Schema: %w", err))
	}
	return compiled, nil
}

// seamSchemaError names the step alias, the declared schema path and what refused the schema, over
// the one sentinel a caller discriminates the class by (rule T12).
func seamSchemaError(alias, path string, err error) error {
	return fmt.Errorf("%w: step %q seam schema %q: %w", ErrSeamSchemaInvalid, alias, path, err)
}

// checkSelfContained applies rule T12's structural half: every $ref the document carries is a
// same-document reference naming something the document holds. The walk covers the whole document, so
// a reference sitting in an unreached $defs is refused like any other.
func checkSelfContained(document any) error {
	anchors := map[string]bool{}
	collectAnchors(document, anchors)
	return walkRefs(document, document, anchors, "")
}

// collectAnchors records every $anchor the document declares — the one same-document reference form
// that is not a JSON pointer.
func collectAnchors(node any, into map[string]bool) {
	switch value := node.(type) {
	case map[string]any:
		if anchor, isText := value["$anchor"].(string); isText {
			into[anchor] = true
		}
		for _, member := range sortedMembers(value) {
			collectAnchors(value[member], into)
		}
	case []any:
		for _, item := range value {
			collectAnchors(item, into)
		}
	}
}

// walkRefs refuses the first $ref that is not a resolving same-document reference, walking members in
// a stable order so a document carrying several earns the same refusal every time.
func walkRefs(node, document any, anchors map[string]bool, location string) error {
	switch value := node.(type) {
	case map[string]any:
		if ref, declared := value["$ref"]; declared {
			if err := checkRef(ref, document, anchors, location); err != nil {
				return err
			}
		}
		for _, member := range sortedMembers(value) {
			if err := walkRefs(value[member], document, anchors, location+"/"+escapeToken(member)); err != nil {
				return err
			}
		}
	case []any:
		for index, item := range value {
			if err := walkRefs(item, document, anchors, location+"/"+strconv.Itoa(index)); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkRef decides one reference: a #-leading JSON pointer or $anchor naming something in the
// document is legal, and every other form — a relative path, a file:, http: or https: URI, any other
// absolute URI, the JSON-Schema meta-schema's own URL included — is refused (rule T12).
func checkRef(ref, document any, anchors map[string]bool, location string) error {
	text, isText := ref.(string)
	if !isText {
		return fmt.Errorf("the $ref at %q is %v, which is not a string", "#"+location, ref)
	}
	fragment, sameDocument := strings.CutPrefix(text, "#")
	if !sameDocument {
		return fmt.Errorf("the $ref %q at %q is not a same-document reference", text, "#"+location)
	}
	switch {
	case fragment == "":
		return nil
	case strings.HasPrefix(fragment, "/"):
		if !pointerResolves(document, fragment) {
			return fmt.Errorf("the $ref %q at %q names nothing in the document", text, "#"+location)
		}
	case !anchors[fragment]:
		return fmt.Errorf("the $ref %q at %q names nothing in the document", text, "#"+location)
	}
	return nil
}

// pointerResolves walks a JSON pointer over the document, reporting whether it names anything — the
// structural half of "a same-document reference resolving to nothing".
func pointerResolves(document any, fragment string) bool {
	// Rule 6: the pointer is percent-decoded (RFC 3986) before it is split into reference tokens,
	// the order RFC 6901 §6 requires of a URI fragment and the one the compiler below reads. A
	// pointer that will not decode names nothing.
	pointer, err := url.PathUnescape(fragment)
	if err != nil {
		return false
	}
	node := document
	for _, token := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		name := unescapeToken(token)
		switch value := node.(type) {
		case map[string]any:
			member, present := value[name]
			if !present {
				return false
			}
			node = member
		case []any:
			index, err := strconv.Atoi(name)
			if err != nil || index < 0 || index >= len(value) {
				return false
			}
			node = value[index]
		default:
			return false
		}
	}
	return true
}

// unescapeToken decodes one JSON-pointer token: RFC 6901's ~1 and ~0, the percent-decoding of the
// whole pointer having already happened in pointerResolves.
func unescapeToken(token string) string {
	return strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
}

// escapeToken renders one member name as a JSON-pointer token, for the location a refusal names.
func escapeToken(member string) string {
	return strings.ReplaceAll(strings.ReplaceAll(member, "~", "~0"), "/", "~1")
}

// sortedMembers orders an object's members, so the walk is deterministic.
func sortedMembers(object map[string]any) []string {
	return slices.Sorted(maps.Keys(object))
}

// validateSeam applies rule 6 to one seam: a violation is a validation StepError naming the step and
// the JSON-Schema error path, and rule 6 makes it non-retryable whatever the step's retry budget is.
func validateSeam(schema *jsonschema.Schema, encoded []byte, alias, seam string) *StepError {
	if schema == nil {
		return nil
	}
	// The instance is validated from the bytes the rule-2 discipline already produced, so what the
	// schema sees is exactly what a remote binding would put on the wire.
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		return stepError(CauseInternal, false, err, "step %q %s could not be re-read as JSON: %s", alias, seam, err)
	}
	if err := schema.Validate(instance); err != nil {
		return stepError(CauseValidation, false, err,
			"step %q %s failed schema validation at %s", alias, seam, violationPath(err))
	}
	return nil
}

// violationPath renders the first violation as `"<json-pointer>": <message>`, the Go spelling of the
// path-plus-message detail rule 6 requires the error to name.
func violationPath(err error) string {
	var violation *jsonschema.ValidationError
	if !errors.As(err, &violation) {
		return err.Error()
	}
	unit := leafUnit(violation.BasicOutput())
	if unit == nil || unit.Error == nil {
		return err.Error()
	}
	location := unit.InstanceLocation
	if location == "" {
		location = "/"
	}
	return fmt.Sprintf("%q: %s", location, unit.Error)
}

// leafUnit picks the most specific unit of a basic output — the one naming a keyword rather than the
// enclosing schema — so the message reads as one violation rather than a tree.
func leafUnit(unit *jsonschema.OutputUnit) *jsonschema.OutputUnit {
	if unit == nil {
		return nil
	}
	for i := range unit.Errors {
		if unit.Errors[i].Error != nil {
			return &unit.Errors[i]
		}
	}
	return unit
}
