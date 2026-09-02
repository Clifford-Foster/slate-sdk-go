package schema

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// constraintTag is the struct tag a field's constraint keywords ride on (rule D1).
const constraintTag = "bbschema"

// defsPointer is the same-document prefix a nested struct's $ref names (rule D1).
const defsPointer = "#/$defs/"

// jsonMarshaler and textMarshaler name the two interfaces whose implementers encode as something
// other than their fields, so their field shape is not their JSON shape and derivation would lie.
var (
	jsonMarshaler = reflect.TypeFor[json.Marshaler]()
	textMarshaler = reflect.TypeFor[encoding.TextMarshaler]()
)

// DeriveError reports a type the derivable subset does not cover (rule D1).
type DeriveError struct {
	// Field is the dotted path to the offending field, empty when the offending type is the root.
	Field string
	// Construct is the Go construct the subset does not cover.
	Construct string
}

// Error names the field and the construct, and points at the schema-first path (rule D1).
func (e *DeriveError) Error() string {
	where := "the type"
	if e.Field != "" {
		where = fmt.Sprintf("field %q", e.Field)
	}
	return fmt.Sprintf(
		"bbsdk/schema: %s is %s, which the derivable subset does not cover — author the document and take the schema-first path (bb schema gen)",
		where, e.Construct)
}

// Derive turns a struct type into a draft 2020-12 document over the bounded subset (rule D1).
func Derive[T any]() (map[string]any, error) {
	return derive(reflect.TypeFor[T]())
}

// derive is the reflective half of Derive, reached by the shim with a runtime type (rules D1, D2).
func derive(t reflect.Type) (map[string]any, error) {
	if t == nil {
		return nil, &DeriveError{Construct: "nil"}
	}
	if t.Kind() != reflect.Struct || refuseCustomEncoding(t) {
		return nil, &DeriveError{Construct: constructOf(t)}
	}
	d := &deriver{defs: map[string]any{}, named: map[string]reflect.Type{}}
	document, err := d.object(t, "")
	if err != nil {
		return nil, err
	}
	if len(d.defs) > 0 {
		document["$defs"] = d.defs
	}
	return document, nil
}

// deriver carries one derivation's $defs block: every nested struct type lands there under its own
// name, so the document is self-contained and a recursive type terminates (rule D1).
type deriver struct {
	defs  map[string]any
	named map[string]reflect.Type
}

// object derives one struct type's object schema, in field declaration order (rule D1).
func (d *deriver) object(t reflect.Type, path string) (map[string]any, error) {
	properties := map[string]any{}
	required := []string{}
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			// An unexported field is not on the wire either: encoding/json skips it.
			continue
		}
		where := qualify(path, field.Name)
		if field.Anonymous {
			// encoding/json flattens an embedded field's members into the enclosing object, a shape
			// the subset does not describe and Decode's unmarshal would disagree with.
			return nil, &DeriveError{Field: where, Construct: "an embedded field"}
		}
		name, omitempty, skipped := memberName(field)
		if skipped {
			continue
		}
		property, optional, err := d.property(field.Type, where)
		if err != nil {
			return nil, err
		}
		constraints, err := constraints(field, where)
		if err != nil {
			return nil, err
		}
		// Constraint keywords are merged verbatim: the SDK checks nothing about them beyond their
		// JSON encoding, exactly as the toolchain and the runtime judge the resulting document.
		for keyword, value := range constraints {
			property[keyword] = value
		}
		properties[name] = property
		if !optional && !omitempty {
			required = append(required, name)
		}
	}
	// Properties ride a map because the Public API table pins Derive's return type: Go's
	// encoding/json orders a map's members lexicographically, so declaration order is carried by
	// required — a JSON object's member order carries no meaning to any validator.
	document := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		document["required"] = required
	}
	return document, nil
}

// property derives one field's schema, reporting whether the field is optional (rule D1).
func (d *deriver) property(t reflect.Type, where string) (map[string]any, bool, error) {
	if t.Kind() == reflect.Pointer {
		if t.Elem().Kind() == reflect.Pointer {
			return nil, false, &DeriveError{Field: where, Construct: constructOf(t)}
		}
		property, _, err := d.property(t.Elem(), where)
		return property, true, err
	}
	if refuseCustomEncoding(t) {
		return nil, false, &DeriveError{Field: where, Construct: constructOf(t) + " (a custom JSON encoding)"}
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}, false, nil
	case reflect.Bool:
		return map[string]any{"type": "boolean"}, false, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}, false, nil
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}, false, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			// encoding/json writes a byte slice as a base64 string, never as an array of integers.
			return nil, false, &DeriveError{Field: where, Construct: constructOf(t)}
		}
		items, _, err := d.property(t.Elem(), where+"[]")
		if err != nil {
			return nil, false, err
		}
		return map[string]any{"type": "array", "items": items}, false, nil
	case reflect.Struct:
		ref, err := d.nested(t, where)
		if err != nil {
			return nil, false, err
		}
		return ref, false, nil
	default:
		return nil, false, &DeriveError{Field: where, Construct: constructOf(t)}
	}
}

// nested files a nested struct type in $defs and returns the same-document reference to it; the
// registration precedes the derivation, so a self-referencing type terminates (rule D1).
func (d *deriver) nested(t reflect.Type, where string) (map[string]any, error) {
	name := t.Name()
	if name == "" {
		return nil, &DeriveError{Field: where, Construct: "an anonymous struct"}
	}
	if known, filed := d.named[name]; filed {
		if known != t {
			return nil, &DeriveError{
				Field:     where,
				Construct: fmt.Sprintf("a second %s type, which the one $defs name %q cannot hold", name, name),
			}
		}
		return map[string]any{"$ref": defsPointer + name}, nil
	}
	d.named[name] = t
	d.defs[name] = map[string]any{}
	document, err := d.object(t, where)
	if err != nil {
		return nil, err
	}
	d.defs[name] = document
	return map[string]any{"$ref": defsPointer + name}, nil
}

// memberName reads a field's json tag: the member name, whether it is omitempty-tagged, and whether
// the field is skipped entirely (rule D1).
func memberName(field reflect.StructField) (name string, omitempty, skipped bool) {
	tag, tagged := field.Tag.Lookup("json")
	if !tagged {
		return field.Name, false, false
	}
	if tag == "-" {
		return "", false, true
	}
	declared, options, _ := strings.Cut(tag, ",")
	name = declared
	if name == "" {
		name = field.Name
	}
	for _, option := range strings.Split(options, ",") {
		if option == "omitempty" {
			omitempty = true
		}
	}
	return name, omitempty, false
}

// constraints reads a field's bbschema tag into the keywords that merge into its schema (rule D1).
func constraints(field reflect.StructField, where string) (map[string]any, error) {
	tag, tagged := field.Tag.Lookup(constraintTag)
	if !tagged || strings.TrimSpace(tag) == "" {
		return nil, nil
	}
	keywords := map[string]any{}
	for _, entry := range strings.Split(tag, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		keyword, value, assigned := strings.Cut(entry, "=")
		if !assigned || keyword == "" {
			return nil, &DeriveError{Field: where, Construct: fmt.Sprintf("tagged %s:%q, which names no keyword", constraintTag, entry)}
		}
		if keyword == "enum" {
			members := []any{}
			for _, member := range strings.Split(value, "|") {
				members = append(members, scalar(member))
			}
			keywords[keyword] = members
			continue
		}
		keywords[keyword] = scalar(value)
	}
	return keywords, nil
}

// scalar reads one tag value as JSON where it is JSON — a number, a boolean, a quoted string — and
// as a plain string otherwise, which is the whole of the SDK's judgement on a constraint (rule D1).
func scalar(text string) any {
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err == nil {
		return decoded
	}
	return text
}

// refuseCustomEncoding reports a struct whose JSON shape is not its field shape.
func refuseCustomEncoding(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	pointer := reflect.PointerTo(t)
	return t.Implements(jsonMarshaler) || pointer.Implements(jsonMarshaler) ||
		t.Implements(textMarshaler) || pointer.Implements(textMarshaler)
}

// constructOf names a type the way a component author wrote it.
func constructOf(t reflect.Type) string {
	if t == nil {
		return "nil"
	}
	return t.String()
}

// qualify renders the dotted path to a field, from the derived type down.
func qualify(path, field string) string {
	if path == "" {
		return field
	}
	return path + "." + field
}
