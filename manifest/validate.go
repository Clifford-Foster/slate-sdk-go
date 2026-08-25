package manifest

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	bb "github.com/Clifford-Foster/slate-sdk-go/blackboard"
)

// Field defaults the loaded model applies.
const (
	defaultCategory     = "agent"
	deliveryPush        = "push"
	deliveryPull        = "pull"
	activationBroadcast = "broadcast"
)

var (
	nameRE             = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)
	semverRE           = regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$`)
	tagRE              = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)
	configNameRE       = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	payloadAliasRE     = regexp.MustCompile(`^[a-z][a-z0-9_]{0,32}$`)
	arbitrationGroupRE = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	urlRE              = regexp.MustCompile(`(?i)^https?://`)
	readRE             = regexp.MustCompile(`^(?:[A-Za-z0-9_]+|\*)(?:\.(?:[A-Za-z0-9_]+|\*))*(?:\.>)?$`)
	writeRE            = regexp.MustCompile(`^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*(?:\.>)?$`)
	subjectRE          = regexp.MustCompile(`^(?:[A-Za-z0-9_-]+|\*)(?:\.(?:[A-Za-z0-9_-]+|\*))*(?:\.>)?$`)
	literalSubjectRE   = regexp.MustCompile(`^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$`)
)

var (
	categories    = set("agent", "tool", "provider", "persistence", "core", "infrastructure", "integration", "utility")
	deliveries    = set(deliveryPush, deliveryPull)
	activations   = set("exclusive", activationBroadcast)
	configTypes   = set("string", "number", "boolean", "enum")
	reservedPorts = set("8722", "8723")
)

var (
	topFields         = set("name", "version", "description", "reads", "writes", "consumes", "precondition", "activation", "arbitration_group", "events", "service", "invokes", "discovery", "component", "config")
	serviceFields     = set("description", "version", "endpoints")
	endpointFields    = set("name", "subject", "description")
	discoveryFields   = set("category", "skills", "metadata")
	skillFields       = set("id", "name", "description", "tags", "examples")
	componentFields   = set("delivery", "activate_url", "event_url", "rpc_url", "timeout_s", "payloads")
	configFields      = set("fields")
	configFieldFields = set("name", "type", "required", "secret", "description", "enum", "default")
	payloadFields     = set("alias", "name", "version", "config")
)

// List, string, and size limits the schema tables declare.
const (
	maxReads             = 128
	maxWrites            = 128
	maxConsumes          = 128
	maxInvokes           = 32
	maxEvents            = 64
	maxEndpoints         = 32
	maxSkills            = 32
	maxTags              = 16
	maxExamples          = 8
	maxDescription       = 500
	maxSkillName         = 200
	maxURL               = 2048
	maxMetadataBytes     = 8 * 1024
	minTimeoutS          = 1
	maxTimeoutS          = 3600
	maxConfigFields      = 32
	maxConfigDescription = 200
	maxPayloads          = 32
)

// fieldOrder is the document order findings sort by; an unlisted first segment sorts last (rule 4).
var fieldOrder = map[string]int{
	"": 0, "name": 1, "version": 2, "description": 3, "reads": 4, "writes": 5,
	"consumes": 6, "precondition": 7, "activation": 8, "arbitration_group": 9,
	"events": 10, "service": 11, "invokes": 12, "discovery": 13, "component": 14, "config": 15,
}

// Validate reports every finding in a parsed manifest mapping, in deterministic order; it performs no I/O (rule 18).
func Validate(data map[string]any) []Finding {
	if data == nil {
		return []Finding{{Code: "MANIFEST_PARSE", Path: "", Message: "Manifest root must be a mapping", Severity: SeverityError}}
	}
	v := &validator{}
	v.run(data)
	sort.SliceStable(v.findings, func(i, j int) bool {
		left, right := v.findings[i], v.findings[j]
		if order := orderOf(left.Path); order != orderOf(right.Path) {
			return order < orderOf(right.Path)
		}
		if left.Path != right.Path {
			return left.Path < right.Path
		}
		return left.Code < right.Code
	})
	return v.findings
}

// ValidWritePattern reports whether a pattern satisfies the manifest write-pattern grammar (§Key and subject grammars).
func ValidWritePattern(pattern string) bool {
	code, _ := writeCheck(pattern)
	return code == ""
}

// ValidComponentName reports whether a name matches the manifest top-level name grammar.
func ValidComponentName(name string) bool {
	return nameRE.MatchString(name)
}

// orderOf reports the document position of a finding's path, keyed on its first segment.
func orderOf(path string) int {
	segment := path
	if index := strings.IndexAny(path, ".["); index >= 0 {
		segment = path[:index]
	}
	if order, ok := fieldOrder[segment]; ok {
		return order
	}
	return 99
}

type validator struct {
	findings []Finding
}

func (v *validator) add(code, path, message string) {
	v.findings = append(v.findings, Finding{Code: code, Path: path, Message: message, Severity: SeverityError})
}

func (v *validator) warn(code, path, message string) {
	v.findings = append(v.findings, Finding{Code: code, Path: path, Message: message, Severity: SeverityWarning})
}

func (v *validator) run(data map[string]any) {
	v.unknownFields(data, topFields, "")

	v.identifier(data, "name", "name", true)
	v.semver(data, "version", "version", true)
	v.topDescription(data)

	reads := v.patternList(data["reads"], "reads", maxReads, readCheck)
	v.patternList(data["writes"], "writes", maxWrites, writeCheck)
	v.patternList(data["events"], "events", maxEvents, eventCheck)
	v.consumes(data["consumes"], reads)

	v.precondition(data, reads)
	v.activation(data)
	v.arbitrationGroup(data)

	service, hasService := data["service"]
	if hasService {
		v.service(service)
	}
	v.invokes(data["invokes"], data["name"])
	if discovery, ok := data["discovery"]; ok {
		v.discovery(discovery)
	}

	hasReads := listLen(data["reads"]) > 0
	hasEvents := listLen(data["events"]) > 0
	hasWrites := listLen(data["writes"]) > 0

	if component, ok := data["component"]; ok {
		v.component(component, hasEvents, hasService)
	} else {
		v.add("FIELD_REQUIRED", "component", "Field 'component' is required")
	}

	if config, ok := data["config"]; ok {
		v.config(config)
	}

	if !hasReads && !hasEvents && !hasService && !hasWrites {
		v.add("INERT_COMPONENT", "", "A component must declare at least one of reads, writes, events, or service")
	}
}

// unknownFields reports every key outside the level's schema (rule 3).
func (v *validator) unknownFields(data map[string]any, allowed map[string]bool, prefix string) {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !allowed[key] {
			// The top-level pass reprs the bare key; the nested passes print the dotted path plain.
			label := prefix + key
			if prefix == "" {
				label = pyQuote(key)
			}
			v.add("UNKNOWN_FIELD", prefix+key, "Unknown field "+label)
		}
	}
}

// --- top-level scalars ---

func (v *validator) identifier(data map[string]any, key, path string, required bool) {
	raw, present := data[key]
	if !present {
		if required {
			v.add("FIELD_REQUIRED", path, fmt.Sprintf("Field %s is required", pyQuote(path)))
		}
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, fmt.Sprintf("Field %s must be a string", pyQuote(path)))
		return
	}
	if !nameRE.MatchString(text) {
		v.add("NAME_INVALID", path, fmt.Sprintf("%s does not match ^[A-Za-z][A-Za-z0-9_-]{0,63}$", pyQuote(path)))
	}
}

func (v *validator) semver(data map[string]any, key, path string, required bool) {
	raw, present := data[key]
	if !present {
		if required {
			v.add("FIELD_REQUIRED", path, fmt.Sprintf("Field %s is required", pyQuote(path)))
		}
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, fmt.Sprintf("Field %s must be a string", pyQuote(path)))
		return
	}
	if !semverRE.MatchString(text) {
		v.add("VERSION_INVALID", path, fmt.Sprintf("%s is not a valid semantic version", pyQuote(path)))
	}
}

func (v *validator) topDescription(data map[string]any) {
	raw, present := data["description"]
	if !present {
		v.add("FIELD_REQUIRED", "description", "Field 'description' is required")
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", "description", "Field 'description' must be a string")
		return
	}
	if text == "" || utf8.RuneCountInString(text) > maxDescription {
		v.add("DESCRIPTION_INVALID", "description", "description must be non-empty and at most 500 characters")
	}
}

// --- list fields ---

// patternCheck reports a grammar violation for one list entry, or empty strings when it is well formed.
type patternCheck func(entry string) (code, message string)

func (v *validator) patternList(raw any, field string, limit int, check patternCheck) []string {
	if raw == nil {
		return nil
	}
	entries, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", field, fmt.Sprintf("Field %s must be a list", pyQuote(field)))
		return nil
	}
	if len(entries) > limit {
		v.add("LIMIT_EXCEEDED", field, fmt.Sprintf("%s may have at most %d entries", pyQuote(field), limit))
	}
	seen := map[string]bool{}
	values := []string{}
	for i, raw := range entries {
		path := fmt.Sprintf("%s[%d]", field, i)
		entry, ok := raw.(string)
		if !ok {
			v.add("FIELD_TYPE", path, "Entry must be a string")
			continue
		}
		values = append(values, entry)
		if seen[entry] {
			v.add("DUPLICATE_ENTRY", path, fmt.Sprintf("Duplicate entry %s", pyQuote(entry)))
			continue
		}
		seen[entry] = true
		if code, message := check(entry); code != "" {
			v.add(code, path, message)
		}
	}
	return values
}

func readCheck(entry string) (string, string) {
	if entry == ">" { // the bare ">" whole-workspace watch; writes still reject it
		return "", ""
	}
	if !readRE.MatchString(entry) {
		return "KEY_PATTERN_INVALID", fmt.Sprintf("%s is not a valid read pattern", pyQuote(entry))
	}
	return "", ""
}

func writeCheck(entry string) (string, string) {
	if entry == ">" || strings.Split(entry, ".")[0] == "meta" {
		return "WRITES_RESERVED", fmt.Sprintf("%s could match a reserved meta.* key", pyQuote(entry))
	}
	if !writeRE.MatchString(entry) {
		return "KEY_PATTERN_INVALID", fmt.Sprintf("%s is not a valid write pattern", pyQuote(entry))
	}
	return "", ""
}

func eventCheck(entry string) (string, string) {
	if !subjectRE.MatchString(entry) {
		return "SUBJECT_INVALID", fmt.Sprintf("%s is not a valid subject pattern", pyQuote(entry))
	}
	return "", ""
}

// --- consumes (rule 29) ---

func (v *validator) consumes(raw any, reads []string) {
	if raw == nil {
		return
	}
	entries, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", "consumes", "Field 'consumes' must be a list")
		return
	}
	if len(entries) > maxConsumes {
		v.add("LIMIT_EXCEEDED", "consumes", fmt.Sprintf("'consumes' may have at most %d entries", maxConsumes))
	}
	seen := map[string]bool{}
	for i, raw := range entries {
		path := fmt.Sprintf("consumes[%d]", i)
		entry, ok := raw.(string)
		if !ok {
			v.add("FIELD_TYPE", path, "Entry must be a string")
			continue
		}
		if seen[entry] {
			v.add("DUPLICATE_ENTRY", path, fmt.Sprintf("Duplicate entry %s", pyQuote(entry)))
			continue
		}
		seen[entry] = true
		if code, message := readCheck(entry); code != "" {
			v.add(code, path, message)
			continue
		}
		if !intersectsAny(entry, reads) {
			v.add("CONSUMES_UNREAD", path, fmt.Sprintf(
				"%s intersects no reads pattern — a consumed key could never have triggered this component",
				pyQuote(entry)))
		}
	}
}

func intersectsAny(entry string, reads []string) bool {
	for _, read := range reads {
		if PatternsIntersect(entry, read) {
			return true
		}
	}
	return false
}

// --- invokes (rule 31) ---

func (v *validator) invokes(raw, name any) {
	if raw == nil {
		return
	}
	entries, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", "invokes", "Field 'invokes' must be a list")
		return
	}
	if len(entries) > maxInvokes {
		v.add("LIMIT_EXCEEDED", "invokes", fmt.Sprintf("'invokes' may have at most %d entries", maxInvokes))
	}
	component, named := name.(string)
	seen := map[string]bool{}
	for i, raw := range entries {
		path := fmt.Sprintf("invokes[%d]", i)
		entry, ok := raw.(string)
		if !ok {
			v.add("FIELD_TYPE", path, "Entry must be a string")
			continue
		}
		if seen[entry] {
			v.add("DUPLICATE_ENTRY", path, fmt.Sprintf("Duplicate entry %s", pyQuote(entry)))
			continue
		}
		seen[entry] = true
		// Split on the FIRST dot: neither the service-name nor the endpoint-name grammar admits one,
		// so a second dot lands in the endpoint half and fails there.
		service, endpoint, found := strings.Cut(entry, ".")
		if !found || !nameRE.MatchString(service) || !nameRE.MatchString(endpoint) {
			// A malformed entry short-circuits the self check — it never yields a second finding.
			v.add("INVOKES_ENTRY_INVALID", path,
				fmt.Sprintf("%s is not a valid '<service>.<endpoint>' grant", pyQuote(entry)))
			continue
		}
		if named && service == component {
			v.add("INVOKES_SELF", path, fmt.Sprintf(
				"%s names this component's own service — a component calls its own code directly", pyQuote(entry)))
		}
	}
}

// --- precondition ---

func (v *validator) precondition(data map[string]any, reads []string) {
	raw, present := data["precondition"]
	if !present {
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", "precondition", "Field 'precondition' must be a string")
		return
	}
	parsed, err := bb.ParsePrecondition(text)
	if err != nil {
		v.add("PRECONDITION_INVALID", "precondition", err.Error())
		return
	}
	for _, key := range parsed.ReferencedKeys() {
		if coveredByAny(key, reads) {
			continue
		}
		v.warn("PRECONDITION_UNWATCHED_KEY", "precondition", fmt.Sprintf(
			"Precondition key %s is not covered by any reads pattern — the precondition can never see it at runtime",
			pyQuote(key)))
	}
}

func coveredByAny(key string, reads []string) bool {
	for _, read := range reads {
		if patternCovers(read, key) {
			return true
		}
	}
	return false
}

func (v *validator) activation(data map[string]any) {
	raw, present := data["activation"]
	if !present {
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", "activation", "Field 'activation' must be a string")
		return
	}
	if !activations[text] {
		v.add("ENUM_INVALID", "activation", fmt.Sprintf("%s is not 'exclusive' or 'broadcast'", pyQuote(text)))
	}
}

// arbitrationGroup validates the group grammar and its exclusive-only precondition (rule 30).
func (v *validator) arbitrationGroup(data map[string]any) {
	raw, present := data["arbitration_group"]
	if !present {
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", "arbitration_group", "Field 'arbitration_group' must be a string")
		return
	}
	if !arbitrationGroupRE.MatchString(text) {
		v.add("ARBITRATION_GROUP_INVALID", "arbitration_group",
			fmt.Sprintf("%s does not match ^[a-z][a-z0-9_-]{0,63}$", pyQuote(text)))
	}
	// An invalid `activation` value reports its own error, so this fires only on a resolved broadcast.
	if stringOr(data, "activation", activationBroadcast) == activationBroadcast {
		v.add("ARBITRATION_REQUIRES_EXCLUSIVE", "arbitration_group",
			"arbitration_group requires activation: 'exclusive' — arbitration is a property of the "+
				"exclusive claim, meaningless under broadcast")
	}
}

// --- service ---

func (v *validator) service(declared any) {
	block, ok := declared.(map[string]any)
	if !ok {
		v.add("FIELD_TYPE", "service", "Field 'service' must be a mapping")
		return
	}
	v.unknownFields(block, serviceFields, "service.")
	if description, ok := block["description"]; ok {
		v.optionalText(description, "service.description", maxDescription)
	}
	v.semver(block, "version", "service.version", false)

	raw, present := block["endpoints"]
	if !present {
		v.add("FIELD_REQUIRED", "service.endpoints", "service requires a non-empty 'endpoints' list")
		return
	}
	endpoints, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", "service.endpoints", "service.endpoints must be a list")
		return
	}
	if len(endpoints) == 0 {
		v.add("FIELD_REQUIRED", "service.endpoints", "service requires a non-empty 'endpoints' list")
		return
	}
	if len(endpoints) > maxEndpoints {
		v.add("LIMIT_EXCEEDED", "service.endpoints", fmt.Sprintf("at most %d endpoints", maxEndpoints))
	}
	seenNames := map[string]bool{}
	seenSubjects := map[string]bool{}
	for i, raw := range endpoints {
		base := fmt.Sprintf("service.endpoints[%d]", i)
		endpoint, ok := raw.(map[string]any)
		if !ok {
			v.add("FIELD_TYPE", base, "Endpoint must be a mapping")
			continue
		}
		v.unknownFields(endpoint, endpointFields, base+".")
		v.endpointName(endpoint, base+".name", seenNames)
		v.endpointSubject(endpoint, base+".subject", seenSubjects)
		if description, ok := endpoint["description"]; ok {
			v.optionalText(description, base+".description", maxDescription)
		}
	}
}

func (v *validator) endpointName(endpoint map[string]any, path string, seen map[string]bool) {
	raw, present := endpoint["name"]
	if !present {
		v.add("FIELD_REQUIRED", path, "Endpoint 'name' is required")
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, "Endpoint 'name' must be a string")
		return
	}
	if !nameRE.MatchString(text) {
		v.add("NAME_INVALID", path, fmt.Sprintf("%s does not match ^[A-Za-z][A-Za-z0-9_-]{0,63}$", pyQuote(text)))
		return
	}
	if seen[text] {
		v.add("DUPLICATE_ENTRY", path, fmt.Sprintf("Duplicate endpoint name %s", pyQuote(text)))
	}
	seen[text] = true
}

func (v *validator) endpointSubject(endpoint map[string]any, path string, seen map[string]bool) {
	raw, present := endpoint["subject"]
	if !present {
		v.add("FIELD_REQUIRED", path, "Endpoint 'subject' is required")
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, "Endpoint 'subject' must be a string")
		return
	}
	if !literalSubjectRE.MatchString(text) {
		v.add("SUBJECT_INVALID", path, fmt.Sprintf("%s is not a literal subject", pyQuote(text)))
		return
	}
	if seen[text] {
		v.add("DUPLICATE_ENTRY", path, fmt.Sprintf("Duplicate endpoint subject %s", pyQuote(text)))
	}
	seen[text] = true
}

// --- discovery ---

func (v *validator) discovery(raw any) {
	block, ok := raw.(map[string]any)
	if !ok {
		v.add("FIELD_TYPE", "discovery", "Field 'discovery' must be a mapping")
		return
	}
	v.unknownFields(block, discoveryFields, "discovery.")
	if raw, ok := block["category"]; ok {
		category, ok := raw.(string)
		switch {
		case !ok:
			v.add("FIELD_TYPE", "discovery.category", "discovery.category must be a string")
		case !categories[category]:
			v.add("ENUM_INVALID", "discovery.category", fmt.Sprintf("%s is not a valid category", pyQuote(category)))
		}
	}
	if skills, ok := block["skills"]; ok {
		v.skills(skills)
	}
	if metadata, ok := block["metadata"]; ok {
		v.metadata(metadata)
	}
}

func (v *validator) skills(raw any) {
	entries, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", "discovery.skills", "discovery.skills must be a list")
		return
	}
	if len(entries) > maxSkills {
		v.add("LIMIT_EXCEEDED", "discovery.skills", fmt.Sprintf("at most %d skills", maxSkills))
	}
	seen := map[string]bool{}
	for i, raw := range entries {
		base := fmt.Sprintf("discovery.skills[%d]", i)
		skill, ok := raw.(map[string]any)
		if !ok {
			v.add("FIELD_TYPE", base, "Skill must be a mapping")
			continue
		}
		v.unknownFields(skill, skillFields, base+".")
		v.skillID(skill, base+".id", seen)
		v.requiredText(skill, "name", base+".name", maxSkillName)
		v.requiredText(skill, "description", base+".description", maxDescription)
		if tags, ok := skill["tags"]; ok {
			v.tags(tags, base+".tags")
		}
		if examples, ok := skill["examples"]; ok {
			v.examples(examples, base+".examples")
		}
	}
}

func (v *validator) skillID(skill map[string]any, path string, seen map[string]bool) {
	raw, present := skill["id"]
	if !present {
		v.add("FIELD_REQUIRED", path, "Skill 'id' is required")
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, "Skill 'id' must be a string")
		return
	}
	if !nameRE.MatchString(text) {
		v.add("NAME_INVALID", path, fmt.Sprintf("%s does not match ^[A-Za-z][A-Za-z0-9_-]{0,63}$", pyQuote(text)))
		return
	}
	if seen[text] {
		v.add("DUPLICATE_ENTRY", path, fmt.Sprintf("Duplicate skill id %s", pyQuote(text)))
	}
	seen[text] = true
}

func (v *validator) tags(raw any, path string) {
	entries, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", path, "tags must be a list")
		return
	}
	if len(entries) > maxTags {
		v.add("LIMIT_EXCEEDED", path, fmt.Sprintf("at most %d tags", maxTags))
	}
	for i, raw := range entries {
		tagPath := fmt.Sprintf("%s[%d]", path, i)
		tag, ok := raw.(string)
		if !ok {
			v.add("FIELD_TYPE", tagPath, "tag must be a string")
			continue
		}
		if !tagRE.MatchString(tag) {
			v.add("NAME_INVALID", tagPath, fmt.Sprintf("%s does not match ^[a-z0-9_-]{1,32}$", pyQuote(tag)))
		}
	}
}

func (v *validator) examples(raw any, path string) {
	entries, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", path, "examples must be a list")
		return
	}
	if len(entries) > maxExamples {
		v.add("LIMIT_EXCEEDED", path, fmt.Sprintf("at most %d examples", maxExamples))
	}
	for i, raw := range entries {
		if _, ok := raw.(string); !ok {
			v.add("FIELD_TYPE", fmt.Sprintf("%s[%d]", path, i), "example must be a string")
		}
	}
}

func (v *validator) metadata(raw any) {
	block, ok := raw.(map[string]any)
	if !ok {
		v.add("FIELD_TYPE", "discovery.metadata", "discovery.metadata must be a mapping")
		return
	}
	serialized, err := json.Marshal(block)
	if err != nil {
		v.add("FIELD_TYPE", "discovery.metadata", "discovery.metadata must be JSON-serializable")
		return
	}
	if len(serialized) > maxMetadataBytes {
		v.add("LIMIT_EXCEEDED", "discovery.metadata", "metadata exceeds 8 KB serialized")
	}
}

// --- component ---

func (v *validator) component(raw any, hasEvents, hasService bool) {
	block, ok := raw.(map[string]any)
	if !ok {
		v.add("FIELD_TYPE", "component", "Field 'component' must be a mapping")
		return
	}
	v.unknownFields(block, componentFields, "component.")

	delivery := deliveryPush
	deliveryValid := true
	if raw, present := block["delivery"]; present {
		text, ok := raw.(string)
		switch {
		case !ok:
			v.add("FIELD_TYPE", "component.delivery", "component.delivery must be a string")
			deliveryValid = false
		case !deliveries[text]:
			v.add("ENUM_INVALID", "component.delivery", fmt.Sprintf("%s is not 'push' or 'pull'", pyQuote(text)))
			deliveryValid = false
		default:
			delivery = text
		}
	}

	urlFields := []string{"activate_url", "event_url", "rpc_url"}
	for _, field := range urlFields {
		if raw, present := block[field]; present {
			if _, ok := raw.(string); !ok {
				v.add("FIELD_TYPE", "component."+field, fmt.Sprintf("component.%s must be a string", field))
			}
		}
	}

	v.timeout(block)
	v.payloads(block)

	if deliveryValid && delivery == deliveryPull && hasService {
		v.add("DELIVERY_UNSUPPORTED", "component", "delivery: pull with a service is unsupported in this version")
	}

	var allowed map[string]bool
	switch {
	case !deliveryValid:
		allowed = set(urlFields...)
	case delivery == deliveryPush:
		v.gatePush(block, hasEvents, hasService)
		allowed = set("activate_url")
		if hasEvents {
			allowed["event_url"] = true
		}
		if hasService {
			allowed["rpc_url"] = true
		}
	default:
		for _, field := range urlFields {
			if _, present := block[field]; present {
				v.add("DELIVERY_CONFLICT", "component."+field,
					fmt.Sprintf("component.%s is forbidden for pull delivery", field))
			}
		}
		allowed = map[string]bool{}
	}

	for _, field := range urlFields {
		if !allowed[field] {
			continue
		}
		text, ok := block[field].(string)
		if !ok {
			continue
		}
		v.componentURL(text, "component."+field)
	}
}

// componentURL validates one callback URL's grammar, length, and reserved-port rule (rules 13, 13a).
func (v *validator) componentURL(raw, path string) {
	if !urlRE.MatchString(raw) || utf8.RuneCountInString(raw) > maxURL {
		v.add("URL_INVALID", path, fmt.Sprintf("%s must be an absolute http(s) URL", path))
		return
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return
	}
	if port := parsed.Port(); reservedPorts[port] {
		v.add("PORT_RESERVED", path, fmt.Sprintf("%s names reserved port %s — the sidecar's own listeners", path, port))
	}
}

// gatePush enforces which callback URLs a push manifest must and must not carry (rule 13).
func (v *validator) gatePush(block map[string]any, hasEvents, hasService bool) {
	// activate_url is optional for push: absent, the loaded model applies the default.
	_, hasEventURL := block["event_url"]
	if hasEvents && !hasEventURL {
		v.add("DELIVERY_CONFLICT", "component.event_url", "event_url is required when events are declared")
	}
	if !hasEvents && hasEventURL {
		v.add("DELIVERY_CONFLICT", "component.event_url", "event_url is forbidden when no events are declared")
	}
	_, hasRPCURL := block["rpc_url"]
	if hasService && !hasRPCURL {
		v.add("DELIVERY_CONFLICT", "component.rpc_url", "rpc_url is required when a service is declared")
	}
	if !hasService && hasRPCURL {
		v.add("DELIVERY_CONFLICT", "component.rpc_url", "rpc_url is forbidden when no service is declared")
	}
}

func (v *validator) timeout(block map[string]any) {
	raw, present := block["timeout_s"]
	if !present {
		return
	}
	seconds, ok := asInt(raw)
	if !ok {
		v.add("FIELD_TYPE", "component.timeout_s", "component.timeout_s must be an integer")
		return
	}
	if seconds < minTimeoutS || seconds > maxTimeoutS {
		v.add("TIMEOUT_INVALID", "component.timeout_s",
			fmt.Sprintf("component.timeout_s must be in [%d, %d]", minTimeoutS, maxTimeoutS))
	}
}

// --- payloads (rule 32) ---

// payloads validates the delivered-payload declarations: four required members each, no cross-check
// against this document's own config block (rule 32).
func (v *validator) payloads(block map[string]any) {
	raw, present := block["payloads"]
	if !present {
		return
	}
	entries, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", "component.payloads", "component.payloads must be a list")
		return
	}
	if len(entries) > maxPayloads {
		v.add("LIMIT_EXCEEDED", "component.payloads", fmt.Sprintf("at most %d payloads", maxPayloads))
	}
	seen := map[string]bool{}
	for i, entry := range entries {
		v.payload(entry, fmt.Sprintf("component.payloads[%d]", i), seen)
	}
}

// payload validates one declaration; every finding uses an existing error family (rule 32).
func (v *validator) payload(declared any, base string, seen map[string]bool) {
	entry, ok := declared.(map[string]any)
	if !ok {
		v.add("FIELD_TYPE", base, "payload must be a mapping")
		return
	}
	v.unknownFields(entry, payloadFields, base+".")
	v.payloadAlias(entry, base+".alias", seen)
	v.payloadMember(entry, "name", base+".name", nameRE, "NAME_INVALID",
		"does not match ^[A-Za-z][A-Za-z0-9_-]{0,63}$")
	v.payloadMember(entry, "version", base+".version", semverRE, "VERSION_INVALID",
		"is not a valid semantic version")
	v.payloadMember(entry, "config", base+".config", configNameRE, "NAME_INVALID",
		"does not match ^[a-z][a-z0-9_]{0,63}$")
}

// payloadAlias validates the delivery slot's name and its uniqueness within the list.
func (v *validator) payloadAlias(entry map[string]any, path string, seen map[string]bool) {
	raw, present := entry["alias"]
	if !present {
		v.add("FIELD_REQUIRED", path, path+" is required")
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, path+" must be a string")
		return
	}
	if !payloadAliasRE.MatchString(text) {
		v.add("NAME_INVALID", path, fmt.Sprintf("%s does not match ^[a-z][a-z0-9_]{0,32}$", pyQuote(text)))
		return
	}
	if seen[text] {
		v.add("DUPLICATE_ENTRY", path, fmt.Sprintf("Duplicate payload alias %s", pyQuote(text)))
	}
	seen[text] = true
}

// payloadMember validates one required string member against its grammar.
func (v *validator) payloadMember(
	entry map[string]any, key, path string, grammar *regexp.Regexp, code, detail string,
) {
	raw, present := entry[key]
	if !present {
		v.add("FIELD_REQUIRED", path, path+" is required")
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, path+" must be a string")
		return
	}
	if !grammar.MatchString(text) {
		v.add(code, path, fmt.Sprintf("%s %s", pyQuote(text), detail))
	}
}

// --- config (rule 24) ---

func (v *validator) config(declared any) {
	block, ok := declared.(map[string]any)
	if !ok {
		v.add("FIELD_TYPE", "config", "Field 'config' must be a mapping")
		return
	}
	v.unknownFields(block, configFields, "config.")
	raw, present := block["fields"]
	if !present {
		v.add("FIELD_REQUIRED", "config.fields", "config requires a 'fields' list")
		return
	}
	fields, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", "config.fields", "config.fields must be a list")
		return
	}
	if len(fields) > maxConfigFields {
		v.add("LIMIT_EXCEEDED", "config.fields", fmt.Sprintf("at most %d config fields", maxConfigFields))
	}
	seen := map[string]bool{}
	for i, field := range fields {
		v.configField(field, fmt.Sprintf("config.fields[%d]", i), seen)
	}
}

func (v *validator) configField(declared any, base string, seen map[string]bool) {
	field, ok := declared.(map[string]any)
	if !ok {
		v.add("FIELD_TYPE", base, "config field must be a mapping")
		return
	}
	v.unknownFields(field, configFieldFields, base+".")
	v.configFieldName(field, base+".name", seen)

	declaredType := ""
	raw, present := field["type"]
	text, isString := raw.(string)
	switch {
	case !present:
		v.add("FIELD_REQUIRED", base+".type", base+".type is required")
	case !isString:
		v.add("FIELD_TYPE", base+".type", base+".type must be a string")
	case !configTypes[text]:
		v.add("CONFIG_INVALID", base+".type", fmt.Sprintf("%s is not a valid config field type", pyQuote(text)))
	default:
		declaredType = text
	}

	if raw, present := field["required"]; present {
		if _, ok := raw.(bool); !ok {
			v.add("FIELD_TYPE", base+".required", base+".required must be a boolean")
		}
	}

	secret := false
	if raw, present := field["secret"]; present {
		flag, ok := raw.(bool)
		if !ok {
			v.add("FIELD_TYPE", base+".secret", base+".secret must be a boolean")
		} else {
			secret = flag
			if secret && declaredType != "" && declaredType != "string" {
				v.add("CONFIG_INVALID", base+".secret", "a secret config field must have type 'string'")
			}
		}
	}

	if description, present := field["description"]; present {
		v.optionalText(description, base+".description", maxConfigDescription)
	}

	raw, enumPresent := field["enum"]
	var enumValues []string
	if enumPresent {
		enumValues = v.configEnum(raw, base+".enum")
	}
	switch {
	case declaredType == "enum" && !enumPresent:
		v.add("CONFIG_INVALID", base+".enum", "enum is required when type is 'enum'")
	case declaredType != "" && declaredType != "enum" && enumPresent:
		v.add("CONFIG_INVALID", base+".enum", "enum is forbidden unless type is 'enum'")
	}

	if fallback, present := field["default"]; present {
		switch {
		case secret:
			v.add("CONFIG_INVALID", base+".default", "default is forbidden on a secret field")
		case declaredType != "":
			v.configDefault(fallback, declaredType, enumValues, base+".default")
		}
	}
}

func (v *validator) configFieldName(field map[string]any, path string, seen map[string]bool) {
	raw, present := field["name"]
	if !present {
		v.add("FIELD_REQUIRED", path, path+" is required")
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, path+" must be a string")
		return
	}
	if !configNameRE.MatchString(text) {
		v.add("NAME_INVALID", path, fmt.Sprintf("%s does not match ^[a-z][a-z0-9_]{0,63}$", pyQuote(text)))
		return
	}
	if seen[text] {
		v.add("DUPLICATE_ENTRY", path, fmt.Sprintf("Duplicate config field name %s", pyQuote(text)))
	}
	seen[text] = true
}

// configEnum validates the allowed-value list, returning the well-formed members or nil.
func (v *validator) configEnum(raw any, path string) []string {
	entries, ok := raw.([]any)
	if !ok {
		v.add("FIELD_TYPE", path, path+" must be a list")
		return nil
	}
	values := []string{}
	for i, entry := range entries {
		text, ok := entry.(string)
		if !ok {
			v.add("FIELD_TYPE", fmt.Sprintf("%s[%d]", path, i), "enum entry must be a string")
			continue
		}
		values = append(values, text)
	}
	if len(entries) == 0 {
		v.add("CONFIG_INVALID", path, "enum must be a non-empty list")
		return nil
	}
	return values
}

func (v *validator) configDefault(value any, declaredType string, enumValues []string, path string) {
	ok := false
	switch declaredType {
	case "string":
		_, ok = value.(string)
	case "number":
		if _, isInt := asInt(value); isInt {
			ok = true
		} else {
			_, ok = value.(float64)
		}
	case "boolean":
		_, ok = value.(bool)
	default: // enum
		text, isString := value.(string)
		ok = isString && contains(enumValues, text)
	}
	if !ok {
		v.add("CONFIG_INVALID", path, fmt.Sprintf("default does not match field type %s", pyQuote(declaredType)))
	}
}

// --- shared helpers ---

func (v *validator) optionalText(raw any, path string, limit int) {
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, path+" must be a string")
		return
	}
	if utf8.RuneCountInString(text) > limit {
		v.add("LIMIT_EXCEEDED", path, fmt.Sprintf("%s exceeds %d characters", path, limit))
	}
}

func (v *validator) requiredText(block map[string]any, key, path string, limit int) {
	raw, present := block[key]
	if !present {
		v.add("FIELD_REQUIRED", path, path+" is required")
		return
	}
	text, ok := raw.(string)
	if !ok {
		v.add("FIELD_TYPE", path, path+" must be a string")
		return
	}
	if text == "" {
		v.add("FIELD_REQUIRED", path, path+" must be non-empty")
		return
	}
	if utf8.RuneCountInString(text) > limit {
		v.add("LIMIT_EXCEEDED", path, fmt.Sprintf("%s exceeds %d characters", path, limit))
	}
}

// asInt reports a YAML/JSON integer scalar, excluding booleans and fractional numbers (rule 21).
func asInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case uint64:
		return int(typed), true
	default:
		return 0, false
	}
}

func listLen(value any) int {
	entries, ok := value.([]any)
	if !ok {
		return 0
	}
	return len(entries)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

const hexDigits = "0123456789abcdef"

// pyQuote renders a string the way Python's repr does — the quoting every validator message carries.
func pyQuote(text string) string {
	quote := "'"
	if strings.Contains(text, "'") && !strings.Contains(text, `"`) {
		quote = `"`
	}
	var out strings.Builder
	out.WriteString(quote)
	for _, symbol := range text {
		switch {
		case string(symbol) == quote:
			out.WriteString(`\` + quote)
		case symbol == '\\':
			out.WriteString(`\\`)
		case symbol == '\n':
			out.WriteString(`\n`)
		case symbol == '\r':
			out.WriteString(`\r`)
		case symbol == '\t':
			out.WriteString(`\t`)
		case symbol < 0x20:
			out.WriteString(`\x` + string([]byte{hexDigits[(symbol>>4)&0xf], hexDigits[symbol&0xf]}))
		default:
			out.WriteRune(symbol)
		}
	}
	out.WriteString(quote)
	return out.String()
}

func set(values ...string) map[string]bool {
	members := make(map[string]bool, len(values))
	for _, value := range values {
		members[value] = true
	}
	return members
}
