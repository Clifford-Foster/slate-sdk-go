package manifest

import (
	bb "github.com/Clifford-Foster/slate-sdk-go/blackboard"
)

// DefaultActivateURL is the push webhook a manifest without an explicit activate_url resolves to (rule 13).
const DefaultActivateURL = "http://127.0.0.1:8080/activate"

// Endpoint is one NATS Micro endpoint a manifest service declares.
type Endpoint struct {
	Name    string
	Subject string
	// Description is empty when the manifest declares none.
	Description string
}

// Service is a manifest's service block: its NATS Micro endpoints and optional metadata.
type Service struct {
	Endpoints []Endpoint
	// Description is empty when the manifest declares none.
	Description string
	// Version defaults to the manifest's top-level version.
	Version string
}

// Skill is one discovery skill a manifest declares.
type Skill struct {
	ID          string
	Name        string
	Description string
	Tags        []string
	// Examples is nil when the manifest declares none, distinct from a declared empty list.
	Examples []string
}

// Discovery is a manifest's discovery block: category, skills, and free-form metadata.
type Discovery struct {
	Category string
	Skills   []Skill
	// Metadata is nil when the manifest declares none.
	Metadata map[string]any
}

// Payload is one published component payload a unit requires delivered before its container starts (rule 32).
type Payload struct {
	Alias   string
	Name    string
	Version string
	Config  string
}

// Component is a manifest's component block: the delivery mode and the sidecar's callback URLs.
type Component struct {
	Delivery string
	// ActivateURL, EventURL, and RPCURL are empty when the delivery mode carries none.
	ActivateURL string
	EventURL    string
	RPCURL      string
	// TimeoutS is the per-activation budget in seconds, zero when the manifest declares none.
	TimeoutS int
	// Payloads are the published payloads delivered before the component container starts, empty when none are declared.
	Payloads []Payload
}

// Manifest is a validated component manifest with every default applied.
type Manifest struct {
	Name        string
	Version     string
	Description string
	Component   Component
	Reads       []string
	Writes      []string
	Consumes    []string
	// Precondition is empty when the manifest declares none.
	Precondition string
	Activation   string
	// ArbitrationGroup is empty when the manifest declares none.
	ArbitrationGroup string
	Events           []string
	// Service is nil when the manifest declares no service block.
	Service *Service
	Invokes []string
	// Config carries the instance-configuration field declarations, nil when there is no config block.
	Config    []map[string]any
	Discovery Discovery
}

// DerivedCard derives the single ComponentCard this manifest publishes (rule 20).
func (m *Manifest) DerivedCard() bb.ComponentCard {
	skills := make([]bb.Skill, 0, len(m.Discovery.Skills))
	for _, skill := range m.Discovery.Skills {
		skills = append(skills, bb.Skill{
			ID:          skill.ID,
			Name:        skill.Name,
			Description: skill.Description,
			Tags:        copyStrings(skill.Tags),
			Examples:    copyStrings(skill.Examples),
		})
	}
	var subjects map[string]string
	if m.Service != nil {
		subjects = make(map[string]string, len(m.Service.Endpoints))
		for _, endpoint := range m.Service.Endpoints {
			subjects[endpoint.Name] = endpoint.Subject
		}
	}
	// Rules 30 and 31: the arbitration group and the outbound grants ride the card's generic extras
	// slot ("who arbitrates with whom" and "who calls whom" are capability metadata), each a copy.
	metadata := copyMap(m.Discovery.Metadata)
	if m.ArbitrationGroup != "" {
		metadata = ensureMap(metadata)
		metadata["arbitration_group"] = m.ArbitrationGroup
	}
	if len(m.Invokes) > 0 {
		metadata = ensureMap(metadata)
		metadata["invokes"] = copyStrings(m.Invokes)
	}
	var config []map[string]any
	if m.Config != nil {
		config = make([]map[string]any, 0, len(m.Config))
		for _, field := range m.Config {
			config = append(config, copyMap(field))
		}
	}
	return bb.ComponentCard{
		Name:         m.Name,
		Description:  m.Description,
		Version:      m.Version,
		Category:     m.Discovery.Category,
		Skills:       skills,
		Subjects:     subjects,
		Reads:        nonEmpty(m.Reads),
		Writes:       nonEmpty(m.Writes),
		Precondition: m.Precondition,
		Config:       config,
		Metadata:     metadata,
	}
}

// ToMap returns the round-trippable plain-map form that re-validates cleanly (rule 19).
func (m *Manifest) ToMap() map[string]any {
	doc := map[string]any{"name": m.Name, "version": m.Version, "description": m.Description}
	putList(doc, "reads", m.Reads)
	putList(doc, "writes", m.Writes)
	putList(doc, "consumes", m.Consumes)
	if m.Precondition != "" {
		doc["precondition"] = m.Precondition
	}
	if m.Activation != activationBroadcast {
		doc["activation"] = m.Activation
	}
	if m.ArbitrationGroup != "" {
		doc["arbitration_group"] = m.ArbitrationGroup
	}
	putList(doc, "events", m.Events)
	if m.Service != nil {
		endpoints := make([]any, 0, len(m.Service.Endpoints))
		for _, endpoint := range m.Service.Endpoints {
			entry := map[string]any{"name": endpoint.Name, "subject": endpoint.Subject}
			if endpoint.Description != "" {
				entry["description"] = endpoint.Description
			}
			endpoints = append(endpoints, entry)
		}
		service := map[string]any{"endpoints": endpoints}
		if m.Service.Description != "" {
			service["description"] = m.Service.Description
		}
		if m.Service.Version != "" && m.Service.Version != m.Version {
			service["version"] = m.Service.Version
		}
		doc["service"] = service
	}
	putList(doc, "invokes", m.Invokes)
	if discovery := m.discoveryMap(); len(discovery) > 0 {
		doc["discovery"] = discovery
	}
	component := map[string]any{"delivery": m.Component.Delivery}
	for field, url := range map[string]string{
		"activate_url": m.Component.ActivateURL,
		"event_url":    m.Component.EventURL,
		"rpc_url":      m.Component.RPCURL,
	} {
		if url != "" {
			component[field] = url
		}
	}
	if m.Component.TimeoutS != 0 {
		component["timeout_s"] = m.Component.TimeoutS
	}
	// Rule 32: payloads ride the round trip only when declared, so a payload-free manifest is
	// byte-for-byte the document it was before the rule existed.
	if len(m.Component.Payloads) > 0 {
		payloads := make([]any, 0, len(m.Component.Payloads))
		for _, payload := range m.Component.Payloads {
			payloads = append(payloads, map[string]any{
				"alias": payload.Alias, "name": payload.Name,
				"version": payload.Version, "config": payload.Config,
			})
		}
		component["payloads"] = payloads
	}
	doc["component"] = component
	if m.Config != nil {
		fields := make([]any, 0, len(m.Config))
		for _, field := range m.Config {
			fields = append(fields, copyMap(field))
		}
		doc["config"] = map[string]any{"fields": fields}
	}
	return doc
}

// discoveryMap renders the discovery block, omitting every member left at its default.
func (m *Manifest) discoveryMap() map[string]any {
	discovery := map[string]any{}
	if m.Discovery.Category != defaultCategory {
		discovery["category"] = m.Discovery.Category
	}
	if len(m.Discovery.Skills) > 0 {
		skills := make([]any, 0, len(m.Discovery.Skills))
		for _, skill := range m.Discovery.Skills {
			entry := map[string]any{"id": skill.ID, "name": skill.Name, "description": skill.Description}
			putList(entry, "tags", skill.Tags)
			if skill.Examples != nil {
				entry["examples"] = anyList(skill.Examples)
			}
			skills = append(skills, entry)
		}
		discovery["skills"] = skills
	}
	if m.Discovery.Metadata != nil {
		discovery["metadata"] = copyMap(m.Discovery.Metadata)
	}
	return discovery
}

// build constructs a Manifest from an already-validated manifest mapping.
func build(data map[string]any) *Manifest {
	version := stringOr(data, "version", "")
	component := mapOf(data["component"])
	delivery := stringOr(component, "delivery", deliveryPush)
	activateURL := stringOr(component, "activate_url", "")
	if delivery == deliveryPush && activateURL == "" {
		// Rule 13: an absent push activate_url resolves to the default on the loaded model.
		activateURL = DefaultActivateURL
	}
	loaded := &Manifest{
		Name:        stringOr(data, "name", ""),
		Version:     version,
		Description: stringOr(data, "description", ""),
		Component: Component{
			Delivery:    delivery,
			ActivateURL: activateURL,
			EventURL:    stringOr(component, "event_url", ""),
			RPCURL:      stringOr(component, "rpc_url", ""),
			TimeoutS:    intOr(component, "timeout_s"),
			Payloads:    buildPayloads(component["payloads"]),
		},
		Reads:            stringList(data["reads"]),
		Writes:           stringList(data["writes"]),
		Consumes:         stringList(data["consumes"]),
		Precondition:     stringOr(data, "precondition", ""),
		Activation:       stringOr(data, "activation", activationBroadcast),
		ArbitrationGroup: stringOr(data, "arbitration_group", ""),
		Events:           stringList(data["events"]),
		Invokes:          stringList(data["invokes"]),
		Discovery:        buildDiscovery(data["discovery"]),
	}
	if service := mapOf(data["service"]); service != nil {
		loaded.Service = buildService(service, version)
	}
	if config := mapOf(data["config"]); config != nil {
		loaded.Config = buildConfig(config)
	}
	return loaded
}

// buildService reads a validated service block, defaulting its version to the manifest's.
func buildService(raw map[string]any, topVersion string) *Service {
	entries := listOf(raw["endpoints"])
	endpoints := make([]Endpoint, 0, len(entries))
	for _, entry := range entries {
		endpoint := mapOf(entry)
		endpoints = append(endpoints, Endpoint{
			Name:        stringOr(endpoint, "name", ""),
			Subject:     stringOr(endpoint, "subject", ""),
			Description: stringOr(endpoint, "description", ""),
		})
	}
	return &Service{
		Endpoints:   endpoints,
		Description: stringOr(raw, "description", ""),
		Version:     stringOr(raw, "version", topVersion),
	}
}

// buildPayloads reads a validated payloads list into its four-member entries (rule 32).
func buildPayloads(raw any) []Payload {
	entries := listOf(raw)
	payloads := make([]Payload, 0, len(entries))
	for _, entry := range entries {
		declared := mapOf(entry)
		payloads = append(payloads, Payload{
			Alias:   stringOr(declared, "alias", ""),
			Name:    stringOr(declared, "name", ""),
			Version: stringOr(declared, "version", ""),
			Config:  stringOr(declared, "config", ""),
		})
	}
	return payloads
}

// buildDiscovery reads a validated discovery block, applying the default category.
func buildDiscovery(raw any) Discovery {
	block := mapOf(raw)
	discovery := Discovery{Category: stringOr(block, "category", defaultCategory), Skills: []Skill{}}
	if metadata := mapOf(block["metadata"]); metadata != nil {
		discovery.Metadata = copyMap(metadata)
	}
	for _, entry := range listOf(block["skills"]) {
		declared := mapOf(entry)
		skill := Skill{
			ID:          stringOr(declared, "id", ""),
			Name:        stringOr(declared, "name", ""),
			Description: stringOr(declared, "description", ""),
			Tags:        stringList(declared["tags"]),
		}
		if _, ok := declared["examples"]; ok {
			skill.Examples = stringList(declared["examples"])
		}
		discovery.Skills = append(discovery.Skills, skill)
	}
	return discovery
}

// buildConfig reads a validated config block into its normalized field declarations (rule 24).
func buildConfig(raw map[string]any) []map[string]any {
	entries := listOf(raw["fields"])
	fields := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		declared := mapOf(entry)
		field := map[string]any{
			"name":     stringOr(declared, "name", ""),
			"type":     stringOr(declared, "type", ""),
			"required": boolOr(declared, "required"),
			"secret":   boolOr(declared, "secret"),
		}
		if description, ok := declared["description"]; ok {
			field["description"] = description
		}
		if values, ok := declared["enum"]; ok {
			field["enum"] = anyList(stringList(values))
		}
		if fallback, ok := declared["default"]; ok {
			field["default"] = fallback
		}
		fields = append(fields, field)
	}
	return fields
}

func stringOr(data map[string]any, key, fallback string) string {
	if value, ok := data[key].(string); ok {
		return value
	}
	return fallback
}

func boolOr(data map[string]any, key string) bool {
	value, ok := data[key].(bool)
	return ok && value
}

// mapOf reads a validated nested block, yielding nil when the key carries none.
func mapOf(value any) map[string]any {
	block, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	return block
}

// listOf reads a validated list field, yielding nil when the key carries none.
func listOf(value any) []any {
	entries, ok := value.([]any)
	if !ok {
		return nil
	}
	return entries
}

func intOr(data map[string]any, key string) int {
	value, ok := asInt(data[key])
	if !ok {
		return 0
	}
	return value
}

// stringList reads a validated list field into a non-nil slice of its string entries.
func stringList(value any) []string {
	entries := listOf(value)
	list := make([]string, 0, len(entries))
	for _, entry := range entries {
		if text, ok := entry.(string); ok {
			list = append(list, text)
		}
	}
	return list
}

func anyList(values []string) []any {
	list := make([]any, 0, len(values))
	for _, value := range values {
		list = append(list, value)
	}
	return list
}

func putList(doc map[string]any, key string, values []string) {
	if len(values) > 0 {
		doc[key] = anyList(values)
	}
}

func copyStrings(values []string) []string {
	if values == nil {
		return nil
	}
	copied := make([]string, len(values))
	copy(copied, values)
	return copied
}

func nonEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return copyStrings(values)
}

func copyMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	copied := make(map[string]any, len(value))
	for key, entry := range value {
		copied[key] = entry
	}
	return copied
}

func ensureMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}
