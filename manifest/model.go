package manifest

import (
	bb "github.com/Clifford-Foster/slate-sdk-go/blackboard"
)

// DefaultActivateURL is the push webhook a manifest without an explicit activate_url resolves to (rule 13).
const DefaultActivateURL = "http://127.0.0.1:8080/activate"

// Endpoint is one request/reply endpoint the sidecar bridges to the component (rule 34).
type Endpoint struct {
	Name    string
	Subject string
	// Description is empty when the manifest declares none.
	Description string
}

// Subscription is one subject subscription whose messages activate the component (rule 34).
type Subscription struct {
	Subject string
	Durable bool
	Mode    string
	Strict  bool
	// Schema is nil when the entry declares none.
	Schema any
}

// Publication is one subject the sidecar publishes a returned name on (rule 34).
type Publication struct {
	Subject string
	// Schema is nil when the entry declares none.
	Schema any
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

// ReadExpectation is the shape a component requires of values it reads on a declared pattern (rule 33).
type ReadExpectation struct {
	// Schema is the declared source: an inline document (a mapping) or a package-relative .json path.
	Schema any `json:"schema"`
	// Strict opts the read into the sidecar's strict activation gate.
	Strict bool `json:"strict"`
}

// Component is a manifest's component block: the delivery mode and the sidecar's callback URLs.
type Component struct {
	Delivery string
	// ActivateURL and RPCURL are empty when the delivery mode carries none.
	ActivateURL string
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
	// Communication is where reads and writes live: "blackboard" or "nats" (rule 37).
	Communication string
	Reads         []string
	Writes        []string
	Consumes      []string
	// Precondition is empty when the manifest declares none.
	Precondition string
	Activation   string
	// ArbitrationGroup is empty when the manifest declares none.
	ArbitrationGroup string
	Subscribes       []Subscription
	Publishes        []Publication
	Serves           []Endpoint
	// SingleFlight is whether the sidecar runs at most one activation at a time (rule 35).
	SingleFlight bool
	Invokes      []string
	// Config carries the instance-configuration field declarations, nil when there is no config block.
	Config    []map[string]any
	Discovery Discovery
	// WriteSchemas maps a writes entry to the shape this component claims for it, nil when none are declared.
	WriteSchemas map[string]any
	// ReadExpectations maps a reads entry to its expectation, nil when none are declared.
	ReadExpectations map[string]ReadExpectation
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
	if len(m.Serves) > 0 {
		subjects = make(map[string]string, len(m.Serves))
		for _, endpoint := range m.Serves {
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
	// Rule 33: declared shapes are discovery metadata (the config-schema posture), riding the same
	// extras slot as copies — mutating the card's maps never reaches the model.
	if len(m.WriteSchemas) > 0 {
		metadata = ensureMap(metadata)
		metadata["write_schemas"] = copyMap(m.WriteSchemas)
	}
	if len(m.ReadExpectations) > 0 {
		metadata = ensureMap(metadata)
		metadata["read_expectations"] = copyExpectations(m.ReadExpectations)
	}
	// Rule 36: each subject list rides the same extras slot as the subject strings alone, in document
	// order, as a copy — no options, no schemas, no endpoint names.
	for _, list := range []struct {
		name     string
		subjects []string
	}{
		{"subscribes", subscriptionSubjects(m.Subscribes)},
		{"publishes", publicationSubjects(m.Publishes)},
		{"serves", endpointSubjects(m.Serves)},
	} {
		if len(list.subjects) > 0 {
			metadata = ensureMap(metadata)
			metadata[list.name] = list.subjects
		}
	}
	// Rule 36: the effective mode is ALWAYS on the card, so it states where its reads/writes live.
	metadata = ensureMap(metadata)
	metadata["communication"] = m.Communication
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
	// Rule 37: communication rides the round trip only under "nats", so a board manifest is
	// byte-for-byte the document it was before the rule existed.
	if m.Communication != communicationBlackboard {
		doc["communication"] = m.Communication
	}
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
	// Rule 34: each subject list rides the round trip only when non-empty, and each entry carries
	// only the members it declared away from their defaults.
	if len(m.Subscribes) > 0 {
		entries := make([]any, 0, len(m.Subscribes))
		for _, subscription := range m.Subscribes {
			entry := map[string]any{"subject": subscription.Subject}
			if subscription.Durable {
				entry["durable"] = subscription.Durable
			}
			if subscription.Mode != subscriptionBroadcast {
				entry["mode"] = subscription.Mode
			}
			if subscription.Strict {
				entry["strict"] = subscription.Strict
			}
			if subscription.Schema != nil {
				entry["schema"] = subscription.Schema
			}
			entries = append(entries, entry)
		}
		doc["subscribes"] = entries
	}
	if len(m.Publishes) > 0 {
		entries := make([]any, 0, len(m.Publishes))
		for _, publication := range m.Publishes {
			entry := map[string]any{"subject": publication.Subject}
			if publication.Schema != nil {
				entry["schema"] = publication.Schema
			}
			entries = append(entries, entry)
		}
		doc["publishes"] = entries
	}
	if len(m.Serves) > 0 {
		entries := make([]any, 0, len(m.Serves))
		for _, endpoint := range m.Serves {
			entry := map[string]any{"name": endpoint.Name, "subject": endpoint.Subject}
			if endpoint.Description != "" {
				entry["description"] = endpoint.Description
			}
			entries = append(entries, entry)
		}
		doc["serves"] = entries
	}
	// Rule 35: single_flight rides the round trip only when false.
	if !m.SingleFlight {
		doc["single_flight"] = m.SingleFlight
	}
	putList(doc, "invokes", m.Invokes)
	if discovery := m.discoveryMap(); len(discovery) > 0 {
		doc["discovery"] = discovery
	}
	component := map[string]any{"delivery": m.Component.Delivery}
	for field, url := range map[string]string{
		"activate_url": m.Component.ActivateURL,
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
	// Rule 33: each map rides the round trip only when non-empty, so a shape-free manifest is
	// byte-for-byte the document it was before the rule existed.
	if len(m.WriteSchemas) > 0 {
		doc["write_schemas"] = copyMap(m.WriteSchemas)
	}
	if len(m.ReadExpectations) > 0 {
		expectations := make(map[string]any, len(m.ReadExpectations))
		for key, expectation := range m.ReadExpectations {
			expectations[key] = map[string]any{"schema": expectation.Schema, "strict": expectation.Strict}
		}
		doc["read_expectations"] = expectations
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
	// Rule 35: single_flight defaults to true on the loaded model.
	singleFlight := true
	if declared, ok := data["single_flight"].(bool); ok {
		singleFlight = declared
	}
	loaded := &Manifest{
		Name:        stringOr(data, "name", ""),
		Version:     version,
		Description: stringOr(data, "description", ""),
		Component: Component{
			Delivery:    delivery,
			ActivateURL: activateURL,
			RPCURL:      stringOr(component, "rpc_url", ""),
			TimeoutS:    intOr(component, "timeout_s"),
			Payloads:    buildPayloads(component["payloads"]),
		},
		Communication:    stringOr(data, "communication", communicationBlackboard),
		Reads:            stringList(data["reads"]),
		Writes:           stringList(data["writes"]),
		Consumes:         stringList(data["consumes"]),
		Precondition:     stringOr(data, "precondition", ""),
		Activation:       stringOr(data, "activation", activationBroadcast),
		ArbitrationGroup: stringOr(data, "arbitration_group", ""),
		Subscribes:       buildSubscribes(data["subscribes"]),
		Publishes:        buildPublishes(data["publishes"]),
		Serves:           buildServes(data["serves"]),
		SingleFlight:     singleFlight,
		Invokes:          stringList(data["invokes"]),
		Discovery:        buildDiscovery(data["discovery"]),
		WriteSchemas:     buildWriteSchemas(data["write_schemas"]),
		ReadExpectations: buildReadExpectations(data["read_expectations"]),
	}
	if config := mapOf(data["config"]); config != nil {
		loaded.Config = buildConfig(config)
	}
	return loaded
}

// buildSubscribes reads a validated subscribes list, applying each entry's defaults (rule 34).
func buildSubscribes(raw any) []Subscription {
	entries := listOf(raw)
	subscriptions := make([]Subscription, 0, len(entries))
	for _, entry := range entries {
		declared := mapOf(entry)
		subscriptions = append(subscriptions, Subscription{
			Subject: stringOr(declared, "subject", ""),
			Durable: boolOr(declared, "durable"),
			Mode:    stringOr(declared, "mode", subscriptionBroadcast),
			Strict:  boolOr(declared, "strict"),
			Schema:  declared["schema"],
		})
	}
	return subscriptions
}

// buildPublishes reads a validated publishes list (rule 34).
func buildPublishes(raw any) []Publication {
	entries := listOf(raw)
	publications := make([]Publication, 0, len(entries))
	for _, entry := range entries {
		declared := mapOf(entry)
		publications = append(publications, Publication{
			Subject: stringOr(declared, "subject", ""),
			Schema:  declared["schema"],
		})
	}
	return publications
}

// buildServes reads a validated serves list into its endpoint entries (rule 34).
func buildServes(raw any) []Endpoint {
	entries := listOf(raw)
	endpoints := make([]Endpoint, 0, len(entries))
	for _, entry := range entries {
		declared := mapOf(entry)
		endpoints = append(endpoints, Endpoint{
			Name:        stringOr(declared, "name", ""),
			Subject:     stringOr(declared, "subject", ""),
			Description: stringOr(declared, "description", ""),
		})
	}
	return endpoints
}

// subscriptionSubjects reports the declared subjects alone, in document order (rule 36).
func subscriptionSubjects(entries []Subscription) []string {
	subjects := make([]string, 0, len(entries))
	for _, entry := range entries {
		subjects = append(subjects, entry.Subject)
	}
	return subjects
}

// publicationSubjects reports the declared subjects alone, in document order (rule 36).
func publicationSubjects(entries []Publication) []string {
	subjects := make([]string, 0, len(entries))
	for _, entry := range entries {
		subjects = append(subjects, entry.Subject)
	}
	return subjects
}

// endpointSubjects reports the declared subjects alone, in document order (rule 36).
func endpointSubjects(entries []Endpoint) []string {
	subjects := make([]string, 0, len(entries))
	for _, entry := range entries {
		subjects = append(subjects, entry.Subject)
	}
	return subjects
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

// buildWriteSchemas reads a validated write_schemas map, yielding nil when none are declared (rule 33).
func buildWriteSchemas(raw any) map[string]any {
	block := mapOf(raw)
	if len(block) == 0 {
		return nil
	}
	return copyMap(block)
}

// buildReadExpectations reads a validated read_expectations map, applying the strict default (rule 33).
func buildReadExpectations(raw any) map[string]ReadExpectation {
	block := mapOf(raw)
	if len(block) == 0 {
		return nil
	}
	expectations := make(map[string]ReadExpectation, len(block))
	for key, declared := range block {
		entry := mapOf(declared)
		expectations[key] = ReadExpectation{Schema: entry["schema"], Strict: boolOr(entry, "strict")}
	}
	return expectations
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

func copyExpectations(value map[string]ReadExpectation) map[string]ReadExpectation {
	copied := make(map[string]ReadExpectation, len(value))
	for key, expectation := range value {
		copied[key] = expectation
	}
	return copied
}

func ensureMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}
