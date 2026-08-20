package blackboard

import (
	"encoding/json"
	"fmt"
)

// Skill is one capability a component provides, as published on its component card (§5).
type Skill struct {
	ID           string
	Name         string
	Description  string
	Tags         []string
	Examples     []string
	InputSchema  string
	OutputSchema string
}

// ComponentCard is a component's self-describing manifest, the unit of capability discovery (§5).
type ComponentCard struct {
	Name         string
	Description  string
	Version      string
	Category     string
	Skills       []Skill
	Subjects     map[string]string
	Reads        []string
	Writes       []string
	Precondition string
	Config       []map[string]any
	Metadata     map[string]any
}

// skillWire is a skill's stored JSON shape: every field present, an unset optional one null.
type skillWire struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Description  string   `json:"description"`
	Tags         []string `json:"tags"`
	Examples     []string `json:"examples"`
	InputSchema  *string  `json:"input_schema"`
	OutputSchema *string  `json:"output_schema"`
}

// cardWire is a card's stored JSON shape: every field present, an unset optional one null.
type cardWire struct {
	Name         string            `json:"name"`
	Description  string            `json:"description"`
	Version      string            `json:"version"`
	Category     string            `json:"category"`
	Skills       []skillWire       `json:"skills"`
	Subjects     map[string]string `json:"subjects"`
	Reads        []string          `json:"reads"`
	Writes       []string          `json:"writes"`
	Precondition *string           `json:"precondition"`
	Config       []map[string]any  `json:"config"`
	Metadata     map[string]any    `json:"metadata"`
}

// MarshalJSON writes the skill's stored shape, with an unset optional field as null.
func (s Skill) MarshalJSON() ([]byte, error) {
	payload, err := json.Marshal(skillWireOf(s))
	if err != nil {
		return nil, fmt.Errorf("blackboard: serializing skill %q: %w", s.ID, err)
	}
	return payload, nil
}

// UnmarshalJSON reads a stored skill, ignoring fields it does not know.
func (s *Skill) UnmarshalJSON(data []byte) error {
	var wire skillWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("blackboard: deserializing skill: %w", err)
	}
	*s = wire.skill()
	return nil
}

// MarshalJSON writes the card's stored shape, with an unset optional field as null.
func (c ComponentCard) MarshalJSON() ([]byte, error) {
	skills := make([]skillWire, 0, len(c.Skills))
	for _, skill := range c.Skills {
		skills = append(skills, skillWireOf(skill))
	}
	payload, err := json.Marshal(cardWire{
		Name:         c.Name,
		Description:  c.Description,
		Version:      c.Version,
		Category:     c.Category,
		Skills:       skills,
		Subjects:     c.Subjects,
		Reads:        c.Reads,
		Writes:       c.Writes,
		Precondition: optional(c.Precondition),
		Config:       c.Config,
		Metadata:     c.Metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("blackboard: serializing card %q: %w", c.Name, err)
	}
	return payload, nil
}

// UnmarshalJSON reads a stored card, ignoring fields it does not know.
func (c *ComponentCard) UnmarshalJSON(data []byte) error {
	var wire cardWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("blackboard: deserializing card: %w", err)
	}
	card := ComponentCard{
		Name:         wire.Name,
		Description:  wire.Description,
		Version:      wire.Version,
		Category:     wire.Category,
		Subjects:     wire.Subjects,
		Reads:        wire.Reads,
		Writes:       wire.Writes,
		Precondition: value(wire.Precondition),
		Config:       wire.Config,
		Metadata:     wire.Metadata,
	}
	for _, skill := range wire.Skills {
		card.Skills = append(card.Skills, skill.skill())
	}
	*c = card
	return nil
}

// skillWireOf converts a skill to its stored shape.
func skillWireOf(s Skill) skillWire {
	return skillWire{
		ID:           s.ID,
		Name:         s.Name,
		Description:  s.Description,
		Tags:         orEmpty(s.Tags),
		Examples:     s.Examples,
		InputSchema:  optional(s.InputSchema),
		OutputSchema: optional(s.OutputSchema),
	}
}

// skill converts a stored skill to its public form.
func (w skillWire) skill() Skill {
	return Skill{
		ID:           w.ID,
		Name:         w.Name,
		Description:  w.Description,
		Tags:         w.Tags,
		Examples:     w.Examples,
		InputSchema:  value(w.InputSchema),
		OutputSchema: value(w.OutputSchema),
	}
}

// cardValue serializes a card to the JSON object a registry stores as its blackboard value.
func cardValue(card ComponentCard) (map[string]any, error) {
	payload, err := json.Marshal(card)
	if err != nil {
		return nil, err
	}
	var stored map[string]any
	if err := json.Unmarshal(payload, &stored); err != nil {
		return nil, fmt.Errorf("blackboard: serializing card %q: %w", card.Name, err)
	}
	return stored, nil
}

// cardFromValue reads a stored registry value back into a card.
func cardFromValue(stored map[string]any) (ComponentCard, error) {
	payload, err := json.Marshal(stored)
	if err != nil {
		return ComponentCard{}, fmt.Errorf("blackboard: deserializing card: %w", err)
	}
	var card ComponentCard
	if err := json.Unmarshal(payload, &card); err != nil {
		return ComponentCard{}, err
	}
	return card, nil
}

// optional renders an unset string as the JSON null the wire shape carries.
func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// value reads an optional wire string, rendering null as the empty string.
func value(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// orEmpty renders an unset list as the empty JSON array the wire shape carries.
func orEmpty(list []string) []string {
	if list == nil {
		return []string{}
	}
	return list
}
