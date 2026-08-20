package manifest

import (
	"errors"
	"os"

	"gopkg.in/yaml.v3"
)

// Environment variables a manifest may be supplied through (rule 17).
const (
	EnvManifestPath = "BB_MANIFEST_PATH"
	EnvManifest     = "BB_MANIFEST"
)

// maxDocumentBytes is the document size limit; anything larger is DOC_TOO_LARGE (rule 2).
const maxDocumentBytes = 64 * 1024

// Parse parses a YAML or JSON manifest document and validates it; warnings never block a load.
func Parse(text string) (*Manifest, error) {
	if len(text) > maxDocumentBytes {
		return nil, invalid("DOC_TOO_LARGE", "Manifest exceeds 64 KB")
	}
	var document any
	if err := yaml.Unmarshal([]byte(text), &document); err != nil {
		return nil, invalid("MANIFEST_PARSE", err.Error())
	}
	data, ok := document.(map[string]any)
	if !ok {
		return nil, invalid("MANIFEST_PARSE", "Manifest root must be a mapping")
	}
	findings := Validate(data)
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			return nil, &InvalidError{Findings: findings}
		}
	}
	return build(data), nil
}

// Load reads a manifest file and parses it; a read failure propagates unchanged.
func Load(path string) (*Manifest, error) {
	text, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(string(text))
}

// LoadFromEnv loads a manifest from BB_MANIFEST_PATH or BB_MANIFEST; exactly one must be set (rule 17).
func LoadFromEnv() (*Manifest, error) {
	path, hasPath := os.LookupEnv(EnvManifestPath)
	inline, hasInline := os.LookupEnv(EnvManifest)
	switch {
	case !hasPath && !hasInline:
		return nil, invalid("ENV_MISSING", "Neither BB_MANIFEST_PATH nor BB_MANIFEST is set")
	case hasPath && hasInline:
		return nil, invalid("ENV_CONFLICT", "Both BB_MANIFEST_PATH and BB_MANIFEST are set")
	case hasPath:
		return Load(path)
	default:
		return Parse(inline)
	}
}

// Findings reports the validation findings an error carries, or nil when it carries none.
func Findings(err error) []Finding {
	var invalid *InvalidError
	if !errors.As(err, &invalid) {
		return nil
	}
	return invalid.Findings
}
