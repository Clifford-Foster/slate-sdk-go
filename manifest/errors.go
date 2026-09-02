package manifest

import "fmt"

// Severities a finding carries; PRECONDITION_UNWATCHED_KEY, PRECONDITION_INERT and CONSUMES_INERT
// are the only warning-severity codes (rules 10, 37).
const (
	SeverityError   = "error"
	SeverityWarning = "warning"
)

// Finding is one validation result: a stable code, a dotted path, a human message, and a severity.
type Finding struct {
	Code     string
	Path     string
	Message  string
	Severity string
}

// InvalidError reports a manifest that failed to parse or validate, carrying every finding.
type InvalidError struct {
	Findings []Finding
}

// Error renders the finding count; the findings themselves carry the codes and paths.
func (e *InvalidError) Error() string {
	return fmt.Sprintf("manifest invalid: %d error(s)", len(e.Findings))
}

// invalid wraps a single document-level finding as the error loading raises.
func invalid(code, message string) error {
	return &InvalidError{Findings: []Finding{{Code: code, Path: "", Message: message, Severity: SeverityError}}}
}
