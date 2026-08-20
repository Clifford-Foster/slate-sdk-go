package blackboard

import "time"

// ErrorRecordOptions carries an error record's attribution and its optional fields (§7).
type ErrorRecordOptions struct {
	Component    string
	Instance     string
	ActivationID string
	// CorrelationID is included only when non-empty.
	CorrelationID string
	// Detail is included only when non-nil and must never carry secrets.
	Detail map[string]any
	// TS defaults to the current UTC time in ISO-8601.
	TS string
}

// ErrorRecord builds a well-formed error.> record; it is pure and never mutates its arguments.
func ErrorRecord(code, message string, opts ErrorRecordOptions) map[string]any {
	record := map[string]any{
		"code":          code,
		"message":       message,
		"component":     opts.Component,
		"instance":      opts.Instance,
		"activation_id": opts.ActivationID,
	}
	if opts.Detail != nil {
		record["detail"] = opts.Detail
	}
	if opts.CorrelationID != "" {
		record["correlation_id"] = opts.CorrelationID
	}
	if opts.TS != "" {
		record["ts"] = opts.TS
	} else {
		record["ts"] = time.Now().UTC().Format(isoMicrosLayout)
	}
	return record
}
