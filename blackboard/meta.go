package blackboard

import (
	"fmt"
	"maps"
	"time"
)

// MetaField is the reserved, platform-owned top-level key inside a stored value (§1 rule 11).
const MetaField = "_meta"

// MinSLADeadline is the shortest declarable SLA deadline; below it a breach is watch-latency noise.
const MinSLADeadline = 5 * time.Second

const (
	correlationField = "correlation_id"
	slaField         = "sla"
	deadlineField    = "deadline_s"
)

// CorrelationOf returns the correlation id in a value's reserved _meta envelope, or "" when absent.
func CorrelationOf(value map[string]any) string {
	meta, ok := value[MetaField].(map[string]any)
	if !ok {
		return ""
	}
	cid, ok := meta[correlationField].(string)
	if !ok {
		return ""
	}
	return cid
}

// WithCorrelation returns a shallow copy of value carrying cid in its reserved _meta envelope.
func WithCorrelation(value map[string]any, cid string) map[string]any {
	result, meta := copyWithMeta(value)
	meta[correlationField] = cid
	return result
}

// SLAOf returns the soft deadline in a value's reserved _meta envelope, or zero when absent or malformed.
func SLAOf(value map[string]any) time.Duration {
	meta, ok := value[MetaField].(map[string]any)
	if !ok {
		return 0
	}
	sla, ok := meta[slaField].(map[string]any)
	if !ok {
		return 0
	}
	seconds, ok := numericSeconds(sla[deadlineField])
	if !ok || seconds < MinSLADeadline {
		return 0
	}
	return seconds
}

// WithSLA returns a shallow copy of value carrying a soft deadline in its reserved _meta envelope.
func WithSLA(value map[string]any, deadline time.Duration) (map[string]any, error) {
	if deadline < MinSLADeadline {
		return nil, fmt.Errorf("%w: SLA deadline must be at least %s, got %s", ErrInvalidValue, MinSLADeadline, deadline)
	}
	result, meta := copyWithMeta(value)
	meta[slaField] = map[string]any{deadlineField: deadline.Seconds()}
	return result, nil
}

// copyWithMeta shallow-copies a value and its _meta object, so neither the input nor its envelope is mutated.
func copyWithMeta(value map[string]any) (result, meta map[string]any) {
	result = make(map[string]any, len(value)+1)
	maps.Copy(result, value)
	meta = make(map[string]any)
	if existing, ok := result[MetaField].(map[string]any); ok {
		maps.Copy(meta, existing)
	}
	result[MetaField] = meta
	return result, meta
}

// numericSeconds reads a JSON number of seconds as a duration, rejecting non-numeric values.
func numericSeconds(value any) (time.Duration, bool) {
	switch number := value.(type) {
	case float64:
		return time.Duration(number * float64(time.Second)), true
	case int:
		return time.Duration(number) * time.Second, true
	case int64:
		return time.Duration(number) * time.Second, true
	default:
		return 0, false
	}
}
