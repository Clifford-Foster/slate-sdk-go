package blackboard

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

// HealthRegistry is the schema-agnostic heartbeat store in a COMPONENT_HEALTH bucket (§5).
type HealthRegistry struct {
	scope  registryScope
	bucket *registryBucket
}

// NewHealthRegistry returns a heartbeat store bound to exactly one bucket, auto-created on first use.
func NewHealthRegistry(js jetstream.JetStream, opts ...RegistryOption) (*HealthRegistry, error) {
	scope, err := newScope(opts)
	if err != nil {
		return nil, err
	}
	return &HealthRegistry{
		scope:  scope,
		bucket: &registryBucket{js: js, name: scope.bucket(HealthBucket), history: healthHistory},
	}, nil
}

// Publish writes a component's heartbeat value and returns its new revision.
func (h *HealthRegistry) Publish(ctx context.Context, name string, value map[string]any) (uint64, error) {
	board, err := h.bucket.blackboard(ctx)
	if err != nil {
		return 0, err
	}
	key, err := h.scope.key(name)
	if err != nil {
		return 0, err
	}
	return board.Put(ctx, key, value)
}

// Get returns a component's current heartbeat value, or nil when the store holds none.
func (h *HealthRegistry) Get(ctx context.Context, name string) (map[string]any, error) {
	board, err := h.bucket.blackboard(ctx)
	if err != nil {
		return nil, err
	}
	key, err := h.scope.key(name)
	if err != nil {
		return nil, err
	}
	entry, err := board.Get(ctx, key)
	if err != nil || entry == nil {
		return nil, err
	}
	return entry.Value, nil
}

// ListEntries returns every heartbeat value in the bucket keyed by its derived KV key (§5).
func (h *HealthRegistry) ListEntries(ctx context.Context) (map[string]map[string]any, error) {
	board, err := h.bucket.blackboard(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := board.Keys(ctx)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]map[string]any, len(keys))
	for _, key := range keys {
		entry, err := board.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if entry != nil {
			entries[key] = entry.Value
		}
	}
	return entries, nil
}

// Remove deletes a component's heartbeat entry; a missing entry is a no-op.
func (h *HealthRegistry) Remove(ctx context.Context, name string) error {
	board, err := h.bucket.blackboard(ctx)
	if err != nil {
		return err
	}
	key, err := h.scope.key(name)
	if err != nil {
		return err
	}
	return board.Delete(ctx, key)
}
