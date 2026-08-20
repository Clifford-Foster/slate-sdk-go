package blackboard

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Reserved meta keys every blackboard carries from creation (§2).
const (
	MetaBlackboardIDKey = "meta.blackboard_id"
	MetaCreatedAtKey    = "meta.created_at"
	MetaStatusKey       = "meta.status"
)

// Blackboard lifecycle statuses (§2).
const (
	StatusActive    = "active"
	StatusSuspended = "suspended"
)

const (
	bucketPrefix    = "bb_"
	kvStreamPrefix  = "KV_"
	metaValueField  = "value"
	isoMicrosLayout = "2006-01-02T15:04:05.000000Z07:00"
)

var blackboardIDRe = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

// Usage is a blackboard bucket's storage usage: stream bytes, configured limit (-1 unlimited), and key count.
type Usage struct {
	Bytes    int64
	MaxBytes int64
	Keys     int
}

// Manager creates, retrieves, and manages the lifecycle of blackboards.
type Manager struct {
	js jetstream.JetStream
}

// NewManager returns a Manager binding blackboards on the given JetStream context.
func NewManager(js jetstream.JetStream) *Manager {
	return &Manager{js: js}
}

// Create creates a blackboard's bucket, writes its meta keys, and returns it.
func (m *Manager) Create(ctx context.Context, blackboardID string) (*Blackboard, error) {
	if err := validateBlackboardID(blackboardID); err != nil {
		return nil, err
	}
	bucket := bucketPrefix + blackboardID
	if _, err := m.js.KeyValue(ctx, bucket); err == nil {
		return nil, fmt.Errorf("%w: %q", ErrExists, blackboardID)
	} else if !errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("blackboard: create %q: %w", blackboardID, err)
	}

	kv, err := m.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:       bucket,
		History:      HistoryDepth,
		MaxValueSize: MaxValueSize,
		Storage:      jetstream.FileStorage,
	})
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketExists) {
			return nil, fmt.Errorf("%w: %q", ErrExists, blackboardID)
		}
		return nil, fmt.Errorf("blackboard: create %q: %w", blackboardID, err)
	}

	bb := New(kv, m.boardOptions()...)
	for _, meta := range [][2]string{
		{MetaBlackboardIDKey, blackboardID},
		{MetaCreatedAtKey, time.Now().UTC().Format(isoMicrosLayout)},
		{MetaStatusKey, StatusActive},
	} {
		if _, err := bb.Put(ctx, meta[0], map[string]any{metaValueField: meta[1]}); err != nil {
			return nil, fmt.Errorf("blackboard: create %q: %w", blackboardID, err)
		}
	}
	return bb, nil
}

// Destroy deletes a blackboard and all its data, permanently.
func (m *Manager) Destroy(ctx context.Context, blackboardID string) error {
	if err := m.js.DeleteKeyValue(ctx, bucketPrefix+blackboardID); err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrInvalidBucketName) {
			return fmt.Errorf("%w: %q", ErrNotFound, blackboardID)
		}
		return fmt.Errorf("blackboard: destroy %q: %w", blackboardID, err)
	}
	return nil
}

// Get returns a Blackboard bound freshly to an existing blackboard's bucket.
func (m *Manager) Get(ctx context.Context, blackboardID string) (*Blackboard, error) {
	kv, err := m.js.KeyValue(ctx, bucketPrefix+blackboardID)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrInvalidBucketName) {
			return nil, fmt.Errorf("%w: %q", ErrNotFound, blackboardID)
		}
		return nil, fmt.Errorf("blackboard: get %q: %w", blackboardID, err)
	}
	return New(kv, m.boardOptions()...), nil
}

// boardOptions returns what every blackboard this manager hands out carries: the client connection
// its watches re-arm on, so a reconnect can never leave one reporting a stale consumer (§1 rule 13).
func (m *Manager) boardOptions() []Option {
	conn := m.js.Conn()
	if conn == nil {
		return nil
	}
	return []Option{withWatchConn(conn)}
}

// Suspend sets a blackboard's status to suspended; already-suspended is a no-op.
func (m *Manager) Suspend(ctx context.Context, blackboardID string) error {
	return m.setStatus(ctx, blackboardID, StatusSuspended)
}

// Resume sets a blackboard's status to active; already-active is a no-op.
func (m *Manager) Resume(ctx context.Context, blackboardID string) error {
	return m.setStatus(ctx, blackboardID, StatusActive)
}

// Status returns a blackboard's current status: active or suspended.
func (m *Manager) Status(ctx context.Context, blackboardID string) (string, error) {
	bb, err := m.Get(ctx, blackboardID)
	if err != nil {
		return "", err
	}
	entry, err := bb.Get(ctx, MetaStatusKey)
	if err != nil {
		return "", err
	}
	if entry == nil {
		return "", fmt.Errorf("%w: %q has no status", ErrNotFound, blackboardID)
	}
	status, ok := entry.Value[metaValueField].(string)
	if !ok {
		return "", fmt.Errorf("%w: %q has no status", ErrNotFound, blackboardID)
	}
	return status, nil
}

// Usage returns the bucket's storage usage read fresh from JetStream stream info, never cached.
func (m *Manager) Usage(ctx context.Context, blackboardID string) (Usage, error) {
	bb, err := m.Get(ctx, blackboardID)
	if err != nil {
		return Usage{}, err
	}
	stream, err := m.js.Stream(ctx, kvStreamPrefix+bucketPrefix+blackboardID)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return Usage{}, fmt.Errorf("%w: %q", ErrNotFound, blackboardID)
		}
		return Usage{}, fmt.Errorf("blackboard: usage %q: %w", blackboardID, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return Usage{}, fmt.Errorf("blackboard: usage %q: %w", blackboardID, err)
	}
	keys, err := bb.Keys(ctx)
	if err != nil {
		return Usage{}, err
	}
	maxBytes := info.Config.MaxBytes
	if maxBytes == 0 {
		maxBytes = -1 // JetStream's other spelling of unlimited; the contract reports -1
	}
	return Usage{Bytes: int64(info.State.Bytes), MaxBytes: maxBytes, Keys: len(keys)}, nil
}

// List returns every blackboard id, discovered by the bb_ bucket prefix.
func (m *Manager) List(ctx context.Context) ([]string, error) {
	prefix := kvStreamPrefix + bucketPrefix
	lister := m.js.StreamNames(ctx)
	ids := []string{}
	for name := range lister.Name() {
		if strings.HasPrefix(name, prefix) {
			ids = append(ids, strings.TrimPrefix(name, prefix))
		}
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("blackboard: list: %w", err)
	}
	return ids, nil
}

// setStatus writes meta.status unless it already holds the wanted value.
func (m *Manager) setStatus(ctx context.Context, blackboardID, status string) error {
	bb, err := m.Get(ctx, blackboardID)
	if err != nil {
		return err
	}
	entry, err := bb.Get(ctx, MetaStatusKey)
	if err != nil {
		return err
	}
	if entry != nil {
		if current, ok := entry.Value[metaValueField].(string); ok && current == status {
			return nil
		}
	}
	if _, err := bb.Put(ctx, MetaStatusKey, map[string]any{metaValueField: status}); err != nil {
		return err
	}
	return nil
}

// validateBlackboardID enforces the ^[a-zA-Z0-9-]+$ id rule (§2 Naming).
func validateBlackboardID(blackboardID string) error {
	if !blackboardIDRe.MatchString(blackboardID) {
		return fmt.Errorf("%w: %q", ErrInvalidBlackboardID, blackboardID)
	}
	return nil
}
