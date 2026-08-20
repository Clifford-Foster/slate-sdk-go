package blackboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Bucket limits the platform configures and enforces on every blackboard (§1 rules 3, §2).
const (
	MaxValueSize = 1_048_576
	HistoryDepth = 5
)

// Operations a HistoryEntry can carry.
const (
	OperationPut    = "PUT"
	OperationDelete = "DEL"
)

// Entry is a key's value, revision, and write time as read by Get or delivered by a watch.
type Entry struct {
	Key      string
	Value    map[string]any
	Revision uint64
	// TS is the revision's JetStream server timestamp, or nil when the client
	// library supplies no stamp for this path — never a local-clock substitute (§1 rule 12).
	TS *time.Time
}

// HistoryEntry is one retained revision of a key from the bucket's history window.
type HistoryEntry struct {
	Key string
	// Value is the decoded JSON object, or nil for a delete tombstone or an undecodable entry.
	Value     map[string]any
	Revision  uint64
	Operation string
	TS        time.Time
}

// Blackboard is the read/write interface to a single JetStream KV bucket.
type Blackboard struct {
	kv      jetstream.KeyValue
	backoff WatchBackoff
	conn    watchConn
}

// Option configures a Blackboard at construction.
type Option func(*Blackboard)

// WithWatchBackoff injects the watch re-subscribe backoff schedule (§1 rule 13).
func WithWatchBackoff(backoff WatchBackoff) Option {
	return func(b *Blackboard) { b.backoff = backoff }
}

// withWatchConn wires the connection watches re-arm on; the manager supplies it, never a consumer.
func withWatchConn(conn watchConn) Option {
	return func(b *Blackboard) { b.conn = conn }
}

// New returns a Blackboard wrapping an existing JetStream KV bucket.
func New(kv jetstream.KeyValue, opts ...Option) *Blackboard {
	b := &Blackboard{kv: kv}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Get returns the current entry for a key, or nil when the key is missing or deleted.
func (b *Blackboard) Get(ctx context.Context, key string) (*Entry, error) {
	entry, err := b.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("blackboard: get %q: %w", key, err)
	}
	// An empty payload is not a missing key: the §1 value rules never store empty bytes, so a key
	// holding them was written past this library and its value is undecodable like any other.
	value, err := decodeValue(entry.Value())
	if err != nil {
		return nil, fmt.Errorf("blackboard: get %q: %w", key, err)
	}
	return &Entry{Key: entry.Key(), Value: value, Revision: entry.Revision(), TS: entryTS(entry.Created())}, nil
}

// Put writes a JSON object to a key unconditionally and returns the new revision.
func (b *Blackboard) Put(ctx context.Context, key string, value map[string]any) (uint64, error) {
	payload, err := encodeValue(key, value)
	if err != nil {
		return 0, err
	}
	revision, err := b.kv.Put(ctx, key, payload)
	if err != nil {
		return 0, fmt.Errorf("blackboard: put %q: %w", key, err)
	}
	return revision, nil
}

// PutCAS writes a JSON object only while the key is at revision, returning ErrRevisionConflict otherwise.
func (b *Blackboard) PutCAS(ctx context.Context, key string, value map[string]any, revision uint64) (uint64, error) {
	payload, err := encodeValue(key, value)
	if err != nil {
		return 0, err
	}
	var newRevision uint64
	if revision == 0 {
		// Create-if-absent (§6 rule 1's absent branch): a deleted key reads as absent (§1 rule 6),
		// so Create — which retries the write at the tombstone's revision — is what revision 0 means.
		newRevision, err = b.kv.Create(ctx, key, payload)
	} else {
		newRevision, err = b.kv.Update(ctx, key, payload, revision)
	}
	if err != nil {
		if isWrongLastRevision(err) {
			return 0, fmt.Errorf("%w: put %q expected revision %d: %w", ErrRevisionConflict, key, revision, err)
		}
		return 0, fmt.Errorf("blackboard: put %q: %w", key, err)
	}
	return newRevision, nil
}

// Delete soft-deletes a key unconditionally; a missing key is a no-op.
func (b *Blackboard) Delete(ctx context.Context, key string) error {
	if err := b.kv.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("blackboard: delete %q: %w", key, err)
	}
	return nil
}

// DeleteCAS soft-deletes a key only while it is at revision, returning ErrRevisionConflict otherwise.
func (b *Blackboard) DeleteCAS(ctx context.Context, key string, revision uint64) error {
	if err := b.kv.Delete(ctx, key, jetstream.LastRevision(revision)); err != nil {
		if isWrongLastRevision(err) {
			return fmt.Errorf("%w: delete %q expected revision %d: %w", ErrRevisionConflict, key, revision, err)
		}
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("blackboard: delete %q: %w", key, err)
	}
	return nil
}

// Keys returns every key name in the bucket; an empty bucket yields an empty slice (§1 rule 7).
func (b *Blackboard) Keys(ctx context.Context) ([]string, error) {
	keys, err := b.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("blackboard: keys: %w", err)
	}
	return keys, nil
}

// History returns a key's retained revision history, oldest first; a key with none yields an empty slice.
func (b *Blackboard) History(ctx context.Context, key string) ([]HistoryEntry, error) {
	if strings.ContainsAny(key, "*>") {
		return nil, fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
	entries, err := b.kv.History(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrNoKeysFound) {
			return []HistoryEntry{}, nil
		}
		return nil, fmt.Errorf("blackboard: history %q: %w", key, err)
	}
	history := make([]HistoryEntry, 0, len(entries))
	for _, entry := range entries {
		history = append(history, historyEntry(entry))
	}
	return history, nil
}

// encodeValue applies the §1 value rules (non-empty key, JSON object, 1 MB ceiling) and serializes.
func encodeValue(key string, value map[string]any) ([]byte, error) {
	if key == "" {
		return nil, fmt.Errorf("%w: key must not be empty", ErrInvalidValue)
	}
	if value == nil {
		return nil, fmt.Errorf("%w: value must be a JSON object", ErrInvalidValue)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: value is not JSON-serializable: %w", ErrInvalidValue, err)
	}
	if len(payload) > MaxValueSize {
		return nil, fmt.Errorf("%w: serialized value exceeds %d bytes", ErrInvalidValue, MaxValueSize)
	}
	return payload, nil
}

// decodeValue deserializes stored bytes into the JSON object every blackboard value is.
func decodeValue(payload []byte) (map[string]any, error) {
	var value map[string]any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, fmt.Errorf("value is not a JSON object: %w", err)
	}
	if value == nil {
		return nil, errors.New("value is not a JSON object: null")
	}
	return value, nil
}

// historyEntry converts a KV entry to its forensic history record; a bad payload yields a nil value.
func historyEntry(entry jetstream.KeyValueEntry) HistoryEntry {
	operation := OperationPut
	if isDelete(entry.Operation()) {
		operation = OperationDelete
	}
	var value map[string]any
	if operation == OperationPut {
		if decoded, err := decodeValue(entry.Value()); err == nil {
			value = decoded
		}
	}
	return HistoryEntry{
		Key:       entry.Key(),
		Value:     value,
		Revision:  entry.Revision(),
		Operation: operation,
		TS:        entry.Created().UTC(),
	}
}

// entryTS normalizes a KV timestamp to UTC, reporting absence as nil rather than a fabricated time (§1 rule 12).
func entryTS(created time.Time) *time.Time {
	if created.IsZero() {
		return nil
	}
	utc := created.UTC()
	return &utc
}

// isDelete reports whether a KV operation is a delete or purge tombstone.
func isDelete(op jetstream.KeyValueOp) bool {
	return op == jetstream.KeyValueDelete || op == jetstream.KeyValuePurge
}

// isWrongLastRevision reports whether a publish failed the expected-last-revision check (CAS loss).
func isWrongLastRevision(err error) bool {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}
