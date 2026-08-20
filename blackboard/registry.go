package blackboard

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The registries' global bucket names; a workspace-scoped registry appends _{blackboard_id} (§5).
const (
	CardsBucket  = "COMPONENT_CARDS"
	HealthBucket = "COMPONENT_HEALTH"
)

const (
	cardsHistory  = 5
	healthHistory = 1
)

// The shape of the stream backing a KV bucket, restated so a registry can create its bucket
// without the account-level request the client library's convenience creation issues first.
const (
	kvSubjectPrefix   = "$KV."
	kvSubjectSuffix   = ".>"
	kvDuplicateWindow = 2 * time.Minute
)

var (
	instanceRe = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	trackRe    = regexp.MustCompile(`^[a-z][a-z0-9-]{0,15}$`)
)

// RegistryOption scopes a card or health registry to a workspace bucket and an instance/track key (§5).
type RegistryOption func(*registryScope)

// WithBlackboardID scopes a registry to the per-workspace bucket, e.g. COMPONENT_CARDS_{blackboard_id}.
func WithBlackboardID(blackboardID string) RegistryOption {
	return func(scope *registryScope) {
		if err := validateBlackboardID(blackboardID); err != nil {
			scope.err = errors.Join(scope.err, err)
			return
		}
		scope.blackboardID = blackboardID
	}
}

// WithInstance qualifies every key a registry derives as {name}.{instance} (§5).
func WithInstance(instance string) RegistryOption {
	return func(scope *registryScope) {
		if !instanceRe.MatchString(instance) {
			scope.err = errors.Join(scope.err, fmt.Errorf("%w: %q", ErrInvalidInstance, instance))
			return
		}
		scope.instance = instance
	}
}

// WithTrack qualifies every key a registry derives as {name}.{instance}.{track}; requires an instance (§5).
//
// The pattern is a charset constraint only: the library places no meaning on any particular track
// value, so binding the platform's stable track derives {name}.{instance}.stable — which is why the
// caller binds this scope only for a non-stable track (sidecar.md rule C18), never for stable.
func WithTrack(track string) RegistryOption {
	return func(scope *registryScope) {
		if !trackRe.MatchString(track) {
			scope.err = errors.Join(scope.err, fmt.Errorf("%w: %q", ErrInvalidTrack, track))
			return
		}
		scope.track = track
	}
}

// registryScope is a registry's resolved bucket and key scoping.
type registryScope struct {
	blackboardID string
	instance     string
	track        string
	err          error
}

// newScope resolves the options into the bucket a registry binds and the keys it derives.
func newScope(opts []RegistryOption) (registryScope, error) {
	var scope registryScope
	for _, opt := range opts {
		opt(&scope)
	}
	if scope.err == nil && scope.track != "" && scope.instance == "" {
		scope.err = fmt.Errorf("%w: track %q needs an instance scope", ErrInvalidTrack, scope.track)
	}
	if scope.err != nil {
		return registryScope{}, scope.err
	}
	return scope, nil
}

// bucket returns the global bucket name, or the per-workspace one when a blackboard id is scoped.
func (s registryScope) bucket(base string) string {
	if s.blackboardID == "" {
		return base
	}
	return base + "_" + s.blackboardID
}

// key derives a component's KV key: {name}, {name}.{instance}, or {name}.{instance}.{track}.
//
// The name is the key base (§5), so an empty one is the empty-key violation of §1 rule 2 whether or
// not an instance is scoped — an instance would otherwise pad it into a syntactically valid key.
func (s registryScope) key(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: name must not be empty", ErrInvalidValue)
	}
	if s.instance == "" {
		return name, nil
	}
	if s.track == "" {
		return name + "." + s.instance, nil
	}
	return name + "." + s.instance + "." + s.track, nil
}

// registryBucket binds a registry's KV bucket lazily, creating it on first use (§5).
type registryBucket struct {
	js      jetstream.JetStream
	name    string
	history uint8

	mu    sync.Mutex
	board *Blackboard
}

// blackboard returns the registry's bucket, creating it the first time it is needed.
func (r *registryBucket) blackboard(ctx context.Context) (*Blackboard, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.board != nil {
		return r.board, nil
	}
	kv, err := r.js.KeyValue(ctx, r.name)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = r.create(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("blackboard: binding registry bucket %q: %w", r.name, err)
	}
	r.board = New(kv)
	return r.board, nil
}

// create makes the bucket by creating its backing stream directly, then binds it.
//
// The client library's convenience bucket creation asks the server for account information first, and
// a per-unit credential is scoped to its own workspace's subject classes with no account-level reach
// (supervisor.md rule B31) — so the first unit in a fresh workspace could never create its own
// registry bucket. Creating the stream stays inside the bucket's own $JS.API subject class, which
// that credential already carries; widening the credential instead would leak every other
// workspace's bucket names into the reply, which is the isolation the scope exists to produce.
func (r *registryBucket) create(ctx context.Context) (jetstream.KeyValue, error) {
	if _, err := r.js.CreateStream(ctx, kvStreamConfig(r.name, r.history)); err != nil &&
		!errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return nil, err
	}
	return r.js.KeyValue(ctx, r.name)
}

// kvStreamConfig returns the stream a KV bucket of the given name and history is backed by.
func kvStreamConfig(bucket string, history uint8) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              kvStreamPrefix + bucket,
		Subjects:          []string{kvSubjectPrefix + bucket + kvSubjectSuffix},
		MaxMsgsPerSubject: int64(history),
		MaxMsgs:           -1,
		MaxBytes:          -1,
		MaxMsgSize:        -1,
		MaxConsumers:      -1,
		Storage:           jetstream.FileStorage,
		Replicas:          1,
		Discard:           jetstream.DiscardNew,
		DenyDelete:        true,
		AllowRollup:       true,
		AllowDirect:       true,
		Duplicates:        kvDuplicateWindow,
	}
}

// CardRegistry publishes and discovers component cards in a COMPONENT_CARDS bucket (§5).
type CardRegistry struct {
	scope  registryScope
	bucket *registryBucket
}

// NewCardRegistry returns a card registry bound to exactly one bucket, auto-created on first use.
func NewCardRegistry(js jetstream.JetStream, opts ...RegistryOption) (*CardRegistry, error) {
	scope, err := newScope(opts)
	if err != nil {
		return nil, err
	}
	return &CardRegistry{
		scope:  scope,
		bucket: &registryBucket{js: js, name: scope.bucket(CardsBucket), history: cardsHistory},
	}, nil
}

// Publish writes a card to the registry verbatim and returns its new revision (§3.4 rule 10).
func (r *CardRegistry) Publish(ctx context.Context, card ComponentCard) (uint64, error) {
	board, err := r.bucket.blackboard(ctx)
	if err != nil {
		return 0, err
	}
	stored, err := cardValue(card)
	if err != nil {
		return 0, err
	}
	key, err := r.scope.key(card.Name)
	if err != nil {
		return 0, err
	}
	return board.Put(ctx, key, stored)
}

// Get returns the card published under a name, or nil when the registry holds none.
func (r *CardRegistry) Get(ctx context.Context, name string) (*ComponentCard, error) {
	board, err := r.bucket.blackboard(ctx)
	if err != nil {
		return nil, err
	}
	key, err := r.scope.key(name)
	if err != nil {
		return nil, err
	}
	entry, err := board.Get(ctx, key)
	if err != nil || entry == nil {
		return nil, err
	}
	card, err := cardFromValue(entry.Value)
	if err != nil {
		return nil, err
	}
	return &card, nil
}

// Remove deletes a card from the registry; a missing card is a no-op.
func (r *CardRegistry) Remove(ctx context.Context, name string) error {
	board, err := r.bucket.blackboard(ctx)
	if err != nil {
		return err
	}
	key, err := r.scope.key(name)
	if err != nil {
		return err
	}
	return board.Delete(ctx, key)
}

// ListCards returns every card in the bucket, across all instances and tracks, ordered by KV key.
func (r *CardRegistry) ListCards(ctx context.Context) ([]ComponentCard, error) {
	entries, err := r.ListEntries(ctx)
	if err != nil {
		return nil, err
	}
	cards := make([]ComponentCard, 0, len(entries))
	for _, key := range slices.Sorted(maps.Keys(entries)) {
		cards = append(cards, entries[key])
	}
	return cards, nil
}

// ListEntries returns every card in the bucket keyed by its derived KV key (§5).
func (r *CardRegistry) ListEntries(ctx context.Context) (map[string]ComponentCard, error) {
	board, err := r.bucket.blackboard(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := board.Keys(ctx)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]ComponentCard, len(keys))
	for _, key := range keys {
		entry, err := board.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if entry == nil {
			continue
		}
		card, err := cardFromValue(entry.Value)
		if err != nil {
			return nil, err
		}
		entries[key] = card
	}
	return entries, nil
}

// FindByTag returns every card carrying a skill tagged with tag.
func (r *CardRegistry) FindByTag(ctx context.Context, tag string) ([]ComponentCard, error) {
	cards, err := r.ListCards(ctx)
	if err != nil {
		return nil, err
	}
	matches := []ComponentCard{}
	for _, card := range cards {
		for _, skill := range card.Skills {
			if slices.Contains(skill.Tags, tag) {
				matches = append(matches, card)
				break
			}
		}
	}
	return matches, nil
}
