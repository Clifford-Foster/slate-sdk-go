package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
	"github.com/Clifford-Foster/slate-sdk-go/blackboard"
)

// plane is the data plane one delivery reaches: the harness's in-memory board with the enforcement,
// the claim table and the invoke fake the sidecar would apply in front of it (rules H7, H12, H14).
// An activation's plane records what it wrote and stamps the activation's effective correlation id;
// an event's and an RPC's record nothing and stamp nothing, because neither is an activation.
type plane struct {
	harness *Harness
	// holder is this delivery's claim identity, so two overlapping activations contend (rule H12).
	holder string
	// inherited is the correlation id an unstamped write inherits (sidecar.md rule B22).
	inherited string
	// strict scopes Get to the manifest reads; it is an activation-only, opt-in aid (rule H13).
	strict bool
	// recording is set for an activation, whose writes land on its Record (rule H8).
	recording bool

	mu     sync.Mutex
	writes []Write
}

// Get reads one key, returning (nil, nil) when it is absent (rule S3).
func (p *plane) Get(ctx context.Context, key string) (*bbsdk.Entry, error) {
	if p.strict && !reserved(key) && !matchesAny(key, p.harness.loaded.Reads) {
		return nil, fmt.Errorf("%w: %q matches no reads pattern", ErrUndeclaredRead, key)
	}
	entry, err := p.harness.board.Get(ctx, key)
	if err != nil || entry == nil {
		return nil, err
	}
	return &bbsdk.Entry{Key: entry.Key, Value: entry.Value, Revision: entry.Revision}, nil
}

// Keys lists every key on the board; reads are never scoped here, strict mode included (rule H13).
func (p *plane) Keys(ctx context.Context) ([]string, error) {
	return p.harness.board.Keys(ctx)
}

// Put writes a declared key unconditionally, carrying the activation's effective id (rules H7, H8).
func (p *plane) Put(ctx context.Context, key string, value map[string]any) (uint64, error) {
	if err := p.authorize(key); err != nil {
		return 0, err
	}
	stamped := stampCorrelation(value, p.inherited)
	revision, err := p.harness.board.Put(ctx, key, stamped)
	if err != nil {
		return 0, err
	}
	p.record(Write{Key: key, Value: stamped, Revision: revision})
	return revision, nil
}

// PutCAS writes a declared key only while it stands at revision (rules H7, H8).
func (p *plane) PutCAS(ctx context.Context, key string, value map[string]any, revision uint64) (uint64, error) {
	if err := p.authorize(key); err != nil {
		return 0, err
	}
	stamped := stampCorrelation(value, p.inherited)
	written, err := p.harness.board.PutCAS(ctx, key, stamped, revision)
	if err != nil {
		return 0, err
	}
	p.record(Write{Key: key, Value: stamped, Revision: written})
	return written, nil
}

// Delete removes a declared key unconditionally (rules H7, H8).
func (p *plane) Delete(_ context.Context, key string) error {
	if err := p.authorize(key); err != nil {
		return err
	}
	p.record(Write{Key: key, Revision: p.harness.store.deleteAt(key)})
	return nil
}

// DeleteCAS removes a declared key only while it stands at revision (rules H7, H8).
func (p *plane) DeleteCAS(_ context.Context, key string, revision uint64) error {
	if err := p.authorize(key); err != nil {
		return err
	}
	written, err := p.harness.store.deleteCAS(key, revision)
	if err != nil {
		return err
	}
	if written != 0 {
		// An absent key is a no-op that removed nothing, so no delete is recorded for it.
		p.record(Write{Key: key, Revision: written})
	}
	return nil
}

// Claim acquires a meta.claim.* lease against the virtual clock; false is a normal outcome (rule H12).
func (p *plane) Claim(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := p.claimable(key); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, fmt.Errorf("harness: claiming %q needs a positive TTL, got %s", key, ttl)
	}
	now := p.harness.clock.now()
	lease := map[string]any{"holder": p.holder, "expires_at": epochSeconds(now.Add(ttl))}
	entry, err := p.harness.board.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if entry == nil {
		// Absent: create-if-absent CAS. A conflict means another acquirer won the race (§6 rule 1).
		return p.acquire(ctx, key, lease, 0)
	}
	if expiry, live := leaseExpiry(entry.Value); live && expiry.After(now) {
		// Live: someone holds the lease, so no write is attempted.
		return false, nil
	}
	// Expired: replace-if-expired CAS, which is what AdvanceTime makes reachable without waiting.
	return p.acquire(ctx, key, lease, entry.Revision)
}

// ReleaseClaim releases a lease this delivery holds; a foreign or stale one is left for expiry (§6 rule 3).
func (p *plane) ReleaseClaim(ctx context.Context, key string) error {
	if err := p.claimable(key); err != nil {
		return err
	}
	entry, err := p.harness.board.Get(ctx, key)
	if err != nil || entry == nil {
		return err
	}
	if holder, ok := entry.Value["holder"].(string); !ok || holder != p.holder {
		return nil
	}
	if _, err := p.harness.store.deleteCAS(key, entry.Revision); err != nil {
		// A conflict means the lease was already replaced after expiry — there is nothing to release.
		if errors.Is(err, blackboard.ErrRevisionConflict) {
			return nil
		}
		return err
	}
	return nil
}

// Invoke answers from the registered responders, gated by the manifest's invokes set (rule H14).
func (p *plane) Invoke(
	ctx context.Context,
	service, endpoint string,
	payload map[string]any,
	_ ...bbsdk.InvokeOption,
) (map[string]any, error) {
	target := service + "." + endpoint
	if !slices.Contains(p.harness.loaded.Invokes, target) {
		// Declaration precedes transport, so no responder is consulted for an undeclared target.
		return nil, bbsdk.HarnessInvokeError(codeInvokeNotDeclared,
			fmt.Sprintf("the manifest declares no invoke grant for %q", target))
	}
	responder := p.harness.options.InvokeResponders[service][endpoint]
	if responder == nil {
		// The fake models responder presence, never discovery: no SERVICE_UNKNOWN is reproduced.
		return nil, bbsdk.HarnessInvokeError(codeInvokeNoResponders,
			fmt.Sprintf("no responder is registered for %q", target))
	}
	reply, err := responder(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %w",
			bbsdk.HarnessInvokeError(codeInvokeServiceError, fmt.Sprintf("the %q responder failed", target)), err)
	}
	if reply == nil {
		return map[string]any{}, nil
	}
	return reply, nil
}

// acquire performs one lease CAS, reading a lost race as "not acquired" (§6 rule 1).
func (p *plane) acquire(ctx context.Context, key string, lease map[string]any, revision uint64) (bool, error) {
	if _, err := p.harness.board.PutCAS(ctx, key, lease, revision); err != nil {
		if errors.Is(err, blackboard.ErrRevisionConflict) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// authorize applies the write half of the data plane: meta.* is blocked unconditionally and every
// other key must match a declared writes pattern (rule H7).
func (p *plane) authorize(key string) error {
	if reserved(key) {
		return fmt.Errorf("%w: %q is in the platform-reserved meta.* key space", ErrUndeclaredWrite, key)
	}
	if !writeAuthorized(key, p.harness.loaded.Writes) {
		return fmt.Errorf("%w: %q matches no writes pattern", ErrUndeclaredWrite, key)
	}
	return nil
}

// claimable reports the claim namespace check: a foreign claim key is refused as a write would be (rule H12).
func (p *plane) claimable(key string) error {
	if !strings.HasPrefix(key, claimPrefix) {
		return fmt.Errorf("%w: %q is not a %s* claim key", ErrUndeclaredWrite, key, claimPrefix)
	}
	return nil
}

// record appends one successful write to the activation's record, in application order (rule H8).
func (p *plane) record(write Write) {
	if !p.recording {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writes = append(p.writes, write)
}

// applied returns the writes this delivery made, in application order.
func (p *plane) applied() []Write {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.writes)
}

// stampCorrelation returns the value carrying the effective correlation id, unless it already
// carries its own — the component's override, which wins — or no id applies (sidecar.md rule B23).
func stampCorrelation(value map[string]any, effective string) map[string]any {
	if effective == "" || blackboard.CorrelationOf(value) != "" {
		return value
	}
	return blackboard.WithCorrelation(value, effective)
}

// effectiveInherited returns an activation's inherited correlation id: the triggering entry's, else
// the id of any correlated snapshot key in deterministic key order, else none (sidecar.md rule B22).
func effectiveInherited(snapshot map[string]map[string]any, changedKeys []string) string {
	for _, key := range changedKeys {
		if cid := blackboard.CorrelationOf(snapshot[key]); cid != "" {
			return cid
		}
	}
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if cid := blackboard.CorrelationOf(snapshot[key]); cid != "" {
			return cid
		}
	}
	return ""
}

// leaseExpiry reads a lease value's expires_at, reporting false when it carries none it can read (§6).
func leaseExpiry(value map[string]any) (time.Time, bool) {
	seconds, ok := value["expires_at"].(float64)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(0, int64(seconds*float64(time.Second))).UTC(), true
}

// epochSeconds renders a time as the fractional epoch seconds a lease's expires_at carries (§6).
func epochSeconds(at time.Time) float64 {
	return float64(at.UnixNano()) / float64(time.Second)
}
