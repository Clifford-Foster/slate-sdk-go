package harness

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/Clifford-Foster/slate-sdk-go/blackboard"
)

// bucket is the in-memory board's bucket name; nothing addresses it, but every entry reports one.
const bucket = "harness"

// tickKey is the reserved key the harness delivers a synthetic tombstone under to make the engine
// re-evaluate after AdvanceTime. The core evaluates on watched-key changes only and exposes no
// re-evaluate entry point, so the poke rides the one channel the harness owns — the watch delivery
// itself. It is under the platform-reserved meta.* space, so no component key can collide with it,
// it enters no snapshot (a delete carries no value), and the harness strips it from every
// evaluation's changed set before a component or a record can see it (rule H5).
const tickKey = "meta.harness.tick"

// errUnsupported reports a KeyValue method the harness board does not implement, because the core
// library never calls it on this path. Failing loudly beats a silent wrong answer.
var errUnsupported = errors.New("harness: the in-memory board does not implement this KeyValue method")

// clock is the harness's virtual clock: it starts at the epoch, stamps every write, and moves only
// through AdvanceTime — wall-clock time never reaches a precondition (rule H5).
type clock struct {
	mu sync.Mutex
	at time.Time
}

// now reads the virtual clock.
func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

// advance shifts the virtual clock forward by d.
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// entry is one stored revision: a live value, or a tombstone the watch delivers as a delete.
type entry struct {
	key      string
	value    []byte
	revision uint64
	created  time.Time
	op       jetstream.KeyValueOp
}

func (e *entry) Bucket() string                  { return bucket }
func (e *entry) Key() string                     { return e.key }
func (e *entry) Value() []byte                   { return e.value }
func (e *entry) Revision() uint64                { return e.revision }
func (e *entry) Created() time.Time              { return e.created }
func (e *entry) Delta() uint64                   { return 0 }
func (e *entry) Operation() jetstream.KeyValueOp { return e.op }
func (e *entry) live() bool                      { return e.op == jetstream.KeyValuePut }
func newTombstone(key string, revision uint64, at time.Time) *entry {
	return &entry{key: key, revision: revision, created: at, op: jetstream.KeyValueDelete}
}

// board is the in-memory jetstream.KeyValue the harness runs the real engine over: revisions are
// bucket-wide monotonic as in JetStream KV, and every entry is stamped from the virtual clock, so
// the harness owns both the revisions rule H13 CAS-deletes at and the time rule H5 evaluates against
// (rule H2). An embedded NATS server would surrender both.
type board struct {
	clock *clock

	mu       sync.Mutex
	revision uint64
	entries  map[string]*entry
	watchers []*watcher
}

var _ jetstream.KeyValue = (*board)(nil)

// newBoard returns an empty in-memory board reading the harness's virtual clock.
func newBoard(c *clock) *board {
	return &board{clock: c, entries: map[string]*entry{}}
}

// Get returns a key's live entry, or ErrKeyNotFound for an absent or deleted one.
func (b *board) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	stored, ok := b.entries[key]
	if !ok || !stored.live() {
		return nil, jetstream.ErrKeyNotFound
	}
	return stored, nil
}

// Put writes a value unconditionally and returns its new bucket-wide revision.
func (b *board) Put(_ context.Context, key string, value []byte) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.write(key, value).revision, nil
}

// Create writes a value only while the key is absent or deleted (§6 rule 1's create-if-absent CAS).
func (b *board) Create(_ context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	if len(opts) > 0 {
		return 0, errUnsupported
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if stored, ok := b.entries[key]; ok && stored.live() {
		// The core reads this as a lost CAS, which is what an occupied key means to an acquirer.
		return 0, jetstream.ErrKeyExists
	}
	return b.write(key, value).revision, nil
}

// Update writes a value only while the key stands at revision (§1 rule 4).
func (b *board) Update(_ context.Context, key string, value []byte, revision uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	stored, ok := b.entries[key]
	if !ok || stored.revision != revision {
		return 0, wrongLastRevision(key, revision)
	}
	return b.write(key, value).revision, nil
}

// Delete soft-deletes a key, exactly as JetStream KV does: a tombstone lands whether or not the key
// was set, and every watcher sees it. The revision-CAS form is the harness's own deleteCAS, because
// the option carrying the revision cannot be read from outside the client library.
func (b *board) Delete(_ context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	if len(opts) > 0 {
		return errUnsupported
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tombstone(key)
	return nil
}

// deleteAt soft-deletes a key unconditionally and reports the tombstone's revision.
func (b *board) deleteAt(key string) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tombstone(key).revision
}

// deleteCAS soft-deletes a key only while it stands at revision, reporting the core library's
// ErrRevisionConflict on a miss; an absent key is a no-op, as an unconditional delete is (§1 rule 4).
func (b *board) deleteCAS(key string, revision uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	stored, ok := b.entries[key]
	if !ok || !stored.live() {
		return 0, nil
	}
	if stored.revision != revision {
		return 0, fmt.Errorf("%w: delete %q expected revision %d, found %d",
			blackboard.ErrRevisionConflict, key, revision, stored.revision)
	}
	return b.tombstone(key).revision, nil
}

// Keys returns every live key name, sorted so two identical runs list them identically (rule H6).
func (b *board) Keys(_ context.Context, opts ...jetstream.WatchOpt) ([]string, error) {
	if len(opts) > 0 {
		return nil, errUnsupported
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, 0, len(b.entries))
	for key, stored := range b.entries {
		if stored.live() {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys, nil
}

// Watch replays the matching keys in revision order, marks the end of the initial values, and then
// delivers every later write to the same pattern (§1 rules 5, 9, 10).
func (b *board) Watch(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	if len(opts) > 0 {
		return nil, errUnsupported
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	live := &watcher{
		pattern:  keys,
		board:    b,
		updates:  make(chan jetstream.KeyValueEntry),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	replay := make([]*entry, 0, len(b.entries))
	for _, stored := range b.entries {
		if matchesPattern(stored.key, keys) {
			replay = append(replay, stored)
		}
	}
	slices.SortFunc(replay, func(a, c *entry) int { return cmp.Compare(a.revision, c.revision) })
	for _, stored := range replay {
		live.push(stored)
	}
	// The nil marker is the KV watcher's end-of-initial-values signal the core reads for on_synced.
	live.push(nil)
	b.watchers = append(b.watchers, live)
	go live.run(ctx)
	return live, nil
}

// tick delivers a synthetic tombstone to one watcher, so the engine runs one more evaluation with no
// key change behind it; it reports false when no watch is armed to carry it (rule H5).
func (b *board) tick() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.watchers) == 0 {
		return false
	}
	// One watcher is enough: every watcher feeds the same agent, so poking one is exactly one
	// re-evaluation, where poking all of them would depend on the debounce to coalesce them.
	b.watchers[0].push(newTombstone(tickKey, b.revision, b.clock.now()))
	return true
}

// revisions reports the current revision of every live key matching one of patterns — the state a
// fully-delivered engine's own snapshot revisions equal, which is how Settle knows it is quiet.
func (b *board) revisions(patterns []string) map[string]uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	current := map[string]uint64{}
	for key, stored := range b.entries {
		if stored.live() && matchesAny(key, patterns) {
			current[key] = stored.revision
		}
	}
	return current
}

// write stores a new revision of a key and hands it to every matching watcher.
func (b *board) write(key string, value []byte) *entry {
	b.revision++
	stored := &entry{
		key:      key,
		value:    slices.Clone(value),
		revision: b.revision,
		created:  b.clock.now(),
		op:       jetstream.KeyValuePut,
	}
	b.entries[key] = stored
	b.notify(stored)
	return stored
}

// tombstone stores a delete marker for a key and hands it to every matching watcher.
func (b *board) tombstone(key string) *entry {
	b.revision++
	stored := newTombstone(key, b.revision, b.clock.now())
	b.entries[key] = stored
	b.notify(stored)
	return stored
}

// notify queues one entry on every watcher whose pattern matches it.
func (b *board) notify(stored *entry) {
	for _, live := range b.watchers {
		if matchesPattern(stored.key, live.pattern) {
			live.push(stored)
		}
	}
}

// detach forgets a watcher whose pump has ended, so no queue grows behind a stopped watch.
func (b *board) detach(gone *watcher) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.watchers = slices.DeleteFunc(b.watchers, func(live *watcher) bool { return live == gone })
}

// wait blocks until every armed watcher's delivery goroutine has ended (rule H16).
func (b *board) wait() {
	b.mu.Lock()
	live := slices.Clone(b.watchers)
	b.mu.Unlock()
	for _, pump := range live {
		<-pump.finished
	}
}

// watcher is one armed key pattern: an ordered queue the board appends to and a pump that hands
// entries to the core's watch loop. The queue is unbounded on purpose — dropping an entry under
// load would cost the determinism rule H6 guarantees.
type watcher struct {
	pattern string
	board   *board
	updates chan jetstream.KeyValueEntry
	wake    chan struct{}
	done    chan struct{}
	// finished closes when the pump has ended, so Stop can prove it left nothing running.
	finished chan struct{}

	mu      sync.Mutex
	queue   []jetstream.KeyValueEntry
	stopped bool
}

// Updates is the channel the core's watch loop reads entries from.
func (w *watcher) Updates() <-chan jetstream.KeyValueEntry { return w.updates }

// Stop ends this watcher's delivery; it is safe to call more than once.
func (w *watcher) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return nil
	}
	w.stopped = true
	close(w.done)
	return nil
}

// push appends one entry (nil is the end-of-initial-values marker) and wakes the pump.
func (w *watcher) push(stored *entry) {
	w.mu.Lock()
	if stored == nil {
		w.queue = append(w.queue, nil)
	} else {
		w.queue = append(w.queue, stored)
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// run hands queued entries to the core's watch loop until the watch is stopped or ctx is cancelled;
// it holds no lock while it blocks, so a slow consumer stalls nothing but its own delivery.
func (w *watcher) run(ctx context.Context) {
	defer close(w.finished)
	defer w.board.detach(w)
	for {
		w.mu.Lock()
		if len(w.queue) == 0 {
			w.mu.Unlock()
			select {
			case <-w.wake:
			case <-w.done:
				return
			case <-ctx.Done():
				return
			}
			continue
		}
		next := w.queue[0]
		w.queue = w.queue[1:]
		w.mu.Unlock()
		select {
		case w.updates <- next:
		case <-w.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

// wrongLastRevision is the lost-CAS signal the core reads a revision conflict from (§1 rule 14).
func wrongLastRevision(key string, expected uint64) error {
	return &jetstream.APIError{
		Code:        400,
		ErrorCode:   jetstream.JSErrCodeStreamWrongLastSequence,
		Description: fmt.Sprintf("wrong last sequence for %q, expected revision %d", key, expected),
	}
}

// The KeyValue surface the core library never reaches on this path. The harness answers each one
// honestly rather than inventing a result no test could trust.

func (b *board) GetRevision(context.Context, string, uint64) (jetstream.KeyValueEntry, error) {
	return nil, errUnsupported
}
func (b *board) PutString(context.Context, string, string) (uint64, error) { return 0, errUnsupported }
func (b *board) Purge(context.Context, string, ...jetstream.KVDeleteOpt) error {
	return errUnsupported
}
func (b *board) WatchAll(context.Context, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return nil, errUnsupported
}
func (b *board) WatchFiltered(context.Context, []string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return nil, errUnsupported
}
func (b *board) ListKeys(context.Context, ...jetstream.WatchOpt) (jetstream.KeyLister, error) {
	return nil, errUnsupported
}
func (b *board) ListKeysFiltered(context.Context, ...string) (jetstream.KeyLister, error) {
	return nil, errUnsupported
}
func (b *board) History(context.Context, string, ...jetstream.WatchOpt) ([]jetstream.KeyValueEntry, error) {
	return nil, errUnsupported
}
func (b *board) Bucket() string                                              { return bucket }
func (b *board) PurgeDeletes(context.Context, ...jetstream.KVPurgeOpt) error { return errUnsupported }
func (b *board) Status(context.Context) (jetstream.KeyValueStatus, error)    { return nil, errUnsupported }

// snapshotOf converts an evaluation's watched-key snapshot to the delivery shape, dropping any value
// the core could not decode as an object — the wire carries only JSON objects (§1 rule 1).
func snapshotOf(state map[string]any) map[string]map[string]any {
	snapshot := make(map[string]map[string]any, len(state))
	for key, value := range state {
		if object, ok := value.(map[string]any); ok {
			snapshot[key] = object
		}
	}
	return snapshot
}
