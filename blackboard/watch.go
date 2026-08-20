package blackboard

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The default watch re-subscribe schedule: 1 s x 2ⁿ capped at 30 s, reset after a healthy minute (§1 rule 8).
const (
	defaultBackoffBase  = time.Second
	defaultBackoffCap   = 30 * time.Second
	defaultBackoffReset = time.Minute
)

// WatchOptions carries the optional watch callbacks; the zero value registers none.
type WatchOptions struct {
	// OnError receives a failing callback, an undeserializable entry, or a watcher failure (§1 rule 8).
	OnError func(error)
	// OnSynced fires once, after the last initial value and before the first delta (§1 rule 9).
	OnSynced func()
	// OnDelete receives a delete or purge of a watched key; the main callback never sees one (§1 rule 10).
	OnDelete func(key string, revision uint64)
}

// WatchBackoff is the injectable watch re-subscribe schedule; zero fields take the rule-8 defaults.
type WatchBackoff struct {
	Base  time.Duration
	Cap   time.Duration
	Reset time.Duration
	// Sleep waits for d, reporting false when ctx is cancelled first.
	Sleep func(ctx context.Context, d time.Duration) bool
	// Now reads the clock the healthy-interval reset is measured against.
	Now func() time.Time
}

// delay returns the re-subscribe wait for an attempt: base x 2ⁿ, capped (§1 rule 8).
func (w WatchBackoff) delay(attempt int) time.Duration {
	base, ceiling := w.Base, w.Cap
	if base <= 0 {
		base = defaultBackoffBase
	}
	if ceiling <= 0 {
		ceiling = defaultBackoffCap
	}
	delay := base
	for i := 0; i < attempt && delay < ceiling; i++ {
		delay *= 2
	}
	if delay > ceiling {
		delay = ceiling
	}
	return delay
}

// resetInterval returns the healthy duration after which the attempt counter returns to zero.
func (w WatchBackoff) resetInterval() time.Duration {
	if w.Reset <= 0 {
		return defaultBackoffReset
	}
	return w.Reset
}

// wait sleeps for the attempt's delay, reporting false when the watch was cancelled instead.
func (w WatchBackoff) wait(ctx context.Context, attempt int) bool {
	sleep := w.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	return sleep(ctx, w.delay(attempt))
}

// now reads the backoff clock.
func (w WatchBackoff) now() time.Time {
	if w.Now == nil {
		return time.Now()
	}
	return w.Now()
}

// watchConn is the NATS connection a watch re-arms on. A connection loss leaves the watcher's
// server-side ordered consumer unverifiable — the client library only notices by its own idle-
// heartbeat floor, tens of seconds later — so a watch that trusted it would report subscribed while
// delivering nothing. Every drop therefore unsubscribes the watch until it has established a fresh
// consumer against a re-attached client (§1 rule 13). *nats.Conn is the production implementation.
type watchConn interface {
	IsConnected() bool
	IsClosed() bool
	StatusChanged(statuses ...nats.Status) chan nats.Status
	RemoveStatusListener(ch chan nats.Status)
}

var _ watchConn = (*nats.Conn)(nil)

// statusEvents returns a listener for this blackboard's connection transitions, nil when none is wired.
func (b *Blackboard) statusEvents() chan nats.Status {
	if b.conn == nil {
		return nil
	}
	return b.conn.StatusChanged()
}

// releaseStatus hands a connection listener back; a nil channel was never registered.
func (b *Blackboard) releaseStatus(events chan nats.Status) {
	if b.conn == nil || events == nil {
		return
	}
	b.conn.RemoveStatusListener(events)
}

// awaitConnection parks until the client is attached again, reporting false when the watch ended
// first. A closed connection returns at once, leaving the failure to the ordinary re-subscribe
// path so rule 8's report-and-back-off survives (§1 rules 8, 13).
func awaitConnection(ctx context.Context, conn watchConn, events <-chan nats.Status) bool {
	for !conn.IsConnected() && !conn.IsClosed() {
		select {
		case <-ctx.Done():
			return false
		case <-events:
		}
	}
	return true
}

// sleepContext waits for d unless ctx is cancelled first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// watchState is one key pattern's live watcher, swapped on every re-subscribe.
type watchState struct {
	mu         sync.Mutex
	watcher    jetstream.KeyWatcher
	closed     bool
	subscribed atomic.Bool
	running    atomic.Bool
}

// adopt installs the pattern's current watcher, reporting false once the handle has been stopped.
func (s *watchState) adopt(watcher jetstream.KeyWatcher) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.watcher = watcher
	return true
}

// release stops and forgets the current watcher, leaving the state re-subscribable.
func (s *watchState) release() {
	s.mu.Lock()
	watcher := s.watcher
	s.watcher = nil
	s.mu.Unlock()
	stopWatcher(watcher)
}

// close stops the current watcher for good; no later watcher is adopted.
func (s *watchState) close() {
	s.mu.Lock()
	watcher := s.watcher
	s.watcher = nil
	s.closed = true
	s.mu.Unlock()
	stopWatcher(watcher)
}

// stopWatcher stops a watcher, logging the failure a caller can do nothing about.
func stopWatcher(watcher jetstream.KeyWatcher) {
	if watcher == nil {
		return
	}
	if err := watcher.Stop(); err != nil {
		slog.Debug("blackboard: stopping watcher failed", "error", err)
	}
}

// WatchHandle cancels a watch and reports its two liveness signals (§1 rules 8, 13).
type WatchHandle struct {
	cancel  context.CancelFunc
	conn    watchConn
	states  []*watchState
	wg      sync.WaitGroup
	once    sync.Once
	stopped atomic.Bool
}

// Alive reports whether the watch is running or backing off toward re-subscribe (§1 rule 8).
func (h *WatchHandle) Alive() bool {
	if h.stopped.Load() || len(h.states) == 0 {
		return false
	}
	for _, state := range h.states {
		if !state.running.Load() {
			return false
		}
	}
	return true
}

// Subscribed reports whether every watcher is established and pumping entries (§1 rule 13).
func (h *WatchHandle) Subscribed() bool {
	if h.stopped.Load() || len(h.states) == 0 {
		return false
	}
	// A detached client makes every consumer stale the instant it happens, well before the pump
	// goroutine can act on it: reporting subscribed across that gap is the silent-zombie window.
	if h.conn != nil && !h.conn.IsConnected() {
		return false
	}
	for _, state := range h.states {
		if !state.subscribed.Load() {
			return false
		}
	}
	return true
}

// Stop cancels the watch and waits for its delivery to finish; it is safe to call more than once.
func (h *WatchHandle) Stop() {
	h.once.Do(func() {
		h.stopped.Store(true)
		h.cancel()
		for _, state := range h.states {
			state.close()
		}
		h.wg.Wait()
	})
}

// Watch delivers each watched key's live values to callback until the handle is stopped or ctx is cancelled.
func (b *Blackboard) Watch(ctx context.Context, keys []string, callback func(Entry), opts WatchOptions) (*WatchHandle, error) {
	watchCtx, cancel := context.WithCancel(ctx)
	handle := &WatchHandle{cancel: cancel, conn: b.conn}
	var tracker *syncTracker
	if opts.OnSynced != nil {
		tracker = &syncTracker{pending: len(keys), onSynced: opts.OnSynced}
	}

	watchers := make([]jetstream.KeyWatcher, 0, len(keys))
	events := make([]chan nats.Status, 0, len(keys))
	for _, pattern := range keys {
		// The connection listener is registered before the watcher it guards exists, so a drop
		// landing between the two cannot be missed and leave the watch on a stale consumer.
		events = append(events, b.statusEvents())
		watcher, err := b.kv.Watch(watchCtx, pattern)
		if err != nil {
			for _, started := range watchers {
				stopWatcher(started)
			}
			for _, listener := range events {
				b.releaseStatus(listener)
			}
			cancel()
			return nil, fmt.Errorf("blackboard: watch %q: %w", pattern, err)
		}
		watchers = append(watchers, watcher)
	}

	for i, pattern := range keys {
		state := &watchState{}
		state.running.Store(true)
		handle.states = append(handle.states, state)
		handle.wg.Add(1)
		watcher, listener := watchers[i], events[i]
		go func() {
			defer handle.wg.Done()
			defer state.running.Store(false)
			defer b.releaseStatus(listener)
			b.runWatch(watchCtx, pattern, state, watcher, listener, callback, opts, tracker)
		}()
	}
	return handle, nil
}

// runWatch pumps one key pattern, re-subscribing with backoff until the watch is cancelled (§1 rules 8, 13).
func (b *Blackboard) runWatch(
	ctx context.Context,
	pattern string,
	state *watchState,
	watcher jetstream.KeyWatcher,
	events <-chan nats.Status,
	callback func(Entry),
	opts WatchOptions,
	tracker *syncTracker,
) {
	// Per-key highest revision delivered: a re-subscribed watcher replays current state, and an entry
	// at or below the last revision delivered for its key is a duplicate initial value (§1 rule 13).
	delivered := map[string]uint64{}
	synced := false
	attempt := 0
	for {
		if watcher == nil {
			next, err := b.kv.Watch(ctx, pattern)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("blackboard: re-subscribing a watch failed", "keys", pattern, "error", err)
				emit(opts.OnError, fmt.Errorf("blackboard: re-subscribing watch %q: %w", pattern, err))
				if !b.backoff.wait(ctx, attempt) {
					return
				}
				attempt++
				continue
			}
			watcher = next
		}
		if !state.adopt(watcher) {
			stopWatcher(watcher)
			return
		}
		state.subscribed.Store(true)
		healthySince := b.backoff.now()
		updates := watcher.Updates()
		dropped := false
	pump:
		for {
			select {
			case <-ctx.Done():
				state.subscribed.Store(false)
				state.release()
				return // only cancellation ends a watch (§1 rule 8)
			case status := <-events:
				// Judged on the event, never on the status read back: a drop that has already
				// reconnected still leaves this watcher's consumer unverified.
				if status == nats.CONNECTED {
					continue
				}
				dropped = true
				break pump
			case entry, open := <-updates:
				if !open {
					break pump
				}
				if attempt > 0 && b.backoff.now().Sub(healthySince) >= b.backoff.resetInterval() {
					attempt = 0
				}
				if entry == nil {
					if tracker != nil && !synced {
						synced = true
						tracker.markSynced()
					}
					continue
				}
				deliver(entry, delivered, callback, opts)
			}
		}
		state.subscribed.Store(false)
		state.release()
		watcher = nil
		if ctx.Err() != nil {
			return
		}
		if dropped {
			// The client detached under this watcher, so its consumer is stale whatever the server
			// still holds: the watch establishes a fresh one the moment the client is attached
			// again rather than waiting out the client library's idle-heartbeat floor. A reconnect
			// is not a watch failure — it costs no on_error and no backoff attempt (§1 rule 13).
			slog.Info("blackboard: NATS detached under a watch; re-arming on reconnect", "keys", pattern)
			if !awaitConnection(ctx, b.conn, events) {
				return
			}
			continue
		}
		slog.Error("blackboard: watch ended unexpectedly; re-subscribing", "keys", pattern)
		emit(opts.OnError, fmt.Errorf("blackboard: watch %q ended unexpectedly; re-subscribing", pattern))
		if !b.backoff.wait(ctx, attempt) {
			return
		}
		attempt++
	}
}

// deliver routes one watched entry to on_delete or the value callback, suppressing already-delivered revisions.
func deliver(entry jetstream.KeyValueEntry, delivered map[string]uint64, callback func(Entry), opts WatchOptions) {
	revision := entry.Revision()
	if revision <= delivered[entry.Key()] {
		return
	}
	delivered[entry.Key()] = revision
	if isDelete(entry.Operation()) {
		if opts.OnDelete != nil {
			guard(func() { opts.OnDelete(entry.Key(), revision) }, opts.OnError, "on-delete handler")
		}
		return
	}
	// An empty payload is a malformed entry, not a delivery: rule 8 reports it rather than dropping it.
	value, err := decodeValue(entry.Value())
	if err != nil {
		wrapped := fmt.Errorf("blackboard: watch entry %q revision %d: %w", entry.Key(), revision, err)
		slog.Error("blackboard: undeserializable watch entry", "key", entry.Key(), "error", err)
		emit(opts.OnError, wrapped)
		return
	}
	update := Entry{Key: entry.Key(), Value: value, Revision: revision, TS: entryTS(entry.Created())}
	guard(func() { callback(update) }, opts.OnError, "callback")
}

// syncTracker fires on_synced once, after every watcher has passed its end-of-initial-values marker.
type syncTracker struct {
	mu       sync.Mutex
	pending  int
	fired    bool
	onSynced func()
}

// markSynced records one watcher's end-of-initial-values marker and fires once all have arrived.
func (t *syncTracker) markSynced() {
	t.mu.Lock()
	if t.fired {
		t.mu.Unlock()
		return
	}
	t.pending--
	if t.pending > 0 {
		t.mu.Unlock()
		return
	}
	t.fired = true
	t.mu.Unlock()
	guard(t.onSynced, nil, "on-synced handler")
}

// guard runs a consumer handler so a panicking one is logged and reported rather than ending the watch (§1 rule 8).
func guard(fn func(), onError func(error), what string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err := fmt.Errorf("blackboard: watch %s panicked: %v", what, recovered)
			slog.Error("blackboard: watch handler panicked", "handler", what, "panic", recovered)
			emit(onError, err)
		}
	}()
	fn()
}

// emit hands an error to the consumer's on_error, absorbing a handler that panics in turn.
func emit(onError func(error), err error) {
	if onError == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("blackboard: watch on-error handler panicked", "panic", recovered)
		}
	}()
	onError(err)
}
