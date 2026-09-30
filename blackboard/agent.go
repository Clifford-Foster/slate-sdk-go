package blackboard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// DefaultDebounceWindow is the quiet period a burst of watched-key changes collapses into (§3.4 rule 1).
const DefaultDebounceWindow = 10 * time.Millisecond

// Debounce is the injectable evaluation debounce; the zero value is the rule-1 default window.
type Debounce struct {
	// Window is the quiet period a burst of changes collapses into one evaluation over; zero means 10ms.
	Window time.Duration
	// Wait blocks for one debounce window, reporting false when ctx ends; nil waits on a real timer.
	Wait func(ctx context.Context, d time.Duration) bool
}

// window returns the configured quiet period, defaulting to the rule-1 10ms.
func (d Debounce) window() time.Duration {
	if d.Window <= 0 {
		return DefaultDebounceWindow
	}
	return d.Window
}

// wait blocks for one debounce window, reporting false when the agent was stopped instead.
func (d Debounce) wait(ctx context.Context) bool {
	wait := d.Wait
	if wait == nil {
		wait = sleepContext
	}
	return wait(ctx, d.window())
}

// Clock is an agent's time source for its write timestamps and the OLDER_THAN now (§3.5).
type Clock func() time.Time

// AgentSpec declares an agent's watched keys, activation rule, events, card, and handlers (§3.1).
type AgentSpec struct {
	// Reads are the KV keys the agent watches; wildcards are supported.
	Reads []string
	// Writes are the KV keys the agent writes; documentation only, published on its card.
	Writes []string
	// Precondition is the activation DSL expression; empty means every change activates (§3.4 rule 4).
	Precondition string
	// Events are the NATS subjects the agent subscribes to.
	Events []string
	// Card is published on Start and removed on Stop whenever a registry is supplied (§3.4 rule 8).
	Card *ComponentCard
	// Activate runs when the precondition is met; only one runs at a time (§3.4 rule 2).
	Activate func(ctx context.Context, bb *Blackboard) error
	// OnEvent handles a message on an Events subject, independent of precondition state (§3.4 rule 5).
	OnEvent func(ctx context.Context, bb *Blackboard, msg *nats.Msg) error
}

// StartOptions carries the optional collaborators an agent starts with; the zero value supplies none.
type StartOptions struct {
	// Conn subscribes the agent's Events subjects; without it no event subscription is made.
	Conn *nats.Conn
	// Registry publishes the agent's card on start and removes it on stop (§3.4 rule 8).
	Registry *CardRegistry
}

// Evaluation is one completed precondition evaluation, reported to WithOnEvaluated's hook (§3.5).
type Evaluation struct {
	// State is the evaluation's inputs exactly as the precondition saw them.
	State EvalState
	// Changed are the watched keys written or deleted since the previous evaluation, sorted.
	Changed []string
	// Revisions is each snapshot key's revision at capture; a deleted or never-delivered key is absent.
	Revisions map[string]uint64
	// Activated reports whether the precondition held, so an activation runs after the hook returns.
	Activated bool
}

// AgentOption configures an agent at construction.
type AgentOption func(*Agent)

// WithDebounce injects the precondition-evaluation debounce (§3.4 rule 1).
func WithDebounce(debounce Debounce) AgentOption {
	return func(a *Agent) { a.debounce = debounce }
}

// WithClock injects the agent's time source; without it the wall clock is used (§3.5).
func WithClock(clock Clock) AgentOption {
	return func(a *Agent) { a.clock = clock }
}

// WithOnEvaluated installs an observation hook called at every evaluation's commit point (§3.5).
func WithOnEvaluated(hook func(Evaluation)) AgentOption {
	return func(a *Agent) { a.onEvaluated = hook }
}

// Agent watches its keys, debounces changes, evaluates its precondition, and activates one at a time (§3.2).
type Agent struct {
	spec         AgentSpec
	debounce     Debounce
	clock        Clock
	precondition *Precondition
	onEvaluated  func(Evaluation)
	// evaluated reports each completed debounced evaluation to this package's tests (testexport.go).
	evaluated func(activated bool)

	mu          sync.Mutex
	snapshot    map[string]any
	previous    map[string]any
	timestamps  map[string]time.Time
	revisions   map[string]uint64
	changedKeys map[string]struct{}
	changed     bool
	board       *Blackboard
	registry    *CardRegistry
	watch       *WatchHandle
	subs        []*nats.Subscription
	cancel      context.CancelFunc
	done        chan struct{}

	wake chan struct{}
}

// NewAgent returns an agent driven by spec, with its precondition parsed once for reuse (§4 rule 1).
func NewAgent(spec AgentSpec, opts ...AgentOption) (*Agent, error) {
	agent := &Agent{
		spec:        spec,
		snapshot:    map[string]any{},
		previous:    map[string]any{},
		timestamps:  map[string]time.Time{},
		revisions:   map[string]uint64{},
		changedKeys: map[string]struct{}{},
		wake:        make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(agent)
	}
	if spec.Precondition != "" {
		precondition, err := ParsePrecondition(spec.Precondition)
		if err != nil {
			return nil, err
		}
		agent.precondition = precondition
	}
	return agent, nil
}

// Start publishes the agent's card, watches its keys, and subscribes to its events (§3.4 rules 6, 8).
func (a *Agent) Start(ctx context.Context, board *Blackboard, opts StartOptions) error {
	runCtx, cancel, err := a.reserve(ctx, board, opts.Registry)
	if err != nil {
		return err
	}

	if a.spec.Card != nil && opts.Registry != nil {
		if _, err := opts.Registry.Publish(ctx, a.publishedCard()); err != nil {
			a.release(cancel)
			return err
		}
	}

	// An agent with no watched keys has no replay to wait on and no change to evaluate, so its
	// loop parks on a channel nothing closes until Stop cancels it (§3.4 rule 3).
	synced := make(chan struct{})
	var handle *WatchHandle
	if len(a.spec.Reads) > 0 {
		handle, err = board.Watch(runCtx, a.spec.Reads, a.onChange, WatchOptions{
			OnSynced: func() { close(synced) },
			OnDelete: a.onDelete,
		})
		if err != nil {
			a.release(cancel)
			return err
		}
	}

	subs := make([]*nats.Subscription, 0, len(a.spec.Events))
	if opts.Conn != nil {
		for _, subject := range a.spec.Events {
			sub, err := opts.Conn.Subscribe(subject, func(msg *nats.Msg) { a.handleEvent(runCtx, msg) })
			if err != nil {
				if handle != nil {
					handle.Stop()
				}
				a.release(cancel)
				return errors.Join(fmt.Errorf("blackboard: subscribing agent to %q: %w", subject, err), unsubscribe(subs))
			}
			subs = append(subs, sub)
		}
	}

	done := make(chan struct{})
	a.mu.Lock()
	a.watch, a.subs, a.done = handle, subs, done
	a.mu.Unlock()
	go func() {
		defer close(done)
		a.run(runCtx, synced)
	}()
	return nil
}

// Stop stops the watches and event subscriptions, then removes the agent's published card (§3.4 rule 8).
func (a *Agent) Stop(ctx context.Context) error {
	a.mu.Lock()
	cancel, done, handle, subs, registry := a.cancel, a.done, a.watch, a.subs, a.registry
	a.cancel, a.done, a.watch, a.subs, a.registry, a.board = nil, nil, nil, nil, nil, nil
	a.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("blackboard: stopping agent: %w", ctx.Err())
		}
	}
	if handle != nil {
		handle.Stop()
	}
	errs := []error{unsubscribe(subs)}
	if a.spec.Card != nil && registry != nil {
		errs = append(errs, registry.Remove(ctx, a.spec.Card.Name))
	}
	return errors.Join(errs...)
}

// reserve claims the agent for one run, returning the context its watches and loop live under.
func (a *Agent) reserve(ctx context.Context, board *Blackboard, registry *CardRegistry) (context.Context, context.CancelFunc, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cancel != nil {
		return nil, nil, ErrAgentStarted
	}
	runCtx, cancel := context.WithCancel(ctx)
	a.board, a.registry, a.cancel = board, registry, cancel
	return runCtx, cancel, nil
}

// release undoes reserve when the rest of Start fails.
func (a *Agent) release(cancel context.CancelFunc) {
	cancel()
	a.mu.Lock()
	a.board, a.registry, a.cancel = nil, nil, nil
	a.mu.Unlock()
}

// publishedCard overlays the agent's declared keys and rule onto its card, never mutating it (§3.4 rule 10).
func (a *Agent) publishedCard() ComponentCard {
	card := *a.spec.Card
	card.Reads, card.Writes, card.Precondition = nil, nil, a.spec.Precondition
	if len(a.spec.Reads) > 0 {
		card.Reads = slices.Clone(a.spec.Reads)
	}
	if len(a.spec.Writes) > 0 {
		card.Writes = slices.Clone(a.spec.Writes)
	}
	return card
}

// now reads the agent's clock, defaulting to the wall clock (§3.5).
func (a *Agent) now() time.Time {
	if a.clock == nil {
		return time.Now().UTC()
	}
	return a.clock().UTC()
}

// onChange records a watched key's new value and wakes the evaluation loop (§3.4 rule 3).
func (a *Agent) onChange(entry Entry) {
	written := a.now()
	if entry.TS != nil {
		written = *entry.TS
	}
	a.mu.Lock()
	a.snapshot[entry.Key] = entry.Value
	a.timestamps[entry.Key] = written
	a.revisions[entry.Key] = entry.Revision
	a.changedKeys[entry.Key] = struct{}{}
	a.changed = true
	a.mu.Unlock()
	a.notify()
}

// onDelete drops a deleted watched key from the snapshot and wakes the evaluation loop (§3.4 rule 9).
func (a *Agent) onDelete(key string, _ uint64) {
	a.mu.Lock()
	delete(a.snapshot, key)
	delete(a.timestamps, key)
	delete(a.revisions, key)
	a.changedKeys[key] = struct{}{}
	a.changed = true
	a.mu.Unlock()
	a.notify()
}

// notify wakes the evaluation loop without ever blocking a watch delivery.
func (a *Agent) notify() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// markChanged records a pending change with no key behind it: the completed replay's own trigger.
func (a *Agent) markChanged() {
	a.mu.Lock()
	a.changed = true
	a.mu.Unlock()
}

// takeChange consumes the pending-change flag, reporting whether one was recorded.
func (a *Agent) takeChange() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	changed := a.changed
	a.changed = false
	return changed
}

// run holds the first evaluation until the watch's initial replay is complete, then evaluates the
// precondition once per debounced burst, one activation at a time (§3.4 rules 1, 2, 3).
func (a *Agent) run(ctx context.Context, synced <-chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case <-synced:
	}
	// The completed replay is itself the first evaluation's trigger, so an agent whose watched keys
	// hold nothing at start still evaluates exactly once against the empty snapshot (§3.4 rule 3).
	a.markChanged()
	for {
		if !a.awaitChange(ctx) {
			return
		}
		if !a.settle(ctx) {
			return
		}
		a.maybeActivate(ctx)
	}
}

// awaitChange blocks until a watched key changes, reporting false once the agent is stopped.
func (a *Agent) awaitChange(ctx context.Context) bool {
	for {
		if a.takeChange() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-a.wake:
		}
	}
}

// settle waits out the debounce window, restarting it while further changes keep arriving (§3.4 rule 1).
func (a *Agent) settle(ctx context.Context) bool {
	for {
		if !a.debounce.wait(ctx) {
			return false
		}
		if !a.takeChange() {
			return true
		}
	}
}

// maybeActivate evaluates the precondition against the current snapshot and activates when it holds.
func (a *Agent) maybeActivate(ctx context.Context) {
	activated := a.evaluate(ctx)
	if a.evaluated != nil {
		a.evaluated(activated)
	}
}

// evaluate runs one precondition evaluation and its activation, reporting whether it activated.
func (a *Agent) evaluate(ctx context.Context) bool {
	board, state, changed, revisions := a.capture()
	if board == nil {
		return false
	}
	activated := a.precondition == nil || a.precondition.Evaluate(state)
	if activated {
		a.mu.Lock()
		a.previous = state.Snapshot // the snapshot CHANGED compares against next time (§3.4 rule 3)
		a.mu.Unlock()
	}
	if a.onEvaluated != nil {
		a.onEvaluated(Evaluation{State: state, Changed: changed, Revisions: revisions, Activated: activated})
	}
	if !activated {
		return false
	}
	a.activate(ctx, board)
	return true
}

// capture takes one evaluation's board, inputs, changed keys and snapshot revisions in one step,
// resetting the changed set at every evaluation whether or not it activates (§3.5).
func (a *Agent) capture() (*Blackboard, EvalState, []string, map[string]uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	changed := slices.Sorted(maps.Keys(a.changedKeys))
	a.changedKeys = map[string]struct{}{}
	state := EvalState{
		Snapshot:   maps.Clone(a.snapshot),
		Previous:   a.previous,
		Timestamps: maps.Clone(a.timestamps),
		Now:        a.now(),
	}
	return a.board, state, changed, maps.Clone(a.revisions)
}

// activate runs the consumer's activation, absorbing the failure Python logs rather than ending the loop.
func (a *Agent) activate(ctx context.Context, board *Blackboard) {
	if a.spec.Activate == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("blackboard: agent activation panicked", "panic", recovered)
		}
	}()
	if err := a.spec.Activate(ctx, board); err != nil {
		slog.Error("blackboard: agent activation failed", "error", err)
	}
}

// handleEvent delivers a NATS event regardless of precondition state (§3.4 rule 5).
func (a *Agent) handleEvent(ctx context.Context, msg *nats.Msg) {
	a.mu.Lock()
	board := a.board
	a.mu.Unlock()
	if board == nil || a.spec.OnEvent == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("blackboard: agent event handler panicked", "panic", recovered)
		}
	}()
	if err := a.spec.OnEvent(ctx, board, msg); err != nil {
		slog.Error("blackboard: agent event handler failed", "subject", msg.Subject, "error", err)
	}
}

// unsubscribe drops every event subscription, collecting the failures.
func unsubscribe(subs []*nats.Subscription) error {
	errs := make([]error, 0, len(subs))
	for _, sub := range subs {
		if err := sub.Unsubscribe(); err != nil {
			errs = append(errs, fmt.Errorf("blackboard: unsubscribing agent from %q: %w", sub.Subject, err))
		}
	}
	return errors.Join(errs...)
}
