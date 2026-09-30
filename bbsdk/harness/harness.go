package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"time"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
	"github.com/Clifford-Foster/slate-sdk-go/blackboard"
	"github.com/Clifford-Foster/slate-sdk-go/manifest"
)

// defaultEpoch is where the virtual clock starts when Options carries no epoch (rule H4).
var defaultEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// blackboardID is the id every harness delivery carries; the harness attaches to no workspace.
const blackboardID = "harness"

// stage is the harness lifecycle: built, started, and terminally stopped (rule H10).
type stage int

const (
	stageBuilt stage = iota
	stageStarted
	stageStopped
)

// InvokeFunc is one registered invoke responder, keyed by service and endpoint (rule H14).
type InvokeFunc func(ctx context.Context, payload map[string]any) (map[string]any, error)

// Options carries the harness's optional collaborators; the zero value is a working harness (rule H4).
type Options struct {
	// Epoch is where the virtual clock starts; the zero value is 2026-01-01T00:00:00Z.
	Epoch time.Time
	// StrictReads scopes an activation's Get to the manifest reads; it is off by default (rule H13).
	StrictReads bool
	// RPCHandlers are the bridged endpoints CallRPC routes to (rule H13).
	RPCHandlers map[string]bbsdk.RPCFunc
	// InvokeResponders are the invoke fake's answers, by service then endpoint (rule H14).
	InvokeResponders map[string]map[string]InvokeFunc
	// Config is the instance configuration every delivery sees; unset is an empty map (rule H4).
	Config map[string]any
}

// Harness is one component under test: the real engine over an in-memory board (rules H1, H2).
type Harness struct {
	loaded    *manifest.Manifest
	component bbsdk.ActivationFunc
	options   Options
	config    map[string]any
	clock     *clock
	store     *board
	board     *blackboard.Blackboard
	agent     *blackboard.Agent
	// shapes are the manifest's declared write, read, publish and subscribe schemas, prepared at
	// construction so an unpreparable one fails there rather than on the first write (rules H20, H21).
	shapes *shapes
	// watchedReads are the keys the engine watches: the manifest's reads under blackboard, and none
	// under nats, where every read is a subscription instead and no board watch exists (rule H21).
	watchedReads []string
	// subscriptions is the effective subscription set Publish resolves a subject against (rule H21).
	subscriptions []manifest.Subscription
	// guard is the single-flight guard both activation sources share under a declared single_flight
	// (rule B39); under false a subject activation takes none of it.
	guard *flightGuard

	mu sync.Mutex
	// changed is closed and replaced on every observable step, so a waiter parks without polling.
	changed     chan struct{}
	stage       stage
	cancel      context.CancelFunc
	evaluations int
	// delivered is the snapshot revisions of the last completed evaluation: the engine's own view of
	// the board, which equals the board's when everything written has been evaluated (rule H11).
	delivered  map[string]uint64
	pending    blackboard.Evaluation
	inFlight   bool
	sequence   int
	deliveries int
	records    []Record
	cursor     int
	skips      []ValidationSkip
	// published is every message the component published, in order (rule H21).
	published []Published
	// subjectPending counts the subject activations queued or running, so Settle and Stop wait for
	// them exactly as they wait for a board activation (rules H10, H11, H21).
	subjectPending int
}

// New builds a harness over a validated manifest and the component's activation function (rule H1).
// A path-form schema source fails here: New has no manifest document to resolve it against, and
// rule H16 forbids the harness the read (rule H20).
func New(m *manifest.Manifest, component bbsdk.ActivationFunc, opts Options) (*Harness, error) {
	return build(m, component, opts, "")
}

// Open loads a manifest from disk and builds a harness over it (rule H1). A path-form schema source
// resolves against the manifest document's parent directory (rule H20).
func Open(path string, component bbsdk.ActivationFunc, opts Options) (*Harness, error) {
	// The manifest package's validation error propagates unchanged: the harness validates nothing
	// of its own, so a document rejected here is rejected in production (rules M1, H1).
	loaded, err := manifest.Load(path)
	if err != nil {
		return nil, err
	}
	return build(loaded, component, opts, filepath.Dir(path))
}

// build is the one construction path, carrying the directory a path-form schema source resolves
// against — Open's, and never New's (rule H20).
func build(m *manifest.Manifest, component bbsdk.ActivationFunc, opts Options, dir string) (*Harness, error) {
	if m == nil {
		return nil, errors.New("harness: New needs a validated manifest")
	}
	if component == nil {
		return nil, errors.New("harness: New needs a component function")
	}
	prepared, err := prepareShapes(m, dir)
	if err != nil {
		return nil, err
	}
	epoch := opts.Epoch
	if epoch.IsZero() {
		epoch = defaultEpoch
	}
	config := opts.Config
	if config == nil {
		config = map[string]any{}
	}
	h := &Harness{
		loaded:        m,
		component:     component,
		options:       opts,
		config:        config,
		clock:         &clock{at: epoch.UTC()},
		shapes:        prepared,
		subscriptions: subscriptions(m),
		guard:         &flightGuard{},
		changed:       make(chan struct{}),
	}
	// Under nats there is no board watch and the precondition is inert: every reads pattern is a
	// subscription instead, so the engine watches nothing and a Put triggers no activation (rule H21).
	if m.Communication != manifestCommunicationNATS {
		h.watchedReads = m.Reads
	}
	h.store = newBoard(h.clock)
	h.board = blackboard.New(h.store)
	// Debounce, snapshot tracking, CHANGED, the single-flight guard and coalesce-not-drop are the
	// core's; only the clock and the observation seam are the harness's (rules H2, H5).
	agent, err := blackboard.NewAgent(blackboard.AgentSpec{
		Reads:        h.watchedReads,
		Writes:       m.Writes,
		Precondition: m.Precondition,
		Activate:     h.activate,
	}, blackboard.WithClock(h.clock.now), blackboard.WithOnEvaluated(h.evaluated))
	if err != nil {
		return nil, err
	}
	h.agent = agent
	return h, nil
}

// Seed writes pre-start state, as another component or the platform would have (rules H7, H10).
func (h *Harness) Seed(key string, value map[string]any) error {
	h.mu.Lock()
	if h.stage != stageBuilt {
		h.mu.Unlock()
		return fmt.Errorf("%w: Seed is pre-start only", ErrHarnessState)
	}
	h.mu.Unlock()
	_, err := h.board.Put(context.Background(), key, value)
	return sdkSentinel(err)
}

// Start arms the engine's watches and begins evaluating; ctx bounds the start, not the run (rule H10).
func (h *Harness) Start(ctx context.Context) error {
	h.mu.Lock()
	if h.stage != stageBuilt {
		h.mu.Unlock()
		return fmt.Errorf("%w: a harness is single-use and starts once", ErrHarnessState)
	}
	h.stage = stageStarted
	// The engine runs until Stop, not until the caller's start context expires: a test that starts
	// under a deadline would otherwise lose its engine the moment that deadline passed.
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	h.cancel = cancel
	h.mu.Unlock()
	if err := h.agent.Start(runCtx, h.board, blackboard.StartOptions{}); err != nil {
		cancel()
		h.mu.Lock()
		h.stage, h.cancel = stageBuilt, nil
		h.mu.Unlock()
		return err
	}
	return nil
}

// Stop waits for an in-flight activation and ends the harness; it is idempotent and terminal (rule H10).
func (h *Harness) Stop(ctx context.Context) error {
	h.mu.Lock()
	started, cancel := h.stage == stageStarted, h.cancel
	h.stage, h.cancel = stageStopped, nil
	h.mu.Unlock()
	if !started {
		return nil
	}
	// A queued or running subject activation is in-flight work exactly as a board one is, and it may
	// be holding the guard the engine's own activation is waiting on, so it is drained first (rules
	// H10, H21).
	if err := h.await(ctx, func() bool { return h.subjectPending == 0 }); err != nil {
		return err
	}
	// Agent.Stop returns once the evaluation loop — and any activation running inside it — is done.
	err := h.agent.Stop(ctx)
	cancel()
	// The watch pumps end with their watchers; waiting for them is what makes "no goroutine outlives
	// Stop" a fact rather than a hope (rule H16).
	h.store.wait()
	return err
}

// Get reads one key straight off the in-memory board, nil when it is absent.
func (h *Harness) Get(key string) (*blackboard.Entry, error) {
	return h.board.Get(context.Background(), key)
}

// readsSnapshot is the manifest-reads snapshot a subject activation carries: the current value of
// every watched key, empty when the component declares no reads — and empty under nats, where no
// board watch exists at all (rules H21, K21).
func (h *Harness) readsSnapshot() map[string]map[string]any {
	snapshot := map[string]map[string]any{}
	for key := range h.store.revisions(h.watchedReads) {
		entry, err := h.board.Get(context.Background(), key)
		if err != nil || entry == nil {
			continue
		}
		snapshot[key] = entry.Value
	}
	return snapshot
}

// Put writes any key as an external actor would, under no write grant at all (rules H7, H10).
func (h *Harness) Put(ctx context.Context, key string, value map[string]any) (uint64, error) {
	if err := h.started("Put"); err != nil {
		return 0, err
	}
	revision, err := h.board.Put(ctx, key, value)
	return revision, sdkSentinel(err)
}

// PutCAS writes any key while it stands at revision, as an external actor would (rules H7, H10).
func (h *Harness) PutCAS(ctx context.Context, key string, value map[string]any, revision uint64) (uint64, error) {
	if err := h.started("PutCAS"); err != nil {
		return 0, err
	}
	written, err := h.board.PutCAS(ctx, key, value, revision)
	return written, sdkSentinel(err)
}

// Delete removes any key as an external actor would (rules H7, H10).
func (h *Harness) Delete(_ context.Context, key string) error {
	if err := h.started("Delete"); err != nil {
		return err
	}
	h.store.deleteAt(key)
	return nil
}

// DeleteCAS removes any key while it stands at revision, as an external actor would (rules H7, H10).
func (h *Harness) DeleteCAS(_ context.Context, key string, revision uint64) error {
	if err := h.started("DeleteCAS"); err != nil {
		return err
	}
	_, err := h.store.deleteCAS(key, revision)
	return sdkSentinel(err)
}

// EmitEvent and Options.OnEvent are retired with the event-handler path (rule H13, Clifford's D2
// ruling, 2026-09-01; test_harness.md rule 30 likewise): a subscription's message is driven by rule
// H21's Publish into the one component function there is.

// CallRPC invokes one bridged endpoint and returns its reply; {} stands in for a nil one (rule H13).
func (h *Harness) CallRPC(ctx context.Context, endpoint string, payload []byte) (map[string]any, error) {
	if err := h.started("CallRPC"); err != nil {
		return nil, err
	}
	handler, known := h.options.RPCHandlers[endpoint]
	if !known {
		return nil, fmt.Errorf("%w: no handler is registered for the %q endpoint", ErrHarnessState, endpoint)
	}
	reply, err := handler(ctx, bbsdk.HarnessRPCRequest(h.delivery("rpc"), endpoint, payload, h.config))
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return map[string]any{}, nil
	}
	return reply, nil
}

// AdvanceTime shifts the virtual clock and runs exactly one re-evaluation on it (rule H5).
func (h *Harness) AdvanceTime(ctx context.Context, d time.Duration) error {
	if err := h.started("AdvanceTime"); err != nil {
		return err
	}
	h.clock.advance(d)
	h.mu.Lock()
	target := h.evaluations + 1
	h.mu.Unlock()
	if !h.store.tick() {
		// No watch is armed, so there is no evaluation to schedule: a manifest with no reads never
		// evaluates in production either.
		return nil
	}
	return h.await(ctx, func() bool { return h.evaluations >= target })
}

// AwaitActivation returns the oldest completed activation not returned before (rule H11).
func (h *Harness) AwaitActivation(ctx context.Context) (Record, error) {
	if err := h.started("AwaitActivation"); err != nil {
		return Record{}, err
	}
	var record Record
	// The take is inside the predicate because await evaluates it under the lock: consuming there is
	// what keeps two waiters from claiming one record.
	err := h.await(ctx, func() bool {
		if h.cursor >= len(h.records) {
			return false
		}
		record = h.records[h.cursor]
		h.cursor++
		return true
	})
	if err != nil {
		return Record{}, err
	}
	return record, nil
}

// Settle returns once no debounce window is open, no activation is in flight, and no coalesced
// re-evaluation is pending — the point at which a negative assertion is meaningful (rule H11).
func (h *Harness) Settle(ctx context.Context) error {
	if err := h.started("Settle"); err != nil {
		return err
	}
	return h.await(ctx, h.quiet)
}

// Activations returns every completed activation, in completion order (rule H11).
func (h *Harness) Activations() []Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.records)
}

// quiet reports engine quiescence: nothing in flight, and every board change already evaluated. The
// engine's own snapshot revisions are what prove delivery — a write not yet delivered, a debounce
// window still open and a coalesced re-evaluation all leave the two views unequal (rule H11).
func (h *Harness) quiet() bool {
	return !h.inFlight && h.subjectPending == 0 &&
		maps.Equal(h.delivered, h.store.revisions(h.watchedReads))
}

// await parks until done — always evaluated under the harness lock — reports true, returning
// ctx.Err() when the context expires with the condition still unmet (rule H11).
func (h *Harness) await(ctx context.Context, done func() bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for !done() {
		if err := ctx.Err(); err != nil {
			return err
		}
		changed := h.changed
		h.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		h.mu.Lock()
	}
	return nil
}

// wake releases every waiter; the caller holds the lock.
func (h *Harness) wake() {
	close(h.changed)
	h.changed = make(chan struct{})
}

// started reports the lifecycle guard every driving call shares (rule H10).
func (h *Harness) started(call string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stage != stageStarted {
		return fmt.Errorf("%w: %s needs a started harness", ErrHarnessState, call)
	}
	return nil
}

// delivery builds the data plane an event or an RPC call reaches: no record, no correlation, and no
// read scoping, because neither delivery is an activation (rule H13).
func (h *Harness) delivery(kind string) *plane {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deliveries++
	return &plane{harness: h, holder: fmt.Sprintf("%s-%d", kind, h.deliveries)}
}
