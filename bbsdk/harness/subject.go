// Contract: contracts/bb_sdk_go.md — rule H21, the subject-activation leg (sidecar.md rules
// B36 to B40; the Go leg of test_harness.md rule 43).
//
// A subject activation reaches the very same component function a board activation reaches, through
// the same guard, and is recorded as an ordinary Record: only Source tells the two apart. Nothing
// here opens a socket or touches a broker (rule H16) — the durable arm exists on the record's Source
// alone, and redelivery is never simulated.

package harness

import (
	"context"
	"fmt"
	"slices"
	"sync"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
	"github.com/Clifford-Foster/slate-sdk-go/manifest"
)

// sourceKindSubject is the source kind every message-driven activation carries (rule K21).
const sourceKindSubject = "subject"

// manifestCommunicationNATS is the manifest communication mode that re-maps every declaration onto
// the wire; the default, blackboard, leaves them on the board (manifest.md rule 37). The name is
// qualified because this package also carries the chain drivers' own communication vocabulary.
const manifestCommunicationNATS = "nats"

// Publish is one returned-map entry the harness routed to a declared publishes subject (rule H21).
type Publish struct {
	// Subject is the declared publishes subject the name was routed to.
	Subject string
	// Payload is the value the component returned under that name, verbatim.
	Payload map[string]any
}

// Published is one message the component published, with the activation that produced it (rule H21).
type Published struct {
	// Subject is the declared publishes subject the message went out on.
	Subject string
	// Payload is the published value, verbatim.
	Payload map[string]any
	// ActivationID is the activation whose return map or data-plane put produced it.
	ActivationID string
}

// Published returns every message the component published, in order (rule H21). A published message
// lands here and nowhere else: the harness never feeds it back to the component's own matching
// subscription, because fan-out is the broker's and this models the component's own edge only.
func (h *Harness) Published() []Published {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.published)
}

// Publish drives one subject activation on a declared subscription (rule H21). It matches subject
// against the effective subscription set with watch-pattern semantics, declines a non-conforming
// payload at a strict subscription's gate, and otherwise hands the message to the component function
// through the same guard the board path uses. It returns once the activation is queued in arrival
// order; Settle and AwaitActivation are how a test waits for it, exactly as after a Put.
func (h *Harness) Publish(_ context.Context, subject string, payload map[string]any) error {
	if err := h.started("Publish"); err != nil {
		return err
	}
	entry, matched := h.subscriptionFor(subject)
	if !matched {
		// The sidecar would never have subscribed it, so neither does the harness.
		return fmt.Errorf("%w: no subscribes entry matches the subject %q", ErrHarnessState, subject)
	}
	// A strict subscription judges the payload before the component function is called: a
	// non-conforming payload is not delivered, produces no Record, and lands one ValidationSkips
	// entry — never a silent non-delivery (rule H21, mirroring rule H20's read gate).
	if verdicts := h.shapes.subscribeVerdicts(entry, subject, payload); len(verdicts) > 0 {
		h.declineSubject(ValidationSkip{
			Snapshot: h.readsSnapshot(), ChangedKeys: []string{}, Verdicts: verdicts,
		})
		return nil
	}
	source := bbsdk.ActivationSource{
		Kind: sourceKindSubject, Subject: entry.Subject, Durable: entry.Durable,
	}
	// The ticket is taken here, on the caller's goroutine, so two Publish calls queue in the order
	// they were made — the arrival order rule H21 promises under single_flight (rule B39).
	ticket := h.guard.enter(h.singleFlight())
	h.mu.Lock()
	h.subjectPending++
	h.mu.Unlock()
	go h.runSubject(ticket, source, payload)
	return nil
}

// runSubject waits for this message's turn at the guard, then delivers it as an ordinary activation.
func (h *Harness) runSubject(ticket *flightTicket, source bbsdk.ActivationSource, payload map[string]any) {
	defer func() {
		h.mu.Lock()
		h.subjectPending--
		h.wake()
		h.mu.Unlock()
	}()
	ticket.wait()
	defer ticket.release()
	// Under nats there is no board watch, so the snapshot is empty; under blackboard it is the
	// current reads snapshot, exactly as on any activation (rules H21, K21).
	h.runActivation(context.Background(), source, payload, h.readsSnapshot(), []string{}, nil)
}

// subscriptionFor resolves one subject against the effective subscription set, first match winning
// so an explicit subscribes entry refines a reads pattern it names rather than duplicating it.
func (h *Harness) subscriptionFor(subject string) (manifest.Subscription, bool) {
	for _, entry := range h.subscriptions {
		if matchesPattern(subject, entry.Subject) {
			return entry, true
		}
	}
	return manifest.Subscription{}, false
}

// subscriptions is the effective subscription set: the explicit subscribes entries under
// blackboard, and those plus every reads pattern under nats, where every read IS a subscription
// with the defaults and an explicit entry naming the same subject refines it (rules H1, H21;
// manifest.md rule 37).
func subscriptions(m *manifest.Manifest) []manifest.Subscription {
	// The explicit entries come first, so a reads pattern an entry names byte-identically is refined
	// by that entry rather than matched as a second, defaulted subscription.
	effective := slices.Clone(m.Subscribes)
	if m.Communication != manifestCommunicationNATS {
		return effective
	}
	for _, pattern := range m.Reads {
		if slices.ContainsFunc(effective, func(e manifest.Subscription) bool { return e.Subject == pattern }) {
			continue
		}
		effective = append(effective, manifest.Subscription{Subject: pattern, Mode: "broadcast"})
	}
	return effective
}

// singleFlight reports whether this unit runs at most one activation at a time across both sources;
// the manifest's default is true (manifest.md rule 35, sidecar.md rule B39).
func (h *Harness) singleFlight() bool { return h.loaded.SingleFlight }

// publishing reports whether the effective communication mode routes every declared name to a
// subject: under nats a writes pattern is a publication scope and nothing is ever put on the board
// (manifest.md rule 37, sidecar.md rules A8, B37).
func (h *Harness) publishing() bool { return h.loaded.Communication == manifestCommunicationNATS }

// publishable reports whether a returned-map or data-plane name is one this component may publish:
// a declared publishes subject under either mode, and — under nats, where writes are publication
// scopes — a name matching a writes pattern too (rule H21).
func (h *Harness) publishable(name string) bool {
	for _, entry := range h.loaded.Publishes {
		if entry.Subject == name {
			return true
		}
	}
	return h.publishing() && writeAuthorized(name, h.loaded.Writes)
}

// boardWritable reports whether a name is a board write: only under blackboard, and only against a
// declared writes pattern. Under nats nothing is ever put on the board (rule H21).
func (h *Harness) boardWritable(name string) bool {
	return !h.publishing() && writeAuthorized(name, h.loaded.Writes)
}

// declineSubject files a strict subscription's refusal: no Record, nothing delivered, and one entry
// on ValidationSkips — the harness's "why didn't it fire" surface (rule H21).
func (h *Harness) declineSubject(skip ValidationSkip) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.skips = append(h.skips, skip)
	h.wake()
}

// recordPublished appends one published message to the harness's log (rule H21).
func (h *Harness) recordPublished(message Published) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.published = append(h.published, message)
	h.wake()
}

// flightGuard is the single-flight guard across both activation sources: under a declared
// single_flight it admits one activation at a time, in the order tickets were taken, and holds it to
// completion. Under single_flight: false a subject activation takes no guard at all and the board
// source keeps the core engine's own guard among board evaluations (rule B39, rule H21).
type flightGuard struct {
	mu      sync.Mutex
	held    bool
	waiting []chan struct{}
}

// flightTicket is one place in the guard's queue; a ticket taken with the guard disabled is free.
type flightTicket struct {
	guard *flightGuard
	turn  chan struct{}
}

// enter takes a place in the queue in arrival order; the returned ticket is waited on and released
// by whichever goroutine runs the activation. A disabled guard hands back a ticket that never waits.
func (g *flightGuard) enter(enabled bool) *flightTicket {
	if !enabled {
		return &flightTicket{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.held {
		g.held = true
		return &flightTicket{guard: g}
	}
	turn := make(chan struct{})
	g.waiting = append(g.waiting, turn)
	return &flightTicket{guard: g, turn: turn}
}

// wait blocks until this ticket's turn comes; a ticket that already holds the guard returns at once.
func (t *flightTicket) wait() {
	if t.turn != nil {
		<-t.turn
	}
}

// release hands the guard to the next waiter in arrival order, or frees it when none is queued.
func (t *flightTicket) release() {
	if t.guard == nil {
		return
	}
	t.guard.mu.Lock()
	defer t.guard.mu.Unlock()
	if len(t.guard.waiting) > 0 {
		next := t.guard.waiting[0]
		t.guard.waiting = t.guard.waiting[1:]
		close(next)
		return
	}
	t.guard.held = false
}
