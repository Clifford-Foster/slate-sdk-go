package blackboard

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go/micro"
)

// DefaultServiceVersion is the semantic version a micro service without one registers under (§3.3).
const DefaultServiceVersion = "1.0.0"

// MicroSpec declares the NATS Micro service a MicroAgent registers alongside its watches (§3.3).
type MicroSpec struct {
	// Name is the NATS Micro service name; without it no service is registered.
	Name string
	// Version is the service's semantic version; empty means 1.0.0.
	Version string
	// Description is the service's human-readable description.
	Description string
	// RegisterEndpoints adds the service's endpoints before the blackboard watches begin (§3.4 rule 6).
	RegisterEndpoints func(service micro.Service) error
}

// MicroAgent is an Agent that also registers as a discoverable NATS Micro service (§3.3).
type MicroAgent struct {
	*Agent
	spec MicroSpec

	mu      sync.Mutex
	service micro.Service
}

// NewMicroAgent returns an agent that registers a NATS Micro service around the activation machinery.
func NewMicroAgent(spec AgentSpec, service MicroSpec, opts ...AgentOption) (*MicroAgent, error) {
	agent, err := NewAgent(spec, opts...)
	if err != nil {
		return nil, err
	}
	return &MicroAgent{Agent: agent, spec: service}, nil
}

// Start registers the NATS Micro service, then begins the blackboard watches (§3.4 rule 6).
func (m *MicroAgent) Start(ctx context.Context, board *Blackboard, opts StartOptions) error {
	if opts.Conn != nil && m.spec.Name != "" {
		if err := m.startService(opts); err != nil {
			return err
		}
	}
	if err := m.Agent.Start(ctx, board, opts); err != nil {
		return errors.Join(err, m.stopService())
	}
	return nil
}

// Stop stops the blackboard watches, then deregisters the NATS Micro service (§3.4 rule 7).
func (m *MicroAgent) Stop(ctx context.Context) error {
	return errors.Join(m.Agent.Stop(ctx), m.stopService())
}

// startService registers the micro service and its endpoints before any watch is established.
func (m *MicroAgent) startService(opts StartOptions) error {
	version := m.spec.Version
	if version == "" {
		version = DefaultServiceVersion
	}
	service, err := micro.AddService(opts.Conn, micro.Config{
		Name:        m.spec.Name,
		Version:     version,
		Description: m.spec.Description,
	})
	if err != nil {
		return fmt.Errorf("blackboard: registering micro service %q: %w", m.spec.Name, err)
	}
	m.mu.Lock()
	m.service = service
	m.mu.Unlock()
	if m.spec.RegisterEndpoints != nil {
		if err := m.spec.RegisterEndpoints(service); err != nil {
			return errors.Join(fmt.Errorf("blackboard: registering endpoints of %q: %w", m.spec.Name, err), m.stopService())
		}
	}
	return nil
}

// stopService deregisters the micro service, leaving the agent restartable.
func (m *MicroAgent) stopService() error {
	m.mu.Lock()
	service := m.service
	m.service = nil
	m.mu.Unlock()
	if service == nil {
		return nil
	}
	if err := service.Stop(); err != nil {
		return fmt.Errorf("blackboard: stopping micro service %q: %w", m.spec.Name, err)
	}
	return nil
}
