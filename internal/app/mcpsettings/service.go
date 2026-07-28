// Package mcpsettings coordinates the local MCP policy and supported client
// integrations without exposing executable or configuration-path choices.
package mcpsettings

import (
	"context"
	"errors"
	"sync"
)

type Client string

const (
	ClientCodex      Client = "codex"
	ClientClaudeCode Client = "claude-code"
	ServerName              = "cy-kaf-client"
)

type Status string

const (
	StatusClientNotFound Status = "client_not_found"
	StatusNotConfigured  Status = "not_configured"
	StatusConfigured     Status = "configured"
	StatusConflict       Status = "configuration_conflict"
	StatusRepairRequired Status = "repair_required"
	StatusError          Status = "error"
)

var (
	ErrMCPDisabled               = errors.New("MCP is disabled")
	ErrWriteConfirmationRequired = errors.New("confirm write access before enabling it")
	ErrClientNotFound            = errors.New("MCP client not found")
	ErrConcurrentModification    = errors.New("MCP client configuration changed")
	ErrRollbackFailed            = errors.New("MCP client configuration rollback failed")
	ErrVerification              = errors.New("MCP client configuration verification failed")
	ErrClientCommand             = errors.New("MCP client command failed")
	ErrIntegration               = errors.New("MCP client integration failed")
)

type Policy struct {
	Enabled     bool
	AllowWrites bool
}

type UpdateRequest struct {
	Enabled       bool
	AllowWrites   bool
	ConfirmWrites bool
}

type Integration struct {
	Client        Client
	Status        Status
	CanConfigure  bool
	ManualCommand string
	Message       string
}

type DesiredServer struct {
	Name    string
	Command string
	Args    []string
}

type PolicyStore interface {
	Load() (Policy, error)
	Save(context.Context, Policy) error
}

type Configurator interface {
	Status(context.Context, Client, DesiredServer) (Integration, error)
	Configure(context.Context, Client, DesiredServer, bool) (Integration, error)
}

type Service struct {
	policy       PolicyStore
	configurator Configurator
	desired      DesiredServer
	transitionMu sync.Mutex
}

func New(
	policy PolicyStore,
	configurator Configurator,
	desired DesiredServer,
) *Service {
	return newService(policy, configurator, desired)
}

func newService(
	policy PolicyStore,
	configurator Configurator,
	desired DesiredServer,
) *Service {
	return &Service{policy: policy, configurator: configurator, desired: desired}
}

func (s *Service) Get(context.Context) (Policy, error) {
	return s.policy.Load()
}

func (s *Service) Update(
	ctx context.Context,
	request UpdateRequest,
) (Policy, error) {
	s.transitionMu.Lock()
	defer s.transitionMu.Unlock()

	current, err := s.policy.Load()
	if err != nil {
		return Policy{}, err
	}

	next := Policy{
		Enabled:     request.Enabled,
		AllowWrites: request.Enabled && request.AllowWrites,
	}
	if !current.AllowWrites && next.AllowWrites && !request.ConfirmWrites {
		return Policy{}, ErrWriteConfirmationRequired
	}
	if err := s.policy.Save(ctx, next); err != nil {
		return Policy{}, err
	}
	return next, nil
}

func (s *Service) Integrations(ctx context.Context) ([]Integration, error) {
	clients := []Client{
		ClientCodex,
		ClientClaudeCode,
	}
	integrations := make([]Integration, 0, len(clients))
	for _, client := range clients {
		integration, err := s.configurator.Status(ctx, client, s.desired)
		if err != nil {
			return nil, err
		}
		integrations = append(integrations, integration)
	}
	return integrations, nil
}

func (s *Service) Configure(
	ctx context.Context,
	client Client,
	replace bool,
) (Integration, error) {
	s.transitionMu.Lock()
	defer s.transitionMu.Unlock()

	policy, err := s.policy.Load()
	if err != nil {
		return Integration{}, err
	}
	if !policy.Enabled {
		return Integration{}, ErrMCPDisabled
	}
	return s.configurator.Configure(ctx, client, s.desired, replace)
}
