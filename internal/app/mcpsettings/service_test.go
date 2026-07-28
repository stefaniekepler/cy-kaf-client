package mcpsettings

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMCPSettingsStateTransitions(t *testing.T) {
	ctx := context.Background()
	store := newControlledPolicyStore(Policy{})
	configurator := &fakeConfigurator{}
	service := New(store, configurator, testDesiredServer())

	got, err := service.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, Policy{}, got)

	got, err = service.Update(ctx, UpdateRequest{Enabled: true})
	require.NoError(t, err)
	require.Equal(t, Policy{Enabled: true, AllowWrites: false}, got)

	beforeRejectedWrite := store.saveCalls
	_, err = service.Update(ctx, UpdateRequest{Enabled: true, AllowWrites: true})
	require.ErrorIs(t, err, ErrWriteConfirmationRequired)
	require.Equal(t, beforeRejectedWrite, store.saveCalls)

	got, err = service.Update(ctx, UpdateRequest{
		Enabled: true, AllowWrites: true, ConfirmWrites: true,
	})
	require.NoError(t, err)
	require.Equal(t, Policy{Enabled: true, AllowWrites: true}, got)

	got, err = service.Update(ctx, UpdateRequest{Enabled: true})
	require.NoError(t, err)
	require.Equal(t, Policy{Enabled: true, AllowWrites: false}, got)

	got, err = service.Update(ctx, UpdateRequest{
		Enabled: false, AllowWrites: true, ConfirmWrites: true,
	})
	require.NoError(t, err)
	require.Equal(t, Policy{}, got)

	_, err = service.Configure(ctx, ClientCodex, false)
	require.ErrorIs(t, err, ErrMCPDisabled)
	require.Zero(t, configurator.configureCalls)
}

func TestMCPSettingsWriteConfirmationIsRequiredOnlyForFalseToTrue(t *testing.T) {
	ctx := context.Background()
	store := newControlledPolicyStore(Policy{})
	service := New(store, &fakeConfigurator{}, testDesiredServer())

	_, err := service.Update(ctx, UpdateRequest{
		Enabled: true, AllowWrites: true, ConfirmWrites: true,
	})
	require.NoError(t, err)

	got, err := service.Update(ctx, UpdateRequest{
		Enabled: true, AllowWrites: true, ConfirmWrites: false,
	})
	require.NoError(t, err)
	require.True(t, got.AllowWrites)
}

func TestMCPSettingsConcurrentDisableCannotCompleteInsideEarlierUpdateTransition(t *testing.T) {
	store := newControlledPolicyStore(Policy{Enabled: true, AllowWrites: true})
	store.blockWritesOnSave = make(chan struct{})
	store.writesOnSaveEntered = make(chan struct{})
	service := newService(store, &fakeConfigurator{}, testDesiredServer())

	reenableDone := make(chan error, 1)
	go func() {
		_, err := service.Update(context.Background(), UpdateRequest{
			Enabled: true, AllowWrites: true,
		})
		reenableDone <- err
	}()
	<-store.writesOnSaveEntered

	if service.transitionMu.TryLock() {
		service.transitionMu.Unlock()
		t.Fatal("Update released the service transition lock before Save completed")
	}

	disableDone := make(chan error, 1)
	go func() {
		_, err := service.Update(context.Background(), UpdateRequest{Enabled: false})
		disableDone <- err
	}()
	close(store.blockWritesOnSave)

	require.NoError(t, <-reenableDone)
	require.NoError(t, <-disableDone)
	got, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, Policy{}, got)
}

func TestMCPSettingsDisableWaitsForAuthorizedConfigureCall(t *testing.T) {
	store := newControlledPolicyStore(Policy{Enabled: true, AllowWrites: false})
	configurator := &blockingConfigurator{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	service := newService(store, configurator, testDesiredServer())

	configureDone := make(chan error, 1)
	go func() {
		_, err := service.Configure(context.Background(), ClientCodex, false)
		configureDone <- err
	}()
	<-configurator.entered

	if service.transitionMu.TryLock() {
		service.transitionMu.Unlock()
		t.Fatal("Configure released the service transition lock before the configurator returned")
	}

	disableDone := make(chan error, 1)
	go func() {
		_, err := service.Update(context.Background(), UpdateRequest{Enabled: false})
		disableDone <- err
	}()
	close(configurator.release)

	require.NoError(t, <-configureDone)
	require.NoError(t, <-disableDone)
	got, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, Policy{}, got)
}

func TestMCPSettingsIntegrationsUseFixedClientsAndDesiredServer(t *testing.T) {
	ctx := context.Background()
	desired := testDesiredServer()
	configurator := &fakeConfigurator{
		statuses: map[Client]Integration{
			ClientCodex: {
				Client: ClientCodex, Status: StatusConfigured,
			},
			ClientClaudeCode: {
				Client: ClientClaudeCode, Status: StatusNotConfigured,
			},
		},
	}
	service := New(
		newControlledPolicyStore(Policy{}),
		configurator,
		desired,
	)

	got, err := service.Integrations(ctx)
	require.NoError(t, err)
	require.Equal(t, []Integration{
		configurator.statuses[ClientCodex],
		configurator.statuses[ClientClaudeCode],
	}, got)
	require.Equal(t, []statusCall{
		{client: ClientCodex, desired: desired},
		{client: ClientClaudeCode, desired: desired},
	}, configurator.statusCalls)
}

func TestMCPSettingsConfigureForwardsEnabledClientRequest(t *testing.T) {
	ctx := context.Background()
	desired := testDesiredServer()
	want := Integration{
		Client: ClientClaudeCode, Status: StatusConfigured,
	}
	configurator := &fakeConfigurator{configureResult: want}
	service := New(
		newControlledPolicyStore(Policy{}),
		configurator,
		desired,
	)
	_, err := service.Update(ctx, UpdateRequest{Enabled: true})
	require.NoError(t, err)

	got, err := service.Configure(ctx, ClientClaudeCode, true)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, 1, configurator.configureCalls)
	require.Equal(t, ClientClaudeCode, configurator.configureClient)
	require.Equal(t, desired, configurator.configureDesired)
	require.True(t, configurator.configureReplace)
}

type statusCall struct {
	client  Client
	desired DesiredServer
}

type fakeConfigurator struct {
	statuses         map[Client]Integration
	statusErr        error
	statusCalls      []statusCall
	configureResult  Integration
	configureErr     error
	configureCalls   int
	configureClient  Client
	configureDesired DesiredServer
	configureReplace bool
}

type blockingConfigurator struct {
	entered chan struct{}
	release chan struct{}
}

func (configurator *blockingConfigurator) Status(
	context.Context,
	Client,
	DesiredServer,
) (Integration, error) {
	return Integration{}, nil
}

func (configurator *blockingConfigurator) Configure(
	_ context.Context,
	client Client,
	_ DesiredServer,
	_ bool,
) (Integration, error) {
	close(configurator.entered)
	<-configurator.release
	return Integration{
		Client: client, Status: StatusConfigured,
	}, nil
}

type controlledPolicyStore struct {
	mu                  sync.Mutex
	policy              Policy
	blockWritesOnSave   chan struct{}
	writesOnSaveEntered chan struct{}
	writesOnSaveOnce    sync.Once
	saveCalls           int
}

func newControlledPolicyStore(policy Policy) *controlledPolicyStore {
	return &controlledPolicyStore{policy: policy}
}

func (store *controlledPolicyStore) Load() (Policy, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.policy, nil
}

func (store *controlledPolicyStore) Save(
	ctx context.Context,
	policy Policy,
) error {
	if policy.AllowWrites && store.blockWritesOnSave != nil {
		store.writesOnSaveOnce.Do(func() { close(store.writesOnSaveEntered) })
		select {
		case <-store.blockWritesOnSave:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saveCalls++
	store.policy = policy
	return nil
}

func (fake *fakeConfigurator) Status(
	_ context.Context,
	client Client,
	desired DesiredServer,
) (Integration, error) {
	fake.statusCalls = append(fake.statusCalls, statusCall{client: client, desired: desired})
	if fake.statusErr != nil {
		return Integration{}, fake.statusErr
	}
	return fake.statuses[client], nil
}

func (fake *fakeConfigurator) Configure(
	_ context.Context,
	client Client,
	desired DesiredServer,
	replace bool,
) (Integration, error) {
	fake.configureCalls++
	fake.configureClient = client
	fake.configureDesired = desired
	fake.configureReplace = replace
	return fake.configureResult, fake.configureErr
}

func testDesiredServer() DesiredServer {
	return DesiredServer{
		Name: ServerName, Command: "/test-app",
		Args: []string{"mcp"},
	}
}

func TestMCPSettingsIntegrationErrorsStopRemainingStatusQueries(t *testing.T) {
	cause := errors.New("status failed")
	configurator := &fakeConfigurator{statusErr: cause}
	service := New(
		newControlledPolicyStore(Policy{}),
		configurator,
		testDesiredServer(),
	)

	_, err := service.Integrations(context.Background())
	require.ErrorIs(t, err, cause)
	require.Len(t, configurator.statusCalls, 1)
}
