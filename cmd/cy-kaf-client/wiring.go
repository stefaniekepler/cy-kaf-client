package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/app/mcpsettings"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/config"
	infraconnect "github.com/cy-kaf/cy-kaf-client/internal/infra/connect"
	infrafilter "github.com/cy-kaf/cy-kaf-client/internal/infra/filter"
	infrakafka "github.com/cy-kaf/cy-kaf-client/internal/infra/kafka"
	infraksql "github.com/cy-kaf/cy-kaf-client/internal/infra/ksql"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/mcpclient"
	schemaregistry "github.com/cy-kaf/cy-kaf-client/internal/infra/schemaregistry"
	infraserde "github.com/cy-kaf/cy-kaf-client/internal/infra/serde"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/cy-kaf/cy-kaf-client/internal/mcpserver"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
	"github.com/cy-kaf/cy-kaf-client/webui"
)

const clusterStateRefreshInterval = 30 * time.Second

type wiredApplication struct {
	Handler      http.Handler
	ClusterCount int
	Cleanup      func()
}

type wiredServices struct {
	States         *appcluster.StateCache
	Brokers        *appcluster.BrokerService
	Topics         *appcluster.TopicService
	Groups         *appcluster.GroupService
	Serdes         *appcluster.SerdeService
	Schemas        *appcluster.SchemaService
	Connects       *appcluster.ConnectService
	Acls           *appcluster.AclService
	Quotas         *appcluster.QuotaService
	KSQL           *appcluster.KsqlService
	KSQLClassifier appcluster.KsqlClassifier
	Messages       *appcluster.MessageService
	Analysis       *appcluster.AnalysisService
	SmartFilters   *appcluster.SmartFilterService
	Config         *appcluster.ConfigService
	Reloader       *appcluster.Reloader
	Resolver       *appcluster.Resolver
	DesktopMCP     api.DesktopMCPServicer
	ClusterCount   int
	Cleanup        func()
}

type runtimeCloser interface {
	Close()
}

func newRuntimeCleanup(closers ...runtimeCloser) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			for _, closer := range closers {
				if closer != nil {
					closer.Close()
				}
			}
		})
	}
}

func wireServices(ctx context.Context, cfg *config.App, configPath string) (*wiredServices, error) {
	defs := make([]cluster.Definition, 0, len(cfg.Kafka.Clusters))
	for _, current := range cfg.Kafka.Clusters {
		def, err := current.ToDomain()
		if err != nil {
			return nil, fmt.Errorf("convert cluster config: %w", err)
		}
		defs = append(defs, def)
	}

	pool := infrakafka.NewPool()
	resolver := appcluster.NewResolver(defs)
	states := appcluster.NewStateCache(resolver, pool, pool, clusterStateRefreshInterval)
	states.Start(ctx)
	brokers := appcluster.NewBrokerService(resolver, pool)
	topics := appcluster.NewTopicService(resolver, states, pool)
	groups := appcluster.NewGroupService(resolver, pool)
	serdeRegistry := infraserde.NewRegistry()
	schemaRegistryPool := schemaregistry.NewPool()
	serdeProvider := infraserde.NewProviderWithSchemaRegistry(serdeRegistry, schemaRegistryPool)
	serdes := appcluster.NewSerdeService(resolver, serdeProvider)
	schemas := appcluster.NewSchemaService(resolver, schemaRegistryPool)
	connectPool := infraconnect.NewPool()
	connects := appcluster.NewConnectService(resolver, connectPool)
	acls := appcluster.NewAclService(resolver, pool)
	quotas := appcluster.NewQuotaService(resolver, pool)
	classifier := infraksql.Classifier{}
	ksqlPool := infraksql.NewPool()
	ksqls := appcluster.NewKsqlService(resolver, ksqlPool, classifier)
	filterEngine := infrafilter.NewEngine()
	cursors := appcluster.NewCursorCache(10*time.Minute, 1000)
	messages := appcluster.NewMessageService(
		resolver,
		pool,
		pool,
		serdeProvider,
		filterEngine,
		cursors,
		states,
	)
	analyses := appcluster.NewAnalysisService(ctx, resolver, pool)
	smartFilters := appcluster.NewSmartFilterService(filterEngine)
	configStore := config.NewStore(configPath, infrakafka.ProbeConnectivity)
	configService := appcluster.NewConfigService(configStore)
	reloader := appcluster.NewReloader(resolver, pool, states, configStore)

	return &wiredServices{
		States:         states,
		Brokers:        brokers,
		Topics:         topics,
		Groups:         groups,
		Serdes:         serdes,
		Schemas:        schemas,
		Connects:       connects,
		Acls:           acls,
		Quotas:         quotas,
		KSQL:           ksqls,
		KSQLClassifier: classifier,
		Messages:       messages,
		Analysis:       analyses,
		SmartFilters:   smartFilters,
		Config:         configService,
		Reloader:       reloader,
		Resolver:       resolver,
		ClusterCount:   len(defs),
		Cleanup:        newRuntimeCleanup(pool, schemaRegistryPool, connectPool, ksqlPool),
	}, nil
}

func (s *wiredServices) apiDeps(desktop *api.DesktopOptions) api.Deps {
	return api.Deps{
		States:       s.States,
		LogDirs:      s.Brokers,
		Brokers:      s.Brokers,
		Topics:       s.Topics,
		Groups:       s.Groups,
		Serdes:       s.Serdes,
		Schemas:      s.Schemas,
		Connects:     s.Connects,
		Acls:         s.Acls,
		Quotas:       s.Quotas,
		Ksql:         s.KSQL,
		Messages:     s.Messages,
		Analysis:     s.Analysis,
		SmartFilters: s.SmartFilters,
		Config:       s.Config,
		Reloader:     s.Reloader,
		IsReadOnly:   s.Resolver.IsReadOnly,
		Build:        version.Info(),
		Static:       webui.FS(),
		Desktop:      desktop,
		DesktopMCP:   s.DesktopMCP,
	}
}

func (s *wiredServices) mcpDeps(policy *mcppolicy.Store) mcpserver.Dependencies {
	return mcpserver.Dependencies{
		States:         s.States,
		Brokers:        s.Brokers,
		Topics:         s.Topics,
		Groups:         s.Groups,
		Serdes:         s.Serdes,
		Schemas:        s.Schemas,
		Connects:       s.Connects,
		Acls:           s.Acls,
		Quotas:         s.Quotas,
		KSQL:           s.KSQL,
		KSQLClassifier: s.KSQLClassifier,
		Messages:       s.Messages,
		Analysis:       s.Analysis,
		SmartFilters:   s.SmartFilters,
		IsReadOnly:     s.Resolver.IsReadOnly,
		Policy:         policy,
	}
}

func wireMCPRuntime(
	ctx context.Context,
	cfg *config.App,
	configPath string,
	policy *mcppolicy.Store,
) (mcpRuntime, error) {
	services, err := wireServices(ctx, cfg, configPath)
	if err != nil {
		return mcpRuntime{}, err
	}
	return mcpRuntime{
		Dependencies: services.mcpDeps(policy),
		Cleanup:      services.Cleanup,
	}, nil
}

func wireApplication(
	ctx context.Context,
	cfg *config.App,
	configPath string,
	configExplicit bool,
	desktop *api.DesktopOptions,
) (*wiredApplication, error) {
	var desktopMCP api.DesktopMCPServicer
	if desktop != nil {
		executable, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolve current executable: %w", err)
		}
		desiredConfigPath := configPath
		if configExplicit {
			desiredConfigPath, err = filepath.Abs(filepath.Clean(configPath))
			if err != nil {
				return nil, errors.New("build MCP client configuration")
			}
		}
		desired, err := mcpclient.NewDesiredServer(
			executable,
			desiredConfigPath,
			configExplicit,
		)
		if err != nil {
			return nil, fmt.Errorf("build MCP client configuration: %w", err)
		}
		policyConfigPath := configPath
		if configExplicit {
			if len(desired.Args) != 3 || desired.Args[1] != "--config" {
				return nil, errors.New("build MCP client configuration")
			}
			policyConfigPath = desired.Args[2]
		}
		configurator, err := productionConfiguratorFactory()
		if err != nil {
			return nil, errors.New("initialize MCP client integration")
		}
		policy := mcppolicy.NewStore(mcppolicy.PathForConfig(policyConfigPath))
		desktopMCP = mcpsettings.New(
			desktopPolicyStoreAdapter{store: policy},
			desktopConfiguratorAdapter{configurator: configurator},
			toDesktopDesiredServer(desired),
		)
	}

	services, err := wireServices(ctx, cfg, configPath)
	if err != nil {
		return nil, err
	}
	services.DesktopMCP = desktopMCP
	return &wiredApplication{
		Handler:      api.NewServer(services.apiDeps(desktop)),
		ClusterCount: services.ClusterCount,
		Cleanup:      services.Cleanup,
	}, nil
}

type desktopPolicyStoreAdapter struct {
	store *mcppolicy.Store
}

func (adapter desktopPolicyStoreAdapter) Load() (mcpsettings.Policy, error) {
	policy, err := adapter.store.Load()
	if err != nil {
		return mcpsettings.Policy{}, err
	}
	return mcpsettings.Policy{
		Enabled: policy.Enabled, AllowWrites: policy.AllowWrites,
	}, nil
}

func (adapter desktopPolicyStoreAdapter) Save(
	ctx context.Context,
	policy mcpsettings.Policy,
) error {
	return adapter.store.Save(ctx, mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     policy.Enabled,
		AllowWrites: policy.AllowWrites,
	})
}

type desktopConfiguratorAdapter struct {
	configurator mcpclient.Configurator
}

func (adapter desktopConfiguratorAdapter) Status(
	ctx context.Context,
	client mcpsettings.Client,
	desired mcpsettings.DesiredServer,
) (mcpsettings.Integration, error) {
	infraClient, ok := toInfraMCPClient(client)
	if !ok {
		return mcpsettings.Integration{}, mcpsettings.ErrIntegration
	}
	integration, err := adapter.configurator.Status(
		ctx, infraClient, toInfraDesiredServer(desired),
	)
	if err != nil {
		return mcpsettings.Integration{}, translateMCPClientError(err)
	}
	return toDesktopIntegration(integration)
}

func (adapter desktopConfiguratorAdapter) Configure(
	ctx context.Context,
	client mcpsettings.Client,
	desired mcpsettings.DesiredServer,
	replace bool,
) (mcpsettings.Integration, error) {
	infraClient, ok := toInfraMCPClient(client)
	if !ok {
		return mcpsettings.Integration{}, mcpsettings.ErrIntegration
	}
	integration, err := adapter.configurator.Configure(
		ctx, infraClient, toInfraDesiredServer(desired), replace,
	)
	if err != nil {
		return mcpsettings.Integration{}, translateMCPClientError(err)
	}
	return toDesktopIntegration(integration)
}

func toInfraMCPClient(client mcpsettings.Client) (mcpclient.Client, bool) {
	switch client {
	case mcpsettings.ClientCodex:
		return mcpclient.ClientCodex, true
	case mcpsettings.ClientClaudeCode:
		return mcpclient.ClientClaudeCode, true
	default:
		return "", false
	}
}

func toDesktopDesiredServer(desired mcpclient.DesiredServer) mcpsettings.DesiredServer {
	return mcpsettings.DesiredServer{
		Name: desired.Name, Command: desired.Command, Args: append([]string(nil), desired.Args...),
	}
}

func toInfraDesiredServer(desired mcpsettings.DesiredServer) mcpclient.DesiredServer {
	return mcpclient.DesiredServer{
		Name: desired.Name, Command: desired.Command, Args: append([]string(nil), desired.Args...),
	}
}

func toDesktopIntegration(
	integration mcpclient.Integration,
) (mcpsettings.Integration, error) {
	client, ok := toDesktopMCPClient(integration.Client)
	if !ok {
		return mcpsettings.Integration{}, mcpsettings.ErrIntegration
	}
	status, ok := toDesktopMCPStatus(integration.Status)
	if !ok {
		return mcpsettings.Integration{}, mcpsettings.ErrIntegration
	}
	return mcpsettings.Integration{
		Client:        client,
		Status:        status,
		CanConfigure:  integration.CanConfigure,
		ManualCommand: integration.ManualCommand,
		Message:       integration.Message,
	}, nil
}

func toDesktopMCPClient(client mcpclient.Client) (mcpsettings.Client, bool) {
	switch client {
	case mcpclient.ClientCodex:
		return mcpsettings.ClientCodex, true
	case mcpclient.ClientClaudeCode:
		return mcpsettings.ClientClaudeCode, true
	default:
		return "", false
	}
}

func toDesktopMCPStatus(status mcpclient.Status) (mcpsettings.Status, bool) {
	switch status {
	case mcpclient.StatusClientNotFound:
		return mcpsettings.StatusClientNotFound, true
	case mcpclient.StatusNotConfigured:
		return mcpsettings.StatusNotConfigured, true
	case mcpclient.StatusConfigured:
		return mcpsettings.StatusConfigured, true
	case mcpclient.StatusConflict:
		return mcpsettings.StatusConflict, true
	case mcpclient.StatusRepairRequired:
		return mcpsettings.StatusRepairRequired, true
	case mcpclient.StatusError:
		return mcpsettings.StatusError, true
	default:
		return "", false
	}
}

func translateMCPClientError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, mcpclient.ErrClientNotFound):
		return mcpsettings.ErrClientNotFound
	case errors.Is(err, mcpclient.ErrConcurrentModification):
		return mcpsettings.ErrConcurrentModification
	case errors.Is(err, mcpclient.ErrRollbackFailed):
		return mcpsettings.ErrRollbackFailed
	case errors.Is(err, mcpclient.ErrVerification):
		return mcpsettings.ErrVerification
	case errors.Is(err, mcpclient.ErrClientCommand):
		return mcpsettings.ErrClientCommand
	default:
		return mcpsettings.ErrIntegration
	}
}

var productionConfiguratorFactory = productionConfigurator
var productionUserHomeDir = os.UserHomeDir

func productionConfigurator() (mcpclient.Configurator, error) {
	userHome, err := productionUserHomeDir()
	if err != nil || userHome == "" {
		return nil, errors.New("resolve user home")
	}
	userHome, err = filepath.Abs(filepath.Clean(userHome))
	if err != nil {
		return nil, errors.New("resolve user home")
	}
	userHome, err = filepath.EvalSymlinks(userHome)
	if err != nil {
		return nil, errors.New("resolve user home")
	}
	userHomeInfo, err := os.Stat(userHome)
	if err != nil || !userHomeInfo.IsDir() {
		return nil, errors.New("resolve user home")
	}
	userHome = filepath.Clean(userHome)

	environment := mcpclient.CaptureEnvironmentSnapshot()
	runner := mcpclient.NewOSRunnerWithSnapshot(5*time.Second, 32<<10, environment)
	configPaths := mcpclient.NewConfigPathResolverWithSnapshot(environment, userHome)
	inspector := mcpclient.NewCodexInspector(runner)
	return mcpclient.NewConfigurator(
		mcpclient.NewLocatorWithSnapshot(environment, userHome),
		runner,
		inspector,
		configPaths,
		mcpclient.NewClaudeEntryReader(8<<20),
	), nil
}
