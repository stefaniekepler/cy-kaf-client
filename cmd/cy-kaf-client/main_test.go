package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/config"
	infraksql "github.com/cy-kaf/cy-kaf-client/internal/infra/ksql"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/cy-kaf/cy-kaf-client/internal/mcpserver"
)

var (
	_ appcluster.KsqlClassifier = (infraksql.Classifier{})
	_ cluster.KsqlPort          = (*infraksql.Pool)(nil)

	_ api.ClusterStater       = (*appcluster.StateCache)(nil)
	_ api.LogDirser           = (*appcluster.BrokerService)(nil)
	_ api.BrokerAdmin         = (*appcluster.BrokerService)(nil)
	_ api.TopicServicer       = (*appcluster.TopicService)(nil)
	_ api.GroupServicer       = (*appcluster.GroupService)(nil)
	_ api.SerdesServicer      = (*appcluster.SerdeService)(nil)
	_ api.SchemaServicer      = (*appcluster.SchemaService)(nil)
	_ api.ConnectServicer     = (*appcluster.ConnectService)(nil)
	_ api.AclServicer         = (*appcluster.AclService)(nil)
	_ api.QuotaServicer       = (*appcluster.QuotaService)(nil)
	_ api.KsqlServicer        = (*appcluster.KsqlService)(nil)
	_ api.MessageServicer     = (*appcluster.MessageService)(nil)
	_ api.AnalysisServicer    = (*appcluster.AnalysisService)(nil)
	_ api.SmartFilterServicer = (*appcluster.SmartFilterService)(nil)
	_ api.ConfigServicer      = (*appcluster.ConfigService)(nil)
	_ api.ReloaderServicer    = (*appcluster.Reloader)(nil)

	_ mcpserver.ClusterStater       = (*appcluster.StateCache)(nil)
	_ mcpserver.BrokerServicer      = (*appcluster.BrokerService)(nil)
	_ mcpserver.TopicServicer       = (*appcluster.TopicService)(nil)
	_ mcpserver.GroupServicer       = (*appcluster.GroupService)(nil)
	_ mcpserver.SerdeServicer       = (*appcluster.SerdeService)(nil)
	_ mcpserver.SchemaServicer      = (*appcluster.SchemaService)(nil)
	_ mcpserver.ConnectServicer     = (*appcluster.ConnectService)(nil)
	_ mcpserver.AclServicer         = (*appcluster.AclService)(nil)
	_ mcpserver.QuotaServicer       = (*appcluster.QuotaService)(nil)
	_ mcpserver.KSQLServicer        = (*appcluster.KsqlService)(nil)
	_ mcpserver.MessageServicer     = (*appcluster.MessageService)(nil)
	_ mcpserver.AnalysisServicer    = (*appcluster.AnalysisService)(nil)
	_ mcpserver.SmartFilterServicer = (*appcluster.SmartFilterService)(nil)
)

func TestSharedWiringSharesHTTPAndMCPDependenciesAndCleansUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	services, err := wireServices(ctx, &config.App{}, filepath.Join(t.TempDir(), "config.yaml"))
	require.NoError(t, err)
	t.Cleanup(services.Cleanup)
	require.NotNil(t, services.States)
	require.NotNil(t, services.Brokers)
	require.NotNil(t, services.Topics)
	require.NotNil(t, services.Groups)
	require.NotNil(t, services.Serdes)
	require.NotNil(t, services.Schemas)
	require.NotNil(t, services.Connects)
	require.NotNil(t, services.Acls)
	require.NotNil(t, services.Quotas)
	require.NotNil(t, services.KSQL)
	require.NotNil(t, services.KSQLClassifier)
	require.NotNil(t, services.Messages)
	require.NotNil(t, services.Analysis)
	require.NotNil(t, services.SmartFilters)
	require.NotNil(t, services.Config)
	require.NotNil(t, services.Reloader)
	require.NotNil(t, services.Resolver)
	require.NotNil(t, services.Cleanup)
	require.Zero(t, services.ClusterCount)

	desktop := &api.DesktopOptions{}
	httpDeps := services.apiDeps(desktop)
	policy := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	mcpDeps := services.mcpDeps(policy)

	require.NotNil(t, httpDeps.States)
	require.NotNil(t, httpDeps.LogDirs)
	require.NotNil(t, httpDeps.Brokers)
	require.NotNil(t, httpDeps.Topics)
	require.NotNil(t, httpDeps.Groups)
	require.NotNil(t, httpDeps.Serdes)
	require.NotNil(t, httpDeps.Schemas)
	require.NotNil(t, httpDeps.Connects)
	require.NotNil(t, httpDeps.Acls)
	require.NotNil(t, httpDeps.Quotas)
	require.NotNil(t, httpDeps.Ksql)
	require.NotNil(t, httpDeps.Messages)
	require.NotNil(t, httpDeps.Analysis)
	require.NotNil(t, httpDeps.SmartFilters)
	require.NotNil(t, httpDeps.Config)
	require.NotNil(t, httpDeps.Reloader)
	require.NotNil(t, httpDeps.IsReadOnly)
	require.NotNil(t, httpDeps.Static)
	require.NotNil(t, httpDeps.Desktop)
	require.Same(t, services.States, httpDeps.States)
	require.Same(t, services.Brokers, httpDeps.LogDirs)
	require.Same(t, services.Brokers, httpDeps.Brokers)
	require.Same(t, services.Topics, httpDeps.Topics)
	require.Same(t, services.Groups, httpDeps.Groups)
	require.Same(t, services.Serdes, httpDeps.Serdes)
	require.Same(t, services.Schemas, httpDeps.Schemas)
	require.Same(t, services.Connects, httpDeps.Connects)
	require.Same(t, services.Acls, httpDeps.Acls)
	require.Same(t, services.Quotas, httpDeps.Quotas)
	require.Same(t, services.KSQL, httpDeps.Ksql)
	require.Same(t, services.Messages, httpDeps.Messages)
	require.Same(t, services.Analysis, httpDeps.Analysis)
	require.Same(t, services.SmartFilters, httpDeps.SmartFilters)
	require.Same(t, services.Config, httpDeps.Config)
	require.Same(t, services.Reloader, httpDeps.Reloader)
	require.Same(t, desktop, httpDeps.Desktop)

	require.NotNil(t, mcpDeps.States)
	require.NotNil(t, mcpDeps.Brokers)
	require.NotNil(t, mcpDeps.Topics)
	require.NotNil(t, mcpDeps.Groups)
	require.NotNil(t, mcpDeps.Serdes)
	require.NotNil(t, mcpDeps.Schemas)
	require.NotNil(t, mcpDeps.Connects)
	require.NotNil(t, mcpDeps.Acls)
	require.NotNil(t, mcpDeps.Quotas)
	require.NotNil(t, mcpDeps.KSQL)
	require.NotNil(t, mcpDeps.KSQLClassifier)
	require.NotNil(t, mcpDeps.Messages)
	require.NotNil(t, mcpDeps.Analysis)
	require.NotNil(t, mcpDeps.SmartFilters)
	require.NotNil(t, mcpDeps.IsReadOnly)
	require.NotNil(t, mcpDeps.Policy)
	require.Same(t, services.States, mcpDeps.States)
	require.Same(t, services.Brokers, mcpDeps.Brokers)
	require.Same(t, services.Topics, mcpDeps.Topics)
	require.Same(t, services.Groups, mcpDeps.Groups)
	require.Same(t, services.Serdes, mcpDeps.Serdes)
	require.Same(t, services.Schemas, mcpDeps.Schemas)
	require.Same(t, services.Connects, mcpDeps.Connects)
	require.Same(t, services.Acls, mcpDeps.Acls)
	require.Same(t, services.Quotas, mcpDeps.Quotas)
	require.Same(t, services.KSQL, mcpDeps.KSQL)
	require.Equal(t, services.KSQLClassifier, mcpDeps.KSQLClassifier)
	require.Same(t, services.Messages, mcpDeps.Messages)
	require.Same(t, services.Analysis, mcpDeps.Analysis)
	require.Same(t, services.SmartFilters, mcpDeps.SmartFilters)
	require.Same(t, policy, mcpDeps.Policy)

	cancel()
	require.NotPanics(t, services.Cleanup)
	require.NotPanics(t, services.Cleanup)
}

func TestRuntimeCleanupClosesEveryPoolExactlyOnce(t *testing.T) {
	closers := []*countingRuntimeCloser{{}, {}, {}, {}}
	cleanup := newRuntimeCleanup(
		closers[0],
		closers[1],
		closers[2],
		closers[3],
	)

	cleanup()
	cleanup()

	for index, closer := range closers {
		require.Equal(t, 1, closer.calls, "closer %d", index)
	}
}

type countingRuntimeCloser struct {
	calls int
}

func (c *countingRuntimeCloser) Close() {
	c.calls++
}

func TestKsqlCompositionConstructsDependencies(t *testing.T) {
	resolver := appcluster.NewResolver(nil)
	classifier := infraksql.Classifier{}
	service := appcluster.NewKsqlService(resolver, infraksql.NewPool(), classifier)
	if service == nil {
		t.Fatal("KSQL service returned nil")
	}

	deps := api.Deps{Ksql: service}
	if deps.Ksql == nil {
		t.Fatal("KSQL dependency was not installed")
	}
}
