package mcpserver

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

const testServerInstructions = "Manage configured Kafka clusters. MCP is server-gated by local policy. " +
	"List and query before changing state; honor pagination and cluster read-only errors."

func TestServerToolListMatchesStartupPolicyAndBuildMetadata(t *testing.T) {
	tests := []struct {
		name        string
		allowWrites bool
		wantTools   int
	}{
		{name: "read only", allowWrites: false, wantTools: 51},
		{name: "write enabled", allowWrites: true, wantTools: 84},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := enabledPolicyStore(t, tt.allowWrites)
			policy, err := store.Load()
			require.NoError(t, err)
			server, err := New(policy, Dependencies{
				Policy:     store,
				IsReadOnly: func(string) bool { return false },
			}, version.BuildInfo{Version: "1.2.3"})
			require.NoError(t, err)

			serverTransport, clientTransport := mcp.NewInMemoryTransports()
			serverSession, err := server.Connect(context.Background(), serverTransport, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, serverSession.Close()) })
			client := mcp.NewClient(&mcp.Implementation{Name: "server-test", Version: "test"}, nil)
			clientSession, err := client.Connect(context.Background(), clientTransport, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, clientSession.Close()) })

			initialize := clientSession.InitializeResult()
			require.Equal(t, "cy-kaf-client", initialize.ServerInfo.Name)
			require.Equal(t, "1.2.3", initialize.ServerInfo.Version)
			require.Equal(t, testServerInstructions, initialize.Instructions)
			tools, err := clientSession.ListTools(context.Background(), nil)
			require.NoError(t, err)
			require.Len(t, tools.Tools, tt.wantTools)
			for _, tool := range tools.Tools {
				require.NotNil(t, tool.InputSchema, tool.Name)
				require.NotNil(t, tool.Annotations, tool.Name)
			}
		})
	}
}

func TestServerRejectsDisabledInvalidAndIncompleteStartup(t *testing.T) {
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	tests := []struct {
		name   string
		policy mcppolicy.Policy
		deps   Dependencies
	}{
		{
			name:   "disabled",
			policy: mcppolicy.Default(),
			deps:   Dependencies{Policy: store, IsReadOnly: func(string) bool { return false }},
		},
		{
			name:   "invalid version",
			policy: mcppolicy.Policy{Version: 99, Enabled: true},
			deps:   Dependencies{Policy: store, IsReadOnly: func(string) bool { return false }},
		},
		{
			name:   "missing executor dependency",
			policy: mcppolicy.Policy{Version: mcppolicy.CurrentVersion, Enabled: true},
			deps:   Dependencies{Policy: store},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := New(tt.policy, tt.deps, version.BuildInfo{Version: "test"})
			require.Error(t, err)
			require.Nil(t, server)
		})
	}
}

func TestServerToolVisibilityIsFixedForSessionAndWidensOnlyAfterRestart(t *testing.T) {
	store := enabledPolicyStore(t, false)
	readOnlyPolicy, err := store.Load()
	require.NoError(t, err)
	deps := Dependencies{
		Policy:     store,
		IsReadOnly: func(string) bool { return false },
	}
	server, err := New(readOnlyPolicy, deps, version.BuildInfo{Version: "test"})
	require.NoError(t, err)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, serverSession.Close()) })
	client := mcp.NewClient(&mcp.Implementation{Name: "visibility-test", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, clientSession.Close()) })

	initial, err := clientSession.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, initial.Tools, 51)
	require.NoError(t, store.Save(context.Background(), mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     true,
		AllowWrites: true,
	}))
	sameSession, err := clientSession.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, sameSession.Tools, 51)

	writePolicy, err := store.Load()
	require.NoError(t, err)
	restarted, err := New(writePolicy, deps, version.BuildInfo{Version: "test"})
	require.NoError(t, err)
	restartedServerTransport, restartedClientTransport := mcp.NewInMemoryTransports()
	restartedServerSession, err := restarted.Connect(
		context.Background(),
		restartedServerTransport,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restartedServerSession.Close()) })
	restartedClient := mcp.NewClient(
		&mcp.Implementation{Name: "restarted-visibility-test", Version: "test"},
		nil,
	)
	restartedClientSession, err := restartedClient.Connect(
		context.Background(),
		restartedClientTransport,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restartedClientSession.Close()) })
	afterRestart, err := restartedClientSession.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, afterRestart.Tools, 84)
}

func enabledPolicyStore(t *testing.T, allowWrites bool) *mcppolicy.Store {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     true,
		AllowWrites: allowWrites,
	}))
	return store
}
