package mcpserver

import (
	"fmt"
	"log/slog"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

const serverInstructions = "Manage configured Kafka clusters. MCP is server-gated by local policy. " +
	"List and query before changing state; honor pagination and cluster read-only errors."

// New assembles one transport-independent MCP server whose visible catalog is
// fixed by the startup policy. The Executor continues to reload policy for
// every tool call.
func New(
	startupPolicy mcppolicy.Policy,
	deps Dependencies,
	build version.BuildInfo,
) (*mcp.Server, error) {
	if startupPolicy.Version != mcppolicy.CurrentVersion {
		return nil, fmt.Errorf("invalid MCP startup policy")
	}
	if !startupPolicy.Enabled {
		return nil, fmt.Errorf("MCP is disabled by local policy")
	}
	executor, err := NewExecutor(deps)
	if err != nil {
		return nil, fmt.Errorf("create MCP executor: %w", err)
	}
	server := mcp.NewServer(
		&mcp.Implementation{Name: "cy-kaf-client", Version: build.Version},
		&mcp.ServerOptions{
			Instructions: serverInstructions,
			Logger:       slog.Default(),
		},
	)
	for _, spec := range VisibleCatalog(startupPolicy) {
		spec.Register(server, executor)
	}
	return server, nil
}
