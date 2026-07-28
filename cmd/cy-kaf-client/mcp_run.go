package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/cy-kaf/cy-kaf-client/internal/infra/config"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/cy-kaf/cy-kaf-client/internal/mcpserver"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

type mcpRuntime struct {
	Dependencies mcpserver.Dependencies
	Cleanup      func()
}

type mcpRuntimeDeps struct {
	LoadConfig func(string, bool) (*config.App, error)
	Wire       func(context.Context, *config.App, string, *mcppolicy.Store) (mcpRuntime, error)
	NewServer  func(mcppolicy.Policy, mcpserver.Dependencies, version.BuildInfo) (*mcp.Server, error)
	BuildInfo  func() version.BuildInfo
}

func productionMCPRuntimeDeps() mcpRuntimeDeps {
	return mcpRuntimeDeps{
		LoadConfig: config.Load,
		Wire:       wireMCPRuntime,
		NewServer:  mcpserver.New,
		BuildInfo:  version.Info,
	}
}

func runMCP(
	parent context.Context,
	opts mcpOptions,
	transport mcp.Transport,
	deps mcpRuntimeDeps,
) error {
	if transport == nil {
		return mcpFailure("RUNTIME_INVALID", fmt.Errorf("MCP transport is required"))
	}
	if deps.LoadConfig == nil || deps.Wire == nil || deps.NewServer == nil || deps.BuildInfo == nil {
		return mcpFailure("RUNTIME_INVALID", fmt.Errorf("MCP runtime dependencies are incomplete"))
	}

	ctx, cancel := context.WithCancel(parent)
	cleanup := func() {}
	defer func() {
		cancel()
		cleanup()
	}()

	cfg, err := deps.LoadConfig(opts.ConfigPath, opts.ConfigExplicit)
	if err != nil {
		return mcpFailure("CONFIG_INVALID", err)
	}

	policyStore := mcppolicy.NewStore(mcppolicy.PathForConfig(opts.ConfigPath))
	startupPolicy, err := policyStore.Load()
	if err != nil {
		return mcpFailure("POLICY_INVALID", err)
	}
	if !startupPolicy.Enabled {
		return mcpFailure("POLICY_DISABLED", fmt.Errorf("MCP is disabled by local policy"))
	}

	runtime, wireErr := deps.Wire(ctx, cfg, opts.ConfigPath, policyStore)
	if runtime.Cleanup != nil {
		cleanup = runtime.Cleanup
	}
	if wireErr != nil {
		return mcpFailure("WIRING_FAILED", wireErr)
	}

	build := deps.BuildInfo()
	server, err := deps.NewServer(startupPolicy, runtime.Dependencies, build)
	if err != nil {
		return mcpFailure("SERVER_FAILED", err)
	}
	if opts.Debug {
		slog.Debug(
			mcpLogStart,
			"tools", len(mcpserver.VisibleCatalog(startupPolicy)),
		)
	}
	if err := server.Run(ctx, transport); err != nil {
		code := "TRANSPORT_FAILED"
		if errors.Is(err, context.Canceled) {
			code = "CANCELLED"
		}
		return mcpFailure(code, err)
	}
	return nil
}
