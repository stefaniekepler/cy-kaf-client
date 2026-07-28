package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/infra/config"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/cy-kaf/cy-kaf-client/internal/mcpserver"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

func TestMCPFailureLogsExposeOnlyStableStageCodes(t *testing.T) {
	const marker = "RAW-MARKER-password=https://user:secret@cluster.invalid"

	tests := []struct {
		name      string
		setup     func(t *testing.T) (mcpOptions, mcp.Transport, mcpRuntimeDeps, error)
		wantCode  string
		wantCause bool
	}{
		{
			name: "explicit missing config path",
			setup: func(t *testing.T) (mcpOptions, mcp.Transport, mcpRuntimeDeps, error) {
				path := filepath.Join(t.TempDir(), marker+".yaml")
				cause := &os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}
				deps := testMCPRuntimeDeps(nil)
				deps.LoadConfig = func(string, bool) (*config.App, error) { return nil, cause }
				return mcpOptions{ConfigPath: path, ConfigExplicit: true}, &connectErrorTransport{}, deps, cause
			},
			wantCode:  "CONFIG_INVALID",
			wantCause: true,
		},
		{
			name: "malformed config",
			setup: func(t *testing.T) (mcpOptions, mcp.Transport, mcpRuntimeDeps, error) {
				path := filepath.Join(t.TempDir(), "config.yaml")
				require.NoError(t, os.WriteFile(path, []byte(
					"kafka:\n  clusters:\n    - name: "+marker+"\n      bootstrapServers: [\n",
				), 0o600))
				return mcpOptions{ConfigPath: path, ConfigExplicit: true},
					&connectErrorTransport{}, testMCPRuntimeDeps(nil), nil
			},
			wantCode: "CONFIG_INVALID",
		},
		{
			name: "policy read path",
			setup: func(t *testing.T) (mcpOptions, mcp.Transport, mcpRuntimeDeps, error) {
				dir := filepath.Join(t.TempDir(), marker)
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "mcp-policy.json"), 0o700))
				path := filepath.Join(dir, "config.yaml")
				require.NoError(t, os.WriteFile(path, []byte("kafka:\n  clusters: []\n"), 0o600))
				return mcpOptions{ConfigPath: path, ConfigExplicit: true},
					&connectErrorTransport{}, testMCPRuntimeDeps(nil), nil
			},
			wantCode: "POLICY_INVALID",
		},
		{
			name: "wire failure",
			setup: func(t *testing.T) (mcpOptions, mcp.Transport, mcpRuntimeDeps, error) {
				path := writeEmptyConfig(t)
				saveMCPPolicy(t, path, false)
				cause := errors.New(marker)
				deps := testMCPRuntimeDeps(func(
					context.Context,
					*config.App,
					string,
					*mcppolicy.Store,
				) (mcpRuntime, error) {
					slog.Error("component wire failure", "error", cause, "cluster", marker)
					return mcpRuntime{}, cause
				})
				return mcpOptions{ConfigPath: path, ConfigExplicit: true}, &connectErrorTransport{}, deps, cause
			},
			wantCode:  "WIRING_FAILED",
			wantCause: true,
		},
		{
			name: "server failure",
			setup: func(t *testing.T) (mcpOptions, mcp.Transport, mcpRuntimeDeps, error) {
				path := writeEmptyConfig(t)
				saveMCPPolicy(t, path, false)
				cause := errors.New(marker)
				deps := testMCPRuntimeDeps(func(
					_ context.Context,
					_ *config.App,
					_ string,
					store *mcppolicy.Store,
				) (mcpRuntime, error) {
					return readyMCPRuntime(store, nil), nil
				})
				deps.NewServer = func(
					mcppolicy.Policy,
					mcpserver.Dependencies,
					version.BuildInfo,
				) (*mcp.Server, error) {
					slog.Error("component server failure", "error", cause, "url", marker)
					return nil, cause
				}
				return mcpOptions{ConfigPath: path, ConfigExplicit: true}, &connectErrorTransport{}, deps, cause
			},
			wantCode:  "SERVER_FAILED",
			wantCause: true,
		},
		{
			name: "transport failure",
			setup: func(t *testing.T) (mcpOptions, mcp.Transport, mcpRuntimeDeps, error) {
				path := writeEmptyConfig(t)
				saveMCPPolicy(t, path, false)
				cause := errors.New(marker)
				transport := &loggingErrorTransport{cause: cause, marker: marker}
				deps := testMCPRuntimeDeps(func(
					_ context.Context,
					_ *config.App,
					_ string,
					store *mcppolicy.Store,
				) (mcpRuntime, error) {
					return readyMCPRuntime(store, nil), nil
				})
				return mcpOptions{ConfigPath: path, ConfigExplicit: true, Debug: true},
					transport, deps, cause
			},
			wantCode:  "TRANSPORT_FAILED",
			wantCause: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, transport, deps, cause := tt.setup(t)
			var stderr bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(newMCPLogger(true, &stderr))
			t.Cleanup(func() { slog.SetDefault(previous) })

			err := runMCP(context.Background(), opts, transport, deps)
			require.EqualError(t, err, "MCP failed: "+tt.wantCode)
			if tt.wantCause {
				require.ErrorIs(t, err, cause)
			}
			logMCPExit(err)

			require.Contains(t, stderr.String(), "MCP process exited")
			require.Contains(t, stderr.String(), "code="+tt.wantCode)
			require.NotContains(t, stderr.String(), marker)
			require.NotContains(t, stderr.String(), opts.ConfigPath)
			require.NotContains(t, stderr.String(), "component ")
			require.NotContains(t, stderr.String(), "error=")
			require.NotContains(t, stderr.String(), "cluster=")
			require.NotContains(t, stderr.String(), "url=")
		})
	}
}

func TestMCPLoggerDropsUntrustedRecordsAndAttributes(t *testing.T) {
	const marker = "RAW-MARKER-credential"
	var stderr bytes.Buffer
	logger := newMCPLogger(true, &stderr).With("credential", marker).WithGroup(marker)

	logger.Error("SDK component "+marker, "error", errors.New(marker), "url", marker)
	logger.Debug("MCP STDIO starting", "tools", 51, "cluster", marker)
	logger.Error("MCP process exited", "code", "TRANSPORT_FAILED", "error", marker)
	logger.Error("MCP process exited", "code", marker)

	require.Contains(t, stderr.String(), "MCP STDIO starting")
	require.Contains(t, stderr.String(), "tools=51")
	require.Contains(t, stderr.String(), "MCP process exited")
	require.Contains(t, stderr.String(), "code=TRANSPORT_FAILED")
	require.NotContains(t, stderr.String(), marker)
	require.NotContains(t, stderr.String(), "error=")
	require.NotContains(t, stderr.String(), "cluster=")
	require.NotContains(t, stderr.String(), "url=")
}

func TestMCPFailureNormalizesUnknownCodeWithoutLosingCause(t *testing.T) {
	cause := errors.New("raw cause")

	err := mcpFailure("RAW-MARKER-credential", cause)

	require.EqualError(t, err, "MCP failed: INTERNAL_FAILED")
	require.ErrorIs(t, err, cause)
}

type loggingErrorTransport struct {
	cause  error
	marker string
}

func (t *loggingErrorTransport) Connect(context.Context) (mcp.Connection, error) {
	slog.Error("SDK transport failure", "error", t.cause, "method", t.marker)
	return nil, t.cause
}
