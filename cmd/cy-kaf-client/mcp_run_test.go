package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/infra/config"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/cy-kaf/cy-kaf-client/internal/mcpserver"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

func TestRunMCPDisabledAndCorruptPolicyFailBeforeWiringOrTransport(t *testing.T) {
	for _, tt := range []struct {
		name      string
		writeFile func(t *testing.T, path string)
	}{
		{name: "disabled policy is missing"},
		{
			name: "corrupt policy",
			writeFile: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path, []byte(`{"version":1,"enabled":`), 0o600))
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			configPath := writeEmptyConfig(t)
			if tt.writeFile != nil {
				tt.writeFile(t, mcppolicy.PathForConfig(configPath))
			}
			var wireCalls atomic.Int32
			transport := &connectErrorTransport{err: errors.New("transport must not connect")}
			err := runMCP(context.Background(), mcpOptions{
				ConfigPath:     configPath,
				ConfigExplicit: true,
			}, transport, testMCPRuntimeDeps(func(
				context.Context,
				*config.App,
				string,
				*mcppolicy.Store,
			) (mcpRuntime, error) {
				wireCalls.Add(1)
				return mcpRuntime{}, nil
			}))

			require.Error(t, err)
			require.Zero(t, wireCalls.Load())
			require.Zero(t, transport.calls.Load())
		})
	}
}

func TestRunMCPPreservesDefaultAndExplicitConfigLoadSemantics(t *testing.T) {
	t.Run("missing default config is accepted", func(t *testing.T) {
		configPath := filepath.Join(t.TempDir(), "missing.yaml")
		saveMCPPolicy(t, configPath, false)
		sentinel := errors.New("transport stopped")
		transport := &connectErrorTransport{err: sentinel}
		var wireCalls atomic.Int32

		err := runMCP(context.Background(), mcpOptions{ConfigPath: configPath}, transport,
			testMCPRuntimeDeps(func(
				_ context.Context,
				cfg *config.App,
				_ string,
				store *mcppolicy.Store,
			) (mcpRuntime, error) {
				wireCalls.Add(1)
				require.Empty(t, cfg.Kafka.Clusters)
				return readyMCPRuntime(store, nil), nil
			}))

		require.ErrorIs(t, err, sentinel)
		require.Equal(t, int32(1), wireCalls.Load())
		require.Equal(t, int32(1), transport.calls.Load())
	})

	t.Run("missing explicit config is rejected", func(t *testing.T) {
		configPath := filepath.Join(t.TempDir(), "missing.yaml")
		saveMCPPolicy(t, configPath, false)
		transport := &connectErrorTransport{err: errors.New("transport must not connect")}
		var wireCalls atomic.Int32

		err := runMCP(context.Background(), mcpOptions{
			ConfigPath:     configPath,
			ConfigExplicit: true,
		}, transport, testMCPRuntimeDeps(func(
			context.Context,
			*config.App,
			string,
			*mcppolicy.Store,
		) (mcpRuntime, error) {
			wireCalls.Add(1)
			return mcpRuntime{}, nil
		}))

		require.Error(t, err)
		require.Zero(t, wireCalls.Load())
		require.Zero(t, transport.calls.Load())
	})
}

func TestRunMCPCancelsBeforeCleanupOnWiringAndTransportErrors(t *testing.T) {
	for _, tt := range []struct {
		name        string
		wireError   error
		transport   mcp.Transport
		wantConnect int32
	}{
		{
			name:        "wiring error",
			wireError:   errors.New("wire failed"),
			transport:   &connectErrorTransport{err: errors.New("must not connect")},
			wantConnect: 0,
		},
		{
			name:        "transport error",
			transport:   &connectErrorTransport{err: errors.New("connect failed")},
			wantConnect: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			configPath := writeEmptyConfig(t)
			saveMCPPolicy(t, configPath, false)
			var cleanupCalls atomic.Int32
			var cleanupSawCancelled atomic.Bool

			err := runMCP(context.Background(), mcpOptions{
				ConfigPath:     configPath,
				ConfigExplicit: true,
			}, tt.transport, testMCPRuntimeDeps(func(
				ctx context.Context,
				_ *config.App,
				_ string,
				store *mcppolicy.Store,
			) (mcpRuntime, error) {
				runtime := readyMCPRuntime(store, func() {
					cleanupCalls.Add(1)
					cleanupSawCancelled.Store(errors.Is(ctx.Err(), context.Canceled))
				})
				return runtime, tt.wireError
			}))

			require.Error(t, err)
			require.Equal(t, int32(1), cleanupCalls.Load())
			require.True(t, cleanupSawCancelled.Load())
			require.Equal(t, tt.wantConnect, tt.transport.(*connectErrorTransport).calls.Load())
		})
	}
}

func TestRunMCPNewServerErrorCancelsBeforeOneCleanup(t *testing.T) {
	configPath := writeEmptyConfig(t)
	saveMCPPolicy(t, configPath, false)
	cause := errors.New("server construction failed")
	transport := &connectErrorTransport{err: errors.New("transport must not connect")}
	var cleanupCalls atomic.Int32
	var cleanupSawCancelled atomic.Bool
	deps := testMCPRuntimeDeps(func(
		ctx context.Context,
		_ *config.App,
		_ string,
		store *mcppolicy.Store,
	) (mcpRuntime, error) {
		return readyMCPRuntime(store, func() {
			cleanupCalls.Add(1)
			cleanupSawCancelled.Store(errors.Is(ctx.Err(), context.Canceled))
		}), nil
	})
	deps.NewServer = func(
		mcppolicy.Policy,
		mcpserver.Dependencies,
		version.BuildInfo,
	) (*mcp.Server, error) {
		return nil, cause
	}

	err := runMCP(context.Background(), mcpOptions{
		ConfigPath:     configPath,
		ConfigExplicit: true,
	}, transport, deps)

	require.EqualError(t, err, "MCP failed: SERVER_FAILED")
	require.ErrorIs(t, err, cause)
	require.Equal(t, int32(1), cleanupCalls.Load())
	require.True(t, cleanupSawCancelled.Load())
	require.Zero(t, transport.calls.Load())
}

func TestRunMCPParentCancellationAndStdinCloseCleanUp(t *testing.T) {
	t.Run("parent cancellation", func(t *testing.T) {
		configPath := writeEmptyConfig(t)
		saveMCPPolicy(t, configPath, false)
		inputReader, inputWriter := io.Pipe()
		t.Cleanup(func() {
			_ = inputReader.Close()
			_ = inputWriter.Close()
		})
		ctx, cancel := context.WithCancel(context.Background())
		var cleanupCalls atomic.Int32
		var cleanupSawCancelled atomic.Bool
		wired := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- runMCP(ctx, mcpOptions{
				ConfigPath:     configPath,
				ConfigExplicit: true,
			}, &mcp.IOTransport{
				Reader: inputReader,
				Writer: discardWriteCloser{Writer: io.Discard},
			}, testMCPRuntimeDeps(func(
				runtimeCtx context.Context,
				_ *config.App,
				_ string,
				store *mcppolicy.Store,
			) (mcpRuntime, error) {
				close(wired)
				return readyMCPRuntime(store, func() {
					cleanupCalls.Add(1)
					cleanupSawCancelled.Store(errors.Is(runtimeCtx.Err(), context.Canceled))
				}), nil
			}))
		}()
		<-wired
		cancel()

		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("MCP run did not stop after parent cancellation")
		}
		require.Equal(t, int32(1), cleanupCalls.Load())
		require.True(t, cleanupSawCancelled.Load())
	})

	t.Run("stdin close", func(t *testing.T) {
		configPath := writeEmptyConfig(t)
		saveMCPPolicy(t, configPath, false)
		var cleanupCalls atomic.Int32
		var cleanupSawCancelled atomic.Bool

		err := runMCP(context.Background(), mcpOptions{
			ConfigPath:     configPath,
			ConfigExplicit: true,
		}, &mcp.IOTransport{
			Reader: io.NopCloser(bytes.NewReader(nil)),
			Writer: discardWriteCloser{Writer: io.Discard},
		}, testMCPRuntimeDeps(func(
			runtimeCtx context.Context,
			_ *config.App,
			_ string,
			store *mcppolicy.Store,
		) (mcpRuntime, error) {
			return readyMCPRuntime(store, func() {
				cleanupCalls.Add(1)
				cleanupSawCancelled.Store(errors.Is(runtimeCtx.Err(), context.Canceled))
			}), nil
		}))

		require.NoError(t, err)
		require.Equal(t, int32(1), cleanupCalls.Load())
		require.True(t, cleanupSawCancelled.Load())
	})
}

func TestRunMCPIOTransportKeepsStdoutJSONRPCAndDebugOnStderr(t *testing.T) {
	configPath := writeEmptyConfig(t)
	saveMCPPolicy(t, configPath, false)

	var stderr bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(newMCPLogger(true, &stderr))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	serverInputReader, clientOutputWriter := io.Pipe()
	clientInputReader, serverOutputWriter := io.Pipe()
	var stdout bytes.Buffer
	serverWriter := &captureWriteCloser{
		Writer: io.MultiWriter(&stdout, serverOutputWriter),
		close:  serverOutputWriter.Close,
	}
	t.Cleanup(func() {
		_ = serverInputReader.Close()
		_ = clientOutputWriter.Close()
		_ = clientInputReader.Close()
		_ = serverOutputWriter.Close()
	})

	runResult := make(chan error, 1)
	go func() {
		runResult <- runMCP(context.Background(), mcpOptions{
			ConfigPath:     configPath,
			ConfigExplicit: true,
			Debug:          true,
		}, &mcp.IOTransport{
			Reader: serverInputReader,
			Writer: serverWriter,
		}, testMCPRuntimeDeps(func(
			_ context.Context,
			_ *config.App,
			_ string,
			store *mcppolicy.Store,
		) (mcpRuntime, error) {
			return readyMCPRuntime(store, nil), nil
		}))
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "stdio-test", Version: "test"}, nil)
	session, err := client.Connect(context.Background(), &mcp.IOTransport{
		Reader: clientInputReader,
		Writer: clientOutputWriter,
	}, nil)
	require.NoError(t, err)
	tools, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, 51)
	require.NoError(t, session.Close())

	select {
	case err := <-runResult:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("MCP run did not stop after the SDK client closed")
	}

	scanner := bufio.NewScanner(strings.NewReader(stdout.String()))
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		require.True(t, json.Valid(scanner.Bytes()), scanner.Text())
	}
	require.NoError(t, scanner.Err())
	require.Positive(t, lineCount)
	require.NotContains(t, stdout.String(), "MCP STDIO starting")
	require.NotContains(t, stdout.String(), "CY_KAF_")
	require.Contains(t, stderr.String(), "MCP STDIO starting")
	require.NotContains(t, stderr.String(), configPath)
}

type connectErrorTransport struct {
	err   error
	calls atomic.Int32
}

func (t *connectErrorTransport) Connect(context.Context) (mcp.Connection, error) {
	t.calls.Add(1)
	return nil, t.err
}

type discardWriteCloser struct {
	io.Writer
}

func (discardWriteCloser) Close() error { return nil }

type captureWriteCloser struct {
	io.Writer
	close func() error
}

func (w *captureWriteCloser) Close() error { return w.close() }

func testMCPRuntimeDeps(
	wire func(context.Context, *config.App, string, *mcppolicy.Store) (mcpRuntime, error),
) mcpRuntimeDeps {
	if wire == nil {
		wire = func(context.Context, *config.App, string, *mcppolicy.Store) (mcpRuntime, error) {
			return mcpRuntime{}, errors.New("unexpected MCP wiring")
		}
	}
	return mcpRuntimeDeps{
		LoadConfig: config.Load,
		Wire:       wire,
		NewServer:  mcpserver.New,
		BuildInfo: func() version.BuildInfo {
			return version.BuildInfo{Version: "test"}
		},
	}
}

func readyMCPRuntime(store *mcppolicy.Store, cleanup func()) mcpRuntime {
	return mcpRuntime{
		Dependencies: mcpserver.Dependencies{
			Policy:     store,
			IsReadOnly: func(string) bool { return false },
		},
		Cleanup: cleanup,
	}
}

func saveMCPPolicy(t *testing.T, configPath string, allowWrites bool) {
	t.Helper()
	store := mcppolicy.NewStore(mcppolicy.PathForConfig(configPath))
	require.NoError(t, store.Save(context.Background(), mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     true,
		AllowWrites: allowWrites,
	}))
}
