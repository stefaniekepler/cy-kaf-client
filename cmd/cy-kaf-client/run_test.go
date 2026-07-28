package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	"github.com/cy-kaf/cy-kaf-client/internal/app/mcpsettings"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/mcpclient"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
)

func TestParseOptionsDesktopKeepsExplicitPortZero(t *testing.T) {
	got, err := parseOptions([]string{"--desktop", "--port", "0"})

	require.NoError(t, err)
	require.True(t, got.Desktop)
	require.True(t, got.PortExplicit)
	require.Zero(t, got.Port)
	require.False(t, got.NoBrowser, "desktop safety is enforced by run even if this flag is omitted")
}

func TestParseOptionsKeepsCLICompatibilityDefaults(t *testing.T) {
	got, err := parseOptions(nil)

	require.NoError(t, err)
	require.Equal(t, 8080, got.Port)
	require.False(t, got.PortExplicit)
	require.False(t, got.ConfigExplicit)
	require.False(t, got.NoBrowser)
	require.False(t, got.Desktop)
	require.False(t, got.Debug)
	require.Equal(t, defaultConfigPath(), got.ConfigPath)
}

func TestParseOptionsHelpIsVisibleAndMapsToSuccessfulExit(t *testing.T) {
	var output bytes.Buffer

	_, err := parseOptionsWithOutput([]string{"--help"}, &output)

	require.ErrorIs(t, err, flag.ErrHelp)
	require.Contains(t, output.String(), "-desktop")
	require.Zero(t, optionsErrorExitCode(err))
	require.Equal(t, 2, optionsErrorExitCode(fmt.Errorf("invalid flag")))
}

func TestRunDesktopUsesActualPortProtectsUIAndShutsDown(t *testing.T) {
	configPath := writeEmptyConfig(t)
	initReader, initWriter := io.Pipe()
	readyReader, readyWriter := io.Pipe()
	t.Cleanup(func() {
		_ = initReader.Close()
		_ = initWriter.Close()
		_ = readyReader.Close()
		_ = readyWriter.Close()
	})

	var browserCalls atomic.Int32
	opts := cliOptions{
		Port:           0,
		PortExplicit:   true,
		ConfigPath:     configPath,
		ConfigExplicit: true,
		Desktop:        true,
	}
	runResult := make(chan error, 1)
	go func() {
		runResult <- run(context.Background(), opts, initReader, readyWriter, runtimeDeps{
			Listen:      net.Listen,
			OpenBrowser: func(string) { browserCalls.Add(1) },
		})
	}()

	token := desktopTestSessionToken()
	_, err := fmt.Fprintf(
		initWriter,
		"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":%q}\n",
		token,
	)
	require.NoError(t, err)

	ready := readDesktopReadyForTest(t, readyReader)
	require.NotEqual(t, "http://127.0.0.1:0", ready.Origin)
	require.True(t, strings.HasPrefix(ready.Origin, "http://127.0.0.1:"))

	client := &http.Client{Timeout: 2 * time.Second}
	health := doDesktopRequest(t, client, http.MethodGet, ready.Origin+"/actuator/health", "", "")
	require.Equal(t, http.StatusOK, health.StatusCode)
	require.JSONEq(t, `{"status":"UP"}`, readResponseBody(t, health))

	anonymousUI := doDesktopRequest(t, client, http.MethodGet, ready.Origin+"/", "", "")
	require.Equal(t, http.StatusUnauthorized, anonymousUI.StatusCode)
	require.NotContains(t, readResponseBody(t, anonymousUI), token)

	authenticatedUI := doDesktopRequest(t, client, http.MethodGet, ready.Origin+"/", token, "")
	require.Equal(t, http.StatusOK, authenticatedUI.StatusCode)
	require.NotEmpty(t, readResponseBody(t, authenticatedUI))

	shutdown := doDesktopRequest(
		t,
		client,
		http.MethodPost,
		ready.Origin+"/__desktop/shutdown",
		token,
		ready.Origin,
	)
	require.Equal(t, http.StatusNoContent, shutdown.StatusCode)
	require.Empty(t, readResponseBody(t, shutdown))

	select {
	case err := <-runResult:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("desktop run did not stop after the protected shutdown request")
	}
	require.Zero(t, browserCalls.Load())
}

func TestRunDesktopParentEOFStopsServerWithoutOpeningBrowser(t *testing.T) {
	configPath := writeEmptyConfig(t)
	initReader, initWriter := io.Pipe()
	readyReader, readyWriter := io.Pipe()
	t.Cleanup(func() {
		_ = initReader.Close()
		_ = readyReader.Close()
		_ = readyWriter.Close()
	})

	var browserCalls atomic.Int32
	runResult := make(chan error, 1)
	go func() {
		runResult <- run(context.Background(), cliOptions{
			Port:           0,
			PortExplicit:   true,
			ConfigPath:     configPath,
			ConfigExplicit: true,
			Desktop:        true,
		}, initReader, readyWriter, runtimeDeps{
			Listen:      net.Listen,
			OpenBrowser: func(string) { browserCalls.Add(1) },
		})
	}()

	_, err := fmt.Fprintf(
		initWriter,
		"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":%q}\n",
		desktopTestSessionToken(),
	)
	require.NoError(t, err)
	_ = readDesktopReadyForTest(t, readyReader)
	require.NoError(t, initWriter.Close())

	select {
	case err := <-runResult:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("desktop run did not stop after parent stdin reached EOF")
	}
	require.Zero(t, browserCalls.Load())
}

func TestRunDesktopWritesOnlyClassifiedStartupErrors(t *testing.T) {
	token := desktopTestSessionToken()
	validInit := fmt.Sprintf(
		"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":%q}\n",
		token,
	)

	t.Run("invalid init", func(t *testing.T) {
		var out bytes.Buffer
		err := run(context.Background(), cliOptions{Desktop: true}, strings.NewReader("invalid\n"), &out, runtimeDeps{
			Listen: func(string, string) (net.Listener, error) {
				t.Fatal("listen must not run before init validation")
				return nil, nil
			},
		})
		require.Error(t, err)
		require.Contains(t, out.String(), `"code":"INIT_INVALID"`)
		require.NotContains(t, out.String(), "invalid\n")
	})

	t.Run("invalid config", func(t *testing.T) {
		var out bytes.Buffer
		err := run(context.Background(), cliOptions{
			Desktop:        true,
			ConfigPath:     filepath.Join(t.TempDir(), "missing.yaml"),
			ConfigExplicit: true,
		}, strings.NewReader(validInit), &out, runtimeDeps{Listen: net.Listen})
		require.Error(t, err)
		require.Contains(t, out.String(), `"code":"CONFIG_INVALID"`)
		require.NotContains(t, out.String(), token)
	})

	t.Run("occupied port", func(t *testing.T) {
		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() { require.NoError(t, occupied.Close()) }()
		port := occupied.Addr().(*net.TCPAddr).Port

		var out bytes.Buffer
		err = run(context.Background(), cliOptions{
			Desktop:        true,
			Port:           port,
			PortExplicit:   true,
			ConfigPath:     writeEmptyConfig(t),
			ConfigExplicit: true,
		}, strings.NewReader(validInit), &out, runtimeDeps{Listen: net.Listen})
		require.Error(t, err)
		require.Contains(t, out.String(), `"code":"PORT_UNAVAILABLE"`)
		require.NotContains(t, out.String(), token)
	})
}

func TestDesktopMCPStartupRunsNoClientCommandAndPreservesConfigExplicit(t *testing.T) {
	cases := []struct {
		name           string
		configExplicit bool
	}{
		{name: "default config omits config flag"},
		{
			name:           "explicit config uses cleaned absolute path",
			configExplicit: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			configPath := writeEmptyConfig(t)
			inputConfigPath := configPath
			if tc.configExplicit {
				workingDirectory, workingDirectoryErr := os.Getwd()
				require.NoError(t, workingDirectoryErr)
				inputConfigPath, workingDirectoryErr = filepath.Rel(workingDirectory, configPath)
				require.NoError(t, workingDirectoryErr)
				require.False(t, filepath.IsAbs(inputConfigPath))
			}
			recorder := &recordingDesktopMCPConfigurator{}
			originalFactory := productionConfiguratorFactory
			productionConfiguratorFactory = func() (mcpclient.Configurator, error) {
				return recorder, nil
			}
			t.Cleanup(func() { productionConfiguratorFactory = originalFactory })

			initReader, initWriter := io.Pipe()
			readyReader, readyWriter := io.Pipe()
			t.Cleanup(func() {
				_ = initReader.Close()
				_ = initWriter.Close()
				_ = readyReader.Close()
				_ = readyWriter.Close()
			})

			runResult := make(chan error, 1)
			go func() {
				runResult <- run(context.Background(), cliOptions{
					Port:           0,
					PortExplicit:   true,
					ConfigPath:     inputConfigPath,
					ConfigExplicit: tc.configExplicit,
					Desktop:        true,
				}, initReader, readyWriter, runtimeDeps{Listen: net.Listen})
			}()

			token := desktopTestSessionToken()
			_, err := fmt.Fprintf(
				initWriter,
				"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":%q}\n",
				token,
			)
			require.NoError(t, err)
			ready := readDesktopReadyForTest(t, readyReader)

			statusCalls, configureCalls, _ := recorder.snapshot()
			require.Zero(t, statusCalls, "server startup must not inspect a client")
			require.Zero(t, configureCalls, "server startup must not configure a client")

			client := &http.Client{Timeout: 2 * time.Second}
			integrations := doDesktopRequest(
				t, client, http.MethodGet, ready.Origin+"/__desktop/mcp-integrations", token, "",
			)
			require.Equal(t, http.StatusOK, integrations.StatusCode)
			_ = readResponseBody(t, integrations)

			statusCalls, configureCalls, desired := recorder.snapshot()
			require.Equal(t, 2, statusCalls)
			require.Zero(t, configureCalls)
			wantArgs := []string{"mcp"}
			if tc.configExplicit {
				canonicalConfig, canonicalErr := filepath.EvalSymlinks(configPath)
				require.NoError(t, canonicalErr)
				canonicalConfig, canonicalErr = filepath.Abs(canonicalConfig)
				require.NoError(t, canonicalErr)
				wantArgs = []string{"mcp", "--config", filepath.Clean(canonicalConfig)}
			}
			require.Equal(t, wantArgs, desired.Args)
			require.Equal(t, mcpclient.ServerName, desired.Name)

			shutdown := doDesktopRequest(
				t, client, http.MethodPost, ready.Origin+"/__desktop/shutdown", token, ready.Origin,
			)
			require.Equal(t, http.StatusNoContent, shutdown.StatusCode)
			require.Empty(t, readResponseBody(t, shutdown))
			select {
			case runErr := <-runResult:
				require.NoError(t, runErr)
			case <-time.After(2 * time.Second):
				t.Fatal("desktop run did not stop")
			}
		})
	}
}

func TestDesktopMCPExplicitConfigSymlinkUsesOneCanonicalPolicyDirectory(t *testing.T) {
	configPath := writeEmptyConfig(t)
	linkDirectory := t.TempDir()
	configLink := filepath.Join(linkDirectory, "linked-config.yaml")
	if err := os.Symlink(configPath, configLink); err != nil {
		t.Skipf("config symlink unavailable: %v", err)
	}

	recorder := &recordingDesktopMCPConfigurator{}
	originalFactory := productionConfiguratorFactory
	productionConfiguratorFactory = func() (mcpclient.Configurator, error) {
		return recorder, nil
	}
	t.Cleanup(func() { productionConfiguratorFactory = originalFactory })

	initReader, initWriter := io.Pipe()
	readyReader, readyWriter := io.Pipe()
	t.Cleanup(func() {
		_ = initReader.Close()
		_ = initWriter.Close()
		_ = readyReader.Close()
		_ = readyWriter.Close()
	})

	runResult := make(chan error, 1)
	go func() {
		runResult <- run(context.Background(), cliOptions{
			Port:           0,
			PortExplicit:   true,
			ConfigPath:     configLink,
			ConfigExplicit: true,
			Desktop:        true,
		}, initReader, readyWriter, runtimeDeps{Listen: net.Listen})
	}()

	token := desktopTestSessionToken()
	_, err := fmt.Fprintf(
		initWriter,
		"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":%q}\n",
		token,
	)
	require.NoError(t, err)
	ready := readDesktopReadyForTest(t, readyReader)

	client := &http.Client{Timeout: 2 * time.Second}
	updateRequest, err := http.NewRequest(
		http.MethodPut,
		ready.Origin+"/__desktop/mcp-settings",
		strings.NewReader(`{"enabled":true,"allowWrites":false}`),
	)
	require.NoError(t, err)
	updateRequest.AddCookie(&http.Cookie{Name: api.DesktopSessionCookieName, Value: token})
	updateRequest.Header.Set("Origin", ready.Origin)
	updateResponse, err := client.Do(updateRequest)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updateResponse.StatusCode)
	_ = readResponseBody(t, updateResponse)

	canonicalConfig, err := filepath.EvalSymlinks(configLink)
	require.NoError(t, err)
	canonicalConfig, err = filepath.Abs(canonicalConfig)
	require.NoError(t, err)
	canonicalPolicy := mcppolicy.PathForConfig(filepath.Clean(canonicalConfig))
	_, err = os.Stat(canonicalPolicy)
	require.NoError(t, err)
	_, err = os.Stat(mcppolicy.PathForConfig(configLink))
	require.ErrorIs(t, err, os.ErrNotExist)

	integrations := doDesktopRequest(
		t, client, http.MethodGet, ready.Origin+"/__desktop/mcp-integrations", token, "",
	)
	require.Equal(t, http.StatusOK, integrations.StatusCode)
	_ = readResponseBody(t, integrations)
	_, configureCalls, desired := recorder.snapshot()
	require.Zero(t, configureCalls)
	require.Equal(t, []string{"mcp", "--config", canonicalConfig}, desired.Args)

	shutdown := doDesktopRequest(
		t, client, http.MethodPost, ready.Origin+"/__desktop/shutdown", token, ready.Origin,
	)
	require.Equal(t, http.StatusNoContent, shutdown.StatusCode)
	require.Empty(t, readResponseBody(t, shutdown))
	select {
	case runErr := <-runResult:
		require.NoError(t, runErr)
	case <-time.After(2 * time.Second):
		t.Fatal("desktop run did not stop")
	}
}

func TestDesktopSettingsConfigureCommandStartsNewSDKProcessesWithCurrentPolicy(t *testing.T) {
	fixture := newDesktopSettingsProcessFixture(t)
	desktopA := fixture.startDesktop(t)

	settings := desktopA.request(t, http.MethodGet, "/__desktop/mcp-settings", "")
	require.Equal(t, http.StatusOK, settings.StatusCode)
	require.JSONEq(t, `{"enabled":false,"allowWrites":false}`, readResponseBody(t, settings))

	settings = desktopA.request(
		t,
		http.MethodPut,
		"/__desktop/mcp-settings",
		`{"enabled":true,"allowWrites":false,"confirmWrites":false}`,
	)
	require.Equal(t, http.StatusOK, settings.StatusCode)
	require.JSONEq(t, `{"enabled":true,"allowWrites":false}`, readResponseBody(t, settings))

	integrations := desktopA.request(t, http.MethodGet, "/__desktop/mcp-integrations", "")
	require.Equal(t, http.StatusOK, integrations.StatusCode)
	integrationsBody := readResponseBody(t, integrations)
	require.Contains(t, integrationsBody, `"client":"codex","status":"not_configured"`)
	require.Contains(t, integrationsBody, `"client":"claude-code","status":"configuration_conflict"`)
	require.NotContains(t, integrationsBody, fixture.secret)
	require.Empty(t, fixture.hostileCalls(t))

	callsBeforeRejectedRequests := fixture.clientCalls(t)
	rejected := desktopA.requestWithAuth(
		t,
		http.MethodPost,
		"/__desktop/mcp-integrations/codex/configure",
		`{"replace":false}`,
		"",
		desktopA.origin,
	)
	require.Equal(t, http.StatusUnauthorized, rejected.StatusCode)
	_ = readResponseBody(t, rejected)
	rejected = desktopA.requestWithAuth(
		t,
		http.MethodPost,
		"/__desktop/mcp-integrations/codex/configure",
		`{"replace":false}`,
		desktopA.token,
		"http://127.0.0.1:9",
	)
	require.Equal(t, http.StatusForbidden, rejected.StatusCode)
	_ = readResponseBody(t, rejected)
	require.Equal(t, callsBeforeRejectedRequests, fixture.clientCalls(t))

	configured := desktopA.request(
		t,
		http.MethodPost,
		"/__desktop/mcp-integrations/codex/configure",
		`{"replace":false}`,
	)
	configuredBody := readResponseBody(t, configured)
	require.Equal(
		t,
		http.StatusOK,
		configured.StatusCode,
		"body=%s calls=%#v",
		configuredBody,
		fixture.clientCalls(t),
	)
	require.Contains(t, configuredBody, `"client":"codex","status":"configured"`)
	require.NotContains(t, configuredBody, fixture.secret)

	desiredCommand, desiredArgs := fixture.configuredCommand(t)
	desktopA.stop(t)
	readOnlySDK := fixture.startSDKProcess(t, desktopA, desiredCommand, desiredArgs)
	readOnlySDK.requireToolCount(t, 51)
	readOnlySDK.close(t)

	desktopB := fixture.startDesktop(t)
	settings = desktopB.request(
		t,
		http.MethodPut,
		"/__desktop/mcp-settings",
		`{"enabled":true,"allowWrites":true,"confirmWrites":true}`,
	)
	require.Equal(t, http.StatusOK, settings.StatusCode)
	require.JSONEq(t, `{"enabled":true,"allowWrites":true}`, readResponseBody(t, settings))
	desktopB.stop(t)

	writeSDK := fixture.startSDKProcess(t, desktopB, desiredCommand, desiredArgs)
	writeSDK.requireToolCount(t, 84)

	desktopC := fixture.startDesktop(t)
	settings = desktopC.request(
		t,
		http.MethodPut,
		"/__desktop/mcp-settings",
		`{"enabled":true,"allowWrites":false,"confirmWrites":false}`,
	)
	require.Equal(t, http.StatusOK, settings.StatusCode)
	require.JSONEq(t, `{"enabled":true,"allowWrites":false}`, readResponseBody(t, settings))
	require.False(t, fixture.loadPolicy(t).AllowWrites)

	callsBeforeStaleWrite := fixture.clientCalls(t)
	writeSDK.requireWriteDisabled(t)
	require.Equal(t, callsBeforeStaleWrite, fixture.clientCalls(t))

	writeSDK.close(t)
	desktopC.stop(t)
	require.NotContains(t, desktopA.stderr.String(), fixture.secret)
	require.NotContains(t, desktopB.stderr.String(), fixture.secret)
	require.NotContains(t, desktopC.stderr.String(), fixture.secret)
	require.Empty(t, fixture.hostileCalls(t))
	fixture.requireAllPathsTestOwned(t)
}

func TestDesktopSettingsProductionClientDiscoveryUsesOnlyTempHomeFallback(t *testing.T) {
	fixture := newDesktopSettingsProcessFixture(t)
	locator := mcpclient.NewLocator(
		[]string{
			"PATH=",
			"HOME=" + fixture.home,
			"NVM_BIN=" + filepath.Join(
				fixture.home,
				".nvm",
				"versions",
				"node",
				"v22-test",
				"bin",
			),
			"VOLTA_HOME=" + filepath.Join(fixture.home, ".volta"),
			"PNPM_HOME=" + filepath.Join(fixture.home, ".pnpm"),
			"BUN_INSTALL=" + filepath.Join(fixture.home, ".bun"),
		},
		fixture.home,
		runtime.GOOS,
	)

	for client, name := range map[mcpclient.Client]string{
		mcpclient.ClientCodex:      "codex",
		mcpclient.ClientClaudeCode: "claude",
	} {
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		got, err := locator.Find(client)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(fixture.clientDirectory, name), got)
	}

	windowsPrograms := filepath.Join(fixture.home, "AppData", "Local", "Programs")
	require.NoError(t, os.MkdirAll(windowsPrograms, 0o700))
	codexValue, err := os.ReadFile(filepath.Join(fixture.clientDirectory, "codex"))
	if runtime.GOOS == "windows" {
		codexValue, err = os.ReadFile(filepath.Join(fixture.clientDirectory, "codex.exe"))
	}
	require.NoError(t, err)
	windowsCodex := filepath.Join(windowsPrograms, "codex.exe")
	require.NoError(t, os.WriteFile(windowsCodex, codexValue, 0o700))
	windowsLocator := mcpclient.NewLocator(
		[]string{
			"PATH=",
			"LOCALAPPDATA=" + filepath.Join(fixture.home, "AppData", "Local"),
			"APPDATA=" + filepath.Join(fixture.home, "AppData", "Roaming"),
			"USERPROFILE=" + fixture.home,
		},
		fixture.home,
		"windows",
	)
	got, err := windowsLocator.Find(mcpclient.ClientCodex)
	require.NoError(t, err)
	require.Equal(t, windowsCodex, got)
}

type desktopSettingsProcessFixture struct {
	root             string
	home             string
	clientDirectory  string
	binary           string
	configPath       string
	policyPath       string
	clientCallsPath  string
	clientStatePath  string
	hostileCallsPath string
	secret           string
	environment      []string
}

type desktopSettingsClientCall struct {
	Executable string   `json:"executable"`
	Args       []string `json:"args"`
}

type desktopSettingsProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  bytes.Buffer
	client  *http.Client
	origin  string
	token   string
	stopped bool
}

type desktopSettingsSDKProcess struct {
	command *exec.Cmd
	session *mcp.ClientSession
	stderr  bytes.Buffer
	closed  bool
}

func newDesktopSettingsProcessFixture(t *testing.T) *desktopSettingsProcessFixture {
	t.Helper()
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	require.NoError(t, err)
	home := filepath.Join(root, "home")
	clientDirectory := filepath.Join(home, ".local", "bin")
	tempDirectory := filepath.Join(root, "tmp")
	binaryDirectory := filepath.Join(root, "bin")
	for _, directory := range []string{
		home,
		clientDirectory,
		tempDirectory,
		binaryDirectory,
		filepath.Join(home, "AppData", "Roaming"),
		filepath.Join(home, "AppData", "Local"),
		filepath.Join(home, ".nvm", "versions", "node", "v22-test", "bin"),
	} {
		require.NoError(t, os.MkdirAll(directory, 0o700))
	}

	executableSuffix := ""
	if runtime.GOOS == "windows" {
		executableSuffix = ".exe"
	}
	fixture := &desktopSettingsProcessFixture{
		root:             root,
		home:             home,
		clientDirectory:  clientDirectory,
		binary:           filepath.Join(binaryDirectory, "cy-kaf-client"+executableSuffix),
		configPath:       filepath.Join(root, "config.yaml"),
		policyPath:       filepath.Join(root, "mcp-policy.json"),
		clientCallsPath:  filepath.Join(root, "client-calls.jsonl"),
		clientStatePath:  filepath.Join(root, "codex-configured"),
		hostileCallsPath: filepath.Join(root, "hostile-calls.jsonl"),
		secret:           "synthetic-desktop-secret-marker",
	}
	require.NoError(
		t,
		os.WriteFile(fixture.configPath, []byte("kafka:\n  clusters: []\n"), 0o600),
	)
	fixture.buildBinary(t)
	fixture.buildFakeClients(t, executableSuffix)
	fixture.writeHostileClaudeConfig(t)
	fixture.environment = []string{
		"PATH=",
		"HOME=" + home,
		"USERPROFILE=" + home,
		"APPDATA=" + filepath.Join(home, "AppData", "Roaming"),
		"LOCALAPPDATA=" + filepath.Join(home, "AppData", "Local"),
		"TMPDIR=" + tempDirectory,
		"TMP=" + tempDirectory,
		"TEMP=" + tempDirectory,
		"FAKE_CLIENT_CALLS=" + fixture.clientCallsPath,
		"FAKE_CLIENT_STATE=" + fixture.clientStatePath,
		"FAKE_HOSTILE_CALLS=" + fixture.hostileCallsPath,
		"FAKE_DESIRED_COMMAND=" + fixture.binary,
		"FAKE_CONFIG_PATH=" + fixture.configPath,
		"FAKE_SECRET=" + fixture.secret,
	}
	if systemRoot := os.Getenv("SystemRoot"); systemRoot != "" {
		fixture.environment = append(fixture.environment, "SystemRoot="+systemRoot)
	}
	return fixture
}

func (fixture *desktopSettingsProcessFixture) buildBinary(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", fixture.binary, ".")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}

func (fixture *desktopSettingsProcessFixture) buildFakeClients(
	t *testing.T,
	executableSuffix string,
) {
	t.Helper()
	sourceDirectory := filepath.Join(fixture.root, "fake-client-source")
	require.NoError(t, os.MkdirAll(sourceDirectory, 0o700))
	sourcePath := filepath.Join(sourceDirectory, "main.go")
	require.NoError(t, os.WriteFile(sourcePath, []byte(desktopSettingsFakeClientSource), 0o600))

	codexPath := filepath.Join(fixture.clientDirectory, "codex"+executableSuffix)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", codexPath, sourcePath)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	value, err := os.ReadFile(codexPath)
	require.NoError(t, err)
	for _, name := range []string{"claude", "hostile-command"} {
		require.NoError(
			t,
			os.WriteFile(
				filepath.Join(fixture.clientDirectory, name+executableSuffix),
				value,
				0o700,
			),
		)
	}
}

func (fixture *desktopSettingsProcessFixture) writeHostileClaudeConfig(t *testing.T) {
	t.Helper()
	hostilePath := filepath.Join(fixture.clientDirectory, "hostile-command")
	if runtime.GOOS == "windows" {
		hostilePath += ".exe"
	}
	value, err := json.Marshal(map[string]any{
		"mcpServers": map[string]any{
			mcpclient.ServerName: map[string]any{
				"type":    "stdio",
				"command": hostilePath,
				"args":    []string{"--synthetic-secret", fixture.secret},
				"env":     map[string]string{"SYNTHETIC_TOKEN": fixture.secret},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(
		t,
		os.WriteFile(filepath.Join(fixture.home, ".claude.json"), value, 0o600),
	)
}

func (fixture *desktopSettingsProcessFixture) startDesktop(
	t *testing.T,
) *desktopSettingsProcess {
	t.Helper()
	process := &desktopSettingsProcess{
		client: &http.Client{Timeout: 3 * time.Second},
		token:  desktopTestSessionToken(),
	}
	process.command = exec.Command(
		fixture.binary,
		"--desktop",
		"--port",
		"0",
		"--config",
		fixture.configPath,
	)
	process.command.Env = append([]string(nil), fixture.environment...)
	var err error
	process.stdin, err = process.command.StdinPipe()
	require.NoError(t, err)
	process.stdout, err = process.command.StdoutPipe()
	require.NoError(t, err)
	process.command.Stderr = &process.stderr
	require.NoError(t, process.command.Start())
	t.Cleanup(func() {
		if process.command.Process == nil || process.command.ProcessState != nil {
			return
		}
		_ = process.command.Process.Kill()
		_ = process.command.Wait()
	})

	_, err = fmt.Fprintf(
		process.stdin,
		"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":%q}\n",
		process.token,
	)
	require.NoError(t, err)
	process.origin = readDesktopReadyForTest(t, process.stdout).Origin
	return process
}

func (process *desktopSettingsProcess) request(
	t *testing.T,
	method string,
	path string,
	body string,
) *http.Response {
	t.Helper()
	origin := ""
	if method != http.MethodGet {
		origin = process.origin
	}
	return process.requestWithAuth(t, method, path, body, process.token, origin)
}

func (process *desktopSettingsProcess) requestWithAuth(
	t *testing.T,
	method string,
	path string,
	body string,
	token string,
	origin string,
) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, process.origin+path, strings.NewReader(body))
	require.NoError(t, err)
	if token != "" {
		request.AddCookie(&http.Cookie{Name: api.DesktopSessionCookieName, Value: token})
	}
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	response, err := process.client.Do(request)
	require.NoError(t, err)
	return response
}

func (process *desktopSettingsProcess) stop(t *testing.T) {
	t.Helper()
	if process.stopped {
		return
	}
	response := process.request(
		t,
		http.MethodPost,
		"/__desktop/shutdown",
		"",
	)
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.Empty(t, readResponseBody(t, response))
	require.NoError(t, process.stdin.Close())

	waitResult := make(chan error, 1)
	go func() { waitResult <- process.command.Wait() }()
	select {
	case err := <-waitResult:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("desktop process did not stop")
	}
	process.stopped = true
}

func (fixture *desktopSettingsProcessFixture) clientCalls(
	t *testing.T,
) []desktopSettingsClientCall {
	t.Helper()
	return fixture.readCalls(t, fixture.clientCallsPath)
}

func (fixture *desktopSettingsProcessFixture) hostileCalls(
	t *testing.T,
) []desktopSettingsClientCall {
	t.Helper()
	return fixture.readCalls(t, fixture.hostileCallsPath)
}

func (fixture *desktopSettingsProcessFixture) readCalls(
	t *testing.T,
	path string,
) []desktopSettingsClientCall {
	t.Helper()
	value, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	lines := bytes.Split(bytes.TrimSpace(value), []byte("\n"))
	calls := make([]desktopSettingsClientCall, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var call desktopSettingsClientCall
		require.NoError(t, json.Unmarshal(line, &call))
		calls = append(calls, call)
	}
	return calls
}

func (fixture *desktopSettingsProcessFixture) configuredCommand(
	t *testing.T,
) (string, []string) {
	t.Helper()
	wantAdd := []string{
		"mcp",
		"add",
		mcpclient.ServerName,
		"--",
		fixture.binary,
		"mcp",
		"--config",
		fixture.configPath,
	}
	wantGet := []string{"mcp", "get", "--json", mcpclient.ServerName}
	calls := fixture.clientCalls(t)
	require.Equal(t, []desktopSettingsClientCall{
		{Executable: "codex", Args: wantGet},
		{Executable: "codex", Args: wantGet},
		{Executable: "codex", Args: wantAdd},
		{Executable: "codex", Args: wantGet},
	}, calls)
	return wantAdd[4], append([]string(nil), wantAdd[5:]...)
}

func (fixture *desktopSettingsProcessFixture) startSDKProcess(
	t *testing.T,
	desktop *desktopSettingsProcess,
	commandPath string,
	args []string,
) *desktopSettingsSDKProcess {
	t.Helper()
	require.True(t, desktop.stopped, "desktop HTTP process must stop before SDK process starts")
	process := &desktopSettingsSDKProcess{}
	process.command = exec.Command(commandPath, args...)
	process.command.Env = append([]string(nil), fixture.environment...)
	process.command.Stderr = &process.stderr
	transport := &mcp.CommandTransport{
		Command:           process.command,
		TerminateDuration: mcpProcessTerminateTimeout,
	}
	client := mcp.NewClient(
		&mcp.Implementation{Name: "desktop-settings-test", Version: "test"},
		nil,
	)
	ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
	defer cancel()
	var err error
	process.session, err = client.Connect(ctx, transport, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if process.closed {
			return
		}
		_ = process.session.Close()
	})
	return process
}

func (process *desktopSettingsSDKProcess) requireToolCount(t *testing.T, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
	defer cancel()
	tools, err := process.session.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Len(t, tools.Tools, want)
}

func (process *desktopSettingsSDKProcess) requireWriteDisabled(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
	defer cancel()
	result, err := process.session.CallTool(ctx, &mcp.CallToolParams{
		Name: "createTopic",
		Arguments: map[string]any{
			"clusterName": "synthetic",
			"body": map[string]any{
				"name":              "never-created",
				"partitions":        float64(1),
				"replicationFactor": float64(1),
			},
		},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Len(t, result.Content, 1)
	text, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok, "content has type %T", result.Content[0])
	require.Equal(t, "writes_disabled", strings.TrimSpace(text.Text))
}

func (process *desktopSettingsSDKProcess) close(t *testing.T) {
	t.Helper()
	if process.closed {
		return
	}
	closeMCPProcessNormally(t, process.session, process.command)
	require.Empty(t, process.stderr.String())
	process.closed = true
}

func (fixture *desktopSettingsProcessFixture) loadPolicy(t *testing.T) mcppolicy.Policy {
	t.Helper()
	require.Equal(t, fixture.policyPath, mcppolicy.PathForConfig(fixture.configPath))
	policy, err := mcppolicy.NewStore(fixture.policyPath).Load()
	require.NoError(t, err)
	return policy
}

func (fixture *desktopSettingsProcessFixture) requireAllPathsTestOwned(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		fixture.home,
		fixture.clientDirectory,
		fixture.binary,
		fixture.configPath,
		fixture.policyPath,
		fixture.clientCallsPath,
		fixture.clientStatePath,
		fixture.hostileCallsPath,
	} {
		relative, err := filepath.Rel(fixture.root, path)
		require.NoError(t, err)
		require.NotEqual(t, "..", relative)
		require.False(t, strings.HasPrefix(relative, ".."+string(filepath.Separator)))
	}
	for _, call := range fixture.clientCalls(t) {
		for _, argument := range call.Args {
			if !filepath.IsAbs(argument) {
				continue
			}
			relative, err := filepath.Rel(fixture.root, argument)
			require.NoError(t, err)
			require.False(t, strings.HasPrefix(relative, ".."+string(filepath.Separator)))
		}
	}
}

const desktopSettingsFakeClientSource = `package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type call struct {
	Executable string   ` + "`json:\"executable\"`" + `
	Args       []string ` + "`json:\"args\"`" + `
}

func main() {
	executable := strings.TrimSuffix(filepath.Base(os.Args[0]), ".exe")
	record(os.Getenv("FAKE_CLIENT_CALLS"), call{
		Executable: executable,
		Args: append([]string(nil), os.Args[1:]...),
	})
	if executable == "hostile-command" {
		record(os.Getenv("FAKE_HOSTILE_CALLS"), call{
			Executable: executable,
			Args: append([]string(nil), os.Args[1:]...),
		})
		os.Exit(97)
	}
	if executable == "claude" {
		fmt.Fprintln(os.Stderr, os.Getenv("FAKE_SECRET"))
		os.Exit(96)
	}
	if executable != "codex" {
		os.Exit(95)
	}

	args := os.Args[1:]
	codexConfigPath := filepath.Join(os.Getenv("CODEX_HOME"), "config.toml")
	if equal(args, []string{"mcp", "get", "--json", "cy-kaf-client"}) {
		if _, err := os.Stat(codexConfigPath); err != nil {
			fmt.Fprintln(os.Stderr, "Error: No MCP server named 'cy-kaf-client' found.")
			os.Exit(1)
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"name": "cy-kaf-client",
			"enabled": true,
			"transport": map[string]any{
				"type": "stdio",
				"command": os.Getenv("FAKE_DESIRED_COMMAND"),
				"args": []string{"mcp", "--config", os.Getenv("FAKE_CONFIG_PATH")},
				"env": map[string]string{},
				"env_vars": []string{},
				"cwd": nil,
			},
		})
		return
	}
	wantAdd := []string{
		"mcp", "add", "cy-kaf-client", "--",
		os.Getenv("FAKE_DESIRED_COMMAND"),
		"mcp", "--config", os.Getenv("FAKE_CONFIG_PATH"),
	}
	if equal(args, wantAdd) {
		if err := os.WriteFile(codexConfigPath, []byte("configured"), 0600); err != nil {
			os.Exit(94)
		}
		fmt.Fprintln(os.Stdout, os.Getenv("FAKE_SECRET"))
		fmt.Fprintln(os.Stderr, os.Getenv("FAKE_SECRET"))
		return
	}
	os.Exit(93)
}

func record(path string, value call) {
	if path == "" {
		return
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(92)
	}
	defer file.Close()
	if json.NewEncoder(file).Encode(value) != nil {
		os.Exit(91)
	}
}

func equal(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
`

func TestProductionConfiguratorRejectsUnavailableOrEmptyUserHome(t *testing.T) {
	cases := []struct {
		name string
		home string
		err  error
	}{
		{name: "lookup failure", err: errors.New("sensitive home lookup failure")},
		{name: "empty home"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			originalUserHomeDir := productionUserHomeDir
			productionUserHomeDir = func() (string, error) { return tc.home, tc.err }
			t.Cleanup(func() { productionUserHomeDir = originalUserHomeDir })

			configurator, err := productionConfigurator()

			require.Nil(t, configurator)
			require.EqualError(t, err, "resolve user home")
			require.NotContains(t, err.Error(), "sensitive")
		})
	}
}

func TestRunDesktopSanitizesConfiguratorConstructionFailure(t *testing.T) {
	originalFactory := productionConfiguratorFactory
	productionConfiguratorFactory = func() (mcpclient.Configurator, error) {
		return nil, errors.New("sensitive configurator construction failure")
	}
	t.Cleanup(func() { productionConfiguratorFactory = originalFactory })

	var output bytes.Buffer
	err := run(context.Background(), cliOptions{
		Port:           0,
		PortExplicit:   true,
		ConfigPath:     writeEmptyConfig(t),
		ConfigExplicit: true,
		Desktop:        true,
	}, strings.NewReader(fmt.Sprintf(
		"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":%q}\n",
		desktopTestSessionToken(),
	)), &output, runtimeDeps{Listen: net.Listen})

	require.Error(t, err)
	require.Contains(t, output.String(), `"code":"START_FAILED"`)
	require.NotContains(t, output.String(), "sensitive")
}

func TestDesktopPolicyStoreAdapterMapsPersistenceVersion(t *testing.T) {
	persisted := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	adapter := desktopPolicyStoreAdapter{store: persisted}

	got, err := adapter.Load()
	require.NoError(t, err)
	require.Equal(t, mcpsettings.Policy{}, got)

	err = adapter.Save(context.Background(), mcpsettings.Policy{
		Enabled: true, AllowWrites: true,
	})
	require.NoError(t, err)
	raw, err := persisted.Load()
	require.NoError(t, err)
	require.Equal(t, mcppolicy.Policy{
		Version: mcppolicy.CurrentVersion, Enabled: true, AllowWrites: true,
	}, raw)
}

func TestDesktopConfiguratorAdapterMapsEveryKnownClientAndStatus(t *testing.T) {
	clients := []struct {
		infra mcpclient.Client
		app   mcpsettings.Client
	}{
		{infra: mcpclient.ClientCodex, app: mcpsettings.ClientCodex},
		{infra: mcpclient.ClientClaudeCode, app: mcpsettings.ClientClaudeCode},
	}
	statuses := []struct {
		infra mcpclient.Status
		app   mcpsettings.Status
	}{
		{infra: mcpclient.StatusClientNotFound, app: mcpsettings.StatusClientNotFound},
		{infra: mcpclient.StatusNotConfigured, app: mcpsettings.StatusNotConfigured},
		{infra: mcpclient.StatusConfigured, app: mcpsettings.StatusConfigured},
		{infra: mcpclient.StatusConflict, app: mcpsettings.StatusConflict},
		{infra: mcpclient.StatusRepairRequired, app: mcpsettings.StatusRepairRequired},
		{infra: mcpclient.StatusError, app: mcpsettings.StatusError},
	}
	desired := mcpsettings.DesiredServer{
		Name: mcpsettings.ServerName, Command: "/test-app", Args: []string{"mcp"},
	}

	for _, client := range clients {
		for _, status := range statuses {
			fake := &adapterTestConfigurator{statusResult: mcpclient.Integration{
				Client: client.infra, Status: status.infra,
				CanConfigure: true, ManualCommand: "fixed command", Message: "hidden",
			}}
			adapter := desktopConfiguratorAdapter{configurator: fake}

			got, err := adapter.Status(context.Background(), client.app, desired)

			require.NoError(t, err)
			require.Equal(t, mcpsettings.Integration{
				Client: client.app, Status: status.app,
				CanConfigure: true, ManualCommand: "fixed command", Message: "hidden",
			}, got)
			require.Equal(t, client.infra, fake.client)
			require.Equal(t, mcpclient.DesiredServer{
				Name: mcpclient.ServerName, Command: "/test-app", Args: []string{"mcp"},
			}, fake.desired)
		}
	}
}

func TestDesktopConfiguratorAdapterRejectsUnknownClientsStatusesAndErrors(t *testing.T) {
	desired := mcpsettings.DesiredServer{
		Name: mcpsettings.ServerName, Command: "/test-app", Args: []string{"mcp"},
	}
	cases := []struct {
		name   string
		client mcpsettings.Client
		result mcpclient.Integration
		err    error
	}{
		{
			name: "unknown app client", client: mcpsettings.Client("future-client"),
			result: mcpclient.Integration{
				Client: mcpclient.ClientCodex, Status: mcpclient.StatusConfigured,
			},
		},
		{
			name: "unknown infra client", client: mcpsettings.ClientCodex,
			result: mcpclient.Integration{
				Client: mcpclient.Client("future-client"), Status: mcpclient.StatusConfigured,
			},
		},
		{
			name: "unknown infra status", client: mcpsettings.ClientCodex,
			result: mcpclient.Integration{
				Client: mcpclient.ClientCodex, Status: mcpclient.Status("future-status"),
			},
		},
		{
			name: "unknown infra error", client: mcpsettings.ClientCodex,
			err: errors.New("sensitive CLI output"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &adapterTestConfigurator{statusResult: tc.result, statusErr: tc.err}
			adapter := desktopConfiguratorAdapter{configurator: fake}

			got, err := adapter.Status(context.Background(), tc.client, desired)

			require.Empty(t, got)
			require.ErrorIs(t, err, mcpsettings.ErrIntegration)
			require.NotContains(t, err.Error(), "sensitive")
		})
	}
}

func TestDesktopConfiguratorAdapterMapsEveryWrappedKnownError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want error
	}{
		{name: "client not found", err: mcpclient.ErrClientNotFound, want: mcpsettings.ErrClientNotFound},
		{
			name: "concurrent modification", err: mcpclient.ErrConcurrentModification,
			want: mcpsettings.ErrConcurrentModification,
		},
		{name: "rollback failed", err: mcpclient.ErrRollbackFailed, want: mcpsettings.ErrRollbackFailed},
		{name: "verification failed", err: mcpclient.ErrVerification, want: mcpsettings.ErrVerification},
		{name: "client command", err: mcpclient.ErrClientCommand, want: mcpsettings.ErrClientCommand},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := translateMCPClientError(errors.Join(tc.err, errors.New("sensitive CLI output")))
			require.ErrorIs(t, got, tc.want)
			require.NotContains(t, got.Error(), "sensitive")
		})
	}
	require.NoError(t, translateMCPClientError(nil))
}

func TestDesktopDesiredServerConversionsCloneArgsInBothDirections(t *testing.T) {
	infra := mcpclient.DesiredServer{
		Name: mcpclient.ServerName, Command: "/infra", Args: []string{"mcp", "--config", "/one"},
	}
	app := toDesktopDesiredServer(infra)
	infra.Args[2] = "/mutated-infra"
	require.Equal(t, []string{"mcp", "--config", "/one"}, app.Args)
	app.Args[2] = "/mutated-app"
	require.Equal(t, []string{"mcp", "--config", "/mutated-infra"}, infra.Args)

	app = mcpsettings.DesiredServer{
		Name: mcpsettings.ServerName, Command: "/app", Args: []string{"mcp", "--config", "/two"},
	}
	infra = toInfraDesiredServer(app)
	app.Args[2] = "/mutated-app"
	require.Equal(t, []string{"mcp", "--config", "/two"}, infra.Args)
	infra.Args[2] = "/mutated-infra"
	require.Equal(t, []string{"mcp", "--config", "/mutated-app"}, app.Args)
}

type adapterTestConfigurator struct {
	statusResult mcpclient.Integration
	statusErr    error
	client       mcpclient.Client
	desired      mcpclient.DesiredServer
}

func (fake *adapterTestConfigurator) Status(
	_ context.Context,
	client mcpclient.Client,
	desired mcpclient.DesiredServer,
) (mcpclient.Integration, error) {
	fake.client = client
	fake.desired = desired
	return fake.statusResult, fake.statusErr
}

func (fake *adapterTestConfigurator) Configure(
	context.Context,
	mcpclient.Client,
	mcpclient.DesiredServer,
	bool,
) (mcpclient.Integration, error) {
	return mcpclient.Integration{}, nil
}

type recordingDesktopMCPConfigurator struct {
	mu             sync.Mutex
	statusCalls    int
	configureCalls int
	desired        mcpclient.DesiredServer
}

func (recorder *recordingDesktopMCPConfigurator) Status(
	_ context.Context,
	client mcpclient.Client,
	desired mcpclient.DesiredServer,
) (mcpclient.Integration, error) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.statusCalls++
	recorder.desired = desired
	return mcpclient.Integration{
		Client: client, Status: mcpclient.StatusNotConfigured, CanConfigure: true,
	}, nil
}

func (recorder *recordingDesktopMCPConfigurator) Configure(
	_ context.Context,
	client mcpclient.Client,
	desired mcpclient.DesiredServer,
	_ bool,
) (mcpclient.Integration, error) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.configureCalls++
	recorder.desired = desired
	return mcpclient.Integration{Client: client, Status: mcpclient.StatusConfigured}, nil
}

func (recorder *recordingDesktopMCPConfigurator) snapshot() (
	statusCalls int,
	configureCalls int,
	desired mcpclient.DesiredServer,
) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.statusCalls, recorder.configureCalls, recorder.desired
}

func writeEmptyConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("kafka:\n  clusters: []\n"), 0o600))
	return path
}

func desktopTestSessionToken() string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte("0123456789abcdef0123456789abcdef"),
	)
}

func readDesktopReadyForTest(t *testing.T, reader io.Reader) desktopReadyWire {
	t.Helper()
	type result struct {
		line string
		err  error
	}
	got := make(chan result, 1)
	go func() {
		line, err := bufio.NewReader(reader).ReadString('\n')
		got <- result{line: line, err: err}
	}()

	select {
	case result := <-got:
		require.NoError(t, result.err)
		require.True(t, strings.HasPrefix(result.line, desktopReadyPrefix), result.line)
		var wire desktopReadyWire
		require.NoError(
			t,
			json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(result.line), desktopReadyPrefix)), &wire),
		)
		require.Equal(t, desktopProtocolVersion, wire.Protocol)
		return wire
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for CY_KAF_READY")
		return desktopReadyWire{}
	}
}

func doDesktopRequest(
	t *testing.T,
	client *http.Client,
	method string,
	url string,
	token string,
	origin string,
) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	require.NoError(t, err)
	if token != "" {
		req.AddCookie(&http.Cookie{Name: api.DesktopSessionCookieName, Value: token})
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	return resp
}

func readResponseBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer func() { require.NoError(t, response.Body.Close()) }()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return string(body)
}
