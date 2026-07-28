package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
)

const (
	mcpProcessOperationTimeout = 5 * time.Second
	mcpProcessTerminateTimeout = time.Second
)

var expectedMCPProcessReadOnlyTools = []string{
	"analyzeTopic",
	"cancelTopicAnalysis",
	"checkSchemaCompatibility",
	"executeKsql",
	"executeSmartFilterTest",
	"getAclAsCsv",
	"getActiveProducerStates",
	"getAllBrokersLogdirs",
	"getAllConnectors",
	"getAllConnectorsCsv",
	"getAllVersionsBySubject",
	"getBrokerConfig",
	"getBrokers",
	"getBrokersCsv",
	"getBrokersMetrics",
	"getClusterMetrics",
	"getClusterStats",
	"getClusters",
	"getConnector",
	"getConnectorConfig",
	"getConnectorPlugins",
	"getConnectorTasks",
	"getConnectors",
	"getConnects",
	"getConnectsCsv",
	"getConsumerGroup",
	"getConsumerGroupsCsv",
	"getConsumerGroupsLag",
	"getConsumerGroupsPage",
	"getGlobalSchemaCompatibilityLevel",
	"getLatestSchema",
	"getSchemaByVersion",
	"getSchemas",
	"getSerdes",
	"getTopicAnalysis",
	"getTopicConfigs",
	"getTopicConnectors",
	"getTopicConsumerGroups",
	"getTopicDetails",
	"getTopicMessagesV2",
	"getTopics",
	"getTopicsCsv",
	"listAcls",
	"listQuotas",
	"listStreams",
	"listTables",
	"listTopicAcls",
	"openKsqlResponsePipe",
	"registerFilter",
	"updateClusterInfo",
	"validateConnectorPluginConfig",
}

var expectedMCPProcessWriteTools = []string{
	"changeReplicationFactor",
	"cloneTopic",
	"createAcl",
	"createConnector",
	"createConsumerAcl",
	"createNewSchema",
	"createProducerAcl",
	"createStreamAppAcl",
	"createTopic",
	"deleteAcl",
	"deleteConnector",
	"deleteConsumerGroup",
	"deleteConsumerGroupOffsets",
	"deleteLatestSchema",
	"deleteSchema",
	"deleteSchemaByVersion",
	"deleteTopic",
	"deleteTopicMessages",
	"increaseTopicPartitions",
	"recreateTopic",
	"resetConnectorOffsets",
	"resetConsumerGroupOffsets",
	"restartConnectorTask",
	"sendTopicMessages",
	"setConnectorConfig",
	"syncAclsCsv",
	"updateBrokerConfigByName",
	"updateBrokerTopicPartitionLogDir",
	"updateConnectorState",
	"updateGlobalSchemaCompatibilityLevel",
	"updateSchemaCompatibilityLevel",
	"updateTopic",
	"upsertClientQuotas",
}

func TestMCPProcessProtocolAcceptance(t *testing.T) {
	binary := buildMCPProcessBinary(t)

	t.Run("disabled policy fails initialize before any server starts", func(t *testing.T) {
		configPath := writeMCPProcessFiles(t, false, false)
		command, transport, stderr := newMCPProcessCommand(t, binary, configPath, false)
		client := mcp.NewClient(
			&mcp.Implementation{Name: "cy-kaf-client-test", Version: "1.0.0"},
			nil,
		)
		ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
		defer cancel()

		session, err := client.Connect(ctx, transport, nil)

		require.Error(t, err)
		require.Nil(t, session)
		require.NotErrorIs(t, err, context.DeadlineExceeded)
		require.NotNil(t, command.Process)
		require.NotNil(t, command.ProcessState)
		require.True(t, command.ProcessState.Exited())
		require.Equal(t, 1, command.ProcessState.ExitCode())
		require.Equal(t, []string{
			`level=ERROR msg="MCP process exited" code=POLICY_DISABLED`,
		}, normalizedMCPProcessLogLines(t, stderr.String()))
		require.NotContains(t, stderr.String(), "CY_KAF_")
		require.NotContains(t, strings.ToLower(stderr.String()), "http")
	})

	t.Run("read-only policy exposes the exact frozen catalog", func(t *testing.T) {
		configPath := writeMCPProcessFiles(t, true, false)
		session, command, stderr := connectMCPProcess(t, binary, configPath, false)
		ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
		defer cancel()

		result, err := session.ListTools(ctx, nil)

		require.NoError(t, err)
		require.Len(t, result.Tools, 51)
		require.Equal(t, expectedMCPProcessReadOnlyTools, sortedMCPProcessToolNames(result))
		closeMCPProcessNormally(t, session, command)
		require.Empty(t, stderr.String())
	})

	t.Run("write policy exposes the exact complete catalog", func(t *testing.T) {
		configPath := writeMCPProcessFiles(t, true, true)
		session, command, stderr := connectMCPProcess(t, binary, configPath, false)
		ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
		defer cancel()

		result, err := session.ListTools(ctx, nil)

		require.NoError(t, err)
		require.Len(t, result.Tools, 84)
		require.Equal(t, expectedMCPProcessAllTools(), sortedMCPProcessToolNames(result))
		closeMCPProcessNormally(t, session, command)
		require.Empty(t, stderr.String())
	})

	t.Run("empty config returns a structured empty cluster result", func(t *testing.T) {
		configPath := writeMCPProcessFiles(t, true, false)
		session, command, stderr := connectMCPProcess(t, binary, configPath, false)
		ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
		defer cancel()

		result, err := session.CallTool(ctx, &mcp.CallToolParams{
			Name:      "getClusters",
			Arguments: map[string]any{},
		})

		require.NoError(t, err)
		require.False(t, result.IsError)
		structured, ok := result.StructuredContent.(map[string]any)
		require.True(t, ok, "structured content has type %T", result.StructuredContent)
		clusters, ok := structured["result"].([]any)
		require.True(t, ok, "structured result has type %T", structured["result"])
		require.Empty(t, clusters)
		closeMCPProcessNormally(t, session, command)
		require.Empty(t, stderr.String())
	})

	t.Run("session close lets the child exit without signal fallback", func(t *testing.T) {
		configPath := writeMCPProcessFiles(t, true, false)
		session, command, stderr := connectMCPProcess(t, binary, configPath, false)

		closeMCPProcessNormally(t, session, command)

		require.Empty(t, stderr.String())
	})

	t.Run("debug stderr is allowlisted while the SDK parses protocol stdout", func(t *testing.T) {
		configPath := writeMCPProcessFiles(t, true, false)
		session, command, stderr := connectMCPProcess(t, binary, configPath, true)
		ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
		defer cancel()

		result, err := session.ListTools(ctx, nil)

		require.NoError(t, err)
		require.Len(t, result.Tools, 51)
		require.Equal(t, expectedMCPProcessReadOnlyTools, sortedMCPProcessToolNames(result))
		closeMCPProcessNormally(t, session, command)
		require.Equal(t, []string{
			`level=DEBUG msg="MCP STDIO starting" tools=51`,
		}, normalizedMCPProcessLogLines(t, stderr.String()))
		require.NotContains(t, stderr.String(), configPath)
	})
}

func buildMCPProcessBinary(t *testing.T) string {
	t.Helper()
	name := "cy-kaf-client"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary := filepath.Join(t.TempDir(), name)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	return binary
}

func writeMCPProcessFiles(t *testing.T, enabled, allowWrites bool) string {
	t.Helper()
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("kafka:\n  clusters: []\n"), 0o600))

	policyPath := mcppolicy.PathForConfig(configPath)
	require.Equal(t, filepath.Join(configDir, "mcp-policy.json"), policyPath)
	require.NoError(t, mcppolicy.NewStore(policyPath).Save(t.Context(), mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     enabled,
		AllowWrites: allowWrites,
	}))
	require.NotEqual(t, defaultConfigPath(), configPath)

	if runtime.GOOS != "windows" {
		dirInfo, err := os.Stat(configDir)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
		policyInfo, err := os.Stat(policyPath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), policyInfo.Mode().Perm())
	}
	return configPath
}

func newMCPProcessCommand(
	t *testing.T,
	binary string,
	configPath string,
	debug bool,
) (*exec.Cmd, *mcp.CommandTransport, *bytes.Buffer) {
	t.Helper()
	args := []string{"mcp", "--config", configPath}
	if debug {
		args = append(args, "--debug")
	}
	command := exec.Command(binary, args...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	transport := &mcp.CommandTransport{
		Command:           command,
		TerminateDuration: mcpProcessTerminateTimeout,
	}
	t.Cleanup(func() {
		if command.Process == nil || command.ProcessState != nil {
			return
		}
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	return command, transport, &stderr
}

func connectMCPProcess(
	t *testing.T,
	binary string,
	configPath string,
	debug bool,
) (*mcp.ClientSession, *exec.Cmd, *bytes.Buffer) {
	t.Helper()
	command, transport, stderr := newMCPProcessCommand(t, binary, configPath, debug)
	client := mcp.NewClient(
		&mcp.Implementation{Name: "cy-kaf-client-test", Version: "1.0.0"},
		nil,
	)
	ctx, cancel := context.WithTimeout(t.Context(), mcpProcessOperationTimeout)
	defer cancel()
	session, err := client.Connect(ctx, transport, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = session.Close()
	})
	return session, command, stderr
}

func closeMCPProcessNormally(t *testing.T, session *mcp.ClientSession, command *exec.Cmd) {
	t.Helper()
	started := time.Now()
	require.NoError(t, session.Close())
	require.Less(t, time.Since(started), mcpProcessTerminateTimeout)
	require.NotNil(t, command.ProcessState)
	require.True(t, command.ProcessState.Exited())
	require.True(t, command.ProcessState.Success())
}

func sortedMCPProcessToolNames(result *mcp.ListToolsResult) []string {
	names := make([]string, 0, len(result.Tools))
	for _, tool := range result.Tools {
		names = append(names, tool.Name)
	}
	sort.Strings(names)
	return names
}

func expectedMCPProcessAllTools() []string {
	names := append([]string(nil), expectedMCPProcessReadOnlyTools...)
	names = append(names, expectedMCPProcessWriteTools...)
	sort.Strings(names)
	return names
}

func normalizedMCPProcessLogLines(t *testing.T, raw string) []string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	normalized := make([]string, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))
		require.NotEmpty(t, fields)
		require.True(t, strings.HasPrefix(fields[0], "time="), line)
		normalized = append(normalized, strings.Join(fields[1:], " "))
	}
	return normalized
}
