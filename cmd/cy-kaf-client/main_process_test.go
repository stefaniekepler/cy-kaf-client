package main

import (
	"bytes"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

const expectedMCPHelp = `Usage: cy-kaf-client mcp [options]

Options:
  --config PATH  Path to the Kafka cluster configuration
  --debug        Enable debug logging on stderr
  --help         Show this help
`

func TestMCPProcessEstablishesSafeOptionsBoundaryBeforeParsing(t *testing.T) {
	binary := buildCLIProcessBinary(t)
	const marker = "RAW-CREDENTIAL-MARKER"

	for _, tt := range []struct {
		name string
		args []string
	}{
		{name: "invalid debug value", args: []string{"mcp", "--debug=" + marker}},
		{name: "unknown option", args: []string{"mcp", "--" + marker}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			exitCode, stdout, stderr := runCLIProcess(t, binary, tt.args...)

			require.Equal(t, 2, exitCode)
			require.Empty(t, stdout)
			require.Contains(t, stderr, "MCP process exited")
			require.Contains(t, stderr, "code=OPTIONS_INVALID")
			require.NotContains(t, stderr, marker)
			require.NotContains(t, stderr, defaultConfigPath())
			require.NotContains(t, stderr, "Usage")
			require.NotContains(t, stderr, "parse error")
			require.NotContains(t, strings.TrimSpace(stderr), "\n")
		})
	}
}

func TestMCPProcessHelpIsStaticEnglishPathFreeAndSuccessful(t *testing.T) {
	binary := buildCLIProcessBinary(t)

	exitCode, stdout, stderr := runCLIProcess(t, binary, "mcp", "--help")

	require.Zero(t, exitCode)
	require.Empty(t, stdout)
	require.Equal(t, expectedMCPHelp, stderr)
	require.NotContains(t, stderr, defaultConfigPath())
	for _, current := range stderr {
		require.False(t, unicode.Is(unicode.Han, current), "help contains non-English rune %q", current)
	}
}

func buildCLIProcessBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "cy-kaf-client")
	command := exec.Command("go", "build", "-o", binary, ".")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	return binary
}

func runCLIProcess(t *testing.T, binary string, args ...string) (int, string, string) {
	t.Helper()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command := exec.Command(binary, args...)
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err == nil {
		return 0, stdout.String(), stderr.String()
	}
	var exitErr *exec.ExitError
	require.True(t, errors.As(err, &exitErr), "run error = %v", err)
	return exitErr.ExitCode(), stdout.String(), stderr.String()
}
