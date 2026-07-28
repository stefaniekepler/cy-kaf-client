package main

import (
	"bytes"
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCommandPreservesLegacyCLIAndSelectsMCP(t *testing.T) {
	legacyArgs := []string{"--desktop", "--port", "0", "--config", "/tmp/legacy.yaml", "--debug"}
	wantLegacy, err := parseOptions(legacyArgs)
	require.NoError(t, err)

	gotLegacy, err := parseCommand(legacyArgs, &bytes.Buffer{})
	require.NoError(t, err)
	require.NotNil(t, gotLegacy.HTTP)
	require.Nil(t, gotLegacy.MCP)
	require.Equal(t, wantLegacy, *gotLegacy.HTTP)

	gotMCP, err := parseCommand([]string{"mcp", "--config", "/tmp/mcp.yaml", "--debug"}, &bytes.Buffer{})
	require.NoError(t, err)
	require.Nil(t, gotMCP.HTTP)
	require.NotNil(t, gotMCP.MCP)
	require.Equal(t, "/tmp/mcp.yaml", gotMCP.MCP.ConfigPath)
	require.True(t, gotMCP.MCP.ConfigExplicit)
	require.True(t, gotMCP.MCP.Debug)
}

func TestParseMCPOptionsDefaultsAndExplicitConfig(t *testing.T) {
	got, err := parseMCPOptions(nil, &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, defaultConfigPath(), got.ConfigPath)
	require.False(t, got.ConfigExplicit)
	require.False(t, got.Debug)

	got, err = parseMCPOptions([]string{"--config", "/tmp/explicit.yaml"}, &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, "/tmp/explicit.yaml", got.ConfigPath)
	require.True(t, got.ConfigExplicit)
}

func TestParseMCPOptionsRejectsHTTPDesktopUnknownAndPositionals(t *testing.T) {
	for _, args := range [][]string{
		{"--port", "9090"},
		{"--desktop"},
		{"--no-browser"},
		{"--unknown"},
		{"cluster-a"},
		{"--debug", "cluster-a"},
	} {
		t.Run(args[0], func(t *testing.T) {
			_, err := parseMCPOptions(args, &bytes.Buffer{})
			require.Error(t, err)
			require.Equal(t, 2, optionsErrorExitCode(err))
		})
	}
}

func TestParseCommandUnknownSubcommandAndBothHelpPaths(t *testing.T) {
	_, err := parseCommand([]string{"unknown-command"}, &bytes.Buffer{})
	require.Error(t, err)
	require.Equal(t, 2, optionsErrorExitCode(err))

	for _, args := range [][]string{{"--help"}, {"mcp", "--help"}} {
		var output bytes.Buffer
		_, err := parseCommand(args, &output)
		require.ErrorIs(t, err, flag.ErrHelp)
		require.Zero(t, optionsErrorExitCode(err))
		require.NotEmpty(t, output.String())
	}
}
