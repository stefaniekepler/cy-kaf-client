package mcpclient

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const currentCodexEntryJSON = `{
  "name":"cy-kaf-client",
  "enabled":true,
  "disabled_reason":null,
  "transport":{
    "type":"stdio",
    "command":"/Applications/CyKafClient.app/Contents/MacOS/cy-kaf-client",
    "args":["mcp","--config","/tmp/config.yaml"],
    "env":null,
    "env_vars":[],
    "cwd":null
  },
  "enabled_tools":null,
  "disabled_tools":null,
  "startup_timeout_sec":null,
  "tool_timeout_sec":null
}`

func TestParseCodexAcceptsCurrentStrictJSONAndPreservesOrderedArgs(t *testing.T) {
	entry, err := parseCodexEntry([]byte(currentCodexEntryJSON))

	require.NoError(t, err)
	require.Equal(t, Entry{
		Name:      ServerName,
		Transport: "stdio",
		Enabled:   true,
		Command:   "/Applications/CyKafClient.app/Contents/MacOS/cy-kaf-client",
		Args:      []string{"mcp", "--config", "/tmp/config.yaml"},
		Env:       nil,
		EnvVars:   []string{},
		CWD:       "",
	}, entry)
}

func TestParseCodexRejectsWrongIdentityTransportShapeUnknownFieldsAndTrailingJSON(t *testing.T) {
	valid := currentCodexEntryJSON
	cases := map[string]string{
		"wrong name":        strings.Replace(valid, `"cy-kaf-client"`, `"other-server"`, 1),
		"non stdio":         strings.Replace(valid, `"type":"stdio"`, `"type":"sse"`, 1),
		"unknown top field": strings.Replace(valid, `"tool_timeout_sec":null`, `"tool_timeout_sec":null,"secret":true`, 1),
		"unknown transport": strings.Replace(valid, `"cwd":null`, `"cwd":null,"headers":{}`, 1),
		"case variant name": strings.Replace(valid, `"name":"cy-kaf-client"`, `"Name":"cy-kaf-client"`, 1),
		"duplicate name": strings.Replace(
			valid,
			`"name":"cy-kaf-client",`,
			`"name":"cy-kaf-client","name":"cy-kaf-client",`,
			1,
		),
		"duplicate command": strings.Replace(
			valid,
			`"command":"/Applications/CyKafClient.app/Contents/MacOS/cy-kaf-client",`,
			`"command":"/Applications/CyKafClient.app/Contents/MacOS/cy-kaf-client","command":"/other",`,
			1,
		),
		"duplicate env key": strings.Replace(
			valid,
			`"env":null`,
			`"env":{"TOKEN":"first","TOKEN":"second"}`,
			1,
		),
		"missing type":      strings.Replace(valid, `"type":"stdio",`, "", 1),
		"missing command":   strings.Replace(valid, `"command":"/Applications/CyKafClient.app/Contents/MacOS/cy-kaf-client",`, "", 1),
		"missing args":      strings.Replace(valid, `"args":["mcp","--config","/tmp/config.yaml"],`, "", 1),
		"missing env":       strings.Replace(valid, `"env":null,`, "", 1),
		"missing env vars":  strings.Replace(valid, `"env_vars":[],`, "", 1),
		"missing cwd":       strings.Replace(valid, ",\n    "+`"cwd":null`, "", 1),
		"null args":         strings.Replace(valid, `"args":["mcp","--config","/tmp/config.yaml"]`, `"args":null`, 1),
		"null env vars":     strings.Replace(valid, `"env_vars":[]`, `"env_vars":null`, 1),
		"missing name":      strings.Replace(valid, `"name":"cy-kaf-client",`, "", 1),
		"missing enabled":   strings.Replace(valid, `"enabled":true,`, "", 1),
		"null enabled":      strings.Replace(valid, `"enabled":true`, `"enabled":null`, 1),
		"missing transport": strings.Replace(valid, `"transport":{`, `"ignored_transport":{`, 1),
		"trailing JSON":     valid + `{"token":"must-not-be-parsed"}`,
		"non string env":    strings.Replace(valid, `"env":null`, `"env":{"TOKEN":17}`, 1),
		"non string cwd":    strings.Replace(valid, `"cwd":null`, `"cwd":17`, 1),
		"non string argument": strings.Replace(
			valid,
			`"args":["mcp","--config","/tmp/config.yaml"]`,
			`"args":["mcp",17]`,
			1,
		),
	}

	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			entry, err := parseCodexEntry([]byte(input))
			require.ErrorIs(t, err, ErrClientCommand)
			require.Equal(t, Entry{}, entry)
		})
	}
}

func TestParseCodexAcceptsTypedTransportEnvironmentAndDisabledEntry(t *testing.T) {
	input := strings.NewReplacer(
		`"enabled":true`,
		`"enabled":false`,
		`"disabled_reason":null`,
		`"disabled_reason":"disabled by user"`,
		`"env":null`,
		`"env":{"SAFE_NAME":"literal value"}`,
		`"env_vars":[]`,
		`"env_vars":["PATH","HOME"]`,
		`"cwd":null`,
		`"cwd":"/tmp/work area"`,
	).Replace(currentCodexEntryJSON)

	entry, err := parseCodexEntry([]byte(input))

	require.NoError(t, err)
	require.False(t, entry.Enabled)
	require.Equal(t, map[string]string{"SAFE_NAME": "literal value"}, entry.Env)
	require.Equal(t, []string{"PATH", "HOME"}, entry.EnvVars)
	require.Equal(t, "/tmp/work area", entry.CWD)
}

func TestCodexInspectUsesOnlyExactGetArgsAndResolvedExecutable(t *testing.T) {
	runner := &recordingRunner{
		result: CommandResult{
			ExitCode: 0,
			Stdout:   []byte(currentCodexEntryJSON),
		},
	}
	inspector := NewCodexInspector(runner)

	entry, found, err := inspector.Get(context.Background(), "/resolved/bin/codex")

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ServerName, entry.Name)
	require.Equal(t, []runnerInvocation{{
		Executable: "/resolved/bin/codex",
		Args:       []string{"mcp", "get", "--json", "cy-kaf-client"},
	}}, runner.invocations)
}

func TestNoClaudeQueryCanBeSelectedOrLaunchedByCodexInspector(t *testing.T) {
	conflictingStoredCommand := strings.Replace(
		currentCodexEntryJSON,
		`"/Applications/CyKafClient.app/Contents/MacOS/cy-kaf-client"`,
		`"claude mcp get should-never-run"`,
		1,
	)
	runner := &recordingRunner{
		result: CommandResult{Stdout: []byte(conflictingStoredCommand)},
	}

	entry, found, err := NewCodexInspector(runner).Get(
		context.Background(),
		"/resolved/bin/codex",
	)

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "claude mcp get should-never-run", entry.Command)
	require.Equal(t, []runnerInvocation{{
		Executable: "/resolved/bin/codex",
		Args:       CodexGetArgs(),
	}}, runner.invocations)
}

func TestCodexInspectMapsOnlyExactKnownNotFoundResult(t *testing.T) {
	runner := &recordingRunner{
		result: CommandResult{
			ExitCode: 1,
			Stderr:   []byte("Error: No MCP server named 'cy-kaf-client' found.\n"),
		},
		err: &CommandError{Code: CommandExitNonZero},
	}

	entry, found, err := NewCodexInspector(runner).Get(context.Background(), "/resolved/bin/codex")

	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, Entry{}, entry)
}

func TestCodexInspectSanitizesAllOtherCommandAndParseFailures(t *testing.T) {
	const secret = "token=codex-inspector-secret"
	cases := map[string]*recordingRunner{
		"other nonzero": {
			result: CommandResult{
				ExitCode: 2,
				Stdout:   []byte(secret),
				Stderr:   []byte("/private/config/path: permission denied"),
			},
			err: errors.New("runner leaked " + secret),
		},
		"deceptive not found stdout": {
			result: CommandResult{
				ExitCode: 1,
				Stdout:   []byte(secret),
				Stderr:   []byte("Error: No MCP server named 'cy-kaf-client' found."),
			},
			err: &CommandError{Code: CommandExitNonZero},
		},
		"oversized stdout": {
			result: CommandResult{
				Stdout: []byte(strings.Repeat(secret, (32<<10)/len(secret)+2)),
			},
		},
		"oversized stderr": {
			result: CommandResult{
				Stdout: []byte(currentCodexEntryJSON),
				Stderr: []byte(strings.Repeat(secret, (32<<10)/len(secret)+2)),
			},
		},
		"malformed JSON": {
			result: CommandResult{
				Stdout: []byte(`{"name":"cy-kaf-client","token":"` + secret + `"}`),
			},
		},
		"runner failure on zero exit": {
			result: CommandResult{
				Stdout: []byte(currentCodexEntryJSON),
			},
			err: errors.New("environment " + secret),
		},
	}

	for name, runner := range cases {
		t.Run(name, func(t *testing.T) {
			entry, found, err := NewCodexInspector(runner).Get(
				context.Background(),
				"/private/resolved/codex",
			)

			require.ErrorIs(t, err, ErrClientCommand)
			requireCommandErrorCode(t, err, CommandInvalidResponse)
			require.False(t, found)
			require.Equal(t, Entry{}, entry)
			require.NotContains(t, err.Error(), secret)
			require.NotContains(t, err.Error(), "/private")
			require.NotContains(t, err.Error(), "permission denied")
		})
	}
}

type runnerInvocation struct {
	Executable string
	Args       []string
}

type recordingRunner struct {
	result      CommandResult
	err         error
	invocations []runnerInvocation
}

func (runner *recordingRunner) Run(
	_ context.Context,
	executable string,
	args []string,
) (CommandResult, error) {
	runner.invocations = append(runner.invocations, runnerInvocation{
		Executable: executable,
		Args:       append([]string(nil), args...),
	})
	result := runner.result
	result.Stdout = append([]byte(nil), result.Stdout...)
	result.Stderr = append([]byte(nil), result.Stderr...)
	return result, runner.err
}
