package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const runnerHelperEnvironment = "CY_KAF_MCP_RUNNER_HELPER"

func TestRunnerPreservesLiteralArgvUsesNoShellAndClosesStdin(t *testing.T) {
	executable := runnerHelperExecutable(t)
	sentinel := filepath.Join(t.TempDir(), "should-not-exist")
	literalShellExpression := "$(touch " + sentinel + ")"
	wantArgs := []string{
		"argument with spaces",
		literalShellExpression,
		"semi;colon",
		"$HOME",
		"*.yaml",
		`quote'"pair`,
	}

	oldStdin := os.Stdin
	parentStdin, keepOpen, err := os.Pipe()
	require.NoError(t, err)
	os.Stdin = parentStdin
	t.Cleanup(func() {
		os.Stdin = oldStdin
		require.NoError(t, keepOpen.Close())
		require.NoError(t, parentStdin.Close())
	})

	result, err := NewOSRunner(2*time.Second, 32<<10).Run(
		context.Background(),
		executable,
		runnerHelperArgs("argv", wantArgs...),
	)
	require.NoError(t, err)
	require.Zero(t, result.ExitCode)
	require.Empty(t, result.Stderr)

	var got struct {
		Args  []string `json:"args"`
		Stdin string   `json:"stdin"`
	}
	require.NoError(t, json.Unmarshal(result.Stdout, &got))
	require.Equal(t, wantArgs, got.Args)
	require.Empty(t, got.Stdin)
	_, statErr := os.Stat(sentinel)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRunnerDrainsBothStreamsAndCapsEachIndependently(t *testing.T) {
	const outputLimit = 32 << 10
	executable := runnerHelperExecutable(t)

	result, err := NewOSRunner(3*time.Second, outputLimit).Run(
		context.Background(),
		executable,
		runnerHelperArgs("flood"),
	)

	require.ErrorIs(t, err, ErrClientCommand)
	requireCommandErrorCode(t, err, CommandOutputLimit)
	require.Zero(t, result.ExitCode)
	require.Len(t, result.Stdout, outputLimit)
	require.Len(t, result.Stderr, outputLimit)
	require.Equal(t, strings.Repeat("o", outputLimit), string(result.Stdout))
	require.Equal(t, strings.Repeat("e", outputLimit), string(result.Stderr))
}

func TestRunnerCapturesNonZeroExitWithoutLeakingOutputInError(t *testing.T) {
	const (
		stdoutMarker = "stdout-token=runner-secret"
		stderrMarker = "stderr-token=runner-secret"
	)
	executable := runnerHelperExecutable(t)

	result, err := NewOSRunner(2*time.Second, 32<<10).Run(
		context.Background(),
		executable,
		runnerHelperArgs("exit", "23", stdoutMarker, stderrMarker),
	)

	require.ErrorIs(t, err, ErrClientCommand)
	requireCommandErrorCode(t, err, CommandExitNonZero)
	require.Equal(t, 23, result.ExitCode)
	require.Equal(t, stdoutMarker, string(result.Stdout))
	require.Equal(t, stderrMarker, string(result.Stderr))
	require.NotContains(t, err.Error(), stdoutMarker)
	require.NotContains(t, err.Error(), stderrMarker)
}

func TestRunnerClampsEveryRequestedTimeoutAboveFiveSeconds(t *testing.T) {
	for _, requested := range []time.Duration{
		defaultCommandTimeout + time.Nanosecond,
		30 * time.Second,
		time.Hour,
	} {
		runner, ok := NewOSRunner(requested, 32<<10).(*osRunner)
		require.True(t, ok)
		require.Equal(t, defaultCommandTimeout, runner.timeout)
	}
}

func TestRunnerTimeoutKillsAndReapsChild(t *testing.T) {
	started := time.Now()
	executable := runnerHelperExecutable(t)
	result, err := NewOSRunner(500*time.Millisecond, 32<<10).Run(
		context.Background(),
		executable,
		runnerHelperArgs("sleep"),
	)

	require.ErrorIs(t, err, ErrClientCommand)
	requireCommandErrorCode(t, err, CommandTimedOut)
	require.Less(t, time.Since(started), 3*time.Second)
	require.NotEmpty(t, result.Stdout)

	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(result.Stdout)))
	require.NoError(t, parseErr)
	if runtime.GOOS != "windows" {
		process, findErr := os.FindProcess(pid)
		require.NoError(t, findErr)
		require.Error(t, process.Signal(syscall.Signal(0)), "timed-out child must be gone and reaped")
	}
}

func TestRunnerStartFailureUsesStableSanitizedError(t *testing.T) {
	const secretArgument = "token=do-not-return"
	secretExecutable := filepath.Join(t.TempDir(), "missing-private-codex")

	result, err := NewOSRunner(time.Second, 32<<10).Run(
		context.Background(),
		secretExecutable,
		[]string{"mcp", "get", secretArgument},
	)

	require.ErrorIs(t, err, ErrClientCommand)
	requireCommandErrorCode(t, err, CommandStartFailed)
	require.Equal(t, -1, result.ExitCode)
	require.Empty(t, result.Stdout)
	require.Empty(t, result.Stderr)
	require.NotContains(t, err.Error(), secretExecutable)
	require.NotContains(t, err.Error(), secretArgument)
}

func TestRunnerWindowsSnapshotRejectsBatchLauncherWithoutStartingIt(t *testing.T) {
	sentinel := filepath.Join(t.TempDir(), "batch-was-started")
	runner := NewOSRunnerWithSnapshot(
		time.Second,
		32<<10,
		newEnvironmentSnapshot(nil, "windows"),
	)

	result, err := runner.Run(
		context.Background(),
		filepath.Join(t.TempDir(), "codex.cmd"),
		[]string{"touch", sentinel},
	)

	require.ErrorIs(t, err, ErrClientCommand)
	requireCommandErrorCode(t, err, CommandStartFailed)
	require.Equal(t, -1, result.ExitCode)
	_, statErr := os.Stat(sentinel)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestRunnerUsesImmutableEnvironmentSnapshot(t *testing.T) {
	const variable = "CY_KAF_MCP_SNAPSHOT_VALUE"
	executable := runnerHelperExecutable(t)
	entries := []string{
		runnerHelperEnvironment + "=1",
		variable + "=snapshot-value",
	}
	snapshot := newEnvironmentSnapshot(entries, runtime.GOOS)
	entries[1] = variable + "=mutated-source"
	t.Setenv(variable, "ambient-value")

	result, err := NewOSRunnerWithSnapshot(2*time.Second, 32<<10, snapshot).Run(
		context.Background(),
		executable,
		runnerHelperArgs("env", variable),
	)
	require.NoError(t, err)
	require.Zero(t, result.ExitCode)
	require.Equal(t, "snapshot-value", string(result.Stdout))
}

func TestRunnerScopesOnlyAbsoluteClientConfigDirectories(t *testing.T) {
	executable := runnerHelperExecutable(t)
	base := NewOSRunnerWithSnapshot(
		2*time.Second,
		32<<10,
		newEnvironmentSnapshot(
			[]string{runnerHelperEnvironment + "=1", "UNCHANGED=value"},
			runtime.GOOS,
		),
	).(*osRunner)
	codexHome := t.TempDir()

	scoped, err := base.WithEnvironment(map[string]string{"CODEX_HOME": codexHome})
	require.NoError(t, err)
	result, err := scoped.Run(
		context.Background(),
		executable,
		runnerHelperArgs("env", "CODEX_HOME"),
	)
	require.NoError(t, err)
	require.Equal(t, codexHome, string(result.Stdout))

	_, err = base.WithEnvironment(map[string]string{"HOME": codexHome})
	require.ErrorIs(t, err, ErrClientCommand)
	_, err = base.WithEnvironment(map[string]string{"CODEX_HOME": "relative"})
	require.ErrorIs(t, err, ErrClientCommand)
	var missing *osRunner
	_, err = missing.WithEnvironment(map[string]string{"CODEX_HOME": codexHome})
	require.ErrorIs(t, err, ErrClientCommand)
}

func TestRunnerDefaultCapturesAmbientEnvironmentAtConstruction(t *testing.T) {
	const variable = "CY_KAF_MCP_DEFAULT_SNAPSHOT_VALUE"
	executable := runnerHelperExecutable(t)
	t.Setenv(variable, "captured-value")
	runner := NewOSRunner(2*time.Second, 32<<10)
	t.Setenv(variable, "later-ambient-value")

	result, err := runner.Run(
		context.Background(),
		executable,
		runnerHelperArgs("env", variable),
	)
	require.NoError(t, err)
	require.Zero(t, result.ExitCode)
	require.Equal(t, "captured-value", string(result.Stdout))
}

func TestRunnerHelperProcess(t *testing.T) {
	if os.Getenv(runnerHelperEnvironment) != "1" {
		return
	}

	args := helperArguments(os.Args)
	if len(args) == 0 {
		os.Exit(90)
	}
	switch args[0] {
	case "argv":
		stdin, err := io.ReadAll(os.Stdin)
		if err != nil {
			os.Exit(91)
		}
		if err := json.NewEncoder(os.Stdout).Encode(struct {
			Args  []string `json:"args"`
			Stdin string   `json:"stdin"`
		}{
			Args:  args[1:],
			Stdin: string(stdin),
		}); err != nil {
			os.Exit(92)
		}
		os.Exit(0)
	case "flood":
		writeRunnerHelperBytes(os.Stdout, 'o', 256<<10)
		writeRunnerHelperBytes(os.Stderr, 'e', 256<<10)
		os.Exit(0)
	case "exit":
		if len(args) != 4 {
			os.Exit(93)
		}
		code, err := strconv.Atoi(args[1])
		if err != nil {
			os.Exit(94)
		}
		_, _ = io.WriteString(os.Stdout, args[2])
		_, _ = io.WriteString(os.Stderr, args[3])
		os.Exit(code)
	case "sleep":
		_, _ = fmt.Fprintln(os.Stdout, os.Getpid())
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "env":
		if len(args) != 2 {
			os.Exit(96)
		}
		_, _ = io.WriteString(os.Stdout, os.Getenv(args[1]))
		os.Exit(0)
	default:
		os.Exit(95)
	}
}

func runnerHelperExecutable(t *testing.T) string {
	t.Helper()
	t.Setenv(runnerHelperEnvironment, "1")
	executable, err := os.Executable()
	require.NoError(t, err)
	return executable
}

func runnerHelperArgs(action string, arguments ...string) []string {
	result := []string{"-test.run=^TestRunnerHelperProcess$", "--", action}
	return append(result, arguments...)
}

func helperArguments(arguments []string) []string {
	for index, argument := range arguments {
		if argument == "--" {
			return arguments[index+1:]
		}
	}
	return nil
}

func writeRunnerHelperBytes(writer io.Writer, value byte, count int) {
	chunk := []byte(strings.Repeat(string(value), 4096))
	for written := 0; written < count; written += len(chunk) {
		_, _ = writer.Write(chunk)
	}
}

func requireCommandErrorCode(t *testing.T, err error, want CommandErrorCode) {
	t.Helper()
	var commandErr *CommandError
	require.True(t, errors.As(err, &commandErr))
	require.Equal(t, want, commandErr.Code)
}
