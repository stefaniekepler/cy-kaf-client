package mcpclient

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultCommandTimeout = 5 * time.Second
	maxCommandOutput      = 32 << 10
	commandWaitDelay      = 250 * time.Millisecond
)

var ErrClientCommand = errors.New("MCP client command failed")

type CommandErrorCode string

const (
	CommandStartFailed     CommandErrorCode = "start_failed"
	CommandTimedOut        CommandErrorCode = "timed_out"
	CommandCanceled        CommandErrorCode = "canceled"
	CommandExitNonZero     CommandErrorCode = "exit_nonzero"
	CommandOutputLimit     CommandErrorCode = "output_limit"
	CommandInvalidResponse CommandErrorCode = "invalid_response"
)

type CommandError struct {
	Code CommandErrorCode
}

func (err *CommandError) Error() string {
	return ErrClientCommand.Error() + ": " + string(err.Code)
}

func (err *CommandError) Unwrap() error {
	return ErrClientCommand
}

type CommandResult struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
}

type Runner interface {
	Run(context.Context, string, []string) (CommandResult, error)
}

type scopedEnvironmentRunner interface {
	WithEnvironment(map[string]string) (Runner, error)
}

type executableDirectoryRunner interface {
	WithExecutableDirectory(string) (Runner, error)
}

type EnvironmentSnapshot interface {
	environmentEntries() []string
	environmentGOOS() string
}

type EnvironmentSnapshotProvider interface {
	EnvironmentSnapshot() EnvironmentSnapshot
}

type environmentSnapshot struct {
	entries []string
	goos    string
}

type osRunner struct {
	timeout             time.Duration
	maxOutput           int
	environment         EnvironmentSnapshot
	executableDirectory string
}

func NewOSRunner(timeout time.Duration, maxOutput int) Runner {
	return NewOSRunnerWithSnapshot(timeout, maxOutput, CaptureEnvironmentSnapshot())
}

func CaptureEnvironmentSnapshot() EnvironmentSnapshot {
	return newEnvironmentSnapshot(os.Environ(), runtime.GOOS)
}

func NewOSRunnerWithSnapshot(
	timeout time.Duration,
	maxOutput int,
	environment EnvironmentSnapshot,
) Runner {
	if timeout <= 0 || timeout > defaultCommandTimeout {
		timeout = defaultCommandTimeout
	}
	if maxOutput <= 0 || maxOutput > maxCommandOutput {
		maxOutput = maxCommandOutput
	}
	if environment == nil {
		environment = CaptureEnvironmentSnapshot()
	}
	return &osRunner{
		timeout:     timeout,
		maxOutput:   maxOutput,
		environment: environment,
	}
}

func newEnvironmentSnapshot(entries []string, goos string) EnvironmentSnapshot {
	values := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, found := splitEnvironmentEntry(entry)
		if !found {
			continue
		}
		if goos == "windows" {
			key = strings.ToUpper(key)
		}
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	normalized := make([]string, 0, len(keys))
	for _, key := range keys {
		normalized = append(normalized, key+"="+values[key])
	}
	return &environmentSnapshot{entries: normalized, goos: goos}
}

func splitEnvironmentEntry(entry string) (string, string, bool) {
	if strings.HasPrefix(entry, "=") {
		index := strings.IndexByte(entry[1:], '=')
		if index < 0 {
			return "", "", false
		}
		index++
		return entry[:index], entry[index+1:], true
	}
	key, value, found := strings.Cut(entry, "=")
	return key, value, found && key != ""
}

func (snapshot *environmentSnapshot) environmentEntries() []string {
	if snapshot == nil {
		return nil
	}
	return cloneStrings(snapshot.entries)
}

func (snapshot *environmentSnapshot) environmentGOOS() string {
	if snapshot == nil {
		return ""
	}
	return snapshot.goos
}

func (runner *osRunner) EnvironmentSnapshot() EnvironmentSnapshot {
	if runner == nil {
		return nil
	}
	return runner.environment
}

func (runner *osRunner) WithEnvironment(overrides map[string]string) (Runner, error) {
	if runner == nil || runner.environment == nil {
		return nil, ErrClientCommand
	}
	allowed := map[string]bool{
		"CODEX_HOME":        true,
		"CLAUDE_CONFIG_DIR": true,
	}
	values := make(map[string]string)
	for _, entry := range runner.environment.environmentEntries() {
		key, value, found := splitEnvironmentEntry(entry)
		if found {
			values[key] = value
		}
	}
	for key, value := range overrides {
		if !allowed[key] || value == "" || !filepath.IsAbs(value) {
			return nil, ErrClientCommand
		}
		values[key] = filepath.Clean(value)
	}
	entries := make([]string, 0, len(values))
	for key, value := range values {
		entries = append(entries, key+"="+value)
	}
	return &osRunner{
		timeout:             runner.timeout,
		maxOutput:           runner.maxOutput,
		environment:         newEnvironmentSnapshot(entries, runner.environment.environmentGOOS()),
		executableDirectory: runner.executableDirectory,
	}, nil
}

func (runner *osRunner) WithExecutableDirectory(directory string) (Runner, error) {
	if runner == nil || runner.environment == nil ||
		directory == "" || !filepath.IsAbs(directory) {
		return nil, ErrClientCommand
	}
	canonical, err := filepath.EvalSymlinks(filepath.Clean(directory))
	if err != nil {
		return nil, ErrClientCommand
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return nil, ErrClientCommand
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return nil, ErrClientCommand
	}
	scoped := *runner
	scoped.executableDirectory = filepath.Clean(canonical)
	return &scoped, nil
}

func (runner *osRunner) Run(
	ctx context.Context,
	executable string,
	args []string,
) (CommandResult, error) {
	result := CommandResult{ExitCode: -1}
	if ctx == nil {
		return result, newCommandError(CommandStartFailed)
	}
	if runner.environment.environmentGOOS() == "windows" &&
		!isNativeWindowsExecutable(executable) {
		return result, newCommandError(CommandStartFailed)
	}

	runContext, cancel := context.WithTimeout(ctx, runner.timeout)
	defer cancel()

	stdout := newLimitWriter(runner.maxOutput)
	stderr := newLimitWriter(runner.maxOutput)
	cmd := exec.CommandContext(runContext, executable, args...)
	cmd.Stdin = nil
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.WaitDelay = commandWaitDelay
	cmd.Env = runner.commandEnvironment()

	runErr := cmd.Run()
	result.Stdout, _ = stdout.snapshot()
	result.Stderr, _ = stderr.snapshot()
	if cmd.ProcessState != nil {
		result.ExitCode = cmd.ProcessState.ExitCode()
	}

	if stdout.exceeded() || stderr.exceeded() {
		return result, newCommandError(CommandOutputLimit)
	}
	if runErr == nil {
		return result, nil
	}
	if ctx.Err() != nil {
		return result, newCommandError(CommandCanceled)
	}
	if runContext.Err() != nil {
		return result, newCommandError(CommandTimedOut)
	}
	if cmd.ProcessState == nil {
		return result, newCommandError(CommandStartFailed)
	}
	return result, newCommandError(CommandExitNonZero)
}

func (runner *osRunner) commandEnvironment() []string {
	entries := runner.environment.environmentEntries()
	if runner.executableDirectory == "" {
		return entries
	}

	pathKey := "PATH"
	pathValue := ""
	pathIndex := -1
	for index, entry := range entries {
		key, value, found := splitEnvironmentEntry(entry)
		if !found {
			continue
		}
		if runner.environment.environmentGOOS() == "windows" {
			key = strings.ToUpper(key)
		}
		if key == pathKey {
			pathIndex = index
			pathValue = value
			break
		}
	}
	if pathValue == "" {
		pathValue = runner.executableDirectory
	} else {
		pathValue = runner.executableDirectory +
			string(os.PathListSeparator) +
			pathValue
	}
	pathEntry := pathKey + "=" + pathValue
	if pathIndex >= 0 {
		entries[pathIndex] = pathEntry
		return entries
	}
	return append(entries, pathEntry)
}

func isNativeWindowsExecutable(executable string) bool {
	extension := strings.ToLower(filepath.Ext(executable))
	return extension == ".exe" || extension == ".com"
}

func newCommandError(code CommandErrorCode) error {
	return &CommandError{Code: code}
}

type limitWriter struct {
	mu       sync.Mutex
	limit    int
	bytes    []byte
	overflow bool
}

func newLimitWriter(limit int) *limitWriter {
	return &limitWriter{
		limit: limit,
		bytes: make([]byte, 0, limit),
	}
}

func (writer *limitWriter) Write(value []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()

	remaining := writer.limit - len(writer.bytes)
	if remaining > 0 {
		kept := len(value)
		if kept > remaining {
			kept = remaining
		}
		writer.bytes = append(writer.bytes, value[:kept]...)
	}
	if len(value) > remaining {
		writer.overflow = true
	}
	return len(value), nil
}

func (writer *limitWriter) snapshot() ([]byte, bool) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return append([]byte(nil), writer.bytes...), writer.overflow
}

func (writer *limitWriter) exceeded() bool {
	_, exceeded := writer.snapshot()
	return exceeded
}
