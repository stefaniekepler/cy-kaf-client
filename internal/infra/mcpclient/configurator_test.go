package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeLocator struct {
	path  string
	err   error
	calls atomic.Int32
}

func (fake *fakeLocator) Find(Client) (string, error) {
	fake.calls.Add(1)
	return fake.path, fake.err
}

type fakeInspector struct {
	mu    sync.Mutex
	entry Entry
	found bool
	err   error
	calls int
}

func (fake *fakeInspector) Get(context.Context, string) (Entry, bool, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	return cloneEntry(fake.entry), fake.found, fake.err
}

type fakeConfigPaths struct {
	path        string
	root        string
	err         error
	afterTarget func(*configTarget)
	calls       atomic.Int32
}

func (fake *fakeConfigPaths) Resolve(Client) (string, error) {
	fake.calls.Add(1)
	return fake.path, fake.err
}

func (fake *fakeConfigPaths) ResolveTarget(client Client) (*configTarget, error) {
	path, err := fake.Resolve(client)
	if err != nil {
		return nil, err
	}
	rootPath := fake.root
	if rootPath == "" {
		rootPath = filepath.Dir(path)
		for {
			if info, statErr := os.Stat(rootPath); statErr == nil && info.IsDir() {
				break
			}
			next := filepath.Dir(rootPath)
			if next == rootPath {
				return nil, ErrInvalidConfig
			}
			rootPath = next
		}
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	info, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	relative, err := filepath.Rel(rootPath, path)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	target := &configTarget{
		path: path, homePath: rootPath, relative: relative,
		root: root, homeInfo: info,
	}
	if fake.afterTarget != nil {
		fake.afterTarget(target)
	}
	return target, nil
}

type fakeClaudeReader struct {
	mu    sync.Mutex
	entry Entry
	found bool
	err   error
	calls int
	paths []string
	read  func(int)
}

func (fake *fakeClaudeReader) ReadClaude(path string) (Entry, bool, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.calls++
	fake.paths = append(fake.paths, path)
	if fake.read != nil {
		fake.read(fake.calls)
	}
	return cloneEntry(fake.entry), fake.found, fake.err
}

type fakeRunner struct {
	mu       sync.Mutex
	calls    [][]string
	run      func([]string) error
	scope    map[string]string
	inFlight atomic.Int32
	max      atomic.Int32
}

type explicitEnvironmentRunner struct {
	snapshot EnvironmentSnapshot
	calls    int
	run      func()
}

func (runner *explicitEnvironmentRunner) Run(
	_ context.Context,
	_ string,
	_ []string,
) (CommandResult, error) {
	runner.calls++
	if runner.run != nil {
		runner.run()
	}
	return CommandResult{ExitCode: 0}, nil
}

func (runner *explicitEnvironmentRunner) EnvironmentSnapshot() EnvironmentSnapshot {
	return runner.snapshot
}

type undeletableBackupStore struct {
	backupStore
}

func (store undeletableBackupStore) Publish(
	ctx context.Context,
	snapshot *fileSnapshot,
) error {
	if err := store.backupStore.Publish(ctx, snapshot); err != nil {
		return err
	}
	return ErrRollbackFailed
}

type externalWriteAfterCaptureStore struct {
	backupStore
	replacement []byte
	snapshot    *fileSnapshot
}

type trackingBackupStore struct {
	backupStore
	snapshot *fileSnapshot
}

type unsupportedAtomicStore struct {
	backupStore
	snapshot   *fileSnapshot
	backupPath string
	stageCalls int
}

type captureErrorStore struct {
	backupStore
	err error
}

func (store captureErrorStore) CaptureTarget(
	_ context.Context,
	target *configTarget,
) (*fileSnapshot, error) {
	if target != nil && target.root != nil {
		_ = target.root.Close()
	}
	return nil, store.err
}

type deleteFailureAfterStageStore struct {
	backupStore
}

func (store deleteFailureAfterStageStore) Stage(
	ctx context.Context,
	snapshot *fileSnapshot,
	client Client,
) (string, error) {
	path, err := store.backupStore.Stage(ctx, snapshot, client)
	if err != nil || snapshot.backupPath == "" {
		return path, err
	}
	if err := os.Remove(snapshot.backupPath); err != nil {
		return "", err
	}
	if err := os.Mkdir(snapshot.backupPath, 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(snapshot.backupPath, "retained"), []byte("x"), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func (store *unsupportedAtomicStore) CaptureTarget(
	ctx context.Context,
	target *configTarget,
) (*fileSnapshot, error) {
	snapshot, err := store.backupStore.CaptureTarget(ctx, target)
	store.snapshot = snapshot
	if snapshot != nil {
		store.backupPath = snapshot.backupPath
	}
	return snapshot, err
}

func (*unsupportedAtomicStore) SupportsAtomic(*fileSnapshot) bool {
	return false
}

func (store *unsupportedAtomicStore) Stage(
	context.Context,
	*fileSnapshot,
	Client,
) (string, error) {
	store.stageCalls++
	return "", ErrRollbackFailed
}

func (store *trackingBackupStore) CaptureTarget(
	ctx context.Context,
	target *configTarget,
) (*fileSnapshot, error) {
	snapshot, err := store.backupStore.CaptureTarget(ctx, target)
	store.snapshot = snapshot
	return snapshot, err
}

func (store *externalWriteAfterCaptureStore) CaptureTarget(
	ctx context.Context,
	target *configTarget,
) (*fileSnapshot, error) {
	snapshot, err := store.backupStore.CaptureTarget(ctx, target)
	if err != nil {
		return nil, err
	}
	store.snapshot = snapshot
	if err := os.WriteFile(snapshot.targetPath, store.replacement, 0o600); err != nil {
		_ = snapshot.Delete()
		return nil, err
	}
	return snapshot, nil
}

func (fake *fakeRunner) Run(_ context.Context, _ string, args []string) (CommandResult, error) {
	current := fake.inFlight.Add(1)
	defer fake.inFlight.Add(-1)
	for {
		max := fake.max.Load()
		if current <= max || fake.max.CompareAndSwap(max, current) {
			break
		}
	}
	fake.mu.Lock()
	fake.calls = append(fake.calls, cloneStrings(args))
	run := fake.run
	fake.mu.Unlock()
	if run != nil {
		if err := run(args); err != nil {
			return CommandResult{ExitCode: 1}, err
		}
	}
	return CommandResult{ExitCode: 0}, nil
}

func (fake *fakeRunner) WithEnvironment(overrides map[string]string) (Runner, error) {
	fake.mu.Lock()
	fake.scope = cloneStringMap(overrides)
	fake.mu.Unlock()
	return fake, nil
}

func (fake *fakeRunner) scopedConfigPath(client Client) string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if client == ClientClaudeCode {
		return filepath.Join(fake.scope["CLAUDE_CONFIG_DIR"], ".claude.json")
	}
	return filepath.Join(fake.scope["CODEX_HOME"], "config.toml")
}

func TestConfiguratorStatusNormalization(t *testing.T) {
	desired := testDesired(t)
	exact := desiredEntry(desired)
	deadCommand := filepath.Join(t.TempDir(), filepath.Base(desired.Command))

	tests := []struct {
		name      string
		locator   *fakeLocator
		inspector *fakeInspector
		want      Status
	}{
		{"client absent", &fakeLocator{err: ErrClientNotFound}, &fakeInspector{}, StatusClientNotFound},
		{"entry absent", &fakeLocator{path: "/fixed/codex"}, &fakeInspector{}, StatusNotConfigured},
		{"exact", &fakeLocator{path: "/fixed/codex"}, &fakeInspector{entry: exact, found: true}, StatusConfigured},
		{"dead old path", &fakeLocator{path: "/fixed/codex"}, &fakeInspector{entry: Entry{Name: ServerName, Transport: "stdio", Enabled: true, Command: deadCommand, Args: cloneStrings(desired.Args)}, found: true}, StatusRepairRequired},
		{"disabled exact", &fakeLocator{path: "/fixed/codex"}, &fakeInspector{entry: withEnabled(exact, false), found: true}, StatusRepairRequired},
		{"different command", &fakeLocator{path: "/fixed/codex"}, &fakeInspector{entry: Entry{Name: ServerName, Transport: "stdio", Enabled: true, Command: "/existing/other", Args: cloneStrings(desired.Args)}, found: true}, StatusConflict},
		{"query error", &fakeLocator{path: "/fixed/codex"}, &fakeInspector{err: errors.New("secret command output")}, StatusError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configurator := NewConfigurator(
				test.locator,
				&fakeRunner{},
				test.inspector,
				&fakeConfigPaths{path: filepath.Join(t.TempDir(), "config.toml")},
				&fakeClaudeReader{},
			)
			got, err := configurator.Status(context.Background(), ClientCodex, desired)
			if err != nil || got.Status != test.want {
				t.Fatalf("Status() = %#v, %v; want %s", got, err, test.want)
			}
			if test.want == StatusError && got.Message != ErrClientCommand.Error() {
				t.Fatalf("safe message = %q", got.Message)
			}
		})
	}
}

func TestConfiguratorCodexRunnerPreservesNVMLauncherDirectoryAcrossStagedEnvironment(
	t *testing.T,
) {
	if runtime.GOOS == "windows" {
		t.Skip("automatic client configuration is intentionally unavailable on Windows")
	}

	home := evalTestPath(t, t.TempDir())
	nvmRoot := filepath.Join(home, ".nvm", "versions", "node", "v22.19.0")
	nvmBin := filepath.Join(nvmRoot, "bin")
	if err := os.MkdirAll(nvmBin, 0o700); err != nil {
		t.Fatalf("create NVM bin directory: %v", err)
	}
	codexTarget := filepath.Join(
		nvmRoot,
		"lib",
		"node_modules",
		"@openai",
		"codex",
		"bin",
		"codex.js",
	)
	writeTask3File(
		t,
		codexTarget,
		[]byte("#!/bin/sh\n"),
		0o700,
	)
	if err := makeTestSymlink(codexTarget, filepath.Join(nvmBin, "codex")); err != nil {
		t.Fatalf("create Codex launcher symlink: %v", err)
	}

	const minimalPATH = "/usr/bin:/bin:/usr/sbin:/sbin"
	snapshot := newEnvironmentSnapshot(
		[]string{"PATH=" + minimalPATH},
		runtime.GOOS,
	)
	baseRunner := NewOSRunnerWithSnapshot(5*time.Second, 32<<10, snapshot)
	configurator := NewConfigurator(
		NewLocatorWithSnapshot(snapshot, home),
		baseRunner,
		NewCodexInspector(baseRunner),
		NewConfigPathResolverWithSnapshot(snapshot, home),
		NewClaudeEntryReader(8<<20),
	).(*configurator)

	executable, launchRunner, err := configurator.findClientRunner(ClientCodex)
	if err != nil || executable != codexTarget {
		t.Fatalf("findClientRunner() = %q, %v; want %q", executable, err, codexTarget)
	}
	stageDirectory := filepath.Join(home, "staged-codex-home")
	stagedRunner, err := configurator.runnerForStage(
		launchRunner,
		ClientCodex,
		stageDirectory,
	)
	if err != nil {
		t.Fatalf("runnerForStage() error = %v", err)
	}
	scoped, ok := stagedRunner.(*osRunner)
	if !ok {
		t.Fatalf("runnerForStage() type = %T; want *osRunner", stagedRunner)
	}

	commandSnapshot := newEnvironmentSnapshot(
		scoped.commandEnvironment(),
		runtime.GOOS,
	)
	wantPATH := nvmBin + string(os.PathListSeparator) + minimalPATH
	if got := environmentValue(commandSnapshot, "PATH"); got != wantPATH {
		t.Fatalf("staged command PATH = %q; want %q", got, wantPATH)
	}
	if got := environmentValue(commandSnapshot, "CODEX_HOME"); got != stageDirectory {
		t.Fatalf("staged CODEX_HOME = %q; want %q", got, stageDirectory)
	}
	base, ok := baseRunner.(*osRunner)
	if !ok {
		t.Fatalf("base runner type = %T; want *osRunner", baseRunner)
	}
	if got := environmentValue(
		newEnvironmentSnapshot(base.commandEnvironment(), runtime.GOOS),
		"PATH",
	); got != minimalPATH {
		t.Fatalf("base command PATH = %q; want unchanged %q", got, minimalPATH)
	}
}

func TestConfiguratorExactEntryRequiresNoEnvironmentOrWorkingDirectory(t *testing.T) {
	desired := testDesired(t)
	tests := map[string]Entry{
		"env":      withEnv(desiredEntry(desired), map[string]string{"TOKEN": "marker"}),
		"env vars": withEnvVars(desiredEntry(desired), []string{"TOKEN"}),
		"cwd":      withCWD(desiredEntry(desired), "/tmp"),
	}
	for name, entry := range tests {
		t.Run(name, func(t *testing.T) {
			configurator := NewConfigurator(
				&fakeLocator{path: "/fixed/codex"},
				&fakeRunner{},
				&fakeInspector{entry: entry, found: true},
				&fakeConfigPaths{path: filepath.Join(t.TempDir(), "config.toml")},
				&fakeClaudeReader{},
			)
			got, err := configurator.Status(context.Background(), ClientCodex, desired)
			if err != nil || got.Status != StatusConflict {
				t.Fatalf("Status() = %#v, %v", got, err)
			}
		})
	}
}

func TestConfiguratorClaudeStatusUsesBoundedReaderAndNeverRunner(t *testing.T) {
	desired := testDesired(t)
	reader := &fakeClaudeReader{entry: desiredEntry(desired), found: true}
	runner := &fakeRunner{run: func(args []string) error {
		t.Fatalf("unexpected Claude runner invocation: %#v", args)
		return nil
	}}
	paths := &fakeConfigPaths{path: filepath.Join(t.TempDir(), ".claude.json")}
	configurator := NewConfigurator(&fakeLocator{path: "/fixed/claude"}, runner, &fakeInspector{}, paths, reader)

	got, err := configurator.Status(context.Background(), ClientClaudeCode, desired)
	if err != nil || got.Status != StatusConfigured {
		t.Fatalf("Status() = %#v, %v", got, err)
	}
	if reader.calls != 1 || len(reader.paths) != 1 ||
		reader.paths[0] == paths.path ||
		filepath.Base(reader.paths[0]) != ".claude.json" {
		t.Fatalf("reader calls = %d, paths %#v", reader.calls, reader.paths)
	}
}

func TestConfiguratorClaudeEnvironmentIsConflictUsingRealReader(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), ".claude.json")
	writeTask3File(t, configPath, []byte(`{"mcpServers":{"cy-kaf-client":{"type":"stdio","command":"`+desired.Command+`","args":["mcp","--config","/tmp/config with spaces.yaml"],"env":{"TOKEN":"target-secret-marker"}}}}`), 0o600)
	runner := &fakeRunner{run: func(args []string) error {
		t.Fatalf("unexpected Claude runner invocation: %#v", args)
		return nil
	}}
	configurator := NewConfigurator(
		&fakeLocator{path: "/fixed/claude"},
		runner,
		&fakeInspector{},
		&fakeConfigPaths{path: configPath},
		NewClaudeEntryReader(8<<20),
	)
	got, err := configurator.Status(context.Background(), ClientClaudeCode, desired)
	if err != nil || got.Status != StatusConflict {
		t.Fatalf("Status() = %#v, %v", got, err)
	}
}

func TestConfiguratorClaudeParseErrorIsSafeStatusError(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), ".claude.json")
	writeTask3File(t, configPath, []byte(`{"sibling":"sibling-secret-marker","mcpServers":`), 0o600)
	runner := &fakeRunner{run: func(args []string) error {
		t.Fatalf("unexpected Claude runner invocation: %#v", args)
		return nil
	}}
	configurator := NewConfigurator(
		&fakeLocator{path: "/fixed/claude"},
		runner,
		&fakeInspector{},
		&fakeConfigPaths{path: configPath},
		NewClaudeEntryReader(8<<20),
	)
	got, err := configurator.Status(context.Background(), ClientClaudeCode, desired)
	if err != nil || got.Status != StatusError || got.Message != ErrClientCommand.Error() {
		t.Fatalf("Status() = %#v, %v", got, err)
	}
	if strings.Contains(got.Message, "sibling-secret-marker") {
		t.Fatalf("status leaked sibling bytes: %q", got.Message)
	}
}

func TestConfiguratorConflictWithoutReplaceDoesNotMutate(t *testing.T) {
	desired := testDesired(t)
	runner := &fakeRunner{run: func(args []string) error {
		t.Fatalf("unexpected mutation: %#v", args)
		return nil
	}}
	configurator := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		&fakeInspector{entry: withCWD(desiredEntry(desired), "/other"), found: true},
		&fakeConfigPaths{path: filepath.Join(t.TempDir(), "config.toml")},
		&fakeClaudeReader{},
	)
	got, err := configurator.Configure(context.Background(), ClientCodex, desired, false)
	if err != nil || got.Status != StatusConflict {
		t.Fatalf("Configure() = %#v, %v", got, err)
	}
}

func TestConfiguratorRepairWithoutReplaceDoesNotMutate(t *testing.T) {
	desired := testDesired(t)
	runner := &fakeRunner{run: func(args []string) error {
		t.Fatalf("unexpected mutation: %#v", args)
		return nil
	}}
	entry := desiredEntry(desired)
	entry.Enabled = false
	configurator := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		&fakeInspector{entry: entry, found: true},
		&fakeConfigPaths{path: filepath.Join(t.TempDir(), "config.toml")},
		&fakeClaudeReader{},
	)
	got, err := configurator.Configure(context.Background(), ClientCodex, desired, false)
	if err != nil || got.Status != StatusRepairRequired {
		t.Fatalf("Configure() = %#v, %v", got, err)
	}
}

func TestConfiguratorExactConfigureIsIdempotent(t *testing.T) {
	desired := testDesired(t)
	tests := []struct {
		name      string
		client    Client
		inspector *fakeInspector
		reader    *fakeClaudeReader
	}{
		{"Codex", ClientCodex, &fakeInspector{entry: desiredEntry(desired), found: true}, &fakeClaudeReader{}},
		{"Claude", ClientClaudeCode, &fakeInspector{}, &fakeClaudeReader{entry: desiredEntry(desired), found: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &fakeRunner{run: func(args []string) error {
				t.Fatalf("unexpected mutation: %#v", args)
				return nil
			}}
			paths := &fakeConfigPaths{path: filepath.Join(t.TempDir(), "config")}
			configurator := NewConfigurator(&fakeLocator{path: "/fixed/client"}, runner, test.inspector, paths, test.reader)
			got, err := configurator.Configure(context.Background(), test.client, desired, false)
			if err != nil || got.Status != StatusConfigured {
				t.Fatalf("Configure() = %#v, %v", got, err)
			}
			if paths.calls.Load() != 1 {
				t.Fatalf("exact entry resolved config path %d times, want one anchored snapshot", paths.calls.Load())
			}
		})
	}
}

func TestConfiguratorAddsAbsentCodexAndDeletesBackup(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	inspector := &fakeInspector{}
	runner := &fakeRunner{}
	runner.run = func(args []string) error {
		if equalStrings(args, AddArgs(ClientCodex, desired)) {
			writeTask3File(t, runner.scopedConfigPath(ClientCodex), []byte("new config"), 0o600)
			inspector.mu.Lock()
			inspector.entry, inspector.found = desiredEntry(desired), true
			inspector.mu.Unlock()
		}
		return nil
	}
	configurator := NewConfigurator(&fakeLocator{path: "/fixed/codex"}, runner, inspector, &fakeConfigPaths{path: configPath}, &fakeClaudeReader{})

	got, err := configurator.Configure(context.Background(), ClientCodex, desired, false)
	if err != nil || got.Status != StatusConfigured {
		t.Fatalf("Configure() = %#v, %v", got, err)
	}
	assertRunnerCalls(t, runner, AddArgs(ClientCodex, desired))
	assertNoBackupFiles(t, filepath.Dir(configPath))
}

func TestConfiguratorAddsAbsentClaudeWithBoundedPostVerification(t *testing.T) {
	desired := testDesired(t)
	tests := []struct {
		name    string
		initial []byte
	}{
		{"absent config", nil},
		{"existing unrelated config", []byte(`{"unrelated":"sibling-secret-marker"}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), ".claude.json")
			if test.initial != nil {
				writeTask3File(t, configPath, test.initial, 0o640)
			}
			runner := &fakeRunner{}
			runner.run = func(args []string) error {
				if !equalStrings(args, AddArgs(ClientClaudeCode, desired)) {
					t.Fatalf("query-shaped or unexpected Claude argv: %#v", args)
				}
				writeTask3File(
					t,
					runner.scopedConfigPath(ClientClaudeCode),
					claudeConfigBytes(t, desired, true),
					0o600,
				)
				return nil
			}
			configurator := NewConfigurator(
				&fakeLocator{path: "/fixed/claude"},
				runner,
				&fakeInspector{},
				&fakeConfigPaths{path: configPath},
				NewClaudeEntryReader(8<<20),
			)
			got, err := configurator.Configure(context.Background(), ClientClaudeCode, desired, false)
			if err != nil || got.Status != StatusConfigured {
				t.Fatalf("Configure() = %#v, %v", got, err)
			}
			assertRunnerCalls(t, runner, AddArgs(ClientClaudeCode, desired))
			assertNoBackupFiles(t, filepath.Dir(configPath))
		})
	}
}

func TestConfiguratorFailedFirstTimeAddRestoresAbsence(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), "new", "config.toml")
	runner := &fakeRunner{}
	runner.run = func([]string) error {
		writeTask3File(t, runner.scopedConfigPath(ClientCodex), []byte("partial"), 0o600)
		return errors.New("synthetic-add-failure")
	}
	configurator := NewConfigurator(&fakeLocator{path: "/fixed/codex"}, runner, &fakeInspector{}, &fakeConfigPaths{path: configPath}, &fakeClaudeReader{})
	_, err := configurator.Configure(context.Background(), ClientCodex, desired, false)
	if !errors.Is(err, ErrClientCommand) {
		t.Fatalf("Configure() error = %v", err)
	}
	if _, statErr := os.Lstat(configPath); !os.IsNotExist(statErr) {
		t.Fatalf("first-time target still exists: %v", statErr)
	}
}

func TestConfiguratorBackupDeleteFailureNeverClaimsSuccess(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	writeTask3File(t, configPath, []byte("original"), 0o600)
	inspector := &fakeInspector{}
	runner := &fakeRunner{}
	runner.run = func(args []string) error {
		writeTask3File(t, runner.scopedConfigPath(ClientCodex), []byte("configured"), 0o600)
		inspector.mu.Lock()
		inspector.entry, inspector.found = desiredEntry(desired), true
		inspector.mu.Unlock()
		return nil
	}
	instance := NewConfigurator(&fakeLocator{path: "/fixed/codex"}, runner, inspector, &fakeConfigPaths{path: configPath}, &fakeClaudeReader{}).(*configurator)
	instance.backups = undeletableBackupStore{backupStore: newFileBackupStore()}

	got, err := instance.Configure(context.Background(), ClientCodex, desired, false)
	if !errors.Is(err, ErrRollbackFailed) || got.Status == StatusConfigured {
		t.Fatalf("Configure() = %#v, %v", got, err)
	}
}

func TestConfiguratorAddsAbsentClaudeWithoutQueryAndPreservesUnrelatedBytesOnFailure(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), ".claude.json")
	original := []byte(`{"unrelated":"sibling-secret-marker"}`)
	writeTask3File(t, configPath, original, 0o640)
	reader := &fakeClaudeReader{}
	runner := &fakeRunner{}
	runner.run = func(args []string) error {
		if !equalStrings(args, AddArgs(ClientClaudeCode, desired)) {
			t.Fatalf("unexpected Claude argv: %#v", args)
		}
		writeTask3File(
			t,
			runner.scopedConfigPath(ClientClaudeCode),
			[]byte("partially changed"),
			0o600,
		)
		return errors.New("target-secret-marker")
	}
	configurator := NewConfigurator(&fakeLocator{path: "/fixed/claude"}, runner, &fakeInspector{}, &fakeConfigPaths{path: configPath}, reader)

	_, err := configurator.Configure(context.Background(), ClientClaudeCode, desired, false)
	if err == nil {
		t.Fatal("Configure() error = nil")
	}
	got, readErr := os.ReadFile(configPath)
	if readErr != nil || string(got) != string(original) {
		t.Fatalf("original bytes not restored: %q, %v", got, readErr)
	}
}

func TestConfiguratorReplacesClaudeUsingOnlyScopedMutations(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), ".claude.json")
	sentinel := filepath.Join(t.TempDir(), "must-not-exist")
	writeTask3File(t, configPath, []byte(`{"mcpServers":{"cy-kaf-client":{"type":"stdio","command":"`+sentinel+`","args":["mcp"]}}}`), 0o600)
	runner := &fakeRunner{}
	runner.run = func(args []string) error {
		stagePath := runner.scopedConfigPath(ClientClaudeCode)
		switch {
		case equalStrings(args, RemoveArgs(ClientClaudeCode)):
			writeTask3File(t, stagePath, []byte(`{"mcpServers":{}}`), 0o600)
		case equalStrings(args, AddArgs(ClientClaudeCode, desired)):
			writeTask3File(t, stagePath, claudeConfigBytes(t, desired, false), 0o600)
		default:
			t.Fatalf("query-shaped or unexpected Claude argv: %#v", args)
		}
		return nil
	}
	configurator := NewConfigurator(&fakeLocator{path: "/fixed/claude"}, runner, &fakeInspector{}, &fakeConfigPaths{path: configPath}, NewClaudeEntryReader(8<<20))

	status, err := configurator.Status(context.Background(), ClientClaudeCode, desired)
	if err != nil || status.Status != StatusConflict {
		t.Fatalf("Status() = %#v, %v", status, err)
	}
	assertRunnerCalls(t, runner)

	got, err := configurator.Configure(context.Background(), ClientClaudeCode, desired, true)
	if err != nil || got.Status != StatusConfigured {
		t.Fatalf("Configure() = %#v, %v", got, err)
	}
	assertRunnerCalls(t, runner, RemoveArgs(ClientClaudeCode), AddArgs(ClientClaudeCode, desired))
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("pre-existing Claude command was launched: %v", err)
	}
}

func TestConfiguratorVerificationMismatchRollsBackExactBytes(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	original := []byte("original exact bytes")
	writeTask3File(t, configPath, original, 0o640)
	inspector := &fakeInspector{}
	runner := &fakeRunner{}
	runner.run = func([]string) error {
		writeTask3File(t, runner.scopedConfigPath(ClientCodex), []byte("mutated"), 0o600)
		inspector.mu.Lock()
		inspector.entry, inspector.found = withCWD(desiredEntry(desired), "/mismatch"), true
		inspector.mu.Unlock()
		return nil
	}
	configurator := NewConfigurator(&fakeLocator{path: "/fixed/codex"}, runner, inspector, &fakeConfigPaths{path: configPath}, &fakeClaudeReader{})

	_, err := configurator.Configure(context.Background(), ClientCodex, desired, false)
	if !errors.Is(err, ErrVerification) {
		t.Fatalf("Configure() error = %v, want ErrVerification", err)
	}
	got, readErr := os.ReadFile(configPath)
	if readErr != nil || string(got) != string(original) {
		t.Fatalf("rollback bytes = %q, %v", got, readErr)
	}
}

func TestConfiguratorConcurrentExternalChangeIsNotOverwritten(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), ".claude.json")
	writeTask3File(t, configPath, []byte("original"), 0o600)
	reader := &fakeClaudeReader{}
	reader.read = func(call int) {
		if call == 2 {
			writeTask3File(t, configPath, []byte("external-change"), 0o600)
		}
	}
	runner := &fakeRunner{run: func([]string) error {
		writeTask3File(t, configPath, []byte("mutation"), 0o600)
		return nil
	}}
	configurator := NewConfigurator(&fakeLocator{path: "/fixed/claude"}, runner, &fakeInspector{}, &fakeConfigPaths{path: configPath}, reader)

	_, err := configurator.Configure(context.Background(), ClientClaudeCode, desired, false)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Configure() error = %v", err)
	}
	got, readErr := os.ReadFile(configPath)
	if readErr != nil || string(got) != "external-change" {
		t.Fatalf("external bytes overwritten: %q, %v", got, readErr)
	}
}

func TestConfiguratorBaselineChangeBeforeFirstMutationAbortsAndRetainsBackup(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	writeTask3File(t, configPath, []byte("old"), 0o600)
	runner := &fakeRunner{run: func(args []string) error {
		t.Fatalf("mutation invoked after baseline changed: %#v", args)
		return errors.New("synthetic mutation failure")
	}}
	instance := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		&fakeInspector{},
		&fakeConfigPaths{path: configPath},
		&fakeClaudeReader{},
	).(*configurator)
	store := &externalWriteAfterCaptureStore{
		backupStore: newFileBackupStore(),
		replacement: []byte("external"),
	}
	instance.backups = store

	_, err := instance.Configure(context.Background(), ClientCodex, desired, false)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Configure() error = %v, want ErrConcurrentModification", err)
	}
	if runner.max.Load() != 0 {
		t.Fatalf("mutation count = %d, want zero", runner.max.Load())
	}
	got, readErr := os.ReadFile(configPath)
	if readErr != nil || string(got) != "external" {
		t.Fatalf("external bytes changed: %q, %v", got, readErr)
	}
	if store.snapshot == nil || store.snapshot.backupPath == "" {
		t.Fatal("snapshot or retained backup missing")
	}
	t.Cleanup(func() { _ = store.snapshot.Delete() })
	if _, statErr := os.Stat(store.snapshot.backupPath); statErr != nil {
		t.Fatalf("private backup not retained: %v", statErr)
	}
}

func TestConfiguratorExternalWriteDuringSuccessfulCLIIsNotOwnedOrOverwritten(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	writeTask3File(t, configPath, []byte("old"), 0o600)
	inspector := &fakeInspector{}
	runner := &fakeRunner{run: func(args []string) error {
		if equalStrings(args, AddArgs(ClientCodex, desired)) {
			writeTask3File(t, configPath, []byte("external-during-cli"), 0o600)
			inspector.mu.Lock()
			inspector.entry, inspector.found = desiredEntry(desired), true
			inspector.mu.Unlock()
		}
		return nil
	}}
	instance := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		inspector,
		&fakeConfigPaths{path: configPath},
		&fakeClaudeReader{},
	).(*configurator)
	store := &trackingBackupStore{backupStore: newFileBackupStore()}
	instance.backups = store

	_, err := instance.Configure(context.Background(), ClientCodex, desired, false)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Configure() error = %v, want ErrConcurrentModification", err)
	}
	got, readErr := os.ReadFile(configPath)
	if readErr != nil || string(got) != "external-during-cli" {
		t.Fatalf("external bytes changed: %q, %v", got, readErr)
	}
	if store.snapshot == nil || store.snapshot.backupPath == "" {
		t.Fatal("recovery backup was deleted")
	}
	t.Cleanup(func() { _ = store.snapshot.Delete() })
	if _, statErr := os.Stat(store.snapshot.backupPath); statErr != nil {
		t.Fatalf("private backup not retained: %v", statErr)
	}
}

func TestConfiguratorParentSwapDuringCLINeverWritesOutsideCapturedHome(t *testing.T) {
	desired := testDesired(t)
	home := t.TempDir()
	configDir := filepath.Join(home, ".codex")
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.Mkdir(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, configPath, []byte("old"), 0o600)
	outside := t.TempDir()
	outsideTarget := filepath.Join(outside, "config.toml")
	movedParent := filepath.Join(home, ".codex-original")
	runner := &fakeRunner{}
	runner.run = func(args []string) error {
		if err := os.Rename(configDir, movedParent); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, configDir); err != nil {
			t.Fatal(err)
		}
		stagePath := runner.scopedConfigPath(ClientCodex)
		if err := os.WriteFile(stagePath, []byte("cli-write"), 0o600); err != nil {
			t.Fatal(err)
		}
		return errors.New("synthetic CLI failure")
	}
	instance := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		&fakeInspector{},
		&fakeConfigPaths{path: configPath},
		&fakeClaudeReader{},
	)

	_, _ = instance.Configure(context.Background(), ClientCodex, desired, false)
	if _, err := os.Lstat(outsideTarget); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("CLI escaped captured home through swapped parent: %v", err)
	}
}

func TestConfiguratorParentSwapBeforeCaptureNeverCreatesThroughSymlink(t *testing.T) {
	desired := testDesired(t)
	home := t.TempDir()
	configDir := filepath.Join(home, ".codex")
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.Mkdir(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	paths := &fakeConfigPaths{path: configPath, root: home}
	paths.afterTarget = func(*configTarget) {
		if err := os.Rename(configDir, filepath.Join(home, ".codex-original")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, configDir); err != nil {
			t.Fatal(err)
		}
	}
	runner := &fakeRunner{run: func([]string) error {
		t.Fatal("runner invoked after trusted parent replacement")
		return nil
	}}
	instance := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		&fakeInspector{},
		paths,
		&fakeClaudeReader{},
	)

	_, _ = instance.Configure(context.Background(), ClientCodex, desired, false)
	if runner.max.Load() != 0 {
		t.Fatalf("runner calls = %d", runner.max.Load())
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("outside directory changed: %v, %v", entries, err)
	}
}

func TestConfiguratorSerializesSameClientButNotDifferentClients(t *testing.T) {
	desired := testDesired(t)
	makeConfig := func(client Client) (Configurator, *fakeRunner) {
		configPath := filepath.Join(t.TempDir(), string(client))
		inspector := &fakeInspector{}
		reader := &fakeClaudeReader{}
		runner := &fakeRunner{run: func(args []string) error {
			time.Sleep(30 * time.Millisecond)
			writeTask3File(t, configPath, []byte("mutation"), 0o600)
			if client == ClientCodex {
				inspector.mu.Lock()
				inspector.entry, inspector.found = desiredEntry(desired), true
				inspector.mu.Unlock()
			} else {
				reader.mu.Lock()
				reader.entry, reader.found = desiredEntry(desired), true
				reader.mu.Unlock()
			}
			return nil
		}}
		return NewConfigurator(&fakeLocator{path: "/fixed/client"}, runner, inspector, &fakeConfigPaths{path: configPath}, reader), runner
	}

	same, sameRunner := makeConfig(ClientCodex)
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			_, _ = same.Configure(context.Background(), ClientCodex, desired, false)
		}()
	}
	wg.Wait()
	if sameRunner.max.Load() != 1 {
		t.Fatalf("same-client max concurrency = %d", sameRunner.max.Load())
	}

	configPath := filepath.Join(t.TempDir(), "shared")
	writeTask3File(t, configPath, []byte("{}"), 0o600)
	block := make(chan struct{})
	started := make(chan struct{}, 2)
	runner := &fakeRunner{run: func([]string) error {
		started <- struct{}{}
		<-block
		return errors.New("stop")
	}}
	mixed := NewConfigurator(&fakeLocator{path: "/fixed/client"}, runner, &fakeInspector{}, &fakeConfigPaths{path: configPath}, &fakeClaudeReader{})
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = mixed.Configure(context.Background(), ClientCodex, desired, false) }()
	go func() {
		defer wg.Done()
		_, _ = mixed.Configure(context.Background(), ClientClaudeCode, desired, false)
	}()
	<-started
	<-started
	close(block)
	wg.Wait()
	if runner.max.Load() < 2 {
		t.Fatalf("different-client max concurrency = %d", runner.max.Load())
	}
}

func TestConfiguratorUnsafeConfigPathDisablesInitialAndReplaceMutation(t *testing.T) {
	desired := testDesired(t)
	tests := []struct {
		name      string
		inspector *fakeInspector
		replace   bool
	}{
		{"initial add", &fakeInspector{}, false},
		{"replace", &fakeInspector{entry: withCWD(desiredEntry(desired), "/conflict"), found: true}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &fakeRunner{run: func(args []string) error {
				t.Fatalf("unexpected mutation: %#v", args)
				return nil
			}}
			home := evalTestPath(t, t.TempDir())
			paths := NewConfigPathResolver([]string{"CODEX_HOME=relative"}, home)
			configurator := NewConfigurator(&fakeLocator{path: "/fixed/codex"}, runner, test.inspector, paths, &fakeClaudeReader{})
			got, err := configurator.Configure(context.Background(), ClientCodex, desired, test.replace)
			if err != nil || got.CanConfigure || got.ManualCommand == "" {
				t.Fatalf("Configure() = %#v, %v", got, err)
			}
		})
	}
}

func TestConfiguratorRejectsMismatchedEnvironmentSnapshots(t *testing.T) {
	desired := testDesired(t)
	home := evalTestPath(t, t.TempDir())
	configPath := filepath.Join(home, "inside", "config.toml")
	resolverSnapshot := newEnvironmentSnapshot(
		[]string{"CODEX_HOME=" + filepath.Dir(configPath)},
		runtime.GOOS,
	)
	runnerSnapshot := newEnvironmentSnapshot(
		[]string{"CODEX_HOME=" + filepath.Join(home, "different")},
		runtime.GOOS,
	)
	runner := &explicitEnvironmentRunner{
		snapshot: runnerSnapshot,
		run: func() {
			t.Fatal("runner invoked with a mismatched environment snapshot")
		},
	}
	configurator := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		&fakeInspector{},
		NewConfigPathResolverWithSnapshot(resolverSnapshot, home),
		&fakeClaudeReader{},
	)

	got, err := configurator.Configure(context.Background(), ClientCodex, desired, false)
	if err != nil || got.Status != StatusError || got.CanConfigure || got.ManualCommand == "" {
		t.Fatalf("Configure() = %#v, %v", got, err)
	}
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0", runner.calls)
	}
}

func TestConfiguratorRejectsCodexInspectorEnvironmentMismatch(t *testing.T) {
	desired := testDesired(t)
	home := evalTestPath(t, t.TempDir())
	resolverSnapshot := newEnvironmentSnapshot(
		[]string{"CODEX_HOME=" + filepath.Join(home, "inside")},
		runtime.GOOS,
	)
	inspectorSnapshot := newEnvironmentSnapshot(
		[]string{"CODEX_HOME=" + filepath.Join(home, "different")},
		runtime.GOOS,
	)
	mutationRunner := &explicitEnvironmentRunner{snapshot: resolverSnapshot}
	inspectorRunner := &explicitEnvironmentRunner{
		snapshot: inspectorSnapshot,
		run: func() {
			t.Fatal("Codex inspector invoked with a mismatched environment snapshot")
		},
	}
	configurator := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		mutationRunner,
		NewCodexInspector(inspectorRunner),
		NewConfigPathResolverWithSnapshot(resolverSnapshot, home),
		&fakeClaudeReader{},
	)

	got, err := configurator.Status(context.Background(), ClientCodex, desired)
	if err != nil || got.Status != StatusError || got.CanConfigure {
		t.Fatalf("Status() = %#v, %v", got, err)
	}
	if mutationRunner.calls != 0 || inspectorRunner.calls != 0 {
		t.Fatalf("runner calls = mutation %d, inspector %d", mutationRunner.calls, inspectorRunner.calls)
	}
}

func TestConfiguratorWindowsIsManualOnlyBeforeConfigSnapshot(t *testing.T) {
	desired := testDesired(t)
	home := evalTestPath(t, t.TempDir())
	override := filepath.Join(home, "mixed")
	snapshot := newEnvironmentSnapshot([]string{"Codex_Home=" + override}, "windows")
	inspector := &fakeInspector{}
	runner := &explicitEnvironmentRunner{snapshot: snapshot}
	runner.run = func() {
		t.Fatal("Windows manual-only path invoked a client command")
	}
	paths := &fakeConfigPaths{}
	reader := &fakeClaudeReader{}
	configurator := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		inspector,
		paths,
		reader,
	)

	status, err := configurator.Status(context.Background(), ClientCodex, desired)
	if err != nil || status.CanConfigure || status.Status != StatusNotConfigured {
		t.Fatalf("Status() = %#v, %v", status, err)
	}
	got, err := configurator.Configure(context.Background(), ClientCodex, desired, false)
	if err != nil || got.CanConfigure || got.Status != StatusNotConfigured {
		t.Fatalf("Configure() = %#v, %v", got, err)
	}
	if runner.calls != 0 || inspector.calls != 0 || paths.calls.Load() != 0 || reader.calls != 0 {
		t.Fatalf(
			"manual-only calls = runner %d inspector %d paths %d reader %d",
			runner.calls,
			inspector.calls,
			paths.calls.Load(),
			reader.calls,
		)
	}
}

func TestConfiguratorUnsupportedFilesystemIsManualBeforeStaging(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	writeTask3File(t, configPath, []byte("existing"), 0o600)
	runner := &fakeRunner{run: func([]string) error {
		t.Fatal("unsupported filesystem invoked a client command")
		return nil
	}}
	instance := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		runner,
		&fakeInspector{},
		&fakeConfigPaths{path: configPath},
		&fakeClaudeReader{},
	).(*configurator)
	store := &unsupportedAtomicStore{backupStore: newFileBackupStore()}
	instance.backups = store

	status, err := instance.Status(context.Background(), ClientCodex, desired)
	if err != nil || status.Status != StatusNotConfigured || status.CanConfigure {
		t.Fatalf("Status() = %#v, %v", status, err)
	}
	if store.stageCalls != 0 || runner.max.Load() != 0 {
		t.Fatalf("unsupported path staged %d times and ran %d commands", store.stageCalls, runner.max.Load())
	}
	if store.snapshot == nil {
		t.Fatal("snapshot was not captured for filesystem classification")
	}
	if _, err := os.Stat(store.backupPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manual-only backup retained: %v", err)
	}

	configurePath := filepath.Join(t.TempDir(), "config.toml")
	writeTask3File(t, configurePath, []byte("existing"), 0o600)
	configure := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		&fakeRunner{},
		&fakeInspector{},
		&fakeConfigPaths{path: configurePath},
		&fakeClaudeReader{},
	).(*configurator)
	configure.backups = &unsupportedAtomicStore{backupStore: newFileBackupStore()}
	result, err := configure.Configure(context.Background(), ClientCodex, desired, false)
	if err != nil || result.Status != StatusNotConfigured || result.CanConfigure {
		t.Fatalf("Configure() = %#v, %v", result, err)
	}
}

func TestConfiguratorMapsCaptureRepairAndClientAbsenceWithoutMutation(t *testing.T) {
	desired := testDesired(t)
	for _, operation := range []string{"status", "configure"} {
		t.Run("repair "+operation, func(t *testing.T) {
			instance := NewConfigurator(
				&fakeLocator{path: "/fixed/codex"},
				&fakeRunner{},
				&fakeInspector{},
				&fakeConfigPaths{path: filepath.Join(t.TempDir(), "config.toml")},
				&fakeClaudeReader{},
			).(*configurator)
			instance.backups = captureErrorStore{
				backupStore: newFileBackupStore(),
				err:         ErrRepairRequired,
			}
			var result Integration
			var err error
			if operation == "status" {
				result, err = instance.Status(context.Background(), ClientCodex, desired)
			} else {
				result, err = instance.Configure(context.Background(), ClientCodex, desired, false)
			}
			if err != nil || result.Status != StatusRepairRequired {
				t.Fatalf("%s = %#v, %v", operation, result, err)
			}
		})
	}

	instance := NewConfigurator(
		&fakeLocator{err: ErrClientNotFound},
		&fakeRunner{},
		&fakeInspector{},
		&fakeConfigPaths{},
		&fakeClaudeReader{},
	)
	result, err := instance.Configure(context.Background(), ClientCodex, desired, false)
	if err != nil || result.Status != StatusClientNotFound {
		t.Fatalf("Configure absent client = %#v, %v", result, err)
	}
}

func TestConfiguratorStatusBackupCleanupFailureDoesNotClaimStatus(t *testing.T) {
	desired := testDesired(t)
	configPath := filepath.Join(t.TempDir(), "config.toml")
	writeTask3File(t, configPath, []byte("existing"), 0o600)
	instance := NewConfigurator(
		&fakeLocator{path: "/fixed/codex"},
		&fakeRunner{},
		&fakeInspector{},
		&fakeConfigPaths{path: configPath},
		&fakeClaudeReader{},
	).(*configurator)
	instance.backups = deleteFailureAfterStageStore{backupStore: newFileBackupStore()}

	result, err := instance.Status(context.Background(), ClientCodex, desired)
	if err != nil || result.Status != StatusError || result.Message != ErrRollbackFailed.Error() {
		t.Fatalf("Status() = %#v, %v", result, err)
	}
}

func TestConfiguratorDefensiveErrorBranches(t *testing.T) {
	current := &configurator{}
	ctx := context.Background()
	if _, _, err := current.inspectStaged(ctx, ClientCodex, nil, "", ""); !errors.Is(err, ErrClientCommand) {
		t.Fatalf("nil Codex inspector error = %v", err)
	}
	if _, _, err := current.inspectStaged(ctx, ClientClaudeCode, nil, "", ""); !errors.Is(err, ErrClientCommand) {
		t.Fatalf("nil Claude reader error = %v", err)
	}
	if _, _, err := current.inspectStaged(ctx, Client("unknown"), nil, "", ""); !errors.Is(err, ErrClientCommand) {
		t.Fatalf("unknown client inspect error = %v", err)
	}
	if _, err := current.runnerForStage(nil, ClientCodex, t.TempDir()); !errors.Is(err, ErrClientCommand) {
		t.Fatalf("unscoped runner error = %v", err)
	}
	if err := current.add(ctx, nil, "", ClientCodex, DesiredServer{}); !errors.Is(err, ErrClientCommand) {
		t.Fatalf("nil add runner error = %v", err)
	}
	if err := current.remove(ctx, nil, "", ClientCodex); !errors.Is(err, ErrClientCommand) {
		t.Fatalf("nil remove runner error = %v", err)
	}

	darwin := &explicitEnvironmentRunner{
		snapshot: newEnvironmentSnapshot([]string{"A=1"}, "darwin"),
	}
	linux := &explicitEnvironmentRunner{
		snapshot: newEnvironmentSnapshot([]string{"A=1"}, "linux"),
	}
	differentLength := &explicitEnvironmentRunner{
		snapshot: newEnvironmentSnapshot([]string{"A=1", "B=2"}, "darwin"),
	}
	if !snapshotsConflict(darwin, linux) {
		t.Fatal("different GOOS snapshots did not conflict")
	}
	if !snapshotsConflict(darwin, differentLength) {
		t.Fatal("different environment lengths did not conflict")
	}
}

func testDesired(t *testing.T) DesiredServer {
	t.Helper()
	command := filepath.Join(t.TempDir(), "cy-kaf-client")
	writeTask3File(t, command, []byte("binary"), 0o700)
	return DesiredServer{Name: ServerName, Command: command, Args: []string{"mcp", "--config", "/tmp/config with spaces.yaml"}}
}

func desiredEntry(desired DesiredServer) Entry {
	return Entry{Name: ServerName, Transport: "stdio", Enabled: true, Command: desired.Command, Args: cloneStrings(desired.Args)}
}

func claudeConfigBytes(t *testing.T, desired DesiredServer, unrelated bool) []byte {
	t.Helper()
	value := map[string]any{
		"mcpServers": map[string]any{
			ServerName: map[string]any{
				"type":    "stdio",
				"command": desired.Command,
				"args":    desired.Args,
			},
		},
	}
	if unrelated {
		value["unrelated"] = "sibling-secret-marker"
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func cloneEntry(entry Entry) Entry {
	entry.Args = cloneStrings(entry.Args)
	entry.Env = cloneStringMap(entry.Env)
	entry.EnvVars = cloneStrings(entry.EnvVars)
	return entry
}

func withEnabled(entry Entry, enabled bool) Entry {
	entry.Enabled = enabled
	return entry
}

func withEnv(entry Entry, env map[string]string) Entry {
	entry.Env = env
	return entry
}

func withEnvVars(entry Entry, envVars []string) Entry {
	entry.EnvVars = envVars
	return entry
}

func withCWD(entry Entry, cwd string) Entry {
	entry.CWD = cwd
	return entry
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func assertRunnerCalls(t *testing.T, runner *fakeRunner, want ...[]string) {
	t.Helper()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.calls) != len(want) {
		t.Fatalf("runner calls = %#v, want %#v", runner.calls, want)
	}
	for index := range want {
		if !equalStrings(runner.calls[index], want[index]) {
			t.Fatalf("runner call %d = %#v, want %#v", index, runner.calls[index], want[index])
		}
	}
}

func assertNoBackupFiles(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".backup" {
			t.Fatalf("backup remains: %s", entry.Name())
		}
	}
}
