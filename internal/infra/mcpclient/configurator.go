package mcpclient

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type Status string

const (
	StatusClientNotFound Status = "client_not_found"
	StatusNotConfigured  Status = "not_configured"
	StatusConfigured     Status = "configured"
	StatusConflict       Status = "configuration_conflict"
	StatusRepairRequired Status = "repair_required"
	StatusError          Status = "error"
)

type Integration struct {
	Client        Client
	Status        Status
	CanConfigure  bool
	ManualCommand string
	Message       string
}

type Configurator interface {
	Status(context.Context, Client, DesiredServer) (Integration, error)
	Configure(context.Context, Client, DesiredServer, bool) (Integration, error)
}

type configurator struct {
	locator             Locator
	runner              Runner
	codex               CodexInspector
	configPaths         ConfigPathResolver
	claude              ClaudeEntryReader
	backups             backupStore
	environmentMismatch bool
	locksMu             sync.Mutex
	locks               map[Client]*sync.Mutex
}

func NewConfigurator(
	locator Locator,
	runner Runner,
	codex CodexInspector,
	configPaths ConfigPathResolver,
	claude ClaudeEntryReader,
) Configurator {
	environmentMismatch := snapshotsConflict(configPaths, runner) ||
		snapshotsConflict(configPaths, codex)
	return &configurator{
		locator: locator, runner: runner, codex: codex,
		configPaths: configPaths, claude: claude,
		backups: newFileBackupStore(), environmentMismatch: environmentMismatch,
		locks: make(map[Client]*sync.Mutex),
	}
}

func (current *configurator) Status(
	ctx context.Context,
	client Client,
	desired DesiredServer,
) (Integration, error) {
	if current.environmentMismatch {
		return current.manualOnly(client, desired, ErrInvalidConfig), nil
	}
	if !current.automaticConfigurationSupported() {
		if _, err := current.locator.Find(client); errors.Is(err, ErrClientNotFound) {
			return current.integration(client, desired, StatusClientNotFound, false, ErrClientNotFound), nil
		} else if err != nil {
			return current.safeError(client, desired, ErrClientCommand), nil
		}
		return current.integration(client, desired, StatusNotConfigured, false, nil), nil
	}
	lock := current.clientLock(client)
	lock.Lock()
	defer lock.Unlock()
	entry, found, _, snapshot, _, err := current.inspectSnapshot(ctx, client)
	if errors.Is(err, ErrClientNotFound) {
		return current.integration(client, desired, StatusClientNotFound, false, ErrClientNotFound), nil
	}
	if errors.Is(err, ErrRepairRequired) {
		closeRetained(snapshot)
		return current.integration(client, desired, StatusRepairRequired, false, ErrRepairRequired), nil
	}
	if errors.Is(err, ErrAutomaticUnavailable) {
		return current.integration(client, desired, StatusNotConfigured, false, nil), nil
	}
	if err != nil {
		if snapshot != nil {
			_ = current.discardStaged(ctx, snapshot, err)
		}
		return current.safeError(client, desired, ErrClientCommand), nil
	}
	if snapshot != nil {
		if err := snapshot.Delete(); err != nil {
			return current.safeError(client, desired, ErrRollbackFailed), nil
		}
	}
	result := current.classify(client, desired, entry, found)
	if !current.automaticConfigurationSupported() {
		result.CanConfigure = false
	}
	return result, nil
}

func (current *configurator) Configure(
	ctx context.Context,
	client Client,
	desired DesiredServer,
	replace bool,
) (Integration, error) {
	if current.environmentMismatch {
		return current.manualOnly(client, desired, ErrInvalidConfig), nil
	}
	if !current.automaticConfigurationSupported() {
		if _, err := current.locator.Find(client); errors.Is(err, ErrClientNotFound) {
			return current.integration(client, desired, StatusClientNotFound, false, ErrClientNotFound), nil
		} else if err != nil {
			return current.safeError(client, desired, ErrClientCommand), nil
		}
		return current.integration(client, desired, StatusNotConfigured, false, nil), nil
	}
	lock := current.clientLock(client)
	lock.Lock()
	defer lock.Unlock()

	before, found, executable, snapshot, stagedRunner, err := current.inspectSnapshot(ctx, client)
	if errors.Is(err, ErrClientNotFound) {
		return current.integration(client, desired, StatusClientNotFound, false, ErrClientNotFound), nil
	}
	if errors.Is(err, ErrRepairRequired) {
		closeRetained(snapshot)
		return current.integration(client, desired, StatusRepairRequired, false, ErrRepairRequired), nil
	}
	if errors.Is(err, ErrAutomaticUnavailable) {
		return current.integration(client, desired, StatusNotConfigured, false, nil), nil
	}
	if err != nil {
		if snapshot != nil {
			_ = current.discardStaged(ctx, snapshot, err)
		}
		return current.safeError(client, desired, ErrClientCommand), nil
	}
	classification := current.classify(client, desired, before, found)
	if !current.automaticConfigurationSupported() {
		_ = snapshot.Delete()
		classification.CanConfigure = false
		return classification, nil
	}
	if classification.Status == StatusConfigured ||
		(found && !replace) {
		if err := snapshot.Delete(); err != nil {
			return current.safeError(client, desired, ErrRollbackFailed), nil
		}
		return classification, nil
	}
	if err := current.backups.CheckBaseline(ctx, snapshot); err != nil {
		closeRetained(snapshot)
		return Integration{}, err
	}
	if found {
		if err := current.remove(ctx, stagedRunner, executable, client); err != nil {
			return Integration{}, current.discardStaged(ctx, snapshot, err)
		}
	}
	if err := current.add(ctx, stagedRunner, executable, client, desired); err != nil {
		return Integration{}, current.discardStaged(ctx, snapshot, err)
	}
	after, verified, inspectErr := current.inspectStaged(
		ctx,
		client,
		stagedRunner,
		snapshot.stagePath,
		executable,
	)
	if inspectErr != nil || !verified ||
		!sameEntry(after, desired) {
		return Integration{}, current.discardStaged(ctx, snapshot, ErrVerification)
	}
	if err := current.backups.Publish(ctx, snapshot); err != nil {
		closeRetained(snapshot)
		return Integration{}, err
	}
	return current.integration(client, desired, StatusConfigured, false, nil), nil
}

func (current *configurator) inspectSnapshot(
	ctx context.Context,
	client Client,
) (Entry, bool, string, *fileSnapshot, Runner, error) {
	if current == nil || current.locator == nil || current.configPaths == nil {
		return Entry{}, false, "", nil, nil, ErrClientCommand
	}
	executable, launchRunner, err := current.findClientRunner(client)
	if err != nil {
		return Entry{}, false, "", nil, nil, err
	}
	target, err := current.configPaths.ResolveTarget(client)
	if err != nil {
		return Entry{}, false, executable, nil, nil, err
	}
	snapshot, err := current.backups.CaptureTarget(ctx, target)
	if err != nil {
		return Entry{}, false, executable, snapshot, nil, err
	}
	if !current.backups.SupportsAtomic(snapshot) {
		if err := snapshot.Delete(); err != nil {
			closeRetained(snapshot)
			return Entry{}, false, executable, snapshot, nil, ErrRollbackFailed
		}
		return Entry{}, false, executable, nil, nil, ErrAutomaticUnavailable
	}
	stagePath, err := current.backups.Stage(ctx, snapshot, client)
	if err != nil {
		snapshot.Keep()
		return Entry{}, false, executable, snapshot, nil, err
	}
	stagedRunner, err := current.runnerForStage(
		launchRunner,
		client,
		filepath.Dir(stagePath),
	)
	if err != nil {
		snapshot.Keep()
		return Entry{}, false, executable, snapshot, nil, err
	}
	entry, found, err := current.inspectStaged(ctx, client, stagedRunner, stagePath, executable)
	return entry, found, executable, snapshot, stagedRunner, err
}

func (current *configurator) findClientRunner(
	client Client,
) (string, Runner, error) {
	if current == nil || current.locator == nil || current.runner == nil {
		return "", nil, ErrClientCommand
	}
	launchLocator, ok := current.locator.(clientLaunchLocator)
	if !ok {
		executable, err := current.locator.Find(client)
		return executable, current.runner, err
	}
	executable, directory, err := launchLocator.FindWithLaunchDirectory(client)
	if err != nil {
		return "", nil, err
	}
	directoryRunner, ok := current.runner.(executableDirectoryRunner)
	if !ok {
		return "", nil, ErrClientCommand
	}
	scoped, err := directoryRunner.WithExecutableDirectory(directory)
	if err != nil {
		return "", nil, err
	}
	return executable, scoped, nil
}

func (current *configurator) inspectStaged(
	ctx context.Context,
	client Client,
	runner Runner,
	stagePath string,
	executable string,
) (Entry, bool, error) {
	switch client {
	case ClientCodex:
		inspector := current.codex
		if _, production := inspector.(*codexInspector); production {
			inspector = NewCodexInspector(runner)
		}
		if inspector == nil {
			return Entry{}, false, ErrClientCommand
		}
		return inspector.Get(ctx, executable)
	case ClientClaudeCode:
		if current.claude == nil {
			return Entry{}, false, ErrClientCommand
		}
		return current.claude.ReadClaude(stagePath)
	default:
		return Entry{}, false, ErrClientCommand
	}
}

func (current *configurator) runnerForStage(
	runner Runner,
	client Client,
	directory string,
) (Runner, error) {
	scoper, ok := runner.(scopedEnvironmentRunner)
	if !ok {
		return nil, ErrClientCommand
	}
	key := "CODEX_HOME"
	if client == ClientClaudeCode {
		key = "CLAUDE_CONFIG_DIR"
	}
	return scoper.WithEnvironment(map[string]string{key: directory})
}

func (current *configurator) discardStaged(
	ctx context.Context,
	snapshot *fileSnapshot,
	cause error,
) error {
	if err := current.backups.CheckBaseline(ctx, snapshot); err != nil {
		closeRetained(snapshot)
		return err
	}
	if err := snapshot.Delete(); err != nil {
		return ErrRollbackFailed
	}
	return cause
}

func closeRetained(snapshot *fileSnapshot) {
	if snapshot != nil {
		snapshot.CloseRetained()
	}
}

func (current *configurator) automaticConfigurationSupported() bool {
	provider, ok := current.runner.(EnvironmentSnapshotProvider)
	if ok && provider.EnvironmentSnapshot() != nil {
		goos := provider.EnvironmentSnapshot().environmentGOOS()
		return goos == "darwin" || goos == "linux"
	}
	return runtime.GOOS == "darwin" || runtime.GOOS == "linux"
}

func (inspector *codexInspector) EnvironmentSnapshot() EnvironmentSnapshot {
	if inspector == nil {
		return nil
	}
	provider, ok := inspector.runner.(EnvironmentSnapshotProvider)
	if !ok {
		return nil
	}
	return provider.EnvironmentSnapshot()
}

func snapshotsConflict(left, right any) bool {
	leftProvider, leftOK := left.(EnvironmentSnapshotProvider)
	rightProvider, rightOK := right.(EnvironmentSnapshotProvider)
	if !leftOK || !rightOK {
		return false
	}
	leftSnapshot := leftProvider.EnvironmentSnapshot()
	rightSnapshot := rightProvider.EnvironmentSnapshot()
	if leftSnapshot == nil || rightSnapshot == nil {
		return leftSnapshot != rightSnapshot
	}
	if leftSnapshot.environmentGOOS() != rightSnapshot.environmentGOOS() {
		return true
	}
	leftEntries := leftSnapshot.environmentEntries()
	rightEntries := rightSnapshot.environmentEntries()
	if len(leftEntries) != len(rightEntries) {
		return true
	}
	for index := range leftEntries {
		if leftEntries[index] != rightEntries[index] {
			return true
		}
	}
	return false
}

func (current *configurator) classify(
	client Client,
	desired DesiredServer,
	entry Entry,
	found bool,
) Integration {
	if !found {
		return current.integration(client, desired, StatusNotConfigured, true, nil)
	}
	if sameEntry(entry, desired) {
		return current.integration(client, desired, StatusConfigured, false, nil)
	}
	if repairEntry(entry, desired) {
		return current.integration(client, desired, StatusRepairRequired, true, nil)
	}
	return current.integration(client, desired, StatusConflict, true, nil)
}

func sameEntry(entry Entry, desired DesiredServer) bool {
	return entry.Name == ServerName &&
		entry.Transport == "stdio" &&
		entry.Enabled &&
		cleanAbsoluteEqual(entry.Command, desired.Command) &&
		equalStringSlices(entry.Args, desired.Args) &&
		len(entry.Env) == 0 &&
		len(entry.EnvVars) == 0 &&
		entry.CWD == ""
}

func repairEntry(entry Entry, desired DesiredServer) bool {
	if entry.Name != ServerName ||
		entry.Transport != "stdio" ||
		!equalStringSlices(entry.Args, desired.Args) ||
		len(entry.Env) != 0 ||
		len(entry.EnvVars) != 0 ||
		entry.CWD != "" {
		return false
	}
	if cleanAbsoluteEqual(entry.Command, desired.Command) {
		return !entry.Enabled
	}
	if !filepath.IsAbs(entry.Command) {
		return false
	}
	if filepath.Base(filepath.Clean(entry.Command)) != filepath.Base(filepath.Clean(desired.Command)) {
		return false
	}
	_, err := os.Stat(filepath.Clean(entry.Command))
	return errors.Is(err, os.ErrNotExist)
}

func cleanAbsoluteEqual(left, right string) bool {
	return filepath.IsAbs(left) &&
		filepath.IsAbs(right) &&
		filepath.Clean(left) == filepath.Clean(right)
}

func equalStringSlices(left, right []string) bool {
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

func (current *configurator) add(
	ctx context.Context,
	runner Runner,
	executable string,
	client Client,
	desired DesiredServer,
) error {
	if runner == nil || executable == "" {
		return ErrClientCommand
	}
	_, err := runner.Run(ctx, executable, AddArgs(client, desired))
	if err != nil {
		return ErrClientCommand
	}
	return nil
}

func (current *configurator) remove(
	ctx context.Context,
	runner Runner,
	executable string,
	client Client,
) error {
	if runner == nil || executable == "" {
		return ErrClientCommand
	}
	_, err := runner.Run(ctx, executable, RemoveArgs(client))
	if err != nil {
		return ErrClientCommand
	}
	return nil
}

func (current *configurator) clientLock(client Client) *sync.Mutex {
	current.locksMu.Lock()
	defer current.locksMu.Unlock()
	lock := current.locks[client]
	if lock == nil {
		lock = &sync.Mutex{}
		current.locks[client] = lock
	}
	return lock
}

func (current *configurator) integration(
	client Client,
	desired DesiredServer,
	status Status,
	canConfigure bool,
	message error,
) Integration {
	result := Integration{
		Client: client, Status: status, CanConfigure: canConfigure,
		ManualCommand: ManualCommand(client, desired, runtime.GOOS),
	}
	if message != nil {
		result.Message = message.Error()
	}
	return result
}

func (current *configurator) manualOnly(
	client Client,
	desired DesiredServer,
	_ error,
) Integration {
	return current.integration(client, desired, StatusError, false, ErrInvalidConfig)
}

func (current *configurator) safeError(
	client Client,
	desired DesiredServer,
	err error,
) Integration {
	return current.integration(client, desired, StatusError, false, err)
}
