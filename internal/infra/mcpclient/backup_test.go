package mcpclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func captureTestTarget(
	t *testing.T,
	ctx context.Context,
	store backupStore,
	path string,
) (*fileSnapshot, error) {
	t.Helper()
	cleanPath := filepath.Clean(path)
	homePath := filepath.Dir(cleanPath)
	root, err := os.OpenRoot(homePath)
	if err != nil {
		return nil, err
	}
	homeInfo, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	snapshot, err := store.CaptureTarget(ctx, &configTarget{
		path:     cleanPath,
		homePath: homePath,
		relative: filepath.Base(cleanPath),
		root:     root,
		homeInfo: homeInfo,
	})
	if snapshot == nil {
		_ = root.Close()
	}
	return snapshot, err
}

func TestPublishSingleExchangeRetainsDisplacedBaselineOnExternalReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	store := &fileBackupStore{}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Delete() })
	stagePath, err := store.Stage(context.Background(), snapshot, ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, stagePath, []byte("staged"), 0o600)
	store.afterPublish = func(current *fileSnapshot) {
		replacement := filepath.Join(filepath.Dir(path), "external-replacement")
		writeTask3File(t, replacement, []byte("external"), 0o600)
		if err := os.Rename(replacement, path); err != nil {
			t.Fatal(err)
		}
	}

	err = store.Publish(context.Background(), snapshot)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Publish() error = %v, want ErrConcurrentModification", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != "external" {
		t.Fatalf("external target changed: %q, %v", got, readErr)
	}
	assertRetainedTransactionMaterial(t, snapshot)
	displaced, readErr := snapshot.root.ReadFile(snapshot.displacedName)
	if readErr != nil || string(displaced) != "baseline" {
		t.Fatalf("displaced baseline = %q, %v", displaced, readErr)
	}
}

func TestPublishSingleExchangeRetainsRecoveryOnExternalInPlaceWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	store := &fileBackupStore{}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Delete() })
	stagePath, err := store.Stage(context.Background(), snapshot, ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, stagePath, []byte("staged"), 0o600)
	store.afterPublish = func(current *fileSnapshot) {
		if err := current.root.WriteFile(current.targetName, []byte("external-in-place"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	err = store.Publish(context.Background(), snapshot)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Publish() error = %v, want ErrConcurrentModification", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != "external-in-place" {
		t.Fatalf("external target changed: %q, %v", got, readErr)
	}
	assertRetainedTransactionMaterial(t, snapshot)
}

func TestPublishPreservesExistingUnixMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix mode bits")
	}
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o640)
	store := &fileBackupStore{}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Delete() })
	stagePath, err := store.Stage(context.Background(), snapshot, ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, stagePath, []byte("staged"), 0o600)

	if err := store.Publish(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("published mode = %o, want 640", info.Mode().Perm())
	}
}

func TestPublishRejectsCandidateChangedAfterCopy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	store := &fileBackupStore{}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Delete() })
	stagePath, err := store.Stage(context.Background(), snapshot, ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, stagePath, []byte("staged"), 0o600)
	store.afterCandidate = func(current *fileSnapshot) {
		if err := current.root.WriteFile(current.candidateName, []byte("corrupt"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	err = store.Publish(context.Background(), snapshot)
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("Publish() error = %v, want ErrConcurrentModification", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != "baseline" {
		t.Fatalf("baseline changed: %q, %v", got, readErr)
	}
}

func TestRetainClosesHandlesWithoutDeletingBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	snapshot, err := captureTestTarget(t, context.Background(), newFileBackupStore(), path)
	if err != nil {
		t.Fatal(err)
	}
	backupPath := snapshot.backupPath

	snapshot.CloseRetained()

	if snapshot.root != nil || snapshot.homeRoot != nil || snapshot.parent != nil {
		t.Fatal("retained snapshot leaked open handles")
	}
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("retained backup missing: %v", err)
	}
}

func TestCopyBoundedHonorsBoundaryOversizeAndCancellation(t *testing.T) {
	var output bytes.Buffer
	if err := copyBounded(
		context.Background(),
		&output,
		bytes.NewReader(make([]byte, maxClientConfigBytes)),
		maxClientConfigBytes,
	); err != nil {
		t.Fatalf("boundary copy: %v", err)
	}
	output.Reset()
	if err := copyBounded(
		context.Background(),
		&output,
		bytes.NewReader(make([]byte, maxClientConfigBytes+1)),
		maxClientConfigBytes,
	); !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("oversized copy error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := copyBounded(ctx, &output, bytes.NewReader([]byte("x")), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled copy error = %v", err)
	}
}

func TestCaptureConfigBoundaryAndOversizeArtifactCleanup(t *testing.T) {
	t.Run("exact boundary", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config")
		writeTask3File(t, path, make([]byte, maxClientConfigBytes), 0o600)
		snapshot, err := captureTestTarget(t, context.Background(), newFileBackupStore(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = snapshot.Delete() })
		info, err := os.Stat(snapshot.backupPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != maxClientConfigBytes {
			t.Fatalf("backup size = %d, want %d", info.Size(), maxClientConfigBytes)
		}
	})

	t.Run("grows beyond boundary", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "config")
		writeTask3File(t, path, make([]byte, maxClientConfigBytes+1), 0o600)
		snapshot, err := captureTestTarget(t, context.Background(), newFileBackupStore(), path)
		if !errors.Is(err, ErrRollbackFailed) {
			t.Fatalf("CaptureTarget() error = %v", err)
		}
		if snapshot != nil {
			snapshot.CloseRetained()
		}
		matches, globErr := filepath.Glob(filepath.Join(directory, ".mcp-config-*.backup"))
		if globErr != nil || len(matches) != 0 {
			t.Fatalf("oversized backup artifact = %v, %v", matches, globErr)
		}
	})

	t.Run("canceled before capture", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "config")
		writeTask3File(t, path, []byte("config"), 0o600)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		snapshot, err := captureTestTarget(t, ctx, newFileBackupStore(), path)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CaptureTarget() error = %v", err)
		}
		if snapshot != nil {
			snapshot.CloseRetained()
		}
		matches, globErr := filepath.Glob(filepath.Join(directory, ".mcp-config-*.backup"))
		if globErr != nil || len(matches) != 0 {
			t.Fatalf("canceled backup artifact = %v, %v", matches, globErr)
		}
	})
}

func TestCaptureAndStageRejectInvalidTargetsAndCancellation(t *testing.T) {
	store := &fileBackupStore{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := captureTestTarget(t, ctx, store, filepath.Join(t.TempDir(), "config")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CaptureTarget() error = %v", err)
	}
	if _, err := store.CaptureTarget(context.Background(), nil); !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("nil CaptureTarget() error = %v", err)
	}

	directory := t.TempDir()
	target := filepath.Join(directory, "config")
	link := filepath.Join(directory, "config-link")
	writeTask3File(t, target, []byte("config"), 0o600)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	snapshot, err := captureTestTarget(t, context.Background(), store, link)
	if !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("symlink CaptureTarget() error = %v", err)
	}
	if snapshot != nil {
		snapshot.CloseRetained()
	}

	snapshot, err = captureTestTarget(t, context.Background(), store, target)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Delete() })
	if _, err := store.Stage(ctx, snapshot, ClientCodex); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Stage() error = %v", err)
	}
	if _, err := store.Stage(context.Background(), nil, ClientCodex); !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("nil Stage() error = %v", err)
	}
}

func TestPublishHonorsCancellationBeforeRealConfigMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	store := &fileBackupStore{}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Delete() })
	stagePath, err := store.Stage(context.Background(), snapshot, ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, stagePath, []byte("staged"), 0o600)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = store.Publish(ctx, snapshot)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != "baseline" {
		t.Fatalf("baseline changed: %q, %v", got, readErr)
	}
}

func TestPendingJournalRejectsUntrustedRecoverySlots(t *testing.T) {
	snapshot := &fileSnapshot{
		targetName:  "config.toml",
		journalName: ".config.toml.cy-kaf-transaction.json",
	}
	validDigest := FileDigest{Exists: true, SHA256: [32]byte{1}}
	tests := []transactionJournal{
		{
			Version: 1, Target: snapshot.targetName,
			Candidate: "../outside", Displaced: "../outside",
			Backup: "../outside", Baseline: validDigest, Staged: validDigest,
			BaselineOwned: true,
		},
		{
			Version: 1, Target: snapshot.targetName,
			Candidate: ".mcp-config-" + strings.Repeat("a", 32) + ".candidate",
			Displaced: ".mcp-config-" + strings.Repeat("b", 32) + ".candidate",
			Backup:    ".mcp-config-" + strings.Repeat("c", 32) + ".backup",
			Baseline:  validDigest, Staged: validDigest, BaselineOwned: true,
		},
	}
	for _, journal := range tests {
		if validateJournal(snapshot, journal) {
			t.Fatalf("accepted untrusted journal slots: %#v", journal)
		}
	}
}

func TestPendingJournalRecoversProvablyUncommittedExistingTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	snapshot := preparePrepublishJournal(t, path, []byte("staged"))
	originalRecoveryPaths := []string{
		filepath.Join(filepath.Dir(path), snapshot.candidateName),
		filepath.Join(filepath.Dir(path), snapshot.backupName),
		filepath.Join(filepath.Dir(path), snapshot.journalName),
	}
	snapshot.CloseRetained()

	recovered, err := captureTestTarget(t, context.Background(), &fileBackupStore{}, path)
	if err != nil {
		t.Fatalf("CaptureTarget() after uncommitted journal error = %v", err)
	}
	t.Cleanup(func() { _ = recovered.Delete() })
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "baseline" {
		t.Fatalf("target after recovery = %q, %v", got, err)
	}
	backup, err := os.ReadFile(recovered.backupPath)
	if err != nil || string(backup) != "baseline" {
		t.Fatalf("new capture backup = %q, %v", backup, err)
	}
	for _, recoveryPath := range originalRecoveryPaths {
		if _, err := os.Lstat(recoveryPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("uncommitted recovery material remains at %s: %v", recoveryPath, err)
		}
	}
}

func TestPendingJournalRecoversProvablyUncommittedMissingTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	snapshot := preparePrepublishJournal(t, path, []byte("staged"))
	originalRecoveryPaths := []string{
		filepath.Join(filepath.Dir(path), snapshot.candidateName),
		filepath.Join(filepath.Dir(path), snapshot.journalName),
	}
	snapshot.CloseRetained()

	recovered, err := captureTestTarget(t, context.Background(), &fileBackupStore{}, path)
	if err != nil {
		t.Fatalf("CaptureTarget() after uncommitted journal error = %v", err)
	}
	t.Cleanup(func() { _ = recovered.Delete() })
	if recovered.existed {
		t.Fatal("missing target became an existing capture")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery created or changed target: %v", err)
	}
	for _, recoveryPath := range originalRecoveryPaths {
		if _, err := os.Lstat(recoveryPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("uncommitted recovery material remains at %s: %v", recoveryPath, err)
		}
	}
}

func TestPendingJournalRejectsUncommittedLayoutWithMismatchedCandidate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	snapshot := preparePrepublishJournal(t, path, []byte("staged"))
	if err := snapshot.root.WriteFile(snapshot.candidateName, []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	recoveryPaths := pendingRecoveryPaths(snapshot)
	snapshot.CloseRetained()

	next, err := captureTestTarget(t, context.Background(), &fileBackupStore{}, path)
	if !errors.Is(err, ErrRepairRequired) {
		t.Fatalf("CaptureTarget() error = %v, want ErrRepairRequired", err)
	}
	if next != nil {
		next.CloseRetained()
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != "baseline" {
		t.Fatalf("target after mismatched candidate = %q, %v", got, readErr)
	}
	assertPathsExist(t, recoveryPaths)
}

func TestPendingJournalFailsClosedAfterPartialUncommittedCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	snapshot := preparePrepublishJournal(t, path, []byte("staged"))
	if err := snapshot.root.Remove(snapshot.candidateName); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.parent.Sync(); err != nil {
		t.Fatal(err)
	}
	remainingRecoveryPaths := []string{
		snapshot.backupPath,
		filepath.Join(snapshot.parentPath, snapshot.journalName),
	}
	snapshot.CloseRetained()

	next, err := captureTestTarget(t, context.Background(), &fileBackupStore{}, path)
	if !errors.Is(err, ErrRepairRequired) {
		t.Fatalf("CaptureTarget() error = %v, want ErrRepairRequired", err)
	}
	if next != nil {
		next.CloseRetained()
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != "baseline" {
		t.Fatalf("target after partial cleanup = %q, %v", got, readErr)
	}
	assertPathsExist(t, remainingRecoveryPaths)
}

func TestPendingJournalRecoversProvablyCommittedPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	store := &fileBackupStore{stopAfterPublish: true}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	stagePath, err := store.Stage(context.Background(), snapshot, ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, stagePath, []byte("committed"), 0o600)
	recoveryPaths := []string{
		filepath.Join(filepath.Dir(path), snapshot.backupName),
		filepath.Join(filepath.Dir(path), snapshot.journalName),
	}

	if err := store.Publish(context.Background(), snapshot); !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("injected crash error = %v", err)
	}
	snapshot.CloseRetained()
	t.Cleanup(func() { _ = os.RemoveAll(snapshot.stageDir) })

	recovered, err := captureTestTarget(t, context.Background(), &fileBackupStore{}, path)
	if err != nil {
		t.Fatalf("Capture after committed journal = %v", err)
	}
	t.Cleanup(func() { _ = recovered.Delete() })
	for _, recoveryPath := range recoveryPaths {
		if _, err := os.Lstat(recoveryPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale recovery material remains at %s: %v", recoveryPath, err)
		}
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "committed" {
		t.Fatalf("committed bytes = %q, %v", got, err)
	}
}

func TestPendingJournalKeepsAmbiguousExternalEditForRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	store := &fileBackupStore{stopAfterPublish: true}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	stagePath, err := store.Stage(context.Background(), snapshot, ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, stagePath, []byte("committed"), 0o600)
	store.afterPublish = func(*fileSnapshot) {
		writeTask3File(t, path, []byte("external"), 0o600)
	}
	journalPath := filepath.Join(filepath.Dir(path), snapshot.journalName)

	if err := store.Publish(context.Background(), snapshot); !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("injected crash error = %v", err)
	}
	snapshot.CloseRetained()
	t.Cleanup(func() {
		_ = os.RemoveAll(snapshot.stageDir)
		_ = os.Remove(journalPath)
	})

	next, err := captureTestTarget(t, context.Background(), &fileBackupStore{}, path)
	if !errors.Is(err, ErrRepairRequired) {
		t.Fatalf("Capture after ambiguous journal = %v", err)
	}
	if next != nil {
		next.CloseRetained()
	}
	if _, err := os.Stat(journalPath); err != nil {
		t.Fatalf("ambiguous journal was removed: %v", err)
	}
	assertPathsExist(t, pendingRecoveryPaths(snapshot))
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "external" {
		t.Fatalf("external bytes = %q, %v", got, err)
	}
}

func TestPendingJournalKeepsCommittedPublishWithMismatchedBackupForRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	writeTask3File(t, path, []byte("baseline"), 0o600)
	store := &fileBackupStore{stopAfterPublish: true}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	stagePath, err := store.Stage(context.Background(), snapshot, ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	writeTask3File(t, stagePath, []byte("committed"), 0o600)
	store.afterPublish = func(current *fileSnapshot) {
		if err := current.root.WriteFile(current.backupName, []byte("other"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.Publish(context.Background(), snapshot); !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("injected crash error = %v", err)
	}
	recoveryPaths := pendingRecoveryPaths(snapshot)
	snapshot.CloseRetained()
	t.Cleanup(func() { _ = os.RemoveAll(snapshot.stageDir) })

	next, err := captureTestTarget(t, context.Background(), &fileBackupStore{}, path)
	if !errors.Is(err, ErrRepairRequired) {
		t.Fatalf("CaptureTarget() error = %v, want ErrRepairRequired", err)
	}
	if next != nil {
		next.CloseRetained()
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil || string(got) != "committed" {
		t.Fatalf("committed target was compensated: %q, %v", got, readErr)
	}
	assertPathsExist(t, recoveryPaths)
}

func preparePrepublishJournal(
	t *testing.T,
	path string,
	staged []byte,
) *fileSnapshot {
	t.Helper()
	store := &fileBackupStore{}
	snapshot, err := captureTestTarget(t, context.Background(), store, path)
	if err != nil {
		t.Fatal(err)
	}
	candidateName, candidate, err := createPrivateRootFile(
		snapshot.root,
		".mcp-config-",
		".candidate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := candidate.Write(staged); err != nil {
		_ = candidate.Close()
		t.Fatal(err)
	}
	if snapshot.existed {
		if err := candidate.Chmod(snapshot.mode.Perm()); err != nil {
			_ = candidate.Close()
			t.Fatal(err)
		}
	}
	if err := candidate.Sync(); err != nil {
		_ = candidate.Close()
		t.Fatal(err)
	}
	if err := candidate.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot.candidateName = candidateName
	if snapshot.existed {
		snapshot.displacedName = candidateName
	}
	stagedDigest := FileDigest{Exists: true, SHA256: sha256.Sum256(staged)}
	if err := writeJournal(context.Background(), snapshot, transactionJournal{
		Version:       1,
		Target:        snapshot.targetName,
		Candidate:     snapshot.candidateName,
		Displaced:     snapshot.displacedName,
		Backup:        snapshot.backupName,
		Baseline:      snapshot.baseline,
		Staged:        stagedDigest,
		BaselineMode:  uint32(snapshot.mode.Perm()),
		BaselineOwned: snapshot.existed,
	}); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func pendingRecoveryPaths(snapshot *fileSnapshot) []string {
	names := []string{
		snapshot.candidateName,
		snapshot.backupName,
		snapshot.journalName,
	}
	paths := make([]string, 0, len(names))
	for _, name := range names {
		if name != "" {
			paths = append(paths, filepath.Join(snapshot.parentPath, name))
		}
	}
	return paths
}

func assertPathsExist(t *testing.T, paths []string) {
	t.Helper()
	for _, path := range paths {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("recovery material missing at %s: %v", path, err)
		}
	}
}

func TestEnsureRootDirectoriesRejectsParentTraversal(t *testing.T) {
	home := t.TempDir()
	root, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()

	if err := ensureRootDirectories(root, filepath.Join("..", "outside")); !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("ensureRootDirectories() error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(home, "outside")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("traversal component was silently normalized: %v", err)
	}
}

func assertRetainedTransactionMaterial(t *testing.T, snapshot *fileSnapshot) {
	t.Helper()
	for name, path := range map[string]string{
		"backup":    snapshot.backupName,
		"displaced": snapshot.displacedName,
		"journal":   snapshot.journalName,
	} {
		if path == "" {
			t.Fatalf("%s recovery name is empty", name)
		}
		if _, err := snapshot.root.Lstat(path); err != nil {
			t.Fatalf("%s recovery material missing: %v", name, err)
		}
	}
}

func TestBackupCapturesPrivateExactBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	original := []byte("sibling-secret-marker\x00\n")
	writeTask3File(t, path, original, 0o640)

	snapshot, err := captureTestTarget(t, context.Background(), newFileBackupStore(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Delete() })
	info, err := os.Stat(snapshot.backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("backup mode = %o, want 600", info.Mode().Perm())
	}
	got, err := os.ReadFile(snapshot.backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(original) {
		t.Fatalf("backup bytes differ")
	}
}

func writeTask3File(t *testing.T, path string, value []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, value, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}
