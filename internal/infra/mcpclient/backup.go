package mcpclient

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxClientConfigBytes int64 = 8 << 20

var (
	ErrVerification           = errors.New("verification_failed")
	ErrConcurrentModification = errors.New("concurrent_modification")
	ErrRollbackFailed         = errors.New("rollback_failed")
	ErrRepairRequired         = errors.New("repair_required")
	ErrAutomaticUnavailable   = errors.New("automatic_configuration_unavailable")
)

type FileDigest struct {
	Exists bool
	SHA256 [sha256.Size]byte
}

type backupStore interface {
	CaptureTarget(context.Context, *configTarget) (*fileSnapshot, error)
	CheckBaseline(context.Context, *fileSnapshot) error
	Stage(context.Context, *fileSnapshot, Client) (string, error)
	SupportsAtomic(*fileSnapshot) bool
	Publish(context.Context, *fileSnapshot) error
}

type fileBackupStore struct {
	afterCandidate   func(*fileSnapshot)
	afterPublish     func(*fileSnapshot)
	stopAfterPublish bool
}

type fileSnapshot struct {
	targetPath string
	backupPath string
	existed    bool
	mode       os.FileMode
	baseline   FileDigest
	targetInfo os.FileInfo
	keep       bool

	root           *os.Root
	homeRoot       *os.Root
	parent         *os.File
	parentPath     string
	parentInfo     os.FileInfo
	homePath       string
	homeInfo       os.FileInfo
	relativeParent string
	targetName     string
	backupName     string
	stageDir       string
	stagePath      string
	candidateName  string
	displacedName  string
	journalName    string
	stageDigest    FileDigest
}

func newFileBackupStore() backupStore {
	return &fileBackupStore{}
}

func (store *fileBackupStore) CaptureTarget(
	ctx context.Context,
	target *configTarget,
) (*fileSnapshot, error) {
	if ctx == nil || ctx.Err() != nil || target == nil || target.root == nil ||
		target.homeInfo == nil || !filepath.IsAbs(target.path) ||
		target.relative == "" || filepath.IsAbs(target.relative) {
		return nil, contextOrRollback(ctx)
	}
	currentHome, err := os.Lstat(target.homePath)
	if err != nil || !os.SameFile(target.homeInfo, currentHome) {
		_ = target.root.Close()
		return nil, ErrConcurrentModification
	}
	relativeParent := filepath.Dir(target.relative)
	if err := ensureRootDirectories(target.root, relativeParent); err != nil {
		_ = target.root.Close()
		return nil, err
	}
	parentRoot, err := target.root.OpenRoot(relativeParent)
	if err != nil {
		_ = target.root.Close()
		return nil, ErrRollbackFailed
	}
	parent, err := parentRoot.Open(".")
	if err != nil {
		_ = parentRoot.Close()
		_ = target.root.Close()
		return nil, ErrRollbackFailed
	}
	parentInfo, err := parent.Stat()
	if err != nil || !parentInfo.IsDir() {
		_ = parent.Close()
		_ = parentRoot.Close()
		_ = target.root.Close()
		return nil, ErrRollbackFailed
	}
	currentParent, err := target.root.Lstat(relativeParent)
	if err != nil || currentParent.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(parentInfo, currentParent) {
		_ = parent.Close()
		_ = parentRoot.Close()
		_ = target.root.Close()
		return nil, ErrRollbackFailed
	}
	snapshot := &fileSnapshot{
		targetPath:     target.path,
		root:           parentRoot,
		homeRoot:       target.root,
		parent:         parent,
		parentPath:     filepath.Dir(target.path),
		parentInfo:     parentInfo,
		homePath:       target.homePath,
		homeInfo:       target.homeInfo,
		relativeParent: relativeParent,
		targetName:     filepath.Base(target.relative),
		journalName:    "." + filepath.Base(target.relative) + ".cy-kaf-transaction.json",
	}
	if err := store.resolvePending(ctx, snapshot); err != nil {
		snapshot.Keep()
		return snapshot, err
	}
	info, err := parentRoot.Lstat(snapshot.targetName)
	if errors.Is(err, os.ErrNotExist) {
		snapshot.baseline = FileDigest{}
		return snapshot, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		snapshot.Keep()
		return snapshot, ErrRollbackFailed
	}
	source, err := parentRoot.Open(snapshot.targetName)
	if err != nil {
		snapshot.Keep()
		return snapshot, ErrRollbackFailed
	}
	defer func() { _ = source.Close() }()
	openedInfo, err := source.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		snapshot.Keep()
		return snapshot, ErrConcurrentModification
	}
	backupName, backup, err := createPrivateRootFile(parentRoot, ".mcp-config-", ".backup")
	if err != nil {
		snapshot.Keep()
		return snapshot, ErrRollbackFailed
	}
	snapshot.backupName = backupName
	snapshot.backupPath = filepath.Join(filepath.Dir(target.path), backupName)
	ok := false
	defer func() {
		_ = backup.Close()
		if !ok {
			_ = parentRoot.Remove(backupName)
		}
	}()
	hash := sha256.New()
	if err := copyBounded(ctx, io.MultiWriter(backup, hash), source, maxClientConfigBytes); err != nil {
		return snapshot, err
	}
	if err := backup.Sync(); err != nil {
		return snapshot, ErrRollbackFailed
	}
	if err := backup.Close(); err != nil {
		return snapshot, ErrRollbackFailed
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	snapshot.existed = true
	snapshot.mode = info.Mode().Perm()
	snapshot.baseline = FileDigest{Exists: true, SHA256: digest}
	snapshot.targetInfo = info
	ok = true
	return snapshot, nil
}

func (store *fileBackupStore) Stage(
	ctx context.Context,
	snapshot *fileSnapshot,
	client Client,
) (string, error) {
	if snapshot == nil || snapshot.root == nil || ctx == nil {
		return "", ErrRollbackFailed
	}
	stageDir, err := os.MkdirTemp("", "cy-kaf-mcp-stage-*")
	if err != nil {
		return "", ErrRollbackFailed
	}
	filename := "config.toml"
	if client == ClientClaudeCode {
		filename = ".claude.json"
	}
	stagePath := filepath.Join(stageDir, filename)
	if snapshot.existed {
		source, openErr := snapshot.root.Open(snapshot.backupName)
		if openErr != nil {
			_ = os.RemoveAll(stageDir)
			return "", ErrRollbackFailed
		}
		stage, createErr := os.OpenFile(stagePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if createErr != nil {
			_ = source.Close()
			_ = os.RemoveAll(stageDir)
			return "", ErrRollbackFailed
		}
		copyErr := copyBounded(ctx, stage, source, maxClientConfigBytes)
		syncErr := stage.Sync()
		closeErr := stage.Close()
		_ = source.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil {
			_ = os.RemoveAll(stageDir)
			if copyErr != nil {
				return "", copyErr
			}
			return "", ErrRollbackFailed
		}
	}
	snapshot.stageDir = stageDir
	snapshot.stagePath = stagePath
	return stagePath, nil
}

func (store *fileBackupStore) SupportsAtomic(snapshot *fileSnapshot) bool {
	return probeAtomicPublish(snapshot)
}

func (store *fileBackupStore) CheckBaseline(
	ctx context.Context,
	snapshot *fileSnapshot,
) error {
	if ctx == nil || ctx.Err() != nil {
		return contextOrRollback(ctx)
	}
	if snapshot == nil || snapshot.root == nil || snapshot.parentInfo == nil {
		return ErrRollbackFailed
	}
	currentHome, err := os.Lstat(snapshot.homePath)
	if err != nil || snapshot.homeInfo == nil ||
		!os.SameFile(snapshot.homeInfo, currentHome) {
		snapshot.Keep()
		return ErrConcurrentModification
	}
	currentParent, err := snapshot.homeRoot.Lstat(snapshot.relativeParent)
	if err != nil || !os.SameFile(snapshot.parentInfo, currentParent) {
		snapshot.Keep()
		return ErrConcurrentModification
	}
	current, info, err := digestRootContext(ctx, snapshot.root, snapshot.targetName)
	if err != nil || current != snapshot.baseline {
		snapshot.Keep()
		return ErrConcurrentModification
	}
	if snapshot.targetInfo != nil &&
		(info == nil || !os.SameFile(snapshot.targetInfo, info)) {
		snapshot.Keep()
		return ErrConcurrentModification
	}
	return nil
}

func (snapshot *fileSnapshot) Keep() {
	if snapshot != nil {
		snapshot.keep = true
	}
}

func (snapshot *fileSnapshot) CloseRetained() {
	if snapshot == nil {
		return
	}
	snapshot.Keep()
	if snapshot.parent != nil {
		_ = snapshot.parent.Close()
		snapshot.parent = nil
	}
	if snapshot.root != nil {
		_ = snapshot.root.Close()
		snapshot.root = nil
	}
	if snapshot.homeRoot != nil {
		_ = snapshot.homeRoot.Close()
		snapshot.homeRoot = nil
	}
}

func (snapshot *fileSnapshot) Delete() error {
	if snapshot == nil {
		return nil
	}
	var failed bool
	if snapshot.root != nil {
		for _, name := range []string{
			snapshot.backupName,
			snapshot.candidateName,
			snapshot.displacedName,
			snapshot.journalName,
		} {
			if name != "" {
				if err := snapshot.root.Remove(name); err != nil &&
					!errors.Is(err, os.ErrNotExist) {
					failed = true
				}
			}
		}
	} else if snapshot.backupPath != "" {
		if err := os.Remove(snapshot.backupPath); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			failed = true
		}
	}
	if snapshot.stageDir != "" {
		if err := os.RemoveAll(snapshot.stageDir); err != nil {
			failed = true
		}
	}
	if failed {
		snapshot.Keep()
		return ErrRollbackFailed
	}
	snapshot.backupPath = ""
	snapshot.backupName = ""
	snapshot.keep = false
	if snapshot.parent != nil {
		_ = snapshot.parent.Close()
		snapshot.parent = nil
	}
	if snapshot.root != nil {
		_ = snapshot.root.Close()
		snapshot.root = nil
	}
	if snapshot.homeRoot != nil {
		_ = snapshot.homeRoot.Close()
		snapshot.homeRoot = nil
	}
	return nil
}

func ensureRootDirectories(root *os.Root, relative string) error {
	if relative == "." || relative == "" {
		return nil
	}
	if root == nil || filepath.IsAbs(relative) {
		return ErrRollbackFailed
	}
	for _, component := range strings.Split(filepath.ToSlash(relative), "/") {
		if component == ".." {
			return ErrRollbackFailed
		}
	}
	current := ""
	components := splitRelativePath(relative)
	for _, component := range components {
		if current == "" {
			current = component
		} else {
			current = filepath.Join(current, component)
		}
		info, err := root.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := root.Mkdir(current, 0o700); err != nil &&
				!errors.Is(err, os.ErrExist) {
				return ErrRollbackFailed
			}
			info, err = root.Lstat(current)
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return ErrConcurrentModification
		}
	}
	return nil
}

func splitRelativePath(relative string) []string {
	var components []string
	for _, component := range strings.Split(filepath.ToSlash(relative), "/") {
		if component != "" && component != "." {
			components = append(components, component)
		}
	}
	return components
}
