package mcpclient

import (
	"context"
	"os"
)

func (store *fileBackupStore) Publish(ctx context.Context, snapshot *fileSnapshot) error {
	if snapshot == nil || snapshot.root == nil || snapshot.parent == nil ||
		snapshot.stagePath == "" || ctx == nil {
		return ErrRollbackFailed
	}
	stageDigest, err := digestPathContext(ctx, snapshot.stagePath)
	if err != nil {
		snapshot.Keep()
		return err
	}
	if !stageDigest.Exists {
		snapshot.Keep()
		return ErrRollbackFailed
	}
	snapshot.stageDigest = stageDigest
	candidateName, candidate, err := createPrivateRootFile(
		snapshot.root,
		".mcp-config-",
		".candidate",
	)
	if err != nil {
		snapshot.Keep()
		return ErrRollbackFailed
	}
	source, err := os.Open(snapshot.stagePath)
	if err != nil {
		_ = candidate.Close()
		_ = snapshot.root.Remove(candidateName)
		snapshot.Keep()
		return ErrRollbackFailed
	}
	copyErr := copyBounded(ctx, candidate, source, maxClientConfigBytes)
	if snapshot.existed {
		if modeErr := candidate.Chmod(snapshot.mode.Perm()); modeErr != nil {
			copyErr = ErrRollbackFailed
		}
	}
	syncErr := candidate.Sync()
	closeErr := candidate.Close()
	_ = source.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		_ = snapshot.root.Remove(candidateName)
		snapshot.Keep()
		if copyErr != nil {
			return copyErr
		}
		return ErrRollbackFailed
	}
	snapshot.candidateName = candidateName
	if store.afterCandidate != nil {
		store.afterCandidate(snapshot)
	}
	candidateDigest, _, err := digestRootContext(ctx, snapshot.root, candidateName)
	if err != nil {
		snapshot.Keep()
		return err
	}
	if candidateDigest != snapshot.stageDigest {
		snapshot.Keep()
		return ErrConcurrentModification
	}
	if snapshot.existed {
		// A single exchange turns the candidate slot into the displaced
		// baseline. There is deliberately no empty placeholder or second
		// exchange window.
		snapshot.displacedName = candidateName
	}
	journal := transactionJournal{
		Version:       1,
		Target:        snapshot.targetName,
		Candidate:     snapshot.candidateName,
		Displaced:     snapshot.displacedName,
		Backup:        snapshot.backupName,
		Baseline:      snapshot.baseline,
		Staged:        snapshot.stageDigest,
		BaselineMode:  uint32(snapshot.mode.Perm()),
		BaselineOwned: snapshot.existed,
	}
	if err := writeJournal(ctx, snapshot, journal); err != nil {
		snapshot.Keep()
		return err
	}
	if err := store.CheckBaseline(ctx, snapshot); err != nil {
		snapshot.Keep()
		return err
	}
	if err := publishAtomic(snapshot); err != nil {
		snapshot.Keep()
		return err
	}
	if store.afterPublish != nil {
		store.afterPublish(snapshot)
	}
	if store.stopAfterPublish {
		snapshot.Keep()
		return ErrRollbackFailed
	}
	if err := store.validatePublished(ctx, snapshot); err != nil {
		snapshot.Keep()
		return err
	}
	return snapshot.Delete()
}

func (store *fileBackupStore) validatePublished(
	ctx context.Context,
	snapshot *fileSnapshot,
) error {
	current, _, err := digestRootContext(ctx, snapshot.root, snapshot.targetName)
	if err != nil || current != snapshot.stageDigest {
		return ErrConcurrentModification
	}
	if snapshot.existed {
		displaced, info, err := digestRootContext(ctx, snapshot.root, snapshot.displacedName)
		if err != nil || displaced != snapshot.baseline ||
			info == nil || snapshot.targetInfo == nil ||
			!os.SameFile(info, snapshot.targetInfo) {
			return ErrConcurrentModification
		}
	}
	return nil
}
