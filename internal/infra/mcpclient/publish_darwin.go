//go:build darwin

package mcpclient

import (
	"golang.org/x/sys/unix"
)

func publishAtomic(snapshot *fileSnapshot) error {
	fd := int(snapshot.parent.Fd())
	if snapshot.existed {
		if err := unix.RenameatxNp(
			fd,
			snapshot.candidateName,
			fd,
			snapshot.targetName,
			unix.RENAME_SWAP,
		); err != nil {
			return ErrRollbackFailed
		}
		return nil
	}
	if err := unix.RenameatxNp(
		fd,
		snapshot.candidateName,
		fd,
		snapshot.targetName,
		unix.RENAME_EXCL,
	); err != nil {
		if _, statErr := snapshot.root.Lstat(snapshot.targetName); statErr == nil {
			return ErrConcurrentModification
		}
		return ErrRollbackFailed
	}
	return nil
}
