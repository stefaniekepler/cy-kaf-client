//go:build darwin

package mcpclient

import "golang.org/x/sys/unix"

func probeAtomicPublish(snapshot *fileSnapshot) bool {
	if snapshot == nil || snapshot.parent == nil {
		return false
	}
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(snapshot.parent.Fd()), &filesystem); err != nil {
		return false
	}
	return filesystem.Flags&unix.MNT_LOCAL != 0
}
