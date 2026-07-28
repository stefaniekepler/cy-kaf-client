//go:build linux

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
	switch filesystem.Type {
	case unix.EXT2_SUPER_MAGIC,
		unix.XFS_SUPER_MAGIC,
		unix.BTRFS_SUPER_MAGIC,
		unix.F2FS_SUPER_MAGIC,
		unix.TMPFS_MAGIC,
		unix.RAMFS_MAGIC,
		unix.OVERLAYFS_SUPER_MAGIC,
		0x2fc12fc1: // ZFS
		return true
	default:
		return false
	}
}
