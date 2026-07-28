//go:build !darwin && !linux

package mcpclient

func publishAtomic(*fileSnapshot) error {
	return ErrRollbackFailed
}
