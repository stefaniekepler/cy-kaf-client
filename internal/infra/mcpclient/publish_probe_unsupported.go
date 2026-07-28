//go:build !darwin && !linux

package mcpclient

func probeAtomicPublish(*fileSnapshot) bool {
	return false
}
