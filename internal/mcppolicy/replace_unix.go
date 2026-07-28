//go:build !windows

package mcppolicy

import (
	"os"
	"path/filepath"
)

func replaceFile(tempPath, destination string) error {
	if err := os.Rename(tempPath, destination); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
