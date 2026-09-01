//go:build linux

package ocihelper

import (
	"errors"
	"os"
	"path/filepath"
)

func writeDurableFile(directory, temporaryPattern, name string, payload []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(directory, temporaryPattern)
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	writeErr := temporary.Chmod(mode)
	if writeErr == nil {
		_, writeErr = temporary.Write(payload)
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	writeErr = errors.Join(writeErr, temporary.Close())
	if writeErr != nil {
		return writeErr
	}
	if err := os.Rename(temporaryPath, filepath.Join(directory, name)); err != nil {
		return err
	}
	return syncDirectory(directory)
}
