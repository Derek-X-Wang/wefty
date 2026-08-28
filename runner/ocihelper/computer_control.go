package ocihelper

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const computerControlFilename = "driver.json"

var (
	computerControlFalse = []byte(`{"version":1,"human_driving":false}`)
	computerControlTrue  = []byte(`{"version":1,"human_driving":true}`)
)

func prepareComputerControlDirectory(logDirectory string, mount, unmount func(string) error) (string, error) {
	if err := os.MkdirAll(logDirectory, 0o700); err != nil {
		return "", fmt.Errorf("create attempt log directory: %w", err)
	}
	controlDirectory := filepath.Join(logDirectory, "control")
	if err := os.Mkdir(controlDirectory, 0o755); err != nil {
		return "", fmt.Errorf("create fresh Computer control directory: %w", err)
	}
	if err := mount(controlDirectory); err != nil {
		return "", fmt.Errorf("mount Computer control tmpfs: %w", err)
	}
	if err := atomicWriteComputerControlState(controlDirectory, false); err != nil {
		return "", errors.Join(err, unmount(controlDirectory))
	}
	return controlDirectory, nil
}

func atomicWriteComputerControlState(controlDirectory string, humanDriving bool) (writeErr error) {
	payload := computerControlFalse
	if humanDriving {
		payload = computerControlTrue
	}
	temporary, err := os.CreateTemp(controlDirectory, ".driver-*.json")
	if err != nil {
		return fmt.Errorf("create Computer control state: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		if temporary != nil {
			if closeErr := temporary.Close(); writeErr == nil && closeErr != nil {
				writeErr = closeErr
			}
		}
		if temporaryName != "" {
			removeErr := os.Remove(temporaryName)
			if writeErr == nil && removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				writeErr = removeErr
			}
		}
	}()
	if err := temporary.Chmod(0o444); err != nil {
		return fmt.Errorf("make Computer control state image-readable: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		return fmt.Errorf("write Computer control state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync Computer control state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Computer control state: %w", err)
	}
	temporary = nil
	if err := os.Rename(temporaryName, filepath.Join(controlDirectory, computerControlFilename)); err != nil {
		return fmt.Errorf("publish Computer control state: %w", err)
	}
	// Rename is deliberately the last fallible operation. Once the guest can
	// observe the new boolean, this verb must report success: a post-publish
	// error could otherwise leave a spurious human_driving=true signal.
	temporaryName = ""
	return nil
}
