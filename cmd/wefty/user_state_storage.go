package main

import (
	"os"
	"path/filepath"
)

// Shared by exec snapshots and Seat receipts/credentials. An explicit override
// retains exec's relative-path behavior; relative XDG_STATE_HOME is ignored.
func userStateStorageRoot(override, kind string) (string, error) {
	if root := os.Getenv(override); root != "" {
		return filepath.Abs(root)
	}
	if root := os.Getenv("XDG_STATE_HOME"); filepath.IsAbs(root) {
		return filepath.Join(root, "wefty", kind), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "state", "wefty", kind), nil
}
