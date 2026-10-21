// Package teststate supplies non-authentic saved-state fixtures for fake
// enrollers and constructor tests. It keeps the backend filename behind the
// adapter boundary. These bytes cannot start an authenticated participant.
package teststate

import (
	"os"
	"path/filepath"
)

func Write(directory string) error {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, "tailscaled.state"), []byte(`{"fixture":"c2F2ZWQ="}`), 0600)
}
