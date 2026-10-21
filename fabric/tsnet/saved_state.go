package tsnet

import (
	"os"
	"path/filepath"
)

// HasSavedState checks for the private state file used by the pinned backend's
// default FileStore. Log/configuration files and ownership locks cannot stand
// in for it. This is an existence check, not identity verification: the backend
// parses its own JSON store and verifies the saved identity before listening.
// Keeping the content opaque also avoids copying its credentials into Wefty.
func HasSavedState(directory string) bool {
	folder, err := os.Lstat(directory)
	if err != nil || !folder.IsDir() || folder.Mode().Perm()&0077 != 0 {
		return false
	}
	info, err := os.Lstat(filepath.Join(directory, "tailscaled.state"))
	return err == nil && info.Mode().IsRegular() && info.Size() > 0 && info.Mode().Perm()&0077 == 0
}
