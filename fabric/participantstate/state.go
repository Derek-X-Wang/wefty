// Package participantstate serializes ownership of a saved Fabric participant.
package participantstate

import (
	"os"
)

// LockName is persistent bookkeeping, never evidence of an enrolled identity.
const LockName = ".participant.lock"

// HasSavedState excludes ownership bookkeeping from the saved-state check.
func HasSavedState(directory string) bool {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() {
		return false
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.Name() != LockName {
			return true
		}
	}
	return false
}
