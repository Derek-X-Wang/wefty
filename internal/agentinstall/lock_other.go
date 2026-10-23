//go:build !darwin && !linux

package agentinstall

import (
	"errors"
	"os"
)

func owned(os.FileInfo) bool { return false }
func lockDirectory(string) (func(), error) {
	return nil, errors.New("generation installation requires Darwin or Linux")
}

func lockCatalog(path string) (func(), error) { return lockDirectory(path) }

func requireLifetimeLock(string) error {
	return errors.New("cannot verify generation lifetime lock on this platform")
}
