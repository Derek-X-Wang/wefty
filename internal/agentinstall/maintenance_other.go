//go:build !darwin && !linux

package agentinstall

import (
	"errors"
	"os"
)

func maintenanceIdentity(*os.Root) (uint64, uint64, error) {
	return 0, 0, errors.New("unsupported maintenance platform")
}
func renameMaintenance(*os.Root, string, string, uint64, uint64) error {
	return errors.New("unsupported maintenance platform")
}
func moveMaintenanceRecord(*os.Root, *os.Root, string) error {
	return errors.New("unsupported maintenance platform")
}
