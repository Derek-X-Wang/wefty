package lima

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// HostVMFreeSpaceError keeps private filesystem diagnostics local while the
// control reply carries the refusal and a concrete recovery action.
type HostVMFreeSpaceError struct {
	Minimum   uint64
	Available uint64
	Cause     error
}

func (err *HostVMFreeSpaceError) Error() string { return err.HostVMStartRefusal() }
func (err *HostVMFreeSpaceError) Unwrap() error { return err.Cause }
func (err *HostVMFreeSpaceError) HostVMStartRefusal() string {
	if err.Cause != nil {
		return "host VM free space is unverified; check access to the host VM disk volume and run wefty node doctor before retrying"
	}
	return fmt.Sprintf("host VM start refused: need %.2f GiB free, have %.2f GiB; free space on the host VM disk volume and retry", float64(err.Minimum)/(1<<30), float64(err.Available)/(1<<30))
}

func HostVMHome() string {
	if home := os.Getenv("LIMA_HOME"); home != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".lima")
}

// withHostVMStartSpaceCheck covers starts inside the guest installer too,
// including its one group-membership restart. Removal must work on a full disk.
func withHostVMStartSpaceCheck(run commandRunner, minimum uint64) commandRunner {
	return func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "start" {
			if err := CheckHostVMFreeSpace(HostVMHome(), minimum); err != nil {
				return nil, err
			}
		}
		return run(ctx, name, args...)
	}
}
