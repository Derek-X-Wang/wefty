//go:build darwin

package agentinstall

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// processStartIdentity reads the kernel's birth timestamp for this exact PID,
// independently of the ready record and launchd's process observation.
func processStartIdentity(pid int) (string, error) {
	if pid <= 0 || int(int32(pid)) != pid {
		return "", errors.New("invalid process PID")
	}
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", fmt.Errorf("read process start identity: %w", err)
	}
	started := info.Proc.P_starttime
	if info.Proc.P_pid != int32(pid) || started.Sec <= 0 || started.Usec < 0 || started.Usec >= 1000000 {
		return "", errors.New("unverified process start identity")
	}
	return fmt.Sprintf("darwin:%d:%06d", started.Sec, started.Usec), nil
}
