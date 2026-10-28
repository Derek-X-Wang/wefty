//go:build darwin

package ocicontrol

import "golang.org/x/sys/unix"

func readParentPID(pid int) (int, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return 0, err
	}
	if info == nil {
		return 0, errPeerPIDUnavailable
	}
	return int(info.Eproc.Ppid), nil
}
