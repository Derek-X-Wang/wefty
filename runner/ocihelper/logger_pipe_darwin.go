//go:build darwin

package ocihelper

import "golang.org/x/sys/unix"

// Darwin's FIONREAD request is _IOR('f', 127, int).
const darwinFIONREAD = 0x4004667f

func pipeUnreadBytes(fd uintptr) (int, error) {
	return unix.IoctlGetInt(int(fd), darwinFIONREAD)
}
