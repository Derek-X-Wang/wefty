//go:build linux

package ocihelper

import "golang.org/x/sys/unix"

func pipeUnreadBytes(fd uintptr) (int, error) {
	return unix.IoctlGetInt(int(fd), unix.TIOCINQ)
}
