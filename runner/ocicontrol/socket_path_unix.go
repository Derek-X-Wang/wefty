//go:build darwin || linux

package ocicontrol

import "golang.org/x/sys/unix"

func maximumUnixSocketPathBytes() int {
	return len(unix.RawSockaddrUnix{}.Path) - 1
}
