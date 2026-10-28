//go:build darwin

package ocicontrol

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

func unixPeerUID(connection net.Conn) (uint32, error) {
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		return 0, errors.New("node-local control peer is not a Unix connection")
	}
	raw, err := unixConnection.SyscallConn()
	if err != nil {
		return 0, err
	}
	var credential *unix.Xucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if controlErr != nil || credential == nil {
		return 0, controlErr
	}
	return credential.Uid, nil
}

func unixPeerPID(connection net.Conn) (int, error) {
	c, ok := connection.(*net.UnixConn)
	if !ok {
		return 0, errPeerPIDUnavailable
	}
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var pid int
	var readErr error
	err = raw.Control(func(fd uintptr) { pid, readErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID) })
	if err != nil {
		return 0, err
	}
	return pid, readErr
}
