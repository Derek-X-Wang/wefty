//go:build linux

package ocihelper

import (
	"errors"
	"net"

	"golang.org/x/sys/unix"
)

func authenticateUnixPeer(connection net.Conn) (Peer, error) {
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		return Peer{}, errors.New("OCI helper requires a Unix socket peer")
	}
	raw, err := unixConnection.SyscallConn()
	if err != nil {
		return Peer{}, err
	}
	var credential *unix.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return Peer{}, err
	}
	if controlErr != nil {
		return Peer{}, controlErr
	}
	return Peer{UID: credential.Uid, GID: credential.Gid}, nil
}
