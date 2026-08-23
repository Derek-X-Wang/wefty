//go:build darwin

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
	var credential *unix.Xucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil {
		return Peer{}, err
	}
	if controlErr != nil {
		return Peer{}, controlErr
	}
	peer := Peer{UID: credential.Uid}
	if credential.Ngroups > 0 {
		peer.GID = credential.Groups[0]
	}
	return peer, nil
}
