//go:build !darwin && !linux

package ocihelper

import (
	"errors"
	"net"
)

func authenticateUnixPeer(net.Conn) (Peer, error) {
	return Peer{}, errors.New("OCI helper peer authentication is unsupported on this platform")
}
