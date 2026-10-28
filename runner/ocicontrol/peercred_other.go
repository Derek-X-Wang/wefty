//go:build !darwin && !linux

package ocicontrol

import (
	"errors"
	"net"
)

func unixPeerUID(net.Conn) (uint32, error) {
	return 0, errors.New("Unix peer credentials are unavailable on this platform")
}

func unixPeerPID(net.Conn) (int, error) { return 0, errPeerPIDUnavailable }
