//go:build darwin || linux

package process

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
	"time"
)

// A process service's backend port is picked by the agent and released
// before the payload binds it, so another local process can take it in the
// gap (#1468). The Guardian therefore confirms who answers on that port
// before it reports readiness, because readiness is what publishes the
// service. "The payload" here is its process group: the same unit the
// Guardian signals, reaps and proves absent.

// serviceListenerVerdict says who holds the TCP listeners on a backend port.
type serviceListenerVerdict int

const (
	// serviceListenerUnseen means no listener on the port is visible.
	serviceListenerUnseen serviceListenerVerdict = iota
	// serviceListenerOwned means every listener on the port belongs to the
	// payload's process group.
	serviceListenerOwned
	// serviceListenerForeign means at least one listener on the port belongs
	// to a process outside the payload's process group.
	serviceListenerForeign
)

// serviceListenerInspectTimeout bounds one ownership inspection, so a stuck
// OS query cannot hold the Guardian's supervision loop.
const serviceListenerInspectTimeout = 5 * time.Second

func serviceAddressPort(address string) (int, error) {
	_, text, err := net.SplitHostPort(address)
	if err != nil {
		return 0, err
	}
	port, err := strconv.Atoi(text)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("invalid service port %q", text)
	}
	return port, nil
}

// servicePortHeld reports whether another socket holds address now: the
// bind the payload needed fails with EADDRINUSE. Call it only after the
// payload group is gone, so nothing found can be the payload's own.
func servicePortHeld(address string) bool {
	listener, err := net.Listen("tcp4", address)
	if err != nil {
		return errors.Is(err, syscall.EADDRINUSE)
	}
	_ = listener.Close()
	return false
}
