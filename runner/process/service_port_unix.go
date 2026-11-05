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
// gap (#1468). The Guardian therefore confirms who listens on that port
// before it reports readiness, because readiness is what publishes the
// service, and again while the service stays ready. "The payload" here is
// its process group: the same unit the Guardian signals, reaps and proves
// absent.

// serviceListenerVerdict says who holds the TCP listeners that can receive
// connections for a backend address.
type serviceListenerVerdict int

const (
	// serviceListenerUnseen means no such listener is visible.
	serviceListenerUnseen serviceListenerVerdict = iota
	// serviceListenerOwned means every such listener belongs to the
	// payload's process group.
	serviceListenerOwned
	// serviceListenerForeign means at least one such listener belongs to a
	// process outside the payload's process group.
	serviceListenerForeign
)

// serviceListenerInspectTimeout bounds one ownership inspection, so a stuck
// OS query cannot hold the Guardian's supervision loop.
const serviceListenerInspectTimeout = 5 * time.Second

// serviceOwnershipRecheckInterval is how often a ready service's listener
// ownership is confirmed again. A takeover with no failed probe in between
// (the payload closes its listener and another process binds the port, or
// adds an SO_REUSEPORT listener) is withdrawn within this interval. It keeps
// lsof on macOS (about 0.15 s wall, 0.04 s CPU) to one run per interval.
// A variable only so in-process tests can shorten it.
var serviceOwnershipRecheckInterval = 2 * time.Second

// serviceListenerInspector is inspectServiceListeners outside in-process
// tests, which replace it to inject inspection failures.
var serviceListenerInspector = inspectServiceListeners

// serviceBackend is the loopback address the agent dials for a process
// service.
type serviceBackend struct {
	ip   net.IP
	port int
}

func parseServiceBackend(address string) (serviceBackend, error) {
	host, text, err := net.SplitHostPort(address)
	if err != nil {
		return serviceBackend{}, err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return serviceBackend{}, fmt.Errorf("invalid service host %q", host)
	}
	port, err := strconv.Atoi(text)
	if err != nil || port <= 0 || port > 65535 {
		return serviceBackend{}, fmt.Errorf("invalid service port %q", text)
	}
	return serviceBackend{ip: ip, port: port}, nil
}

// receives reports whether a listener bound to ip can accept a connection
// dialed to the backend. A wildcard counts: an IPv6 wildcard may be
// dual-stack, and this check cannot see IPV6_V6ONLY, so it errs toward
// counting it. A listener on another address (for example ::1 or another
// interface) cannot receive the backend's traffic and does not count.
func (backend serviceBackend) receives(ip net.IP) bool {
	if ip.Equal(backend.ip) {
		return true
	}
	if !ip.IsUnspecified() {
		return false
	}
	return ip.To4() == nil || backend.ip.To4() != nil
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
