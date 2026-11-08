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

// serviceListenerInspectTimeout bounds one ownership inspection. Inspection
// runs beside the Guardian's supervision loop and is cancelled on any stop,
// so this bounds only how long an answer can take, never a stop.
// A variable only so in-process tests can shorten it.
var serviceListenerInspectTimeout = 5 * time.Second

// serviceOwnershipRecheckInterval is how often a ready service's listener
// ownership is confirmed again. A takeover with no failed probe in between
// (the payload closes its listener and another process binds the port, or
// adds an SO_REUSEPORT listener) is withdrawn within this interval plus one
// probe cycle and one inspection: nominally about 10.3 s, because an lsof
// inspection usually answers in tens of milliseconds. Forwarding stays as
// it is while an inspection runs, so the bound includes the whole
// inspection timeout: at most about 15 s plus one probe cycle (250 ms
// interval and up to 200 ms connect timeout), about 15.5 s. The interval is set by
// the CPU budget of 1% of one core per ready service: one lsof inspection
// on macOS costs about 0.04 s of CPU, and one ready service measured 0.76%
// at 10 s (0.39% with no rechecks, 2.3% at 2 s).
// A variable only so in-process tests can shorten it.
var serviceOwnershipRecheckInterval = 10 * time.Second

// serviceListenerInspector is inspectServiceListeners outside in-process
// tests, which replace it to inject inspection failures.
var serviceListenerInspector = inspectServiceListeners

// guardianReadinessReason is a no-op outside in-process tests, which replace
// it to learn why the Guardian reports each readiness change (#1564). The
// readiness message itself carries no reason.
var guardianReadinessReason = func(ready bool, reason string) {}

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

// listenerOwner says whose a listening socket is.
type listenerOwner int

const (
	listenerOfPayload listenerOwner = iota
	listenerOfOther
	// listenerOfUnknown means this user cannot tell, for example because a
	// payload group member's descriptors cannot be read.
	listenerOfUnknown
)

// serviceListener is one TCP listening socket on the backend port.
type serviceListener struct {
	ip    net.IP
	owner listenerOwner
}

// receiving keeps the listeners that can accept a connection dialed to the
// backend, in the kernel's own lookup order on Linux and macOS: a listener
// bound to the backend address itself (also as an IPv4-mapped IPv6 address)
// takes every such connection, so wildcards count only when there is none.
// A listener on another address, such as ::1 or another interface, never
// counts. Among wildcards an IPv6 one counts because it may be dual-stack:
// neither /proc/net/tcp6 nor lsof shows IPV6_V6ONLY, and another process's
// socket option cannot be read without ptrace rights.
func (backend serviceBackend) receiving(listeners []serviceListener) []serviceListener {
	var exact, wildcard []serviceListener
	for _, listener := range listeners {
		switch {
		case listener.ip.Equal(backend.ip):
			exact = append(exact, listener)
		case listener.ip.IsUnspecified() && (listener.ip.To4() == nil || backend.ip.To4() != nil):
			wildcard = append(wildcard, listener)
		}
	}
	if len(exact) > 0 {
		return exact
	}
	return wildcard
}

// verdict decides ownership from the listeners on the backend port. unknown
// explains a listener whose owner could not be read.
func (backend serviceBackend) verdict(listeners []serviceListener, unknown error) (serviceListenerVerdict, error) {
	receiving := backend.receiving(listeners)
	if len(receiving) == 0 {
		return serviceListenerUnseen, nil
	}
	inconclusive := false
	for _, listener := range receiving {
		switch listener.owner {
		case listenerOfOther:
			return serviceListenerForeign, nil
		case listenerOfUnknown:
			inconclusive = true
		}
	}
	if inconclusive {
		// Neither confirmed as the payload's nor proven foreign.
		if unknown == nil {
			unknown = errors.New("a listener's owner could not be read")
		}
		return 0, unknown
	}
	return serviceListenerOwned, nil
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
