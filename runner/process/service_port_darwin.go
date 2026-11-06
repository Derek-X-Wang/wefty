//go:build darwin

package process

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
)

// macOS has no /proc, and Go's standard library and x/sys expose no wrapper
// for the libproc socket-owner query. lsof ships with every macOS install
// and reads the same data; its -F output is a stable machine format.
// A variable only so tests can point it at a failing stand-in.
var lsofPath = "/usr/sbin/lsof"

// inspectServiceListeners lists the processes that hold a TCP listener on
// the backend port, their process groups and bound addresses. lsof run by an
// ordinary user sees only that user's processes. A listener of another user
// is therefore unseen, never owned, and readiness waits for a listener this
// check can attribute.
func inspectServiceListeners(ctx context.Context, backend serviceBackend, processGroupID int) (serviceListenerVerdict, error) {
	output, err := exec.CommandContext(ctx, lsofPath, "-nP", "-w", "-iTCP:"+strconv.Itoa(backend.port), "-sTCP:LISTEN", "-Fgnt").Output()
	var exit *exec.ExitError
	if err != nil && (ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 || len(output) != 0) {
		// lsof exits 1 with no output when nothing matches.
		return 0, fmt.Errorf("list listeners on port %d: %w", backend.port, err)
	}
	return lsofListenerVerdict(output, backend, processGroupID)
}

// lsofListenerVerdict reads lsof -Fgnt output: each process set is p<pid>,
// then g<pgid>, then f<fd>, t<IPv4 or IPv6> and n<address:port> for each
// listening socket.
func lsofListenerVerdict(output []byte, backend serviceBackend, processGroupID int) (serviceListenerVerdict, error) {
	var listeners []serviceListener
	group, family := "", ""
	for _, line := range strings.Split(string(output), "\n") {
		switch {
		case strings.HasPrefix(line, "p"):
			group = ""
		case strings.HasPrefix(line, "g"):
			group = strings.TrimPrefix(line, "g")
		case strings.HasPrefix(line, "f"):
			family = ""
		case strings.HasPrefix(line, "t"):
			family = strings.TrimPrefix(line, "t")
		case strings.HasPrefix(line, "n"):
			host, _, err := net.SplitHostPort(strings.TrimPrefix(line, "n"))
			if err != nil {
				return 0, fmt.Errorf("malformed lsof listener name %q: %w", line, err)
			}
			// lsof prints either wildcard as *; its t field names the family.
			ip := net.IPv6unspecified
			if family == "IPv4" {
				ip = net.IPv4zero
			}
			if host != "*" {
				if ip = net.ParseIP(host); ip == nil {
					return 0, fmt.Errorf("malformed lsof listener address %q", host)
				}
			}
			owner := listenerOfOther
			if group == strconv.Itoa(processGroupID) {
				owner = listenerOfPayload
			}
			listeners = append(listeners, serviceListener{ip: ip, owner: owner})
		}
	}
	return backend.verdict(listeners, nil)
}
