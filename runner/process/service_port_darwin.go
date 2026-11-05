//go:build darwin

package process

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// macOS has no /proc, and Go's standard library and x/sys expose no wrapper
// for the libproc socket-owner query. lsof ships with every macOS install
// and reads the same data; its -F output is a stable machine format.
const lsofPath = "/usr/sbin/lsof"

// inspectServiceListeners lists the processes that hold a TCP listener on
// port and their process groups. lsof run by an ordinary user sees only that
// user's processes. A listener of another user is therefore unseen, never
// owned, and readiness waits for a listener this check can attribute.
func inspectServiceListeners(port, processGroupID int) (serviceListenerVerdict, error) {
	ctx, cancel := context.WithTimeout(context.Background(), serviceListenerInspectTimeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, lsofPath, "-nP", "-w", "-iTCP:"+strconv.Itoa(port), "-sTCP:LISTEN", "-Fg").Output()
	var exit *exec.ExitError
	if err != nil && (ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 || len(output) != 0) {
		// lsof exits 1 with no output when nothing matches.
		return 0, fmt.Errorf("list listeners on port %d: %w", port, err)
	}
	// Each process set starts with p<pid>, then g<pgid>; file lines follow.
	var groups []string
	for _, line := range strings.Split(string(output), "\n") {
		switch {
		case strings.HasPrefix(line, "p"):
			groups = append(groups, "")
		case strings.HasPrefix(line, "g") && len(groups) > 0:
			groups[len(groups)-1] = strings.TrimPrefix(line, "g")
		}
	}
	if len(groups) == 0 {
		return serviceListenerUnseen, nil
	}
	for _, group := range groups {
		if group != strconv.Itoa(processGroupID) {
			return serviceListenerForeign, nil
		}
	}
	return serviceListenerOwned, nil
}
