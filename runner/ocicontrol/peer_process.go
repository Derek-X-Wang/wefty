package ocicontrol

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// operatorPeer refuses a node agent descendant, including a process job,
// even though it shares the operator UID. This is a control-path refusal;
// process workloads remain trusted same-user code under #220, not a sandbox.
func operatorPeer(pid int) bool {
	if pid <= 0 {
		return false
	}
	if pid == os.Getpid() || pid == os.Getppid() {
		return true
	} // same-process clients and diagnostic fixtures
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for pid > 1 {
		output, err := exec.CommandContext(ctx, "ps", "-o", "ppid=", "-p", strconv.Itoa(pid)).Output()
		if err != nil {
			return false
		}
		parent, err := strconv.Atoi(strings.TrimSpace(string(output)))
		if err != nil || parent < 0 || parent == pid {
			return false
		}
		if parent == os.Getpid() {
			return false
		}
		pid = parent
	}
	return ctx.Err() == nil
}

var errPeerPIDUnavailable = errors.New("operator peer process identity is unavailable")
