package process

import (
	"errors"
	"syscall"
	"testing"
	"time"
)

func prepareEscapedOutputReaper(t *testing.T) {}

func reapEscapedOutputChild(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("released escaped child %d did not exit", pid)
}
