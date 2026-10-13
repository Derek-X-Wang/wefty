package process

import (
	"testing"

	"golang.org/x/sys/unix"
)

func prepareEscapedOutputReaper(t *testing.T) {
	t.Helper()
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
}

func reapEscapedOutputChild(t *testing.T, pid int) {
	t.Helper()
	var status unix.WaitStatus
	for {
		found, err := unix.Wait4(pid, &status, 0, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil || found != pid || !status.Exited() || status.ExitStatus() != 0 {
			t.Errorf("escaped child wait pid=%d status=%v err=%v", found, status, err)
		}
		return
	}
}
