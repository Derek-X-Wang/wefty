//go:build darwin || linux

package process

import (
	"os/exec"
	"sync/atomic"
	"testing"
	"time"
)

// Once a payload's leader is reaped its group ID is no longer reserved, so a
// stop must not send the final SIGKILL to a group that has already emptied: the
// ID may belong to an unrelated process by then (#1258).
func TestReapedLeaderGroupEscalatesOnlyWhileStillPresent(t *testing.T) {
	// reaped starts a payload whose leader exits zero at once and is reaped,
	// leaving one child that ignores TERM and lives for childLife.
	reaped := func(t *testing.T, childLife string) (int, chan waitResult) {
		t.Helper()
		command := exec.Command("/bin/sh", "-c", `(trap "" TERM; exec sleep `+childLife+`) & exit 0`)
		if err := configureProcessGroup(command); err != nil {
			t.Fatal(err)
		}
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		err := command.Wait()
		wait := make(chan waitResult, 1)
		wait <- waitResult{err: err, state: command.ProcessState}
		if !processGroupAlive(command.Process.Pid) {
			t.Fatal("the child did not keep the reaped leader's group alive")
		}
		t.Cleanup(func() { _ = killProcessGroup(command.Process.Pid) })
		return command.Process.Pid, wait
	}
	countingRunner := func(grace time.Duration) (*Runner, *atomic.Int32) {
		runner := New(Config{TerminationGraceTime: grace})
		var kills atomic.Int32
		runner.killGroup = func(processGroupID int) error {
			kills.Add(1)
			return killProcessGroup(processGroupID)
		}
		return runner, &kills
	}
	const grace = 5 * time.Second
	stops := []struct {
		name string
		stop func(*Runner, int, chan waitResult)
	}{
		{"cleanup after a spontaneous exit", func(r *Runner, group int, _ chan waitResult) { r.cleanupRemainingGroup(group) }},
		{"agent stop after the exit was reaped", func(r *Runner, group int, wait chan waitResult) { r.terminateAndWait(group, wait) }},
	}
	for _, stop := range stops {
		t.Run(stop.name+": group empties within grace", func(t *testing.T) {
			group, wait := reaped(t, "0.3")
			runner, kills := countingRunner(grace)
			started := time.Now()
			stop.stop(runner, group, wait)
			if got := kills.Load(); got != 0 {
				t.Fatalf("SIGKILL sent %d time(s) to a group that had already emptied", got)
			}
			if elapsed := time.Since(started); elapsed >= grace {
				t.Fatalf("stop took %s; it should end once the group is gone, not wait out the %s grace", elapsed, grace)
			}
		})
		t.Run(stop.name+": group outlives grace", func(t *testing.T) {
			group, wait := reaped(t, "30")
			runner, kills := countingRunner(200 * time.Millisecond)
			stop.stop(runner, group, wait)
			if got := kills.Load(); got != 1 {
				t.Fatalf("SIGKILL sent %d time(s), want 1 for a group still present after grace", got)
			}
			waitForCondition(t, func() bool { return !processGroupAlive(group) }, "the final SIGKILL clears the group")
		})
	}
}
