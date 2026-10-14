//go:build darwin || linux

package process

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	wr "github.com/Derek-X-Wang/wefty/runner"
)

func TestGuardianSpontaneousExitRefusesUnconfirmedGroup(t *testing.T) {
	var member *exec.Cmd
	var memberOutput io.ReadCloser
	pgid := 0
	defer func() {
		if member != nil {
			_ = member.Process.Kill()
			_ = member.Wait()
		}
		if memberOutput != nil {
			memberOutput.Close()
		}
		if pgid > 0 {
			waitForGroupGone(t, pgid)
		}
	}()
	dir := t.TempDir()
	release := filepath.Join(dir, "release")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a := NewAdapter(New(Config{GuardianExecutable: guardianAgentPath, TerminationGraceTime: 20 * time.Millisecond, ProcessReapTimeout: 100 * time.Millisecond}))
	authority := testAuthority("spontaneous-job", "spontaneous-attempt", "spontaneous-fence")
	result, err := a.Run(ctx, wr.Request{Authority: authority, LifetimeBoundary: wr.AgentBootLifetime, IdlePolicy: IgnoreIdle,
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: helperPath}, Argv: []string{"processhelper", "wait-release", filepath.Join(dir, "ready"), release, "10s"}, WorkingDirectory: t.TempDir()},
		ProcessStarted: func(group int) error {
			pgid = group
			// The test is this member's parent, outside the payload's group. Withhold
			// its Wait until the run returns. Even after KILL, zombie membership is
			// not kernel ESRCH and cannot authorize a receipt. No PID 1 assumptions.
			member = exec.Command(helperPath, "hang")
			member.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: group}
			var err error
			memberOutput, err = member.StdoutPipe()
			if err != nil {
				return err
			}
			if err := member.Start(); err != nil {
				member = nil
				return err
			}
			_, err = bufio.NewReader(memberOutput).ReadString('\n')
			if err != nil {
				return err
			}
			return os.WriteFile(release, nil, 0600)
		},
	}, nil)
	if !errors.Is(err, ErrGuardianLost) || !result.Outcome.LogEvidenceIncomplete {
		t.Fatalf("spontaneous cleanup result=%+v err=%v, want Guardian loss for unproven group", result, err)
	}
	if pgid <= 0 || isMissingProcessGroup(queryProcessGroup(pgid)) {
		t.Fatal("probe did not retain unconfirmed group membership")
	}
	receipt, err := a.ReapAndVerify(context.Background(), wr.ReapRequest{Authority: authority})
	if !errors.Is(err, ErrGuardianLost) || receipt.RuntimeQuiesced {
		t.Fatalf("receipt=%+v err=%v, want refused receipt", receipt, err)
	}
}

func TestGuardianFinalKillHasBoundedAbsenceProof(t *testing.T) {
	// The kernel group is already absent, but the Guardian's Wait delivery is
	// missing. Neither absence alone nor sending KILL can invent the outcome.
	signalCalls := 0
	term := func(int) bool { signalCalls++; return false }
	kill := func(int) error { signalCalls++; return nil }
	query := func(int) error { return syscall.ESRCH }
	wait := make(chan waitResult, 1)
	done := make(chan error, 1)
	go func() {
		_, _, err := guardianTerminateAndWaitWithGroupOps(12345, 20*time.Millisecond, 100*time.Millisecond, wait, term, kill, query)
		done <- err
	}()
	joined := false
	defer func() {
		wait <- waitResult{}
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("proof waiter did not join")
			}
		}
	}()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, ErrGuardianLost) || signalCalls != 2 {
			t.Fatalf("proof err=%v signalCalls=%d, want bounded Guardian loss after TERM/KILL", err, signalCalls)
		}
	case <-time.After(time.Second):
		t.Fatal("final SIGKILL left an unbounded group/Wait proof")
	}
}
