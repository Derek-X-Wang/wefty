//go:build darwin || linux

package process

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestGuardedPayloadDoesNotInheritGuardianControlEndpoint(t *testing.T) {
	sink := newCollectingSink()
	result, err := New(Config{GuardianExecutable: guardianAgentPath}).Run(context.Background(), Request{
		AttemptID: "attempt-guardian-fd", Guarded: true, IdlePolicy: IgnoreIdle,
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: helperPath},
			Argv:       []string{"process-helper", "check-no-inherited-socket"}, WorkingDirectory: t.TempDir(),
		},
	}, sink)
	if err != nil || result.SpawnError != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("guarded socket check result = %#v err=%v stderr=%q, want no inherited control socket", result, err, joinStream(sink.Events(), contract.LogStderr))
	}
}

func TestAdapterRefusesQuiescenceAfterGuardianExitedReportAndWaitError(t *testing.T) {
	// This faulty Guardian fixture sends valid statuses, then exits nonzero.
	// Exercise the real guarded runner's finish arm and adapter receipt policy.
	// It reports its own PID, never a real group such as 1, so no code that
	// later signals the reported group can reach unrelated processes.
	guardianPath := filepath.Join(t.TempDir(), "faulty-guardian")
	script := `#!/bin/sh
IFS= read -r start <&3 || exit 18
printf '{"type":"started","pid":%d,"process_group_id":%d}\n' "$$" "$$" >&3
printf '%s\n' '{"type":"exited","result":{"exit_code":0}}' >&3
exit 17
`
	if err := os.WriteFile(guardianPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	adapter := NewAdapter(New(Config{GuardianExecutable: guardianPath}))
	authority := testAuthority("job-guardian-wait", "attempt-guardian-wait", "fence-guardian-wait")
	result, runErr := adapter.Run(context.Background(), workloadrunner.Request{
		Authority: authority, LifetimeBoundary: workloadrunner.AgentBootLifetime, IdlePolicy: IgnoreIdle,
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: helperPath},
			Argv:       []string{"process-helper", "exit", "0"}, WorkingDirectory: t.TempDir(),
		},
	}, nil)
	var exitErr *exec.ExitError
	if !errors.Is(runErr, ErrGuardianLost) || !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 17 || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureProcessWait {
		t.Errorf("Adapter.Run result = %#v err=%v, want Guardian loss preserving wait error", result.Outcome, runErr)
	}
	receipt, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: authority})
	if !errors.Is(err, ErrGuardianLost) || receipt.RuntimeQuiesced {
		t.Errorf("ReapAndVerify receipt = %#v err=%v, want refused quiescence after suspect Guardian exit", receipt, err)
	}
}

// A Guardian exit is not evidence that its independently grouped payload exited.
func TestAdapterRefusesQuiescenceAfterGuardianLost(t *testing.T) {
	directory := t.TempDir()
	pidPath := filepath.Join(directory, "payload.pid")
	releasePath := filepath.Join(directory, "release")
	// Redirect stdout and stderr so the surviving payload does not hold the
	// Guardian's output pipes open and block the agent's Guardian Wait. Keeping
	// those pipes open safely withholds a receipt but strands reaping (#871).
	// Force a Guardian crash; the payload remains in its own process group.
	execution := contract.ExecutionSpec{
		Executable:       contract.ExecutableSpec{Path: "/bin/sh"},
		Argv:             []string{"sh", "-c", `exec >/dev/null 2>&1; echo $$ > "$1"; while [ ! -f "$2" ]; do /bin/sleep 0.01; done; kill -KILL "$PPID"; exec /bin/sleep 30`, "guardian-loss", pidPath, releasePath},
		WorkingDirectory: t.TempDir(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	adapter := NewAdapter(New(Config{GuardianExecutable: guardianAgentPath, TerminationGraceTime: 10 * time.Millisecond}))
	authority := testAuthority("job-guardian-probe", "attempt-guardian-probe", "fence-guardian-probe")
	t.Cleanup(func() {
		data, _ := os.ReadFile(pidPath)
		pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		if pid > 0 {
			_ = killProcessGroup(pid)
			waitForProcessGone(t, pid)
		}
	})
	finished := make(chan runOutcome, 1)
	go func() {
		result, err := adapter.Run(ctx, workloadrunner.Request{
			Authority: authority, Execution: execution, IdlePolicy: IgnoreIdle,
			LifetimeBoundary: workloadrunner.AgentBootLifetime,
			Started: func() {
				_ = os.WriteFile(releasePath, nil, 0600)
			},
		}, nil)
		finished <- runOutcome{result: result.Outcome, err: err}
	}()
	outcome := awaitRun(t, finished)
	data, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		t.Fatalf("payload PID = %q: %v", data, err)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("payload is not alive after Guardian crash: %v", err)
	}
	receipt, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: authority})
	if !errors.Is(err, ErrGuardianLost) || receipt.RuntimeQuiesced {
		t.Errorf("ReapAndVerify receipt = %#v err=%v, want refused quiescence after Guardian loss", receipt, err)
	}
	if !errors.Is(outcome.err, ErrGuardianLost) || outcome.result.SpawnError == nil || outcome.result.SpawnError.Code != contract.SpawnFailureProcessWait {
		t.Errorf("Adapter.Run result = %#v err=%v, want Guardian process-wait failure", outcome.result, outcome.err)
	}
}

func TestAdapterVerifiesQuiescenceAfterGuardedStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	directory := t.TempDir()
	adapter := NewAdapter(New(Config{GuardianExecutable: guardianAgentPath, TerminationGraceTime: 10 * time.Millisecond}))
	authority := testAuthority("job-guarded-stop", "attempt-guarded-stop", "fence-guarded-stop")
	finished := make(chan runOutcome, 1)
	go func() {
		result, err := adapter.Run(ctx, workloadrunner.Request{
			Authority: authority, LifetimeBoundary: workloadrunner.AgentBootLifetime,
			Execution: contract.ExecutionSpec{
				Executable: contract.ExecutableSpec{Path: helperPath},
				Argv:       []string{"process-helper", "hang"}, WorkingDirectory: directory,
			},
			IdlePolicy: IgnoreIdle, Started: func() { close(started) },
		}, nil)
		finished <- runOutcome{result: result.Outcome, err: err}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("guarded payload did not start")
	}
	cancel()
	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, context.Canceled) || outcome.result.SpawnError != nil || outcome.result.Signal == "" || outcome.result.TerminationCause != contract.TerminationCauseGuardian {
		t.Fatalf("Adapter.Run result = %#v err=%v, want guarded stop with payload exit", outcome.result, outcome.err)
	}
	receipt, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: authority})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceAttempt || receipt.BootSessionID != authority.BootSessionID {
		t.Fatalf("ReapAndVerify receipt = %#v err=%v, want positive guarded-stop receipt", receipt, err)
	}
}
