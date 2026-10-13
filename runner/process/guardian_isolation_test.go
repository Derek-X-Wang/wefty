//go:build darwin || linux

package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	wr "github.com/Derek-X-Wang/wefty/runner"
	"golang.org/x/sys/unix"
)

type groupChildResult struct {
	Positive  bool
	Lost      bool
	GroupGone bool
}

// This branch runs before TestMain builds its fixtures. The parent supplies
// their paths, and only signals this child's newly created group.
func guardianGroupChild() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() { <-signals; cancel() }()
	adapter := NewAdapter(New(Config{GuardianExecutable: os.Getenv("WEFTY_871_GUARDIAN"), TerminationGraceTime: 20 * time.Millisecond, ProcessReapTimeout: time.Second}))
	authority := testAuthority("group-job", "group-attempt", "group-fence")
	var pgid int
	var stdoutFrame strings.Builder
	_, runErr := adapter.Run(ctx, wr.Request{
		Authority: authority, LifetimeBoundary: wr.AgentBootLifetime, IdlePolicy: IgnoreIdle,
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: os.Getenv("WEFTY_871_PAYLOAD")}, Argv: []string{"processhelper", "hang-both-streams"}, WorkingDirectory: os.TempDir()},
	}, OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
		if event.Stream == contract.LogStdout {
			stdoutFrame.Write(event.Bytes)
			frame, _, complete := strings.Cut(stdoutFrame.String(), "\n")
			if !complete {
				return nil
			}
			id, err := strconv.Atoi(strings.TrimSpace(frame))
			if err != nil {
				return err
			}
			pgid = id
			sid, err := unix.Getsid(id)
			if err != nil {
				return err
			}
			ownSID, err := unix.Getsid(0)
			if err != nil {
				return err
			}
			if sid != ownSID {
				return fmt.Errorf("payload session %d differs from agent %d", sid, ownSID)
			}
			ready := os.Getenv("WEFTY_871_READY")
			if err := os.WriteFile(ready+".tmp", []byte(strconv.Itoa(id)), 0600); err != nil {
				return err
			}
			return os.Rename(ready+".tmp", ready)
		}
		return nil
	}))
	receipt, _ := adapter.ReapAndVerify(context.Background(), wr.ReapRequest{Authority: authority})
	value := groupChildResult{Positive: receipt.RuntimeQuiesced, Lost: errors.Is(runErr, ErrGuardianLost), GroupGone: pgid > 0 && errors.Is(syscall.Kill(-pgid, 0), syscall.ESRCH)}
	data, _ := json.Marshal(value)
	_ = os.WriteFile(os.Getenv("WEFTY_871_RESULT"), data, 0600)
}

func TestGuardianSurvivesAgentGroupSignals(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			dir := t.TempDir()
			ready := filepath.Join(dir, "ready")
			result := filepath.Join(dir, "result")
			self, _ := os.Executable()
			command := exec.Command(self)
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			command.Env = append(os.Environ(), "WEFTY_871_CHILD=1", "WEFTY_871_GUARDIAN="+guardianAgentPath, "WEFTY_871_PAYLOAD="+helperPath, "WEFTY_871_READY="+ready, "WEFTY_871_RESULT="+result)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			joined := false
			pgid := 0
			groupGone := false
			defer func() {
				if !joined {
					_ = command.Process.Kill()
					<-done
				}
				if pgid > 0 && !groupGone && !isMissingProcessGroup(queryProcessGroup(pgid)) {
					_ = killProcessGroup(pgid)
					waitForGroupGone(t, pgid)
				}
			}()
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if data, err := os.ReadFile(ready); err == nil {
					pgid, _ = strconv.Atoi(string(data))
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if pgid <= 0 {
				t.Fatal("child payload did not report group and session")
			}
			if err := syscall.Kill(-command.Process.Pid, sig); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				joined = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("group signal did not finish guarded stop")
			}
			data, err := os.ReadFile(result)
			if err != nil {
				t.Fatal(err)
			}
			var value groupChildResult
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatal(err)
			}
			groupGone = value.GroupGone
			if !value.Positive || value.Lost || !value.GroupGone {
				t.Fatalf("group signal result=%+v, want positive receipt after group ESRCH", value)
			}
		})
	}
}

func waitForGroupGone(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(syscall.Kill(-pgid, 0), syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("created group %d not confirmed gone", pgid)
}

func TestPriorBootLegacyRefusesReceipt(t *testing.T) {
	a := NewAdapterForBoot(nil, "new")
	receipt, err := a.ReapPriorBoot(context.Background(), wr.PriorBootReapRequest{NodeID: "node", JobID: "job", PriorBootSessionID: "old", CurrentBootSessionID: "new"})
	if err == nil || receipt.RuntimeQuiesced {
		t.Fatalf("legacy record receipt=%+v err=%v, want refusal", receipt, err)
	}
}
