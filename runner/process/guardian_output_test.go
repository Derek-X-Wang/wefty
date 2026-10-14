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
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	wr "github.com/Derek-X-Wang/wefty/runner"
)

func TestGuardianExitedWithEscapedOutput(t *testing.T) {
	if os.Getenv("WEFTY_871_OUTPUT_CHILD") != "1" {
		self, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, self, "-test.run=^TestGuardianExitedWithEscapedOutput$", "-test.v")
		command.Env = append(os.Environ(), "WEFTY_871_OUTPUT_CHILD=1", "WEFTY_871_PAYLOAD="+helperPath, "WEFTY_871_GUARDIAN="+guardianAgentPath)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("output fixture: %v\n%s", err, output)
		}
		t.Logf("%s", output)
		return
	}
	// Isolate Linux subreaper state in this child test process. It adopts and
	// waits for the real escaped descendant, so no test depends on PID 1 reaping.
	prepareEscapedOutputReaper(t)
	for _, blocked := range []bool{false, true} {
		t.Run(strconv.FormatBool(blocked), func(t *testing.T) {
			dir := t.TempDir()
			ready := filepath.Join(dir, "ready")
			release := filepath.Join(dir, "release")
			pidPath := filepath.Join(dir, "pid")
			defer func() {
				if err := os.WriteFile(release, nil, 0600); err != nil {
					t.Error(err)
				}
				data, err := os.ReadFile(pidPath)
				if err == nil {
					pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
					if err != nil {
						t.Error(err)
					} else {
						reapEscapedOutputChild(t, pid)
					}
				}
			}()
			adapter := NewAdapter(New(Config{GuardianExecutable: guardianAgentPath, TerminationGraceTime: 20 * time.Millisecond, ProcessReapTimeout: time.Second}))
			authority := testAuthority("job", "attempt", "fence")
			entered := false
			sink := OutputSinkFunc(func(ctx context.Context, event contract.LogEvent) error {
				entered = true
				if blocked {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			})
			result, err := adapter.Run(context.Background(), wr.Request{Authority: authority, LifetimeBoundary: wr.AgentBootLifetime, IdlePolicy: IgnoreIdle, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: helperPath}, Argv: []string{"processhelper", "escape-stdout", ready, release, pidPath}, WorkingDirectory: dir}}, sink)
			if !entered || err != nil || errors.Is(err, ErrGuardianLost) || !result.Outcome.LogEvidenceIncomplete || result.Outcome.SpawnError != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
				t.Fatalf("proven exit with escaped output: result=%+v err=%v entered=%t", result, err, entered)
			}
			receipt, err := adapter.ReapAndVerify(context.Background(), wr.ReapRequest{Authority: authority})
			if err != nil || !receipt.RuntimeQuiesced {
				t.Fatalf("receipt=%+v err=%v", receipt, err)
			}
		})
	}
}
