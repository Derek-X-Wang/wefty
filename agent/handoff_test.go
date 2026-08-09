//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestHandoffDirectoryExistsPrivatelyBeforeExecutionAndCleansOnSuccess(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	runID := "run_lifecycle"
	path := filepath.Join(root, runID)
	manager := newHandoffManager(root, time.Hour)
	runner := &handoffAssertingRunner{t: t, path: path}
	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		runner:       runner,
		handoffs:     manager,
	}
	claim := handoffClaim(runID, path, nil)
	result, err := a.runProcess(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 || !runner.called {
		t.Fatalf("runProcess() = (%#v, %v), called=%v", result, err, runner.called)
	}
	markerInfo, err := os.Stat(filepath.Join(path, handoffMarkerName))
	if err != nil {
		t.Fatal(err)
	}
	if markerInfo.Mode().Perm() != 0o600 {
		t.Fatalf("marker permissions = %#o, want 0600", markerInfo.Mode().Perm())
	}
	if err := manager.finish(claim.Job.Spec, "node-1", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("successful handoff directory still exists: %v", err)
	}
}

func TestColdRerunWithHandoffFilesPinsOrFailsExplicitly(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	runID := "run_retry"
	path := filepath.Join(root, runID)
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	manager := newHandoffManager(root, time.Hour)
	manager.now = func() time.Time { return now }
	claim := handoffClaim(runID, path, []string{"linux"})

	if err := manager.prepare(claim.Job.Spec, "node-1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "plan.md"), []byte("handoff"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := manager.prepare(claim.Job.Spec, "node-1"); err == nil || !strings.Contains(err.Error(), contract.StableNodeTagPrefix+"node-1") {
		t.Fatalf("unpinned cold rerun error = %v, want explicit stable-node tag failure", err)
	}
	rerun := handoffClaim("run_retry_2", path, []string{"linux", contract.StableNodeTagPrefix + "node-1"})
	rerun.Job.Spec.Labels["handoff_owner_run_id"] = runID
	if err := manager.prepare(rerun.Job.Spec, "node-1"); err != nil {
		t.Fatalf("pinned cold rerun: %v", err)
	}

	if err := manager.finish(rerun.Job.Spec, "node-1", false); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if err := manager.cleanupExpired(""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expired retained handoff directory still exists: %v", err)
	}
}

func TestHandoffDirectoryRejectsSymlink(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	path := filepath.Join(root, "run_symlink")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	manager := newHandoffManager(root, time.Hour)
	err := manager.prepare(handoffClaim("run_symlink", path, nil).Job.Spec, "node-1")
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("symlink prepare error = %v", err)
	}
}

func TestInlineJobProcessReceivesExactRunEnvironment(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	runID := "run_environment"
	handoff := filepath.Join(root, runID)
	token := "wrun_process_secret"
	script := []byte("#!/bin/sh\nprintf '%s\\n' \"$WEFTY_RUN_ID\" \"$WEFTY_L3_ENDPOINT\" \"$WEFTY_L1_ENDPOINT\" \"$WEFTY_RUN_TOKEN\" \"$WEFTY_HANDOFF_DIR\"\n")
	digest := sha256.Sum256(script)
	var output bytes.Buffer
	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		runner:       processrunner.New(processrunner.Config{}),
		handoffs:     newHandoffManager(root, time.Hour),
		outputSinkFactory: func(l1.Claim) processrunner.OutputSink {
			return processrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
				if event.Stream == contract.LogStdout {
					_, _ = output.Write(event.Bytes)
				}
				return nil
			})
		},
	}
	claim := handoffClaim(runID, handoff, nil)
	claim.Job.Spec.Execution = contract.ExecutionSpec{
		Executable: contract.ExecutableSpec{
			InlineBase64: base64.StdEncoding.EncodeToString(script),
			SHA256:       hex.EncodeToString(digest[:]),
			Interpreter:  []string{"/bin/sh"},
			Mode:         0o700,
		},
		Argv: []string{"wefty-inline-" + runID},
		Env: map[string]string{
			contract.EnvRunID: runID, contract.EnvL3Endpoint: "wefty://l3",
			contract.EnvL1Endpoint: "wefty://l1", contract.EnvHandoffDir: handoff,
		},
		SensitiveEnv:     map[string]string{contract.EnvRunToken: token},
		WorkingDirectory: t.TempDir(),
		HandoffDirectory: handoff,
	}
	result, err := a.runProcess(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("runProcess() = (%#v, %v)", result, err)
	}
	want := runID + "\nwefty://l3\nwefty://l1\n[REDACTED]\n" + handoff + "\n"
	if output.String() != want {
		t.Fatalf("process environment output = %q, want %q", output.String(), want)
	}
}

func handoffClaim(runID, path string, tags []string) l1.Claim {
	return l1.Claim{
		Job: l1.Job{Spec: contract.JobSpec{
			Kind:        "process",
			RoutingTags: tags,
			Labels:      map[string]string{"run_id": runID},
			Execution: contract.ExecutionSpec{
				HandoffDirectory: path,
			},
		}},
		Lease: l1.AttemptLease{AttemptID: "attempt-handoff"},
	}
}

type handoffAssertingRunner struct {
	t      *testing.T
	path   string
	called bool
}

func (r *handoffAssertingRunner) Run(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	r.t.Helper()
	r.called = true
	info, err := os.Stat(r.path)
	if err != nil {
		r.t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		r.t.Fatalf("handoff directory mode = %v/%#o", info.IsDir(), info.Mode().Perm())
	}
	if request.Execution.HandoffDirectory != r.path {
		r.t.Fatalf("runner handoff directory = %q, want %q", request.Execution.HandoffDirectory, r.path)
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}
