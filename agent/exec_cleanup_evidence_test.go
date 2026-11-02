//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/execsweep"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// The command mode runs under the shipped wrapper. Its child escapes into a
// separate session, closes output pipes, and ignores TERM like a daemon.
func TestAgentExecSweepProcessHelper(t *testing.T) {
	mode := os.Getenv("AGENT_EXEC_SWEEP_HELPER")
	if mode == "" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	if mode == "command" {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		child := exec.Command(binary, "-test.run=^TestAgentExecSweepProcessHelper$")
		child.Env = append(os.Environ(), "AGENT_EXEC_SWEEP_HELPER=child")
		child.Dir = os.Getenv("TMPDIR")
		child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
	} else if err := os.WriteFile(os.Getenv("AGENT_EXEC_SWEEP_READY"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func startExecSweepProcess(t *testing.T, directory string) (*exec.Cmd, <-chan struct{}) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(t.TempDir(), "ready")
	child := exec.Command(binary, "-test.run=^TestAgentExecSweepProcessHelper$")
	child.Env = append(os.Environ(), "AGENT_EXEC_SWEEP_HELPER=child", "AGENT_EXEC_SWEEP_READY="+ready, "AGENT_EXEC_SWEEP_BINARIES="+filepath.Dir(agentBinaryPath))
	child.Stderr = os.Stderr
	child.Dir = directory
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = child.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = child.Process.Kill()
		waitExecSweepProcess(t, done)
	})
	waitRestartRecoveryPath(t, ready)
	return child, done
}

func waitExecSweepProcess(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(testwait.Bound):
		t.Fatal("exec storage process did not stop")
	}
}

func captureSweepAppender() *httpRunLedgerAppender {
	return &httpRunLedgerAppender{client: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: 4, Body: io.NopCloser(strings.NewReader("good")), Header: make(http.Header)}, nil
	})}}
}

func TestExecCaptureAgentSweepAfterKilledWrapper(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_killed_wrapper")
	claim := retentionClaim(t, "run_killed_wrapper", path)
	claim.Job.Spec.Execution.Env = mailboxSpec("run_killed_wrapper", path, "").Execution.Env
	claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: mailboxTestToken}
	claim.SubmittedByRunLedger = true
	claim.Job.Spec.Labels[contract.LabelRunParams] = captureSpec("good").Labels[contract.LabelRunParams]
	var escapedPID int
	var stderr bytes.Buffer
	executor := completionDirectiveRunFunc(func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
		storage := filepath.Join(request.Execution.Env[contract.EnvRunDir], "wefty-exec-work")
		// Use the shipped wrapper, including its real EXIT/TERM cleanup and
		// blocking wait on a command that ignores TERM. Two empty commits
		// supply the snapshot and its base without unrelated Git fixtures.
		for _, args := range [][]string{
			{"init", "--quiet", storage + "/snapshot.git"},
			{"-C", storage + "/snapshot.git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "--allow-empty", "-m", "base"},
			{"-C", storage + "/snapshot.git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "--allow-empty", "-m", "snapshot"},
		} {
			if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
				return contract.ProcessResult{}, fmt.Errorf("prepare wrapper snapshot: %w: %s", err, output)
			}
		}
		template, err := os.ReadFile("../cmd/wefty/exec_wrapper.sh")
		if err != nil {
			return contract.ProcessResult{}, err
		}
		binary, err := os.Executable()
		if err != nil {
			return contract.ProcessResult{}, err
		}
		quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
		input := "storage=" + quote(storage) + "\nfolder=''\nheartbeat_tick=0.01\nheartbeat_ticks=6000\ncommand=(" + quote(binary) + " '-test.run=^TestAgentExecSweepProcessHelper$')\n"
		script := strings.Replace(string(template), "__WEFTY_EXEC_CLEANUP__", execsweep.Script, 1)
		script = strings.Replace(script, "__WEFTY_EXEC_INPUT__", input, 1)
		ready := filepath.Join(t.TempDir(), "escaped-ready")
		request.Execution.Executable = contract.ExecutableSpec{Path: "/bin/bash"}
		request.Execution.Argv = []string{"bash", "-c", script}
		request.Execution.Env["AGENT_EXEC_SWEEP_HELPER"] = "command"
		request.Execution.Env["AGENT_EXEC_SWEEP_READY"] = ready
		request.Execution.Env["AGENT_EXEC_SWEEP_BINARIES"] = filepath.Dir(agentBinaryPath)
		request.Execution.Env[contract.EnvHandoffDir] = path
		request.Guarded = false // Exercise the direct process runner, without a host boot identity read.
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		type outcome struct {
			result contract.ProcessResult
			err    error
		}
		done := make(chan outcome, 1)
		go func() {
			result, err := processrunner.New(processrunner.Config{TerminationGraceTime: 100 * time.Millisecond}).Run(runCtx, request, sink)
			done <- outcome{result, err}
		}()
		waitRestartRecoveryPath(t, ready)
		raw, err := os.ReadFile(ready)
		if err != nil {
			return contract.ProcessResult{}, err
		}
		escapedPID, err = strconv.Atoi(string(raw))
		if err != nil {
			return contract.ProcessResult{}, err
		}
		t.Cleanup(func() { _ = syscall.Kill(escapedPID, syscall.SIGKILL) })
		cancel()
		select {
		case outcome := <-done:
			if outcome.result.Signal != "killed" {
				t.Errorf("wrapper was not killed before cleanup: %+v, %v", outcome.result, outcome.err)
			}
			if _, err := os.Stat(storage); err != nil {
				t.Errorf("wrapper cleanup unexpectedly retired storage: %v", err)
			}
			if err := syscall.Kill(escapedPID, 0); err != nil {
				t.Errorf("escaped process ended before agent sweep: %v", err)
			}
			return outcome.result, outcome.err
		case <-time.After(testwait.Bound):
			return contract.ProcessResult{}, fmt.Errorf("wrapper did not end after cancellation")
		}
	})
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		handoffs: harness.manager, runtimes: testRuntimeSet(executor), runLedger: captureSweepAppender(),
		clock: systemClock{}, nodeID: "node-1", bootSessionID: "sweep-test", logf: t.Logf,
		outputSinkFactory: func(l1.Claim) processrunner.OutputSink {
			return processrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
				if event.Stream == contract.LogStderr {
					_, _ = stderr.Write(event.Bytes)
				}
				return nil
			})
		},
	})
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if result.Signal != "killed" || result.TerminationCause != contract.TerminationCauseAgent || len(result.StoppedProcesses) != 1 || result.StoppedProcesses[0].PID != escapedPID || (!strings.Contains(result.StoppedProcesses[0].CWD, "/wefty-run-data.") || !strings.HasSuffix(result.StoppedProcesses[0].CWD, "/tmp")) {
		t.Fatalf("killed wrapper verdict/evidence: %+v", result)
	}
	// Orphan zombies on Linux no longer have a cwd; on Darwin launchd reaps
	// the orphan and kill -0 proves it gone. Neither is a live escaped child.
	deadline := time.Now().Add(testwait.Bound)
	for {
		gone := errors.Is(syscall.Kill(escapedPID, 0), syscall.ESRCH)
		if runtime.GOOS == "linux" {
			_, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", escapedPID))
			gone = os.IsNotExist(err)
		}
		if gone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("escaped descendant survived agent sweep")
		}
		time.Sleep(10 * time.Millisecond)
	}
	completion := completionRequest(claim, result, "attempt")
	if completion.Result.StoppedProcesses == nil || (*completion.Result.StoppedProcesses)[0].PID != escapedPID {
		t.Fatalf("Attempt completion omitted agent evidence: %+v", completion)
	}
	if !strings.Contains(stderr.String(), fmt.Sprintf("wefty: stopped process %d (", escapedPID)) {
		t.Fatalf("agent sweep did not reach run stderr: %s", &stderr)
	}
}

func TestExecCaptureAgentSweepMergesWrapperEvidence(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_merge_sweep")
	claim := retentionClaim(t, "run_merge_sweep", path)
	claim.Job.Spec.Execution.Env = mailboxSpec("run_merge_sweep", path, "").Execution.Env
	claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: mailboxTestToken}
	claim.SubmittedByRunLedger = true
	claim.Job.Spec.Labels[contract.LabelRunParams] = captureSpec("good").Labels[contract.LabelRunParams]
	var pid int
	var done <-chan struct{}
	run := completionDirectiveRunFunc(func(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
		storage := filepath.Join(request.Execution.Env[contract.EnvRunDir], "wefty-exec-work", "work")
		if err := os.MkdirAll(storage, 0700); err != nil {
			return contract.ProcessResult{}, err
		}
		var child *exec.Cmd
		child, done = startExecSweepProcess(t, storage)
		pid = child.Process.Pid
		return writingRun(path, "result.json", []byte(`{"exit_code":99,"stopped_processes":[{"pid":4321,"name":"wrapper-child","cwd":"/tmp/earlier"}]}`))(ctx, request, sink)
	})
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		handoffs: harness.manager, runtimes: testRuntimeSet(run), runLedger: captureSweepAppender(),
		clock: systemClock{}, nodeID: "node-1", bootSessionID: "sweep-test", logf: t.Logf,
	})
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	waitExecSweepProcess(t, done)
	if result.ExitCode == nil || *result.ExitCode != 0 || len(result.StoppedProcesses) != 2 || result.StoppedProcesses[0].PID != 4321 || result.StoppedProcesses[1].PID != pid {
		t.Fatalf("wrapper evidence replaced agent evidence or runtime verdict: %+v", result)
	}
}

func TestCapturedExecSnapshotStorageCarriesCleanupEvidence(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_cleanup")
	document := []byte(`{"exit_code":0,"stopped_processes":[{"pid":4321,"name":"sleep","cwd":"/tmp/capture/work"}]}`)
	lifecycle := uploadingLifecycle(t, harness.manager, &resultUploadRecorder{}, writingRun(path, "result.json", document))
	claim := retentionClaim(t, "run_cleanup", path)
	claim.SubmittedByRunLedger = true
	claim.Job.Spec.Labels[contract.LabelRunParams] = `{"snapshot_storage":"/tmp/capture"}`
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.StoppedProcesses) != 1 || result.StoppedProcesses[0].PID != 4321 {
		t.Fatalf("Attempt result omitted cleanup evidence for snapshot_storage: %+v", result)
	}
}
