//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func restartRecoveryClaim(dir string) l1.Claim {
	return l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Restartable: true, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", "exit 0"}, WorkingDirectory: dir}}}, Lease: l1.AttemptLease{AttemptID: "successor", FencingToken: "fence", LeaseTTL: time.Minute}}
}

func waitRestartRecoveryPath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func TestRestartableFailureBeforeControlRemainsFailure(t *testing.T) {
	for _, control := range []string{"shutdown", "restart directive"} {
		t.Run(control, func(t *testing.T) {
			_, _, resource, _ := processInventoryLifecycleFixture(t)
			failure := errors.New("output sink broke before stop")
			executor := &restartFailureExecutor{t: t, failure: failure}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{managedResource: resource, runtimes: testRuntimeSet(executor), clock: systemClock{}, observer: newLifecycleObserver(systemClock{})})
			executor.later = func() {
				if control == "shutdown" {
					lifecycle.fenceMailbox(ErrAgentShutdown)
				} else {
					lifecycle.interruption.Store(&restartInterruption{reason: l1.InterruptionUpgrade, key: "late"})
					lifecycle.fenceMailbox(errAttemptDirectiveRestart)
				}
			}
			claim := restartRecoveryClaim(t.TempDir())
			result, err := lifecycle.runWorkload(t.Context(), claim)
			request := lifecycle.deliveryCompletionRequest(claim, result, "attempt")
			if !errors.Is(err, failure) || request.InterruptionReason != "" || request.Result.OutputError != failure.Error() {
				t.Fatalf("failure relabelled: %+v err=%v", request, err)
			}
		})
	}
}

type restartFailureExecutor struct {
	t       *testing.T
	failure error
	later   func()
	ran     bool
}

func (*restartFailureExecutor) GuardianAvailable() bool { return true }
func (e *restartFailureExecutor) Run(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	e.ran = true
	if request.StopInitiated == nil {
		e.t.Fatal("missing supervision stop fence")
	}
	request.StopInitiated(e.failure)
	if e.later != nil {
		e.later()
	}
	return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, e.failure
}

func TestRestartableDelayedCleanupCannotTouchSuccessor(t *testing.T) {
	dir := t.TempDir()
	spec := contract.JobSpec{Restartable: true, Execution: contract.ExecutionSpec{HandoffDirectory: dir, Env: map[string]string{contract.EnvRunID: "run"}}}
	prepare := func(attempt string) (*runMailbox, string, string) {
		t.Helper()
		mailbox, err := prepareRunMailbox(spec, attempt, newRecordingAppender(""), time.Hour, nil, t.Logf)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(mailbox.close)
		storage := filepath.Join(mailbox.directory, "wefty-exec-work")
		allocation := execCaptureTempAllocation(t, storage)
		for _, name := range []string{"work"} {
			if err := os.MkdirAll(filepath.Join(storage, name), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(storage, name, "keep"), []byte(attempt), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(allocation, "tmp", "keep"), []byte(attempt), 0600); err != nil {
			t.Fatal(err)
		}
		file, err := os.CreateTemp("/tmp", "wefty-run.")
		if err != nil {
			t.Fatal(err)
		}
		link := file.Name()
		file.Close()
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(allocation, "tmp"), link); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Remove(link) })
		if err := os.WriteFile(filepath.Join(storage, "tmp-link"), []byte(link+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		return mailbox, storage, link
	}
	_, previous, previousTmp := prepare("previous")
	previousTarget, err := os.Readlink(previousTmp)
	if err != nil {
		t.Fatal(err)
	}
	observed, release := filepath.Join(dir, "cleanup-observed"), filepath.Join(dir, "cleanup-release")
	hook := filepath.Join(dir, "bash-env")
	if err := os.WriteFile(hook, []byte(fmt.Sprintf(`function /bin/chmod() {
  if [ "$1" = u+w ]; then
    : > %q
    while [ ! -f %q ] && kill -0 %d 2>/dev/null; do sleep 0.01; done
  fi
  command /bin/chmod "$@"
}
`, observed, release, os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile("../internal/execsweep/exec_cleanup.sh")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/bash", "-c", string(script)+"\nretire_exec_storage \"$1\" capture\n", "cleanup", previous)
	command.Env = append(os.Environ(), "BASH_ENV="+hook)
	done := make(chan error, 1)
	go func() { done <- command.Run() }()
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
	waitRestartRecoveryPath(t, observed)
	next, storage, tmp := prepare("successor")
	if storage == previous {
		t.Fatal("exec storage reused across Attempts")
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("predecessor cleanup: %v", err)
	}
	for _, path := range []string{previous, previousTmp} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("old cleanup did not retire %s: %v", path, err)
		}
	}
	// The helper acknowledges the rename, not the deletion: its detached
	// delete step still owns trash and lock folders beside the old storage.
	// Wait for them, so TempDir cleanup does not race it (#1197).
	waitRestartRecoveryNoExecTrash(t, filepath.Dir(previous))
	if _, err := os.Lstat(filepath.Dir(previousTarget)); !os.IsNotExist(err) {
		t.Fatalf("predecessor temp survived deletion: %v", err)
	}
	target, err := os.Readlink(tmp)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(storage, "work/keep"), filepath.Join(target, "keep"), filepath.Join(tmp, "keep"), filepath.Join(next.directory, "params.json")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("delayed predecessor cleanup touched successor: %s: %v", path, err)
		}
	}
}

func waitRestartRecoveryNoExecTrash(t *testing.T, directory string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		var left []string
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".wefty-") {
				left = append(left, entry.Name())
			}
		}
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("exec cleanup still owns %v in %s", left, directory)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Managed resources belong to services again. Their preparation failures keep
// ordinary failure completion; restartable one-shots also fail normally when
// their guarded command cannot prepare its working directory.
type restartPendingFailureResource struct{ *processManagedResource }

func (resource *restartPendingFailureResource) prepareAttempt(job, attempt string) (managedResourceAttempt, func(), error) {
	result, cleanup, err := resource.processManagedResource.prepareAttempt(job, attempt)
	result.prepareProcess = func() error { return &os.PathError{Op: "write", Path: "process inventory", Err: os.ErrPermission} }
	return result, cleanup, err
}

type preparationUnexpectedExecutor struct{ ran bool }

func (*preparationUnexpectedExecutor) GuardianAvailable() bool { return true }
func (e *preparationUnexpectedExecutor) Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error) {
	e.ran = true
	code := 0
	return contract.ProcessResult{ExitCode: &code}, nil
}

func TestRestartableOrdinaryPreparationErrorsCompleteFailure(t *testing.T) {
	for _, stage := range []string{"initial denied managed directory", "pending record write", "restartable missing working directory"} {
		t.Run(stage, func(t *testing.T) {
			_, root, resource, _ := processInventoryLifecycleFixture(t)
			var managed managedResourceManager = resource
			claim := restartRecoveryClaim(t.TempDir())
			executor := &preparationUnexpectedExecutor{}
			var runtime processrunner.Executor = executor
			if stage == "restartable missing working directory" {
				claim.Job.Spec.Execution.WorkingDirectory = filepath.Join(t.TempDir(), "missing")
				runtime = processrunner.New(processrunner.Config{GuardianExecutable: agentBinaryPath})
			} else {
				claim.Job.Spec.Class = contract.JobClassService
				claim.Job.Spec.Restartable = false
				if stage == "initial denied managed directory" {
					paths, err := root.CreateService("job", 1)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.Chmod(paths.Attempts, 0500); err != nil {
						t.Fatal(err)
					}
					defer os.Chmod(paths.Attempts, 0700)
				} else {
					managed = &restartPendingFailureResource{resource}
				}
			}
			var completed l1.CompletionRequest
			var calls atomic.Int64
			client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/complete") {
					http.NotFound(w, r)
					return
				}
				if err := json.NewDecoder(r.Body).Decode(&completed); err != nil {
					t.Error(err)
				}
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(l1.Job{})
			}), time.Second)
			defer stop()
			defer client.Close()
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, managedResource: managed, runtimes: testRuntimeSet(runtime), clock: systemClock{}, renewalInterval: time.Hour, observer: newLifecycleObserver(systemClock{})})
			_, err := lifecycle.execute(t.Context(), claim, time.Now())
			if executor.ran || calls.Load() != 1 || completed.Result.SpawnError == nil || completed.InterruptionReason != "" {
				t.Fatalf("preparation did not complete as failure: calls=%d completion=%+v ran=%v err=%v", calls.Load(), completed, executor.ran, err)
			}
		})
	}
}

func TestRestartableLeaseRecoveryTimingBudget(t *testing.T) {
	teardown := processrunner.DefaultTerminationGraceTime + processrunner.DefaultProcessReapTimeout
	const margin = time.Second
	if l1.MinimumOneshotRestartDelay < teardown+margin {
		t.Fatalf("lease-loss delay %s shorter than Guardian teardown %s plus margin", l1.MinimumOneshotRestartDelay, teardown)
	}
	for _, ttl := range []time.Duration{l1.DefaultLeaseDuration, 2 * time.Second} {
		interval := renewalDelay(ttl, DefaultRenewalInterval)
		window := ttl - interval
		if window+l1.MinimumOneshotRestartDelay < teardown+margin {
			t.Fatalf("ttl %s: expiry window %s does not cover teardown", ttl, window)
		}
	}
}
