package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

// shortenExecReadRecovery makes a read that never succeeds give up quickly.
// Use it only while that read is stubbed to fail or to block, so the short
// budget ends the same way however slowly the real stack answers. A healthy
// read must not share it: restore the production budget before one.
func shortenExecReadRecovery(t *testing.T) (restore func()) {
	t.Helper()
	old := execReadRecoveryBudget
	execReadRecoveryBudget = 300 * time.Millisecond
	restore = func() { execReadRecoveryBudget = old }
	t.Cleanup(restore)
	shortenExecTimers(t)
	return restore
}

func execReadFailure(kind string) (*http.Response, error) {
	if kind == "transport" {
		return nil, fmt.Errorf("L3 is restarting")
	}
	status, code := 503, "unavailable"
	if kind == "internal" {
		status, code = 500, "internal"
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"error":{"code":%q,"message":"L3 restarting","retryable":true}}`, code)))}, nil
}

func TestExecObservationRetriesWithoutCancellation(t *testing.T) {
	for _, path := range []string{"logs", "record"} {
		for _, kind := range []string{"transport", "unavailable", "internal"} {
			t.Run(path+"/"+kind, func(t *testing.T) {
				// The run is healthy, so its reads keep the production budget.
				// Only the stubbed failures decide how a read recovers.
				shortenExecTimers(t)
				stack := newExecStack(t, execTestIdleLimit)
				repo := execTestRepo(t)
				t.Chdir(repo)
				base := stack.clients.l3.client.Transport
				reads, cancels := 0, 0
				stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if strings.HasSuffix(r.URL.Path, "/cancel") {
						cancels++
					}
					target := strings.HasSuffix(r.URL.Path, "/logs")
					if path == "record" {
						target = strings.Count(r.URL.Path, "/") == 3
					}
					if r.Method == http.MethodGet && target {
						reads++
						if reads <= 2 {
							return execReadFailure(kind)
						}
					}
					return base.RoundTrip(r)
				})
				// Keep the two output batches separate until the observer has
				// delivered the first, instead of pacing the command with a sleep.
				release := filepath.Join(t.TempDir(), "release")
				out := &execReviewOutput{token: "first\n", seen: make(chan struct{})}
				ctx, stop := context.WithTimeout(t.Context(), 2*time.Minute)
				defer stop()
				released := make(chan error, 1)
				go func() {
					select {
					case <-out.seen:
						released <- os.WriteFile(release, nil, 0600)
					case <-ctx.Done():
						// Also release the command if observation fails early.
						_ = os.WriteFile(release, nil, 0600)
						released <- ctx.Err()
					}
				}()
				var errOut bytes.Buffer
				err := execute(ctx, stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "printf 'first\\n'; while [ ! -f \"$1\" ]; do sleep 0.01; done; printf 'last\\n'", "sh", release}, out, &errOut)
				stop()
				releaseErr := <-released
				if err != nil || releaseErr != nil || cancels != 0 || reads < 3 || strings.Count(out.String(), "first\n") != 1 || strings.Count(out.String(), "last\n") != 1 {
					t.Fatalf("observation interrupted healthy run: err=%v release=%v reads=%d cancels=%d stdout=%q stderr=%q", err, releaseErr, reads, cancels, out.String(), errOut.String())
				}
			})
		}
	}
}

func TestExecLostObservationLeavesRunAndStorage(t *testing.T) {
	shortenExecReadRecovery(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	base := stack.clients.l3.client.Transport
	cancels := 0
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			cancels++
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/logs") {
			return execReadFailure("transport")
		}
		return base.RoundTrip(r)
	})
	var errOut bytes.Buffer
	started := time.Now()
	holdExecNode(t, stack)
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "exit 0"}, io.Discard, &errOut)
	stack.clients.l3.client.Transport = base
	var params execParams
	_ = json.Unmarshal(stack.requests[0].Params, &params)
	t.Cleanup(func() { _ = cleanupExecStorage(params.Storage) })
	runs, e := stack.clients.listRuns(t.Context(), "", 20)
	if e != nil || len(runs.Runs) != 1 {
		t.Fatalf("runs: %+v %v", runs, e)
	}
	id := runs.Runs[0].RunID
	if commandExitCode(err) != exitUnavailable || cancels != 0 || runIsTerminal(runs.Runs[0].Status) {
		t.Fatalf("lost watch cancelled run or reported wrong exit: err=%v cancels=%d run=%+v", err, cancels, runs.Runs[0])
	}
	if time.Since(started) > 5*time.Second {
		t.Fatal("read recovery exceeded its bound")
	}
	for _, text := range []string{"the run continues", id, "wefty logs " + id + " --follow", "wefty cancel " + id, params.Storage} {
		if !strings.Contains(errOut.String(), text) {
			t.Errorf("lost-watch message lacks %q: %q", text, errOut.String())
		}
	}
	if _, e := os.Stat(params.Storage); e != nil {
		t.Fatalf("active capture removed: %v", e)
	}
}

func TestExecVerdictUsesStderr(t *testing.T) {
	for _, exit := range []int{0, 7} {
		t.Run(fmt.Sprint(exit), func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			var out, errOut bytes.Buffer
			err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", fmt.Sprintf("printf 'command-output\\n'; exit %d", exit)}, &out, &errOut)
			verdict, code := "succeeded", 0
			if exit != 0 {
				verdict, code = "failed", 10
			}
			if (err != nil && commandExitCode(err) != code) || (err == nil && code != 0) || out.String() != "command-output\n" || !strings.HasSuffix(errOut.String(), verdict+"\n") {
				t.Fatalf("verdict leaked into stdout: %v stdout=%q stderr=%q", err, out.String(), errOut.String())
			}
		})
	}
}

func TestExecTagConflictNamesExec(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--tag", "wefty:node:other", "--", "true"}, io.Discard, io.Discard)
	if err == nil || commandExitCode(err) != exitUsage || !strings.Contains(err.Error(), "--tag") || strings.Contains(err.Error(), "--mount") || strings.Contains(err.Error(), "--node") || len(stack.requests) != 0 {
		t.Fatalf("exec tag conflict: %v requests=%d", err, len(stack.requests))
	}

	// Stable tags are normalized even when the installed node ID has capitals.
	t.Chdir(execTestRepo(t))
	node, e := stack.clients.getNode(t.Context(), "node-cli")
	if e != nil {
		t.Fatal(e)
	}
	node.NodeID = "NODE-CLI"
	writeExecFile(t, stack.clients.nodeConfigPath, `{"version":1,"node_id":"NODE-CLI","control_socket":"/tmp/unused.sock"}`)
	base := stack.clients.l1.client.Transport
	stack.clients.l1.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/nodes/NODE-CLI" {
			raw, _ := json.Marshal(node)
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}, nil
		}
		return base.RoundTrip(r)
	})
	err = execute(t.Context(), stack.clients, false, []string{"exec", "--tag", "wefty:node:NODE-CLI", "--", "true"}, io.Discard, io.Discard)
	if err != nil || len(stack.requests) != 1 {
		t.Fatalf("exec refused its own normalized tag: %v requests=%d", err, len(stack.requests))
	}
}

func TestExecCleanupFailurePreservesVerdict(t *testing.T) {
	for _, exit := range []int{0, 7} {
		t.Run(fmt.Sprint(exit), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			root := os.Getenv("WEFTY_EXEC_STORAGE_ROOT")
			t.Cleanup(func() { _ = os.Chmod(root, 0700) })
			base := stack.clients.l3.client.Transport
			injected := false
			stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				response, err := base.RoundTrip(r)
				if err != nil || injected || r.Method != http.MethodGet || strings.Count(r.URL.Path, "/") != 3 {
					return response, err
				}
				raw, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				response.Body = io.NopCloser(bytes.NewReader(raw))
				var record contract.RunRecord
				if err == nil && json.Unmarshal(raw, &record) == nil && runIsTerminal(record.Status) {
					injected = true
					var params execParams
					_ = json.Unmarshal(stack.requests[0].Params, &params)
					// The wrapper is done, but its detached delete step may still
					// need the root to remove its marker. Let it finish first, so
					// the fault below hits only this CLI's cleanup (#1262).
					waitExecDeleteSteps(t, root)
					// Recreate owned residue, then make its parent unwritable:
					// real rm fails even after chmod of storage.
					writeExecFile(t, filepath.Join(params.Storage, "residue"), "test")
					if err := os.Chmod(root, 0500); err != nil {
						t.Fatal(err)
					}
				}
				return response, err
			})
			var out, errOut bytes.Buffer
			err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", fmt.Sprintf("exit %d", exit)}, &out, &errOut)
			want, verdict := 0, "succeeded\n"
			if exit != 0 {
				want, verdict = 10, "failed\n"
			}
			text := errOut.String()
			resultAt, warningAt := strings.Index(text, verdict), strings.Index(text, "warning: snapshot cleanup failed")
			if !injected || (err != nil && commandExitCode(err) != want) || (err == nil && want != 0) || resultAt < 0 || warningAt < resultAt {
				t.Fatalf("cleanup replaced result: injected=%v err=%v stdout=%q stderr=%q", injected, err, out.String(), text)
			}
		})
	}
}

func TestExecRemovesLockedFolders(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	root := os.Getenv("WEFTY_EXEC_STORAGE_ROOT")
	// Read-only, write-only and no-permission folders, in the work tree and
	// in the run's TMPDIR, like a test suite that checks delete protection.
	// A hard link shares its mode with a file outside the run, like a package
	// store does; unlocking the trash must not make that file executable.
	shared := filepath.Join(t.TempDir(), "shared")
	if err := os.WriteFile(shared, nil, 0644); err != nil {
		t.Fatal(err)
	}
	// WriteFile applies the umask; the check below needs the exact mode.
	if err := os.Chmod(shared, 0644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if entry != nil && entry.IsDir() {
				_ = os.Chmod(path, 0700)
			}
			return nil
		})
	})
	var errOut bytes.Buffer
	lock := `for base in frozen "$TMPDIR/frozen"; do
  mkdir -p "$base/sealed/inner" "$base/closed" && touch "$base/file" "$base/sealed/inner/file" "$base/closed/file" || exit
  ln "$1" "$base/closed/shared" || exit
  chmod 000 "$base/sealed/inner" && chmod 200 "$base/sealed" && chmod 000 "$base/closed" && chmod a-w "$base" || exit
done`
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", lock, "sh", shared}, io.Discard, &errOut)
	var params execParams
	_ = json.Unmarshal(stack.requests[0].Params, &params)
	if err != nil || strings.Contains(errOut.String(), "cleanup failed") {
		t.Fatalf("locked cleanup: %v %q", err, errOut.String())
	}
	if _, err := os.Stat(params.Storage); !os.IsNotExist(err) {
		t.Fatalf("storage remains: %v", err)
	}
	// Retirement renames the storage at once; only an empty root proves the
	// trash was deleted too.
	waitExecStorageEmpty(t, root)
	if info, err := os.Stat(shared); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0644 {
		t.Fatalf("cleanup changed a file outside the run: %v", info.Mode())
	}
}

func TestExecCancelFeedbackAndAcceptedSettlement(t *testing.T) {
	// The cancel request is healthy and keeps L1's settlement budget. Only
	// terminal confirmation is lost: its reads fail until the short read
	// budget ends, however slowly the real stack answers the cancel.
	shortenExecReadRecovery(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stack.afterAccept = cancel
	var errOut bytes.Buffer
	base := stack.clients.l3.client.Transport
	accepted := false
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			id := strings.Split(r.URL.Path, "/")[3]
			if !strings.Contains(errOut.String(), "wefty: cancelling run "+id+"\n") {
				t.Error("cancel request arrived before immediate signal feedback")
			}
			response, err := base.RoundTrip(r)
			if err != nil {
				return response, err
			}
			_ = response.Body.Close()
			raw, _ := json.Marshal(contract.RunRecord{RunID: id, Status: contract.RunRunning, CancelStatus: "accepted"})
			response.Body = io.NopCloser(bytes.NewReader(raw))
			accepted = true
			return response, nil
		}
		if accepted && r.Method == http.MethodGet {
			return execReadFailure("transport")
		}
		return base.RoundTrip(r)
	})
	holdExecNode(t, stack)
	err := execute(ctx, stack.clients, false, []string{"exec", "--", "true"}, io.Discard, &errOut)
	var params execParams
	_ = json.Unmarshal(stack.requests[0].Params, &params)
	t.Cleanup(func() { _ = cleanupExecStorage(params.Storage) })
	if !accepted || err == nil || !strings.Contains(err.Error(), "cancelled run") || !errors.Is(err, context.Canceled) || !strings.Contains(errOut.String(), "snapshot retained at "+params.Storage) {
		t.Fatalf("accepted cancellation replaced by observation error: %v %q", err, errOut.String())
	}
	if _, err := os.Stat(params.Storage); err != nil {
		t.Fatalf("unconfirmed-terminal capture removed: %v", err)
	}
}

func TestExecCancelWaitUsesL1Budget(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stack.afterAccept = cancel
	base := stack.clients.l3.client.Transport
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			deadline, ok := r.Context().Deadline()
			if !ok || time.Until(deadline) < 29*time.Second || time.Until(deadline) > 30*time.Second {
				t.Errorf("cancel settlement does not allow L1's 30 seconds: %v", time.Until(deadline))
			}
		}
		return base.RoundTrip(r)
	})
	holdExecNode(t, stack)
	if err := execute(ctx, stack.clients, false, []string{"exec", "--", "true"}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "cancelled run") {
		t.Fatalf("cancel: %v", err)
	}
}

func TestExecStartupFailuresAreInfrastructure(t *testing.T) {
	for _, step := range []string{"clone", "checkout", "reset", "cd"} {
		t.Run(step, func(t *testing.T) {
			shortenExecTimers(t)
			repo := execTestRepo(t)
			t.Chdir(filepath.Join(repo, "sub"))
			git, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			shim := t.TempDir()
			script := "#!/bin/sh\nclone=false; checkout=false; reset=false; dir=''\nprev=''\nfor arg do\n [ \"$arg\" = --no-checkout ] && clone=true\n [ \"$arg\" = checkout ] && checkout=true\n [ \"$arg\" = reset ] && reset=true\n [ \"$prev\" = -C ] && dir=$arg\n prev=$arg\ndone\n"
			switch step {
			case "clone":
				script += "$clone && exit 1\n"
			case "checkout":
				script += "$checkout && exit 1\n"
			case "reset":
				script += "$reset && exit 1\n"
			case "cd":
				script += fmt.Sprintf("if $checkout; then '%s' \"$@\" || exit; /bin/rm -rf \"$dir/sub\"; exit; fi\n", git)
			}
			script += fmt.Sprintf("exec '%s' \"$@\"\n", git)
			path := filepath.Join(shim, "git")
			writeExecFile(t, path, script)
			if err := os.Chmod(path, 0755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
			stack := newExecStack(t, execTestIdleLimit)
			var out, errOut bytes.Buffer
			err = execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "printf should-not-run"}, &out, &errOut)
			if commandExitCode(err) != exitUnavailable || !strings.Contains(errOut.String(), "wefty: snapshot checkout failed") || out.Len() != 0 {
				t.Errorf("startup looked like command failure: %v stdout=%q stderr=%q", err, out.String(), errOut.String())
			}
			runs, e := stack.clients.listRuns(t.Context(), "", 20)
			if e != nil || len(runs.Runs) != 1 {
				t.Fatalf("runs: %+v %v", runs, e)
			}
			resultResponse, e := stack.clients.getRunResult(t.Context(), runs.Runs[0].RunID)
			if e != nil {
				t.Fatalf("startup result missing: %v", e)
			}
			document := string(resultResponse.Document)
			var result map[string]any
			if err := json.Unmarshal([]byte(document), &result); err != nil {
				t.Fatal(err)
			}
			if _, ok := result["exit_code"]; ok {
				t.Errorf("startup wrote command exit_code: %s", document)
			}
			if result["infrastructure_error"] != "snapshot checkout failed" {
				t.Errorf("startup result lacks infrastructure cause: %s", document)
			}
		})
	}
}

type execReviewOutput struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	token  string
	seen   chan struct{}
	once   sync.Once
}

func (w *execReviewOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.buffer.Write(p)
	matched := strings.Contains(w.buffer.String(), w.token)
	w.mu.Unlock()
	if matched {
		w.once.Do(func() { close(w.seen) })
	}
	return n, err
}
func (w *execReviewOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

// Bash can catch and lose a signal that reaches a new subshell while it still
// carries the wrapper's traps (seen with Linux Bash 5.2). A wrapper that stops
// its heartbeat with a signal then waits out a whole heartbeat period after
// the command. Concurrent runs with the production period make that likely.
func TestExecWrapperEndsWellInsideAHeartbeatPeriod(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	execTestStorageRoot(t)
	old := execHeartbeatSeconds
	execHeartbeatSeconds = "60"
	t.Cleanup(func() { execHeartbeatSeconds = old })
	repo := execTestRepo(t)
	const runs, workers = 32, 4
	wrappers := make(chan string, runs)
	for range runs {
		capture, err := captureExec(t.Context(), repo)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
		wrappers <- buildExecWrapper(capture, []string{"/bin/sh", "-c", "exit 7"})
	}
	close(wrappers)
	var group sync.WaitGroup
	failures := make(chan string, runs)
	for range workers {
		group.Go(func() {
			for wrapper := range wrappers {
				if failure := runExecWrapperPromptly(t, wrapper, 7); failure != "" {
					failures <- failure
				}
			}
		})
	}
	group.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

// The command can make its capture storage unwritable, so the finished
// marker cannot be written. The liveness loop must still stop, and cleanup
// must still remove the storage.
func TestExecWrapperEndsWhenTheCommandLocksStorage(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the directory mode this test relies on")
	}
	t.Setenv("HOME", t.TempDir())
	execTestStorageRoot(t)
	old := execHeartbeatSeconds
	execHeartbeatSeconds = "60"
	t.Cleanup(func() { execHeartbeatSeconds = old })
	capture, err := captureExec(t.Context(), execTestRepo(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
	// The command runs in <storage>/work, so ".." is the capture storage.
	if failure := runExecWrapperPromptly(t, buildExecWrapper(capture, []string{"/bin/sh", "-c", "chmod 500 .. && exit 3"}), 3); failure != "" {
		t.Fatal(failure)
	}
	if _, err := os.Lstat(capture.Storage); !os.IsNotExist(err) {
		t.Fatalf("storage after a locked run: %v", err)
	}
}

// runExecWrapperPromptly runs a built wrapper and reports a failure unless it
// exits with want well inside the production heartbeat period. The bound
// covers the clone, the command and cleanup; it catches a wrapper that waits
// out a 60 s period, not the exact tick.
func runExecWrapperPromptly(t *testing.T, wrapper string, want int) string {
	ctx, stop := context.WithTimeout(t.Context(), 45*time.Second)
	defer stop()
	// A timeout stops the whole wrapper group, so a surviving helper cannot
	// hold the output open and hang the test.
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", wrapper)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	started := time.Now()
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if elapsed := time.Since(started); !errors.As(err, &exit) || exit.ExitCode() != want || elapsed > 20*time.Second {
		return fmt.Sprintf("took %s: %v %q", elapsed.Round(time.Millisecond), err, output)
	}
	return ""
}

func TestExecHeartbeatStopsAtCommandEnd(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	execTestStorageRoot(t)
	shortenExecTimers(t)
	execHeartbeatSeconds = "0.02"
	repo := execTestRepo(t)
	capture, err := captureExec(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
	handoff := t.TempDir()
	fifo := filepath.Join(handoff, "result.json")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	// DEBUG runs immediately before result publication, after the command has
	// exited. Its marker separates permitted liveness output from late output.
	probe := filepath.Join(t.TempDir(), "result-probe.sh")
	writeExecFile(t, probe, "trap 'case \"$BASH_COMMAND\" in *exit_code*result.json*) printf \"result-write-start\\n\" ;; esac' DEBUG\n")
	output := &execReviewOutput{token: "result-write-start\n", seen: make(chan struct{})}
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", buildExecWrapper(capture, []string{"/bin/sh", "-c", "sleep 0.15; printf 'command-last\\n'"}))
	cmd.Env = append(os.Environ(), "WEFTY_HANDOFF_DIR="+handoff, "BASH_ENV="+probe)
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-output.seen:
	case <-ctx.Done():
		t.Fatal("wrapper never reached result publication")
	}
	// Hold result publication open after the child exits. A timer tied to
	// wrapper exit will still print here; a timer tied to the command will stop.
	if err := waitForContext(ctx, 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	observed := output.String()
	reader, err := os.OpenFile(fifo, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wrapper: %v %q", err, output.String())
	}
	boundary := strings.Index(observed, output.token) + len(output.token)
	if !strings.Contains(observed[:boundary], "command-last\n") || !strings.Contains(observed[:boundary], "still running") || strings.Contains(output.String()[boundary:], "still running") {
		t.Fatalf("heartbeat continued after command exited: %q", output.String())
	}
}

// Every exec stop path prints feedback. A second Ctrl-C that follows any of
// it must meet default signal handling, so exec observes cancellation only
// after the reset has finished, however slow the reset is.
func TestExecSignalContextCancelsOnlyAfterReset(t *testing.T) {
	base, cancelBase := context.WithCancel(t.Context())
	var reset atomic.Bool
	parent := context.WithValue(base, execSignalResetKey{}, context.CancelFunc(func() {
		time.Sleep(50 * time.Millisecond)
		reset.Store(true)
	}))
	ctx, stop := execSignalContext(parent)
	defer stop()
	cancelBase()
	select {
	case <-ctx.Done():
	case <-time.After(testwait.Bound):
		t.Fatal("exec context ignored the parent's cancellation")
	}
	if !reset.Load() {
		t.Fatal("exec observed cancellation before the signal reset finished")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("exec context error = %v, want canceled", ctx.Err())
	}
}

func TestExecSecondInterruptExitsDuringCancellation(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execRealBinaryIdleLimit)
	t.Chdir(execTestRepo(t))
	cancelling := make(chan struct{})
	ledger := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			close(cancelling)
			<-r.Context().Done()
			return
		}
		request, err := http.NewRequestWithContext(r.Context(), r.Method, "http://wefty.invalid"+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		request.Header = r.Header.Clone()
		response, err := stack.clients.l3.client.Do(request)
		if err != nil {
			if r.Context().Err() == nil {
				t.Error(err)
			}
			return
		}
		defer response.Body.Close()
		for key, values := range response.Header {
			w.Header()[key] = values
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	})
	output := &execReviewOutput{token: "wefty: run ", seen: make(chan struct{})}
	holdExecNode(t, stack)
	cmd := exec.Command(binary, "--l1="+stack.l1Address, "--l3="+ledger, "--node-config="+stack.clients.nodeConfigPath, "exec", "--", "true")
	cmd.Env = os.Environ()
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	select {
	case <-output.seen:
	case <-time.After(testwait.Bound):
		t.Fatal("binary did not submit run")
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelling:
	case <-time.After(testwait.Bound):
		t.Fatal("first signal did not request cancellation")
	}
	// The server can see the cancel before the test's reader has copied the
	// CLI's feedback line, so wait for the line itself.
	feedbackDeadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(output.String(), "wefty: cancelling run ") && time.Now().Before(feedbackDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	feedback := output.String()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatal("second Ctrl-C did not exit immediately")
	}
	if !strings.Contains(feedback, "wefty: cancelling run ") {
		t.Fatalf("first Ctrl-C lacked immediate feedback: %q", feedback)
	}
	var params execParams
	_ = json.Unmarshal(stack.requests[0].Params, &params)
	if _, err := os.Stat(params.Storage); err != nil {
		t.Fatalf("second signal removed unconfirmed capture: %v", err)
	}
	t.Cleanup(func() { _ = cleanupExecStorage(params.Storage) })
}

func TestExecUnconfirmedSubmissionPrintsStorage(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	base := stack.clients.l3.client.Transport
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/runs" {
			return nil, fmt.Errorf("lost reply")
		}
		return base.RoundTrip(r)
	})
	var errOut bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, &errOut)
	entries, e := os.ReadDir(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"))
	if e != nil || len(entries) != 1 {
		t.Fatalf("retained capture: %v %v", entries, e)
	}
	storage := filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), entries[0].Name())
	t.Cleanup(func() { _ = cleanupExecStorage(storage) })
	if commandExitCode(err) != exitUnavailable || strings.Count(errOut.String(), "submission reply lost") != 1 || !strings.Contains(errOut.String(), "snapshot retained at "+storage) {
		t.Fatalf("uncertain submission lacked recovery/path feedback: %v %q", err, errOut.String())
	}
}

func TestExecCaptureUsesUserDataFolder(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", "")
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Chdir(execTestRepo(t))
	expected := filepath.Join(state, "wefty", "exec")
	stack.afterAccept = func() {
		var params execParams
		_ = json.Unmarshal(stack.requests[0].Params, &params)
		if filepath.Dir(params.Storage) != expected {
			t.Errorf("capture uses %q, want user data root %q", params.Storage, expected)
		}
		if info, err := os.Stat(params.Storage); err != nil || info.Mode().Perm() != 0700 {
			t.Errorf("private capture mode: %v %v", info, err)
		}
	}
	if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestExecCLIRemovesReadOnlyResidue(t *testing.T) {
	shortenExecTimers(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	base := stack.clients.l3.client.Transport
	injected := false
	var params execParams
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if err != nil || injected || r.Method != http.MethodGet || strings.Count(r.URL.Path, "/") != 3 {
			return response, err
		}
		raw, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(raw))
		var record contract.RunRecord
		if err == nil && json.Unmarshal(raw, &record) == nil && runIsTerminal(record.Status) {
			injected = true
			_ = json.Unmarshal(stack.requests[0].Params, &params)
			writeExecFile(t, filepath.Join(params.Storage, "frozen", "file"), "read-only residue")
			if err := os.Chmod(filepath.Join(params.Storage, "frozen"), 0500); err != nil {
				t.Fatal(err)
			}
		}
		return response, err
	})
	t.Cleanup(func() {
		if params.Storage != "" {
			_ = os.Chmod(filepath.Join(params.Storage, "frozen"), 0700)
			_ = cleanupExecStorage(params.Storage)
		}
	})
	var errOut bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, &errOut)
	if !injected || err != nil || strings.Contains(errOut.String(), "cleanup failed") {
		t.Fatalf("CLI could not remove read-only storage: injected=%v err=%v stderr=%q", injected, err, errOut.String())
	}
	if _, err := os.Stat(params.Storage); !os.IsNotExist(err) {
		t.Fatalf("CLI left read-only residue: %v", err)
	}
}

func TestExecCancelRemovesPrivateTemp(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	marker := filepath.Join(t.TempDir(), "temp-ready")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	settled := false
	defer func() {
		cancel()
		if !settled {
			select {
			case <-done:
			case <-time.After(35 * time.Second):
				t.Error("cancelled temp probe did not stop")
			}
		}
	}()
	go func() {
		done <- execute(ctx, stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", `set -e; printf data > "$TMPDIR/run-file"; printf '%s' "$TMPDIR" > "$1.pending"; mv "$1.pending" "$1"; sleep 60`, "sh", marker}, io.Discard, io.Discard)
	}()
	waitExecPath(t, marker, true)
	raw, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	link := string(raw)
	if !strings.HasPrefix(link, "/tmp/wefty-run.") {
		t.Fatalf("run has no private TMPDIR: %q", link)
	}
	target, err := os.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(target)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("temp folder mode: %v %v", info, err)
	}
	info, err = os.Stat(filepath.Join(filepath.Dir(target), "tmp-link"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("temp link record mode: %v %v", info, err)
	}
	waitExecPath(t, filepath.Join(target, "run-file"), true)
	cancel()
	select {
	case err := <-done:
		settled = true
		if err == nil || !strings.Contains(err.Error(), "cancelled run") {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cancellation never settled")
	}
	page, err := stack.clients.listRuns(t.Context(), "", 10)
	if err != nil || len(page.Runs) != 1 || page.Runs[0].Status != contract.RunFailed {
		t.Fatalf("terminal cancellation: %+v %v", page, err)
	}
	for _, path := range []string{link, target, filepath.Join(target, "run-file")} {
		waitExecPath(t, path, false)
	}
	waitExecStorageEmpty(t, os.Getenv("WEFTY_EXEC_STORAGE_ROOT"))
}
