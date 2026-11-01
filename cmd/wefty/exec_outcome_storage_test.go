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
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestExecExternalCancellationIsTerminal(t *testing.T) {
	// Every read here is healthy, and one carries a whole cancel command, so
	// the reads keep the production budget.
	shortenExecTimers(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	base := stack.clients.l3.client.Transport
	cancelled := false
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/logs") && !cancelled {
			cancelled = true
			id := strings.Split(r.URL.Path, "/")[3]
			if e := execute(t.Context(), stack.clients, false, []string{"cancel", id}, io.Discard, io.Discard); e != nil {
				t.Errorf("external cancel: %v", e)
			}
		}
		return response, err
	})
	var stderr bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "sleep 60"}, io.Discard, &stderr)
	if !cancelled || commandExitCode(err) != 10 || !strings.Contains(stderr.String(), "cancelled") {
		t.Fatalf("external cancellation: %v %q", err, stderr.String())
	}
}

func TestExecSweepsAbandonedCaptures(t *testing.T) {
	// Only the first exec loses observation; its log reads always fail.
	restoreReadRecovery := shortenExecReadRecovery(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	base := stack.clients.l3.client.Transport
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			return execReadFailure("transport")
		}
		return base.RoundTrip(r)
	})
	// Leave a live run for the sweep while allowing later commands to start.
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "sleep 600"}, io.Discard, io.Discard)
	if commandExitCode(err) != 13 {
		t.Fatalf("lost CLI: %v", err)
	}
	stack.clients.l3.client.Transport = base
	// The later execs are healthy, so they keep the production budget.
	restoreReadRecovery()
	runs, err := stack.clients.listRuns(t.Context(), "", 20)
	if err != nil || len(runs.Runs) != 1 {
		t.Fatalf("runs: %+v %v", runs, err)
	}
	id := runs.Runs[0].RunID
	var params execParams
	_ = json.Unmarshal(stack.requests[0].Params, &params)
	marker, err := os.ReadFile(filepath.Join(params.Storage, "run-id"))
	if err != nil || strings.TrimSpace(string(marker)) != id {
		t.Errorf("accepted run not recorded in capture: %q %v", marker, err)
	}
	// A live capture must survive even when it is older than the orphan TTL.
	stamp := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(params.Storage, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	root := os.Getenv("WEFTY_EXEC_STORAGE_ROOT")
	orphan := func(name string, age time.Duration) string {
		path := filepath.Join(root, name)
		writeExecFile(t, filepath.Join(path, "snapshot.git", "objects", "retained"), "bare clone")
		stamp := time.Now().Add(-age)
		if e := os.Chtimes(path, stamp, stamp); e != nil {
			t.Fatal(e)
		}
		return path
	}
	old := orphan("wefty-exec-never-accepted", 2*time.Hour)
	young := orphan("wefty-exec-capturing", 30*time.Minute)
	unrelated := orphan("owner-files", 2*time.Hour)
	unknown := orphan("wefty-exec-unverifiable", 2*time.Hour)
	writeExecFile(t, filepath.Join(unknown, "run-id"), "run_unknown")
	outside := t.TempDir()
	writeExecFile(t, filepath.Join(outside, "retained"), "owner")
	if e := os.Symlink(outside, filepath.Join(root, "wefty-exec-link")); e != nil {
		t.Fatal(e)
	}
	var stderr bytes.Buffer
	if e := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, &stderr); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{params.Storage, young, unrelated, unknown, outside} {
		if _, e := os.Stat(path); e != nil {
			t.Errorf("sweep removed protected folder %s: %v", path, e)
		}
	}
	if _, e := os.Stat(old); !os.IsNotExist(e) {
		t.Errorf("never-accepted capture leaked: %v", e)
	}
	if !strings.Contains(stderr.String(), "warning") {
		t.Errorf("unverifiable sweep did not warn: %q", stderr.String())
	}
	// Re-create residue from the succeeded run, as if cleanup was interrupted.
	terminalRuns, e := stack.clients.listRuns(t.Context(), "", 20)
	if e != nil {
		t.Fatal(e)
	}
	var terminalID string
	for _, run := range terminalRuns.Runs {
		if run.Status == contract.RunSucceeded {
			terminalID = run.RunID
		}
	}
	if terminalID == "" {
		t.Fatal("no succeeded run")
	}
	residue := orphan("wefty-exec-killed-cleanup", 0)
	writeExecFile(t, filepath.Join(residue, "run-id"), terminalID)
	if e := execute(t.Context(), stack.clients, false, []string{"cancel", id}, io.Discard, io.Discard); e != nil {
		t.Fatal(e)
	}
	// Cancel returns before the run ends, and a sweep keeps a live run's
	// capture. Sweep only once the cancelled run is terminal (#1220).
	waitExecRunTerminal(t, stack.clients, id)
	if e := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, io.Discard); e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{params.Storage, residue} {
		if _, e := os.Stat(path); !os.IsNotExist(e) {
			t.Errorf("terminal capture leaked at %s: %v", path, e)
		}
	}
}

func waitExecRunTerminal(t *testing.T, clients *apiClients, id string) {
	t.Helper()
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	for {
		runs, err := clients.listRuns(ctx, "", 20)
		if err == nil {
			for _, run := range runs.Runs {
				if run.RunID == id && (run.Status == contract.RunSucceeded || run.Status == contract.RunFailed) {
					return
				}
			}
		}
		if err := waitForContext(ctx, 20*time.Millisecond); err != nil {
			t.Fatalf("run %s did not end: %v", id, err)
		}
	}
}

// waitExecRunResult reads a Run's result once it has arrived. On the normal
// path the node completes the Attempt before it uploads the result, so a
// terminal Run can briefly answer not_found (docs/contracts/exec.md: a
// succeeded run does not wait for its result). Any other read error fails at
// once.
func waitExecRunResult(t *testing.T, clients *apiClients, id string) l3.RunResult {
	t.Helper()
	ctx, stop := context.WithTimeout(t.Context(), 40*time.Second)
	defer stop()
	for {
		record, err := clients.getRunResult(ctx, id)
		var responseErr *apiResponseError
		if err == nil || !errors.As(err, &responseErr) || responseErr.APIError.Code != contract.ErrorNotFound {
			if err != nil {
				t.Fatal(err)
			}
			return record
		}
		if err := waitForContext(ctx, 20*time.Millisecond); err != nil {
			t.Fatalf("run %s result did not arrive: %v", id, err)
		}
	}
}

func TestExecSweepCountIsBounded(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	root := os.Getenv("WEFTY_EXEC_STORAGE_ROOT")
	for i := 0; i < 100; i++ {
		path := filepath.Join(root, fmt.Sprintf("wefty-exec-old-%03d", i))
		writeExecFile(t, filepath.Join(path, "snapshot.git", "object"), "old")
		stamp := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) < 36 || len(entries) >= 100 {
		t.Fatalf("sweep count not bounded to 64: remaining=%d err=%v", len(entries), err)
	}
}

func TestExecSweepTimeAndFailureAreBounded(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	path := filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), "wefty-exec-stalled")
	writeExecFile(t, filepath.Join(path, "run-id"), "run_stalled")
	base := stack.clients.l3.client.Transport
	probed := false
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/v1/runs/run_stalled" {
			probed = true
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return base.RoundTrip(r)
	})
	started := time.Now()
	var stderr bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, &stderr)
	if err != nil || !probed || !strings.Contains(stderr.String(), "warning") || time.Since(started) > 5*time.Second {
		t.Fatalf("sweep stall: err=%v probed=%v elapsed=%v stderr=%q", err, probed, time.Since(started), stderr.String())
	}
	if _, e := os.Stat(path); e != nil {
		t.Fatalf("unverified capture removed: %v", e)
	}
}

func TestExecStorageRootStateAndPermissions(t *testing.T) {
	for _, state := range []bool{true, false} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			t.Setenv("WEFTY_EXEC_STORAGE_ROOT", "")
			t.Setenv("XDG_STATE_HOME", "")
			root := filepath.Join(os.Getenv("HOME"), ".local", "state", "wefty", "exec")
			if state {
				dir := t.TempDir()
				t.Setenv("XDG_STATE_HOME", dir)
				root = filepath.Join(dir, "wefty", "exec")
			}
			if err := os.MkdirAll(root, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(root, 0755); err != nil {
				t.Fatal(err)
			}
			t.Chdir(execTestRepo(t))
			stack.afterAccept = func() {
				var params execParams
				_ = json.Unmarshal(stack.requests[0].Params, &params)
				info, err := os.Stat(root)
				if filepath.Dir(params.Storage) != root || err != nil || info.Mode().Perm() != 0700 {
					t.Errorf("state root/mode: storage=%s root=%s info=%v err=%v", params.Storage, root, info, err)
				}
			}
			if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExecSignalAfterTerminalPreservesVerdict(t *testing.T) {
	for _, exit := range []int{0, 7} {
		t.Run(fmt.Sprint(exit), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			t.Chdir(execTestRepo(t))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			base := stack.clients.l3.client.Transport
			cancels := 0
			signalled := false
			stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/cancel") {
					cancels++
				}
				response, err := base.RoundTrip(r)
				// Keep one event pending so cancellation also exercises terminal log drain.
				if err == nil && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/logs") {
					raw, e := io.ReadAll(response.Body)
					_ = response.Body.Close()
					if e != nil {
						return nil, e
					}
					var page l1.LogPage
					if e = json.Unmarshal(raw, &page); e != nil {
						return nil, e
					}
					page.Events = append(page.Events, contract.LogEvent{Stream: contract.LogStdout})
					raw, _ = json.Marshal(page)
					response.Body = io.NopCloser(bytes.NewReader(raw))
				}
				if err == nil && r.Method == http.MethodGet && strings.Count(r.URL.Path, "/") == 3 {
					raw, e := io.ReadAll(response.Body)
					_ = response.Body.Close()
					if e != nil {
						return nil, e
					}
					response.Body = io.NopCloser(bytes.NewReader(raw))
					var record contract.RunRecord
					if json.Unmarshal(raw, &record) == nil && runIsTerminal(record.Status) {
						signalled = true
						cancel()
					}
				}
				return response, err
			})
			var stderr bytes.Buffer
			err := execute(ctx, stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", fmt.Sprintf("printf 'tail\\n'; exit %d", exit)}, io.Discard, &stderr)
			want, verdict := 0, "succeeded"
			if exit != 0 {
				want, verdict = 10, "failed"
			}
			if !signalled || (err != nil && commandExitCode(err) != want) || (err == nil && want != 0) || cancels != 0 || !strings.Contains(stderr.String(), verdict) {
				t.Fatalf("late signal changed verdict: %v cancels=%d signalled=%v stderr=%q", err, cancels, signalled, stderr.String())
			}
		})
	}
}

func TestExecStartFolderUsesSnapshotCase(t *testing.T) {
	_ = newExecStack(t, execTestIdleLimit)
	repo := execTestRepo(t)
	if _, err := os.Stat(filepath.Join(repo, "Sub", "tracked")); err != nil {
		t.Skip("filesystem is case-sensitive")
	}
	execTestGit(t, repo, "config", "core.ignorecase", "true")
	capture, err := captureExec(t.Context(), filepath.Join(repo, "Sub"))
	if err != nil {
		t.Fatalf("case-insensitive folder refused: %v", err)
	}
	t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
	if capture.Folder != "sub" {
		t.Fatalf("folder case not resolved: %q", capture.Folder)
	}
	// Liveness lines go to stderr; the command's own output is stdout.
	command := exec.Command("/bin/bash", "-c", buildExecWrapper(capture, []string{"cat", "tracked"}))
	out, err := command.Output()
	if err != nil || string(out) != "base" {
		t.Fatalf("resolved wrapper: %v %q", err, out)
	}
}

func TestExecSlowResultUploadPreservesFailure(t *testing.T) {
	shortenExecTimers(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	base := stack.clients.l3.client.Transport
	resultStart := time.Time{}
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/result") {
			if resultStart.IsZero() {
				resultStart = time.Now()
			}
			if time.Since(resultStart) < statusProbeBudget+300*time.Millisecond {
				return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"not_found","message":"upload pending","retryable":false}}`))}, nil
			}
		}
		return base.RoundTrip(r)
	})
	var stderr bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "exit 7"}, io.Discard, &stderr)
	if commandExitCode(err) != 10 || !strings.Contains(stderr.String(), "failed") {
		t.Fatalf("slow upload lost failure: %v %q", err, stderr.String())
	}
}

func TestExecInfrastructureExit13FromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	path := filepath.Join(shim, "git")
	writeExecFile(t, path, fmt.Sprintf("#!/bin/sh\nfor arg do [ \"$arg\" = --no-checkout ] && exit 1; done\nexec '%s' \"$@\"\n", git))
	if err := os.Chmod(path, 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	stack := newExecStack(t, execRealBinaryIdleLimit)
	t.Chdir(execTestRepo(t))
	code, out := runWefty(t, binary, 30*time.Second, "--l1="+stack.l1Address, "--l3="+stack.l3Address, "--node-config="+stack.clients.nodeConfigPath, "exec", "--", "true")
	if code != 13 || !strings.Contains(out, "infrastructure failure") {
		t.Fatalf("infrastructure process exit: %d %q", code, out)
	}
}
