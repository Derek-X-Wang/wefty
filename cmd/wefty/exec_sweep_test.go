package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"github.com/Derek-X-Wang/wefty/l3"
)

func ageExecCapture(t *testing.T, storage string) {
	t.Helper()
	stamp := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(storage, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}

func TestExecSweepMarkerlessAcceptedCapture(t *testing.T) {
	for _, state := range []contract.RunState{contract.RunQueued, contract.RunRunning} {
		t.Run(string(state), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, 10*time.Second)
			capture, err := captureExec(t.Context(), execTestRepo(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
			argv := []string{"/bin/sh", "-c", "while :; do sleep 0.05; done"}
			params, err := json.Marshal(execParams{execCapture: capture, Argv: argv})
			if err != nil {
				t.Fatal(err)
			}
			script := buildExecWrapper(capture, argv)
			digest := sha256.Sum256([]byte(script))
			tags := []string{contract.StableNodeTagPrefix + "node-cli"}
			if state == contract.RunQueued {
				tags = append(tags, "no-node")
			}
			// Submit through real L3 without executing the CLI's marker write.
			accepted, err := stack.clients.submitRun(t.Context(), l3.CreateRunRequest{
				Params: params, Tags: tags,
				InlineScript: &l3.InlineScriptInput{Content: script, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/bash"}},
			}, "markerless-"+string(state))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = execute(context.Background(), stack.clients, false, []string{"cancel", accepted.RunID}, io.Discard, io.Discard)
			})
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			for {
				record, err := stack.clients.getRun(ctx, accepted.RunID)
				if err != nil {
					t.Fatal(err)
				}
				_, workErr := os.Stat(filepath.Join(capture.Storage, "work", "sub", "tracked"))
				if record.Status == state && (state == contract.RunQueued || workErr == nil) {
					break
				}
				if err := waitForContext(ctx, 10*time.Millisecond); err != nil {
					t.Fatalf("run never reached %s: %v", state, err)
				}
			}
			// Also exercise lookup if the wrapper marker is missing on a live run.
			if err := os.Remove(filepath.Join(capture.Storage, "run-id")); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			ageExecCapture(t, capture.Storage)
			var stderr bytes.Buffer
			sweepExecStorage(t.Context(), stack.clients, &stderr)
			if _, err := os.Stat(filepath.Join(capture.Storage, "snapshot.git")); err != nil {
				t.Errorf("sweep deleted %s run's snapshot: %v; %s", state, err, stderr.String())
			}
			if state == contract.RunRunning {
				if _, err := os.Stat(filepath.Join(capture.Storage, "work", "sub", "tracked")); err != nil {
					t.Errorf("sweep deleted running work tree: %v", err)
				}
			}
		})
	}
}

func TestExecSweepMarkerlessLookup(t *testing.T) {
	for _, kind := range []string{"unaccepted", "list-failure", "list-timeout", "record-failure", "record-timeout"} {
		t.Run(kind, func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			storage := filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), "wefty-exec-"+kind)
			writeExecFile(t, filepath.Join(storage, "snapshot.git", "object"), "retained")
			ageExecCapture(t, storage)
			base := stack.clients.l3.client.Transport
			probed := false
			stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/v1/runs" {
					if strings.HasPrefix(kind, "record-") {
						return jsonResponse(http.StatusOK, l3.RunListPage{Runs: []l3.RunSummary{{RunID: "run-unverifiable"}}}), nil
					}
					probed = true
					if kind == "list-failure" {
						return nil, fmt.Errorf("lookup unavailable")
					}
					if kind == "list-timeout" {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
				}
				if r.URL.Path == "/v1/runs/run-unverifiable" {
					probed = true
					if kind == "record-timeout" {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return nil, fmt.Errorf("run read unavailable")
				}
				return base.RoundTrip(r)
			})
			var stderr bytes.Buffer
			started := time.Now()
			sweepExecStorage(t.Context(), stack.clients, &stderr)
			if !probed {
				t.Error("sweep did not verify markerless capture with L3")
			}
			_, err := os.Stat(storage)
			if kind == "unaccepted" {
				if !os.IsNotExist(err) {
					t.Errorf("unaccepted capture older than one hour survived: %v", err)
				}
			} else if err != nil || !strings.Contains(stderr.String(), "warning") {
				t.Errorf("unverifiable capture was removed or not reported: %v %q", err, stderr.String())
			}
			if time.Since(started) > execSweepBudget+time.Second {
				t.Errorf("lookup exceeded sweep budget: %v", time.Since(started))
			}
		})
	}
}

func TestExecWrapperRecordsRunBeforeClone(t *testing.T) {
	execTestStorageRoot(t)
	capture, err := captureExec(t.Context(), execTestRepo(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	cloneStarted := filepath.Join(shim, "clone-started")
	release := filepath.Join(shim, "release")
	shimGit := filepath.Join(shim, "git")
	writeExecFile(t, shimGit, fmt.Sprintf("#!/bin/sh\nprintf started > '%s'\nwhile [ ! -f '%s' ]; do sleep 0.01; done\nexec '%s' \"$@\"\n", cloneStarted, release, git))
	if err := os.Chmod(shimGit, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", buildExecWrapper(capture, []string{"true"}))
	cmd.Env = append(os.Environ(), "PATH="+shim+string(os.PathListSeparator)+os.Getenv("PATH"), "WEFTY_RUN_ID=run-wrapper-marker")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(cloneStarted); err == nil {
			break
		}
		if err := waitForContext(ctx, 10*time.Millisecond); err != nil {
			t.Fatal("wrapper never started clone")
		}
	}
	marker, err := os.ReadFile(filepath.Join(capture.Storage, "run-id"))
	if err != nil || string(marker) != "run-wrapper-marker\n" {
		t.Errorf("wrapper did not record run before clone: %q %v", marker, err)
	}
	writeExecFile(t, release, "go")
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wrapper: %v %q", err, output.String())
	}
}

func TestExecWrapperCleanupAlreadyRemoved(t *testing.T) {
	execTestStorageRoot(t)
	capture, err := captureExec(t.Context(), execTestRepo(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	// A competing sweep has already removed the folder when wrapper cleanup starts.
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", buildExecWrapper(capture, []string{"/bin/sh", "-c", `printf '%s' "$TMPDIR"; /bin/rm -rf -- "$1"`, "sh", capture.Storage}))
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	// Removing storage outside retirement leaves the run's /tmp link behind.
	if link := stdout.String(); strings.HasPrefix(link, "/tmp/wefty-run.") {
		t.Cleanup(func() { _ = os.Remove(link) })
	}
	if err != nil || strings.Contains(stderr.String(), "cleanup failed") {
		t.Fatalf("already removed folder was treated as cleanup failure: %v %q", err, stderr.String())
	}
	if _, err := os.Stat(capture.Storage); !os.IsNotExist(err) {
		t.Fatalf("capture still exists: %v", err)
	}
}
