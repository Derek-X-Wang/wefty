package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func injectExecStorageOS(t *testing.T, goos string) {
	t.Helper()
	previous := execStorageGOOS
	execStorageGOOS = goos
	t.Cleanup(func() { execStorageGOOS = previous })
}

func TestExecStorageFallbackInjectedOS(t *testing.T) {
	for _, goos := range []string{"darwin", "linux"} {
		t.Run(goos, func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			injectExecStorageOS(t, goos)
			t.Chdir(execTestRepo(t))
			t.Setenv("WEFTY_EXEC_STORAGE_ROOT", "")
			state := t.TempDir()
			t.Setenv("XDG_STATE_HOME", state)
			denyExecStorageParent(t, state)
			var stdout, stderr bytes.Buffer
			err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "cat", "sub/tracked"}, &stdout, &stderr)
			if goos == "linux" {
				want := strings.TrimSuffix(strings.TrimPrefix(wantExecStorageRefusal(filepath.Join(state, "wefty", "exec")), "wefty: "), "\n")
				if err == nil || err.Error() != want || commandExitCode(err) != 1 || stderr.Len() != 0 || len(stack.requests) != 0 {
					t.Fatalf("non-darwin refusal: %v stderr=%q requests=%d", err, stderr.String(), len(stack.requests))
				}
				if _, err := os.Stat(filepath.Join(os.TempDir(), "wefty-exec")); !os.IsNotExist(err) {
					t.Fatalf("Linux used temp: %v", err)
				}
				return
			}
			if err != nil || stdout.String() != "base" || len(stack.requests) != 1 {
				t.Fatalf("fallback run: %v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
			}
			var params execParams
			if err := json.Unmarshal(stack.requests[0].Params, &params); err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(os.TempDir(), "wefty-exec")
			info, err := os.Stat(root)
			if filepath.Dir(params.Storage) != root || err != nil || info.Mode().Perm() != 0700 {
				t.Fatalf("fallback capture/mode: storage=%s info=%v err=%v", params.Storage, info, err)
			}
		})
	}
}

func TestExecSweepFallbackRoots(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprint(denied), func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			injectExecStorageOS(t, "darwin")
			t.Setenv("WEFTY_EXEC_STORAGE_ROOT", "")
			state := t.TempDir()
			t.Setenv("XDG_STATE_HOME", state)
			if denied {
				denyExecStorageParent(t, state)
			}
			root := filepath.Join(os.TempDir(), "wefty-exec")
			trash := filepath.Join(root, ".wefty-trash-leftover")
			old := filepath.Join(root, "wefty-exec-old")
			young := filepath.Join(root, "wefty-exec-young")
			unverifiable := filepath.Join(root, "wefty-exec-unverifiable")
			outside := t.TempDir()
			for _, path := range []string{trash, old, young, unverifiable, outside} {
				writeExecFile(t, filepath.Join(path, "kept"), "data")
			}
			ageExecCapture(t, old)
			writeExecFile(t, filepath.Join(trash, "run-id"), "no-lookup-for-trash")
			writeExecFile(t, filepath.Join(unverifiable, "run-id"), "run-missing")
			if err := os.Symlink(outside, filepath.Join(root, ".wefty-trash-link")); err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			if err := sweepExecStorage(t.Context(), stack.clients, &stderr); err != nil {
				t.Fatal(err)
			}
			waitExecPath(t, trash, false)
			waitExecPath(t, old, false)
			for _, path := range []string{young, unverifiable, filepath.Join(outside, "kept"), filepath.Join(root, ".wefty-trash-link")} {
				waitExecPath(t, path, true)
			}
			if !strings.Contains(stderr.String(), "run-missing") || strings.Contains(stderr.String(), "no-lookup-for-trash") {
				t.Fatalf("fallback sweep verification: %q", stderr.String())
			}
		})
	}
}

func TestExecSweepFallbackTrashPrecedesLookup(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	injectExecStorageOS(t, "darwin")
	state := os.Getenv("WEFTY_EXEC_STORAGE_ROOT")
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", "")
	t.Setenv("XDG_STATE_HOME", state)
	root := filepath.Join(state, "wefty", "exec")
	// Fill the state root past its share of the cap. Fallback still gets swept.
	for i := range execSweepLimit {
		writeExecFile(t, filepath.Join(root, fmt.Sprintf("wefty-exec-%03d", i), "run-id"), "run-stalled")
	}
	trash := filepath.Join(os.TempDir(), "wefty-exec", ".wefty-trash-before-lookup")
	writeExecFile(t, filepath.Join(trash, "work", "leftover"), "data")
	probed := false
	stack.clients.l3.client.Transport = storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1/runs/run-stalled" {
			t.Errorf("unexpected lookup: %s", r.URL.Path)
		}
		probed = true
		// A blocked verification must not delay dispatching fallback trash.
		waitExecPath(t, trash, false)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	start := time.Now()
	if err := sweepExecStorage(t.Context(), stack.clients, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !probed || time.Since(start) > execSweepBudget+time.Second {
		t.Fatalf("fallback sweep budget: probed=%v elapsed=%s", probed, time.Since(start))
	}
	waitExecPath(t, trash, false)
}
