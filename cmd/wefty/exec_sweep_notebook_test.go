//go:build darwin || linux

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/execsweep"
)

type wrapperSweepEntry struct {
	Source             string `json:"source"`
	Pending            string `json:"pending"`
	StorageIdentity    string `json:"storage_identity"`
	AttemptID          string `json:"attempt_id"`
	OriginalStorage    string `json:"original_storage"`
	Allocation         string `json:"allocation"`
	AllocationIdentity string `json:"allocation_identity"`
	CreatedAt          int64  `json:"created_at"`
}

func wrapperSweepIdentity(t *testing.T, path string) string {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
}
func wrapperScannerShim(t *testing.T, body string) {
	t.Helper()
	tool := "find"
	if runtime.GOOS == "darwin" {
		tool = "lsof"
	}
	dir := t.TempDir()
	path := filepath.Join(dir, tool)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
}

// (a) Execute the shipped wrapper and inspect the publication at its actual
// storage rename. Both skipped and incomplete outcomes must be deferred.
func TestExecWrapperDefersSweep(t *testing.T) {
	for _, outcome := range []string{"skipped", "incomplete"} {
		t.Run(outcome, func(t *testing.T) {
			root := execTestStorageRoot(t)
			capture, err := captureExec(t.Context(), execTestRepo(t))
			if err != nil {
				t.Fatal(err)
			}
			identity := wrapperSweepIdentity(t, capture.Storage)
			notebook := filepath.Join(t.TempDir(), "notebook")
			observed := filepath.Join(t.TempDir(), "published-before-rename")
			env := filepath.Join(t.TempDir(), "bash-env")
			writeExecFile(t, env, fmt.Sprintf(`function /bin/mv() {
  if [ "$2" = %q ]; then
    allocation=$(cat "$2/tmp-dir") || return 95
    [ -f "$allocation/sweep-pending" ] || return 95
    found=0
    for entry in %q/*/entry.json; do [ ! -f "$entry" ] || found=1; done
    [ "$found" = 1 ] || return 95
    printf 'published\n' > %q
  fi
  command /bin/mv "$@"
}
`, capture.Storage, notebook, observed))
			argv := []string{"/bin/sh", "-c", `test -z "${WEFTY_EXEC_SWEEP_NOTEBOOK+x}" && test -z "${WEFTY_EXEC_SWEEP_ATTEMPT_ID+x}"`}
			ready := filepath.Join(t.TempDir(), "child")
			t.Cleanup(func() {
				raw, err := os.ReadFile(ready)
				if err != nil {
					return
				}
				pid, err := strconv.Atoi(string(raw))
				if err == nil {
					_ = syscall.Kill(pid, syscall.SIGKILL)
					_ = execStorageChildGone(pid)
				}
			})
			extra := []string{"BASH_ENV=" + env, "WEFTY_EXEC_SWEEP_NOTEBOOK=" + notebook, "WEFTY_EXEC_SWEEP_ATTEMPT_ID=attempt-wrapper"}
			if outcome == "skipped" {
				wrapperScannerShim(t, "exit 2")
			} else {
				tool := "find"
				if runtime.GOOS == "darwin" {
					tool = "lsof"
				}
				actual, err := exec.LookPath(tool)
				if err != nil {
					t.Fatal(err)
				}
				calls := filepath.Join(t.TempDir(), "scan-called")
				wrapperScannerShim(t, fmt.Sprintf("if [ -e %q ]; then exit 2; fi\n: > %q\nexec %q \"$@\"", calls, calls, actual))
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				argv = []string{binary, "-test.run=^TestExecStorageProcessHelper$"}
				extra = append(extra, "EXEC_STORAGE_PROCESS_HELPER=normal", "EXEC_STORAGE_READY="+ready)
			}
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", buildExecWrapper(capture, argv))
			command.Env = append(os.Environ(), extra...)
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err := command.Run(); err != nil {
				t.Fatalf("wrapper: %v: %s", err, &stderr)
			}
			if outcome == "incomplete" {
				raw, err := os.ReadFile(ready)
				if err != nil {
					t.Fatal(err)
				}
				pid, err := strconv.Atoi(string(raw))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL); _ = execStorageChildGone(pid) })
			}
			if raw, err := os.ReadFile(observed); err != nil || !strings.HasPrefix(string(raw), "published") {
				t.Fatalf("publication order not observed: %v %s; %s", err, raw, &stderr)
			}
			dirs, err := os.ReadDir(notebook)
			if err != nil {
				t.Fatal(err)
			}
			if len(dirs) != 1 || strings.HasSuffix(dirs[0].Name(), ".tmp") {
				t.Fatalf("notebook=%+v", dirs)
			}
			raw, err := os.ReadFile(filepath.Join(notebook, dirs[0].Name(), "entry.json"))
			if err != nil {
				t.Fatal(err)
			}
			var entry wrapperSweepEntry
			if err := json.Unmarshal(raw, &entry); err != nil {
				t.Fatal(err)
			}
			if entry.Source != capture.Storage || entry.OriginalStorage != capture.Storage || entry.StorageIdentity != identity || entry.AttemptID != "attempt-wrapper" || entry.CreatedAt <= 0 {
				t.Fatalf("wrong entry: %s", raw)
			}
			if filepath.Dir(entry.Pending) != filepath.Dir(capture.Storage) || !strings.HasPrefix(filepath.Base(entry.Pending), ".wefty-sweep-pending-") {
				t.Fatalf("not one in-parent rename: %s", raw)
			}
			if wrapperSweepIdentity(t, entry.Pending) != identity || wrapperSweepIdentity(t, entry.Allocation) != entry.AllocationIdentity {
				t.Fatalf("identity changed: %s", raw)
			}
			if _, err := os.Stat(filepath.Join(entry.Allocation, "sweep-pending")); err != nil {
				t.Fatal(err)
			}
			linkRecord, err := os.ReadFile(filepath.Join(entry.Pending, "tmp-link"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(strings.TrimSuffix(string(linkRecord), "\n")); !os.IsNotExist(err) {
				t.Fatalf("deferred cleanup retained the short temp alias: %v", err)
			}
			if _, err := os.Stat(capture.Storage); !os.IsNotExist(err) {
				t.Fatalf("source remained: %v", err)
			}
			old := time.Now().Add(-2 * time.Hour)
			if err := os.Chtimes(entry.Pending, old, old); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(entry.Allocation, old, old); err != nil {
				t.Fatal(err)
			}
			// The CLI storage sweep ignores pending names, and the shared allocation
			// sweeper used by both the CLI and node agent must honor the marker.
			if err := sweepExecStorage(t.Context(), nil, io.Discard); err != nil {
				t.Fatal(err)
			}
			if err := execsweep.SweepTempDirs(t.Context(), []string{root}); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(entry.Pending); err != nil {
				t.Fatalf("CLI swept pending storage: %v", err)
			}
			if _, err := os.Stat(entry.Allocation); err != nil {
				t.Fatalf("allocation protection lost: %v", err)
			}
			if outcome == "skipped" && !strings.Contains(stderr.String(), "could not scan processes") || outcome == "incomplete" && !strings.Contains(stderr.String(), "process cleanup in run storage was incomplete") {
				t.Fatalf("outcome warning missing: %s", &stderr)
			}
		})
	}
}

// (k3) A failed publication writes no marker and leaves no half-entry. Run
// through today's ordinary retirement, including asynchronous deletion.
func TestExecWrapperSweepPublicationFailure(t *testing.T) {
	root := execTestStorageRoot(t)
	capture, err := captureExec(t.Context(), execTestRepo(t))
	if err != nil {
		t.Fatal(err)
	}
	notebook := filepath.Join(t.TempDir(), "notebook")
	observed := filepath.Join(t.TempDir(), "no-marker-at-publish")
	env := filepath.Join(t.TempDir(), "bash-env")
	writeExecFile(t, env, fmt.Sprintf(`function /bin/mv() {
  case "$2" in
    *.tmp)
      allocation=$(cat %q/tmp-dir) || return 95
      [ ! -e "$allocation/sweep-pending" ] || return 95
      printf 'no-marker\n' > %q
      return 93
      ;;
  esac
  command /bin/mv "$@"
}
`, capture.Storage, observed))
	wrapperScannerShim(t, "exit 2")
	command := exec.CommandContext(t.Context(), "/bin/bash", "-c", buildExecWrapper(capture, []string{"/bin/sh", "-c", "exit 7"}))
	command.Env = append(os.Environ(), "BASH_ENV="+env, "WEFTY_EXEC_SWEEP_NOTEBOOK="+notebook, "WEFTY_EXEC_SWEEP_ATTEMPT_ID=attempt-failed")
	output, err := command.CombinedOutput()
	if err == nil || command.ProcessState.ExitCode() != 7 {
		t.Fatalf("verdict changed: %v %s", err, output)
	}
	if _, err := os.Stat(observed); err != nil {
		t.Fatalf("failed publication did not precede marker: %v", err)
	}
	if !strings.Contains(string(output), "could not defer run storage process sweep; deleting storage") {
		t.Fatalf("missing fallback warning: %s", output)
	}
	waitExecStorageEmpty(t, root)
	entries, err := os.ReadDir(notebook)
	if err != nil || len(entries) != 0 {
		t.Fatalf("half-published entry remained: %v %+v", err, entries)
	}
	entries, err = os.ReadDir(os.Getenv("WEFTY_EXEC_TEMP_ROOT"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("allocation remained after fallback: %v %+v", err, entries)
	}
}

// (g) The version 1 control requires notebook publication; version 2 must
// retain delete-and-warn even when the notebook environment is present.
func TestExecWrapperSweepVersionScope(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("version=%d", version), func(t *testing.T) {
			root := execTestStorageRoot(t)
			capture, err := captureExec(t.Context(), execTestRepo(t))
			if err != nil {
				t.Fatal(err)
			}
			mailbox := t.TempDir()
			script := buildExecWrapper(capture, []string{"/bin/sh", "-c", "exit 0"})
			if version == 2 {
				_, err = execGit(t.Context(), capture.Storage, filepath.Join(capture.Storage, "snapshot.git"), nil, "bundle", "create", filepath.Join(mailbox, "capture.bundle"), "--all")
				if err != nil {
					t.Fatal(err)
				}
				script, err = buildExecBundleWrapper(capture, []string{"/bin/sh", "-c", "exit 0"})
				if err != nil {
					t.Fatal(err)
				}
			}
			notebook := filepath.Join(t.TempDir(), "notebook")
			wrapperScannerShim(t, "exit 2")
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", script)
			command.Env = append(os.Environ(), "WEFTY_RUN_DIR="+mailbox, "WEFTY_EXEC_SWEEP_NOTEBOOK="+notebook, "WEFTY_EXEC_SWEEP_ATTEMPT_ID=attempt-version-scope")
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("version %d wrapper: %v %s", version, err, output)
			}
			if !strings.Contains(string(output), "could not scan processes in run storage") {
				t.Fatalf("missing scan warning: %s", output)
			}
			if version == 1 {
				entries, err := os.ReadDir(notebook)
				if err != nil || len(entries) != 1 {
					t.Fatalf("version 1 did not defer: %v %+v", err, entries)
				}
				return
			}
			waitExecPath(t, filepath.Join(mailbox, "wefty-exec-work"), false)
			waitExecDeleteSteps(t, mailbox)
			if _, err := os.Stat(notebook); !os.IsNotExist(err) {
				t.Fatalf("version 2 wrote notebook: %v", err)
			}
			if err := cleanupExecStorage(capture.Storage); err != nil {
				t.Fatal(err)
			}
			waitExecStorageEmpty(t, root)
		})
	}
}

// A publisher that observes its entry was removed falls back at both remaining
// publication steps, without leaving pending storage or a protection marker.
func TestExecWrapperSweepEntryRemoved(t *testing.T) {
	for _, phase := range []string{"before-marker", "before-rename"} {
		t.Run(phase, func(t *testing.T) {
			root := execTestStorageRoot(t)
			capture, err := captureExec(t.Context(), execTestRepo(t))
			if err != nil {
				t.Fatal(err)
			}
			notebook := filepath.Join(t.TempDir(), "notebook")
			observed := filepath.Join(t.TempDir(), "entry-removed")
			unexpected := filepath.Join(t.TempDir(), "unexpected-marker")
			env := filepath.Join(t.TempDir(), "bash-env")
			writeExecFile(t, env, fmt.Sprintf(`remove_publishing_entry() {
  local allocation found entry
  allocation=$(cat %q/tmp-dir) || return 95
  if [ %q = before-marker ]; then
    [ ! -e "$allocation/sweep-pending" ] || return 95
  else
    [ -f "$allocation/sweep-pending" ] || return 95
  fi
  found=0
  for entry in %q/*/entry.json; do
    [ -f "$entry" ] || continue
    command /bin/rm -rf -- "${entry%%/*}" || return 95
    found=1
  done
  [ "$found" = 1 ] || return 95
  printf 'removed\n' > %q
}
function /bin/mv() {
  command /bin/mv "$@" || return $?
  case "$2" in
    *.tmp) if [ %q = before-marker ]; then remove_publishing_entry; fi ;;
  esac
}
function /bin/chmod() {
  local allocation
  if [ "$1" = u+w ] && [ "$2" = %q ] && [ %q = before-marker ]; then
    allocation=$(cat %q/tmp-dir) || return 95
    if [ -e "$allocation/sweep-pending" ]; then : > %q; fi
  fi
  if [ "$1" = u+w ] && [ "$2" = %q ] && [ %q = before-rename ] && [ ! -e %q ]; then
    remove_publishing_entry || return $?
  fi
  command /bin/chmod "$@"
}
`, capture.Storage, phase, notebook, observed, phase, capture.Storage, phase, capture.Storage, unexpected, capture.Storage, phase, observed))
			wrapperScannerShim(t, "exit 2")
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", buildExecWrapper(capture, []string{"/bin/sh", "-c", "exit 7"}))
			command.Env = append(os.Environ(), "BASH_ENV="+env, "WEFTY_EXEC_SWEEP_NOTEBOOK="+notebook, "WEFTY_EXEC_SWEEP_ATTEMPT_ID=attempt-entry-removed")
			output, err := command.CombinedOutput()
			if err == nil || command.ProcessState.ExitCode() != 7 {
				t.Fatalf("verdict changed: %v %s", err, output)
			}
			if _, err := os.Stat(observed); err != nil {
				t.Fatalf("entry was not removed at %s: %v; %s", phase, err, output)
			}
			if _, err := os.Stat(unexpected); !os.IsNotExist(err) {
				t.Fatalf("marker written after entry was removed: %v", err)
			}
			if !strings.Contains(string(output), "could not defer run storage process sweep; deleting storage") {
				t.Fatalf("missing fallback warning: %s", output)
			}
			waitExecStorageEmpty(t, root)
			entries, err := os.ReadDir(notebook)
			if err != nil || len(entries) != 0 {
				t.Fatalf("notebook remained: %v %+v", err, entries)
			}
			entries, err = os.ReadDir(os.Getenv("WEFTY_EXEC_TEMP_ROOT"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("allocation or marker remained: %v %+v", err, entries)
			}
		})
	}
}
