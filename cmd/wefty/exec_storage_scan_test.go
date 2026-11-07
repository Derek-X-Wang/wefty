package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecTrashRetirementSkipsProcessInventory(t *testing.T) {
	root := execTestStorageRoot(t)
	trash := filepath.Join(root, ".wefty-trash-ready")
	writeExecFile(t, filepath.Join(trash, "leftover"), "data")
	marker := filepath.Join(t.TempDir(), "scanner-invoked")
	shim := t.TempDir()
	for _, name := range []string{"lsof", "find"} {
		path := filepath.Join(shim, name)
		writeExecFile(t, path, fmt.Sprintf("#!/bin/sh\nprintf invoked > %q\n", marker))
		if err := os.Chmod(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.CommandContext(t.Context(), "/bin/bash", "-c", execCleanupScript+"\nretire_exec_storage \"$1\" trash", "trash-retirement", trash)
	command.Env = append(os.Environ(), "PATH="+shim+":"+os.Getenv("PATH"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("retirement: %v %s", err, output)
	}
	waitExecStorageEmpty(t, root)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("trash retirement invoked process discovery: %v", err)
	}
}

func TestExecCaptureSweepRemovesOnlyRetiredStorage(t *testing.T) {
	root := execTestStorageRoot(t)
	storage := filepath.Join(root, "wefty-exec-capture")
	writeExecFile(t, filepath.Join(storage, "leftover"), "data")
	record := filepath.Join(t.TempDir(), "removals")
	env := filepath.Join(t.TempDir(), "bash-env")
	writeExecFile(t, env, fmt.Sprintf(`function /bin/rm() {
 printf '%%s\n' "$*" >> %q
 command /bin/rm "$@"
}
`, record))
	command := exec.CommandContext(t.Context(), "/bin/bash", "-c", execCleanupScript+"\nretire_exec_storage \"$1\" capture", "capture-retirement", storage)
	command.Env = append(os.Environ(), "BASH_ENV="+env)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("retirement: %v %s", err, output)
	}
	waitExecStorageEmpty(t, root)
	raw, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		// Inventory may unlink its private status file, never a scratch folder.
		if strings.HasPrefix(line, "-f -- /tmp/wefty-exec-scan.") {
			continue
		}
		target := strings.TrimPrefix(line, "-rf -- ")
		if !strings.HasPrefix(filepath.Base(target), ".wefty-trash-") {
			t.Errorf("sweep removed a scratch path before retirement: %s", line)
		}
	}
}

func TestExecCaptureScanFailureWarnsOnce(t *testing.T) {
	root := execTestStorageRoot(t)
	storage := filepath.Join(root, "wefty-exec-discovery-failure")
	writeExecFile(t, filepath.Join(storage, "leftover"), "data")
	shim := t.TempDir()
	for _, name := range []string{"lsof", "find"} {
		path := filepath.Join(shim, name)
		writeExecFile(t, path, "#!/bin/sh\nprintf 'scanner detail\\n' >&2\nexit 1\n")
		if err := os.Chmod(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.CommandContext(t.Context(), "/bin/bash", "-c", execCleanupScript+"\nretire_exec_storage \"$1\" capture", "failed-inventory", storage)
	command.Env = append(os.Environ(), "PATH="+shim+":"+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("scan failure blocked retirement: %v %s", err, output)
	}
	if string(output) != "wefty: warning: could not scan processes in run storage\n" {
		t.Errorf("scan failure did not emit exactly one warning: %q", output)
	}
	waitExecStorageEmpty(t, root)
}

func TestExecStorageSweepConfirmsProcessBeforeKill(t *testing.T) {
	// Fabricated pids start above every real pid (Linux PID_MAX_LIMIT is
	// 4194304; macOS stops at 99999). The sweep skips its own $$ and $PPID,
	// so a fabricated pid equal to the test shell would vanish.
	for _, test := range []struct {
		name        string
		first       string
		second      string
		folder      string
		terms       int
		noTerm      bool
		gone        bool
		kill        bool
		stopped     int
		warning     bool
		scanWarning bool
		capWarning  bool
		termFailure bool
		killFailure bool
	}{
		{name: "absent", warning: true},
		{name: "absent-and-gone", gone: true, stopped: 1},
		{name: "outside", second: `printf 'p4194400\ncsleep\nn%s-outside\n' "$test_storage"`},
		{name: "different-name", second: `printf 'p4194400\ncother\nn%s\n' "$test_storage"`},
		{name: "agent", second: `printf 'p4194400\ncwefty-agent\nn%s\n' "$test_storage"`},
		{name: "still-matches", second: `printf 'p4194400\ncsleep\nn%s\n' "$test_storage"`, kill: true, stopped: 1},
		{name: "failed", second: "return 1", warning: true},
		{name: "timeout", second: "return 137", warning: true},
		{name: "failed-and-gone", second: "return 1", gone: true, stopped: 1, warning: true},
		{name: "term-failed-alive", termFailure: true, warning: true},
		{name: "term-failed-gone", termFailure: true, gone: true},
		{name: "kill-failed-alive", second: `printf 'p4194400\ncsleep\nn%s\n' "$test_storage"`, killFailure: true, kill: true, warning: true},
		{name: "escaped-cwd", first: `printf 'p4194400\ncsleep\nn%s/%s\n' "$test_storage" 'escaped\folder'`, noTerm: true},
		{name: "backslash-storage", folder: `capture\folder`, noTerm: true, scanWarning: true},
		{name: "caret-cwd", first: `printf 'p4194400\ncsleep\nn%s/%s\n' "$test_storage" '^A'`, noTerm: true},
		{name: "caret-storage", folder: "capture^A", noTerm: true, scanWarning: true},
		{name: "tab-storage", folder: "capture\tcontrol", noTerm: true, scanWarning: true},
		{name: "newline-storage", folder: "capture\n", noTerm: true, scanWarning: true},
		{name: "control-storage", folder: "capture\x01control", noTerm: true, scanWarning: true},
		{name: "delete-control-storage", folder: "capture\x7fcontrol", noTerm: true, scanWarning: true},
		{name: "process-cap", first: `for ((test_i=0; test_i<65; test_i++)); do
 printf 'p%d\ncsleep\nn%s\n' "$((4194400+test_i))" "$test_storage"
 done`, terms: 64, gone: true, stopped: 64, capWarning: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			storage, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if test.folder != "" {
				storage = filepath.Join(storage, test.folder)
				if err := os.Mkdir(storage, 0700); err != nil {
					t.Fatal(err)
				}
			}
			first := test.first
			if first == "" {
				first = `printf 'p4194400\ncsleep\nn%s\n' "$test_storage"`
			}
			signals := filepath.Join(t.TempDir(), "signals")
			// Override shell functions only in this test invocation. Darwin-format
			// records exercise both passes on either host; kill records decisions
			// without ever signaling a fabricated pid or an unrelated process.
			script := execCleanupScript + fmt.Sprintf(`
 test_storage=$1 test_signals=$2
 uname() { printf 'Darwin\n'; }
 exec_storage_bounded_inventory() {
   if [ "${3:-1}" = 1 ]; then
     %s
   else
     [ "$3" = 0.6 ] || return 1
     %s
   fi
 }
 kill() {
   printf '%%s\n' "$*" >> "$test_signals"
   if [ "$1" = -0 ] && [ %t = true ]; then return 1; fi
   if [ "$1" = -TERM ] && [ %t = true ]; then return 1; fi
   if [ "$1" = -KILL ] && [ %t = true ]; then return 1; fi
   return 0
 }
 sweep_exec_storage_processes "$test_storage"
 printf '{"outcome":"%%s","stopped":[%%s]}\n' "$exec_sweep_outcome" "$exec_stopped_processes"
`, first, test.second+"\n   :", test.gone, test.termFailure, test.killFailure)
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", script, "confirm-kill", storage, signals)
			var stderr strings.Builder
			command.Stderr = &stderr
			output, err := command.Output()
			if err != nil {
				t.Fatalf("sweep: %v: %s", err, &stderr)
			}
			raw, err := os.ReadFile(signals)
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			wantTerms := test.terms
			if wantTerms == 0 && !test.noTerm {
				wantTerms = 1
			}
			if got := strings.Count(string(raw), "-TERM "); got != wantTerms {
				t.Errorf("TERM count = %d, want %d: %s", got, wantTerms, raw)
			}
			if test.noTerm && len(raw) != 0 {
				t.Errorf("ambiguous path received signals: %s", raw)
			}
			if got := strings.Contains(string(raw), "-KILL 4194400\n"); got != test.kill {
				t.Errorf("KILL = %t, want %t: %s", got, test.kill, raw)
			}
			var stopped []struct {
				PID  int    `json:"pid"`
				Name string `json:"name"`
				CWD  string `json:"cwd"`
			}
			var evidence struct {
				Outcome string          `json:"outcome"`
				Stopped json.RawMessage `json:"stopped"`
			}
			if err := json.Unmarshal(output, &evidence); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(evidence.Stopped, &stopped); err != nil {
				t.Fatal(err)
			}
			wantOutcome := "complete"
			if test.scanWarning {
				wantOutcome = "skipped"
			} else if test.warning || test.capWarning || test.name == "outside" || test.name == "different-name" || test.name == "agent" {
				wantOutcome = "incomplete"
			}
			if evidence.Outcome != wantOutcome {
				t.Errorf("outcome=%q, want %s", evidence.Outcome, wantOutcome)
			}
			if len(stopped) != test.stopped {
				t.Errorf("stopped count = %d, want %d: %s", len(stopped), test.stopped, output)
			}
			for i, process := range stopped {
				if process.PID != 4194400+i || process.Name != "sleep" || process.CWD != storage {
					t.Errorf("stopped process lost original evidence: %s", output)
				}
			}
			wantWarning, wantScanWarning, wantCapWarning := 0, 0, 0
			if test.warning {
				wantWarning = 1
			}
			if test.scanWarning {
				wantScanWarning = 1
			}
			if test.capWarning {
				wantCapWarning = 1
			}
			if got := strings.Count(stderr.String(), "wefty: stopped process "); got != test.stopped {
				t.Errorf("stopped log count = %d: %s", got, &stderr)
			}
			if got := strings.Count(stderr.String(), "wefty: warning: could not scan processes in run storage\n"); got != wantScanWarning {
				t.Errorf("scan warning count = %d: %s", got, &stderr)
			}
			if got := strings.Count(stderr.String(), "wefty: warning: too many processes in run storage; limited cleanup to the first 64 matching processes\n"); got != wantCapWarning {
				t.Errorf("cap warning count = %d: %s", got, &stderr)
			}
			if got := strings.Count(stderr.String(), "wefty: warning: could not stop a process in run storage\n"); got != wantWarning {
				t.Errorf("warning count = %d: %s", got, &stderr)
			}
			wantIncompleteWarning := 0
			if wantOutcome == "incomplete" {
				wantIncompleteWarning = 1
			}
			if got := strings.Count(stderr.String(), "wefty: warning: process cleanup in run storage was incomplete\n"); got != wantIncompleteWarning {
				t.Errorf("incomplete warning count = %d: %s", got, &stderr)
			}
		})
	}
}

func TestExecSweepOutcomesRetireStorage(t *testing.T) {
	for _, outcome := range []string{"complete", "skipped", "incomplete"} {
		t.Run(outcome, func(t *testing.T) {
			root := execTestStorageRoot(t)
			storage := filepath.Join(root, "capture")
			writeExecFile(t, filepath.Join(storage, "leftover"), "data")
			// The sweep matches physical cwd paths (cd -P), so report the same
			// path even where the temp root is a symlink, such as /var on macOS.
			physical, err := filepath.EvalSymlinks(storage)
			if err != nil {
				t.Fatal(err)
			}
			shim := t.TempDir()
			calls := filepath.Join(shim, "calls")
			// Darwin records let this fixture use a fabricated PID on both OSes.
			// The second scanner invocation fails to inject confirmation failure.
			writeExecFile(t, filepath.Join(shim, "lsof"), fmt.Sprintf(`#!/bin/sh
if [ %q = skipped ]; then exit 2; fi
if [ %q = complete ]; then exit 0; fi
if [ -e %q ]; then exit 2; fi
: > %q
printf 'p4194400\ncsleep\nn%%s\n' %q
`, outcome, outcome, calls, calls, physical))
			if err := os.Chmod(filepath.Join(shim, "lsof"), 0700); err != nil {
				t.Fatal(err)
			}
			script := execCleanupScript + `
uname() { printf 'Darwin\n'; }
kill() {
  case "$*" in *' 4194400') return 0 ;; esac
  command kill "$@"
}
retire_exec_storage "$1" capture || exit $?
printf '%s\n' "$exec_sweep_outcome"
`
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", script, "sweep-outcome", storage)
			command.Env = append(os.Environ(), "PATH="+shim+":"+os.Getenv("PATH"))
			var stderr strings.Builder
			command.Stderr = &stderr
			output, err := command.Output()
			if err != nil || strings.TrimSpace(string(output)) != outcome {
				t.Fatalf("retirement outcome=%q want=%s: %v; %s", output, outcome, err, &stderr)
			}
			warnings := ""
			switch outcome {
			case "skipped":
				warnings = "wefty: warning: could not scan processes in run storage\n"
			case "incomplete":
				warnings = "wefty: warning: could not stop a process in run storage\nwefty: warning: process cleanup in run storage was incomplete\n"
			}
			if stderr.String() != warnings {
				t.Errorf("diagnostics=%q, want %q", &stderr, warnings)
			}
			waitExecStorageEmpty(t, root)
		})
	}
}

func TestExecWrapperScanDeadlineValidation(t *testing.T) {
	for _, tc := range []struct{ value, want string }{
		{"", "1"}, {"3", "3"}, {"2.5", "2.5"}, {"0", "1"}, {"4", "1"},
		{"-1", "1"}, {"1; exit 99", "1"}, {"NaN", "1"}, {"3.1", "1"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			root := execTestStorageRoot(t)
			storage := filepath.Join(root, "capture")
			writeExecFile(t, filepath.Join(storage, "leftover"), "data")
			deadline := filepath.Join(t.TempDir(), "deadline")
			// A missing snapshot enters the real wrapper's cleanup trap early.
			// Observe the scanner argument without creating an executable snapshot.
			cleanup := execCleanupScript + fmt.Sprintf(`
exec_storage_bounded_inventory() { printf '%%s' "$3" > %q; }
`, deadline)
			script := strings.Replace(execWrapper, "__WEFTY_EXEC_CLEANUP__", cleanup, 1)
			script = strings.Replace(script, "__WEFTY_EXEC_INPUT__", fmt.Sprintf("storage=%q", storage), 1)
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", script)
			command.Env = append(os.Environ(), "WEFTY_EXEC_SCAN_DEADLINE="+tc.value)
			if out, err := command.CombinedOutput(); err == nil {
				t.Fatalf("missing snapshot unexpectedly succeeded: %s", out)
			}
			got, err := os.ReadFile(deadline)
			if err != nil || string(got) != tc.want {
				t.Fatalf("deadline=%q want=%s: %v", got, tc.want, err)
			}
			waitExecStorageEmpty(t, root)
		})
	}
}
