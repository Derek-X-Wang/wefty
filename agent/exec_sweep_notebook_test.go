//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/execsweep"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

type execNotebookFixture struct {
	manager  *handoffManager
	clock    *manualClock
	entry    execSweepEntry
	entryDir string
	mu       sync.Mutex
	logs     []string
}

func newExecNotebookFixture(t *testing.T, source bool) *execNotebookFixture {
	t.Helper()
	physical, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &execNotebookFixture{clock: newManualClock(time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC))}
	f.manager = newHandoffManager(filepath.Join(physical, "handoffs"), t.TempDir(), "node-1", time.Hour, func(format string, args ...any) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.logs = append(f.logs, fmt.Sprintf(format, args...))
	})
	f.manager.now = f.clock.Now
	f.entry = execSweepEntry{
		Source:     filepath.Join(physical, "storage", "wefty-exec-capture"),
		Pending:    filepath.Join(physical, "storage", ".wefty-sweep-pending-wefty-exec-capture-123"),
		Allocation: filepath.Join(physical, "wefty-run-data.notebook"),
		AttemptID:  "attempt-notebook", CreatedAt: f.clock.Now().Unix(),
	}
	f.entry.OriginalStorage = f.entry.Source
	for _, path := range []string{filepath.Join(f.entry.Source, "work"), filepath.Join(f.entry.Allocation, "tmp")} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	notebookWrite(t, filepath.Join(f.entry.Source, "tmp-dir"), f.entry.Allocation+"\n")
	notebookWrite(t, filepath.Join(f.entry.Allocation, "owner"), f.entry.Source+"\n")
	notebookWrite(t, filepath.Join(f.entry.Allocation, "sweep-pending"), "")
	f.entry.StorageIdentity = notebookIdentity(t, f.entry.Source)
	f.entry.AllocationIdentity = notebookIdentity(t, f.entry.Allocation)
	f.entryDir = filepath.Join(f.manager.execSweepNotebook(), "entry-test")
	f.publish(t)
	if !source {
		if err := os.Rename(f.entry.Source, f.entry.Pending); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func notebookWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
func notebookIdentity(t *testing.T, path string) string {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return execDirectoryIdentity(info)
}
func (f *execNotebookFixture) publish(t *testing.T) {
	t.Helper()
	raw, err := json.Marshal(f.entry)
	if err != nil {
		t.Fatal(err)
	}
	notebookWrite(t, filepath.Join(f.entryDir, "entry.json"), string(raw))
}
func (f *execNotebookFixture) collect(t *testing.T) {
	t.Helper()
	if err := f.manager.collectExecSweeps(t.Context(), 3*time.Second); err != nil {
		t.Fatal(err)
	}
}
func (f *execNotebookFixture) messages() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.logs, "\n")
}
func notebookExists(t *testing.T, path string, want bool) {
	t.Helper()
	_, err := os.Lstat(path)
	if want && err != nil || !want && !os.IsNotExist(err) {
		t.Fatalf("path %s exists=%v: %v", path, want, err)
	}
}
func notebookScannerShim(t *testing.T, body string) string {
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
	return path
}

// (b) A successful empty inventory resolves storage and allocation before entry.
func TestExecSweepNotebookEmpty(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	notebookScannerShim(t, "exit 0")
	f.collect(t)
	for _, path := range []string{f.entry.Pending, f.entry.Allocation, f.entryDir} {
		notebookExists(t, path, false)
	}
}

// (c) The shared TERM -> confirmation -> KILL path covers both recorded roots.
func TestExecSweepNotebookProcesses(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	first, firstDone := startExecSweepProcess(t, filepath.Join(f.entry.Pending, "work"))
	second, secondDone := startExecSweepProcess(t, filepath.Join(f.entry.Allocation, "tmp"))
	tool := "find"
	if runtime.GOOS == "darwin" {
		tool = "lsof"
	}
	actual, err := exec.LookPath(tool)
	if err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(t.TempDir(), "inventory-calls")
	notebookScannerShim(t, fmt.Sprintf("printf 'scan\\n' >> %q\nexec %q \"$@\"", calls, actual))
	signals := filepath.Join(t.TempDir(), "signals")
	env := filepath.Join(t.TempDir(), "bash-env")
	notebookWrite(t, env, fmt.Sprintf("kill() { printf '%%s\\n' \"$*\" >> %q; builtin kill \"$@\"; }\n", signals))
	t.Setenv("BASH_ENV", env)
	f.collect(t)
	waitExecSweepProcess(t, firstDone)
	waitExecSweepProcess(t, secondDone)
	raw, err := os.ReadFile(signals)
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range []int{first.Process.Pid, second.Process.Pid} {
		term := strings.Index(string(raw), fmt.Sprintf("-TERM %d\n", pid))
		kill := strings.Index(string(raw), fmt.Sprintf("-KILL %d\n", pid))
		if term < 0 || kill <= term {
			t.Fatalf("missing ordered TERM/KILL for %d: %s", pid, raw)
		}
	}
	raw, err = os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(raw), "scan\n") < 4 {
		t.Fatalf("both roots were not confirmed: %s", raw)
	}
	for _, path := range []string{f.entry.Pending, f.entry.Allocation, f.entryDir} {
		notebookExists(t, path, false)
	}
}

// (d) Repeated failure retains the entry, emits one warning, and retries on
// the next existing collector tick. The same clock drives ticks and expiry.
func TestExecSweepNotebookCollectorTicks(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	mode := filepath.Join(t.TempDir(), "mode")
	notebookWrite(t, mode, "fail")
	calls := filepath.Join(t.TempDir(), "scans")
	notebookScannerShim(t, fmt.Sprintf("printf 'scan\\n' >> %q\n[ \"$(cat %q)\" = complete ] || exit 2\nexit 0", calls, mode))
	a := &Agent{clock: f.clock, handoffs: f.manager}
	a.startResultCollector(t.Context())
	t.Cleanup(a.stopResultCollector)
	if err := awaitFixtureCondition(t.Context(), "startup sweep and hourly collector timer", func() bool { return f.clock.hasDeadline(f.clock.Now().Add(time.Hour)) }); err != nil {
		t.Fatal(err)
	}
	notebookExists(t, f.entryDir, true)
	if got := strings.Count(f.messages(), "warning:"); got != 1 {
		t.Fatalf("startup warnings=%d: %s", got, f.messages())
	}
	f.clock.Advance(time.Hour - time.Second)
	raw, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "scan\n" {
		t.Fatalf("collector ran before hour: %s", raw)
	}
	notebookWrite(t, mode, "complete")
	f.clock.Advance(time.Second)
	if err := awaitFixtureCondition(t.Context(), "next tick resolves notebook", func() bool { _, err := os.Stat(f.entryDir); return os.IsNotExist(err) }); err != nil {
		t.Fatal(err)
	}
	a.stopResultCollector()
	for _, path := range []string{f.entry.Pending, f.entry.Allocation, f.entryDir} {
		notebookExists(t, path, false)
	}
	if got := strings.Count(f.messages(), "warning:"); got != 1 {
		t.Fatalf("warnings=%d: %s", got, f.messages())
	}
}

// (e,h) A source entry is skipped without positive reap proof, including after
// restart. Proof for another attempt cannot authorize this publisher's rename.
func TestExecSweepNotebookReapRule(t *testing.T) {
	f := newExecNotebookFixture(t, true)
	scans := filepath.Join(t.TempDir(), "scans")
	notebookScannerShim(t, fmt.Sprintf("printf 'scan\\n' >> %q\nexit 0", scans))
	f.collect(t)
	f.manager.noteExecSweepReaped("another-attempt")
	f.collect(t)
	for _, path := range []string{f.entry.Source, f.entry.Allocation, f.entryDir} {
		notebookExists(t, path, true)
	}
	notebookExists(t, scans, false)
	f.manager.noteExecSweepReaped(f.entry.AttemptID)
	restarted := newHandoffManager(f.manager.root, f.manager.stateRoot, "node-1", time.Hour, t.Logf)
	restarted.now = f.clock.Now
	if err := restarted.collectExecSweeps(t.Context(), 3*time.Second); err != nil {
		t.Fatal(err)
	}
	notebookExists(t, f.entry.Source, true)
	f.collect(t)
	for _, path := range []string{f.entry.Source, f.entry.Pending, f.entry.Allocation, f.entryDir} {
		notebookExists(t, path, false)
	}
}

// (f) A changed identity resolves only that part, without touching replacements.
func TestExecSweepNotebookIdentityChanged(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	for _, path := range []string{f.entry.Pending, f.entry.Allocation} {
		// Keep the old inode linked: the replacement cannot reuse it.
		if err := os.Rename(path, path+"-old"); err != nil {
			t.Fatal(err)
		}
		notebookWrite(t, filepath.Join(path, "keep"), "replacement")
	}
	scans := filepath.Join(t.TempDir(), "scans")
	notebookScannerShim(t, fmt.Sprintf("printf 'unexpected\\n' > %q\nexit 2", scans))
	f.collect(t)
	notebookExists(t, f.entryDir, false)
	notebookExists(t, scans, false)
	for _, path := range []string{f.entry.Pending, f.entry.Allocation} {
		notebookExists(t, filepath.Join(path, "keep"), true)
	}
}

// (i) The first deletion completed but the process crashed before the second.
func TestExecSweepNotebookDeletionResume(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	if err := os.RemoveAll(f.entry.Pending); err != nil {
		t.Fatal(err)
	}
	notebookScannerShim(t, "exit 0")
	f.collect(t)
	notebookExists(t, f.entry.Allocation, false)
	notebookExists(t, f.entryDir, false)
}

// (j) Exact 24-hour give-up never invokes a scanner or sends a signal. A real
// same-user process in the pending folder survives its folder's deletion.
func TestExecSweepNotebookGiveUp(t *testing.T) {
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprintf("source=%v", source), func(t *testing.T) {
			f := newExecNotebookFixture(t, source)
			storage := f.entry.Pending
			if source {
				storage = f.entry.Source
			}
			process := exec.Command("/bin/sleep", "600")
			process.Dir = filepath.Join(storage, "work")
			if err := process.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { _ = process.Wait(); close(done) }()
			t.Cleanup(func() { _ = process.Process.Kill(); waitExecSweepProcess(t, done) })
			scans := filepath.Join(t.TempDir(), "scans")
			notebookScannerShim(t, fmt.Sprintf("printf 'unexpected\\n' > %q\nexit 2", scans))
			f.clock.Advance(24*time.Hour - time.Second)
			f.collect(t)
			notebookExists(t, f.entryDir, true)
			notebookExists(t, filepath.Join(f.entry.Allocation, "sweep-pending"), true)
			// A failing scan may run before the deadline; none may run on give-up.
			_ = os.Remove(scans)
			before := strings.Count(f.messages(), "warning:")
			f.clock.Advance(time.Second)
			f.collect(t)
			notebookExists(t, scans, false)
			notebookExists(t, f.entryDir, false)
			notebookExists(t, filepath.Join(f.entry.Allocation, "sweep-pending"), false)
			notebookExists(t, f.entry.Allocation, true)
			notebookExists(t, storage, source)
			if err := process.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("give-up stopped process: %v", err)
			}
			select {
			case <-done:
				t.Fatal("process exited on give-up")
			default:
			}
			if got := strings.Count(f.messages(), "warning:") - before; got != 1 {
				t.Fatalf("give-up warnings=%d: %s", got, f.messages())
			}
			if !strings.Contains(f.messages(), "giving up exec sweep entry entry-test") {
				t.Fatalf("entry not named: %s", f.messages())
			}
			f.collect(t)
			if got := strings.Count(f.messages(), "warning:") - before; got != 1 {
				t.Fatalf("give-up warning repeated: %s", f.messages())
			}
		})
	}
}

func TestExecSweepNotebookLeaseAndStaging(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	notebookScannerShim(t, "exit 0")
	guard, err := f.manager.sharedLease("node-results-collection")
	if err != nil {
		t.Fatal(err)
	}
	f.collect(t)
	notebookExists(t, f.entry.Pending, true)
	notebookExists(t, f.entryDir, true)
	guard.Close()
	stale := filepath.Join(f.manager.execSweepNotebook(), "stale.tmp")
	fresh := filepath.Join(f.manager.execSweepNotebook(), "fresh.tmp")
	notebookWrite(t, filepath.Join(stale, "entry.json"), "partial")
	notebookWrite(t, filepath.Join(fresh, "entry.json"), "partial")
	old := f.clock.Now().Add(-time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(fresh, f.clock.Now(), f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	f.collect(t)
	notebookExists(t, f.entryDir, false)
	notebookExists(t, stale, false)
	notebookExists(t, fresh, true)
}

// (k) Kill the publisher's own process group at precise shell seams. The
// ordinary EXIT fallback cannot run after SIGKILL.
func TestExecSweepNotebookPublicationCrashes(t *testing.T) {
	for _, phase := range []string{"after-publish", "after-marker"} {
		t.Run(phase, func(t *testing.T) {
			f := newExecNotebookFixture(t, true)
			if err := os.RemoveAll(f.entryDir); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(f.entry.Allocation, "sweep-pending")); err != nil {
				t.Fatal(err)
			}
			t.Setenv("WEFTY_EXEC_TEMP_ROOT", filepath.Dir(f.entry.Allocation))
			notebookScannerShim(t, "exit 2")
			env := filepath.Join(t.TempDir(), "bash-env")
			notebookWrite(t, env, fmt.Sprintf(`function /bin/mv() {
  case "$2" in
    *.tmp)
      command /bin/mv "$@" || return $?
      if [ %q = after-publish ]; then kill -KILL -- "-$$"; fi
      ;;
    *)
      if [ %q = after-marker ]; then kill -KILL -- "-$$"; fi
      command /bin/mv "$@"
      ;;
  esac
}
`, phase, phase))
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", execsweep.Script+`
exec_sweep_notebook=$1
retire_exec_storage "$2" capture
`, "publication-crash", f.manager.execSweepNotebook(), f.entry.Source)
			command.Env = append(os.Environ(), "BASH_ENV="+env, "WEFTY_EXEC_SWEEP_ATTEMPT_ID="+f.entry.AttemptID)
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("publisher was not killed: %s", output)
			}
			entries, err := os.ReadDir(f.manager.execSweepNotebook())
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || strings.HasSuffix(entries[0].Name(), ".tmp") {
				t.Fatalf("entry not published: %+v", entries)
			}
			f.entryDir = filepath.Join(f.manager.execSweepNotebook(), entries[0].Name())
			raw, err := os.ReadFile(filepath.Join(f.entryDir, "entry.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &f.entry); err != nil {
				t.Fatal(err)
			}
			f.entry.CreatedAt = f.clock.Now().Unix()
			f.publish(t)
			notebookExists(t, f.entry.Source, true)
			notebookExists(t, f.entry.Pending, false)
			notebookExists(t, filepath.Join(f.entry.Allocation, "sweep-pending"), phase == "after-marker")
			old := time.Now().Add(-2 * time.Hour)
			if err := os.Chtimes(f.entry.Allocation, old, old); err != nil {
				t.Fatal(err)
			}
			if err := execsweep.SweepTempDirs(t.Context(), []string{filepath.Dir(f.entry.Source)}); err != nil {
				t.Fatal(err)
			}
			notebookExists(t, f.entry.Allocation, true)
			f.collect(t) // No reap proof; the source and entry stay.
			notebookExists(t, f.entry.Source, true)
			notebookExists(t, f.entryDir, true)
			if phase == "after-publish" {
				f.manager.noteExecSweepReaped(f.entry.AttemptID)
				f.collect(t) // Failed scan after recovery must keep marker protection.
				notebookExists(t, f.entry.Pending, true)
				notebookExists(t, filepath.Join(f.entry.Allocation, "sweep-pending"), true)
			}
			f.clock.Advance(24 * time.Hour)
			f.collect(t)
			notebookExists(t, f.entryDir, false)
			notebookExists(t, filepath.Join(f.entry.Allocation, "sweep-pending"), false)
			notebookExists(t, f.entry.Source, phase == "after-marker")
			if phase == "after-publish" {
				notebookExists(t, f.entry.Pending, false)
			}
		})
	}
}

// A swapped directory must also be refused after its handle was opened.
func TestExecSweepNotebookRechecksBeforeAct(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	held, err := openExecNotebookDirectory(f.entry.Pending, f.entry.StorageIdentity)
	if err != nil || held == nil {
		t.Fatalf("open: %v", err)
	}
	defer held.close()
	if err := os.Rename(f.entry.Pending, f.entry.Pending+"-moved"); err != nil {
		t.Fatal(err)
	}
	notebookWrite(t, filepath.Join(f.entry.Pending, "keep"), "replacement")
	notebookScannerShim(t, "exit 0")
	if outcome, _ := held.sweep(t.Context(), time.Second); outcome != execSweepSkipped {
		t.Fatalf("swapped path scanned: %s", outcome)
	}
	if err := held.remove(context.Background()); err == nil {
		t.Fatal("swapped path deleted")
	}
	notebookExists(t, filepath.Join(f.entry.Pending, "keep"), true)
}

func TestExecSweepNotebookIncomplete(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	process, _ := startExecSweepProcess(t, filepath.Join(f.entry.Pending, "work"))
	tool := "find"
	if runtime.GOOS == "darwin" {
		tool = "lsof"
	}
	actual, err := exec.LookPath(tool)
	if err != nil {
		t.Fatal(err)
	}
	called := filepath.Join(t.TempDir(), "called")
	notebookScannerShim(t, fmt.Sprintf("if [ -e %q ]; then exit 2; fi\n: > %q\nexec %q \"$@\"", called, called, actual))
	f.collect(t)
	for _, path := range []string{f.entry.Pending, f.entry.Allocation, f.entryDir} {
		notebookExists(t, path, true)
	}
	if err := process.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("failed confirmation sent KILL: %v", err)
	}
	if got := strings.Count(f.messages(), "warning:"); got != 1 || !strings.Contains(f.messages(), "incomplete") {
		t.Fatalf("warnings=%d: %s", got, f.messages())
	}
}

// Verify that the lifecycle, rather than elapsed time or a claimed success,
// supplies the receipt used for source recovery.
type execNotebookReapRuntime struct {
	captureRuntime
	receipt workloadrunner.ReapReceipt
	err     error
}

func (r *execNotebookReapRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return r.receipt, r.err
}

func TestExecSweepNotebookLifecycleReapReceipt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		receipt workloadrunner.ReapReceipt
		err     error
		want    bool
	}{
		{"valid", workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil, true},
		{"not-quiesced", workloadrunner.ReapReceipt{Evidence: workloadrunner.ReapEvidenceAttempt}, nil, false},
		{"no-evidence", workloadrunner.ReapReceipt{RuntimeQuiesced: true}, nil, false},
		{"error", workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, errors.New("reap failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExecNotebookFixture(t, true)
			path := filepath.Join(f.manager.root, "run_notebook")
			claim := retentionClaim(t, "run_notebook", path)
			claim.Lease.AttemptID = f.entry.AttemptID
			claim.Job.Spec.Execution.Env = mailboxSpec("run_notebook", path, "").Execution.Env
			claim.Job.Spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: mailboxTestToken}
			claim.SubmittedByRunLedger = true
			claim.Job.Spec.Labels[contract.LabelRunParams] = `{"exec_version":1}`
			runtime := &execNotebookReapRuntime{receipt: tc.receipt, err: tc.err}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				handoffs: f.manager, runtimes: workloadRuntimeSet{contract.JobKindProcess: runtime}, runLedger: captureSweepAppender(),
				clock: systemClock{}, nodeID: "node-1", bootSessionID: "notebook-test", logf: t.Logf,
			})
			_, err := lifecycle.runWorkload(t.Context(), claim)
			if tc.want && err != nil {
				t.Fatal(err)
			}
			f.manager.mu.Lock()
			got := f.manager.execSweepReaped[f.entry.AttemptID]
			f.manager.mu.Unlock()
			if got != tc.want {
				t.Fatalf("reap proof=%v, want %v; err=%v", got, tc.want, err)
			}
		})
	}
}

func TestExecSweepNotebookUsesNodeDeadline(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	notebookScannerShim(t, "/bin/sleep 1.5\nexit 0")
	a := &Agent{handoffs: f.manager, runtimes: workloadRuntimeSet{contract.JobKindProcess: processrunner.NewAdapter(processrunner.New(processrunner.Config{TerminationGraceTime: 4 * time.Second}))}}
	a.sweepExecNotebook(t.Context())
	for _, path := range []string{f.entry.Pending, f.entry.Allocation, f.entryDir} {
		notebookExists(t, path, false)
	}
}

// Startup collection belongs to the collector goroutine. A slow scan or
// diagnostic sink must not block the caller that is starting the node agent.
func TestExecSweepNotebookStartupDoesNotBlock(t *testing.T) {
	f := newExecNotebookFixture(t, false)
	notebookScannerShim(t, "exit 2")
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.manager.logf = func(string, ...any) { close(entered); <-release }
	a := &Agent{clock: f.clock, handoffs: f.manager}
	go func() { a.startResultCollector(t.Context()); close(returned) }()
	t.Cleanup(func() {
		close(release)
		testwait.Receive(t, "collector start returns during cleanup", returned)
		a.stopResultCollector()
	})
	testwait.Receive(t, "startup sweep reached its diagnostic sink", entered)
	testwait.Receive(t, "collector start returns while startup sweep is blocked", returned)
}
