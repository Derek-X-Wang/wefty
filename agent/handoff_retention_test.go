//go:build darwin || linux

package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// retentionHarness builds a handoff root with a manual clock, so every bound
// below is proved by moving time rather than by waiting.
type retentionHarness struct {
	t       *testing.T
	root    string
	manager *handoffManager
	now     time.Time
	logs    []string
}

func newRetentionHarness(t *testing.T, retention time.Duration) *retentionHarness {
	t.Helper()
	harness := &retentionHarness{
		t: t, root: filepath.Join(t.TempDir(), "handoffs"),
		now: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
	}
	harness.manager = newHandoffManager(harness.root, retention, func(format string, args ...any) {
		harness.logs = append(harness.logs, fmt.Sprintf(format, args...))
	})
	harness.manager.now = func() time.Time { return harness.now }
	return harness
}

// retain finishes one run, with the files it produced already in place.
func (h *retentionHarness) retain(runID string, succeeded, published bool, files map[string]int) string {
	h.t.Helper()
	path := filepath.Join(h.root, runID)
	spec := handoffClaim(runID, path, nil).Job.Spec
	if err := h.manager.prepare(spec, "node-1"); err != nil {
		h.t.Fatal(err)
	}
	for name, size := range files {
		if err := os.WriteFile(filepath.Join(path, name), make([]byte, size), 0o600); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := h.manager.finish(spec, "node-1", succeeded, published); err != nil {
		h.t.Fatal(err)
	}
	return path
}

func (h *retentionHarness) logged(needle string) bool {
	for _, line := range h.logs {
		if strings.Contains(line, needle) {
			return true
		}
	}
	return false
}

// TestResultsSurviveASuccessfulRunUntilTheyExpire is the whole point of the
// change: the run that worked used to be the only one that left nothing behind.
func TestResultsSurviveASuccessfulRunUntilTheyExpire(t *testing.T) {
	harness := newRetentionHarness(t, contract.DefaultResultRetention)
	path := harness.retain("run_ok", true, true, map[string]int{"result.json": 32})

	if _, err := os.Stat(filepath.Join(path, "result.json")); err != nil {
		t.Fatalf("a successful run's result.json was not retained: %v", err)
	}
	// One second before the window closes, nothing is swept.
	harness.now = harness.now.Add(contract.DefaultResultRetention - time.Second)
	if err := harness.manager.cleanupExpired(""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("results expired before their window closed: %v", err)
	}
	// One second after, they are gone.
	harness.now = harness.now.Add(2 * time.Second)
	if err := harness.manager.cleanupExpired(""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("results outlived their retention window: %v", err)
	}
	if !harness.logged("expired and were removed") {
		t.Fatalf("expiry was silent: %v", harness.logs)
	}
}

// TestTheRunInFlightIsNeverSwept pins the exception every sweep depends on.
func TestTheRunInFlightIsNeverSwept(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := harness.retain("run_live", true, true, map[string]int{"result.json": 16})
	harness.now = harness.now.Add(2 * time.Hour)
	if err := harness.manager.cleanupExpired(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the sweep removed the run it was told to skip: %v", err)
	}
}

// TestOneRunPastItsBoundKeepsItsResultDocument is the per-run bound's one
// promise: whatever else goes, the result document stays whole.
func TestOneRunPastItsBoundKeepsItsResultDocument(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.runBytes = 4096
	path := harness.retain("run_big", true, true, map[string]int{
		"result.json":  512,
		"failures.txt": 4096,
		"huge.bin":     8192,
	})

	if payload, err := os.ReadFile(filepath.Join(path, "result.json")); err != nil || len(payload) != 512 {
		t.Fatalf("result.json = %d bytes, err %v; it must survive whole", len(payload), err)
	}
	if _, err := os.Stat(filepath.Join(path, "huge.bin")); !os.IsNotExist(err) {
		t.Fatalf("the largest file survived the per-run bound: %v", err)
	}
	_, size, err := handoffEntries(path)
	if err != nil {
		t.Fatal(err)
	}
	if size > harness.manager.runBytes {
		t.Fatalf("retained %d bytes, past the %d byte bound", size, harness.manager.runBytes)
	}
	if !harness.logged("dropped \"huge.bin\"") {
		t.Fatalf("the drop was silent: %v", harness.logs)
	}
}

// TestAResultDocumentPastTheBoundOnItsOwnIsKeptWhole: a partial result is not a
// result, so the bound yields rather than truncating it.
func TestAResultDocumentPastTheBoundOnItsOwnIsKeptWhole(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.runBytes = 1024
	path := harness.retain("run_huge_result", true, true, map[string]int{"result.json": 4096})

	if payload, err := os.ReadFile(filepath.Join(path, "result.json")); err != nil || len(payload) != 4096 {
		t.Fatalf("result.json = %d bytes, err %v; it must never be partially written", len(payload), err)
	}
	if !harness.logged("alone exceeds it") {
		t.Fatalf("an over-bound result was not reported: %v", harness.logs)
	}
}

// TestTheNodeBoundEvictsPublishedRunsFirstThenOldest pins the eviction order,
// which is the one place this design chooses what to lose.
func TestTheNodeBoundEvictsPublishedRunsFirstThenOldest(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.rootBytes = 3000

	// Two published runs, oldest first, then one whose evidence never reached
	// the ledger. Each is 1 KiB, so two must go.
	unpublishedOld := harness.retain("run_unpublished", true, false, map[string]int{"result.json": 1000})
	harness.now = harness.now.Add(time.Minute)
	publishedOld := harness.retain("run_published_old", true, true, map[string]int{"result.json": 1000})
	harness.now = harness.now.Add(time.Minute)
	publishedNew := harness.retain("run_published_new", true, true, map[string]int{"result.json": 1000})
	harness.now = harness.now.Add(time.Minute)
	newest := harness.retain("run_newest", true, true, map[string]int{"result.json": 1000})

	if err := harness.manager.cleanupExpired(""); err != nil {
		t.Fatal(err)
	}
	for _, evicted := range []string{publishedOld, publishedNew} {
		if _, err := os.Stat(evicted); !os.IsNotExist(err) {
			t.Fatalf("%s survived eviction: %v", filepath.Base(evicted), err)
		}
	}
	for _, kept := range []string{unpublishedOld, newest} {
		if _, err := os.Stat(kept); err != nil {
			t.Fatalf("%s was evicted: %v", filepath.Base(kept), err)
		}
	}
	// The unpublished run is the oldest of all and still survives: its files
	// are the only copy of what it did.
	if !harness.logged("evicted run run_published_old") {
		t.Fatalf("eviction was silent or out of order: %v", harness.logs)
	}
}

// TestASweepLeavesDirectoriesItDoesNotOwn keeps the sweep off anything that is
// not an agent-managed run, however full the node is.
func TestASweepLeavesDirectoriesItDoesNotOwn(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.rootBytes = 1
	foreign := filepath.Join(harness.root, "someone-elses-directory")
	if err := os.MkdirAll(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "data"), make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	harness.now = harness.now.Add(48 * time.Hour)
	if err := harness.manager.cleanupExpired(""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(foreign, "data")); err != nil {
		t.Fatalf("the sweep removed a directory it does not own: %v", err)
	}
}
