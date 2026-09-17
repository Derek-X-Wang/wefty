//go:build darwin || linux

package agent

import (
	"context"
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
	harness.manager = newHandoffManager(harness.root, t.TempDir(), retention, func(format string, args ...any) {
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
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("results expired before their window closed: %v", err)
	}
	// One second after, they are gone.
	harness.now = harness.now.Add(2 * time.Second)
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("results outlived their retention window: %v", err)
	}
	if !harness.logged("expired and were removed") {
		t.Fatalf("expiry was silent: %v", harness.logs)
	}
}

// TestRunsInFlightAreNeverSwept proves the exclusion the sweep actually uses:
// the lock registry every attempt passes through, not a path the caller
// remembered to pass in. Two runs hold their locks at once, both are long past
// their window, and the node is far over its byte budget -- and neither is
// touched, because deleting a directory a workload is writing into is the one
// thing collection must never do.
func TestRunsInFlightAreNeverSwept(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.rootBytes = 1

	live := make([]string, 0, 2)
	for _, runID := range []string{"run_live_one", "run_live_two"} {
		path := harness.retain(runID, true, true, map[string]int{"result.json": 4096})
		unlock, err := harness.manager.lock(t.Context(), handoffClaim(runID, path, nil).Job.Spec)
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		live = append(live, path)
	}

	harness.now = harness.now.Add(48 * time.Hour)
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	for _, path := range live {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("collection removed %s while an attempt held it: %v", filepath.Base(path), err)
		}
	}
	// Over budget with nothing evictable is a fact the node states rather than
	// a reason to take a running attempt's directory.
	if !harness.logged("still in flight") {
		t.Fatalf("an unevictable overrun was silent: %v", harness.logs)
	}
}

// TestAQuiescedRunIsSweptOnceItsLockIsReleased is the other half: exclusion is
// for the duration of the attempt, not forever.
func TestAQuiescedRunIsSweptOnceItsLockIsReleased(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := harness.retain("run_live", true, true, map[string]int{"result.json": 16})
	unlock, err := harness.manager.lock(t.Context(), handoffClaim("run_live", path, nil).Job.Spec)
	if err != nil {
		t.Fatal(err)
	}
	harness.now = harness.now.Add(2 * time.Hour)
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("collection removed a run still holding its lock: %v", err)
	}
	unlock()
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("an expired run survived after its attempt released it: %v", err)
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

	if err := harness.manager.collect(); err != nil {
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
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(foreign, "data")); err != nil {
		t.Fatalf("the sweep removed a directory it does not own: %v", err)
	}
}

// requireRetentionRecord reads the agent-local record, which is the only thing
// that decides retention and eviction.
func requireRetentionRecord(t *testing.T, manager *handoffManager, runID string) retentionRecord {
	t.Helper()
	for _, record := range manager.loadRecords() {
		if record.RunID == runID {
			return record
		}
	}
	t.Fatalf("no retention record for run %s", runID)
	return retentionRecord{}
}

// TestASymlinkNamedResultIsNotAProtectedResult is finding 6: the per-run bound
// protects a document, not a name. Protecting a link would keep the link and
// delete what it points at, which is the worst of both outcomes.
func TestASymlinkNamedResultIsNotAProtectedResult(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.runBytes = 2048
	path := filepath.Join(harness.root, "run_linked")
	spec := handoffClaim("run_linked", path, nil).Job.Spec
	if err := harness.manager.prepare(spec, "node-1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "payload.bin"), make([]byte, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("payload.bin", filepath.Join(path, handoffResultName)); err != nil {
		t.Fatal(err)
	}
	if err := harness.manager.finish(spec, "node-1", true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(path, handoffResultName)); !os.IsNotExist(err) {
		t.Fatalf("a symlink named %s was protected as a result: %v", handoffResultName, err)
	}
	if _, err := os.Stat(filepath.Join(path, "payload.bin")); !os.IsNotExist(err) {
		t.Fatalf("the oversize payload survived: %v", err)
	}
	if !harness.logged("is not a regular file") {
		t.Fatalf("the unprotected alias was silent: %v", harness.logs)
	}
}

// TestHardLinkedFilesAreChargedOnce keeps the byte budget honest: two names for
// one inode are one file on the disk, and charging both would evict results
// that are not using the space.
func TestHardLinkedFilesAreChargedOnce(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_linked_inode")
	spec := handoffClaim("run_linked_inode", path, nil).Job.Spec
	if err := harness.manager.prepare(spec, "node-1"); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(path, "payload.bin")
	if err := os.WriteFile(original, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	single, err := measureRetainedResults(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Link(original, filepath.Join(path, "payload-again.bin")); err != nil {
		t.Skipf("this filesystem cannot hard link: %v", err)
	}
	linked, err := measureRetainedResults(path)
	if err != nil {
		t.Fatal(err)
	}
	if linked != single {
		t.Fatalf("a second name for one inode added %d bytes to the run's budget", linked-single)
	}
}

// TestASymlinkIntoTheNodeIsNeverMeasuredOrFollowed keeps measurement from
// becoming a way to walk the filesystem, and from charging a run for storage
// that is not its own.
func TestASymlinkIntoTheNodeIsNeverMeasuredOrFollowed(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	elsewhere := t.TempDir()
	if err := os.WriteFile(filepath.Join(elsewhere, "big"), make([]byte, 65536), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(harness.root, "run_escape")
	spec := handoffClaim("run_escape", path, nil).Job.Spec
	if err := harness.manager.prepare(spec, "node-1"); err != nil {
		t.Fatal(err)
	}
	before, err := measureRetainedResults(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(path, "escape")); err != nil {
		t.Fatal(err)
	}
	after, err := measureRetainedResults(path)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("a symlink out of the run added %d bytes to its budget", after-before)
	}
	if _, err := os.Stat(filepath.Join(elsewhere, "big")); err != nil {
		t.Fatalf("measurement disturbed the target of a symlink: %v", err)
	}
}

// TestASweepIgnoresADirectoryWithNoRecord is finding 3's rule: a directory the
// agent has no record for is not the agent's to measure, evict or remove -- so
// a forged marker inside one is not deletion authority over it.
func TestASweepIgnoresADirectoryWithNoRecord(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.rootBytes = 1
	foreign := filepath.Join(harness.root, "run_not_ours")
	if err := os.MkdirAll(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "data"), make([]byte, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	// A marker that says whatever a writer wants it to say.
	if err := writeHandoffMarker(foreign, handoffMarker{
		RunID: "run_not_ours", NodeID: "node-1",
		RetainUntil: harness.now.Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	harness.now = harness.now.Add(48 * time.Hour)
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(foreign, "data")); err != nil {
		t.Fatalf("a directory with no agent record was swept on the strength of its own marker: %v", err)
	}
}

// TestACorruptRecordIsSkippedAndCollectionContinues: one unusable file must not
// be a way to stop a node from ever collecting again.
func TestACorruptRecordIsSkippedAndCollectionContinues(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	expired := harness.retain("run_expired", true, true, map[string]int{"result.json": 16})
	if err := os.WriteFile(filepath.Join(harness.manager.recordRoot(), "run_corrupt.json"),
		[]byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A record whose identity does not match where it is filed is refused too.
	if err := harness.manager.writeRecord(retentionRecord{
		RunID: "run_elsewhere", NodeID: "node-1",
		Directory:  "/etc",
		RetainedAt: harness.now, RetainUntil: harness.now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	harness.now = harness.now.Add(2 * time.Hour)
	if err := harness.manager.collect(); err != nil {
		t.Fatalf("a corrupt record stopped collection: %v", err)
	}
	if _, err := os.Stat(expired); !os.IsNotExist(err) {
		t.Fatalf("collection skipped a healthy expired run: %v", err)
	}
	if _, err := os.Stat("/etc"); err != nil {
		t.Fatalf("a record naming a directory outside the root was acted on: %v", err)
	}
	if !harness.logged("skip unusable retention record") || !harness.logged("skip untrustworthy retention record") {
		t.Fatalf("refusals were silent: %v", harness.logs)
	}
}

// TestFinishingAnAttemptCollectsWithoutAnyoneAskingIt is finding 1's rule, and
// it is deliberately driven through the lifecycle rather than by calling the
// sweep: collection that only a test remembers to invoke is collection a
// long-lived agent never performs.
func TestFinishingAnAttemptCollectsWithoutAnyoneAskingIt(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	manager := newHandoffManager(root, t.TempDir(), time.Hour, nil)
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	manager.now = func() time.Time { return now }

	// An older run, already retained and already past its window.
	stale := filepath.Join(root, "run_stale")
	staleSpec := handoffClaim("run_stale", stale, nil).Job.Spec
	if err := manager.prepare(staleSpec, "node-1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "result.json"), make([]byte, 64), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := manager.finish(staleSpec, "node-1", true, true); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)

	// A fresh attempt runs to completion through the lifecycle. Nothing in the
	// test asks for collection.
	runID := "run_fresh"
	path := filepath.Join(root, runID)
	runner := &handoffAssertingRunner{t: t, path: path}
	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		runtimes:     testRuntimeSet(runner),
		handoffs:     manager,
	}
	claim := handoffClaim(runID, path, nil)
	lifecycle := a.newAttemptLifecycle()
	result, runErr := lifecycle.runWorkload(context.Background(), claim)
	if runErr != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("runWorkload() = (%#v, %v)", result, runErr)
	}
	if _, err := lifecycle.finishCompletedAttempt(context.Background(), claim, result, runErr); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("finishing an attempt did not collect the expired run: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the run that just finished was collected: %v", err)
	}
}

// TestAnAttemptThatNeverCompletesStillAccountsForItsResults covers the paths
// that used to bypass retention entirely: an attempt whose completion never
// landed left its directory unbounded and invisible to the node's budget.
func TestAnAttemptThatNeverCompletesStillAccountsForItsResults(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	manager := newHandoffManager(root, t.TempDir(), time.Hour, nil)
	runID := "run_abandoned"
	path := filepath.Join(root, runID)
	spec := handoffClaim(runID, path, nil).Job.Spec
	if err := manager.prepare(spec, "node-1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "result.json"), make([]byte, 32), 0o600); err != nil {
		t.Fatal(err)
	}

	a := &Agent{
		registration: contract.NodeRegistration{NodeID: "node-1"},
		handoffs:     manager,
	}
	lifecycle := a.newAttemptLifecycle()
	// The conservative fallback the error paths take.
	if err := lifecycle.retainResults(handoffClaim(runID, path, nil), false, false); err != nil {
		t.Fatal(err)
	}
	record := requireRetentionRecord(t, manager, runID)
	if record.Succeeded || record.Published {
		t.Fatalf("an abandoned attempt recorded a verdict it never reached: %#v", record)
	}
	// And the completion path cannot overwrite it afterwards: whichever exit
	// reaches retention first owns it.
	if err := lifecycle.retainResults(handoffClaim(runID, path, nil), true, true); err != nil {
		t.Fatal(err)
	}
	if again := requireRetentionRecord(t, manager, runID); again.Succeeded {
		t.Fatalf("retention was recorded twice: %#v", again)
	}
}
