//go:build darwin || linux

package agent

import (
	"encoding/json"
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
	harness.manager = newHandoffManager(harness.root, t.TempDir(), "node-1", retention, func(format string, args ...any) {
		harness.logs = append(harness.logs, fmt.Sprintf(format, args...))
	})
	harness.manager.now = func() time.Time { return harness.now }
	return harness
}

// prepareHandoffForTest uses the same explicit lock receipt as production.
func prepareHandoffForTest(t *testing.T, manager *handoffManager, spec contract.JobSpec) *handoffOwnership {
	t.Helper()
	lease, err := manager.lock(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.release)
	owner, err := manager.prepare(lease, spec, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

// retain finishes one run, with the files it produced already in place.
func (h *retentionHarness) retain(runID string, succeeded, published bool, files map[string]int) string {
	h.t.Helper()
	path := filepath.Join(h.root, runID)
	spec := handoffClaim(runID, path, nil).Job.Spec
	owner := prepareHandoffForTest(h.t, h.manager, spec)
	defer owner.lease.release()
	for name, size := range files {
		if err := os.WriteFile(filepath.Join(path, name), make([]byte, size), 0o600); err != nil {
			h.t.Fatal(err)
		}
	}
	if err := h.manager.finish(owner, spec, "node-1", succeeded, published); err != nil {
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

	live := make([]string, 0, 2)
	for _, runID := range []string{"run_live_one", "run_live_two"} {
		path := harness.retain(runID, true, true, map[string]int{"result.json": 4096})
		unlock, err := harness.manager.lock(t.Context(), handoffClaim(runID, path, nil).Job.Spec)
		if err != nil {
			t.Fatal(err)
		}
		defer unlock.release()
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
	unlock.release()
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
	size := measureRun(t, harness.manager, "run_big")
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

// TestASweepLeavesDirectoriesItDoesNotOwn keeps the sweep off anything that is
// not an agent-managed run, however full the node is.
func TestASweepLeavesDirectoriesItDoesNotOwn(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
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

// measureRun reports one run's retained bytes through the manager's own view.
func measureRun(t *testing.T, manager *handoffManager, runID string) int64 {
	t.Helper()
	run, err := manager.openRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	_, size, err := handoffEntries(run)
	if err != nil {
		t.Fatal(err)
	}
	return size
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
	owner := prepareHandoffForTest(t, harness.manager, spec)
	if err := os.WriteFile(filepath.Join(path, "payload.bin"), make([]byte, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("payload.bin", filepath.Join(path, handoffResultName)); err != nil {
		t.Fatal(err)
	}
	if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
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
func TestHardLinkedFilesAreChargedPerLink(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_linked_inode")
	spec := handoffClaim("run_linked_inode", path, nil).Job.Spec
	prepareHandoffForTest(t, harness.manager, spec)
	original := filepath.Join(path, "payload.bin")
	if err := os.WriteFile(original, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	single := measureRun(t, harness.manager, "run_linked_inode")
	if err := os.Link(original, filepath.Join(path, "payload-again.bin")); err != nil {
		t.Skipf("this filesystem cannot hard link: %v", err)
	}
	// Part 1 charges per link rather than per inode: cross-run inode identity
	// is accounting the node budget needed, and the node budget is #494. The
	// assertion is that the second link is charged, not that it is free.
	linked := measureRun(t, harness.manager, "run_linked_inode")
	if linked != single+4096 {
		t.Fatalf("a second name for a 4096 byte inode changed the run by %d bytes, want 4096", linked-single)
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
	prepareHandoffForTest(t, harness.manager, spec)
	before := measureRun(t, harness.manager, "run_escape")
	if err := os.Symlink(elsewhere, filepath.Join(path, "escape")); err != nil {
		t.Fatal(err)
	}
	after := measureRun(t, harness.manager, "run_escape")
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
	foreign := filepath.Join(harness.root, "run_not_ours")
	if err := os.MkdirAll(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "data"), make([]byte, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	// A marker that says whatever a writer wants it to say.
	foreignRoot, err := os.OpenRoot(foreign)
	if err != nil {
		t.Fatal(err)
	}
	defer foreignRoot.Close()
	if err := writeHandoffMarker(foreignRoot, handoffMarker{
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

// TestARecordNamingAPathComponentItIsNotIsRefused covers the validation that
// stands between a record and a deletion. A record is authority to remove a
// directory, so every field is checked and anything missing fails closed.
func TestARecordNamingAPathComponentItIsNotIsRefused(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		record retentionRecord
		want   string
	}{
		{
			name: "a run id that is not one component",
			record: retentionRecord{
				RunID: "../escape", NodeID: "node-1", Directory: "/x",
			},
			want: "one safe path component",
		},
		{
			name: "a directory outside this root",
			record: retentionRecord{
				RunID: "run_ok", NodeID: "node-1", Directory: "/etc",
			},
			want: "is not this root's run",
		},
		{
			name: "another node's record",
			record: retentionRecord{
				RunID: "run_ok", NodeID: "other-node", Directory: "",
			},
			want: "belongs to node",
		},
		{
			name: "no retention window",
			record: retentionRecord{
				RunID: "run_ok", NodeID: "node-1", Directory: "",
			},
			want: "carries no retention window",
		},
		{
			name: "a window longer than the contract allows",
			record: retentionRecord{
				RunID: "run_ok", NodeID: "node-1", Directory: "",
			},
			want: "past the retention window",
		},
		{
			name: "retained in the future",
			record: retentionRecord{
				RunID: "run_ok", NodeID: "node-1", Directory: "",
			},
			want: "retained in the future",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root := "/tmp/handoffs"
			now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
			record := testCase.record
			if record.Directory == "" {
				record.Directory = filepath.Join(root, record.RunID)
			}
			switch testCase.want {
			case "carries no retention window":
			case "past the retention window":
				record.RetainedAt = now
				record.RetainUntil = now.Add(48 * time.Hour)
			case "retained in the future":
				record.RetainedAt = now.Add(time.Hour)
				record.RetainUntil = record.RetainedAt.Add(time.Minute)
			default:
				record.RetainedAt = now
				record.RetainUntil = now.Add(time.Minute)
			}
			err := validRetentionRecord(record, recordComponent(record.RunID), root, "node-1", time.Hour, now)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("validation error = %v, want one mentioning %q", err, testCase.want)
			}
		})
	}
}

// TestASymlinkedHandoffPathIsNeverFollowed: a record naming a path that has
// been replaced with a link is skipped rather than followed into the node.
func TestASymlinkedHandoffPathIsNeverFollowed(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	elsewhere := t.TempDir()
	if err := os.WriteFile(filepath.Join(elsewhere, "not-ours"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := harness.retain("run_swapped", true, true, map[string]int{"result.json": 16})
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, path); err != nil {
		t.Fatal(err)
	}

	harness.now = harness.now.Add(2 * time.Hour)
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(elsewhere, "not-ours")); err != nil {
		t.Fatalf("collection followed a symlinked handoff path: %v", err)
	}
	if !harness.logged("not a directory") {
		t.Fatalf("a swapped handoff path was skipped silently: %v", harness.logs)
	}
}

// TestTheResultCollectorStopsWhenTheAgentDoes: a sweep that outlived its agent
// would be deleting under a node another agent has already claimed, so Close
// cancels it and waits.
func TestTheResultCollectorStopsWhenTheAgentDoes(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	a := &Agent{
		clock:    systemClock{},
		handoffs: newHandoffManager(root, t.TempDir(), "node-1", time.Hour, nil),
	}
	a.startResultCollector()
	if a.collectorDone == nil {
		t.Fatal("the collector did not start")
	}
	done := a.collectorDone
	a.stopResultCollector()
	select {
	case <-done:
	default:
		t.Fatal("stopResultCollector returned before the collector exited")
	}
	if a.collectorCancel != nil {
		t.Fatal("the collector was not released")
	}
	// Idempotent: Close may run after an explicit stop.
	a.stopResultCollector()
}

// TestThePerRunBoundMeasuresTheDirectoryThatIsActuallyThere is the recreated-
// directory no-op. A workload that does `rm -rf $handoff && mkdir $handoff`
// leaves the handle preparation pinned pointing at an unlinked inode, so finish
// trimmed nothing, reported nothing, and left every oversized byte on the node
// while the node believed the run fit its bound.
//
// The test proves the old behaviour rather than asserting it from memory: it
// trims through the pinned handle first, exactly as the old finish did, and
// shows the recreated files survive that untouched.
func TestThePerRunBoundMeasuresTheDirectoryThatIsActuallyThere(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.manager.runBytes = 1024
	path := filepath.Join(harness.root, "run_recreated")
	spec := handoffClaim("run_recreated", path, nil).Job.Spec
	owner := prepareHandoffForTest(t, harness.manager, spec)

	// The workload's own rm -rf and mkdir, between preparation and finish.
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, size := range map[string]int{"result.json": 32, "oversize.bin": 8192} {
		if err := os.WriteFile(filepath.Join(path, name), make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// The negative, run first: trimming through the pinned handle -- which is
	// all the old finish did -- removes nothing, and says nothing is wrong.
	if err := harness.manager.enforceRunBound(owner.run, "run_recreated"); err != nil {
		t.Fatalf("trimming the pinned handle reported an error: %v", err)
	}
	if info, err := os.Lstat(filepath.Join(path, "oversize.bin")); err != nil || info.Size() != 8192 {
		t.Fatalf("the pinned handle is not the unlinked inode this test needs: %v, %v", info, err)
	}

	if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(path, "oversize.bin")); !os.IsNotExist(err) {
		t.Fatalf("the per-run bound skipped the recreated directory: %v", err)
	}
	if size := measureRun(t, harness.manager, "run_recreated"); size > harness.manager.runBytes {
		t.Fatalf("the recreated directory retains %d bytes, past the %d byte bound", size, harness.manager.runBytes)
	}
	if _, err := os.Stat(filepath.Join(path, "result.json")); err != nil {
		t.Fatalf("the recreated run's result document was not kept: %v", err)
	}
	if !harness.logged("no longer the directory preparation pinned") {
		t.Fatalf("the identity mismatch was silent: %v", harness.logs)
	}
	if !harness.logged("run run_recreated:") {
		t.Fatalf("the mismatch did not name the run: %v", harness.logs)
	}
	// Nothing went wrong that the record has to carry: the bound reached the
	// directory that is really there.
	if anomaly := requireRetentionRecord(t, harness.manager, "run_recreated").BoundAnomaly; anomaly != "" {
		t.Fatalf("bound anomaly = %q, want none", anomaly)
	}
}

// TestABoundThatCannotReachItsDirectoryRecordsWhy covers the two ways
// re-acquisition refuses. In both the directory standing at the run's name is
// left exactly as it is -- a symlink is never followed and another run's files
// are never trimmed to bound this one -- and the refusal is on the record
// instead of nowhere.
func TestABoundThatCannotReachItsDirectoryRecordsWhy(t *testing.T) {
	t.Run("a symlink planted at the run's name", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		harness.manager.runBytes = 1024
		elsewhere := t.TempDir()
		if err := os.WriteFile(filepath.Join(elsewhere, "not-ours.bin"), make([]byte, 8192), 0o600); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(harness.root, "run_linked")
		spec := handoffClaim("run_linked", path, nil).Job.Spec
		owner := prepareHandoffForTest(t, harness.manager, spec)
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, path); err != nil {
			t.Fatal(err)
		}

		if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(elsewhere, "not-ours.bin")); err != nil {
			t.Fatalf("the bound followed a symlink out of the handoff root: %v", err)
		}
		record := requireRetentionRecord(t, harness.manager, "run_linked")
		if record.BoundAnomaly != handoffBoundDirectoryUnreachable {
			t.Fatalf("bound anomaly = %q, want %q", record.BoundAnomaly, handoffBoundDirectoryUnreachable)
		}
		if !harness.logged("cannot be bounded") {
			t.Fatalf("the refusal was silent: %v", harness.logs)
		}
	})

	t.Run("another run's directory at this run's name", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		harness.manager.runBytes = 1024
		path := filepath.Join(harness.root, "run_displaced")
		spec := handoffClaim("run_displaced", path, nil).Job.Spec
		owner := prepareHandoffForTest(t, harness.manager, spec)
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(handoffMarker{
			RunID: "run_somebody_else", NodeID: "node-1", RetainUntil: harness.now.Add(time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, handoffMarkerName), payload, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "theirs.bin"), make([]byte, 8192), 0o600); err != nil {
			t.Fatal(err)
		}

		if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
			t.Fatal(err)
		}
		if info, err := os.Lstat(filepath.Join(path, "theirs.bin")); err != nil || info.Size() != 8192 {
			t.Fatalf("another run's files were trimmed to bound this one: %v, %v", info, err)
		}
		record := requireRetentionRecord(t, harness.manager, "run_displaced")
		if record.BoundAnomaly != handoffBoundDirectoryForeign {
			t.Fatalf("bound anomaly = %q, want %q", record.BoundAnomaly, handoffBoundDirectoryForeign)
		}
		if !harness.logged("not this run's to trim") {
			t.Fatalf("the refusal was silent: %v", harness.logs)
		}
	})
}

// TestAnUnremovableExpiredRunIsQuarantinedRatherThanRetriedForever: a symlink
// planted at an expired run's name is refused by every sweep, so before this
// the same refusal was logged hourly for as long as the node ran and nothing
// ever decided. The retry is bounded; then the record is quarantined with a
// typed reason a node doctor can show, and the directory is still neither
// followed nor deleted.
func TestAnUnremovableExpiredRunIsQuarantinedRatherThanRetriedForever(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	elsewhere := t.TempDir()
	if err := os.WriteFile(filepath.Join(elsewhere, "not-ours"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := harness.retain("run_stuck", true, true, map[string]int{"result.json": 16})
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, path); err != nil {
		t.Fatal(err)
	}
	harness.now = harness.now.Add(2 * time.Hour)

	for attempt := 1; attempt < maxExpiryAttempts; attempt++ {
		if err := harness.manager.collect(); err != nil {
			t.Fatal(err)
		}
		record := requireRetentionRecord(t, harness.manager, "run_stuck")
		if record.ExpiryFailures != attempt {
			t.Fatalf("after %d sweeps the record counts %d failures", attempt, record.ExpiryFailures)
		}
		if record.Quarantine != "" {
			t.Fatalf("the record was quarantined after %d of %d attempts", attempt, maxExpiryAttempts)
		}
	}
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	record := requireRetentionRecord(t, harness.manager, "run_stuck")
	if record.Quarantine != handoffExpiryDirectoryUnremovable {
		t.Fatalf("quarantine = %q, want %q", record.Quarantine, handoffExpiryDirectoryUnremovable)
	}
	if record.QuarantinedAt.IsZero() || !strings.Contains(record.QuarantineDetail, "not a directory") {
		t.Fatalf("quarantine carries no usable cause: %+v", record)
	}
	if !harness.logged("is left exactly as it is") {
		t.Fatalf("the quarantine was silent: %v", harness.logs)
	}

	// The retry stops: a further sweep says nothing more about this run.
	before := len(harness.logs)
	for range 3 {
		if err := harness.manager.collect(); err != nil {
			t.Fatal(err)
		}
	}
	for _, line := range harness.logs[before:] {
		if strings.Contains(line, "run_stuck") {
			t.Fatalf("a quarantined record was retried: %q", line)
		}
	}
	if _, err := os.Stat(filepath.Join(elsewhere, "not-ours")); err != nil {
		t.Fatalf("quarantine followed the symlink it refused: %v", err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("the planted name was removed: %v, %v", info, err)
	}
}
