//go:build darwin || linux

package agent

import (
	"context"
	"errors"
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
	// measured is what the last budget pass found before it acted on it. A
	// pass that evicted reports twice, and the figures a test wants are
	// usually the first ones.
	measured RetainedResultsStatus
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
			// A record may legitimately carry an admission and no window --
			// that is a run still executing -- but one carrying neither says
			// nothing at all about the directory it names.
			name: "neither an admission nor a retention window",
			record: retentionRecord{
				RunID: "run_ok", NodeID: "node-1", Directory: "",
			},
			want: "carries neither an admission nor a retention window",
		},
		{
			name: "half a retention window",
			record: retentionRecord{
				RunID: "run_ok", NodeID: "node-1", Directory: "",
			},
			want: "carries half a retention window",
		},
		{
			name: "admitted in the future",
			record: retentionRecord{
				RunID: "run_ok", NodeID: "node-1", Directory: "",
			},
			want: "admitted in the future",
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
			case "carries neither an admission nor a retention window":
			case "carries half a retention window":
				record.AdmittedAt = now
				record.RetainedAt = now
			case "admitted in the future":
				record.AdmittedAt = now.Add(time.Hour)
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
	a.startResultCollector(t.Context())
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

// TestThePerRunBoundReportsADriftedDirectoryRatherThanSkippingItSilently is the
// recreated-directory no-op. A workload that does
// `rm -rf $handoff && mkdir $handoff` between preparation and finish left the
// handle preparation pinned pointing at an unlinked inode, so finish trimmed
// nothing, reported nothing, and left every oversized byte on the node while
// the node believed the run fit its bound.
//
// The fix is deliberately the conservative half. Nothing here can prove a
// directory that appeared after preparation is this run's -- the only evidence
// would be the ownership marker, which a workload can write whatever it likes
// into -- so the replacement is left alone and the drift is named in the log
// and on the record instead. The bound is no longer silently skipped; it is
// openly not met.
func TestThePerRunBoundReportsADriftedDirectoryRatherThanSkippingItSilently(t *testing.T) {
	t.Run("removed and recreated by the workload", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		harness.manager.runBytes = 1024
		path := filepath.Join(harness.root, "run_recreated")
		spec := handoffClaim("run_recreated", path, nil).Job.Spec
		owner := prepareHandoffForTest(t, harness.manager, spec)

		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "oversize.bin"), make([]byte, 8192), 0o600); err != nil {
			t.Fatal(err)
		}

		if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
			t.Fatal(err)
		}
		if info, err := os.Lstat(filepath.Join(path, "oversize.bin")); err != nil || info.Size() != 8192 {
			t.Fatalf("a directory this agent cannot prove is the run's was trimmed: %v, %v", info, err)
		}
		if size := measureRun(t, harness.manager, "run_recreated"); size <= harness.manager.runBytes {
			t.Fatalf("the fixture is not over the bound: %d bytes", size)
		}
		if !harness.logged("no longer the directory preparation pinned") {
			t.Fatalf("the drift was silent: %v", harness.logs)
		}
		if !harness.logged("run run_recreated:") {
			t.Fatalf("the drift did not name the run: %v", harness.logs)
		}
		if anomaly := requireRetentionRecord(t, harness.manager, "run_recreated").BoundAnomaly; anomaly != handoffBoundDirectoryReplaced {
			t.Fatalf("bound anomaly = %q, want %q", anomaly, handoffBoundDirectoryReplaced)
		}
	})

	t.Run("renamed away and replaced", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		harness.manager.runBytes = 1024
		path := filepath.Join(harness.root, "run_renamed")
		spec := handoffClaim("run_renamed", path, nil).Job.Spec
		owner := prepareHandoffForTest(t, harness.manager, spec)
		if err := os.WriteFile(filepath.Join(path, "pinned-oversize.bin"), make([]byte, 8192), 0o600); err != nil {
			t.Fatal(err)
		}
		moved := path + "-moved"
		if err := os.Rename(path, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "theirs.bin"), make([]byte, 8192), 0o600); err != nil {
			t.Fatal(err)
		}

		if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
			t.Fatal(err)
		}
		// Neither is trimmed: the name no longer leads to the prepared
		// directory, and the replacement is not provably this run's.
		if info, err := os.Lstat(filepath.Join(moved, "pinned-oversize.bin")); err != nil || info.Size() != 8192 {
			t.Fatalf("a directory the run's name no longer leads to was trimmed: %v, %v", info, err)
		}
		if info, err := os.Lstat(filepath.Join(path, "theirs.bin")); err != nil || info.Size() != 8192 {
			t.Fatalf("an unproven replacement was trimmed: %v, %v", info, err)
		}
		if anomaly := requireRetentionRecord(t, harness.manager, "run_renamed").BoundAnomaly; anomaly != handoffBoundDirectoryReplaced {
			t.Fatalf("bound anomaly = %q, want %q", anomaly, handoffBoundDirectoryReplaced)
		}
	})
}

// TestABoundThatCannotReachItsDirectoryRecordsWhy: a symlink planted at the
// run's name is never followed, never trimmed and never opened, and the fact
// that the bound stopped at the pinned directory is on the record rather than
// nowhere.
func TestABoundThatCannotReachItsDirectoryRecordsWhy(t *testing.T) {
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
	if info, err := os.Lstat(filepath.Join(elsewhere, "not-ours.bin")); err != nil || info.Size() != 8192 {
		t.Fatalf("the bound followed a symlink out of the handoff root: %v, %v", info, err)
	}
	record := requireRetentionRecord(t, harness.manager, "run_linked")
	if record.BoundAnomaly != handoffBoundDirectoryReplaced {
		t.Fatalf("bound anomaly = %q, want %q", record.BoundAnomaly, handoffBoundDirectoryReplaced)
	}
	if !harness.logged("no longer the directory preparation pinned") {
		t.Fatalf("the drift was silent: %v", harness.logs)
	}
}

// plantSymlinkAtExpiredRun retains one run, replaces its name with a symlink
// out of the handoff root, and moves the clock past the window -- the shape the
// sweep structurally cannot remove and must not follow.
func plantSymlinkAtExpiredRun(t *testing.T, harness *retentionHarness, runID string) (path, elsewhere string) {
	t.Helper()
	elsewhere = t.TempDir()
	if err := os.WriteFile(filepath.Join(elsewhere, "not-ours"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	path = harness.retain(runID, true, true, map[string]int{"result.json": 16})
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, path); err != nil {
		t.Fatal(err)
	}
	harness.now = harness.now.Add(2 * time.Hour)
	return path, elsewhere
}

// TestAStructurallyUnremovableExpiredRunPausesAndResumesByItself: a symlink at
// an expired run's name is refused identically by every sweep, so before this
// the same refusal was logged hourly for as long as the node ran and nothing
// ever decided. The retry is bounded -- and the pause is not a retirement: the
// sweep keeps looking at the name, once and cheaply, and resumes the moment a
// directory is back there. No operator, no restart.
func TestAStructurallyUnremovableExpiredRunPausesAndResumesByItself(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path, elsewhere := plantSymlinkAtExpiredRun(t, harness, "run_stuck")

	for attempt := 1; attempt < maxStructuralRefusals; attempt++ {
		if err := harness.manager.collect(); err != nil {
			t.Fatal(err)
		}
		record := requireRetentionRecord(t, harness.manager, "run_stuck")
		if record.StructuralRefusals != attempt {
			t.Fatalf("after %d sweeps the record counts %d structural refusals", attempt, record.StructuralRefusals)
		}
		if record.Quarantine != "" {
			t.Fatalf("the sweep paused after %d of %d refusals", attempt, maxStructuralRefusals)
		}
	}
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	record := requireRetentionRecord(t, harness.manager, "run_stuck")
	if record.Quarantine != handoffExpiryNameNotADirectory {
		t.Fatalf("quarantine = %q, want %q", record.Quarantine, handoffExpiryNameNotADirectory)
	}
	if record.QuarantinedAt.IsZero() || !strings.Contains(record.QuarantineDetail, "not a directory") {
		t.Fatalf("quarantine carries no usable cause: %+v", record)
	}
	if !harness.logged("left exactly as it is until a directory is back there") {
		t.Fatalf("the pause was silent: %v", harness.logs)
	}

	// Paused: further sweeps say nothing more, and touch nothing.
	before := len(harness.logs)
	for range 3 {
		if err := harness.manager.collect(); err != nil {
			t.Fatal(err)
		}
	}
	for _, line := range harness.logs[before:] {
		if strings.Contains(line, "run_stuck") {
			t.Fatalf("a paused record was retried: %q", line)
		}
	}
	if _, err := os.Stat(filepath.Join(elsewhere, "not-ours")); err != nil {
		t.Fatalf("the sweep followed the symlink it refused: %v", err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("the planted name was removed: %v, %v", info, err)
	}

	// The workload puts a directory back. The pause lifts by itself and the
	// same sweep expires the run.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if !harness.logged("quarantine is lifted") {
		t.Fatalf("the pause never lifted: %v", harness.logs)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("the run was not expired once its name was a directory again: %v", err)
	}
	for _, record := range harness.manager.loadRecords() {
		if record.RunID == "run_stuck" {
			t.Fatalf("the expired run's record survived: %+v", record)
		}
	}
}

// TestATransientExpiryFailureIsNeverQuarantined: four unlucky sweeps must not
// end the seven-day sweep for a run. An attempt's own finalization triggers
// collection, so four failures can happen in minutes, and a busy, briefly
// unreadable or briefly unwritable filesystem is exactly the fault that clears
// on its own.
func TestATransientExpiryFailureIsNeverQuarantined(t *testing.T) {
	t.Run("a retryable failure never pauses the sweep", func(t *testing.T) {
		harness := newRetentionHarness(t, time.Hour)
		harness.retain("run_busy", true, true, map[string]int{"result.json": 16})
		harness.now = harness.now.Add(2 * time.Hour)
		record := requireRetentionRecord(t, harness.manager, "run_busy")

		// The failures a sweep really meets: EBUSY, EIO, a permission the node
		// regains. None of them is the one structural refusal repeating cannot
		// fix, so none of them may ever stop the sweep.
		for sweep := 1; sweep <= maxStructuralRefusals+2; sweep++ {
			harness.manager.noteRemovalFailure(record, fmt.Errorf("remove run_busy: %w", errors.New("device or resource busy")))
			record = requireRetentionRecord(t, harness.manager, "run_busy")
			if record.Quarantine != "" {
				t.Fatalf("a transient failure paused the sweep after %d of them: %+v", sweep, record)
			}
			if record.StructuralRefusals != 0 {
				t.Fatalf("a transient failure counted as structural: %+v", record)
			}
			if record.ExpiryFailures != sweep {
				t.Fatalf("after %d failures the record counts %d", sweep, record.ExpiryFailures)
			}
		}
		if !harness.logged("the next pass will try again") {
			t.Fatalf("a retryable failure was not reported as one: %v", harness.logs)
		}

		// And the sweep really does still act: the directory is there, so this
		// one expires it.
		if err := harness.manager.collect(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(filepath.Join(harness.root, "run_busy")); !os.IsNotExist(err) {
			t.Fatalf("the run was never expired: %v", err)
		}
	})

	t.Run("a directory the sweep cannot empty is expired once the fault clears", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("this fixture denies the sweep write permission, which root ignores")
		}
		harness := newRetentionHarness(t, time.Hour)
		path := harness.retain("run_locked", true, true, map[string]int{"result.json": 16})
		harness.now = harness.now.Add(2 * time.Hour)
		child := filepath.Join(path, "stubborn")
		if err := os.Mkdir(child, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o700) })

		for sweep := 1; sweep <= maxStructuralRefusals+2; sweep++ {
			if err := harness.manager.collect(); err != nil {
				t.Fatal(err)
			}
			record := requireRetentionRecord(t, harness.manager, "run_locked")
			if record.Quarantine != "" {
				t.Fatalf("a transient failure paused the sweep after %d sweeps: %+v", sweep, record)
			}
			if record.ExpiryFailures != sweep {
				t.Fatalf("after %d sweeps the record counts %d failures", sweep, record.ExpiryFailures)
			}
		}
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := harness.manager.collect(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("the run was not expired once the fault cleared: %v", err)
		}
	})
}

// TestAFailedSweepNeverOverwritesRefreshedTerminalFacts is the interleaving the
// sweep used to lose results to. Deletion fails, the sweep persists that
// failure from a snapshot it loaded before it held the lease, and a rerun that
// finished in between has its fresh deadline and verdict overwritten by the
// expired copy -- after which the next sweep deletes results a run retained
// seconds ago.
//
// The staging is deterministic rather than concurrent: the seam runs inside the
// sweep's failure handling, and what it does there is exactly what the losing
// interleaving does.
func TestAFailedSweepNeverOverwritesRefreshedTerminalFacts(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	plantSymlinkAtExpiredRun(t, harness, "run_raced")

	// What a rerun's finish would have written: a new window, a new verdict.
	refreshed := retentionRecord{
		RunID: "run_raced", NodeID: "node-1", Directory: filepath.Join(harness.root, "run_raced"),
		RetainedAt: harness.now, RetainUntil: harness.now.Add(time.Hour),
		Published: false, Succeeded: false,
	}
	var seamRan bool
	handoffSweepRace = func(stage string, loaded retentionRecord) {
		if stage != handoffSweepFailureRecorded {
			return
		}
		seamRan = true
		// The failure is being persisted while the sweep still holds this
		// record's path lease: a rerun could not have reached finish here.
		if lease := harness.manager.tryCollectLease(handoffPathLeaseKey(loaded.Directory)); lease != nil {
			lease.release()
			t.Error("the sweep recorded its failure without holding the record's path lease")
		}
		if err := harness.manager.writeRecord(refreshed); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { handoffSweepRace = nil })

	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if !seamRan {
		t.Fatal("the interleaving never happened; this test proves nothing")
	}
	record := requireRetentionRecord(t, harness.manager, "run_raced")
	if !record.RetainUntil.Equal(refreshed.RetainUntil) || record.Succeeded || record.Published {
		t.Fatalf("a failed sweep overwrote a rerun's terminal facts: %+v", record)
	}
	if record.ExpiryFailures != 0 || record.StructuralRefusals != 0 || record.Quarantine != "" {
		t.Fatalf("the sweep wrote its own bookkeeping over the newer record: %+v", record)
	}
	if !harness.logged("the sweep's copy is the older one") {
		t.Fatalf("the refusal to overwrite was silent: %v", harness.logs)
	}
}

// TestASweepActsOnTheRecordAsItIsUnderTheLease is the other half of the same
// interleaving. The sweep's candidate list is a snapshot taken before it holds
// anything; if an attempt finishes in that window and retains the run again,
// deciding from the snapshot deletes files a run retained seconds ago. The
// decision is taken from the record as it is under the lease instead.
func TestASweepActsOnTheRecordAsItIsUnderTheLease(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := harness.retain("run_rerun", true, true, map[string]int{"result.json": 16})
	harness.now = harness.now.Add(2 * time.Hour)

	// What a rerun that finished between loadRecords and the lease writes: a
	// window that has not closed yet.
	refreshed := retentionRecord{
		RunID: "run_rerun", NodeID: "node-1", Directory: path,
		RetainedAt: harness.now, RetainUntil: harness.now.Add(time.Hour),
		Published: true, Succeeded: true,
	}
	var seamRan bool
	handoffSweepRace = func(stage string, _ retentionRecord) {
		if stage != handoffSweepLeaseAcquired {
			return
		}
		seamRan = true
		if err := harness.manager.writeRecord(refreshed); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { handoffSweepRace = nil })

	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if !seamRan {
		t.Fatal("the interleaving never happened; this test proves nothing")
	}
	if _, err := os.Stat(filepath.Join(path, "result.json")); err != nil {
		t.Fatalf("the sweep deleted results a rerun had just retained: %v", err)
	}
	record := requireRetentionRecord(t, harness.manager, "run_rerun")
	if !record.RetainUntil.Equal(refreshed.RetainUntil) {
		t.Fatalf("the rerun's window was replaced: %+v", record)
	}
	if !harness.logged("the sweep acts on the newer record") {
		t.Fatalf("the sweep did not say it was re-reading: %v", harness.logs)
	}
}

// TestCancellingTheAgentInterruptsAStartupAccountingPass: the first pass runs
// inside Run, before anything else is serving, and it walks every retained run
// on the node. A pass that cannot hear Run's cancellation makes shutdown wait
// for a workload's directory tree before the node lock is released -- the same
// stall as the timer pass, arriving earlier.
func TestCancellingTheAgentInterruptsAStartupAccountingPass(t *testing.T) {
	root := filepath.Join(t.TempDir(), "handoffs")
	a := &Agent{
		clock:    systemClock{},
		handoffs: newHandoffManager(root, t.TempDir(), "node-1", time.Hour, nil),
	}
	ctx, cancel := context.WithCancel(t.Context())
	a.startResultCollector(ctx)
	t.Cleanup(a.stopResultCollector)
	if a.collectorContext == nil {
		t.Fatal("the collector published no context for the startup pass")
	}

	// A retained run with a tree deep enough that a pass over it is
	// interruptible rather than instantaneous.
	harness := &retentionHarness{t: t, root: root, now: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)}
	harness.manager = a.handoffs
	harness.manager.now = func() time.Time { return harness.now }
	harness.manager.logf = func(format string, args ...any) {
		harness.logs = append(harness.logs, fmt.Sprintf(format, args...))
	}
	harness.retain("run_deep_startup", true, true, map[string]int{"result.json": 16})
	buildComb(t, openRootAt(t, filepath.Join(root, "run_deep_startup")), 64, 0)

	var status RetainedResultsStatus
	harness.manager.observeAccounting = func(pass RetainedResultsStatus) { status = pass }
	// Cancel once the pass is under way, so this proves the walk hears the
	// cancellation rather than that it never started.
	handoffWalkDescended = func(depth int) {
		if depth >= 4 {
			cancel()
		}
	}
	t.Cleanup(func() { handoffWalkDescended = nil })

	if err := a.handoffs.accountNode(a.collectorContext); err != nil {
		t.Fatalf("a cancelled startup pass returned an error rather than stopping: %v", err)
	}
	if a.collectorContext.Err() == nil {
		t.Fatal("cancelling the agent did not reach the accounting pass's context")
	}
	if status.Truncated == 0 {
		t.Fatalf("a startup pass cancelled partway did not report itself truncated: %#v", status)
	}
}
