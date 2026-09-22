//go:build darwin || linux

package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// account runs one collection and returns what its accounting pass found,
// through the same hook the agent wires to its status projection.
func (h *retentionHarness) account() RetainedResultsStatus {
	h.t.Helper()
	var status RetainedResultsStatus
	h.manager.observeAccounting = func(pass RetainedResultsStatus) { status = pass }
	if err := h.manager.collect(); err != nil {
		h.t.Fatal(err)
	}
	h.manager.observeAccounting = nil
	return status
}

func (h *retentionHarness) record(runID string) retentionRecord {
	h.t.Helper()
	for _, record := range h.manager.loadRecords() {
		if record.RunID == runID {
			return record
		}
	}
	h.t.Fatalf("no retention record for run %s", runID)
	return retentionRecord{}
}

// TestTwoRunsHardLinkingOneFileAreChargedItsBytesOnce is the cross-run identity
// the node figure is built on. Part 1 measured one run at a time, so a file two
// runs share was counted once per run and a node that held one 4 MiB artifact
// reported eight.
func TestTwoRunsHardLinkingOneFileAreChargedItsBytesOnce(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	const payload = 4 << 20
	first := harness.retain("run_first", true, true, map[string]int{"result.json": 16, "big.bin": payload})
	second := harness.retain("run_second", true, true, map[string]int{"result.json": 16})
	if err := os.Link(filepath.Join(first, "big.bin"), filepath.Join(second, "big.bin")); err != nil {
		t.Fatalf("link one file into two runs: %v", err)
	}

	status := harness.account()
	if status.Runs != 2 {
		t.Fatalf("accounted %d runs, want 2", status.Runs)
	}
	if status.LogicalBytes < payload {
		t.Fatalf("the shared file's %d bytes are not counted at all: %d", payload, status.LogicalBytes)
	}
	if status.LogicalBytes >= 2*payload {
		t.Fatalf("the shared file is charged once per link: %d logical bytes for %d bytes of storage",
			status.LogicalBytes, payload)
	}
	if status.ChargedBytes >= 2*payload {
		t.Fatalf("the charged figure double-counts the shared file: %d", status.ChargedBytes)
	}
	// The second name costs the entry floor and nothing more: the data is
	// charged once, the directory slot holding it is charged every time.
	if status.ChargedBytes < status.LogicalBytes {
		t.Fatalf("charged %d bytes for %d logical bytes", status.ChargedBytes, status.LogicalBytes)
	}
	// Two runs, each with a directory, a marker, a result.json and the shared
	// file.
	if status.Entries != 8 {
		t.Fatalf("reached %d entries, want 8", status.Entries)
	}

	// The per-run bound's unit is unchanged: it trims names, so each run still
	// sees the whole file it can drop.
	for _, path := range []string{first, second} {
		run, err := harness.manager.openRun(filepath.Base(path))
		if err != nil {
			t.Fatal(err)
		}
		_, size, err := handoffEntries(run)
		run.Close()
		if err != nil {
			t.Fatal(err)
		}
		if size < payload {
			t.Fatalf("the per-run bound for %q stopped seeing the file it can trim: %d bytes", path, size)
		}
	}
}

// TestASwarmOfEmptyFilesIsChargedItsEntryFloor is the case logical bytes cannot
// see: a tree that holds almost nothing and costs the node an inode, a
// directory slot and a block per file.
func TestASwarmOfEmptyFilesIsChargedItsEntryFloor(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	const swarm = 3000
	path := harness.retain("run_swarm", true, true, map[string]int{"result.json": 16})
	for index := range swarm {
		name := filepath.Join(path, fmt.Sprintf("tiny-%05d", index))
		if err := os.WriteFile(name, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	status := harness.account()
	if status.LogicalBytes > 4<<10 {
		t.Fatalf("a swarm of empty files reported %d logical bytes; the point is that it reports almost none",
			status.LogicalBytes)
	}
	// 4 KiB is written out rather than taken from the constant: the number the
	// node is charged is the assertion, and a test that reads it back from the
	// code it is checking would pass whatever that code said.
	if want := int64(swarm) * 4096; status.ChargedBytes < want {
		t.Fatalf("charged %d bytes for %d entries, want at least %d", status.ChargedBytes, swarm, want)
	}
	// The directory, its marker, its result.json and the swarm.
	if want := int64(swarm + 3); status.Entries != want {
		t.Fatalf("reached %d entries, want %d", status.Entries, want)
	}
}

// TestACrashedRunsDirectoryIsAdoptedWithItsMarkersDeadline covers the residue a
// record written only at finish could never account for: the agent died between
// preparation and finish, so the directory is on the node and nothing names it.
func TestACrashedRunsDirectoryIsAdoptedWithItsMarkersDeadline(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	deadline := harness.now.Add(40 * time.Minute).UTC()
	crashed := harness.plantDirectory("run_crashed", &handoffMarker{
		RunID: "run_crashed", NodeID: "node-1", RetainUntil: deadline,
	})
	// A directory with no marker at all, and one whose marker names another
	// node. Neither is this agent's to claim.
	harness.plantDirectory("run_foreign", nil)
	harness.plantDirectory("run_elsewhere", &handoffMarker{
		RunID: "run_elsewhere", NodeID: "node-2", RetainUntil: deadline,
	})

	if err := harness.manager.adoptResidue(); err != nil {
		t.Fatal(err)
	}
	records := harness.manager.loadRecords()
	if len(records) != 1 {
		t.Fatalf("adoption wrote %d records, want only the marked one: %#v", len(records), records)
	}
	adopted := records[0]
	if adopted.RunID != "run_crashed" {
		t.Fatalf("adopted run %q", adopted.RunID)
	}
	if !adopted.RetainUntil.Equal(deadline) {
		t.Fatalf("adopted with deadline %s, want the marker's %s", adopted.RetainUntil, deadline)
	}
	if !adopted.Adopted {
		t.Fatal("the adopted record does not say the agent derived its window")
	}

	// The two it did not claim are still there, untouched, and counted.
	status := harness.account()
	if status.Unaccounted != 2 {
		t.Fatalf("reported %d unaccounted entries, want 2", status.Unaccounted)
	}
	for _, name := range []string{"run_foreign", "run_elsewhere"} {
		if _, err := os.Stat(filepath.Join(harness.root, name)); err != nil {
			t.Fatalf("a directory the agent does not claim was removed: %v", err)
		}
	}

	// Adoption put the run back inside the sweep: it expires on the marker's
	// deadline rather than sitting on the node for as long as it runs.
	harness.now = deadline.Add(time.Minute)
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(crashed); !os.IsNotExist(err) {
		t.Fatalf("the adopted run did not expire: %v", err)
	}
}

// TestAnAdoptedDeadlineCannotOutrunTheRetentionWindow: the marker is a file the
// workload can write, so adopting its deadline has to be safe against one it
// chose. The worst a forged marker may do is shorten its own run's retention.
func TestAnAdoptedDeadlineCannotOutrunTheRetentionWindow(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.plantDirectory("run_greedy", &handoffMarker{
		RunID: "run_greedy", NodeID: "node-1", RetainUntil: harness.now.Add(400 * time.Hour).UTC(),
	})
	if err := harness.manager.adoptResidue(); err != nil {
		t.Fatal(err)
	}
	adopted := harness.record("run_greedy")
	if want := harness.now.Add(time.Hour).UTC(); !adopted.RetainUntil.Equal(want) {
		t.Fatalf("a forged marker bought %s of retention; the window is %s", adopted.RetainUntil, want)
	}
}

// TestARecordIsWrittenAtPreparationAndCompletedAtFinish is the state change the
// whole slice rests on: an in-flight run is accounted for, and finishing
// updates that record rather than replacing it with a different one.
func TestARecordIsWrittenAtPreparationAndCompletedAtFinish(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := filepath.Join(harness.root, "run_live")
	spec := handoffClaim("run_live", path, nil).Job.Spec
	owner := prepareHandoffForTest(t, harness.manager, spec)
	admitted := harness.now.UTC()

	prepared := harness.record("run_live")
	if !prepared.AdmittedAt.Equal(admitted) {
		t.Fatalf("preparation recorded admission at %s, want %s", prepared.AdmittedAt, admitted)
	}
	if !prepared.RetainUntil.IsZero() || !prepared.RetainedAt.IsZero() {
		t.Fatalf("a run that has not finished carries a retention window: %#v", prepared)
	}

	// While it is running its bytes are the node's, and the pass says so.
	if err := os.WriteFile(filepath.Join(path, "working.bin"), make([]byte, 8<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	status := harness.account()
	if status.Runs != 1 || status.InFlight != 1 {
		t.Fatalf("accounted %d runs, %d in flight; want 1 and 1", status.Runs, status.InFlight)
	}
	if status.LogicalBytes < 8<<10 {
		t.Fatalf("an in-flight run's %d logical bytes are invisible to accounting", status.LogicalBytes)
	}

	harness.now = harness.now.Add(10 * time.Minute)
	if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
		t.Fatal(err)
	}
	finished := harness.record("run_live")
	if !finished.AdmittedAt.Equal(admitted) {
		t.Fatalf("finishing rewrote the run's admission to %s, want %s", finished.AdmittedAt, admitted)
	}
	if !finished.RetainedAt.Equal(harness.now.UTC()) ||
		!finished.RetainUntil.Equal(harness.now.Add(time.Hour).UTC()) {
		t.Fatalf("finishing did not write the terminal window: %#v", finished)
	}
	if !finished.Succeeded || !finished.Published || finished.Adopted {
		t.Fatalf("finishing did not write the terminal facts: %#v", finished)
	}

	// S1's revalidation still holds over the new earlier state: a sweep still
	// carrying the preparation record has the older copy and does not get to
	// write it back over the run's terminal facts.
	stale := prepared
	stale.Quarantine = handoffExpiryNameNotADirectory
	if harness.manager.rewriteRecord(prepared, stale) {
		t.Fatal("a sweep holding the preparation record rewrote the finished one")
	}
	if again := harness.record("run_live"); again.Quarantine != "" || !again.RetainUntil.Equal(finished.RetainUntil) {
		t.Fatalf("the stale rewrite landed anyway: %#v", again)
	}
}

// walkFrames measures one walk and returns the peak number of directory
// handles it held. The walker reports its own peak through a seam rather than
// the test sampling /dev/fd: a sample can miss the peak entirely, which made
// the previous assertion able to pass a walk that held thousands.
func walkFrames(t *testing.T, run *os.Root, name string) (int64, int) {
	t.Helper()
	peak := -1
	handoffWalkFramesObserved = func(observed int) { peak = observed }
	t.Cleanup(func() { handoffWalkFramesObserved = nil })
	info, err := run.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}
	size, err := measureEntry(run, name, info)
	if err != nil {
		t.Fatalf("measure %q: %v", name, err)
	}
	if peak < 0 {
		t.Fatal("the walk reported no peak frame count")
	}
	return size, peak
}

// TestMeasureEntryWalksADeepChainWithOneHandle: how deep a retained tree goes is
// the workload's decision, and the measurement used to recurse once per level
// and hold that level's directory open until the whole subtree returned. Node
// accounting walks every run on the node, so an adversarial tree stopped being
// one run's problem.
//
// A chain is the easy half: every level's last subdirectory is also its only
// one, so the parent is released as the child is opened and the whole descent
// costs one handle.
func TestMeasureEntryWalksADeepChainWithOneHandle(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	depth := deepTreeDepth()
	const payload = 4096
	path := harness.plantDirectory("run_deep", nil)
	run, err := harness.manager.openRun("run_deep")
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	buildDeepChain(t, path, depth, payload)

	size, peak := walkFrames(t, run, "deep")
	if size != payload {
		t.Fatalf("a %d-deep chain measured %d bytes, want %d", depth, size, payload)
	}
	if peak != 1 {
		t.Fatalf("a %d-deep chain held %d directory handles at once, want 1", depth, peak)
	}
}

// TestMeasureEntryWalksACombWithinItsFrameBound is the shape the chain
// optimization alone does not survive. Give every level one child that
// continues downward and one empty sibling visited after it, and every ancestor
// stays open with a pending child for the whole descent: 2N directories pinned
// N descriptors, and a node out of descriptors stops measuring and stops
// serving.
func TestMeasureEntryWalksACombWithinItsFrameBound(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	// Written out rather than derived from maxOpenWalkFrames: the bound is what
	// is being asserted, and a fixture and an assertion that both move with the
	// constant would pass whatever the constant said. 256 teeth are well past
	// the bound of 64, and 96 handles is a ceiling a walk holding one per level
	// cannot slip under.
	const teeth = 256
	const frameCeiling = 96
	const payload = 2048
	path := harness.plantDirectory("run_comb", nil)
	run, err := harness.manager.openRun("run_comb")
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	buildComb(t, path, teeth, payload)

	size, peak := walkFrames(t, run, "comb")
	if want := int64(teeth * payload); size != want {
		t.Fatalf("a comb of %d teeth measured %d bytes, want %d", teeth, size, want)
	}
	if peak > frameCeiling {
		t.Fatalf("a comb of %d teeth held %d directory handles at once, past the ceiling of %d",
			teeth, peak, frameCeiling)
	}
}

// TestACombSubtreeReplacedMidWalkIsCountedRatherThanMeasured: a released
// ancestor is re-opened by name, and a workload owns these directories. The
// walk accepts the re-open only if it is the same inode, and what it gives up
// is reported rather than quietly left out of the node's figures.
func TestACombSubtreeReplacedMidWalkIsCountedRatherThanMeasured(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	const teeth = 192
	const releasedBelow = 96
	path := harness.plantDirectory("run_swapped", nil)
	run, err := harness.manager.openRun("run_swapped")
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	buildComb(t, path, teeth, 0)

	// Replace the shallowest released ancestor with a different directory
	// while the walk is below it. The walk is deterministic, so the swap is
	// staged from the seam at the moment the frame bound has released it.
	combRoot := filepath.Join(path, "comb")
	swapped := false
	handoffWalkDescended = func(depth int) {
		if swapped || depth <= releasedBelow {
			return
		}
		swapped = true
		// The walk has released this ancestor's handle by now and can only
		// reach it again by name. Move the real one aside and leave a
		// different directory at the name it will re-open.
		victim := filepath.Join(combRoot, "down")
		if err := os.Rename(victim, filepath.Join(path, "elsewhere")); err != nil {
			t.Error(err)
			return
		}
		if err := os.Mkdir(victim, 0o700); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { handoffWalkDescended = nil })
	info, err := run.Lstat("comb")
	if err != nil {
		t.Fatal(err)
	}
	tally, err := walkHandoffTree(run, "comb", info, nil)
	if err != nil {
		t.Fatalf("walk a comb whose ancestor was replaced: %v", err)
	}
	if !swapped {
		t.Fatal("the fixture never got deep enough to release an ancestor")
	}
	if tally.replaced == 0 {
		t.Fatalf("the walk re-opened a name that leads somewhere else and measured through it: %#v", tally)
	}
	if tally.unaccounted < tally.replaced {
		t.Fatalf("a replaced subtree was not counted as unaccounted: %#v", tally)
	}
}

// deepTreeDepth is how deep the chain fixture goes, and it is not the same
// number on both platforms.
//
// On Linux each directory costs a constant handful of syscalls, so the depth is
// the one worth asserting: far past any per-level frame. On macOS the kernel
// does work proportional to a path's depth on every operation at that depth, so
// building and removing the fixture is superlinear -- ten thousand levels is
// minutes of system time, and a unit lane that takes minutes is a unit lane
// people stop running. The frame bound is asserted on the walker's own count,
// which is exact at either depth.
func deepTreeDepth() int {
	if runtime.GOOS == "linux" {
		return 12_000
	}
	return 600
}

// plantDirectory creates one directory under the handoff root the way residue
// exists on a node: as files, with no agent record behind them. A nil marker
// plants none, which is a directory the agent must never claim.
func (h *retentionHarness) plantDirectory(runID string, marker *handoffMarker) string {
	h.t.Helper()
	path := filepath.Join(h.root, runID)
	if err := os.MkdirAll(path, 0o700); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "result.json"), make([]byte, 32), 0o600); err != nil {
		h.t.Fatal(err)
	}
	if marker == nil {
		return path
	}
	run, err := h.manager.openRun(runID)
	if err != nil {
		h.t.Fatal(err)
	}
	defer run.Close()
	if err := writeHandoffMarker(run, *marker); err != nil {
		h.t.Fatal(err)
	}
	return path
}

// buildDeepChain builds a chain of directories too deep to reach by pathname,
// descending through one handle at a time so building the fixture costs no more
// descriptors than measuring it should.
func buildDeepChain(t *testing.T, path string, depth, payload int) {
	t.Helper()
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { root.Close() }()
	if err := root.Mkdir("deep", 0o700); err != nil {
		t.Fatal(err)
	}
	current, err := root.OpenRoot("deep")
	if err != nil {
		t.Fatal(err)
	}
	for range depth {
		if err := current.Mkdir("d", 0o700); err != nil {
			current.Close()
			t.Fatal(err)
		}
		next, err := current.OpenRoot("d")
		current.Close()
		if err != nil {
			t.Fatal(err)
		}
		current = next
	}
	file, err := current.Create("leaf.bin")
	if err != nil {
		current.Close()
		t.Fatal(err)
	}
	_, err = file.Write(make([]byte, payload))
	file.Close()
	current.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// buildComb builds the shape a chain optimization alone does not survive: every
// level holds the directory that continues downward and, visited after it, an
// empty sibling that keeps the level pending for the whole descent. Each level
// also holds one file, so the measurement has something to be right about.
func buildComb(t *testing.T, path string, teeth, payload int) {
	t.Helper()
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.Mkdir("comb", 0o700); err != nil {
		t.Fatal(err)
	}
	current, err := root.OpenRoot("comb")
	if err != nil {
		t.Fatal(err)
	}
	for range teeth {
		// "down" sorts before "sibling", so the descent happens first and the
		// sibling stays pending behind it -- which is the whole attack.
		if err := current.Mkdir("down", 0o700); err != nil {
			current.Close()
			t.Fatal(err)
		}
		if err := current.Mkdir("sibling", 0o700); err != nil {
			current.Close()
			t.Fatal(err)
		}
		if payload > 0 {
			file, err := current.Create("tooth.bin")
			if err != nil {
				current.Close()
				t.Fatal(err)
			}
			_, err = file.Write(make([]byte, payload))
			file.Close()
			if err != nil {
				current.Close()
				t.Fatal(err)
			}
		}
		next, err := current.OpenRoot("down")
		current.Close()
		if err != nil {
			t.Fatal(err)
		}
		current = next
	}
	current.Close()
}

// admitAndAbandon leaves the node in the state a crash leaves it in: an
// admission record written at preparation, its lease released, and nothing that
// ever finished the run.
func (h *retentionHarness) admitAndAbandon(runID string) string {
	h.t.Helper()
	path := filepath.Join(h.root, runID)
	spec := handoffClaim(runID, path, nil).Job.Spec
	owner := prepareHandoffForTest(h.t, h.manager, spec)
	if err := os.WriteFile(filepath.Join(path, "working.bin"), make([]byte, 2048), 0o600); err != nil {
		h.t.Fatal(err)
	}
	owner.lease.release()
	return path
}

// TestAnAdmissionWhoseDirectoryIsGoneLosesItsRecord: reconciling directory
// entries rather than records left this state permanently "in flight" -- no
// entry to walk, so nothing to reconcile, and the sweep skips a record with no
// deadline. The record then outlived everything it described.
func TestAnAdmissionWhoseDirectoryIsGoneLosesItsRecord(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	path := harness.admitAndAbandon("run_vanished")
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}

	if err := harness.manager.adoptResidue(); err != nil {
		t.Fatal(err)
	}
	if records := harness.manager.loadRecords(); len(records) != 0 {
		t.Fatalf("the record of a run whose directory is gone survived: %#v", records)
	}
	if !harness.logged("is gone; its record is removed") {
		t.Fatalf("the removal was silent: %v", harness.logs)
	}
}

// TestAnAdmissionWhoseNameIsASymlinkIsQuarantinedAndRecovers: the other state
// startup used to skip. A workload that replaces its own handoff name with a
// symlink before the node restarts used to buy its record permanent residence,
// because a record with no deadline is invisible to the sweep. It now gets the
// deadline it was admitted for and enters S1's reversible quarantine.
func TestAnAdmissionWhoseNameIsASymlinkIsQuarantinedAndRecovers(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	admitted := harness.now.UTC()
	path := harness.admitAndAbandon("run_swapped_name")
	elsewhere := t.TempDir()
	if err := os.WriteFile(filepath.Join(elsewhere, "not-ours"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, path); err != nil {
		t.Fatal(err)
	}

	if err := harness.manager.adoptResidue(); err != nil {
		t.Fatal(err)
	}
	record := harness.record("run_swapped_name")
	if want := admitted.Add(time.Hour); !record.RetainUntil.Equal(want) {
		t.Fatalf("the quarantined admission expires at %s, want the window it was admitted for (%s)",
			record.RetainUntil, want)
	}
	if record.Quarantine != handoffExpiryNameNotADirectory {
		t.Fatalf("the admission did not reach the structural quarantine: %#v", record)
	}

	// Past the deadline, the sweep still refuses to follow the link, and what
	// it points at is untouched.
	harness.now = record.RetainUntil.Add(time.Minute)
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("the sweep removed a name it refuses to follow: %v", err)
	}
	if _, err := os.Stat(filepath.Join(elsewhere, "not-ours")); err != nil {
		t.Fatalf("the sweep followed the link into the node: %v", err)
	}

	// A directory back at the name lifts the quarantine by itself, with no
	// restart and no operator, and the run expires normally.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the recovered run did not expire: %v", err)
	}
	if records := harness.manager.loadRecords(); len(records) != 0 {
		t.Fatalf("the expired record survived: %#v", records)
	}
}

// TestAnAdmissionWhoseDirectoryRemainsGetsItsAdmittedDeadline is the third
// crash state, and the one that already worked. It is asserted beside the other
// two so the three are read together.
func TestAnAdmissionWhoseDirectoryRemainsGetsItsAdmittedDeadline(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	admitted := harness.now.UTC()
	harness.admitAndAbandon("run_interrupted")

	harness.now = harness.now.Add(20 * time.Minute)
	if err := harness.manager.adoptResidue(); err != nil {
		t.Fatal(err)
	}
	record := harness.record("run_interrupted")
	if want := admitted.Add(time.Hour); !record.RetainUntil.Equal(want) {
		t.Fatalf("the interrupted run expires at %s, want the window it was admitted for (%s)",
			record.RetainUntil, want)
	}
	if !record.AdmittedAt.Equal(admitted) || !record.Adopted {
		t.Fatalf("reconciliation did not keep the run's admission or mark the window derived: %#v", record)
	}
	if record.Quarantine != "" {
		t.Fatalf("a directory that is still there was quarantined: %#v", record)
	}
}

// TestTwoRunNamesThatOnceSharedARecordFileNoLongerDo: "run.live" and "run_live"
// are both valid run IDs, and the old mapping folded both onto run_live.json.
// One run's record standing in for another's is one run holding another's
// expiry.
func TestTwoRunNamesThatOnceSharedARecordFileNoLongerDo(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	harness.retain("run.live", true, true, map[string]int{"result.json": 16})
	harness.retain("run_live", true, true, map[string]int{"result.json": 16})

	if first, second := recordComponent("run.live"), recordComponent("run_live"); first == second {
		t.Fatalf("both runs are still filed as %q", first)
	}
	records := harness.manager.loadRecords()
	if len(records) != 2 {
		t.Fatalf("two runs produced %d records: %#v", len(records), records)
	}
	for _, record := range records {
		if record.Directory != filepath.Join(harness.root, record.RunID) {
			t.Fatalf("record for run %q names %q", record.RunID, record.Directory)
		}
	}
}

// TestTwoRunNamesSharingTheirFirstBytesNoLongerShareARecordFile: the old
// mapping cut at 96 bytes, and a run ID may be 128.
func TestTwoRunNamesSharingTheirFirstBytesNoLongerShareARecordFile(t *testing.T) {
	prefix := strings.Repeat("a", 120)
	first, second := prefix+"-one", prefix+"-two"
	if !validRunMailboxSegment(first) || !validRunMailboxSegment(second) {
		t.Fatalf("the fixture run IDs are not valid run names")
	}
	if legacyRecordComponent(first) != legacyRecordComponent(second) {
		t.Fatal("the fixture does not reproduce the collision it is here for")
	}
	if recordComponent(first) == recordComponent(second) {
		t.Fatalf("two runs sharing their first 96 bytes are still filed as %q", recordComponent(first))
	}
	if length := len(recordComponent(first)); length > maxRecordComponentBytes+len(".json") {
		t.Fatalf("the record name is %d bytes, past the bound", length)
	}
}

// TestAdoptionNeverWritesOverARecordItDoesNotOwn: adoption is the one path that
// writes a record for a directory it did not prepare, on evidence a workload
// can write. It creates; it never replaces.
func TestAdoptionNeverWritesOverARecordItDoesNotOwn(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		planted  string
		expected string
	}{
		{
			name:     "a record that names another run",
			planted:  `{"run_id":"run_other","node_id":"node-1","directory":"/elsewhere/run_other","retained_at":"2026-09-17T11:00:00Z","retain_until":"2026-09-17T12:30:00Z"}`,
			expected: "already holds run",
		},
		{
			name:     "a record that cannot be read",
			planted:  "{not json",
			expected: "could not be read",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newRetentionHarness(t, time.Hour)
			path := harness.plantDirectory("run_planted", &handoffMarker{
				RunID: "run_planted", NodeID: "node-1", RetainUntil: harness.now.Add(time.Hour).UTC(),
			})
			file := harness.manager.recordPath("run_planted")
			if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, []byte(testCase.planted), 0o600); err != nil {
				t.Fatal(err)
			}

			if err := harness.manager.adoptResidue(); err != nil {
				t.Fatal(err)
			}
			payload, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if string(payload) != testCase.planted {
				t.Fatalf("adoption replaced a record it does not own: %s", payload)
			}
			if !harness.logged(testCase.expected) {
				t.Fatalf("the refusal was silent: %v", harness.logs)
			}
			// The directory itself is untouched, and the pass reports it.
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("a directory adoption refused was removed: %v", err)
			}
			if status := harness.account(); status.Unaccounted == 0 {
				t.Fatalf("a directory adoption refused is not reported as unaccounted: %#v", status)
			}
		})
	}
}

// TestTheAccountingPassReachesTheStatusTheNodeDoctorReads: the measurement
// existed and the only place it reached was the agent log. This is the seam
// between the collector and Agent.RetainedResults, which is what the node
// doctor asks for.
func TestTheAccountingPassReachesTheStatusTheNodeDoctorReads(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	observer := newLifecycleObserver(systemClock{})
	if _, measured := observer.retainedResultsSnapshot(); measured {
		t.Fatal("a node that has not measured reported a measurement")
	}
	harness.manager.observeAccounting = observer.recordRetainedResults
	harness.retain("run_reported", true, true, map[string]int{"result.json": 16, "payload.bin": 64 << 10})
	harness.plantDirectory("run_not_ours", nil)
	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}

	status, measured := observer.retainedResultsSnapshot()
	if !measured {
		t.Fatal("a completed accounting pass did not reach the status the doctor reads")
	}
	if status.Runs != 1 || status.LogicalBytes < 64<<10 || status.ChargedBytes < status.LogicalBytes {
		t.Fatalf("the status lost the pass's figures: %#v", status)
	}
	if status.Unaccounted != 1 {
		t.Fatalf("the directory this agent does not own is not reported: %#v", status)
	}
	if status.MeasuredAt.IsZero() {
		t.Fatalf("the status does not say when it was measured: %#v", status)
	}
	// Status() carries the same figures, so a reader of either surface sees one
	// answer rather than two.
	projected := observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).RetainedResults
	if projected == nil || *projected != status {
		t.Fatalf("the lifecycle projection and the doctor accessor disagree: %#v vs %#v", projected, status)
	}
}
