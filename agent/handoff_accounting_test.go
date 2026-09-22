//go:build darwin || linux

package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
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

// TestMeasureEntryWalksADeepTreeWithoutRecursing: how deep a retained tree goes
// is the workload's decision, and the measurement used to recurse once per
// level and hold that level's directory open until the whole subtree returned.
// Node accounting walks every run on the node, so an adversarial tree stops
// being one run's problem.
//
// Two things are asserted, because the recursion had two costs. The depth is
// past what a per-level frame and a per-level allocation survive comfortably;
// the descriptor sampling is the other half, and it is the sharper one -- a
// walk that keeps every level open runs a node out of descriptors long before
// it runs out of stack. This walk releases a directory as soon as its last
// subdirectory is opened, so a chain costs one.
func TestMeasureEntryWalksADeepTreeWithoutRecursing(t *testing.T) {
	harness := newRetentionHarness(t, time.Hour)
	depth := deepTreeDepth()
	const payload = 4096
	path := harness.plantDirectory("run_deep", nil)
	run, err := harness.manager.openRun("run_deep")
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()
	buildDeepTree(t, path, depth, payload)

	var peak atomic.Int64
	baseline := openDescriptors(t)
	stop := make(chan struct{})
	sampled := make(chan int)
	go func() {
		samples := 0
		for {
			select {
			case <-stop:
				sampled <- samples
				return
			case <-time.After(time.Millisecond):
			}
			if count := countDescriptors(); count > 0 {
				samples++
				if delta := int64(count - baseline); delta > peak.Load() {
					peak.Store(delta)
				}
			}
		}
	}()

	info, err := run.Lstat("deep")
	if err != nil {
		t.Fatal(err)
	}
	size, err := measureEntry(run, "deep", info)
	close(stop)
	samples := <-sampled
	if err != nil {
		t.Fatalf("measure a %d-deep tree: %v", depth, err)
	}
	if size != payload {
		t.Fatalf("a %d-deep tree measured %d bytes, want %d", depth, size, payload)
	}
	// A walk holding one descriptor per level would need one per level. The
	// bound is generous on purpose: this fails on the shape, not on a number.
	// Too few samples means the walk outran the sampler, which is a reason to
	// claim nothing rather than to fail.
	if samples >= 8 && peak.Load() > 64 {
		t.Fatalf("the walk held %d descriptors over its baseline across a %d-deep tree (%d samples)",
			peak.Load(), depth, samples)
	}
}

// deepTreeDepth is how deep the fixture above goes, and it is not the same
// number on both platforms.
//
// On Linux each directory costs a constant handful of syscalls, so the depth is
// the one worth asserting: far past any per-level frame. On macOS the kernel
// does work proportional to a path's depth on every operation *at* that depth,
// so building and removing the fixture is superlinear -- ten thousand levels is
// minutes of system time, and a unit lane that takes minutes is a unit lane
// people stop running. The shape being proved is identical at either depth: one
// explicit stack, and a parent released as its last child is opened. Six
// hundred levels is already an order of magnitude past the descriptor bound
// this test fails on.
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

// buildDeepTree builds a chain of directories too deep to reach by pathname,
// descending through one handle at a time so building the fixture costs no more
// descriptors than measuring it should.
func buildDeepTree(t *testing.T, path string, depth, payload int) {
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

func descriptorDirectory() string {
	if runtime.GOOS == "linux" {
		return "/proc/self/fd"
	}
	return "/dev/fd"
}

// countDescriptors reads names only. Stating the entries of the descriptor
// directory fails on macOS -- one of the descriptors it lists is the one being
// read with -- and the names are the whole answer anyway.
func countDescriptors() int {
	directory, err := os.Open(descriptorDirectory())
	if err != nil {
		return 0
	}
	names, err := directory.Readdirnames(-1)
	directory.Close()
	if err != nil && len(names) == 0 {
		return 0
	}
	return len(names)
}

func openDescriptors(t *testing.T) int {
	t.Helper()
	count := countDescriptors()
	if count == 0 {
		t.Skipf("this platform does not expose %s", descriptorDirectory())
	}
	return count
}
