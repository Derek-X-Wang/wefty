//go:build linux

package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// These tests need no containerd and no Docker: a temp directory is the whole
// fixture, because everything under test here is the helper's own filesystem
// bookkeeping. They are linux-tagged only because the engine they exercise is.

func handoffRetentionEngine(t *testing.T, root string, retention time.Duration, now time.Time) *ContainerdEngine {
	t.Helper()
	return &ContainerdEngine{config: NativeEngineConfig{
		RuntimeRoot: root, HandoffRetention: retention, Clock: fixedHandoffClock{at: now},
	}, attempts: map[string]*containerdAttempt{}}
}

type fixedHandoffClock struct{ at time.Time }

func (clock fixedHandoffClock) Now() time.Time { return clock.at }
func (clock fixedHandoffClock) NewTimerAt(time.Time) Timer {
	return systemClock{}.NewTimerAt(clock.at)
}

// quiescent is the post-reap inventory the sweep hands the handoff pass: every
// task and container gone. Anything else means the node has not proved its
// previous workloads stopped.
func quiescent() ResourceInventory { return ResourceInventory{} }

func makeHandoffVolume(t *testing.T, root, ownerKey string) (string, string) {
	t.Helper()
	name, err := DeterministicHandoffVolumeDirectory(ownerKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "handoffs", name)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return name, path
}

func readHandoffReceipt(t *testing.T, root, name string) handoffRetentionReceipt {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join(root, handoffRetentionStateDirectory, HandoffRetentionRecordName(name)))
	if err != nil {
		t.Fatal(err)
	}
	var receipt handoffRetentionReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func receiptPresent(t *testing.T, root, name string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(root, handoffRetentionStateDirectory, HandoffRetentionRecordName(name)))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

// stamp writes one volume's terminal receipt the way finalization does.
func stamp(t *testing.T, engine *ContainerdEngine, name string, terminalAt time.Time) {
	t.Helper()
	if err := engine.writeHandoffRetentionReceipt(t.Context(), name, terminalAt); err != nil {
		t.Fatal(err)
	}
}

// expire runs the expiry half of the sweep's handoff pass.
func expire(t *testing.T, engine *ContainerdEngine, now time.Time) {
	t.Helper()
	names, err := engine.handoffVolumeNames()
	if err != nil {
		t.Fatal(err)
	}
	live, err := engine.liveHandoffVolumes()
	if err != nil {
		t.Fatal(err)
	}
	engine.cleanupExpiredHandoffs(now, names, live)
}

// A handoff volume is mounted read-write into the container and a uid-0
// workload owns it, so its mtime is a number the workload writes. Once the
// helper has stamped its own terminal time, moving that mtime -- in either
// direction -- must change nothing about when the volume expires.
func TestHandoffExpiryRunsFromTheHelpersReceiptAndNotTheWorkloadsMtime(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "run-owner")
	stamp(t, engine, name, now.Add(-30*time.Minute))

	// The workload backdates its own directory by two hours, well past the
	// window. The receipt says thirty minutes, and the receipt is what counts.
	stale := now.Add(-2 * time.Hour)
	if err := os.Chtimes(path, stale, stale); err != nil {
		t.Fatal(err)
	}
	expire(t, engine, now)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a moved mtime expired a handoff volume whose receipt was inside the window: %v", err)
	}

	// The same volume forward-dated to the future must not be rescued either,
	// once its own receipt has run out.
	stamp(t, engine, name, now.Add(-2*time.Hour))
	fresh := now.Add(time.Hour)
	if err := os.Chtimes(path, fresh, fresh); err != nil {
		t.Fatal(err)
	}
	expire(t, engine, now)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a forward-dated mtime kept a handoff volume past its receipt: %v", err)
	}
	if receiptPresent(t, root, name) {
		t.Fatal("the receipt outlived the volume it is bound to")
	}
}

// With no receipt there is no helper-owned timestamp, and the mtime is the
// workload's. Expiring on it is exactly what #494 closes, so a volume without
// one is reported with its age labelled and is never removed on it.
func TestAHandoffVolumeWithNoReceiptIsReportedAndNeverExpiredOnItsMtime(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "run-owner")
	stale := now.Add(-2 * time.Hour)
	if err := os.Chtimes(path, stale, stale); err != nil {
		t.Fatal(err)
	}
	expire(t, engine, now)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a handoff volume was expired on a timestamp its workload could have written: %v", err)
	}
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Volumes) != 1 || inventory.Volumes[0].Name != name {
		t.Fatalf("handoff inventory = %+v", inventory.Volumes)
	}
	volume := inventory.Volumes[0]
	if volume.TerminalKnown {
		t.Fatal("a volume with no receipt reported a helper-owned terminal time")
	}
	if volume.TerminalAt.Unix() != stale.Unix() {
		t.Fatalf("the labelled mtime fallback = %v, want the directory's own %v", volume.TerminalAt, stale)
	}
	if !slices.Contains(volume.Anomalies, HandoffAnomalyNoReceipt) {
		t.Fatalf("a receiptless volume carried no labelling anomaly: %v", volume.Anomalies)
	}
}

// The boot sweep is what turns crash residue into something the node can give
// back: a volume with no receipt cannot be expired at all, so without this it
// would sit on the node forever.
func TestTheSweepStampsATerminalTimeOnAHandoffVolumeThatHasNone(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "crash-residue")
	if err := os.WriteFile(filepath.Join(root, "handoffs", name, "result.json"), []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	receipt := readHandoffReceipt(t, root, name)
	if receipt.Version != handoffRetentionReceiptVersion || !receipt.TerminalAt.Equal(now) {
		t.Fatalf("swept receipt = %+v, want version 1 stamped at %v", receipt, now)
	}
	if receipt.LogicalBytes != 11 || receipt.Entries != 1 || receipt.Truncated {
		t.Fatalf("swept receipt measured %d bytes over %d entries (truncated=%v), want 11 over 1 whole", receipt.LogicalBytes, receipt.Entries, receipt.Truncated)
	}

	// Stamping is create-only: a second sweep must not re-date a window that
	// has already started, or a volume would never reach its deadline.
	engine.reconcileHandoffRetention(t.Context(), now.Add(30*time.Minute), quiescent())
	if again := readHandoffReceipt(t, root, name); !again.TerminalAt.Equal(now) {
		t.Fatalf("a later sweep re-dated an existing receipt to %v", again.TerminalAt)
	}
	engine.reconcileHandoffRetention(t.Context(), now.Add(2*time.Hour), quiescent())
	if _, err := os.Stat(filepath.Join(root, "handoffs", name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a swept-and-stamped volume never reached its deadline: %v", err)
	}
}

// A live attempt is still writing into its handoff volume. Stamping a terminal
// time on it would start the retention window while the run is producing the
// results the window exists to keep.
func TestTheSweepDoesNotStampAVolumeALiveAttemptIsStillWriting(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "live-run")
	engine.attempts["live"] = &containerdAttempt{resources: ResourceIdentity{HandoffVolumeDirectory: name}}
	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	if receiptPresent(t, root, name) {
		t.Fatal("a live attempt's handoff volume was stamped terminal")
	}
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Volumes) != 1 || !inventory.Volumes[0].Live {
		t.Fatalf("a live attempt's volume did not report live: %+v", inventory.Volumes)
	}
}

// The in-memory attempts map is empty in a freshly constructed engine, so a
// helper that restarted over a still-running workload would have called every
// volume idle. What survives a restart is the durable attempt-ownership
// record, and it carries the owner-key-derived handoff volume name precisely
// because the boot sweep cannot re-derive it.
func TestAHelperThatRestartedOverARunningWorkloadDoesNotStampItsVolume(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "survivor")
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "prior-boot", JobID: "survivor", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassOneShot, RemovalGeneration: "1",
	}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	resources.HandoffVolumeDirectory = name
	if err := os.MkdirAll(engine.attemptOwnershipRoot(), 0o700); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(durableAttemptOwnership{
		Version: durableAttemptOwnershipVersion, Authority: authority, Resources: resources,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(engine.attemptOwnershipPath(resources), payload, 0o600); err != nil {
		t.Fatal(err)
	}

	// Nothing is in the in-memory map: this is the state a restarted helper is
	// actually in.
	if len(engine.attempts) != 0 {
		t.Fatal("the fixture is not the restart case")
	}
	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	if receiptPresent(t, root, name) {
		t.Fatal("a restarted helper stamped a terminal time on a volume whose attempt it had not proved stopped")
	}
	live, err := engine.liveHandoffVolumes()
	if err != nil {
		t.Fatal(err)
	}
	if _, writing := live[name]; !writing {
		t.Fatalf("the durable ownership record did not make %s live: %v", name, live)
	}
}

// Reaping surviving tasks is mandatory and comes first. A terminal time taken
// before that proof is a lie the node then acts on for seven days.
func TestNothingIsStampedOrExpiredWhileRuntimeResourcesSurviveTheSweep(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	fresh, _ := makeHandoffVolume(t, root, "no-receipt-yet")
	expired, expiredPath := makeHandoffVolume(t, root, "long-finished")
	stamp(t, engine, expired, now.Add(-2*time.Hour))

	engine.reconcileHandoffRetention(t.Context(), now, ResourceInventory{Tasks: []string{"wefty-container-" + strings.Repeat("a", 32)}})
	if receiptPresent(t, root, fresh) {
		t.Fatal("a volume was stamped while a task survived the sweep")
	}
	if _, err := os.Stat(expiredPath); err != nil {
		t.Fatalf("an expired volume was removed while a task survived the sweep: %v", err)
	}

	// Once the reap has proved quiescence, the same pass does both.
	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	if !receiptPresent(t, root, fresh) {
		t.Fatal("a quiescent node did not stamp its receiptless volume")
	}
	if _, err := os.Stat(expiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a quiescent node did not remove its expired volume: %v", err)
	}
}

// A node upgraded with receiptless volumes on a full filesystem used to fail on
// the first temporary receipt and abort the whole pass -- before freeing the
// already-expired volumes that would have made room -- and then do it again on
// the next boot.
func TestOneFailedReceiptDoesNotStopTheRestOfTheNodeBeingSweptOrStamped(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	blocked, _ := makeHandoffVolume(t, root, "aaa-cannot-be-stamped")
	stampable, _ := makeHandoffVolume(t, root, "zzz-can-be-stamped")
	expired, expiredPath := makeHandoffVolume(t, root, "mmm-already-expired")
	stamp(t, engine, expired, now.Add(-2*time.Hour))

	// A non-empty directory standing where one volume's receipt goes makes the
	// publishing rename fail for that volume and no other. It sorts first, so
	// the failure happens before the rest of the pass.
	blockedPath := filepath.Join(engine.handoffRetentionRoot(), HandoffRetentionRecordName(blocked))
	if err := os.MkdirAll(filepath.Join(blockedPath, "occupied"), 0o700); err != nil {
		t.Fatal(err)
	}

	engine.reconcileHandoffRetention(t.Context(), now, quiescent())

	if !receiptPresent(t, root, stampable) {
		t.Fatal("one volume's failed receipt stopped a later volume being stamped")
	}
	if _, err := os.Stat(expiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("one volume's failed receipt stopped an expired volume being removed: %v", err)
	}
	// The volume that could not be stamped keeps its files and stays
	// non-expirable, and the next pass tries again.
	if _, err := os.Stat(filepath.Join(root, "handoffs", blocked)); err != nil {
		t.Fatalf("a volume whose receipt could not be written lost its files: %v", err)
	}
	if err := os.RemoveAll(blockedPath); err != nil {
		t.Fatal(err)
	}
	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	if !receiptPresent(t, root, blocked) {
		t.Fatal("a later sweep did not repair the receipt an earlier one could not write")
	}
}

// The descriptor is authority and the record is evidence. A receipt naming a
// directory that is no longer the one standing at this name says nothing about
// the bytes that are there now, so it is refused rather than believed.
func TestAReceiptBoundToAReplacedDirectoryIsRefused(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "replaced")
	stamp(t, engine, name, now.Add(-2*time.Hour))

	// The workload replaces its own directory with another: same name, a
	// different inode, and files the expired receipt knows nothing about. The
	// replacement is created while the original still stands, so the two
	// cannot share an inode number the filesystem has just freed.
	replacement := filepath.Join(root, "replacement")
	if err := os.MkdirAll(replacement, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	fact, err := engine.readHandoffRetentionFact(name)
	if err != nil {
		t.Fatal(err)
	}
	if fact.terminalKnown {
		t.Fatal("a receipt bound to a replaced inode was accepted as the terminal time")
	}
	if fact.anomaly != HandoffAnomalyReceiptMismatched {
		t.Fatalf("a mismatched receipt carried anomaly %q", fact.anomaly)
	}
	expire(t, engine, now)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a replaced directory was expired on a receipt that is not its own: %v", err)
	}
}

// One unreadable receipt must not fail a node-wide call. There is no new error
// code for this: it is a per-volume anomaly, the ComputerDiskAnomalies
// precedent, and every other volume is still reported.
func TestAnUnreadableReceiptIsAPerVolumeAnomalyRatherThanAFailedCall(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	broken, brokenPath := makeHandoffVolume(t, root, "broken-receipt")
	sound, soundPath := makeHandoffVolume(t, root, "sound-receipt")
	stamp(t, engine, sound, now.Add(-2*time.Hour))
	if err := os.WriteFile(engine.handoffRetentionPath(broken), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatalf("one unreadable receipt failed the whole node-wide call: %v", err)
	}
	if len(inventory.Volumes) != 2 {
		t.Fatalf("handoff inventory = %+v, want both volumes", inventory.Volumes)
	}
	for _, volume := range inventory.Volumes {
		switch volume.Name {
		case broken:
			if volume.TerminalKnown || !slices.Contains(volume.Anomalies, HandoffAnomalyReceiptInvalid) {
				t.Fatalf("the unreadable receipt reported %+v", volume)
			}
		case sound:
			if !volume.TerminalKnown || len(volume.Anomalies) != 0 {
				t.Fatalf("a sound receipt was spoiled by its neighbour: %+v", volume)
			}
		}
	}

	// The same has to hold for the sweep. One garbage receipt must not stop a
	// node expiring everything else it holds, or a single unreadable file
	// would make a node keep every run's results forever.
	expire(t, engine, now)
	if _, err := os.Stat(soundPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an expired volume survived because a neighbour's receipt was unreadable: %v", err)
	}
	if _, err := os.Stat(brokenPath); err != nil {
		t.Fatalf("a volume with an unreadable receipt was removed on a timestamp its workload could have written: %v", err)
	}
}

// A file two volumes hard-link is on the node once. Charging it twice would
// report a node holding more than it does, and would make a budget give up a
// second volume for bytes the first one still holds.
func TestTheHandoffInventoryChargesOneInodeToOneVolume(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	first, firstPath := makeHandoffVolume(t, root, "link-owner-a")
	second, secondPath := makeHandoffVolume(t, root, "link-owner-b")
	payload := []byte(strings.Repeat("x", 4096))
	if err := os.WriteFile(filepath.Join(firstPath, "shared"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(firstPath, "shared"), filepath.Join(secondPath, "shared")); err != nil {
		t.Fatal(err)
	}
	// A nested directory and a symlink pointing at the node's own root: the
	// walk must count the former and never follow the latter.
	if err := os.MkdirAll(filepath.Join(secondPath, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secondPath, "nested", "own"), []byte("own"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/", filepath.Join(secondPath, "escape")); err != nil {
		t.Fatal(err)
	}
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]RetainedHandoffVolume{}
	for _, volume := range inventory.Volumes {
		byName[volume.Name] = volume
	}
	if byName[first].LogicalBytes != 4096 || byName[second].LogicalBytes != 4096+3 {
		t.Fatalf("logical bytes = %d and %d, want each volume charged what trimming it would recover",
			byName[first].LogicalBytes, byName[second].LogicalBytes)
	}
	if total := byName[first].DedupedBytes + byName[second].DedupedBytes; total != 4096+3 {
		t.Fatalf("deduped bytes across the root = %d, want the shared inode counted once", total)
	}
	if byName[second].Entries != 4 {
		t.Fatalf("entries under the second volume = %d, want the link, the directory, the nested file and the symlink each counted", byName[second].Entries)
	}
}

// The tree being measured belongs to the thing being measured. A workload that
// makes its own directory too expensive to walk must get a floor and a label,
// never an unbounded walk somebody else's deadline is waiting behind.
func TestAMeasurementThatSpendsItsBudgetStillLeavesAnExactTerminalTime(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	engine.handoffMeasureEntryBudget = 4
	name, path := makeHandoffVolume(t, root, "wide")
	for index := range 32 {
		if err := os.WriteFile(filepath.Join(path, fmt.Sprintf("file-%02d", index)), []byte("0123456789"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	stamp(t, engine, name, now)
	receipt := readHandoffReceipt(t, root, name)
	if !receipt.TerminalAt.Equal(now) {
		t.Fatalf("a truncated measurement lost the terminal time: %+v", receipt)
	}
	if !receipt.Truncated {
		t.Fatalf("a measurement that spent its budget was published as a measurement: %+v", receipt)
	}
	if receipt.Entries != 4 || receipt.LogicalBytes != 40 {
		t.Fatalf("the floor = %d entries / %d bytes, want exactly the budget it spent", receipt.Entries, receipt.LogicalBytes)
	}
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	volume := inventory.Volumes[0]
	if !volume.Truncated || !slices.Contains(volume.Anomalies, HandoffAnomalyMeasurementTruncated) {
		t.Fatalf("the inventory did not say the figures are a floor: %+v", volume)
	}
	// Truncated or not, the volume has a terminal time and therefore a
	// deadline: a tree too expensive to measure must not become a tree the
	// node keeps forever.
	expire(t, engine, now.Add(2*time.Hour))
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a truncated volume never reached its deadline: %v", err)
	}
}

// Delete's cleanup context bounds its own work. Measuring used to accept no
// context at all, so an attempt's finalization could sit in a workload's tree
// past the deadline the caller had already set.
func TestACancelledContextStopsMeasuringAndStillPublishesTheTerminalTime(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "cancelled")
	if err := os.WriteFile(filepath.Join(path, "result.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := engine.writeHandoffRetentionReceipt(ctx, name, now); err != nil {
		t.Fatalf("a cancelled measurement refused to record the terminal time: %v", err)
	}
	receipt := readHandoffReceipt(t, root, name)
	if !receipt.TerminalAt.Equal(now) || !receipt.Truncated || receipt.Entries != 0 {
		t.Fatalf("cancelled receipt = %+v, want an exact terminal time over a zero floor", receipt)
	}
}

// Measuring is a workload's cost and must not be on a node-wide lock. It used
// to be: one run's directory sat in front of every other attempt's Delete, and
// Delete's own ten-second cleanup context did not bound it.
func TestOneAttemptsMeasurementDoesNotBlockAnothersFinalization(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	slow, _ := makeHandoffVolume(t, root, "slow-tree")
	quick, _ := makeHandoffVolume(t, root, "quick-tree")

	held := make(chan struct{})
	release := make(chan struct{})
	engine.handoffMeasureEntered = func(volume string) {
		if volume != slow {
			return
		}
		close(held)
		<-release
	}
	blocked := make(chan error, 1)
	go func() { blocked <- engine.writeHandoffRetentionReceipt(t.Context(), slow, now) }()
	<-held

	// The other attempt finalizes while the first is still measuring.
	if err := engine.writeHandoffRetentionReceipt(t.Context(), quick, now); err != nil {
		t.Fatalf("one attempt's measurement blocked another's finalization: %v", err)
	}
	if !receiptPresent(t, root, quick) {
		t.Fatal("the unblocked attempt published no receipt")
	}
	close(release)
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}
	if !receiptPresent(t, root, slow) {
		t.Fatal("the blocked attempt published no receipt")
	}
}

// A live workload can swap a directory between the moment the walk observes it
// and the moment the walk enters it. Measuring what is there afterwards would
// attribute another tree's bytes to this volume; following a symlink planted
// there would take the helper outside the tree it pinned.
func TestASubtreeReplacedBetweenItsStatAndItsOpenIsReportedNotMeasured(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "racing-workload")
	if err := os.MkdirAll(filepath.Join(path, "subtree"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "subtree", "mine"), []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	// What the workload swaps in: a different directory holding bytes that are
	// not this volume's.
	decoy := filepath.Join(root, "decoy")
	if err := os.MkdirAll(decoy, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(decoy, "not-mine"), []byte(strings.Repeat("y", 4096)), 0o600); err != nil {
		t.Fatal(err)
	}
	engine.handoffMeasureDescend = func(volume, entry string) {
		if volume != name || entry != "subtree" {
			return
		}
		engine.handoffMeasureDescend = nil
		if err := os.RemoveAll(filepath.Join(path, "subtree")); err != nil {
			t.Error(err)
		}
		if err := os.Rename(decoy, filepath.Join(path, "subtree")); err != nil {
			t.Error(err)
		}
	}
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	volume := inventory.Volumes[0]
	if !slices.Contains(volume.Anomalies, HandoffAnomalySubtreeReplaced) {
		t.Fatalf("a replaced subtree was not reported: %+v", volume)
	}
	if volume.LogicalBytes != 0 {
		t.Fatalf("a replaced subtree contributed %d bytes; it must be counted and not measured", volume.LogicalBytes)
	}
	if volume.Entries != 1 {
		t.Fatalf("entries = %d, want the replaced directory counted once and nothing under it", volume.Entries)
	}
}

// Reuse of a stable owner key inherits the directory, and it must not inherit
// the previous attempt's deadline. When that deadline passed mid-run, the new
// attempt's own Delete verified its volume as runtime residue and never
// reached Absent, so it retried until its cleanup deadline instead of
// recording the completion.
func TestReusingAVolumeSupersedesThePreviousAttemptsTerminalTime(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "rerun-owner")
	stamp(t, engine, name, now.Add(-2*time.Hour))

	request := RunRequest{
		Resources: ResourceIdentity{HandoffVolumeDirectory: name},
		Workload:  WorkloadInput{ManagedVolumes: []ManagedVolumeDescriptor{{Kind: ManagedVolumeHandoff, OwnerKey: "rerun-owner"}}},
	}
	if _, _, _, err := engine.managedVolumeSources(t.Context(), &request); err != nil {
		t.Fatal(err)
	}
	if receiptPresent(t, root, name) {
		t.Fatal("a rerun inherited the previous attempt's terminal time")
	}

	// It is now a live run with no terminal time, which is exactly what "this
	// run has not finished" means -- and neither expiry nor the absence
	// projection may call it residue.
	engine.attempts["rerun"] = &containerdAttempt{resources: ResourceIdentity{HandoffVolumeDirectory: name}}
	live, err := engine.liveHandoffVolumes()
	if err != nil {
		t.Fatal(err)
	}
	expired, err := engine.handoffVolumeExpired(name, now, live)
	if err != nil || expired {
		t.Fatalf("a live rerun's volume was expirable: expired=%v err=%v", expired, err)
	}
	// Defence in depth for the same failure: a terminal time that survives
	// onto a live volume by any route -- a supersede that did not run, a
	// clock step -- must still not make that volume residue. Expiry skips a
	// live volume, so the projection has to as well, or the two disagree
	// about the same directory and Delete's Verify never reaches Absent.
	stamp(t, engine, name, now.Add(-2*time.Hour))
	if expired, err := engine.handoffVolumeExpired(name, now, live); err != nil || expired {
		t.Fatalf("a live rerun carrying an expired receipt was expirable: expired=%v err=%v", expired, err)
	}
	observed := ResourceInventory{}
	if err := inventoryManagedVolumeResources(root, &observed); err != nil {
		t.Fatal(err)
	}
	residue, _, err := engine.runtimeAbsenceInventory(observed, now)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(residue.ManagedVolumes, name) {
		t.Fatal("a live rerun's handoff volume was classified as runtime residue, so its own Delete could never reach Absent")
	}
	if slices.Contains(residue.HandoffRetentionRecords, HandoffRetentionRecordName(name)) {
		t.Fatal("a live rerun's retention receipt was classified as runtime residue alongside its volume")
	}
	if err := engine.removeHandoffRetentionReceipt(name); err != nil {
		t.Fatal(err)
	}

	// And a receipt this attempt cannot publish leaves the volume receiptless
	// rather than expired: the next sweep stamps it.
	blocked := filepath.Join(engine.handoffRetentionRoot(), HandoffRetentionRecordName(name))
	if err := os.MkdirAll(filepath.Join(blocked, "occupied"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := engine.writeHandoffRetentionReceipt(t.Context(), name, now); err == nil {
		t.Fatal("a receipt that cannot be published reported success")
	}
	if expired, err := engine.handoffVolumeExpired(name, now, map[string]struct{}{}); err != nil || expired {
		t.Fatalf("a volume whose receipt could not be written became expirable: expired=%v err=%v", expired, err)
	}
}

// A response over MaxFrameBytes is not a shorter answer: the transport refuses
// it, the server closes the connection, and the client reads that as a lost
// session. A row cap did not bound this -- 4096 receiptless volumes encode
// past the frame limit on their own.
func TestTheHandoffInventoryIsBoundedInEncodedBytesNotRows(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	if err := os.MkdirAll(filepath.Join(root, "handoffs"), 0o700); err != nil {
		t.Fatal(err)
	}
	// Enough receiptless volumes that a row-capped response would encode past
	// the frame limit.
	for index := range MaxInventoriedHandoffVolumes {
		if err := os.MkdirAll(filepath.Join(root, "handoffs", fmt.Sprintf("%s%064x", handoffVolumeNamePrefix, index)), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !inventory.Exhausted {
		t.Fatal("a response that could not carry the node's volumes did not say so")
	}
	encoded, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > MaxFrameBytes-handoffInventoryFrameHeadroom {
		t.Fatalf("the response encodes to %d bytes, past its %d-byte budget", len(encoded), MaxFrameBytes-handoffInventoryFrameHeadroom)
	}
	if len(inventory.Volumes) == 0 {
		t.Fatal("the budget left no room for any volume at all")
	}
}

// The boot receipt's union invariant -- observed = runtime residue union
// durable retained -- has to keep holding over every durable class the helper
// writes. A receipt is retained exactly while its volume is.
func TestARetentionReceiptIsProjectedRetainedExactlyWhileItsVolumeIs(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	retained, _ := makeHandoffVolume(t, root, "retained-run")
	expired, _ := makeHandoffVolume(t, root, "expired-run")
	stamp(t, engine, retained, now.Add(-30*time.Minute))
	stamp(t, engine, expired, now.Add(-2*time.Hour))
	orphan := HandoffRetentionRecordName(handoffVolumeNamePrefix + strings.Repeat("c", 32))
	if err := os.WriteFile(filepath.Join(engine.handoffRetentionRoot(), orphan), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	observed := ResourceInventory{}
	if err := inventoryManagedVolumeResources(root, &observed); err != nil {
		t.Fatal(err)
	}
	if len(observed.HandoffRetentionRecords) != 3 {
		t.Fatalf("observed retention records = %v, want all three", observed.HandoffRetentionRecords)
	}
	residue, _, err := engine.runtimeAbsenceInventory(observed, now)
	if err != nil {
		t.Fatal(err)
	}
	retainedRecord := HandoffRetentionRecordName(retained)
	if slices.Contains(residue.HandoffRetentionRecords, retainedRecord) {
		t.Fatal("a receipt whose volume is retained was classified as runtime residue")
	}
	for _, want := range []string{HandoffRetentionRecordName(expired), orphan} {
		if !slices.Contains(residue.HandoffRetentionRecords, want) {
			t.Fatalf("receipt %q was neither residue nor paired with a retained volume; the union invariant is broken", want)
		}
	}

	// And the sweep is what removes them, so the residue it reports does not
	// keep coming back.
	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	if _, err := os.Stat(filepath.Join(engine.handoffRetentionRoot(), orphan)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an orphan receipt survived the sweep: %v", err)
	}
}

// The receipt lives in its own root rather than beside the volumes, because
// every scan of the handoff root matches on the volume prefix and a sibling
// carrying that prefix would be read as a volume.
func TestRetentionReceiptsAreNotMistakenForHandoffVolumes(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "separate-roots")
	stamp(t, engine, name, now)
	names, err := engine.handoffVolumeNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != name {
		t.Fatalf("handoff volume scan = %v, want only the volume itself", names)
	}
	if receiptRoot := engine.handoffRetentionRoot(); filepath.Dir(receiptRoot) != root || filepath.Base(receiptRoot) != handoffRetentionStateDirectory {
		t.Fatalf("the receipt root %q is not a sibling of the handoff root", receiptRoot)
	}
}

// The forgery boundary is that the container is never given a path to the
// receipt root. An operator mount that exposes the managed runtime root takes
// that away: a uid-0 workload with a writable path there can read the volume
// identity out of a receipt and rewrite it, and device/inode matching is no
// defence against a writer who can spell both.
func TestAnOperatorMountMayNotOverlapTheManagedRuntimeRoot(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		managed  string
		source   string
		rejected bool
	}{
		{name: "the managed root itself", managed: "/var/lib/wefty/oci", source: "/var/lib/wefty/oci", rejected: true},
		{name: "an ancestor of it", managed: "/var/lib/wefty/oci", source: "/var/lib/wefty", rejected: true},
		{name: "the receipt root inside it", managed: "/var/lib/wefty/oci", source: "/var/lib/wefty/oci/" + handoffRetentionStateDirectory, rejected: true},
		{name: "a handoff volume inside it", managed: "/var/lib/wefty/oci", source: "/var/lib/wefty/oci/handoffs", rejected: true},
		{name: "a sibling that merely shares a prefix", managed: "/var/lib/wefty/oci", source: "/var/lib/wefty/ocidata"},
		{name: "an unrelated operator directory", managed: "/var/lib/wefty/oci", source: "/srv/data"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := rejectManagedRootOverlap(testCase.managed, testCase.source)
			if testCase.rejected && err == nil {
				t.Fatalf("operator source %q was permitted beside managed root %q", testCase.source, testCase.managed)
			}
			if !testCase.rejected && err != nil {
				t.Fatalf("operator source %q was refused beside managed root %q: %v", testCase.source, testCase.managed, err)
			}
		})
	}
}
