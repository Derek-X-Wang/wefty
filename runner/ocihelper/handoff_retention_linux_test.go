//go:build linux

package ocihelper

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// A handoff volume is mounted read-write into the container and a uid-0
// workload owns it, so its mtime is a number the workload writes. Once the
// helper has stamped its own terminal time, moving that mtime -- in either
// direction -- must change nothing about when the volume expires.
func TestHandoffExpiryRunsFromTheHelpersReceiptAndNotTheWorkloadsMtime(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "run-owner")
	if err := engine.writeHandoffRetentionReceipt(name, now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}

	// The workload backdates its own directory by two hours, well past the
	// window. The receipt says thirty minutes, and the receipt is what counts.
	stale := now.Add(-2 * time.Hour)
	if err := os.Chtimes(path, stale, stale); err != nil {
		t.Fatal(err)
	}
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a moved mtime expired a handoff volume whose receipt was inside the window: %v", err)
	}

	// The same volume forward-dated to the future must not be rescued either,
	// once its own receipt has run out.
	if err := engine.writeHandoffRetentionReceipt(name, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	fresh := now.Add(time.Hour)
	if err := os.Chtimes(path, fresh, fresh); err != nil {
		t.Fatal(err)
	}
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a forward-dated mtime kept a handoff volume past its receipt: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, handoffRetentionStateDirectory, HandoffRetentionRecordName(name))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the receipt outlived the volume it is bound to: %v", err)
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
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatal(err)
	}
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
	if !strings.Contains(volume.Anomaly, "no helper-owned retention receipt") {
		t.Fatalf("a receiptless volume carried no labelling anomaly: %q", volume.Anomaly)
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
	if err := engine.stampMissingHandoffRetentionReceipts(now); err != nil {
		t.Fatal(err)
	}
	receipt := readHandoffReceipt(t, root, name)
	if receipt.Version != handoffRetentionReceiptVersion || !receipt.TerminalAt.Equal(now) {
		t.Fatalf("swept receipt = %+v, want version 1 stamped at %v", receipt, now)
	}
	if receipt.LogicalBytes != 11 || receipt.Entries != 1 {
		t.Fatalf("swept receipt measured %d bytes over %d entries, want 11 over 1", receipt.LogicalBytes, receipt.Entries)
	}

	// Stamping is create-only: a second sweep must not re-date a window that
	// has already started, or a volume would never reach its deadline.
	later := now.Add(30 * time.Minute)
	if err := engine.stampMissingHandoffRetentionReceipts(later); err != nil {
		t.Fatal(err)
	}
	if again := readHandoffReceipt(t, root, name); !again.TerminalAt.Equal(now) {
		t.Fatalf("a later sweep re-dated an existing receipt to %v", again.TerminalAt)
	}
	if err := engine.cleanupExpiredHandoffs(now.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
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
	if err := engine.stampMissingHandoffRetentionReceipts(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, handoffRetentionStateDirectory, HandoffRetentionRecordName(name))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a live attempt's handoff volume was stamped terminal: %v", err)
	}
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Volumes) != 1 || !inventory.Volumes[0].Live {
		t.Fatalf("a live attempt's volume did not report live: %+v", inventory.Volumes)
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
	if err := engine.writeHandoffRetentionReceipt(name, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}

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
	if !strings.Contains(fact.anomaly, "different directory") {
		t.Fatalf("a mismatched receipt carried no anomaly: %q", fact.anomaly)
	}
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatal(err)
	}
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
	if err := engine.writeHandoffRetentionReceipt(sound, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(engine.handoffRetentionRoot(), 0o700); err != nil {
		t.Fatal(err)
	}
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
			if volume.TerminalKnown || !strings.Contains(volume.Anomaly, "version-1 helper receipt") {
				t.Fatalf("the unreadable receipt reported %+v", volume)
			}
		case sound:
			if !volume.TerminalKnown || volume.Anomaly != "" {
				t.Fatalf("a sound receipt was spoiled by its neighbour: %+v", volume)
			}
		}
	}

	// The same has to hold for the sweep. One garbage receipt must not stop a
	// node expiring everything else it holds, or a single unreadable file
	// would make a node keep every run's results forever.
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatalf("one unreadable receipt failed the whole expiry pass: %v", err)
	}
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

// The boot receipt's union invariant -- observed = runtime residue union
// durable retained -- has to keep holding over every durable class the helper
// writes. A receipt is retained exactly while its volume is.
func TestARetentionReceiptIsProjectedRetainedExactlyWhileItsVolumeIs(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	retained, _ := makeHandoffVolume(t, root, "retained-run")
	expired, _ := makeHandoffVolume(t, root, "expired-run")
	if err := engine.writeHandoffRetentionReceipt(retained, now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := engine.writeHandoffRetentionReceipt(expired, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
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
	for _, name := range residue.HandoffRetentionRecords {
		if name == retainedRecord {
			t.Fatal("a receipt whose volume is retained was classified as runtime residue")
		}
	}
	for _, want := range []string{HandoffRetentionRecordName(expired), orphan} {
		found := false
		for _, name := range residue.HandoffRetentionRecords {
			found = found || name == want
		}
		if !found {
			t.Fatalf("receipt %q was neither residue nor paired with a retained volume; the union invariant is broken", want)
		}
	}

	// And the sweep is what removes them, so the residue it reports does not
	// keep coming back.
	if err := engine.stampMissingHandoffRetentionReceipts(now); err != nil {
		t.Fatal(err)
	}
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
	if err := engine.writeHandoffRetentionReceipt(name, now); err != nil {
		t.Fatal(err)
	}
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
