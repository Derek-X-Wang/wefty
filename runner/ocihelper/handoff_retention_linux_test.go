//go:build linux

package ocihelper

import (
	"bytes"
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
	engine.cleanupExpiredHandoffs(t.Context(), now, names, live)
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
	// A live attempt holds it, so nothing stamps it and the read has to say
	// what it is looking at: an age the workload owns.
	engine.attempts["live"] = &containerdAttempt{resources: ResourceIdentity{HandoffVolumeDirectory: name}}
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
	// The pass walks the root in name order, and names are digests, so the
	// fixture only proves "a failure does not stop what comes after" if the
	// blocked volume really does come first. Assert it rather than hope: a
	// changed owner key would otherwise turn this into a test of nothing.
	if blocked >= stampable {
		t.Fatalf("the fixture is not in failure-first order: blocked=%s stampable=%s", blocked, stampable)
	}

	// The test seam fails the temporary receipt's write for the first volume.
	// It sorts first, so a real publication failure happens before the rest of
	// the pass without turning the receipt path into an unreadable existing
	// record that create-only repair would correctly leave alone.
	writeRefused := errors.New("receipt write refused")
	engine.handoffRepairWrite = func(file *os.File, payload []byte) error {
		if strings.HasPrefix(filepath.Base(file.Name()), "."+HandoffRetentionRecordName(blocked)+".tmp-") {
			return writeRefused
		}
		_, err := file.Write(payload)
		return err
	}

	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	engine.handoffRepairWrite = nil

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
	// A directory where the receipt goes: the read fails operationally rather
	// than returning something invalid, which is the case the repair must not
	// write over -- a receipt that cannot be read may still be a valid one.
	if err := os.MkdirAll(engine.handoffRetentionPath(broken), 0o700); err != nil {
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
			if volume.TerminalKnown || !slices.Contains(volume.Anomalies, HandoffAnomalyReceiptUnreadable) {
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

// Repair fills only an absent receipt path. Invalid, mismatched, and unreadable
// paths remain anomalies because accounting is create-only and may not replace
// evidence already present there.
func TestTheRepairFillsOnlyAnAbsentReceiptGap(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	absent, _ := makeHandoffVolume(t, root, "absent-receipt")
	invalid, _ := makeHandoffVolume(t, root, "invalid-receipt")
	unreadable, _ := makeHandoffVolume(t, root, "unreadable-receipt")
	if err := os.MkdirAll(engine.handoffRetentionRoot(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(engine.handoffRetentionPath(invalid), []byte("{not a receipt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(engine.handoffRetentionPath(unreadable), 0o700); err != nil {
		t.Fatal(err)
	}

	engine.reconcileHandoffRetention(t.Context(), now, quiescent())

	receipt := readHandoffReceipt(t, root, absent)
	if !receipt.TerminalAt.Equal(now) {
		t.Fatalf("an absent receipt was not repaired: %+v", receipt)
	}
	invalidPayload, err := os.ReadFile(engine.handoffRetentionPath(invalid))
	if err != nil || string(invalidPayload) != "{not a receipt" {
		t.Fatalf("an invalid receipt was replaced: payload=%q err=%v", invalidPayload, err)
	}
	info, err := os.Lstat(engine.handoffRetentionPath(unreadable))
	if err != nil || !info.IsDir() {
		t.Fatalf("an operational read failure was written over: %v", err)
	}

	for anomaly, repairable := range map[HandoffVolumeAnomaly]bool{
		HandoffAnomalyNoReceipt:            true,
		HandoffAnomalyReceiptInvalid:       false,
		HandoffAnomalyReceiptMismatched:    false,
		HandoffAnomalyReceiptUnreadable:    false,
		HandoffAnomalyVolumeUnreadable:     false,
		HandoffAnomalyMeasurementTruncated: false,
		HandoffAnomalySubtreeReplaced:      false,
		"":                                 false,
	} {
		if handoffReceiptGapIsRepairable(anomaly) != repairable {
			t.Fatalf("anomaly %q is treated as repairable=%v", anomaly, !repairable)
		}
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
	// A live attempt holds it, so the repair pass skips it and the one walk
	// that happens is the one being observed.
	engine.attempts["live"] = &containerdAttempt{resources: ResourceIdentity{HandoffVolumeDirectory: name}}
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
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "rerun", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassOneShot, RemovalGeneration: "1",
	}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	resources.HandoffVolumeDirectory = name

	request := RunRequest{
		Authority: authority, Resources: resources,
		Workload: WorkloadInput{ManagedVolumes: []ManagedVolumeDescriptor{{Kind: ManagedVolumeHandoff, OwnerKey: "rerun-owner"}}},
	}
	if _, _, _, err := engine.managedVolumeSources(t.Context(), &request); err != nil {
		t.Fatal(err)
	}
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	if err := engine.registerAttemptLiveAndSupersedeHandoff(&containerdAttempt{authority: authority, resources: resources}); err != nil {
		t.Fatal(err)
	}
	if receiptPresent(t, root, name) {
		t.Fatal("a rerun inherited the previous attempt's terminal time")
	}

	// It is now a live run with no terminal time, which is exactly what "this
	// run has not finished" means -- and neither expiry nor the absence
	// projection may call it residue.
	live, err := engine.liveHandoffVolumes()
	if err != nil {
		t.Fatal(err)
	}
	expired, err := engine.handoffVolumeExpired(name, now, live)
	if err != nil || expired {
		t.Fatalf("a live rerun's volume was expirable: expired=%v err=%v", expired, err)
	}
	// The projection has to agree with expiry about this exact directory, or
	// the rerun's own Delete verifies its volume as residue and never reaches
	// Absent. Superseding is what makes both say the same thing: an owned,
	// receiptless volume is a run that has not finished.
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
	engine.handoffInventoryBytes = 2 << 10
	if err := os.MkdirAll(filepath.Join(root, "handoffs"), 0o700); err != nil {
		t.Fatal(err)
	}
	const volumes = 64
	for index := range volumes {
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
	if len(inventory.Volumes) == 0 || len(inventory.Volumes) >= volumes {
		t.Fatalf("the response carried %d of %d volumes; the bound is in bytes, so it must stop partway", len(inventory.Volumes), volumes)
	}
	encoded, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > engine.handoffInventoryByteBudget() {
		t.Fatalf("the response encodes to %d bytes, past its %d-byte budget", len(encoded), engine.handoffInventoryByteBudget())
	}

	// And the production budget is the frame limit less its envelope headroom,
	// because a frame the transport refuses costs the node its session.
	unbounded := handoffRetentionEngine(t, t.TempDir(), time.Hour, now)
	if budget := unbounded.handoffInventoryByteBudget(); budget >= MaxFrameBytes || budget != MaxFrameBytes-handoffInventoryFrameHeadroom {
		t.Fatalf("the production response budget is %d against a %d-byte frame limit", budget, MaxFrameBytes)
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

// A receipt the filesystem refused used to wait for another boot: the sweep
// was the only thing that stamped one, so a node that filled up mid-run kept
// the affected volumes non-expirable until somebody restarted the helper. The
// agent reads the inventory hourly, which is the right cadence for the repair
// too.
func TestTheInventoryCallRepairsAReceiptTheSweepCouldNotWrite(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "refused-receipt")

	// Fail the temporary receipt write itself. An existing directory at the
	// receipt path would be an unreadable receipt, which create-only repair must
	// leave alone rather than treating as a refused publication.
	engine.handoffRepairWrite = func(*os.File, []byte) error {
		return errors.New("receipt write refused")
	}
	first, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	engine.handoffRepairWrite = nil
	if err != nil {
		t.Fatalf("a receipt the filesystem refused failed the whole read: %v", err)
	}
	if len(first.Volumes) != 1 || first.Volumes[0].TerminalKnown {
		t.Fatalf("first read = %+v, want one volume with no terminal time", first.Volumes)
	}

	// The filesystem recovers. No sweep, no restart -- the next accounting
	// read is what repairs it.
	second, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Volumes) != 1 || !second.Volumes[0].TerminalKnown {
		t.Fatalf("second read = %+v, want the receipt repaired without a sweep", second.Volumes)
	}
	if receipt := readHandoffReceipt(t, root, name); !receipt.TerminalAt.Equal(now) {
		t.Fatalf("the repaired receipt = %+v, want the helper's own clock", receipt)
	}
}

// The repair is create-only and skips a volume a live attempt holds: an
// accounting read must not declare a run finished while it is still writing.
func TestTheInventoryCallNeverStampsAVolumeALiveAttemptHolds(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	live, _ := makeHandoffVolume(t, root, "still-running")
	idle, _ := makeHandoffVolume(t, root, "finished-without-a-receipt")
	engine.attempts["live"] = &containerdAttempt{resources: ResourceIdentity{HandoffVolumeDirectory: live}}

	if _, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{}); err != nil {
		t.Fatal(err)
	}
	if receiptPresent(t, root, live) {
		t.Fatal("an accounting read stamped a terminal time on a volume a live attempt holds")
	}
	if !receiptPresent(t, root, idle) {
		t.Fatal("an accounting read did not repair the volume no attempt holds")
	}
}

func TestInventoryRepairRacingRunPreparationDoesNotStampTheLiveVolume(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "preparing-run")
	measured := make(chan struct{})
	resume := make(chan struct{})
	engine.handoffRepairMeasured = func(volume string) {
		if volume == name {
			close(measured)
			<-resume
		}
	}
	type result struct {
		inventory InventoryHandoffVolumesResponse
		err       error
	}
	done := make(chan result, 1)
	go func() {
		inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
		done <- result{inventory: inventory, err: err}
	}()
	<-measured

	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "preparing", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassOneShot, RemovalGeneration: "1",
	}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	resources.HandoffVolumeDirectory = name
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	if err := engine.registerAttemptLiveAndSupersedeHandoff(&containerdAttempt{authority: authority, resources: resources}); err != nil {
		t.Fatal(err)
	}
	close(resume)
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	if receiptPresent(t, root, name) {
		t.Fatal("repair published a terminal receipt after Run made the volume live")
	}
	if len(got.inventory.Volumes) != 1 || !got.inventory.Volumes[0].Live || got.inventory.Volumes[0].TerminalKnown {
		t.Fatalf("inventory after Run preparation = %+v, want one live volume with no terminal time", got.inventory.Volumes)
	}
}

func TestInventoryRepairRacingDeleteCannotReplaceDeletesReceipt(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	deletedAt := now.Add(5 * time.Minute)
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "finalizing-run")
	measured := make(chan struct{})
	resume := make(chan struct{})
	engine.handoffRepairMeasured = func(volume string) {
		if volume == name {
			close(measured)
			<-resume
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
		done <- err
	}()
	<-measured
	if err := engine.writeHandoffRetentionReceipt(t.Context(), name, deletedAt); err != nil {
		t.Fatal(err)
	}
	receiptPath := engine.handoffRetentionPath(name)
	want, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("repair replaced Delete's receipt: got %q want %q", got, want)
	}
}

func TestInventoryRepairDoesNotPublishAfterTheVolumeIdentityIsReused(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "reused-name")
	replacement := filepath.Join(root, "preallocated-replacement")
	if err := os.Mkdir(replacement, 0o700); err != nil {
		t.Fatal(err)
	}
	measured := make(chan struct{})
	resume := make(chan struct{})
	engine.handoffRepairMeasured = func(volume string) {
		if volume == name {
			close(measured)
			<-resume
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
		done <- err
	}()
	<-measured
	// Linux does not replace an existing directory with rename. Remove the
	// empty measured directory while repair is paused, then move the already
	// allocated replacement inode into the same stable name.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if receiptPresent(t, root, name) {
		t.Fatal("repair published figures measured from the directory previously at the reused name")
	}
}

// Reading is not deleting. Expiry belongs to the sweep and eviction to the
// agent, so an accounting read may repair state but must never cost a node a
// run's results.
func TestTheInventoryCallRemovesNothing(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	expired, expiredPath := makeHandoffVolume(t, root, "long-expired")
	stamp(t, engine, expired, now.Add(-2*time.Hour))
	orphan := HandoffRetentionRecordName(handoffVolumeNamePrefix + strings.Repeat("d", 32))
	if err := os.WriteFile(filepath.Join(engine.handoffRetentionRoot(), orphan), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(expiredPath); err != nil {
		t.Fatalf("an accounting read removed an expired volume: %v", err)
	}
	if _, err := os.Stat(filepath.Join(engine.handoffRetentionRoot(), orphan)); err != nil {
		t.Fatalf("an accounting read removed an orphan receipt: %v", err)
	}
}

// `SweepResponse.removed` is the number of observed identities absent from the
// final inventory. Taking that count before the last removal made a sweep that
// had just expired a handoff volume and its receipt report that it had removed
// nothing.
//
// The containerd half of the final inventory needs a real daemon; the handoff
// half does not, so `observe` here is the same filesystem scan the engine's
// own inventory performs over the managed-volume classes.
func TestSweepCountsTheHandoffVolumeAndReceiptItExpiredLast(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	expired, expiredPath := makeHandoffVolume(t, root, "expired-at-sweep")
	retained, _ := makeHandoffVolume(t, root, "still-retained")
	stamp(t, engine, expired, now.Add(-2*time.Hour))
	stamp(t, engine, retained, now.Add(-30*time.Minute))

	observe := func() (ResourceInventory, error) {
		observed := ResourceInventory{}
		return observed, inventoryManagedVolumeResources(root, &observed)
	}
	observed, err := observe()
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.ManagedVolumes) != 2 || len(observed.HandoffRetentionRecords) != 2 {
		t.Fatalf("the fixture observed %+v", observed)
	}

	removed, err := engine.reconcileHandoffsThenCountRemoved(t.Context(), now, observed, quiescent(), observe)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(expiredPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the expired volume survived the sweep: %v", err)
	}
	if removed != 2 {
		t.Fatalf("sweep reported %d identities removed, want the expired volume and its receipt", removed)
	}
	if !receiptPresent(t, root, retained) {
		t.Fatal("the retained volume's receipt was counted away with the expired one")
	}
}

// Removing the receipt first and then the volume unlocked left a window: a
// repair that had already measured the volume saw it receiptless, published
// one, and the volume then disappeared underneath -- leaving a receipt the
// absence projection reads as runtime residue, which refuses the boot barrier
// until another sweep collects it.
func TestDeletingAHandoffVolumeLeavesNoReceiptForARacingRepairToOrphan(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "raced-deletion")
	if err := os.WriteFile(filepath.Join(path, "result.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The interleaving that matters is inside the deletion: a repair that has
	// already measured tries to publish while the deletion is between its two
	// removals. Under one lock, volume first, the repair blocks and then finds
	// nothing to bind to; with the receipt taken first and the volume removed
	// unlocked, the repair slips in and its receipt outlives the volume.
	measured := make(chan struct{})
	attempted := make(chan struct{})
	engine.handoffRepairMeasured = func(volume string) {
		if volume != name {
			return
		}
		close(measured)
		<-attempted
	}
	done := make(chan struct{})
	entered := false
	engine.handoffVolumeRemoved = func(string) error {
		if entered {
			return nil
		}
		entered = true
		// Release the parked repair inside the window and give it every
		// chance to publish. Holding one lock across both removals is what
		// makes it block here instead; a deletion that let go of the lock
		// between them would let this repair reach a volume that is about to
		// disappear.
		close(attempted)
		select {
		case <-done:
		case <-time.After(250 * time.Millisecond):
		}
		return nil
	}

	go func() {
		defer close(done)
		engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	}()
	<-measured
	response, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeHandoff, OwnerKey: "raced-deletion"})
	if err != nil || !response.Deleted {
		t.Fatalf("finalization during repair = %+v err=%v", response, err)
	}
	<-done
	if !entered {
		t.Fatal("the fixture never reached the window between the two removals")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the finalized volume remains: %v", err)
	}
	if receiptPresent(t, root, name) {
		t.Fatal("a repair published a receipt for a volume that had just been finalized, orphaning it")
	}

	// And the projection agrees, which is what the boot barrier reads.
	observed := ResourceInventory{}
	if err := inventoryManagedVolumeResources(root, &observed); err != nil {
		t.Fatal(err)
	}
	if len(observed.HandoffRetentionRecords) != 0 || len(observed.ManagedVolumes) != 0 {
		t.Fatalf("the node still observes %+v after a finalized deletion", observed)
	}
}

// A crash between writing a receipt and publishing it leaves the temporary
// behind. It carries no volume prefix, so nothing paired it with a volume and
// nothing ever collected it.
func TestTheSweepCollectsReceiptTemporariesACrashLeftBehind(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, _ := makeHandoffVolume(t, root, "crashed-mid-publish")
	stamp(t, engine, name, now)
	temporary := filepath.Join(engine.handoffRetentionRoot(), "."+HandoffRetentionRecordName(name)+".tmp-123456")
	if err := os.WriteFile(temporary, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	engine.reconcileHandoffRetention(t.Context(), now, quiescent())

	if _, err := os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a crash-left receipt temporary survived the sweep: %v", err)
	}
	if !receiptPresent(t, root, name) {
		t.Fatal("collecting the temporary took the published receipt with it")
	}
}

// The orphan pass used to run off the names observed before expiry, so a
// receipt whose removal failed immediately after its volume was taken still
// counted as paired and survived -- runtime residue by the projection's own
// rule -- until another sweep.
func TestTheOrphanPassSeesTheReceiptsExpiryCouldNotTake(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "expired-then-orphaned")
	stamp(t, engine, name, now.Add(-2*time.Hour))

	// Expiry takes the volume and then cannot take its receipt. The pass
	// observed the volume, so a pairing built from that observation still
	// calls the receipt paired -- and leaves an orphan the projection reads as
	// runtime residue.
	engine.handoffVolumeRemoved = func(string) error {
		return errors.New("the receipt could not be removed after its volume")
	}
	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the expired volume was not removed: %v", err)
	}
	if receiptPresent(t, root, name) {
		t.Fatal("a receipt whose volume is gone survived the same pass that observed it")
	}
}

// An ownership release the helper had to defer leaves the record in place. That
// used to keep the volume live forever, so a run whose Delete had already
// published a terminal time never expired at all. A valid receipt is proof of
// finalization: Delete writes one only after the task is reaped and absence is
// verified, and repair is create-only and skips every owned volume.
func TestAStaleOwnershipRecordDoesNotKeepAFinalizedVolumeAliveForever(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	finalized, finalizedPath := makeHandoffVolume(t, root, "finalized-but-still-owned")
	unfinished, unfinishedPath := makeHandoffVolume(t, root, "owned-and-unfinished")
	writeOwnershipRecord(t, engine, "finalized-but-still-owned", finalized)
	writeOwnershipRecord(t, engine, "owned-and-unfinished", unfinished)

	// Delete published this one's terminal time before the release was
	// deferred.
	stamp(t, engine, finalized, now.Add(-2*time.Hour))

	live, err := engine.liveHandoffVolumes()
	if err != nil {
		t.Fatal(err)
	}
	if _, writing := live[finalized]; writing {
		t.Fatal("a volume whose Delete had published a terminal time was still called live")
	}
	if _, writing := live[unfinished]; !writing {
		t.Fatal("an owned volume with no terminal time stopped being live")
	}

	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	if _, err := os.Stat(finalizedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a finalized volume held by a stale ownership record never expired: %v", err)
	}
	if _, err := os.Stat(unfinishedPath); err != nil {
		t.Fatalf("an unfinished owned volume was expired: %v", err)
	}
	if receiptPresent(t, root, unfinished) {
		t.Fatal("an owned, unfinished volume was stamped terminal by the repair")
	}
}

// writeOwnershipRecord plants the durable record a previous helper generation
// leaves behind, carrying the owner-key-derived handoff volume name.
func writeOwnershipRecord(t *testing.T, engine *ContainerdEngine, jobID, volume string) {
	t.Helper()
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "prior-boot", JobID: jobID, AttemptID: "attempt-" + jobID,
		FencingToken: "fence", Class: contract.JobClassOneShot, RemovalGeneration: "1",
	}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	resources.HandoffVolumeDirectory = volume
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
}

// Published is published. The link fallback can create the name and then fail
// to unlink the temporary; returning that as a plain failure skipped the
// directory fsync on a record already on disk, so a crash could lose a receipt
// the helper had committed.
func TestACreateOnlyPublicationThatCannotUnlinkItsTemporaryIsStillPublished(t *testing.T) {
	root := t.TempDir()
	published, err := writeAtomicDurableJSONRecordCreateOnlyWithRename(root, "record", map[string]string{"a": "b"}, nil,
		func(oldPath, newPath string) (bool, error) {
			if err := os.Link(oldPath, newPath); err != nil {
				return false, err
			}
			return true, errors.New("temporary could not be unlinked")
		})
	if !published {
		t.Fatalf("a record that reached its name was reported unpublished: %v", err)
	}
	if err == nil {
		t.Fatal("the unlink failure was swallowed")
	}
	if _, statErr := os.Stat(filepath.Join(root, "record")); statErr != nil {
		t.Fatalf("the published record is not there: %v", statErr)
	}
}

// Freeing a handoff volume walks a directory a workload built, so its cost is
// a workload's choice. Holding the node-wide retention lock across that walk
// put one run's tree in front of every other attempt's finalization -- the
// same mistake measuring made, and neither side observed the ten-second
// cleanup deadline the caller had already set.
func TestFreeingOneVolumesTreeDoesNotBlockAnotherAttemptsFinalization(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	slow, slowPath := makeHandoffVolume(t, root, "slow-to-free")
	quick, _ := makeHandoffVolume(t, root, "finishing-meanwhile")
	if err := os.WriteFile(filepath.Join(slowPath, "result.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	freeing := make(chan struct{})
	release := make(chan struct{})
	engine.handoffDetachedRemoving = func(detached string) {
		if !strings.HasPrefix(detached, handoffDetachedVolumePrefix+slow) {
			return
		}
		close(freeing)
		<-release
	}
	deleted := make(chan error, 1)
	go func() {
		_, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeHandoff, OwnerKey: "slow-to-free"})
		deleted <- err
	}()
	<-freeing

	// The volume's own name is already gone: detaching is what makes the
	// deletion's absence verification true, not the walk that follows.
	if _, err := os.Stat(slowPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the volume name survived detachment: %v", err)
	}

	// Another attempt finalizes while that tree is still being freed.
	published := make(chan error, 1)
	go func() { published <- engine.writeHandoffRetentionReceipt(t.Context(), quick, now) }()
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("finalizing another attempt during a free: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("one volume's free blocked another attempt's finalization")
	}
	if !receiptPresent(t, root, quick) {
		t.Fatal("the unblocked attempt published no receipt")
	}

	close(release)
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(filepath.Join(root, "handoffs")); err != nil {
		t.Fatal(err)
	} else {
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), handoffDetachedVolumePrefix) {
				t.Fatalf("a detached tree survived its own deletion: %s", entry.Name())
			}
		}
	}
}

// A crash between detaching a volume and freeing it, or a deletion whose
// caller was cancelled mid-free, leaves a detached tree. It is invisible to
// every scan that matches the volume prefix, so nothing else would ever
// collect it.
func TestTheSweepFreesDetachedTreesACrashLeftBehind(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	live, _ := makeHandoffVolume(t, root, "an-ordinary-volume")
	detached, err := detachedHandoffVolumeName(live)
	if err != nil {
		t.Fatal(err)
	}
	orphanTree := filepath.Join(root, "handoffs", detached)
	if err := os.MkdirAll(filepath.Join(orphanTree, "deep"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanTree, "deep", "bytes"), []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}

	// It is nobody's volume: not inventoried, not measured, not expired.
	names, err := engine.handoffVolumeNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != live {
		t.Fatalf("a detached tree was read as a handoff volume: %v", names)
	}

	engine.reconcileHandoffRetention(t.Context(), now, quiescent())
	if _, err := os.Stat(orphanTree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a detached tree a crash left behind survived the sweep: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "handoffs", live)); err != nil {
		t.Fatalf("collecting the detached tree took a live volume with it: %v", err)
	}
}

// A deletion whose caller ran out of time leaves its tree detached. Nothing
// else can see those bytes -- a detached name is invisible to the inventory,
// to expiry and to repair -- so waiting for the next boot sweep hid them for
// as long as the session lived.
func TestACancelledFreeIsFinishedByTheNextAccountingReadWithNoSweep(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	_, path := makeHandoffVolume(t, root, "cancelled-free")
	if err := os.MkdirAll(filepath.Join(path, "deep"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "deep", "bytes"), []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	response, err := engine.DeleteManagedVolume(cancelled, DeleteManagedVolumeRequest{Kind: ManagedVolumeHandoff, OwnerKey: "cancelled-free"})
	if err != nil || !response.Deleted {
		t.Fatalf("a cancelled free refused the deletion = %+v err=%v", response, err)
	}
	// The volume's own name and its receipt are gone -- that is what the
	// deletion promised -- but the bytes are still here.
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the volume name survived: %v", err)
	}
	if detachedTrees(t, root) != 1 {
		t.Fatalf("a cancelled free left %d detached tree(s), want exactly one", detachedTrees(t, root))
	}

	// No sweep. The agent's own accounting read finishes it, and says so
	// until it has.
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if inventory.DetachedTrees != 0 {
		t.Fatalf("the read reported %d detached tree(s) still pending after freeing them", inventory.DetachedTrees)
	}
	if detachedTrees(t, root) != 0 {
		t.Fatal("an accounting read left the detached tree on the node")
	}
	if len(inventory.Volumes) != 0 {
		t.Fatalf("a detached tree was reported as a volume: %+v", inventory.Volumes)
	}
}

// One child that cannot be freed stops this tree and leaves the rest of it
// where it is: the next pass tries again, and the response says the node is
// still holding bytes no volume accounts for.
func TestAFreeThatFailsOnOneChildLeavesTheRestForTheNextPass(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	_, path := makeHandoffVolume(t, root, "stubborn-child")
	for _, child := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(path, child), []byte(child), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Whichever child the directory yields first, and the same one on every
	// pass, so the fixture depends on no readdir order.
	refuse, stubborn := true, ""
	engine.handoffFreeChild = func(_, child string) error {
		if stubborn == "" {
			stubborn = child
		}
		if refuse && child == stubborn {
			return errors.New("this child cannot be freed")
		}
		return nil
	}
	if _, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeHandoff, OwnerKey: "stubborn-child"}); err == nil {
		t.Fatal("a free that could not finish reported success")
	}
	if detachedTrees(t, root) != 1 {
		t.Fatal("the failing tree did not stay detached")
	}
	// The rest of the tree is left where it is rather than half-freed: the
	// next pass tries the whole thing again, and grinding through the
	// remainder of a tree that is already failing buys nothing.
	if remaining := detachedChildren(t, root); remaining != 3 {
		t.Fatalf("a failing child left %d of 3 children; the rest of the tree was freed anyway", remaining)
	}

	// The read reports it rather than silently carrying bytes nothing names.
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if inventory.DetachedTrees != 1 {
		t.Fatalf("the read reported %d detached tree(s), want the one it could not free", inventory.DetachedTrees)
	}

	// And the next pass, once the filesystem allows it, finishes the job.
	refuse = false
	after, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if after.DetachedTrees != 0 || detachedTrees(t, root) != 0 {
		t.Fatalf("the next pass left %d detached tree(s) on the node", after.DetachedTrees)
	}
}

func detachedTrees(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "handoffs"))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), handoffDetachedVolumePrefix) {
			count++
		}
	}
	return count
}

// Cancellation is checked between the detached tree's top-level children, not
// once before the walk: a caller out of time stops within one child's subtree
// rather than after the whole tree, which is the difference between honouring
// a ten-second cleanup deadline and merely starting inside it.
func TestAFreeCancelledMidTreeStopsWithinOneChild(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	_, path := makeHandoffVolume(t, root, "cancelled-mid-tree")
	for _, child := range []string{"a", "b", "c", "d"} {
		if err := os.MkdirAll(filepath.Join(path, child), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	freed := 0
	engine.handoffFreeChild = func(string, string) error {
		freed++
		// The caller's time runs out while this child is being freed.
		cancel()
		return nil
	}
	if _, err := engine.DeleteManagedVolume(ctx, DeleteManagedVolumeRequest{Kind: ManagedVolumeHandoff, OwnerKey: "cancelled-mid-tree"}); err != nil {
		t.Fatal(err)
	}
	if freed != 1 {
		t.Fatalf("a cancelled free reached %d children, want exactly the one it had begun", freed)
	}
	if remaining := detachedChildren(t, root); remaining != 3 {
		t.Fatalf("a cancelled free left %d of 4 children; it walked past its own cancellation", remaining)
	}

	// And nothing is lost: the next pass finishes it.
	engine.handoffFreeChild = nil
	inventory, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if inventory.DetachedTrees != 0 || detachedTrees(t, root) != 0 {
		t.Fatalf("the next pass left %d detached tree(s)", inventory.DetachedTrees)
	}
}

// detachedChildren counts the top-level entries under the node's one detached
// tree.
func detachedChildren(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "handoffs"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), handoffDetachedVolumePrefix) {
			continue
		}
		children, err := os.ReadDir(filepath.Join(root, "handoffs", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		return len(children)
	}
	t.Fatal("no detached tree is present")
	return 0
}

// --- #494 S4 review: the page cursor, the per-entry charge, and the refusal
// of a volume an attempt owns ---

// TestTheHandoffInventoryPagesToTheEndOfItsRoot is what makes "no published
// result remains" a thing a node can establish. A bound with no cursor left
// the tail of a large root unreadable by any number of calls, so the node
// could never tell an empty published list from an unseen one.
func TestTheHandoffInventoryPagesToTheEndOfItsRoot(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	owners := []string{"run-a", "run-b", "run-c", "run-d", "run-e"}
	expected := make(map[string]struct{}, len(owners))
	for _, owner := range owners {
		name, _ := makeHandoffVolume(t, root, owner)
		expected[name] = struct{}{}
	}
	// One volume per page, which is the shape a byte budget produces on a real
	// root and the shape that makes the cursor load-bearing.
	engine.handoffInventoryBytes = len(`{"volumes":[],"exhausted":false,"next":""}`) + 1

	seen := make(map[string]struct{}, len(owners))
	after, pages := "", 0
	for {
		pages++
		if pages > 2*len(owners)+2 {
			t.Fatalf("the listing never reached the end of its root: after %d pages it had seen %d of %d", pages, len(seen), len(owners))
		}
		page, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{After: after})
		if err != nil {
			t.Fatal(err)
		}
		for _, volume := range page.Volumes {
			if _, repeated := seen[volume.Name]; repeated {
				t.Fatalf("the listing returned %s twice", volume.Name)
			}
			seen[volume.Name] = struct{}{}
		}
		if !page.Exhausted {
			break
		}
		if page.Next == "" || page.Next <= after {
			t.Fatalf("page %d stopped at %q and gave no way to resume (next=%q)", pages, after, page.Next)
		}
		after = page.Next
	}
	if len(seen) != len(expected) {
		t.Fatalf("the listing showed %d volumes across %d pages, want all %d", len(seen), pages, len(expected))
	}
	for name := range expected {
		if _, shown := seen[name]; !shown {
			t.Fatalf("%s was never shown", name)
		}
	}
	if pages < 2 {
		t.Fatalf("the fixture read the whole root in %d page(s); it does not exercise the cursor", pages)
	}
}

// TestAFinishedListingSaysSoWithExhaustedRatherThanAnEmptyCursor pins which
// answer means "this is everything". A page that stops before its first row
// has an empty cursor too, so emptiness cannot carry that meaning.
func TestAFinishedListingSaysSoWithExhaustedRatherThanAnEmptyCursor(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	makeHandoffVolume(t, root, "only-run")

	page, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if page.Exhausted || page.Next != "" || len(page.Volumes) != 1 {
		t.Fatalf("a root read whole reported %+v", page)
	}
	// Resuming past the only volume is an empty, finished page -- not an
	// exhausted one.
	beyond, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{After: page.Volumes[0].Name})
	if err != nil {
		t.Fatal(err)
	}
	if beyond.Exhausted || len(beyond.Volumes) != 0 {
		t.Fatalf("a cursor past the end reported %+v", beyond)
	}
}

// TestAVolumeIsChargedAFloorUnderEveryEntry is the one charging rule over both
// handoff roots. The mixed tree is the case a volume total and an entry count
// cannot reproduce: its data is one large file and its cost to the node is
// that file plus a slot for every empty one beside it.
func TestAVolumeIsChargedAFloorUnderEveryEntry(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	name, path := makeHandoffVolume(t, root, "mixed-run")
	const (
		large = 64 << 10
		empty = 200
	)
	if err := os.WriteFile(filepath.Join(path, "big.bin"), make([]byte, large), 0o600); err != nil {
		t.Fatal(err)
	}
	for index := range empty {
		if err := os.WriteFile(filepath.Join(path, fmt.Sprintf("tiny-%04d", index)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	page, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var volume RetainedHandoffVolume
	for _, candidate := range page.Volumes {
		if candidate.Name == name {
			volume = candidate
		}
	}
	// The receipt this pass stamps is one entry of its own, so the volume
	// holds the large file, the empty ones, and nothing else.
	if volume.Entries != empty+1 {
		t.Fatalf("the volume reached %d entries, want %d", volume.Entries, empty+1)
	}
	if volume.LogicalBytes != large || volume.DedupedBytes != large {
		t.Fatalf("the volume's byte figures = %d/%d, want %d", volume.LogicalBytes, volume.DedupedBytes, large)
	}
	// 4096 is written out rather than read back from the code under test: the
	// number the node is charged is the assertion.
	if want := int64(large) + empty*4096; volume.ChargedBytes != want {
		t.Fatalf("the volume was charged %d bytes, want %d -- the large file plus a floor under every empty entry",
			volume.ChargedBytes, want)
	}
	if volume.ChargedBytes <= volume.DedupedBytes {
		t.Fatal("the charged figure did not exceed the data, so the per-entry floor is missing")
	}
}

// TestDeletingAHandoffVolumeAnAttemptOwnsIsRefused is the helper's half of the
// eviction guard. The node selects from an inventory snapshot, so a rerun can
// register ownership after that read; only the lock that publishes ownership
// can close the window, and it closes it by refusing.
func TestDeletingAHandoffVolumeAnAttemptOwnsIsRefused(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	engine := handoffRetentionEngine(t, root, time.Hour, now)
	const ownerKey = "reused-run"
	name, path := makeHandoffVolume(t, root, ownerKey)
	if err := os.WriteFile(filepath.Join(path, "result.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// The volume reads as idle first, which is the snapshot the node acts on.
	if _, err := engine.InventoryHandoffVolumes(t.Context(), InventoryHandoffVolumesRequest{}); err != nil {
		t.Fatal(err)
	}

	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: ownerKey, AttemptID: "attempt-2",
		FencingToken: "fence", Class: contract.JobClassOneShot, RemovalGeneration: "1",
	}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	resources.HandoffVolumeDirectory = name
	if err := engine.ensureAttemptOwnershipRecord(authority, resources); err != nil {
		t.Fatal(err)
	}
	if err := engine.registerAttemptLiveAndSupersedeHandoff(&containerdAttempt{authority: authority, resources: resources}); err != nil {
		t.Fatal(err)
	}

	_, err = engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{
		Kind: ManagedVolumeHandoff, OwnerKey: ownerKey,
	})
	var live *HandoffVolumeLiveError
	if !errors.As(err, &live) {
		t.Fatalf("deleting a volume a live attempt owns = %v, want the typed refusal", err)
	}
	if live.Name != name {
		t.Fatalf("the refusal names %q, want the volume %q", live.Name, name)
	}
	// Nothing was detached and nothing was freed: the running attempt still
	// has its files.
	if _, err := os.Stat(filepath.Join(path, "result.json")); err != nil {
		t.Fatalf("a refused deletion took the running attempt's files anyway: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a refused deletion detached the volume anyway: %v", err)
	}

	// And it is replayable: once the attempt finishes -- which is a reaped
	// task and a published terminal receipt -- the identical request succeeds.
	engine.mu.Lock()
	delete(engine.attempts, authority.key())
	engine.mu.Unlock()
	if err := engine.writeHandoffRetentionReceipt(t.Context(), name, now); err != nil {
		t.Fatal(err)
	}
	response, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{
		Kind: ManagedVolumeHandoff, OwnerKey: ownerKey,
	})
	if err != nil || !response.Deleted {
		t.Fatalf("the replayed deletion = %+v, %v", response, err)
	}
}
