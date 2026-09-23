//go:build linux

package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// A handoff volume's retention has to run from a timestamp the workload cannot
// move.
//
// The volume is mounted into the container at /wefty/handoff, read-write, and
// a uid-0 workload owns it: it can `utimensat` the directory, and merely
// creating a file moves the mtime. Expiring on that mtime let a workload keep
// its own results on the node forever, or -- pointing the timestamp the other
// way -- have them swept while it was still writing them.
//
// So the helper writes the terminal time itself, into a root the container has
// no path to. `handoffs-state/` mirrors `service-data-state/` exactly, and is
// deliberately a separate root rather than a sibling inside `handoffs/`,
// because every scan of the handoff root matches on handoffVolumeNamePrefix
// and a sibling carrying that prefix would be read as a volume.
//
// The receipt is evidence, not authority: it is bound to the device and inode
// of the directory as observed through a no-follow directory descriptor, the
// serviceVolumeOwnerRecord pattern, so a receipt standing over a directory
// that has been replaced is refused rather than believed.
const (
	handoffRetentionStateDirectory = "handoffs-state"
	handoffRetentionReceiptVersion = 1
	// maxHandoffMeasureEntries and maxHandoffMeasureOpens bound one pass. The
	// tree being walked is a workload's, so its shape is chosen by the thing
	// being measured; a pass that spends its budget reports a floor and says
	// so rather than running until somebody's deadline expires.
	maxHandoffMeasureEntries = 1 << 20
	maxHandoffMeasureOpens   = 1 << 16
	// maxHandoffMeasureDepth bounds how deep one volume is walked. One open
	// descriptor is held per level and never released while its children are
	// being measured, so this is also the bound on descriptors in flight.
	maxHandoffMeasureDepth = 64
	// handoffReadChunk is how many names one directory read returns. Reading a
	// whole directory at once would let a workload choose the helper's
	// allocation, which is the same mistake as an unbounded walk.
	handoffReadChunk = 256
	// maxHandoffVolumeAnomalies bounds one volume's observations so a response
	// cannot grow with a workload's tree.
	maxHandoffVolumeAnomalies = 4
)

// handoffRetentionReceipt is the helper-owned terminal fact for one handoff
// volume. The bytes are what the volume held when the receipt was written;
// they are evidence of the terminal moment, while the inventory call reports
// what the volume holds now.
type handoffRetentionReceipt struct {
	Version      uint8     `json:"version"`
	Device       uint64    `json:"device"`
	Inode        uint64    `json:"inode"`
	TerminalAt   time.Time `json:"terminal_at"`
	LogicalBytes int64     `json:"logical_bytes"`
	DedupedBytes int64     `json:"deduped_bytes"`
	Entries      int64     `json:"entries"`
	// Truncated says the measurement above stopped early, so those figures are
	// a floor. The terminal time is not a floor: it is exact, and it is the
	// field retention runs from. A measurement that could not finish must
	// still leave a terminal time, or a workload could keep its results
	// forever by making its own tree too expensive to measure.
	Truncated bool `json:"truncated,omitempty"`
}

type handoffInodeIdentity struct {
	device uint64
	inode  uint64
}

type handoffVolumeMeasurement struct {
	logical   int64
	deduped   int64
	entries   int64
	truncated bool
	anomalies []HandoffVolumeAnomaly
}

func (measurement *handoffVolumeMeasurement) note(anomaly HandoffVolumeAnomaly) {
	if slices.Contains(measurement.anomalies, anomaly) || len(measurement.anomalies) >= maxHandoffVolumeAnomalies {
		return
	}
	measurement.anomalies = append(measurement.anomalies, anomaly)
}

// handoffMeasureBudget is one pass's remaining work. It is shared across every
// volume of a node-wide pass, so the whole call is bounded, not each volume.
type handoffMeasureBudget struct {
	entries int64
	opens   int64
}

// newHandoffMeasureBudget is a method so a test can prove the bound without
// planting a million files, the way the agent's per-run bound is a field
// rather than a direct constant read.
func (engine *ContainerdEngine) newHandoffMeasureBudget() *handoffMeasureBudget {
	budget := &handoffMeasureBudget{entries: maxHandoffMeasureEntries, opens: maxHandoffMeasureOpens}
	if engine.handoffMeasureEntryBudget > 0 {
		budget.entries = engine.handoffMeasureEntryBudget
	}
	if engine.handoffMeasureOpenBudget > 0 {
		budget.opens = engine.handoffMeasureOpenBudget
	}
	return budget
}

// handoffRetentionFact is one volume's retention state as the helper reads it
// now: the terminal time when a bound receipt says so, and otherwise the
// directory's own mtime, labelled as the fallback it is.
type handoffRetentionFact struct {
	terminalAt    time.Time
	terminalKnown bool
	receipt       handoffRetentionReceipt
	anomaly       HandoffVolumeAnomaly
}

func (engine *ContainerdEngine) handoffVolumeRoot() string {
	return filepath.Join(engine.config.RuntimeRoot, "handoffs")
}

func (engine *ContainerdEngine) handoffRetentionRoot() string {
	return filepath.Join(engine.config.RuntimeRoot, handoffRetentionStateDirectory)
}

func (engine *ContainerdEngine) handoffRetention() time.Duration {
	retention := engine.config.HandoffRetention
	if retention <= 0 {
		retention = defaultHandoffRetention
	}
	return retention
}

func (engine *ContainerdEngine) handoffNow() time.Time {
	if engine.config.Clock != nil {
		return engine.config.Clock.Now()
	}
	return time.Now()
}

// openHandoffVolume opens one handoff volume as a directory descriptor that
// followed no symlink at its last component, which is the only identity this
// file ever trusts.
func (engine *ContainerdEngine) openHandoffVolume(name string) (*os.File, handoffInodeIdentity, error) {
	path := filepath.Join(engine.handoffVolumeRoot(), name)
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, handoffInodeIdentity{}, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		file.Close()
		return nil, handoffInodeIdentity{}, err
	}
	return file, handoffInodeIdentity{device: uint64(stat.Dev), inode: stat.Ino}, nil
}

// openHandoffVolumeIdentity is openHandoffVolume without keeping the handle:
// the descriptor exists only to take an identity nothing could have swapped
// underneath it.
func (engine *ContainerdEngine) openHandoffVolumeIdentity(name string) (os.FileInfo, handoffInodeIdentity, error) {
	file, identity, err := engine.openHandoffVolume(name)
	if err != nil {
		return nil, handoffInodeIdentity{}, err
	}
	info, statErr := file.Stat()
	closeErr := file.Close()
	if err := errors.Join(statErr, closeErr); err != nil {
		return nil, handoffInodeIdentity{}, err
	}
	return info, identity, nil
}

// readHandoffRetentionFact answers what this volume's retention runs from.
//
// An unreadable, invalid, or mismatched receipt is never an error that fails a
// node-wide call: it comes back as terminalKnown=false plus an anomaly token,
// the ComputerDiskAnomalies precedent. The caller then knows the volume is
// evictable under a budget and is not expirable on a timestamp a workload
// could have written.
func (engine *ContainerdEngine) readHandoffRetentionFact(name string) (handoffRetentionFact, error) {
	fact := handoffRetentionFact{}
	info, identity, err := engine.openHandoffVolumeIdentity(name)
	if err != nil {
		return fact, err
	}
	fact.terminalAt = info.ModTime()
	payload, err := os.ReadFile(engine.handoffRetentionPath(name))
	if errors.Is(err, os.ErrNotExist) {
		fact.anomaly = HandoffAnomalyNoReceipt
		return fact, nil
	}
	if err != nil {
		fact.anomaly = HandoffAnomalyReceiptUnreadable
		return fact, nil
	}
	var receipt handoffRetentionReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil || receipt.Version != handoffRetentionReceiptVersion {
		fact.anomaly = HandoffAnomalyReceiptInvalid
		return fact, nil
	}
	if receipt.Device != identity.device || receipt.Inode != identity.inode {
		// The record is evidence and the descriptor is authority. A receipt
		// naming a directory that is no longer the one standing at this name
		// says nothing about the bytes that are there now.
		fact.anomaly = HandoffAnomalyReceiptMismatched
		return fact, nil
	}
	if receipt.TerminalAt.IsZero() {
		fact.anomaly = HandoffAnomalyReceiptInvalid
		return fact, nil
	}
	fact.receipt = receipt
	fact.terminalAt = receipt.TerminalAt
	fact.terminalKnown = true
	return fact, nil
}

// handoffInventoryByteBudget is a method so a test can prove the bound without
// building a megabyte of fixture, the way the measurement budgets are.
func (engine *ContainerdEngine) handoffInventoryByteBudget() int {
	if engine.handoffInventoryBytes > 0 {
		return engine.handoffInventoryBytes
	}
	return MaxFrameBytes - handoffInventoryFrameHeadroom
}

func (engine *ContainerdEngine) handoffRetentionPath(volume string) string {
	return filepath.Join(engine.handoffRetentionRoot(), HandoffRetentionRecordName(volume))
}

// writeHandoffRetentionReceipt stamps one volume's terminal time.
//
// It is called from Delete after the attempt's task has been reaped and its
// absence independently verified -- the namespace is quiescent and no workload
// can still be writing here -- and from the sweep for a volume that has none.
//
// Measuring happens *outside* handoffRetentionMu, under the caller's context
// and a bounded budget. The tree being measured is a workload's, so its cost
// is a workload's choice; holding a node-wide lock across it let one run's
// directory sit in front of every other attempt's Delete, and Delete's own
// ten-second cleanup context did not bound it. The mutex now covers the
// publish alone -- a few syscalls -- and the volume's identity is re-taken
// under it, so figures measured against a directory that has since been
// replaced are never published as that directory's.
func (engine *ContainerdEngine) writeHandoffRetentionReceipt(ctx context.Context, name string, terminalAt time.Time) error {
	if name == "" {
		return nil
	}
	file, identity, err := engine.openHandoffVolume(name)
	if err != nil {
		return err
	}
	measurement := engine.measureHandoffVolume(ctx, name, file, nil, engine.newHandoffMeasureBudget())
	if err := file.Close(); err != nil {
		return err
	}
	return engine.publishHandoffRetentionReceipt(name, identity, terminalAt, measurement)
}

func (engine *ContainerdEngine) publishHandoffRetentionReceipt(name string, measured handoffInodeIdentity, terminalAt time.Time, measurement handoffVolumeMeasurement) error {
	engine.handoffRetentionMu.Lock()
	defer engine.handoffRetentionMu.Unlock()
	_, identity, err := engine.openHandoffVolumeIdentity(name)
	if err != nil {
		return err
	}
	if identity != measured {
		return fmt.Errorf("handoff volume %s was replaced while its terminal figures were measured", name)
	}
	root := engine.handoffRetentionRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create handoff retention state root: %w", err)
	}
	return writeAtomicDurableJSONRecord(root, HandoffRetentionRecordName(name), handoffRetentionReceipt{
		Version: handoffRetentionReceiptVersion, Device: identity.device, Inode: identity.inode,
		TerminalAt: terminalAt.UTC(), LogicalBytes: measurement.logical,
		DedupedBytes: measurement.deduped, Entries: measurement.entries,
		Truncated: measurement.truncated,
	})
}

// removeHandoffVolumeAndReceipt detaches a handoff volume and its terminal
// receipt together, and then frees the bytes with nothing held.
//
// Both halves are load-bearing, and they are separate because the tree being
// freed is a workload's.
//
// Detaching is one rename and one unlink under the retention mutex. Doing the
// receipt first and the volume afterwards, unlocked, left a window a
// concurrent repair walked into: it saw a receiptless volume, published a
// receipt, and the volume disappeared underneath -- an orphan the absence
// projection reads as runtime residue, which refuses the boot barrier until
// another sweep collects it. Repair reopens the volume under this same mutex
// before publishing, so once the name is gone there is nothing left to bind
// to.
//
// Freeing happens outside the mutex, because holding a node-wide lock across
// a walk of a directory a workload built is the mistake this slice already
// fixed once for measurement: one run's tree would sit in front of every other
// attempt's Delete, and neither would observe the ten-second cleanup deadline
// the caller had already set. The detached name is dot-prefixed, so the
// inventory scan and repair -- both keyed on the volume prefix -- cannot see
// it, and a crash between the two halves leaves a tree the sweep collects.
func (engine *ContainerdEngine) removeHandoffVolumeAndReceipt(ctx context.Context, name string) error {
	detached, err := engine.detachHandoffVolumeAndReceipt(name)
	if err != nil {
		return err
	}
	return engine.freeDetachedHandoffTree(ctx, detached)
}

// detachHandoffVolumeAndReceipt is the locked half: after it returns, the
// volume's name and its receipt are both absent, whatever is still on the
// disk under the detached name.
func (engine *ContainerdEngine) detachHandoffVolumeAndReceipt(name string) (string, error) {
	engine.handoffRetentionMu.Lock()
	defer engine.handoffRetentionMu.Unlock()
	detached, err := detachedHandoffVolumeName(name)
	if err != nil {
		return "", err
	}
	root := engine.handoffVolumeRoot()
	if err := os.Rename(filepath.Join(root, name), filepath.Join(root, detached)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		detached = ""
	}
	if engine.handoffVolumeRemoved != nil {
		// A test observes the window between the two: whether a concurrent
		// repair can reach it, and what a receipt removal that fails right
		// after its volume is gone leaves behind.
		if err := engine.handoffVolumeRemoved(name); err != nil {
			return detached, err
		}
	}
	// The receipt is retained exactly while its volume is. Removing them
	// together is what keeps "observed = residue union retained" true over
	// the durable class.
	if err := os.Remove(engine.handoffRetentionPath(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return detached, err
	}
	return detached, nil
}

// freeDetachedHandoffTree frees a detached tree with nothing held.
//
// A cancelled caller stops rather than working through a workload's tree past
// its own deadline; the tree keeps its detached name and the next sweep
// collects it, so the bytes are never lost track of.
func (engine *ContainerdEngine) freeDetachedHandoffTree(ctx context.Context, detached string) error {
	if detached == "" {
		return nil
	}
	if engine.handoffDetachedRemoving != nil {
		engine.handoffDetachedRemoving(detached)
	}
	if ctx != nil && ctx.Err() != nil {
		log.Printf("handoff retention: %s is detached and will be freed by the next sweep: %v", detached, ctx.Err())
		return nil
	}
	if err := os.RemoveAll(filepath.Join(engine.handoffVolumeRoot(), detached)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// detachedHandoffVolumeName is the name a volume wears between being detached
// and being freed. The dot keeps it out of every scan that matches the volume
// prefix, and the nonce keeps two detachments of the same name apart.
func detachedHandoffVolumeName(name string) (string, error) {
	nonce, err := randomCapability()
	if err != nil {
		return "", err
	}
	return handoffDetachedVolumePrefix + name + "-" + nonce[:16], nil
}

// collectDetachedHandoffTrees frees what a crash left between the two halves
// of a deletion, and what a cancelled deletion deliberately left behind. It is
// the handoff root's counterpart to the retention root's temporary sweep.
func (engine *ContainerdEngine) collectDetachedHandoffTrees(ctx context.Context) {
	entries, err := readDirectoryIfPresent(engine.handoffVolumeRoot())
	if err != nil {
		log.Printf("handoff retention: read the handoff root for detached trees: %v", err)
		return
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), handoffDetachedVolumePrefix) {
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		if err := os.RemoveAll(filepath.Join(engine.handoffVolumeRoot(), entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("handoff retention: free detached tree %s: %v", entry.Name(), err)
		}
	}
}

func (engine *ContainerdEngine) removeHandoffRetentionReceipt(name string) error {
	engine.handoffRetentionMu.Lock()
	defer engine.handoffRetentionMu.Unlock()
	return engine.removeHandoffRetentionReceiptLocked(name)
}

func (engine *ContainerdEngine) removeHandoffRetentionReceiptLocked(name string) error {
	if name == "" {
		return nil
	}
	if err := os.Remove(engine.handoffRetentionPath(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// registerAttemptLiveAndSupersedeHandoff is the Run transition from durable
// ownership to an in-memory live attempt. Durable ownership is published first
// by ensureAttemptOwnershipRecord. Under the same lock repair uses for its
// final liveness check, this adds the in-memory owner and only then removes the
// prior terminal receipt. From the moment either owner exists, repair must not
// stamp the volume.
func (engine *ContainerdEngine) registerAttemptLiveAndSupersedeHandoff(attempt *containerdAttempt) error {
	engine.handoffRetentionMu.Lock()
	defer engine.handoffRetentionMu.Unlock()
	engine.mu.Lock()
	engine.attempts[attempt.authority.key()] = attempt
	engine.mu.Unlock()
	return engine.removeHandoffRetentionReceiptLocked(attempt.resources.HandoffVolumeDirectory)
}

// supersedeHandoffRetentionReceipt drops the prior terminal time when a volume
// is prepared for reuse.
//
// Without this, a rerun of a stable owner key inherits the previous attempt's
// receipt, which can expire while the new attempt is still running. The next
// `Delete` then calls `Verify` first, which classified the volume and its
// receipt as runtime residue, so `Absent` was never reached and the attempt
// retried until its cleanup deadline instead of recording its own completion.
//
// A volume under a live attempt is deliberately left with no terminal time at
// all: "this run has not finished" is exactly what no receipt means, and the
// live-attempt guard keeps it out of expiry until its own Delete writes one.
func (engine *ContainerdEngine) supersedeHandoffRetentionReceipt(name string) error {
	return engine.removeHandoffRetentionReceipt(name)
}

// liveHandoffVolumes is the set of handoff volumes whose attempt this node has
// not proved stopped.
//
// It is deliberately two sources. The in-memory attempts map is empty in a
// freshly constructed engine, so a helper that restarted over a still-running
// workload would have called every one of its volumes idle and stamped a
// terminal time on a directory that was still being written. The durable
// attempt-ownership records are what survive that restart, and they carry the
// owner-key-derived handoff volume name precisely because the boot sweep
// cannot re-derive it.
func (engine *ContainerdEngine) liveHandoffVolumes() (map[string]struct{}, error) {
	owned := make(map[string]struct{})
	engine.mu.Lock()
	for _, attempt := range engine.attempts {
		if name := attempt.resources.HandoffVolumeDirectory; name != "" {
			owned[name] = struct{}{}
		}
	}
	engine.mu.Unlock()
	records, _, err := engine.loadAttemptOwnershipSnapshot()
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if name := record.Resources.HandoffVolumeDirectory; name != "" {
			owned[name] = struct{}{}
		}
	}
	live := make(map[string]struct{}, len(owned))
	for name := range owned {
		fact, err := engine.readHandoffRetentionFact(name)
		if err == nil && fact.terminalKnown {
			// An owner that has already been finalized is not a run still
			// writing here. An ownership release the helper had to defer --
			// a retryable inventory failure leaves the record in place -- used
			// to keep its volume live forever, so a run whose Delete had
			// published a terminal time never expired at all.
			//
			// A valid receipt is exactly the proof needed: Delete publishes
			// one only after the task is reaped and the attempt's absence is
			// independently verified, and repair is create-only and skips
			// every owned volume, so no other writer can produce one while an
			// owner is registered. Reuse stays consistent because Run
			// registers ownership and only then supersedes the receipt, so a
			// rerun is owned and receiptless -- live -- from that moment.
			continue
		}
		live[name] = struct{}{}
	}
	return live, nil
}

func (engine *ContainerdEngine) handoffVolumeNames() ([]string, error) {
	entries, err := readDirectoryIfPresent(engine.handoffVolumeRoot())
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), handoffVolumeNamePrefix) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// reconcileHandoffRetention is the sweep's whole handoff pass, and it runs
// only once the node has proved its previous workloads stopped.
//
// Order matters and is the point. Reaping surviving tasks is mandatory and
// happens first, in Sweep; this runs afterwards, against the post-reap
// inventory, because a terminal time taken while a workload is still writing
// is a lie the node then acts on for seven days. If anything of the runtime
// survived the reap, nothing here runs at all.
//
// Everything here is best-effort per volume. A node upgraded with receiptless
// volumes on a full filesystem used to fail on the first temporary receipt and
// abort the whole pass -- before freeing the already-expired volumes that
// would have made room -- and then do it again on the next boot. One volume's
// failure now costs that volume its receipt until the next sweep, and nothing
// else.
func (engine *ContainerdEngine) reconcileHandoffRetention(ctx context.Context, now time.Time, remaining ResourceInventory) {
	if surviving := len(remaining.Tasks) + len(remaining.Containers) + len(remaining.Shims); surviving != 0 {
		log.Printf("handoff retention: %d runtime resource(s) survived the sweep, so no handoff volume is stamped or expired this pass", surviving)
		return
	}
	names, live, ok := engine.repairMissingHandoffRetentionReceipts(ctx, now)
	if !ok {
		return
	}
	engine.cleanupExpiredHandoffs(ctx, now, names, live)
	// The orphan pass re-lists rather than reusing the names above. Driving it
	// off the pre-cleanup observation meant a receipt whose removal failed
	// immediately after its volume was taken still counted as paired, so it
	// survived as an orphan -- runtime residue by the projection's own rule --
	// until another sweep.
	survivors, err := engine.handoffVolumeNames()
	if err != nil {
		log.Printf("handoff retention: re-read the handoff root after expiry: %v", err)
		return
	}
	if err := engine.removeOrphanHandoffRetentionReceipts(survivors); err != nil {
		log.Printf("handoff retention: remove receipts whose volume is gone: %v", err)
	}
	engine.collectDetachedHandoffTrees(ctx)
}

// repairMissingHandoffRetentionReceipts is the stamping half on its own, and
// it is separate because it has a second caller that deletes nothing.
//
// A receipt the filesystem refused used to wait for another boot: the sweep
// was the only thing that stamped, so a node that filled up mid-run kept the
// affected volumes non-expirable until somebody restarted the helper. The
// agent's hourly accounting read now runs this first, so the repair happens on
// the same schedule as the reading of it. Nothing here removes anything --
// expiry belongs to the sweep, and budget eviction to the agent -- so a read
// can never cost a node a run's results.
//
// It returns the volume names and the live set it computed, so the sweep's
// remaining work uses exactly the observation this pass acted on.
func (engine *ContainerdEngine) repairMissingHandoffRetentionReceipts(ctx context.Context, now time.Time) ([]string, map[string]struct{}, bool) {
	names, err := engine.handoffVolumeNames()
	if err != nil {
		log.Printf("handoff retention: read the handoff root: %v", err)
		return nil, nil, false
	}
	live, err := engine.liveHandoffVolumes()
	if err != nil {
		log.Printf("handoff retention: read durable attempt ownership: %v", err)
		return nil, nil, false
	}
	engine.stampMissingHandoffRetentionReceipts(ctx, now, names, live)
	// Publication deliberately rechecks liveness per volume. Refresh once more
	// for the report so an attempt that became live while repair was measuring
	// is not returned as idle from the earlier snapshot.
	live, err = engine.liveHandoffVolumes()
	if err != nil {
		log.Printf("handoff retention: refresh durable attempt ownership after repair: %v", err)
		return nil, nil, false
	}
	return names, live, true
}

// stampMissingHandoffRetentionReceipts converts crash residue into accounted,
// expirable state.
//
// A volume with no receipt is one an older helper left, or one whose attempt
// never reached finalization. It cannot be expired -- its mtime is the
// workload's to write -- so without this it would sit on the node forever.
// Stamping it at sweep time starts its retention window from a helper-owned
// timestamp, which is the whole point: residue becomes something the node can
// give back. It runs on every sweep, so a volume a previous pass could not
// stamp is repaired by the next one.
func (engine *ContainerdEngine) stampMissingHandoffRetentionReceipts(ctx context.Context, now time.Time, names []string, live map[string]struct{}) {
	budget := engine.newHandoffMeasureBudget()
	for _, name := range names {
		if ctx != nil && ctx.Err() != nil {
			// The caller's deadline bounds the whole pass, not one volume's
			// walk. A sweep or an accounting read that was cancelled stops
			// stamping rather than working through the rest of the node.
			return
		}
		if _, writing := live[name]; writing {
			continue
		}
		fact, err := engine.readHandoffRetentionFact(name)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				log.Printf("handoff retention: read %s's terminal time: %v", name, err)
			}
			continue
		}
		if fact.terminalKnown || !handoffReceiptGapIsRepairable(fact.anomaly) {
			// An operational read failure may clear on its own, and a receipt
			// that cannot be read may still be a valid one. Writing over it
			// would destroy a terminal time this node had already recorded.
			// Repair is create-only, so malformed and mismatched receipt paths
			// are reported but never replaced here either. Only absence is a
			// gap this read may fill.
			continue
		}
		if err := engine.stampOneHandoffRetentionReceipt(ctx, name, now, budget); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("handoff retention: stamp a terminal time on %s: %v (it keeps its files and the next sweep tries again)", name, err)
		}
	}
}

// handoffReceiptGapIsRepairable reports whether accounting may fill this gap.
// Repair never replaces any receipt path, even one it cannot validate.
func handoffReceiptGapIsRepairable(anomaly HandoffVolumeAnomaly) bool {
	return anomaly == HandoffAnomalyNoReceipt
}

// stampOneHandoffRetentionReceipt measures and publishes one volume against a
// budget the whole pass shares, so a node-wide migration is bounded rather
// than each volume being bounded on its own.
func (engine *ContainerdEngine) stampOneHandoffRetentionReceipt(ctx context.Context, name string, now time.Time, budget *handoffMeasureBudget) error {
	file, identity, err := engine.openHandoffVolume(name)
	if err != nil {
		return err
	}
	measurement := engine.measureHandoffVolume(ctx, name, file, nil, budget)
	if err := file.Close(); err != nil {
		return err
	}
	if engine.handoffRepairMeasured != nil {
		engine.handoffRepairMeasured(name)
	}
	return engine.publishHandoffRetentionReceiptCreateOnly(name, identity, now, measurement)
}

// publishHandoffRetentionReceiptCreateOnly is repair's commit point. The
// measurement happens before this lock. Under it, repair refreshes both live
// ownership sources, confirms that no receipt exists, and reopens the volume
// without following symlinks to bind publication to the measured identity.
// Delete uses publishHandoffRetentionReceipt instead and may replace this
// repair receipt with its authoritative terminal fact.
func (engine *ContainerdEngine) publishHandoffRetentionReceiptCreateOnly(name string, measured handoffInodeIdentity, terminalAt time.Time, measurement handoffVolumeMeasurement) error {
	engine.handoffRetentionMu.Lock()
	defer engine.handoffRetentionMu.Unlock()
	live, err := engine.liveHandoffVolumes()
	if err != nil {
		return fmt.Errorf("refresh live handoff ownership before repairing %s: %w", name, err)
	}
	if _, writing := live[name]; writing {
		log.Printf("handoff retention: skip repairing %s because a fresh ownership snapshot says it is live", name)
		return nil
	}
	receiptPath := engine.handoffRetentionPath(name)
	if _, err := os.Lstat(receiptPath); err == nil {
		log.Printf("handoff retention: skip repairing %s because a receipt now exists", name)
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check for a concurrent handoff receipt for %s: %w", name, err)
	}
	_, identity, err := engine.openHandoffVolumeIdentity(name)
	if err != nil {
		return err
	}
	if identity != measured {
		log.Printf("handoff retention: skip repairing %s because its identity changed after measurement", name)
		return nil
	}
	root := engine.handoffRetentionRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create handoff retention state root: %w", err)
	}
	published, err := engine.writeAtomicDurableJSONRecordCreateOnly(root, HandoffRetentionRecordName(name), handoffRetentionReceipt{
		Version: handoffRetentionReceiptVersion, Device: identity.device, Inode: identity.inode,
		TerminalAt: terminalAt.UTC(), LogicalBytes: measurement.logical,
		DedupedBytes: measurement.deduped, Entries: measurement.entries,
		Truncated: measurement.truncated,
	})
	if err != nil {
		return err
	}
	if !published {
		log.Printf("handoff retention: skip repairing %s because a receipt won create-only publication", name)
	}
	return nil
}

// removeOrphanHandoffRetentionReceipts keeps the durable class honest: a
// receipt is retained exactly while its volume is, so one standing over a
// volume that is gone is residue, and the union invariant would fail if it
// were left.
func (engine *ContainerdEngine) removeOrphanHandoffRetentionReceipts(volumes []string) error {
	entries, err := readDirectoryIfPresent(engine.handoffRetentionRoot())
	if err != nil {
		return err
	}
	present := make(map[string]struct{}, len(volumes))
	for _, name := range volumes {
		present[HandoffRetentionRecordName(name)] = struct{}{}
	}
	var failures []error
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") && strings.Contains(name, ".tmp-") && !entry.IsDir() && entry.Type()&os.ModeSymlink == 0 {
			// A crash between writing a receipt and publishing it leaves the
			// temporary behind. It carries no volume prefix, so the pairing
			// below can never reach it, and it would accumulate on the node
			// forever -- the same reason the attempt-ownership root collects
			// its own `.attempt.tmp-` leftovers.
			engine.handoffRetentionMu.Lock()
			err := os.Remove(filepath.Join(engine.handoffRetentionRoot(), name))
			engine.handoffRetentionMu.Unlock()
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				failures = append(failures, err)
			}
			continue
		}
		if !strings.HasPrefix(name, handoffVolumeNamePrefix) || !strings.HasSuffix(name, handoffRetentionRecordSuffix) {
			continue
		}
		if _, paired := present[name]; paired {
			continue
		}
		engine.handoffRetentionMu.Lock()
		err := os.Remove(filepath.Join(engine.handoffRetentionRoot(), name))
		engine.handoffRetentionMu.Unlock()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// handoffVolumeExpired is the single predicate for "this volume's retention
// window has run out", and it is deliberately the same one the runtime-absence
// projection reads, live-attempt guard included. A volume the projection calls
// retained is exactly one the sweep will not remove.
func (engine *ContainerdEngine) handoffVolumeExpired(name string, now time.Time, live map[string]struct{}) (bool, error) {
	if _, writing := live[name]; writing {
		return false, nil
	}
	fact, err := engine.readHandoffRetentionFact(name)
	if errors.Is(err, os.ErrNotExist) {
		// A concurrently removed entry cannot be retained evidence.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !fact.terminalKnown {
		// Never expired on a timestamp the workload could have written. The
		// sweep stamps a receipt for this volume, and the window runs from
		// there.
		return false, nil
	}
	return now.Sub(fact.terminalAt) >= engine.handoffRetention(), nil
}

// cleanupExpiredHandoffs removes the handoff volumes whose retention window has
// run out, and it runs from the helper-owned receipt rather than the directory
// mtime.
//
// One volume it cannot read is one volume it does not remove. It used to
// abort, which meant a single directory replaced by a symlink stopped a node
// expiring anything at all.
func (engine *ContainerdEngine) cleanupExpiredHandoffs(ctx context.Context, now time.Time, names []string, live map[string]struct{}) {
	for _, name := range names {
		expired, err := engine.handoffVolumeExpired(name, now, live)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				log.Printf("handoff retention: decide whether %s has expired: %v (it keeps its files)", name, err)
			}
			continue
		}
		if !expired {
			continue
		}
		if err := engine.removeHandoffVolumeAndReceipt(ctx, name); err != nil {
			log.Printf("handoff retention: remove expired %s: %v", name, err)
		}
	}
}

// InventoryHandoffVolumes reports every handoff volume this node still holds.
//
// One pass, one (device, inode) map: a file two volumes hard-link is charged
// to whichever this pass reached first, so the node total counts it once. The
// per-volume logical figure counts it in both, because that is what trimming
// one of them would recover.
//
// No owner key crosses this boundary in either direction. The agent derives
// every name it knows from its own runs, so a name here it cannot map back is
// the crash-residue signal, and no identity the helper holds leaves it.
//
// The response is bounded in *encoded bytes*, not in rows. A frame over
// MaxFrameBytes is not a shorter answer: the transport refuses it, the server
// discards the write error and closes the connection, and the client reads
// that as a lost session -- so a node with enough retained results would have
// lost its runtime every time it tried to count them.
func (engine *ContainerdEngine) InventoryHandoffVolumes(ctx context.Context, _ InventoryHandoffVolumesRequest) (InventoryHandoffVolumesResponse, error) {
	// Repair before reporting. The boot sweep used to be the only thing that
	// stamped a missing receipt, so a receipt the filesystem refused stayed
	// missing until somebody restarted the helper -- and a volume with no
	// receipt is one the node can never expire. The agent reads this on its
	// hourly accounting pass, which is the right cadence for the repair too.
	// This is create-only, skips volumes a live attempt holds, and removes
	// nothing at all: expiry is the sweep's and eviction is the agent's.
	names, live, ok := engine.repairMissingHandoffRetentionReceipts(ctx, engine.handoffNow())
	if !ok {
		return InventoryHandoffVolumesResponse{}, errors.New("the handoff root could not be read")
	}
	response := InventoryHandoffVolumesResponse{Volumes: make([]RetainedHandoffVolume, 0, len(names))}
	if len(names) > MaxInventoriedHandoffVolumes {
		names = names[:MaxInventoriedHandoffVolumes]
		response.Exhausted = true
	}
	seen := make(map[handoffInodeIdentity]struct{})
	budget := engine.newHandoffMeasureBudget()
	encoded := len(`{"volumes":[],"exhausted":false}`)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			response.Exhausted = true
			return response, nil
		}
		volume, present := engine.retainedHandoffVolume(ctx, name, live, seen, budget)
		if !present {
			continue
		}
		size, err := json.Marshal(volume)
		if err != nil {
			return InventoryHandoffVolumesResponse{}, err
		}
		if encoded+len(size)+1 > engine.handoffInventoryByteBudget() {
			response.Exhausted = true
			break
		}
		encoded += len(size) + 1
		response.Volumes = append(response.Volumes, volume)
	}
	return response, nil
}

// retainedHandoffVolume reads and measures one volume. A volume that went away
// between the listing and the read is not on the node any more, so it is not
// in the node's figures at all, which `present` says.
func (engine *ContainerdEngine) retainedHandoffVolume(ctx context.Context, name string, live map[string]struct{}, seen map[handoffInodeIdentity]struct{}, budget *handoffMeasureBudget) (RetainedHandoffVolume, bool) {
	volume := RetainedHandoffVolume{Name: name}
	if _, writing := live[name]; writing {
		volume.Live = true
	}
	fact, err := engine.readHandoffRetentionFact(name)
	if errors.Is(err, os.ErrNotExist) {
		return RetainedHandoffVolume{}, false
	}
	measurement := handoffVolumeMeasurement{}
	if err != nil {
		measurement.note(HandoffAnomalyVolumeUnreadable)
		volume.Anomalies = measurement.anomalies
		return volume, true
	}
	volume.TerminalAt, volume.TerminalKnown = fact.terminalAt.UTC(), fact.terminalKnown
	if fact.anomaly != "" {
		measurement.note(fact.anomaly)
	}
	file, _, err := engine.openHandoffVolume(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RetainedHandoffVolume{}, false
		}
		measurement.note(HandoffAnomalyVolumeUnreadable)
		volume.Anomalies = measurement.anomalies
		return volume, true
	}
	walked := engine.measureHandoffVolume(ctx, name, file, seen, budget)
	if err := file.Close(); err != nil {
		walked.note(HandoffAnomalyVolumeUnreadable)
	}
	for _, anomaly := range walked.anomalies {
		measurement.note(anomaly)
	}
	volume.LogicalBytes, volume.DedupedBytes, volume.Entries = walked.logical, walked.deduped, walked.entries
	volume.Truncated, volume.Anomalies = walked.truncated, measurement.anomalies
	return volume, true
}

// measureHandoffVolume walks one volume through a descriptor that is already
// open.
//
// Every fact about an entry is taken relative to the still-open descriptor of
// the directory it was listed in -- `Fstatat` with AT_SYMLINK_NOFOLLOW, never
// a pathname -- and every subdirectory is entered with `openat` from that same
// descriptor, no-follow, with its identity re-checked against what was just
// observed. An earlier draft read names, closed the directory, and then called
// `DirEntry.Info()`, which resolves by pathname: a workload that replaced an
// enumerated ancestor with a symlink in that window would have had the helper
// stat outside the tree it had pinned.
//
// One descriptor is held per level and none is released while its children are
// being measured, so a queued ancestor can never be re-opened into something
// else. Depth is bounded, so this costs at most maxHandoffMeasureDepth open
// descriptors.
//
// `seen`, when it is supplied, spans a whole pass over the handoff root, which
// is what makes the deduped figure a node figure rather than a per-volume one;
// a nil map measures this volume alone, which is what a receipt records.
func (engine *ContainerdEngine) measureHandoffVolume(ctx context.Context, name string, volume *os.File, seen map[handoffInodeIdentity]struct{}, budget *handoffMeasureBudget) handoffVolumeMeasurement {
	measurement := handoffVolumeMeasurement{}
	if engine.handoffMeasureEntered != nil {
		engine.handoffMeasureEntered(name)
	}
	duplicate, err := unix.Dup(int(volume.Fd()))
	if err != nil {
		measurement.note(HandoffAnomalyVolumeUnreadable)
		return measurement
	}
	directory := os.NewFile(uintptr(duplicate), volume.Name())
	engine.walkHandoffDirectory(ctx, name, directory, 0, seen, budget, &measurement)
	if err := directory.Close(); err != nil {
		measurement.note(HandoffAnomalyVolumeUnreadable)
	}
	if measurement.truncated {
		measurement.note(HandoffAnomalyMeasurementTruncated)
	}
	return measurement
}

func (engine *ContainerdEngine) walkHandoffDirectory(ctx context.Context, volume string, directory *os.File, depth int, seen map[handoffInodeIdentity]struct{}, budget *handoffMeasureBudget, measurement *handoffVolumeMeasurement) {
	descriptor := int(directory.Fd())
	for {
		if ctx != nil && ctx.Err() != nil {
			measurement.truncated = true
			return
		}
		names, err := directory.Readdirnames(handoffReadChunk)
		if err != nil && !errors.Is(err, io.EOF) {
			measurement.note(HandoffAnomalyVolumeUnreadable)
			return
		}
		for _, entry := range names {
			if budget.entries <= 0 {
				measurement.truncated = true
				return
			}
			budget.entries--
			measurement.entries++
			var stat unix.Stat_t
			if unix.Fstatat(descriptor, entry, &stat, unix.AT_SYMLINK_NOFOLLOW) != nil {
				// Gone between the listing and the stat, or unreadable. Either
				// way there is nothing here to count.
				continue
			}
			switch stat.Mode & unix.S_IFMT {
			case unix.S_IFDIR:
				engine.descendHandoffDirectory(ctx, volume, descriptor, entry, stat, depth, seen, budget, measurement)
			case unix.S_IFREG:
				measurement.logical += stat.Size
				if chargeHandoffInode(stat, seen) {
					measurement.deduped += stat.Size
				}
			}
		}
		if errors.Is(err, io.EOF) || len(names) == 0 {
			return
		}
	}
}

func (engine *ContainerdEngine) descendHandoffDirectory(ctx context.Context, volume string, parent int, entry string, observed unix.Stat_t, depth int, seen map[handoffInodeIdentity]struct{}, budget *handoffMeasureBudget, measurement *handoffVolumeMeasurement) {
	if depth+1 >= maxHandoffMeasureDepth || budget.opens <= 0 {
		measurement.truncated = true
		return
	}
	budget.opens--
	if engine.handoffMeasureDescend != nil {
		engine.handoffMeasureDescend(volume, entry)
	}
	child, err := unix.Openat(parent, entry, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		// It was a directory a moment ago and is not one now, or cannot be
		// entered. It is counted as an entry and measured as nothing.
		measurement.note(HandoffAnomalySubtreeReplaced)
		return
	}
	var actual unix.Stat_t
	if unix.Fstat(child, &actual) != nil || actual.Dev != observed.Dev || actual.Ino != observed.Ino {
		// A different directory now stands where this pass was about to
		// measure. Measuring it would attribute another tree's bytes to this
		// volume, so it is reported and not measured.
		unix.Close(child)
		measurement.note(HandoffAnomalySubtreeReplaced)
		return
	}
	file := os.NewFile(uintptr(child), entry)
	engine.walkHandoffDirectory(ctx, volume, file, depth+1, seen, budget, measurement)
	if err := file.Close(); err != nil {
		measurement.note(HandoffAnomalyVolumeUnreadable)
	}
}

// chargeHandoffInode reports whether this file's bytes belong in the deduped
// figure. A file with one link is always its own; a file with several is
// charged to whichever entry of the pass reached it first.
func chargeHandoffInode(stat unix.Stat_t, seen map[handoffInodeIdentity]struct{}) bool {
	if stat.Nlink <= 1 || seen == nil {
		return true
	}
	identity := handoffInodeIdentity{device: uint64(stat.Dev), inode: stat.Ino}
	if _, counted := seen[identity]; counted {
		return false
	}
	seen[identity] = struct{}{}
	return true
}

// writeAtomicDurableJSONRecord is writeAtomicDurableOwnerRecord's shape for a
// second durable class: versioned bytes, 0600, fsynced, renamed into place,
// and the directory fsynced after, so a crash leaves a whole record or none.
func writeAtomicDurableJSONRecord(root, name string, record any) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(root, "."+name+".tmp-")
	if err != nil {
		return fmt.Errorf("create durable record %q: %w", name, err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	writeErr := temporary.Chmod(0o600)
	if writeErr == nil {
		_, writeErr = temporary.Write(payload)
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	writeErr = errors.Join(writeErr, temporary.Close())
	if writeErr != nil {
		return fmt.Errorf("write durable record %q: %w", name, writeErr)
	}
	if err := os.Rename(temporaryName, filepath.Join(root, name)); err != nil {
		return fmt.Errorf("publish durable record %q: %w", name, err)
	}
	directory, err := os.Open(root)
	if err != nil {
		return fmt.Errorf("open durable record root %q: %w", root, err)
	}
	return errors.Join(directory.Sync(), directory.Close())
}

// writeAtomicDurableJSONRecordCreateOnly publishes repair evidence without
// replacing anything already at name. renameat2 provides the atomic Linux
// primitive; filesystems or kernels that do not support it use a same-directory
// hard link followed by unlinking the temporary name, which has the same
// no-replace property.
func (engine *ContainerdEngine) writeAtomicDurableJSONRecordCreateOnly(root, name string, record any) (bool, error) {
	return writeAtomicDurableJSONRecordCreateOnlyWithRename(root, name, record, engine.handoffRepairWrite, renameNoReplace)
}

// writeAtomicDurableJSONRecordCreateOnlyWithRename takes its write and publish
// steps as arguments so a test can drive the outcomes the filesystem decides:
// a partial write, and a link fallback that creates the name and then cannot
// unlink the temporary.
func writeAtomicDurableJSONRecordCreateOnlyWithRename(root, name string, record any, write func(*os.File, []byte) error, publish func(string, string) (bool, error)) (bool, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return false, err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(root, "."+name+".tmp-")
	if err != nil {
		return false, fmt.Errorf("create durable record %q: %w", name, err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	writeErr := temporary.Chmod(0o600)
	if writeErr == nil {
		if write != nil {
			writeErr = write(temporary, payload)
		} else {
			_, writeErr = temporary.Write(payload)
		}
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	writeErr = errors.Join(writeErr, temporary.Close())
	if writeErr != nil {
		return false, fmt.Errorf("write durable record %q: %w", name, writeErr)
	}
	path := filepath.Join(root, name)
	published, publishErr := publish(temporaryName, path)
	if !published {
		if publishErr != nil {
			return false, fmt.Errorf("publish durable record %q create-only: %w", name, publishErr)
		}
		return false, nil
	}
	// Published is published. The link fallback can succeed at creating the
	// name and then fail to unlink the temporary, and returning that as a
	// plain failure skipped the directory fsync on a record that is now on
	// disk -- so a crash could lose a receipt the helper had already
	// committed. The unlink failure is still reported, joined rather than
	// masking the publication.
	directory, err := os.Open(root)
	if err != nil {
		return true, errors.Join(publishErr, fmt.Errorf("open durable record root %q: %w", root, err))
	}
	return true, errors.Join(publishErr, directory.Sync(), directory.Close())
}

func renameNoReplace(oldPath, newPath string) (bool, error) {
	err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, unix.EEXIST) {
		return false, nil
	}
	if !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.EOPNOTSUPP) {
		return false, err
	}
	if err := os.Link(oldPath, newPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, err
	}
	if err := os.Remove(oldPath); err != nil {
		return true, err
	}
	return true, nil
}
