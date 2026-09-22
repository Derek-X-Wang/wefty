//go:build linux

package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	// maxHandoffMeasureEntries bounds one volume's measurement. A workload
	// that plants a tree deeper or wider than this gets a per-volume anomaly
	// and a floor for its figures; it does not get to stall a node-wide call.
	maxHandoffMeasureEntries = 1 << 20
	// maxHandoffMeasureDepth bounds how deep one volume is walked, for the
	// same reason.
	maxHandoffMeasureDepth = 64
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
}

type handoffInodeIdentity struct {
	device uint64
	inode  uint64
}

type handoffVolumeMeasurement struct {
	logical int64
	deduped int64
	entries int64
	anomaly string
}

// handoffRetentionFact is one volume's retention state as the helper reads it
// now: the terminal time when a bound receipt says so, and otherwise the
// directory's own mtime, labelled as the fallback it is.
type handoffRetentionFact struct {
	terminalAt    time.Time
	terminalKnown bool
	receipt       handoffRetentionReceipt
	anomaly       string
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

// readHandoffRetentionFact answers what this volume's retention runs from.
//
// An unreadable, invalid, or mismatched receipt is never an error that fails a
// node-wide call: it comes back as terminalKnown=false plus an anomaly string,
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
		fact.anomaly = "no helper-owned retention receipt; the directory mtime is a workload-writable fallback"
		return fact, nil
	}
	if err != nil {
		fact.anomaly = "retention receipt could not be read: " + err.Error()
		return fact, nil
	}
	var receipt handoffRetentionReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil || receipt.Version != handoffRetentionReceiptVersion {
		fact.anomaly = "retention receipt is not a version-1 helper receipt"
		return fact, nil
	}
	if receipt.Device != identity.device || receipt.Inode != identity.inode {
		// The record is evidence and the descriptor is authority. A receipt
		// naming a directory that is no longer the one standing at this name
		// says nothing about the bytes that are there now.
		fact.anomaly = "retention receipt names a different directory than the one standing at this volume's name"
		return fact, nil
	}
	if receipt.TerminalAt.IsZero() {
		fact.anomaly = "retention receipt carries no terminal time"
		return fact, nil
	}
	fact.receipt = receipt
	fact.terminalAt = receipt.TerminalAt
	fact.terminalKnown = true
	return fact, nil
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

func (engine *ContainerdEngine) handoffRetentionPath(volume string) string {
	return filepath.Join(engine.handoffRetentionRoot(), HandoffRetentionRecordName(volume))
}

// writeHandoffRetentionReceipt stamps one volume's terminal time.
//
// It is called from Delete after the attempt's task has been reaped and its
// absence independently verified -- the namespace is quiescent and no workload
// can still be writing here -- and from the sweep for a volume that has no
// receipt at all. Writing it is versioned, fsynced and write-then-rename, so a
// crash leaves either the old receipt or the new one.
func (engine *ContainerdEngine) writeHandoffRetentionReceipt(name string, terminalAt time.Time) error {
	if name == "" {
		return nil
	}
	engine.handoffRetentionMu.Lock()
	defer engine.handoffRetentionMu.Unlock()
	file, identity, err := engine.openHandoffVolume(name)
	if err != nil {
		return err
	}
	measurement := measureHandoffVolume(file, nil)
	closeErr := file.Close()
	if closeErr != nil {
		return closeErr
	}
	root := engine.handoffRetentionRoot()
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create handoff retention state root: %w", err)
	}
	return writeAtomicDurableJSONRecord(root, HandoffRetentionRecordName(name), handoffRetentionReceipt{
		Version: handoffRetentionReceiptVersion, Device: identity.device, Inode: identity.inode,
		TerminalAt: terminalAt.UTC(), LogicalBytes: measurement.logical,
		DedupedBytes: measurement.deduped, Entries: measurement.entries,
	})
}

func (engine *ContainerdEngine) removeHandoffRetentionReceipt(name string) error {
	engine.handoffRetentionMu.Lock()
	defer engine.handoffRetentionMu.Unlock()
	if err := os.Remove(engine.handoffRetentionPath(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// liveHandoffVolumes is the set of handoff volumes a live attempt of this
// session is still writing into. Nothing stamps a terminal time on one of
// these, and the inventory reports them as live so no budget ever treats an
// unfinished run's bytes as an eviction candidate.
func (engine *ContainerdEngine) liveHandoffVolumes() map[string]struct{} {
	live := make(map[string]struct{})
	engine.mu.Lock()
	defer engine.mu.Unlock()
	for _, attempt := range engine.attempts {
		if name := attempt.resources.HandoffVolumeDirectory; name != "" {
			live[name] = struct{}{}
		}
	}
	return live
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

// stampMissingHandoffRetentionReceipts converts crash residue into accounted,
// expirable state.
//
// A volume with no receipt is one an older helper left, or one whose attempt
// never reached finalization. It cannot be expired -- its mtime is the
// workload's to write -- so without this it would sit on the node forever.
// Stamping it at sweep time starts its retention window from a helper-owned
// timestamp, which is the whole point: residue becomes something the node can
// give back.
func (engine *ContainerdEngine) stampMissingHandoffRetentionReceipts(now time.Time) error {
	names, err := engine.handoffVolumeNames()
	if err != nil {
		return err
	}
	live := engine.liveHandoffVolumes()
	for _, name := range names {
		if _, writing := live[name]; writing {
			continue
		}
		fact, err := engine.readHandoffRetentionFact(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			// A volume whose descriptor cannot be taken is not one to stamp a
			// terminal time on. It stays visible in the inventory with its
			// anomaly.
			continue
		}
		if fact.terminalKnown {
			continue
		}
		if err := engine.writeHandoffRetentionReceipt(name, now); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return engine.removeOrphanHandoffRetentionReceipts(names)
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
	for _, entry := range entries {
		name := entry.Name()
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
			return err
		}
	}
	return nil
}

// handoffVolumeExpired is the single predicate for "this volume's retention
// window has run out", and it is deliberately the same one the runtime-absence
// projection reads. A volume the projection calls retained is exactly one this
// will not remove.
func (engine *ContainerdEngine) handoffVolumeExpired(name string, now time.Time) (bool, error) {
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
func (engine *ContainerdEngine) InventoryHandoffVolumes(ctx context.Context, _ InventoryHandoffVolumesRequest) (InventoryHandoffVolumesResponse, error) {
	names, err := engine.handoffVolumeNames()
	if err != nil {
		return InventoryHandoffVolumesResponse{}, err
	}
	response := InventoryHandoffVolumesResponse{Volumes: make([]RetainedHandoffVolume, 0, len(names))}
	if len(names) > MaxInventoriedHandoffVolumes {
		names = names[:MaxInventoriedHandoffVolumes]
		response.Exhausted = true
	}
	live := engine.liveHandoffVolumes()
	seen := make(map[handoffInodeIdentity]struct{})
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			response.Exhausted = true
			return response, nil
		}
		volume := RetainedHandoffVolume{Name: name}
		if _, writing := live[name]; writing {
			volume.Live = true
		}
		fact, err := engine.readHandoffRetentionFact(name)
		if errors.Is(err, os.ErrNotExist) {
			// Removed between the listing and the read. It is not on the node
			// any more, so it is not in the node's figures.
			continue
		}
		if err != nil {
			volume.Anomaly = "handoff volume could not be opened as a helper-owned directory: " + err.Error()
			response.Volumes = append(response.Volumes, volume)
			continue
		}
		volume.TerminalAt, volume.TerminalKnown, volume.Anomaly = fact.terminalAt.UTC(), fact.terminalKnown, fact.anomaly
		file, _, err := engine.openHandoffVolume(name)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				volume.Anomaly = joinHandoffAnomaly(volume.Anomaly, "handoff volume could not be measured: "+err.Error())
				response.Volumes = append(response.Volumes, volume)
			}
			continue
		}
		measurement := measureHandoffVolume(file, seen)
		if err := file.Close(); err != nil {
			measurement.anomaly = joinHandoffAnomaly(measurement.anomaly, "handoff volume descriptor could not be closed: "+err.Error())
		}
		volume.LogicalBytes, volume.DedupedBytes, volume.Entries = measurement.logical, measurement.deduped, measurement.entries
		volume.Anomaly = joinHandoffAnomaly(volume.Anomaly, measurement.anomaly)
		response.Volumes = append(response.Volumes, volume)
	}
	return response, nil
}

func joinHandoffAnomaly(left, right string) string {
	switch {
	case left == "":
		return right
	case right == "":
		return left
	default:
		return left + "; " + right
	}
}

// measureHandoffVolume walks one volume through a descriptor that is already
// open, breadth-first, holding exactly one directory handle at a time.
//
// Symlinks are never followed and never descended into, so a workload cannot
// point the measurement at the rest of the node. `seen`, when it is supplied,
// spans a whole pass over the handoff root, which is what makes the deduped
// figure a node figure rather than a per-volume one; a nil map measures this
// volume alone, which is what a receipt records.
func measureHandoffVolume(volume *os.File, seen map[handoffInodeIdentity]struct{}) handoffVolumeMeasurement {
	measurement := handoffVolumeMeasurement{}
	type frame struct {
		path  string
		depth int
	}
	pending := []frame{{path: ".", depth: 0}}
	for len(pending) > 0 {
		current := pending[0]
		pending = pending[1:]
		directory, err := openHandoffSubdirectory(volume, current.path)
		if err != nil {
			measurement.anomaly = joinHandoffAnomaly(measurement.anomaly, "a directory under this volume could not be read: "+err.Error())
			continue
		}
		children, readErr := directory.ReadDir(-1)
		closeErr := directory.Close()
		if readErr != nil {
			measurement.anomaly = joinHandoffAnomaly(measurement.anomaly, "a directory under this volume could not be read: "+readErr.Error())
		}
		if closeErr != nil {
			measurement.anomaly = joinHandoffAnomaly(measurement.anomaly, "a directory handle under this volume could not be closed: "+closeErr.Error())
		}
		for _, child := range children {
			if measurement.entries >= maxHandoffMeasureEntries {
				measurement.anomaly = joinHandoffAnomaly(measurement.anomaly, "measurement stopped at its entry bound; these figures are a floor")
				return measurement
			}
			measurement.entries++
			info, err := child.Info()
			if err != nil {
				// An entry that went away between the listing and the stat is
				// not on the node any more.
				continue
			}
			if info.Mode()&os.ModeSymlink != 0 {
				continue
			}
			if info.IsDir() {
				if current.depth+1 >= maxHandoffMeasureDepth {
					measurement.anomaly = joinHandoffAnomaly(measurement.anomaly, "measurement stopped at its depth bound; these figures are a floor")
					continue
				}
				pending = append(pending, frame{path: filepath.Join(current.path, child.Name()), depth: current.depth + 1})
				continue
			}
			if !info.Mode().IsRegular() {
				continue
			}
			measurement.logical += info.Size()
			if !chargeHandoffInode(info, seen) {
				continue
			}
			measurement.deduped += info.Size()
		}
	}
	return measurement
}

// chargeHandoffInode reports whether this file's bytes belong in the deduped
// figure. A file with one link is always its own; a file with several is
// charged to whichever entry of the pass reached it first.
func chargeHandoffInode(info os.FileInfo, seen map[handoffInodeIdentity]struct{}) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink <= 1 || seen == nil {
		return true
	}
	identity := handoffInodeIdentity{device: uint64(stat.Dev), inode: stat.Ino}
	if _, counted := seen[identity]; counted {
		return false
	}
	seen[identity] = struct{}{}
	return true
}

// openHandoffSubdirectory descends by descriptor from the volume's own handle,
// one component at a time, refusing a symlink at every component. The volume
// is workload-writable, so a path resolved by name would be a path the
// workload chooses.
func openHandoffSubdirectory(volume *os.File, path string) (*os.File, error) {
	if path == "." {
		duplicate, err := unix.Dup(int(volume.Fd()))
		if err != nil {
			return nil, err
		}
		return os.NewFile(uintptr(duplicate), volume.Name()), nil
	}
	current := int(volume.Fd())
	opened := -1
	defer func() {
		if opened >= 0 {
			unix.Close(opened)
		}
	}()
	for _, component := range strings.Split(path, string(filepath.Separator)) {
		next, err := unix.Openat(current, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		if opened >= 0 {
			unix.Close(opened)
		}
		opened, current = next, next
	}
	file := os.NewFile(uintptr(opened), filepath.Join(volume.Name(), path))
	opened = -1
	return file, nil
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
