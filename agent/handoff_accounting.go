package agent

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// handoffChargedEntryFloor is what one directory entry costs the node figure
// however little it holds.
//
// Logical bytes are the right unit for the per-run bound -- it trims files, so
// it has to count what trimming recovers -- and the wrong one for a node. A
// million empty files are ~0 logical bytes and are a real node out of inodes,
// a directory read that takes minutes, and a backup that never finishes. The
// node figure therefore puts a floor under every entry, so that tree is charged
// roughly 4 GiB and a node budget can see it. The two units are deliberately
// different and are reported separately; neither is derived from the other.
const handoffChargedEntryFloor = 4 << 10

// handoffTally is what one walk over retained results adds up.
type handoffTally struct {
	logical int64
	charged int64
	entries int64
}

// add records one directory entry.
//
// logical is the entry's own logical bytes: the length of a regular file, and
// zero for everything else -- a directory, a symlink (never followed: a link
// into the node is not this run's storage), a socket, a device, and a second
// name for a file this pass has already counted.
//
// charged is that same number with the entry floor under it, because an entry
// is an inode and a directory slot whatever it contains, including the second
// link -- the data is charged once, the name it takes is charged every time.
func (tally *handoffTally) add(logical int64) {
	tally.entries++
	tally.logical += logical
	if logical < handoffChargedEntryFloor {
		logical = handoffChargedEntryFloor
	}
	tally.charged += logical
}

// handoffWalkFrame is one directory the walk has read and still has
// subdirectories to descend into. It holds no path: every level is reached
// through the handle of the level above it.
type handoffWalkFrame struct {
	run *os.Root
	// directories are the subdirectory names not yet descended into. A frame
	// with none is closed and dropped rather than carried, which is what keeps
	// a deep tree cheap.
	directories []string
}

// walkHandoffTree sums one directory entry and, when it is a directory,
// everything beneath it. It never follows a symlink and reaches every level
// through the directory handle above it rather than by pathname.
//
// It is iterative because it used to recurse and the depth it recursed to was
// the workload's to choose. That was one run's problem while only the per-run
// bound measured; node accounting walks every retained run on the node, so an
// adversarial tree became a node-wide stall. A depth cap instead of iteration
// would have been worse than the recursion: it stops counting and says nothing,
// which is exactly the silent under-count this slice exists to remove.
//
// The stack holds open directory handles, but only for a directory that still
// has a subdirectory left: the frame whose last subdirectory is being opened is
// dropped and closed as part of opening it. A chain of ten thousand
// directories therefore costs one open handle and a stack of one, and forcing a
// second handle costs the workload a second subtree at that level -- so the
// handles a tree can demand grow with the logarithm of the directories it
// holds, not with its depth.
//
// seen, when non-nil, is the pass's cross-run file identity (handoffInodeOf).
// Passing nil counts every name, which is what the per-run bound wants: it
// trims names, and part 1 states its unit as logical bytes per link.
func walkHandoffTree(parent *os.Root, name string, info os.FileInfo, seen map[handoffInode]struct{}) (handoffTally, error) {
	var tally handoffTally
	tally.add(handoffLogicalBytes(info, seen))
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return tally, nil
	}
	top, err := openHandoffDirectory(parent, name)
	if err != nil {
		if handoffWalkSkips(err) {
			return tally, nil
		}
		return tally, err
	}
	var stack []*handoffWalkFrame
	defer func() {
		for _, frame := range stack {
			frame.run.Close()
		}
	}()
	push := func(run *os.Root) error {
		directories, err := tallyHandoffChildren(run, &tally, seen)
		if err != nil {
			run.Close()
			return err
		}
		if len(directories) == 0 {
			return run.Close()
		}
		stack = append(stack, &handoffWalkFrame{run: run, directories: directories})
		return nil
	}
	if err := push(top); err != nil {
		return tally, err
	}
	for len(stack) > 0 {
		frame := stack[len(stack)-1]
		child := frame.directories[0]
		frame.directories = frame.directories[1:]
		run, err := openHandoffDirectory(frame.run, child)
		if len(frame.directories) == 0 {
			// Nothing here needs this handle again. Dropping it now, rather
			// than when the subtree returns, is the whole reason a chain of
			// directories costs one descriptor instead of one per level.
			stack = stack[:len(stack)-1]
			frame.run.Close()
		}
		if err != nil {
			if handoffWalkSkips(err) {
				continue
			}
			return tally, err
		}
		if err := push(run); err != nil {
			return tally, err
		}
	}
	return tally, nil
}

// handoffWalkSkips reports the two ways a directory can stop being one while a
// pass is reading it. A workload owns these files and may remove or replace
// them mid-pass; neither is a reason to abandon measuring the node. What the
// entry was worth is already counted, and the next pass sees whatever is there
// then.
func handoffWalkSkips(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, errHandoffNameNotADirectory)
}

// tallyHandoffChildren adds every child of one directory to the tally and
// returns the subdirectories still to descend into.
func tallyHandoffChildren(run *os.Root, tally *handoffTally, seen map[handoffInode]struct{}) ([]string, error) {
	directory, err := run.Open(".")
	if err != nil {
		return nil, err
	}
	children, err := directory.ReadDir(-1)
	directory.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read handoff directory: %w", err)
	}
	var directories []string
	for _, child := range children {
		info, err := run.Lstat(child.Name())
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("inspect %q: %w", child.Name(), err)
		}
		tally.add(handoffLogicalBytes(info, seen))
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			directories = append(directories, child.Name())
		}
	}
	return directories, nil
}

// handoffLogicalBytes reports what one entry adds to the logical figure, and
// consumes the pass's file identity while doing it.
func handoffLogicalBytes(info os.FileInfo, seen map[handoffInode]struct{}) int64 {
	if !info.Mode().IsRegular() {
		return 0
	}
	if seen != nil {
		if identity, shared := handoffInodeOf(info); shared {
			if _, counted := seen[identity]; counted {
				return 0
			}
			seen[identity] = struct{}{}
		}
	}
	return info.Size()
}

// measureNode is the accounting pass: what this node is holding in retained
// results, in both units, across every run it has a record for.
//
// It enforces nothing. There is no node budget here and nothing is evicted;
// this slice's whole job is that the figure a budget will need exists, is
// right, and is visible before anything acts on it.
//
// Every record is measured, including a quarantined one. Quarantine means "the
// sweep cannot safely delete this", which is the opposite of a reason to stop
// charging it: a run whose name a workload replaced with a symlink still has
// its files on the node, and excluding it would make quarantine a way to hide
// storage from the accounting.
//
// Records are re-read here rather than reused from the expiry loop above,
// because expiry removes some of them and the figure has to be what is left.
//
// One `seen` map spans the whole pass, so two runs that hard-link one file are
// charged its bytes once. Part 1 charged them once per link, per run, which
// overstated a node by however many links it held.
func (m *handoffManager) measureNode(root *os.Root, now time.Time) RetainedResultsStatus {
	status := RetainedResultsStatus{MeasuredAt: now}
	seen := make(map[handoffInode]struct{})
	recorded := make(map[string]struct{})
	for _, record := range m.loadRecords() {
		recorded[record.RunID] = struct{}{}
		status.Runs++
		if record.RetainUntil.IsZero() {
			status.InFlight++
		}
		if record.Quarantine != "" {
			status.QuarantinedRecords++
		}
		info, err := root.Lstat(record.RunID)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				m.log("agent: measure run %s's retained results: %v", record.RunID, err)
			}
			continue
		}
		tally, err := walkHandoffTree(root, record.RunID, info, seen)
		if err != nil {
			// The partial tally is kept rather than discarded: the bytes it
			// did reach are on the node whether or not the rest could be read,
			// and a pass that drops them would report a node emptier than it is.
			m.log("agent: measure run %s's retained results: %v (the figure below counts what could be read)",
				record.RunID, err)
		}
		status.LogicalBytes += tally.logical
		status.ChargedBytes += tally.charged
		status.Entries += tally.entries
	}
	status.Unaccounted = m.countUnaccounted(root, recorded)
	return status
}

// countUnaccounted counts what is under the handoff root that no record names.
//
// It counts and stops there. A directory with no record is not this agent's --
// it is measured by nobody and removed by nobody, however full the node is
// (docs/contracts/run-execution-context.md) -- and the one thing worse than not
// knowing it is there is knowing and not saying. Adoption (adoptResidue) is the
// one path that turns some of these into the agent's own, and it runs at
// startup on the evidence of an ownership marker, not on a byte count.
func (m *handoffManager) countUnaccounted(root *os.Root, recorded map[string]struct{}) int {
	directory, err := root.Open(".")
	if err != nil {
		m.log("agent: read the handoff root %q while accounting: %v", m.root, err)
		return 0
	}
	children, err := directory.ReadDir(-1)
	directory.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		m.log("agent: read the handoff root %q while accounting: %v", m.root, err)
		return 0
	}
	unaccounted := 0
	for _, child := range children {
		if _, known := recorded[child.Name()]; !known {
			unaccounted++
		}
	}
	return unaccounted
}

// reportNodeAccounting is the one line per pass a person reads, and the same
// figures on the agent's status projection for whatever reads it next.
func (m *handoffManager) reportNodeAccounting(status RetainedResultsStatus) {
	m.log("agent: retained results on this node: %d runs (%d still in flight), %d logical bytes, %d charged bytes across %d entries; %d quarantined records, still charged; %d entries under %q have no record and are neither measured nor removed",
		status.Runs, status.InFlight, status.LogicalBytes, status.ChargedBytes, status.Entries,
		status.QuarantinedRecords, status.Unaccounted, m.root)
	if m.observeAccounting != nil {
		m.observeAccounting(status)
	}
}

// adoptResidue gives the agent back the directories a crash left behind, and
// runs once, at startup, before anything is executing.
//
// Two shapes reach it. A run that was in flight when the node stopped has a
// record with an admission and no deadline -- preparation writes it precisely
// so a crash leaves a record instead of invisible residue -- and needs a
// deadline before the sweep will ever look at it. A directory from before that
// record existed, or one whose record was lost, has no record at all, and is
// adopted when it carries an ownership marker naming this node and that run.
//
// The marker is advisory, and that is enough here and nowhere else. Adoption
// only ever *adds* a directory this agent created to this agent's own
// accounting: the marker cannot name another node's run, the deadline it
// carries is clamped to the retention window, and a workload that forges one
// has arranged for its own directory to expire. Deletion authority still comes
// only from a record under the agent's state root.
//
// A directory with neither record nor marker is left exactly as it is, and is
// counted every pass (countUnaccounted) rather than silently invisible.
//
// This is what openRun exists for. It had no caller outside tests until here.
func (m *handoffManager) adoptResidue() error {
	m.collectMu.Lock()
	defer m.collectMu.Unlock()
	root, err := openPrivateHandoffDirectory(m.root)
	if err != nil {
		return err
	}
	defer root.Close()
	recorded := make(map[string]retentionRecord)
	for _, record := range m.loadRecords() {
		recorded[record.RunID] = record
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	children, err := directory.ReadDir(-1)
	directory.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read handoff root %q: %w", m.root, err)
	}
	now := m.now().UTC()
	for _, child := range children {
		name := child.Name()
		record, known := recorded[name]
		if known && !record.RetainUntil.IsZero() {
			continue
		}
		info, err := root.Lstat(name)
		if err != nil {
			continue
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		m.adoptRun(name, record, known, now)
	}
	return nil
}

// adoptRun gives one directory a deadline, under that directory's own path
// lease and on the same revalidation every other write to a record uses.
func (m *handoffManager) adoptRun(runID string, record retentionRecord, known bool, now time.Time) {
	if !validRunMailboxSegment(runID) {
		return
	}
	path := filepath.Join(m.root, runID)
	lease := m.tryCollectLease(path)
	if lease == nil {
		return
	}
	defer lease.release()
	marker, valid := m.adoptableMarker(runID, path)
	if !known && !valid {
		// Neither a record nor a marker this node wrote. Not this agent's, and
		// the accounting pass says so every hour.
		return
	}
	deadline := m.adoptedDeadline(marker, valid, record.AdmittedAt, now)
	updated := record
	if !known {
		updated = retentionRecord{RunID: runID, NodeID: m.nodeID, Directory: path}
	}
	if updated.AdmittedAt.IsZero() {
		updated.AdmittedAt = deadline.Add(-m.retention)
	}
	updated.RetainedAt = deadline.Add(-m.retention)
	updated.RetainUntil = deadline
	updated.Adopted = true
	switch {
	case known:
		m.log("agent: run %s was still in flight when this node last stopped; its results are now accounted and expire at %s",
			runID, deadline.Format(time.RFC3339))
		m.rewriteRecord(record, updated)
	default:
		m.log("agent: adopted %q: it carries this node's ownership marker for run %s but had no retention record, so it expires at %s",
			path, runID, deadline.Format(time.RFC3339))
		if err := m.writeRecord(updated); err != nil {
			m.log("agent: record the adoption of %q: %v", path, err)
		}
	}
}

// adoptableMarker reads one directory's ownership marker and reports whether it
// names this node and this run. Anything else -- unreadable, another node's,
// another run's, no deadline -- is not adoptable, and is not an error either:
// it simply stays a directory the agent does not claim.
func (m *handoffManager) adoptableMarker(runID, path string) (handoffMarker, bool) {
	run, err := m.openRun(runID)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			m.log("agent: inspect %q for an ownership marker: %v", path, err)
		}
		return handoffMarker{}, false
	}
	defer run.Close()
	marker, exists, err := readHandoffMarker(run)
	if err != nil {
		m.log("agent: read the ownership marker in %q: %v", path, err)
		return handoffMarker{}, false
	}
	if !exists || marker.RunID != runID || marker.NodeID != m.nodeID || marker.RetainUntil.IsZero() {
		return handoffMarker{}, false
	}
	return marker, true
}

// adoptedDeadline decides when an adopted directory's results expire.
//
// The marker's own deadline is preferred, because it is the one preparation
// wrote and the one a cold rerun would have honoured. It is clamped to the
// retention window from now: the marker is workload-writable, so the worst a
// forged one may do is shorten its own run's retention, never extend a node's.
func (m *handoffManager) adoptedDeadline(marker handoffMarker, valid bool, admittedAt, now time.Time) time.Time {
	deadline := time.Time{}
	if valid {
		deadline = marker.RetainUntil.UTC()
	}
	if deadline.IsZero() && !admittedAt.IsZero() {
		deadline = admittedAt.Add(m.retention)
	}
	if deadline.IsZero() {
		deadline = now.Add(m.retention)
	}
	if limit := now.Add(m.retention); deadline.After(limit) {
		deadline = limit
	}
	return deadline
}
