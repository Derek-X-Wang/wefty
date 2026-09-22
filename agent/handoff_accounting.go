package agent

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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
	// unaccounted counts the subtrees this walk gave up on because it could no
	// longer reach the directory it had been measuring. Their bytes are on the
	// node and are not in the figures above, which is exactly what the pass
	// has to say rather than quietly report a smaller node.
	unaccounted int
	// replaced counts the subset of those where the name was reachable and led
	// somewhere else: a different directory now stands where the walk had been
	// measuring. That is a different fact from a subtree that simply went
	// away, and it is the one worth naming -- a workload owns these
	// directories, and swapping one mid-pass is how a tree would otherwise be
	// measured as somewhere it is not.
	replaced int
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

// maxOpenWalkFrames bounds how many of a tree's directories one walk holds open
// at the same time.
//
// Without it, how many descriptors the agent spends is the workload's decision.
// Releasing an ancestor as its last subdirectory is opened handles a chain, and
// a comb defeats it: give every level one child that continues downward and one
// empty sibling visited second, and every ancestor stays open with a pending
// child for the whole descent. 2N directories then pin N descriptors, and an
// agent out of descriptors stops measuring, stops serving, and stops logging
// why.
//
// The cost of the bound is re-opening: an ancestor released and later needed
// again is re-opened component-wise from the root, verified against the
// identity it had, and a deep comb therefore pays O(depth) opens per release.
// That is the trade taken deliberately -- syscalls a node can refuse to make
// faster are better than descriptors it cannot get back -- and the re-open
// keeps the window full, so the next maxOpenWalkFrames levels of unwinding cost
// nothing.
const maxOpenWalkFrames = 64

// errHandoffWalkMoved marks a directory that is no longer the one this walk
// measured part of. It is not a failure of the pass: the workload owns these
// files and may replace them mid-walk. What it costs is the rest of that
// subtree, which is reported as unaccounted rather than guessed at.
var errHandoffWalkMoved = errors.New("a directory changed identity while the pass was walking it")

// handoffWalkFrame is one directory the walk has read and still has
// subdirectories to descend into.
//
// It holds no path. A frame that still has its descriptor is reached through
// that; a frame whose descriptor was released to stay inside the bound is
// reached again by opening its ancestors' names one component at a time, each
// no-follow, and is accepted only if it is the same inode it was.
type handoffWalkFrame struct {
	// name is this directory's component inside its parent.
	name string
	// run is the open handle, or nil once it has been released.
	run *os.Root
	// identity is what this directory was when the walk first opened it, and
	// is what a re-open has to match.
	identity os.FileInfo
	// directories are the subdirectory names not yet descended into.
	directories []string
}

// handoffWalk is one iterative walk over one retained run's tree.
//
// Open frames are always a contiguous suffix of the stack: releases take the
// oldest frame, which is the one the walk will need again last, and re-opens
// refill from the bottom. firstOpen is where that suffix starts, so
// len(stack)-firstOpen is the number of descriptors this walk is holding.
type handoffWalk struct {
	base      *os.Root
	seen      map[handoffInode]struct{}
	tally     handoffTally
	stack     []*handoffWalkFrame
	firstOpen int
	// peak is the most frames ever open at once, which is what a test asserts
	// instead of watching the process's descriptor table and hoping to sample
	// at the right moment.
	peak int
}

// handoffWalkFramesObserved is a test seam. It reports the peak number of
// directory handles one walk held, so the bound above is asserted on the
// walker's own count rather than on a sampled /dev/fd -- a sample can miss the
// peak entirely and pass a walk that held thousands. Nothing outside a test
// ever sets it.
var handoffWalkFramesObserved func(peak int)

// handoffWalkDescended is a test seam. It runs each time the walk pushes a
// level, so a test can replace a directory the walk has already released and
// prove the re-open refuses it -- an interleaving that has to be staged rather
// than raced for. Nothing outside a test ever sets it.
var handoffWalkDescended func(depth int)

// walkHandoffTree sums one directory entry and, when it is a directory,
// everything beneath it. It never follows a symlink and reaches every level
// through a directory handle rather than by pathname.
//
// It is iterative because it used to recurse and the depth it recursed to was
// the workload's choice. A depth cap instead of iteration would have been worse
// than the recursion: it stops counting and says nothing, which is exactly the
// silent under-count this slice exists to remove.
//
// seen, when non-nil, is the pass's cross-run file identity (handoffInodeOf).
// Passing nil counts every name, which is what the per-run bound wants: it
// trims names, and part 1 states its unit as logical bytes per link.
func walkHandoffTree(parent *os.Root, name string, info os.FileInfo, seen map[handoffInode]struct{}) (handoffTally, error) {
	walk := &handoffWalk{base: parent, seen: seen}
	defer walk.closeAll()
	walk.tally.add(handoffLogicalBytes(info, seen))
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		walk.report()
		return walk.tally, nil
	}
	err := walk.descend(name)
	walk.report()
	return walk.tally, err
}

func (walk *handoffWalk) report() {
	if handoffWalkFramesObserved != nil {
		handoffWalkFramesObserved(walk.peak)
	}
}

func (walk *handoffWalk) descend(name string) error {
	top, err := openHandoffDirectory(walk.base, name)
	if err != nil {
		if handoffWalkSkips(err) {
			return nil
		}
		return err
	}
	if err := walk.push(name, top); err != nil {
		return err
	}
	for len(walk.stack) > 0 {
		frame := walk.stack[len(walk.stack)-1]
		if len(frame.directories) == 0 {
			walk.pop()
			continue
		}
		child := frame.directories[0]
		frame.directories = frame.directories[1:]
		run, err := walk.topRoot()
		if err != nil {
			// The walk can no longer reach the directory this frame stands
			// for. What was counted stays counted; what is left of it is named
			// as unaccounted rather than measured through a directory the walk
			// cannot prove is the same one.
			walk.tally.unaccounted++
			if errors.Is(err, errHandoffWalkMoved) {
				walk.tally.replaced++
			}
			walk.pop()
			continue
		}
		next, err := openHandoffDirectory(run, child)
		if len(frame.directories) == 0 {
			// Nothing here needs this handle again. Dropping it now, rather
			// than when the subtree returns, is what keeps a chain of
			// directories at one descriptor instead of one per level.
			walk.pop()
		}
		if err != nil {
			if handoffWalkSkips(err) {
				continue
			}
			return err
		}
		if err := walk.push(child, next); err != nil {
			return err
		}
	}
	return nil
}

// push reads one directory, adds its children to the tally, and keeps it on the
// stack only if it has subdirectories left to visit.
func (walk *handoffWalk) push(name string, run *os.Root) error {
	identity, err := run.Stat(".")
	if err != nil {
		run.Close()
		if handoffWalkSkips(err) {
			return nil
		}
		return err
	}
	directories, err := tallyHandoffChildren(run, &walk.tally, walk.seen)
	if err != nil {
		run.Close()
		return err
	}
	if len(directories) == 0 {
		return run.Close()
	}
	walk.stack = append(walk.stack, &handoffWalkFrame{
		name: name, run: run, identity: identity, directories: directories,
	})
	walk.trim(len(walk.stack) - 1)
	if handoffWalkDescended != nil {
		handoffWalkDescended(len(walk.stack))
	}
	return nil
}

func (walk *handoffWalk) pop() {
	last := len(walk.stack) - 1
	if walk.stack[last].run != nil {
		walk.stack[last].run.Close()
		walk.stack[last].run = nil
	}
	walk.stack = walk.stack[:last]
	if walk.firstOpen > last {
		walk.firstOpen = last
	}
}

// topRoot hands back the deepest frame's handle, re-opening the frames that
// were released to stay inside the bound.
//
// Re-opening is component by component from the walk's own base, each component
// no-follow and directory-only, and every re-opened frame must be the same
// inode the walk first saw. A frame that is not stops the descent: the walk
// refuses to keep measuring through a directory it cannot prove is the one it
// was measuring.
func (walk *handoffWalk) topRoot() (*os.Root, error) {
	index := len(walk.stack) - 1
	if run := walk.stack[index].run; run != nil {
		return run, nil
	}
	current := walk.base
	for position := 0; position <= index; position++ {
		frame := walk.stack[position]
		if frame.run == nil {
			child, err := openHandoffDirectory(current, frame.name)
			if err != nil {
				return nil, err
			}
			opened, err := child.Stat(".")
			if err != nil || !os.SameFile(frame.identity, opened) {
				child.Close()
				return nil, errHandoffWalkMoved
			}
			frame.run = child
			if position < walk.firstOpen {
				walk.firstOpen = position
			}
		}
		current = frame.run
		// Keep the window behind this position inside the bound. The frame in
		// hand is never one of the released ones: the bound is far larger than
		// the one handle this loop is holding.
		walk.trim(position)
	}
	return walk.stack[index].run, nil
}

// trim releases the oldest open frames until at most maxOpenWalkFrames handles
// are alive behind position, and records the peak.
func (walk *handoffWalk) trim(position int) {
	for position-walk.firstOpen >= maxOpenWalkFrames {
		walk.stack[walk.firstOpen].run.Close()
		walk.stack[walk.firstOpen].run = nil
		walk.firstOpen++
	}
	if open := position + 1 - walk.firstOpen; open > walk.peak {
		walk.peak = open
	}
}

func (walk *handoffWalk) closeAll() {
	for position := walk.firstOpen; position < len(walk.stack); position++ {
		if walk.stack[position].run != nil {
			walk.stack[position].run.Close()
			walk.stack[position].run = nil
		}
	}
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
		if tally.unaccounted != 0 {
			m.log("agent: run %s: %d subtree(s) could not be reached again and are counted as unaccounted rather than measured; %d of them had a different directory standing where this pass had been measuring",
				record.RunID, tally.unaccounted, tally.replaced)
		}
		status.LogicalBytes += tally.logical
		status.ChargedBytes += tally.charged
		status.Entries += tally.entries
		status.Unaccounted += tally.unaccounted
	}
	status.Unaccounted += m.countUnaccounted(root, recorded)
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

// adoptResidue reconciles what a crash left behind, and runs once, at startup,
// before anything is executing.
//
// Two shapes reach it, and they are reached from two different directions on
// purpose.
//
// An admission that never finished is reached through the **records**: every
// record with an admission and no deadline is reconciled, whether or not a
// directory still stands at its name. Walking directory entries instead was a
// hole -- a run whose directory was removed, or whose name a workload replaced
// with a symlink, produced no entry to reconcile, so its record stayed "in
// flight" for as long as the node ran, the hourly sweep skipped it (no
// deadline), and it never reached the structural quarantine that exists for
// exactly that shape. The three states are now all answered: the directory is
// gone and the record goes with it; the name is not a directory and the record
// gets a bounded deadline and enters S1's reversible quarantine, which lifts by
// itself the moment a directory is back; the directory is there and it gets its
// deadline.
//
// A directory no record names is reached through the **root**, and is adopted
// when it carries an ownership marker naming this node and that run.
//
// The marker is advisory, and that is enough for what adoption does. Adoption
// only ever *creates*: it refuses at a key an existing record already holds,
// and it cannot reach another node's runs, because the marker must name this
// node. It is not proof the agent created the directory -- a workload can write
// a marker with matching fields, and the contract says so -- and it is not
// deletion authority: the worst an adopted directory can be is an expiry
// schedule on a directory under this node's own handoff root.
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
	now := m.now().UTC()
	recorded := make(map[string]struct{})
	for _, record := range m.loadRecords() {
		recorded[record.RunID] = struct{}{}
		if !record.RetainUntil.IsZero() {
			continue
		}
		m.reconcileAdmission(root, record, now)
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
	for _, child := range children {
		name := child.Name()
		if _, known := recorded[name]; known {
			continue
		}
		info, err := root.Lstat(name)
		if err != nil {
			continue
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		m.adoptDirectory(name, now)
	}
	return nil
}

// reconcileAdmission answers one prior-boot admission, under that run's path
// lease and on the same revalidation every other write to a record uses.
//
// It re-reads the record under the lease first, because an attempt for the same
// run can have finished between the load and the lease, and a finished record
// is not this function's to touch.
func (m *handoffManager) reconcileAdmission(root *os.Root, record retentionRecord, now time.Time) {
	lease := m.tryCollectLease(record.Directory)
	if lease == nil {
		// An attempt holds this path, so the run is not a prior boot's after
		// all and its own finish writes the window.
		return
	}
	defer lease.release()
	current, ok := m.currentRecord(record)
	if !ok {
		return
	}
	record = current
	if !record.RetainUntil.IsZero() {
		return
	}
	info, err := root.Lstat(record.RunID)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		m.log("agent: run %s was admitted and never finished, and %q is gone; its record is removed rather than left in flight for as long as this node runs",
			record.RunID, record.Directory)
		if err := m.removeRecord(record.RunID); err != nil {
			m.log("agent: remove run %s's admission record: %v", record.RunID, err)
		}
	case err != nil:
		m.log("agent: run %s: inspect %q while reconciling its admission: %v; it stays in flight until the next startup",
			record.RunID, record.Directory, err)
	case !info.IsDir() || info.Mode()&os.ModeSymlink != 0:
		m.quarantineAdmission(record, info, now)
	default:
		deadline := m.adoptedDeadline(m.adoptableMarker(record.RunID, record.Directory))(record.AdmittedAt, now)
		m.log("agent: run %s was still in flight when this node last stopped; its results are now accounted and expire at %s",
			record.RunID, deadline.Format(time.RFC3339))
		m.rewriteRecord(record, m.adoptedWindow(record, deadline))
	}
}

// quarantineAdmission gives an admission whose name is not a directory a
// bounded deadline and hands it to S1's structural quarantine.
//
// Without a deadline the record is invisible to the sweep forever: collection
// skips a record that has none, so a workload that replaced its own handoff
// name with a symlink before this node restarted used to buy that record
// permanent residence. With one, the record is an ordinary quarantined record:
// every sweep spends one Lstat re-checking the name, says nothing while the
// shape is unchanged, and resumes normal expiry the moment a directory is back
// there. Nothing is ever followed and nothing is deleted.
//
// The deadline is the one the run was admitted for, never a fresh window: a
// crash must not extend retention, and a record whose clock sits ahead of this
// node's is capped at one window from now.
func (m *handoffManager) quarantineAdmission(record retentionRecord, info os.FileInfo, now time.Time) {
	deadline := record.AdmittedAt.Add(m.retention)
	if limit := now.Add(m.retention); deadline.IsZero() || deadline.After(limit) {
		deadline = limit
	}
	updated := m.adoptedWindow(record, deadline)
	updated.ExpiryFailures = record.ExpiryFailures + 1
	updated.StructuralRefusals = maxStructuralRefusals
	updated.Quarantine = handoffExpiryNameNotADirectory
	updated.QuarantineDetail = boundedQuarantineDetail(fmt.Errorf("%w: %q is %s at startup",
		errHandoffNameNotADirectory, record.Directory, info.Mode()))
	updated.QuarantinedAt = now
	m.log("agent: run %s was admitted and never finished, and %q is %s rather than a directory; nothing is followed or removed, its results expire at %s, and the sweep re-checks the name until a directory is back there",
		record.RunID, record.Directory, info.Mode(), deadline.Format(time.RFC3339))
	m.rewriteRecord(record, updated)
}

// adoptedWindow puts a derived window on one record without inventing anything
// else about it. Adopted says the agent derived the window rather than an
// attempt writing it.
func (m *handoffManager) adoptedWindow(record retentionRecord, deadline time.Time) retentionRecord {
	updated := record
	if updated.AdmittedAt.IsZero() {
		updated.AdmittedAt = deadline.Add(-m.retention)
	}
	updated.RetainedAt = deadline.Add(-m.retention)
	updated.RetainUntil = deadline
	updated.Adopted = true
	return updated
}

// adoptDirectory adopts one directory that no record names.
func (m *handoffManager) adoptDirectory(runID string, now time.Time) {
	if !validRunMailboxSegment(runID) {
		return
	}
	path := filepath.Join(m.root, runID)
	lease := m.tryCollectLease(path)
	if lease == nil {
		return
	}
	defer lease.release()
	if !m.recordKeyIsThisRuns(runID, path) {
		return
	}
	marker, valid := m.adoptableMarker(runID, path)
	if !valid {
		// No marker this node wrote. Not this agent's, and the accounting pass
		// says so every hour.
		return
	}
	deadline := m.adoptedDeadline(marker, valid)(time.Time{}, now)
	record := m.adoptedWindow(retentionRecord{RunID: runID, NodeID: m.nodeID, Directory: path}, deadline)
	m.log("agent: adopted %q: it carries this node's ownership marker for run %s but had no retention record, so it expires at %s",
		path, runID, deadline.Format(time.RFC3339))
	if err := m.writeRecord(record); err != nil {
		m.log("agent: record the adoption of %q: %v", path, err)
	}
}

// recordKeyIsThisRuns reports that writing this run's record would not replace
// somebody else's.
//
// Adoption is the one path that writes a record for a directory it did not
// prepare, on evidence a workload can write. It therefore only ever *creates*:
// if a record already stands at the name a write would use, and that record is
// not this run's on this node, the adoption is refused and the directory stays
// exactly as it is -- unaccounted, which the pass reports, rather than holding
// another run's expiry. A record that cannot be read is refused too: adoption
// never replaces a record whose identity it could not establish.
//
// Its own untrustworthy record is not a refusal. That file already belongs to
// this run, so re-deriving this run's window replaces nothing else.
func (m *handoffManager) recordKeyIsThisRuns(runID, path string) bool {
	if strings.TrimSpace(m.stateRoot) == "" {
		return true
	}
	file := m.recordPath(runID)
	current, err := m.readRecord(file)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return true
	case err != nil:
		m.log("agent: leave %q unadopted and unaccounted: a record already stands at %q and could not be read (%v); adoption never replaces a record it cannot identify",
			path, file, err)
		return false
	case current.RunID == runID && current.NodeID == m.nodeID && current.Directory == path:
		return true
	default:
		m.log("agent: refuse to adopt %q: %q already holds run %q on node %q; the directory is left untouched and stays unaccounted",
			path, file, current.RunID, current.NodeID)
		return false
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
// wrote and the one a cold rerun would have honoured. It is clamped to one
// retention window from now, which is the exact limit: a workload that writes
// a marker cannot extend its results past a window after the adoption, and it
// can shorten them as much as it likes. It is not clamped to the run's original
// deadline, because an adoption has no trustworthy record of what that was --
// that is the state adoption exists to replace.
func (m *handoffManager) adoptedDeadline(marker handoffMarker, valid bool) func(admittedAt, now time.Time) time.Time {
	return func(admittedAt, now time.Time) time.Time {
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
}
