package agent

import (
	"context"
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
	// vanished counts the subtrees this walk gave up on because the directory
	// it had been measuring stopped being reachable at all. Their bytes may
	// still be on the node and are not in the figures above, which is what the
	// pass has to say rather than quietly report a smaller node.
	vanished int
	// replaced counts the subtrees where the name was reachable and led
	// somewhere else: a different directory now stands where the walk had been
	// measuring. That is a different fact from a subtree that simply went
	// away, and it is the one worth naming -- a workload owns these
	// directories, and swapping one mid-pass is how a tree would otherwise be
	// measured as somewhere it is not.
	//
	// Each is counted once per loss. Every frame beneath a replaced ancestor
	// fails on the same component as the walk unwinds, and counting those
	// would report a hundred losses where a workload renamed one directory.
	replaced int
	// truncated marks this run's tree as measured incompletely, because the
	// pass spent its opens budget or the agent is shutting down.
	truncated int
	// closeFailures counts directories the walk finished with and could not
	// close. It costs that entry's handle, not the pass.
	closeFailures int
	// reopenOpens counts the directories opened re-establishing an ancestry
	// the walk had released, as opposed to visiting the tree. It is what the
	// prefix cache exists to keep down, and a test asserts on it because
	// "how many handles were held" says nothing about how much work was
	// repeated to hold them.
	reopenOpens int
	// reopenFailures counts every re-open that did not complete, which is more
	// than the losses above: one replaced ancestor refuses once for every
	// frame beneath it as the walk unwinds. Nothing outside this walk reports
	// it -- the node is told how many subtrees it lost, not how many times the
	// walk noticed -- and a test uses it to know the retry path really ran.
	reopenFailures int
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

// maxWalkOpens is how many directories one accounting pass may open before it
// stops and says so.
//
// Accounting is a background pass over a workload's own directories, and how
// much work they are worth is that workload's choice. Without a budget an
// adversarial tree is an unbounded amount of syscalls inside the collector --
// which the agent joins before releasing the node lock, so a shutdown waits on
// it. A pass that stops early reports a truncated run by name; a pass that
// never finishes reports nothing at all, which is the worse of the two.
//
// Low millions: far past any real retained-results tree, and seconds rather
// than hours at a few microseconds an open.
const maxWalkOpens = 4_000_000

// errHandoffWalkMoved marks a directory that is no longer the one this walk
// measured part of. It is not a failure of the pass: the workload owns these
// files and may replace them mid-walk. What it costs is the rest of that
// subtree, which is reported as replaced rather than guessed at.
var errHandoffWalkMoved = errors.New("a directory changed identity while the pass was walking it")

// errHandoffWalkTruncated marks a walk that stopped before it finished, either
// because the pass ran out of its opens budget or because the agent is shutting
// down. What it has counted is real and incomplete, and saying which run was
// cut short is the whole point of having the error.
var errHandoffWalkTruncated = errors.New("the accounting pass stopped before it finished this run")

// errHandoffWalkOwnership marks the one internal inconsistency this walk can
// notice about itself: a frame being handed a second handle. It stops the walk
// rather than the process. A background pass that panics takes the collector
// goroutine, and with it the agent, over an accounting figure.
var errHandoffWalkOwnership = errors.New("a walk frame was handed a second handle")

// handoffWalkFrame is one directory the walk has read and still has
// subdirectories to descend into.
//
// A frame that still has its descriptor is reached through that; a frame whose
// descriptor was released to stay inside the bound is reached again by opening
// its ancestry one component at a time, each no-follow, and is accepted only if
// it is the same inode it was.
type handoffWalkFrame struct {
	// depth is where this directory's own component sits in the walk's path.
	// The ancestry of this frame is therefore path[:depth+1], whatever the
	// stack currently holds.
	depth int
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
	base  *os.Root
	seen  map[handoffInode]struct{}
	tally handoffTally
	stack []*handoffWalkFrame
	// path is the component ancestry from the walk's base down to the deepest
	// frame, and it is kept independently of the stack on purpose.
	//
	// The stack is not the ancestry and cannot stand in for it. A directory
	// whose last subdirectory is being opened is dropped from the stack right
	// then -- that is what keeps a chain at one descriptor -- so for a tree
	// shaped chain/comb/... the stack holds only the comb while the chain
	// above it is a component nothing remembers. Rebuilding a released frame
	// from the stack then opened base/comb, which does not exist, and an
	// unchanged tree reported its postponed siblings as unaccounted.
	path      []string
	firstOpen int
	// opens is what is left of the pass's budget, shared by every run the pass
	// measures. It is a pointer because the budget bounds the pass, not one
	// tree: a node with a hundred adversarial runs is the same problem as one.
	opens *int64
	// ctx ends the walk when the agent is shutting down.
	ctx context.Context
	// lostDepth is the depth a re-open last failed at, so one replaced
	// ancestor is counted once rather than once for every frame beneath it
	// that then fails on the same component.
	lostDepth int
	// openFrames is the number of handles owned by frames. Every frame handle
	// has exactly one owner, and every ownership change goes through own and
	// release so reopened handles participate in the bound as well.
	openFrames int
	// liveHandles counts every directory handle opened by this walk until its
	// one close, including transient handles and candidates not yet owned by a
	// frame. It makes an overwritten owner visible at cleanup.
	liveHandles int
	// peakOpenFrames is the most frame handles ever owned at once, which is
	// what a test asserts instead of watching the process's descriptor table
	// and hoping to sample at the right moment.
	peakOpenFrames int
}

// handoffWalkFramesObserved is a test seam. It reports the peak number of
// frame-owned handles and every directory handle still live after cleanup, so
// the bound and exact release are asserted on the walker's own accounting
// rather than on a sampled /dev/fd. Nothing outside a test ever sets it.
var handoffWalkFramesObserved func(open, peak int)

// handoffWalkDescended is a test seam. It runs each time the walk pushes a
// level, so a test can replace a directory the walk has already released and
// prove the re-open refuses it -- an interleaving that has to be staged rather
// than raced for. Nothing outside a test ever sets it.
var handoffWalkDescended func(depth int)

// handoffWalkReopened is a test seam. It runs after each re-open that reached
// its frame, so a test can change the ancestry *between* two re-opens -- the
// interleaving a cached prefix used to walk straight past, and the only one
// that shows whether every component is really resolved by name every time.
// Nothing outside a test ever sets it.
var handoffWalkReopened func(depth int)

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
func walkHandoffTree(ctx context.Context, budget *int64, parent *os.Root, name string, info os.FileInfo, seen map[handoffInode]struct{}) (handoffTally, error) {
	walk := &handoffWalk{base: parent, seen: seen, opens: budget, ctx: ctx, lostDepth: -1}
	defer walk.report()
	defer walk.closeAll()
	walk.tally.add(handoffLogicalBytes(info, seen))
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return walk.tally, nil
	}
	err := walk.descend(name)
	if errors.Is(err, errHandoffWalkTruncated) || errors.Is(err, errHandoffWalkOwnership) {
		walk.tally.truncated++
	}
	return walk.tally, err
}

func (walk *handoffWalk) report() {
	if handoffWalkFramesObserved != nil {
		handoffWalkFramesObserved(walk.liveHandles, walk.peakOpenFrames)
	}
}

func (walk *handoffWalk) descend(name string) error {
	top, err := walk.openDirectory(walk.base, name)
	if err != nil {
		if handoffWalkSkips(err) {
			return nil
		}
		return err
	}
	if err := walk.push(0, name, top); err != nil {
		return err
	}
	for len(walk.stack) > 0 {
		frame := walk.stack[len(walk.stack)-1]
		if len(frame.directories) == 0 {
			walk.complete()
			continue
		}
		child := frame.directories[0]
		frame.directories = frame.directories[1:]
		run, depthLost, err := walk.topRoot()
		if err != nil {
			if errors.Is(err, errHandoffWalkTruncated) || errors.Is(err, errHandoffWalkOwnership) {
				return err
			}
			// The walk can no longer reach the directory this frame stands
			// for. What was counted stays counted; what is left of it is named
			// rather than measured through a directory the walk cannot prove
			// is the one it was measuring.
			//
			// One replaced ancestor is one loss. Every frame beneath it fails
			// on the same component as the walk unwinds, and counting each of
			// those would report a hundred lost subtrees where a workload
			// renamed one directory.
			walk.tally.reopenFailures++
			if depthLost != walk.lostDepth {
				walk.lostDepth = depthLost
				if errors.Is(err, errHandoffWalkMoved) {
					walk.tally.replaced++
				} else {
					walk.tally.vanished++
				}
			}
			walk.complete()
			continue
		}
		walk.lostDepth = -1
		depth := frame.depth + 1
		next, err := walk.openDirectory(run, child)
		if len(frame.directories) == 0 {
			// Nothing here needs this handle again. Dropping it now, rather
			// than when the subtree returns, is what keeps a chain of
			// directories at one descriptor instead of one per level. The
			// frame goes; its component stays in path, because the child about
			// to be pushed still lives under it.
			walk.discard()
		}
		if err != nil {
			if handoffWalkSkips(err) {
				continue
			}
			return err
		}
		if err := walk.push(depth, child, next); err != nil {
			return err
		}
	}
	return nil
}

// push reads one directory, adds its children to the tally, and keeps it on the
// stack only if it has subdirectories left to visit. depth is where this
// directory's component belongs in the walk's ancestry, which the caller knows
// and the stack no longer does.
func (walk *handoffWalk) push(depth int, name string, run *os.Root) error {
	identity, err := run.Stat(".")
	if err != nil {
		walk.closeHandle(run)
		if handoffWalkSkips(err) {
			return nil
		}
		return err
	}
	directories, err := tallyHandoffChildren(run, &walk.tally, walk.seen)
	if err != nil {
		walk.closeHandle(run)
		return err
	}
	if len(directories) == 0 {
		if err := walk.closeHandle(run); err != nil {
			// Closing a directory the walk is finished with cannot change what
			// it counted, and it is not a reason to abandon every other run's
			// tree. The entry it belongs to is already tallied; the failure is
			// one entry's, not the pass's.
			walk.tally.closeFailures++
		}
		return nil
	}
	// Truncate rather than append: a child opened after an earlier sibling's
	// subtree finished belongs at its parent's depth plus one, not after
	// whatever that subtree left behind.
	walk.path = append(walk.path[:depth], name)
	frame := &handoffWalkFrame{depth: depth, identity: identity, directories: directories}
	walk.stack = append(walk.stack, frame)
	walk.trim(len(walk.stack) - 1)
	if err := walk.own(frame, run); err != nil {
		return err
	}
	if handoffWalkDescended != nil {
		handoffWalkDescended(len(walk.path))
	}
	return nil
}

// discard drops the deepest frame and its descriptor, and leaves the ancestry
// alone: the walk is about to descend into that directory's last child, which
// is reached through its name.
func (walk *handoffWalk) discard() {
	last := len(walk.stack) - 1
	walk.release(walk.stack[last])
	walk.stack = walk.stack[:last]
	if walk.firstOpen > last {
		walk.firstOpen = last
	}
}

// complete drops the deepest frame because its subtree is finished, and takes
// its component out of the ancestry with it.
func (walk *handoffWalk) complete() {
	depth := walk.stack[len(walk.stack)-1].depth
	walk.discard()
	if depth < len(walk.path) {
		walk.path = walk.path[:depth]
	}
	if walk.lostDepth >= depth {
		// The walk has unwound past whatever it lost, so the next failure is a
		// new one rather than the same component refusing again.
		walk.lostDepth = -1
	}
}

// topRoot hands back the deepest frame's handle, re-opening what was released
// to stay inside the bound.
//
// Re-opening walks the recorded ancestry from the walk's own base, component by
// component, each no-follow and directory-only, **every time**. Most of those
// components are not frames at all -- a chain above a branch leaves components
// and no frames -- so they are stepped through on transient handles. Every
// component that is a frame must be the same inode the walk first saw; one that
// is not stops the descent, because the walk refuses to keep measuring through
// a directory it cannot prove is the one it was measuring.
//
// Caching those steps is what a previous version of this did, and the saving
// was not worth what it cost. Measured on a 260-deep chain over a 160-tooth
// comb it removed 32 of 420 re-opening opens -- the work that actually matters
// is retaining the *frames* a re-open re-establishes, which the window below
// does -- and it bought that 8% by never resolving the cached components again.
// A workload that renamed the prefix away and left a different directory at its
// name then kept the walk measuring the detached original: every identity check
// below the cache passed, because they were checks on the original's own
// descendants, and nothing was reported. Re-resolving by name every time is the
// only thing that makes those checks mean anything.
//
// It is only ever called on the deepest frame, and open frames are a contiguous
// suffix of the stack, so reaching here means every frame is released and the
// walk starts from the base.
func (walk *handoffWalk) topRoot() (*os.Root, int, error) {
	index := len(walk.stack) - 1
	frame := walk.stack[index]
	if frame.run != nil {
		return frame.run, -1, nil
	}
	current := walk.base
	var transient *os.Root
	firstOpen := walk.firstOpen
	var openedFrames []*handoffWalkFrame
	complete := false
	defer func() {
		if transient != nil {
			walk.closeHandle(transient)
		}
		if !complete {
			// A failed re-open owns none of the frame handles it acquired.
			// Restoring both ownership and the window marker makes every retry
			// start with the same handle count.
			for _, frame := range openedFrames {
				walk.release(frame)
			}
			walk.firstOpen = firstOpen
		}
	}()
	cursor := 0
	for depth := 0; depth <= frame.depth; depth++ {
		child, err := walk.openDirectory(current, walk.path[depth])
		walk.tally.reopenOpens++
		if transient != nil {
			// The step that led here is no longer needed now that its child is
			// open, so it never counts against the descriptor bound.
			walk.closeHandle(transient)
			transient = nil
		}
		if err != nil {
			return nil, depth, err
		}
		for cursor <= index && walk.stack[cursor].depth < depth {
			cursor++
		}
		if cursor > index || walk.stack[cursor].depth != depth {
			// A step on the way rather than a frame, held only until its own
			// child is open.
			transient, current = child, child
			continue
		}
		target := walk.stack[cursor]
		actual, statErr := child.Stat(".")
		if statErr != nil || !os.SameFile(target.identity, actual) {
			walk.closeHandle(child)
			return nil, depth, errHandoffWalkMoved
		}
		if target.run != nil {
			// The frame already owns the original directory. The name still
			// reaches that identity, so keep the existing owner and discard the
			// duplicate handle used to prove it.
			walk.closeHandle(child)
			current = target.run
			continue
		}
		if cursor < walk.firstOpen {
			walk.firstOpen = cursor
		}
		// Keep the window behind this frame inside the bound. The frame in
		// hand is never one of the released ones: the bound is far larger than
		// the one handle this loop is holding.
		walk.trim(cursor)
		if err := walk.own(target, child); err != nil {
			walk.closeHandle(child)
			return nil, depth, err
		}
		openedFrames = append(openedFrames, target)
		current = child
	}
	complete = true
	if handoffWalkReopened != nil {
		handoffWalkReopened(frame.depth)
	}
	return frame.run, -1, nil
}

// openDirectory records every handle this walk opens. A handle leaves this
// count only through closeHandle, whether it becomes frame-owned or remains a
// transient reconstruction step.
func (walk *handoffWalk) openDirectory(parent *os.Root, name string) (*os.Root, error) {
	if walk.ctx != nil && walk.ctx.Err() != nil {
		return nil, fmt.Errorf("%w: the agent is shutting down", errHandoffWalkTruncated)
	}
	if walk.opens != nil {
		if *walk.opens <= 0 {
			return nil, fmt.Errorf("%w: this pass has spent its budget of %d directory opens",
				errHandoffWalkTruncated, maxWalkOpens)
		}
		*walk.opens--
	}
	run, err := openHandoffDirectory(parent, name)
	if err == nil {
		walk.liveHandles++
	}
	return run, err
}

func (walk *handoffWalk) closeHandle(run *os.Root) error {
	err := run.Close()
	walk.liveHandles--
	return err
}

// own gives one open handle to a frame. A frame is its handle's sole owner.
//
// A frame that already owns one is this walk noticing its own inconsistency,
// and it refuses rather than panics: this runs on the collector goroutine, and
// taking the agent down over an accounting figure is a worse outcome than a
// run reported as truncated. The caller closes the handle it could not hand
// over.
func (walk *handoffWalk) own(frame *handoffWalkFrame, run *os.Root) error {
	if frame.run != nil {
		return errHandoffWalkOwnership
	}
	frame.run = run
	walk.openFrames++
	if walk.openFrames > walk.peakOpenFrames {
		walk.peakOpenFrames = walk.openFrames
	}
	return nil
}

// release closes the handle owned by frame exactly once.
func (walk *handoffWalk) release(frame *handoffWalkFrame) {
	if frame.run == nil {
		return
	}
	walk.closeHandle(frame.run)
	frame.run = nil
	walk.openFrames--
}

// trim releases the oldest open frames until at most maxOpenWalkFrames handles
// are alive behind position, and records the peak.
func (walk *handoffWalk) trim(position int) {
	for position-walk.firstOpen >= maxOpenWalkFrames {
		walk.release(walk.stack[walk.firstOpen])
		walk.firstOpen++
	}
}

func (walk *handoffWalk) closeAll() {
	for _, frame := range walk.stack {
		walk.release(frame)
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
func (m *handoffManager) measureNode(ctx context.Context, root *os.Root, now time.Time) RetainedResultsStatus {
	status := RetainedResultsStatus{MeasuredAt: now}
	seen := make(map[handoffInode]struct{})
	recorded := make(map[string]struct{})
	budget := int64(maxWalkOpens)
	for _, record := range m.loadRecords() {
		if _, counted := recorded[record.RunID]; counted {
			// loadRecords already answers one record per run. This is the
			// second guard on the one arithmetic that must not double: the
			// pass holds no lock, so what it reads is whatever the record
			// directory looked like as it read it.
			continue
		}
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
		tally, err := walkHandoffTree(ctx, &budget, root, record.RunID, info, seen)
		if err != nil && !errors.Is(err, errHandoffWalkTruncated) {
			// The partial tally is kept rather than discarded: the bytes it
			// did reach are on the node whether or not the rest could be read,
			// and a pass that drops them would report a node emptier than it is.
			m.log("agent: measure run %s's retained results: %v (the figure below counts what could be read)",
				record.RunID, err)
		}
		if tally.truncated != 0 {
			m.log("agent: run %s: its retained results were measured incompletely and the figures below are short by whatever is under it: %v",
				record.RunID, err)
		}
		if tally.replaced != 0 || tally.vanished != 0 {
			m.log("agent: run %s: %d subtree(s) had a different directory standing where this pass had been measuring and %d went away entirely; neither is counted in its bytes",
				record.RunID, tally.replaced, tally.vanished)
		}
		if tally.closeFailures != 0 {
			m.log("agent: run %s: %d directory handle(s) could not be closed after measuring; the figures are unaffected",
				record.RunID, tally.closeFailures)
		}
		status.LogicalBytes += tally.logical
		status.ChargedBytes += tally.charged
		status.Entries += tally.entries
		status.Replaced += tally.replaced + tally.vanished
		status.Truncated += tally.truncated
		// One row per run, in memory and nowhere else. The node budget the
		// next slice enforces has to choose *which* run to give up, and
		// choosing needs each run's own figure -- but a figure that moves with
		// the files does not belong on the record, which is authority to
		// delete and has to stay what an attempt wrote.
		status.PerRun = append(status.PerRun, RetainedRunFigures{
			RunID: record.RunID, Entries: tally.entries,
			LogicalBytes: tally.logical, ChargedBytes: tally.charged,
			Published: record.Published, Truncated: tally.truncated != 0,
		})
		if ctx != nil && ctx.Err() != nil {
			break
		}
	}
	status.Unrecorded = m.countUnrecorded(root, recorded)
	return status
}

// countUnrecorded counts what is under the handoff root that no record names.
//
// It counts and stops there. A directory with no record is not this agent's --
// it is measured by nobody and removed by nobody, however full the node is
// (docs/contracts/run-execution-context.md) -- and the one thing worse than not
// knowing it is there is knowing and not saying. Adoption (adoptResidue) is the
// one path that turns some of these into the agent's own, and it runs at
// startup on the evidence of an ownership marker, not on a byte count. A
// directory an agent created and died before marking is one of these, and stays
// one: nothing distinguishes it from a directory that was never the agent's.
func (m *handoffManager) countUnrecorded(root *os.Root, recorded map[string]struct{}) int {
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
	unrecorded := 0
	for _, child := range children {
		if _, known := recorded[child.Name()]; !known {
			unrecorded++
		}
	}
	return unrecorded
}

// accountNode is the accounting pass, and it is deliberately not on the path an
// attempt's finalization takes.
//
// It holds no collector lock. Measuring takes no authority over anything: it
// reads records and walks directories, and a directory expiry removes while it
// reads is an ordinary missing entry the walk skips. Taking collectMu would
// put this pass in front of every finalization again, which is the whole reason
// it moved off that path.
func (m *handoffManager) accountNode(ctx context.Context) error {
	root, err := openPrivateHandoffDirectory(m.root)
	if err != nil {
		return err
	}
	defer root.Close()
	m.reportNodeAccounting(m.measureNode(ctx, root, m.now().UTC()))
	return nil
}

// reportNodeAccounting is the one line per pass a person reads, and the same
// figures on the agent's status projection for whatever reads it next.
//
// Each number is worded to what it counts. They used to be one "unaccounted"
// total over two different facts -- directories that are not this agent's, and
// subtrees that stopped being the ones being measured -- described as only the
// first, which made the sentence wrong whenever the second was not zero.
func (m *handoffManager) reportNodeAccounting(status RetainedResultsStatus) {
	m.log("agent: retained results on this node: %d runs (%d still in flight), %d logical bytes, %d charged bytes across %d entries; %d quarantined records, still charged; %d run(s) measured incompletely; %d subtree(s) stopped being the directory this pass was measuring; %d entries under %q have no record and are neither measured nor removed",
		status.Runs, status.InFlight, status.LogicalBytes, status.ChargedBytes, status.Entries,
		status.QuarantinedRecords, status.Truncated, status.Replaced, status.Unrecorded, m.root)
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
// counted every pass (countUnrecorded) rather than silently invisible.
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
