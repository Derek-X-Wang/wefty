package ocihelper

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// The run mailbox read path is privileged surface. The helper runs as root and
// the directory it reads is writable by the workload, so every operation here
// assumes the workload is actively trying to redirect it.
//
// The confinement is descriptor-based descent and nothing else. Only the
// managed root is ever opened by absolute path; every component below it is
// opened relative to the descriptor above it, refusing any component that is a
// symlink and proving with SameFile that the object opened is the object that
// was checked. No path from the wire is ever joined into a filesystem path: the
// wire carries an owner key, which is hashed into a deterministic directory
// name, a run ID and an entry name, both of which must be single bounded
// components. A read proves the opened object is a regular file before it
// returns a byte, and opens non-blocking so a FIFO cannot hold the helper.

// openRunMailboxEvents descends managedRoot -> handoffs -> <volume> ->
// .wefty -> <run id> -> events and returns the events directory. The caller
// closes it.
func openRunMailboxEvents(managedRoot string, reference RunMailboxReference) (*os.Root, error) {
	volume, err := DeterministicHandoffVolumeDirectory(reference.OwnerKey)
	if err != nil {
		return nil, err
	}
	if !ValidRunMailboxName(reference.RunID) {
		return nil, errors.New("run mailbox run ID is not a bounded mailbox name")
	}
	return openConfinedDirectory(managedRoot,
		"handoffs", volume, RunMailboxDirectoryName, reference.RunID, RunMailboxEventsDirectoryName)
}

// openConfinedDirectory opens the managed root and then walks one component at
// a time, never following a symlink and never re-resolving a name it already
// opened.
func openConfinedDirectory(root string, segments ...string) (_ *os.Root, resultErr error) {
	if err := rejectSymlinkComponents(root); err != nil {
		return nil, fmt.Errorf("managed root is not stable: %w", err)
	}
	current, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			_ = current.Close()
		}
	}()
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." || filepath.Base(segment) != segment {
			return nil, errors.New("run mailbox path contains an invalid component")
		}
		before, err := current.Lstat(segment)
		if err != nil {
			return nil, err
		}
		if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
			return nil, fmt.Errorf("run mailbox component %q is not a directory", segment)
		}
		next, err := current.OpenRoot(segment)
		if err != nil {
			return nil, err
		}
		opened, statErr := next.Stat(".")
		after, lstatErr := current.Lstat(segment)
		if statErr != nil || lstatErr != nil || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
			_ = next.Close()
			return nil, fmt.Errorf("run mailbox component %q changed while opening", segment)
		}
		_ = current.Close()
		current = next
	}
	return current, nil
}

// readConfinedRegularFile proves the entry is a regular file before and after
// opening it, opens non-blocking so a FIFO planted in the directory cannot
// block the helper, and reads no more than limit bytes.
func readConfinedRegularFile(root *os.Root, name string, limit int) ([]byte, bool, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, false, err
	}
	if !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("run mailbox entry %q is not a regular file", name)
	}
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, false, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, false, fmt.Errorf("run mailbox entry %q changed identity while opening", name)
	}
	payload, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, false, err
	}
	if len(payload) > limit {
		return payload[:limit], true, nil
	}
	return payload, false, nil
}

// seedRunMailbox creates the run-scoped mailbox inside an already-created
// handoff volume and delivers the run's parameters.
//
// Ownership is the honest part of this design, and it is deliberately the
// smallest arrangement that works. The handoff volume is created root-owned and
// the container process may be any uid the image declares, so exactly the two
// directories the workload must write — tmp/ and events/ — are chowned to that
// process owner, the same way the service-data volume and the Computer control
// files already are. Everything above them stays root-owned and merely
// traversable (0711): the workload can reach its own mailbox but cannot replace
// events/ with a symlink, cannot rewrite params.json, and cannot enumerate the
// directory it was handed. The agent's own bookkeeping is not in the volume at
// all — for an OCI attempt it lives on the agent side of the boundary, so a
// workload cannot reach or forge it, which is strictly stronger than the
// advisory, workload-writable bookkeeping a process attempt has.
func seedRunMailbox(volumePath string, seed RunMailboxSeed, uid, gid uint32) error {
	if err := seed.validate(); err != nil {
		return err
	}
	root, err := openConfinedDirectory(volumePath)
	if err != nil {
		return fmt.Errorf("open handoff volume for the run mailbox: %w", err)
	}
	defer root.Close()
	mailbox, err := makeRunMailboxDirectory(root, RunMailboxDirectoryName, 0o711, 0, 0)
	if err != nil {
		return err
	}
	defer mailbox.Close()
	scoped, err := makeRunMailboxDirectory(mailbox, seed.RunID, 0o711, 0, 0)
	if err != nil {
		return err
	}
	defer scoped.Close()
	for _, name := range []string{RunMailboxStagingDirectoryName, RunMailboxEventsDirectoryName} {
		child, err := makeRunMailboxDirectory(scoped, name, 0o700, uid, gid)
		if err != nil {
			return err
		}
		_ = child.Close()
	}
	document := seed.Params
	if len(document) == 0 {
		document = []byte("{}\n")
	}
	return writeRunMailboxParams(scoped, document)
}

// makeRunMailboxDirectory creates the component if it is absent, refuses
// anything that is not already a real directory, and applies ownership through
// the opened descriptor rather than by name.
func makeRunMailboxDirectory(parent *os.Root, name string, mode os.FileMode, uid, gid uint32) (_ *os.Root, resultErr error) {
	if err := parent.Mkdir(name, mode); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create run mailbox directory %q: %w", name, err)
	}
	before, err := parent.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("inspect run mailbox directory %q: %w", name, err)
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return nil, fmt.Errorf("run mailbox path %q is not a directory", name)
	}
	opened, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("open run mailbox directory %q: %w", name, err)
	}
	defer func() {
		if resultErr != nil {
			_ = opened.Close()
		}
	}()
	current, err := opened.Stat(".")
	if err != nil || !os.SameFile(before, current) {
		return nil, fmt.Errorf("run mailbox directory %q changed while opening", name)
	}
	// Mode and ownership are applied through the descriptor just opened, never
	// by name, and always explicitly: the helper's umask must not decide
	// whether the workload can reach its own mailbox, and a directory that
	// already existed from an earlier attempt must be corrected rather than
	// trusted.
	handle, err := opened.Open(".")
	if err != nil {
		return nil, err
	}
	applyErr := handle.Chmod(mode)
	if applyErr == nil && (uid != 0 || gid != 0) {
		if err := unix.Fchown(int(handle.Fd()), int(uid), int(gid)); err != nil {
			applyErr = fmt.Errorf("own run mailbox directory %q as %d:%d: %w", name, uid, gid, err)
		}
	}
	closeErr := handle.Close()
	if applyErr != nil {
		return nil, applyErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return opened, nil
}

// writeRunMailboxParams writes then renames inside the mailbox, so the
// workload never observes a partial document and a name a workload planted is
// replaced rather than written through. The document stays root-owned and
// world-readable: every uid the image might declare must be able to read its
// own run's parameters, and none of them may rewrite them.
func writeRunMailboxParams(mailbox *os.Root, document []byte) (resultErr error) {
	staging := RunMailboxParamsFileName + ".tmp"
	if err := mailbox.Remove(staging); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := mailbox.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create run mailbox params: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = mailbox.Remove(staging)
		}
	}()
	if _, err := file.Write(document); err != nil {
		_ = file.Close()
		return err
	}
	// Umask may have narrowed the mode the create requested; the document
	// carries no secret and every uid must be able to read it.
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return mailbox.Rename(staging, RunMailboxParamsFileName)
}

func listRunMailbox(runtimeRoot string, request ListRunMailboxRequest) (ListRunMailboxResponse, error) {
	events, err := openRunMailboxEvents(runtimeRoot, request.RunMailboxReference)
	if err != nil {
		return ListRunMailboxResponse{}, err
	}
	defer events.Close()
	directory, err := events.Open(".")
	if err != nil {
		return ListRunMailboxResponse{}, err
	}
	defer directory.Close()
	limit := request.boundedLimit()
	entries, err := directory.ReadDir(limit)
	if err != nil && !errors.Is(err, io.EOF) {
		return ListRunMailboxResponse{}, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		// An entry the agent could never ask about is still reported, so the
		// agent's own junk-removal rules see it and the handoff is retained
		// rather than silently looking drained.
		names = append(names, entry.Name())
	}
	return ListRunMailboxResponse{Names: names, Exhausted: len(names) >= limit}, nil
}

func readRunMailbox(runtimeRoot string, request ReadRunMailboxRequest) (ReadRunMailboxResponse, error) {
	if !ValidRunMailboxName(request.Name) {
		return ReadRunMailboxResponse{}, errors.New("run mailbox entry name is not a bounded mailbox name")
	}
	events, err := openRunMailboxEvents(runtimeRoot, request.RunMailboxReference)
	if err != nil {
		return ReadRunMailboxResponse{}, err
	}
	defer events.Close()
	limit := request.boundedLimit()
	payload, truncated, err := readConfinedRegularFile(events, request.Name, limit)
	if err != nil {
		return ReadRunMailboxResponse{}, err
	}
	return ReadRunMailboxResponse{Payload: payload, Truncated: truncated}, nil
}

func removeRunMailboxEntry(runtimeRoot string, request RemoveRunMailboxEntryRequest) (RemoveRunMailboxEntryResponse, error) {
	if !ValidRunMailboxName(request.Name) {
		return RemoveRunMailboxEntryResponse{}, errors.New("run mailbox entry name is not a bounded mailbox name")
	}
	events, err := openRunMailboxEvents(runtimeRoot, request.RunMailboxReference)
	if err != nil {
		return RemoveRunMailboxEntryResponse{}, err
	}
	defer events.Close()
	info, err := events.Lstat(request.Name)
	if errors.Is(err, os.ErrNotExist) {
		return RemoveRunMailboxEntryResponse{Absent: true}, nil
	}
	if err != nil {
		return RemoveRunMailboxEntryResponse{}, err
	}
	// Removal never recurses. A nonempty directory or an object the helper
	// cannot classify stays where it is, which keeps the agent's sweep
	// undrained and the handoff volume retained.
	if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 && !info.IsDir() {
		return RemoveRunMailboxEntryResponse{}, fmt.Errorf("run mailbox entry %q has unsupported type %s", request.Name, info.Mode())
	}
	if err := events.Remove(request.Name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RemoveRunMailboxEntryResponse{Absent: true}, nil
		}
		return RemoveRunMailboxEntryResponse{}, err
	}
	return RemoveRunMailboxEntryResponse{Removed: true}, nil
}
