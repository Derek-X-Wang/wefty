package ocihelper

import (
	"context"
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

// openRunMailboxScope descends managedRoot -> handoffs -> <volume> and then,
// for the event scope, on through .wefty -> <run id> -> events. The handoff
// scope stops at the volume, which is where a run writes result.json. The
// caller closes the returned root.
//
// Both descents use the identical confinement: one component at a time, no
// symlink followed, SameFile proof on every step. The scope chooses between two
// fixed descents and is never itself a path component.
func openRunMailboxScope(managedRoot string, reference RunMailboxReference) (*os.Root, error) {
	volume, err := DeterministicHandoffVolumeDirectory(reference.OwnerKey)
	if err != nil {
		return nil, err
	}
	if !ValidRunMailboxName(reference.RunID) {
		return nil, errors.New("run mailbox run ID is not a bounded mailbox name")
	}
	switch reference.Scope {
	case RunMailboxScopeHandoffFiles:
		return openConfinedDirectory(managedRoot, "handoffs", volume)
	case RunMailboxScopeEvents:
		return openConfinedDirectory(managedRoot,
			"handoffs", volume, RunMailboxDirectoryName, reference.RunID, RunMailboxEventsDirectoryName)
	default:
		return nil, fmt.Errorf("run mailbox scope %q is not a known scope", string(reference.Scope))
	}
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

// runMailboxUnusableEntryError is the helper's positive classification of an
// entry that can never become an event. It is separated from every other
// failure because the two must not be confused at the far end: junk is safe to
// delete, and an unreachable entry never is.
type runMailboxUnusableEntryError struct {
	name   string
	reason string
}

func (err *runMailboxUnusableEntryError) Error() string {
	return fmt.Sprintf("run mailbox entry %q %s", err.name, err.reason)
}

func unusableRunMailboxEntry(name, reason string) error {
	return &runMailboxUnusableEntryError{name: name, reason: reason}
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
		return nil, false, unusableRunMailboxEntry(name, "is not a regular file")
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
		return nil, false, unusableRunMailboxEntry(name, "changed identity while opening")
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
// Be precise about what the permissions here are and are not.
//
// They are NOT the security boundary. An image that declares no USER runs as
// 0:0, wefty gives it no user namespace, and root inside the container is root
// on this bind mount: such a workload can rewrite params.json and replace the
// directories below regardless of what is set here. The security boundary is
// the helper's descriptor descent in this file — the agent never opens this
// volume, and a workload that replaces events/ with a symlink only makes its
// own mailbox unreadable, because the descent refuses it. The volume is scoped
// to one Run, so there is no sibling run inside it to reach, and the agent's
// bookkeeping is not in the volume at all: for an OCI attempt it lives on the
// agent side of the boundary, so it cannot be forged from in here at any uid.
//
// What they ARE is defense in depth for the ordinary case, a non-root image.
// Exactly the two directories such a workload must write — tmp/ and events/ —
// are chowned to its process owner, the same way the service-data volume and
// the Computer control files already are. Everything above them stays
// root-owned and traversable but not writable (0711), so a non-root workload
// reaches its own mailbox, reads its parameters, and can neither rewrite them
// nor enumerate the directory it was handed.
//
// The volume root itself is part of that chain: containerd creates the handoff
// directory 0700 root-owned and mounts it at /wefty/handoff, so without the
// 0711 applied here a non-root image could not traverse into its mailbox at
// all, whatever the modes below.
func seedRunMailbox(volumePath string, seed RunMailboxSeed, uid, gid uint32) error {
	if err := seed.validate(); err != nil {
		return err
	}
	root, err := openConfinedDirectory(volumePath)
	if err != nil {
		return fmt.Errorf("open handoff volume for the run mailbox: %w", err)
	}
	defer root.Close()
	// The directories above the workload's two belong to the helper. That is
	// the invariant, and it is stated as the helper's own identity rather than
	// as a literal 0 so it stays true of whatever the helper runs as -- root in
	// production, and whoever runs the tests elsewhere.
	helperUID, helperGID := uint32(os.Geteuid()), uint32(os.Getegid())
	if err := applyRunMailboxOwnership(root, "handoff volume root", 0o711, helperUID, helperGID); err != nil {
		return err
	}
	mailbox, err := makeRunMailboxDirectory(root, RunMailboxDirectoryName, 0o711, helperUID, helperGID)
	if err != nil {
		return err
	}
	defer mailbox.Close()
	scoped, err := makeRunMailboxDirectory(mailbox, seed.RunID, 0o711, helperUID, helperGID)
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
	if err := applyRunMailboxOwnership(opened, name, mode, uid, gid); err != nil {
		return nil, err
	}
	return opened, nil
}

// applyRunMailboxOwnership sets mode and ownership through an already-opened
// descriptor, never by name. The helper's umask must not decide whether the
// workload can reach its own mailbox, and a directory left behind by an earlier
// attempt must be restored to what this attempt requires rather than trusted as
// it stands -- including back to root ownership, which is why there is no
// special case for a requested owner of 0:0.
//
// The chown is issued only when the observed owner differs from the requested
// one. That is not an optimization: chowning to an owner an unprivileged
// process does not hold is refused even when it is already correct, so a
// conditional chown is what keeps this function honest about the one thing it
// must do -- change ownership that drifted -- without failing on the far more
// common case where nothing has.
func applyRunMailboxOwnership(root *os.Root, name string, mode os.FileMode, uid, gid uint32) error {
	handle, err := root.Open(".")
	if err != nil {
		return err
	}
	applyErr := handle.Chmod(mode)
	if applyErr == nil {
		var observed unix.Stat_t
		if err := unix.Fstat(int(handle.Fd()), &observed); err != nil {
			applyErr = fmt.Errorf("inspect run mailbox directory %q owner: %w", name, err)
		} else if observed.Uid != uid || observed.Gid != gid {
			if err := unix.Fchown(int(handle.Fd()), int(uid), int(gid)); err != nil {
				applyErr = fmt.Errorf("own run mailbox directory %q as %d:%d: %w", name, uid, gid, err)
			}
		}
	}
	closeErr := handle.Close()
	if applyErr != nil {
		return applyErr
	}
	return closeErr
}

// writeRunMailboxParams writes then renames inside the mailbox, so the
// workload never observes a partial document and a name a workload planted is
// replaced rather than written through. The document is helper-owned and
// world-readable: every uid the image might declare must be able to read its
// own run's parameters, and a non-root one cannot rewrite them. A uid-0
// workload can, as it can rewrite anything in the volume; that is the
// defense-in-depth limit stated above, not a boundary.
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

func listRunMailbox(ctx context.Context, runtimeRoot string, request ListRunMailboxRequest) (ListRunMailboxResponse, error) {
	if err := ctx.Err(); err != nil {
		return ListRunMailboxResponse{}, err
	}
	events, err := openRunMailboxScope(runtimeRoot, request.RunMailboxReference)
	if err != nil {
		return ListRunMailboxResponse{}, err
	}
	defer events.Close()
	directory, err := events.Open(".")
	if err != nil {
		return ListRunMailboxResponse{}, err
	}
	defer directory.Close()
	if err := ctx.Err(); err != nil {
		return ListRunMailboxResponse{}, err
	}
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

func readRunMailbox(ctx context.Context, runtimeRoot string, request ReadRunMailboxRequest) (ReadRunMailboxResponse, error) {
	if err := ctx.Err(); err != nil {
		return ReadRunMailboxResponse{}, err
	}
	if !ValidRunMailboxName(request.Name) {
		return ReadRunMailboxResponse{}, errors.New("run mailbox entry name is not a bounded mailbox name")
	}
	events, err := openRunMailboxScope(runtimeRoot, request.RunMailboxReference)
	if err != nil {
		return ReadRunMailboxResponse{}, err
	}
	defer events.Close()
	if err := ctx.Err(); err != nil {
		return ReadRunMailboxResponse{}, err
	}
	limit := request.boundedLimit()
	payload, truncated, err := readConfinedRegularFile(events, request.Name, limit)
	if err != nil {
		// An entry that is not there is reported as absent rather than as a
		// failure, so the caller can tell "nothing was written" apart from "I
		// could not read what was written".
		if errors.Is(err, os.ErrNotExist) {
			return ReadRunMailboxResponse{Absent: true}, nil
		}
		// An entry the helper positively classified as unpublishable is a
		// fact about the entry, reported in the response. Everything else --
		// an I/O failure, a cancelled context -- stays an error, because the
		// caller must not mistake "I could not read this" for "this is junk"
		// and delete a workload's only copy of its evidence.
		var unusable *runMailboxUnusableEntryError
		if errors.As(err, &unusable) {
			return ReadRunMailboxResponse{Unusable: true, Reason: unusable.reason}, nil
		}
		return ReadRunMailboxResponse{}, err
	}
	return ReadRunMailboxResponse{Payload: payload, Truncated: truncated}, nil
}

func removeRunMailboxEntry(ctx context.Context, runtimeRoot string, request RemoveRunMailboxEntryRequest) (RemoveRunMailboxEntryResponse, error) {
	if err := ctx.Err(); err != nil {
		return RemoveRunMailboxEntryResponse{}, err
	}
	if !ValidRunMailboxName(request.Name) {
		return RemoveRunMailboxEntryResponse{}, errors.New("run mailbox entry name is not a bounded mailbox name")
	}
	events, err := openRunMailboxScope(runtimeRoot, request.RunMailboxReference)
	if err != nil {
		return RemoveRunMailboxEntryResponse{}, err
	}
	defer events.Close()
	if err := ctx.Err(); err != nil {
		return RemoveRunMailboxEntryResponse{}, err
	}
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
