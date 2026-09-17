package agent

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// mailboxFS is the publisher's entire view of one attempt's events directory.
// The publisher needs exactly three operations on it — a bounded listing, a
// bounded read of one entry, and the removal of an entry it is finished with —
// and nothing else: parsing, ordering, bookkeeping, the publication bounds and
// the fence all live above this seam and are the same whatever implements it.
//
// A process attempt's mailbox is a directory the agent itself opened, so the
// implementation here is an os.Root. An OCI attempt's handoff volume is
// helper-owned inside the node, so its implementation will be a bounded helper
// read path; keeping the publisher blind to the difference is what lets that
// arrive without touching the rules this file's caller enforces.
type mailboxFS interface {
	// list enumerates at most limit entries and reports whether the listing
	// reached that bound. Order is not part of the contract: the publisher
	// sorts, because lexical publication order must not depend on how any one
	// implementation happens to enumerate.
	list(limit int) (names []string, exhausted bool, err error)
	// read returns at most limit bytes of one entry and reports truncation.
	// An entry the implementation positively classifies as unpublishable is
	// reported as errRunMailboxEntryUnusable; every other failure -- a
	// transport, deadline, authority or I/O error -- is reported as itself.
	// The caller deletes the first and preserves the second, so the two must
	// never be conflated by an implementation.
	read(name string, limit int) (payload []byte, truncated bool, err error)
	// remove deletes one entry. A missing entry reports os.ErrNotExist so the
	// caller can treat an already-retired event as retired.
	remove(name string) error
	close() error
}

// errRunMailboxEntryUnusable marks an entry that can never become an event:
// not a readable regular file, or one that changed identity while being opened.
// It is the only read failure that permits removal. Anything else leaves the
// entry where it is, because deleting a workload's only copy of its evidence on
// the strength of a timeout is the one mistake this publisher must never make.
var errRunMailboxEntryUnusable = errors.New("run mailbox entry is not a publishable event")

// osRootMailboxFS reaches the events directory only through the root opened at
// preparation. The root is never re-resolved by name, so a workload that
// replaces events/ with a FIFO or a symlink afterwards changes nothing the
// agent looks at.
type osRootMailboxFS struct {
	events *os.Root
}

func newOSRootMailboxFS(events *os.Root) *osRootMailboxFS {
	return &osRootMailboxFS{events: events}
}

// list reads the whole listing up to a hard cap. Hitting the cap is reported
// as exhausted even if exactly that many entries exist; no partial listing is
// published and no page cursor depends on directory order.
func (fs *osRootMailboxFS) list(limit int) ([]string, bool, error) {
	directory, err := fs.events.Open(".")
	if err != nil {
		return nil, false, err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(limit)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names, len(names) >= limit, nil
}

// read opens the entry through the events directory opened at preparation,
// proves the object it actually opened is a regular file, and reads it under
// the size bound. The non-blocking open is what keeps a FIFO planted in the
// events directory from holding finalization open forever.
func (fs *osRootMailboxFS) read(name string, limit int) ([]byte, bool, error) {
	return readBoundedRegularFile(fs.events, name, limit)
}

// remove never recursively walks workload data. Unknown objects and nonempty
// directories fail here and remain pending, which forces handoff retention.
func (fs *osRootMailboxFS) remove(name string) error {
	info, err := fs.events.Lstat(name)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 && !info.IsDir() {
		return fmt.Errorf("run mailbox entry %q has unsupported type %s", name, info.Mode())
	}
	return fs.events.Remove(name)
}

func (fs *osRootMailboxFS) close() error {
	if fs == nil || fs.events == nil {
		return nil
	}
	return fs.events.Close()
}
