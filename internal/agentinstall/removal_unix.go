//go:build darwin || linux

package agentinstall

import (
	"errors"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"golang.org/x/sys/unix"
	"os"
)

// Open, validate and lock the same inode. Path changes cannot transfer the
// lifetime lock or redirect unlinkat to a sibling generation.
type heldGenerationDirectory struct {
	root *os.Root
	fd   *os.File
	info os.FileInfo
}

func holdGenerationDirectory(parent *os.Root, id string) (*heldGenerationDirectory, error) {
	before, err := parent.Lstat(id)
	if err != nil {
		return nil, err
	}
	if !privateInfo(before, 0700) {
		return nil, &VerificationRefusal{Message: "generation directory ownership or type changed"}
	}
	dir, err := parent.OpenRoot(id)
	if err != nil {
		return nil, err
	}
	f, err := dir.Open(".")
	if err != nil {
		dir.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		dir.Close()
		return nil, err
	}
	h := &heldGenerationDirectory{dir, f, info}
	if !privateInfo(info, 0700) || !os.SameFile(before, info) {
		h.Close()
		return nil, &VerificationRefusal{Message: "generation directory ownership changed"}
	}
	if err = h.verify(parent, id); err != nil {
		h.Close()
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		h.Close()
		return nil, err
	}
	return h, nil
}
func (h *heldGenerationDirectory) Close() {
	_ = unix.Flock(int(h.fd.Fd()), unix.LOCK_UN)
	h.fd.Close()
	h.root.Close()
}
func (h *heldGenerationDirectory) verify(parent *os.Root, id string) error {
	info, err := parent.Lstat(id)
	if err != nil {
		return err
	}
	if !info.IsDir() || !os.SameFile(info, h.info) {
		return &VerificationRefusal{Message: "generation directory identity changed"}
	}
	return nil
}
func (h *heldGenerationDirectory) remove(parent *os.Root, id, name string) error {
	if err := h.verify(parent, id); err != nil {
		return err
	}
	if err := unix.Unlinkat(int(h.fd.Fd()), name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return &os.PathError{Op: "unlinkat", Path: name, Err: err}
	}
	return nil
}
func (h *heldGenerationDirectory) retainEmpty(parent *os.Root, id string, afterVerify func(string) error) error {
	if err := h.verify(parent, id); err != nil {
		return err
	}
	f, err := h.root.Open(".")
	if err != nil {
		return err
	}
	entries, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return &VerificationRefusal{Message: "generation directory retains unowned files"}
	}
	// The directory remains, so its unlinks need their own durable sync before
	// the parent catalog can forget this generation.
	if err := durable.SyncDir(h.root); err != nil {
		return err
	}
	// Retain the empty directory. unlinkat(parent, id, AT_REMOVEDIR) would
	// resolve id again after verification and could remove a replacement inode.
	// Catalog removal and receipt-bound file cleanup do not require that unlink.
	if err := h.verify(parent, id); err != nil {
		return err
	}
	// Deliberately after the last namespace check: even a swap here has no
	// destructive effect because the empty entry is retained, never unlinked.
	if afterVerify != nil {
		return afterVerify(id)
	}
	return nil
}
