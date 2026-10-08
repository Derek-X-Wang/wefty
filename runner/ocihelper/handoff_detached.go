package ocihelper

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"
)

// handoffReadChunk bounds the allocation for each directory read.
const handoffReadChunk = 256

type detachedHandoffDirectory interface {
	Readdirnames(int) ([]string, error)
	Close() error
}

// freeDetachedHandoffRoot opens and frees a previously detached tree, then
// removes its root. Concurrent collectors may already have removed it.
func freeDetachedHandoffRoot(ctx context.Context, root string, beforeChild func(string) error) error {
	return freeDetachedHandoffRootWithOpenHook(ctx, root, beforeChild, nil)
}

// beforeOpen lets portable tests replace the detached name after Lstat.
func freeDetachedHandoffRootWithOpenHook(ctx context.Context, root string, beforeChild func(string) error, beforeOpen func()) error {
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// A detached name is deletion authority for this entry only. In
	// particular, never open a symlink and walk the target's children.
	if !info.IsDir() {
		if ctx != nil && ctx.Err() != nil {
			log.Printf("handoff retention: %s stays detached and the next pass frees it: %v", filepath.Base(root), ctx.Err())
			return nil
		}
		if err := os.Remove(root); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if beforeOpen != nil {
		beforeOpen()
	}
	rootHandle, err := os.OpenRoot(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer rootHandle.Close()
	opened, err := rootHandle.Stat(".")
	if err != nil {
		return err
	}
	if !os.SameFile(info, opened) {
		return errors.New("detached handoff directory changed before opening")
	}
	directory, err := rootHandle.Open(".")
	if err != nil {
		return err
	}
	complete, err := freeDetachedHandoffContents(ctx, filepath.Base(root), directory, func(child string) error {
		if beforeChild != nil {
			if err := beforeChild(child); err != nil {
				return err
			}
		}
		return rootHandle.RemoveAll(child)
	})
	if err != nil || !complete {
		return err
	}
	if err := os.Remove(root); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// freeDetachedHandoffContents shares the directory-reading path with portable
// tests. The caller has already detached the volume and removed its receipt
// under the retention lock; concurrent collectors may now free the same tree.
func freeDetachedHandoffContents(ctx context.Context, detached string, directory detachedHandoffDirectory, freeChild func(string) error) (complete bool, err error) {
	for {
		if ctx != nil && ctx.Err() != nil {
			log.Printf("handoff retention: %s stays detached and the next pass frees it: %v", detached, ctx.Err())
			return false, directory.Close()
		}
		children, readErr := directory.Readdirnames(handoffReadChunk)
		for _, child := range children {
			if ctx != nil && ctx.Err() != nil {
				log.Printf("handoff retention: %s stays detached and the next pass frees it: %v", detached, ctx.Err())
				return false, directory.Close()
			}
			if err := freeChild(child); err != nil {
				return false, errors.Join(err, directory.Close())
			}
		}
		// Linux may report ENOENT after returning buffered names when another
		// collector has removed the open directory. It is already deleted.
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, os.ErrNotExist) {
			break
		}
		// A non-directory detached entry has no children to free. Close it
		// cleanly and let the caller unlink the entry itself.
		if len(children) == 0 && errors.Is(readErr, syscall.ENOTDIR) {
			break
		}
		if readErr != nil {
			return false, errors.Join(readErr, directory.Close())
		}
		if len(children) == 0 {
			break
		}
	}
	if err := directory.Close(); err != nil {
		return false, err
	}
	return true, nil
}
