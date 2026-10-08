package ocihelper

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
)

// handoffReadChunk bounds the allocation for each directory read.
const handoffReadChunk = 256

type detachedHandoffDirectory interface {
	Readdirnames(int) ([]string, error)
	Close() error
}

// freeDetachedHandoffContents shares the directory-reading path with portable
// tests. The caller has already detached the volume and removed its receipt
// under the retention lock; concurrent collectors may now free the same tree.
func freeDetachedHandoffContents(ctx context.Context, detached string, directory detachedHandoffDirectory, freeChild func(string) error) error {
	for {
		if ctx != nil && ctx.Err() != nil {
			log.Printf("handoff retention: %s stays detached and the next pass frees it: %v", detached, ctx.Err())
			return directory.Close()
		}
		children, readErr := directory.Readdirnames(handoffReadChunk)
		for _, child := range children {
			if ctx != nil && ctx.Err() != nil {
				log.Printf("handoff retention: %s stays detached and the next pass frees it: %v", detached, ctx.Err())
				return directory.Close()
			}
			if err := freeChild(child); err != nil {
				return errors.Join(err, directory.Close())
			}
		}
		// Linux may report ENOENT after returning buffered names when another
		// collector has removed the open directory. It is already deleted.
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, os.ErrNotExist) {
			break
		}
		if readErr != nil {
			return errors.Join(readErr, directory.Close())
		}
		if len(children) == 0 {
			break
		}
	}
	if err := directory.Close(); err != nil {
		return err
	}
	return nil
}
