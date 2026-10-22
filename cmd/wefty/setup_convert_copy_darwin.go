//go:build darwin

package main

import (
	"context"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func cloneConversionInventoryFile(ctx context.Context, source *os.File, to string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(to))
	if err != nil {
		return err
	}
	defer directory.Close()
	// Source is the descriptor opened through os.Root, never a reopened path.
	return unix.Fclonefileat(int(source.Fd()), int(directory.Fd()), filepath.Base(to), unix.CLONE_NOFOLLOW)
}
