package execsweep

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// SweepTempDirs removes allocations left after storage removal or a crash.
func SweepTempDirs(ctx context.Context) error {
	return sweepTempDirs(ctx, []string{"/var/tmp", "/tmp"}, time.Now())
}

func sweepTempDirs(ctx context.Context, roots []string, now time.Time) error {
	seen := make(map[string]bool)
	var failures []error
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		physical, err := filepath.EvalSymlinks(root)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if seen[physical] {
			continue
		}
		seen[physical] = true
		entries, err := os.ReadDir(physical)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return errors.Join(append(failures, err)...)
			}
			if !strings.HasPrefix(entry.Name(), "wefty-run-data.") {
				continue
			}
			path := filepath.Join(physical, entry.Name())
			if err := sweepTempDir(ctx, path, now); err != nil {
				failures = append(failures, fmt.Errorf("%s: %w", path, err))
			}
		}
	}
	return errors.Join(failures...)
}

func sweepTempDir(ctx context.Context, path string, now time.Time) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Getuid()) {
		return nil
	}
	ownerPath := filepath.Join(path, "owner")
	owner, err := os.Lstat(ownerPath)
	storage := ""
	if err == nil && owner.Mode().IsRegular() {
		if raw, err := os.ReadFile(ownerPath); err == nil {
			storage = strings.TrimSuffix(string(raw), "\n")
		}
	}
	if !filepath.IsAbs(storage) {
		// Incomplete initialization gets the same grace as a missing owner.
		if now.Sub(info.ModTime()) <= time.Hour {
			return nil
		}
	} else if _, err := os.Lstat(storage); !os.IsNotExist(err) {
		return err
	}
	// WalkDir does not follow links. Open each real directory before reading
	// its children, including folders a command left without owner access.
	if err := filepath.WalkDir(path, func(path string, entry fs.DirEntry, err error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.Chmod(path, 0700)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// RemoveAll unlinks entries inside the allocation without following links.
	return os.RemoveAll(path)
}
