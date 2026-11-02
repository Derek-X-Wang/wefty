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

// SweepTempDirs removes abandoned allocations whose recorded storage is beneath
// one of the caller's ownerRoots. Unattributable allocations stay intact.
func SweepTempDirs(ctx context.Context, ownerRoots []string) error {
	if len(ownerRoots) == 0 {
		return nil
	}
	roots, err := tempRoots()
	if err != nil {
		return err
	}
	return sweepTempDirs(ctx, roots, ownerRoots, time.Now())
}

func tempRoots() ([]string, error) {
	if root := os.Getenv("WEFTY_EXEC_TEMP_ROOT"); root != "" {
		if !filepath.IsAbs(root) {
			return nil, fmt.Errorf("WEFTY_EXEC_TEMP_ROOT must be absolute: %q", root)
		}
		physical, err := filepath.EvalSymlinks(root)
		if err != nil {
			return nil, err
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		physicalHome, err := filepath.EvalSymlinks(home)
		if err != nil {
			return nil, err
		}
		if physical == physicalHome || beneath(physical, physicalHome) {
			return nil, fmt.Errorf("WEFTY_EXEC_TEMP_ROOT must be outside home: %q", root)
		}
		return []string{root}, nil
	}
	return []string{"/var/tmp", "/tmp"}, nil
}

// beneath compares cleaned absolute paths with a separator boundary.
func beneath(path, root string) bool {
	return strings.HasPrefix(path, strings.TrimSuffix(root, string(filepath.Separator))+string(filepath.Separator)) && path != root
}

func sweepTempDirs(ctx context.Context, roots, ownerRoots []string, now time.Time) error {
	var storageRoots []string
	for _, root := range ownerRoots {
		if !filepath.IsAbs(root) {
			continue
		}
		root = filepath.Clean(root)
		storageRoots = append(storageRoots, root)
		if physical, err := filepath.EvalSymlinks(root); err == nil {
			storageRoots = append(storageRoots, physical)
		}
	}
	if len(storageRoots) == 0 {
		return nil
	}
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
			if err := sweepTempDir(ctx, path, storageRoots, now); err != nil {
				failures = append(failures, fmt.Errorf("%s: %w", path, err))
			}
		}
	}
	return errors.Join(failures...)
}

func sweepTempDir(ctx context.Context, path string, ownerRoots []string, now time.Time) error {
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
	// A young allocation may still be initializing, or its storage may be
	// mid-retirement; leftovers wait an hour, which also keeps concurrent
	// sweeps away from fresh fixtures.
	if now.Sub(info.ModTime()) <= time.Hour {
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
	// Missing or invalid owners cannot be attributed to this caller.
	if !filepath.IsAbs(storage) {
		return nil
	}
	storage = filepath.Clean(storage)
	owned := false
	for _, root := range ownerRoots {
		if beneath(storage, root) {
			owned = true
			break
		}
	}
	if !owned {
		return nil
	}
	if _, err := os.Lstat(storage); !os.IsNotExist(err) {
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
