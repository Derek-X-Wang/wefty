package execsweep

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSweepTempDirs(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	storage := filepath.Join(root, "storage")
	if err := os.Mkdir(storage, 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, owner string
		old         bool
	}{
		{"wefty-run-data.gone", filepath.Join(root, "gone"), true},
		{"wefty-run-data.gone-fresh", filepath.Join(root, "gone"), false},
		{"wefty-run-data.live", storage, false},
		{"wefty-run-data.old", "", true},
		{"wefty-run-data.fresh", "", false},
		{"foreign", filepath.Join(root, "gone"), true},
	} {
		path := filepath.Join(root, tc.name)
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if tc.owner != "" {
			if err := os.WriteFile(filepath.Join(path, "owner"), []byte(tc.owner+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		// Neither a linked allocation nor links inside one may delete outside data.
		if err := os.Symlink(storage, filepath.Join(path, "linked-storage")); err != nil {
			t.Fatal(err)
		}
		if tc.old {
			old := now.Add(-2 * time.Hour)
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	link := filepath.Join(root, "wefty-run-data.symlink")
	if err := os.Symlink(storage, link); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if err := sweepTempDirs(t.Context(), []string{root, alias, filepath.Join(root, "absent")}, now); err != nil {
		t.Fatal(err)
	}
	for name, removed := range map[string]bool{
		"wefty-run-data.gone": true, "wefty-run-data.gone-fresh": false, "wefty-run-data.live": false,
		"wefty-run-data.old": true, "wefty-run-data.fresh": false,
		"foreign": false, "wefty-run-data.symlink": false, "storage": false,
	} {
		_, err := os.Lstat(filepath.Join(root, name))
		if removed && !os.IsNotExist(err) || !removed && err != nil {
			t.Errorf("%s removed=%v: %v", name, removed, err)
		}
	}
}

func TestSweepTempDirsInvalidOwner(t *testing.T) {
	for _, kind := range []string{"empty", "directory", "symlink", "unreadable", "relative"} {
		for _, old := range []bool{false, true} {
			name := kind + "/fresh"
			if old {
				name = kind + "/old"
			}
			t.Run(name, func(t *testing.T) {
				root := t.TempDir()
				allocation := filepath.Join(root, "wefty-run-data.invalid")
				if err := os.Mkdir(allocation, 0700); err != nil {
					t.Fatal(err)
				}
				owner := filepath.Join(allocation, "owner")
				var err error
				switch kind {
				case "empty":
					err = os.WriteFile(owner, nil, 0600)
				case "directory":
					err = os.Mkdir(owner, 0700)
				case "symlink":
					err = os.Symlink(filepath.Join(root, "absent"), owner)
				case "unreadable":
					// A readable owner naming live storage would keep this forever.
					err = os.WriteFile(owner, []byte(root+"\n"), 0000)
				case "relative":
					err = os.WriteFile(owner, []byte("relative\n"), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
				now := time.Now()
				if old {
					then := now.Add(-2 * time.Hour)
					if err := os.Chtimes(allocation, then, then); err != nil {
						t.Fatal(err)
					}
				}
				if err := sweepTempDirs(t.Context(), []string{root}, now); err != nil {
					t.Fatal(err)
				}
				_, err = os.Lstat(allocation)
				if old && !os.IsNotExist(err) || !old && err != nil {
					t.Fatalf("removed=%v: %v", old, err)
				}
			})
		}
	}
}

func TestSweepTempDirsLockedDescendant(t *testing.T) {
	root := t.TempDir()
	storage := filepath.Join(root, "storage")
	if err := os.Mkdir(storage, 0700); err != nil {
		t.Fatal(err)
	}
	allocation := filepath.Join(root, "wefty-run-data.locked")
	locked := filepath.Join(allocation, "tmp", "locked")
	if err := os.MkdirAll(locked, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0700) })
	if err := os.WriteFile(filepath.Join(locked, "data"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(allocation, "owner"), []byte(storage+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0000); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(storage); err != nil {
		t.Fatal(err)
	}
	if err := sweepTempDirs(t.Context(), []string{root}, time.Now().Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(allocation); !os.IsNotExist(err) {
		t.Fatalf("locked allocation survived: %v", err)
	}
}

func TestSweepTempDirsCancelled(t *testing.T) {
	root := t.TempDir()
	allocation := filepath.Join(root, "wefty-run-data.cancelled")
	if err := os.Mkdir(allocation, 0700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(allocation, old, old); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sweepTempDirs(ctx, []string{root}, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled sweep: %v", err)
	}
	// Also exercise the allocation walk directly with cancellation already set.
	if err := sweepTempDir(ctx, allocation, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled walk: %v", err)
	}
	if _, err := os.Stat(allocation); err != nil {
		t.Fatalf("cancelled sweep removed allocation: %v", err)
	}
}
