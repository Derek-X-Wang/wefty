package execsweep

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
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
		{"wefty-run-data.live", storage, true},
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
	if err := sweepTempDirs(t.Context(), []string{root, alias, filepath.Join(root, "absent")}, []string{root}, now); err != nil {
		t.Fatal(err)
	}
	for name, removed := range map[string]bool{
		"wefty-run-data.gone": true, "wefty-run-data.gone-fresh": false, "wefty-run-data.live": false,
		"wefty-run-data.old": false, "wefty-run-data.fresh": false,
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
				if err := sweepTempDirs(t.Context(), []string{root}, []string{root}, now); err != nil {
					t.Fatal(err)
				}
				_, err = os.Lstat(allocation)
				if err != nil {
					t.Fatalf("unattributable allocation removed: %v", err)
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
	if err := sweepTempDirs(t.Context(), []string{root}, []string{root}, time.Now().Add(2*time.Hour)); err != nil {
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
	if err := sweepTempDirs(ctx, []string{root}, []string{root}, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled sweep: %v", err)
	}
	// Also exercise the allocation walk directly with cancellation already set.
	if err := os.WriteFile(filepath.Join(allocation, "owner"), []byte(filepath.Join(root, "gone")), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(allocation, old, old); err != nil {
		t.Fatal(err)
	}
	if err := sweepTempDir(ctx, allocation, []string{root}, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled walk: %v", err)
	}
	if _, err := os.Stat(allocation); err != nil {
		t.Fatalf("cancelled sweep removed allocation: %v", err)
	}
}

func TestSweepTempDirsOverride(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	t.Setenv("WEFTY_EXEC_TEMP_ROOT", root)
	roots, err := tempRoots()
	if err != nil || !slices.Equal(roots, []string{root}) {
		t.Fatalf("scan roots = %q, error = %v", roots, err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, parent := range []string{root, outside} {
		path := filepath.Join(parent, "wefty-run-data.orphan")
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "owner"), []byte(filepath.Join(root, "gone")), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := SweepTempDirs(t.Context(), []string{root}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "wefty-run-data.orphan")); !os.IsNotExist(err) {
		t.Fatalf("configured orphan survived: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "wefty-run-data.orphan")); err != nil {
		t.Fatalf("outside orphan touched: %v", err)
	}
}

func TestTempRoots(t *testing.T) {
	t.Setenv("WEFTY_EXEC_TEMP_ROOT", "")
	if roots, err := tempRoots(); err != nil || !slices.Equal(roots, []string{"/var/tmp", "/tmp"}) {
		t.Fatalf("default roots = %q, error = %v", roots, err)
	}
	t.Setenv("WEFTY_EXEC_TEMP_ROOT", "relative")
	if err := SweepTempDirs(t.Context(), []string{t.TempDir()}); err == nil {
		t.Fatal("relative root accepted")
	}
}

// A shared allocation root must never let one installation sweep another's data.
func TestSweepTempDirsOwnerRoots(t *testing.T) {
	root, owners := t.TempDir(), t.TempDir()
	owners, err := filepath.EvalSymlinks(owners)
	if err != nil {
		t.Fatal(err)
	}
	a, b := filepath.Join(owners, "A"), filepath.Join(owners, "B")
	if err := os.Mkdir(a, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(owners, "alias")
	if err := os.Symlink(a, alias); err != nil {
		t.Fatal(err)
	}
	physical, err := filepath.EvalSymlinks(a)
	if err != nil {
		t.Fatal(err)
	}
	for _, roots := range [][]string{nil, {}, {a}, {alias}} {
		t.Run(strings.Join(roots, ","), func(t *testing.T) {
			t.Setenv("WEFTY_EXEC_TEMP_ROOT", root)
			for name, storage := range map[string]string{
				"A": filepath.Join(a, "gone"), "B": filepath.Join(b, "gone"),
				"prefix":   filepath.Join(a+"-other", "gone"),
				"physical": filepath.Join(physical, "gone"), "alias": filepath.Join(alias, "gone"),
				"equal": a, "cleaned-outside": a + "/../B/gone",
			} {
				path := filepath.Join(root, "wefty-run-data."+name)
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "owner"), []byte(storage+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
				old := time.Now().Add(-2 * time.Hour)
				if err := os.Chtimes(path, old, old); err != nil {
					t.Fatal(err)
				}
			}
			if err := SweepTempDirs(t.Context(), roots); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"A", "B", "prefix", "physical", "alias", "equal", "cleaned-outside"} {
				path := filepath.Join(root, "wefty-run-data."+name)
				removed := len(roots) > 0 && (name == "physical" || name == "A" || name == "alias" && roots[0] == alias)
				_, err := os.Stat(path)
				if removed && !os.IsNotExist(err) || !removed && err != nil {
					t.Errorf("%s removed=%v: %v", name, removed, err)
				}
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestTempRootHomeRejection(t *testing.T) {
	for _, kind := range []string{"home", "child", "symlink", "home-symlink"} {
		t.Run(kind, func(t *testing.T) {
			home := t.TempDir()
			root := home
			if kind == "child" || kind == "symlink" {
				root = filepath.Join(home, "child")
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
			}
			physical, err := filepath.EvalSymlinks(root)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "symlink" || kind == "home-symlink" {
				alias := filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(root, alias); err != nil {
					t.Fatal(err)
				}
				if kind == "symlink" {
					root = alias
				} else {
					home = alias
				}
			}
			t.Setenv("HOME", home)
			t.Setenv("WEFTY_EXEC_TEMP_ROOT", root)
			storage := filepath.Join(t.TempDir(), "gone")
			allocation := filepath.Join(physical, "wefty-run-data.kept")
			if err := os.Mkdir(allocation, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(allocation, "owner"), []byte(storage+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			old := time.Now().Add(-2 * time.Hour)
			if err := os.Chtimes(allocation, old, old); err != nil {
				t.Fatal(err)
			}
			if err := SweepTempDirs(t.Context(), []string{filepath.Dir(storage)}); err == nil {
				t.Fatal("home override accepted")
			}
			if _, err := os.Stat(allocation); err != nil {
				t.Fatalf("rejected sweep touched allocation: %v", err)
			}
			record := t.TempDir()
			if err := os.WriteFile(filepath.Join(record, "tmp-dir"), []byte(allocation+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), "/bin/bash", "-c", Script+`
exec_storage_temp_dir "$1" "$2"; [ -z "$tmp_dir" ]`, "bash", record, storage)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("cleanup accepted home allocation: %v %s", err, output)
			}
		})
	}
}
