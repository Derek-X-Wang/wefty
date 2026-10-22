package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSetupConvertInventoryCloneFallback(t *testing.T) {
	for _, failure := range []error{syscall.ENOTSUP, syscall.EXDEV} {
		t.Run(failure.Error(), func(t *testing.T) {
			directory := t.TempDir()
			source := filepath.Join(directory, "source")
			target := filepath.Join(directory, "target")
			payload := []byte("fallback bytes must match the original")
			if err := os.WriteFile(source, payload, 0640); err != nil {
				t.Fatal(err)
			}
			original, err := os.Stat(source)
			if err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			clones, spaceChecks := 0, 0
			ops := &plainConversionCopyOps{clone: func(context.Context, *os.File, string) error { clones++; return failure }, available: func(string) (uint64, error) {
				spaceChecks++
				if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("fallback wrote before checking space: %v", err)
				}
				return 1 << 40, nil
			}}
			if err := copyConversionInventoryFile(t.Context(), root, "source", target, false, ops); err != nil {
				t.Fatal(err)
			}
			if clones != 1 || spaceChecks != 1 {
				t.Fatalf("clone fallback not used: clones=%d space checks=%d", clones, spaceChecks)
			}
			got, err := os.ReadFile(target)
			if err != nil || string(got) != string(payload) {
				t.Fatalf("fallback bytes differ: %v", err)
			}
			copied, err := os.Stat(target)
			if err != nil || copied.Mode().Perm() != 0600 {
				t.Fatalf("fallback is not private: %v", err)
			}
			after, err := os.Stat(source)
			if err != nil || !original.ModTime().Equal(after.ModTime()) || original.Mode() != after.Mode() {
				t.Fatalf("fallback changed original: %v", err)
			}
		})
	}
}

func TestSetupConvertInventoryNoSpaceRestores(t *testing.T) {
	for _, available := range []uint64{0, 1024 * 1024} {
		t.Run(fmt.Sprint(available), func(t *testing.T) {
			f := newConversionFixture(t)
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			queries := 0
			f.host.upgrade.command = func(context.Context, string, []string, string, ...string) ([]byte, error) {
				queries++
				return []byte(`{"version":1,"blockers":[]}`), nil
			}
			f.host.conversionCopy = &plainConversionCopyOps{clone: func(context.Context, *os.File, string) error { return syscall.ENOTSUP }, available: func(string) (uint64, error) { return available, nil }}
			_, err := runSetup(t.Context(), f.host, f.request(t))
			if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "not enough free space") || !strings.Contains(err.Error(), "margin") {
				t.Fatalf("low-space conversion not refused clearly with exit 5: %v", err)
			}
			if queries != 0 {
				t.Fatal("inventory queried without a safe copy")
			}
			f.assertPlain(t, before)
		})
	}
}

func TestSetupConvertInventoryCopyByteVerification(t *testing.T) {
	for _, payload := range []string{"wrong size", "corrupt-original"} {
		t.Run(payload, func(t *testing.T) {
			directory := t.TempDir()
			target := filepath.Join(directory, "target")
			if err := os.WriteFile(filepath.Join(directory, "source"), []byte("correct-original"), 0600); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			ops := &plainConversionCopyOps{clone: func(_ context.Context, _ *os.File, to string) error { return os.WriteFile(to, []byte(payload), 0600) }, available: func(string) (uint64, error) { t.Fatal("corrupt clone fell back to a byte copy"); return 0, nil }}
			if err := copyConversionInventoryFile(t.Context(), root, "source", target, false, ops); err == nil || !strings.Contains(err.Error(), "copy verification") {
				t.Fatalf("corrupt clone accepted: %v", err)
			}
		})
	}
}

func TestSetupConvertInventoryCopyAndQueryBudgets(t *testing.T) {
	f := newConversionFixture(t)
	clones, queries := 0, 0
	f.host.conversionCopy = &plainConversionCopyOps{clone: func(ctx context.Context, _ *os.File, _ string) error {
		clones++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) < 29*time.Second {
			t.Fatalf("copy still uses query budget: deadline=%v", deadline)
		}
		return syscall.EXDEV
	}, available: func(string) (uint64, error) { return 1 << 40, nil }}
	f.host.upgrade.command = func(ctx context.Context, _ string, _ []string, _ string, _ ...string) ([]byte, error) {
		queries++
		deadline, ok := ctx.Deadline()
		remaining := time.Until(deadline)
		if !ok || remaining > 5*time.Second || remaining < 4*time.Second {
			t.Fatalf("query did not get a fresh five-second budget: %s", remaining)
		}
		return []byte(`{"version":1,"blockers":[]}`), nil
	}
	if _, err := runSetup(t.Context(), f.host, f.request(t)); err != nil {
		t.Fatal(err)
	}
	if clones == 0 || queries != 1 {
		t.Fatalf("copy/query seam not exercised: clones=%d queries=%d", clones, queries)
	}
}

func TestSetupConvertInventoryCloneErrorDoesNotFallback(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "source"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	denied := errors.New("clone permission denied")
	ops := &plainConversionCopyOps{clone: func(context.Context, *os.File, string) error { return denied }, available: func(string) (uint64, error) { t.Fatal("unexpected fallback after clone error"); return 0, nil }}
	if err := copyConversionInventoryFile(t.Context(), root, "source", filepath.Join(directory, "target"), false, ops); !errors.Is(err, denied) {
		t.Fatalf("clone error ignored: %v", err)
	}
}

func TestSetupConvertInventoryFallbackRealSpaceCheck(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(source, []byte("byte fallback on the destination volume"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ops := &plainConversionCopyOps{clone: func(context.Context, *os.File, string) error { return syscall.ENOTSUP }}
	if err := copyConversionInventoryFile(t.Context(), root, "source", target, false, ops); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "byte fallback on the destination volume" {
		t.Fatalf("real free-space fallback failed: %v", err)
	}
}

func TestSetupConvertInventoryCloneIdentityRefused(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(source, []byte("preserve original mode"), 0640); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ops := &plainConversionCopyOps{clone: func(context.Context, *os.File, string) error { return os.Link(source, target) }}
	if err := copyConversionInventoryFile(t.Context(), root, "source", target, false, ops); err == nil || !strings.Contains(err.Error(), "copy verification") {
		t.Fatalf("clone alias accepted: %v", err)
	}
	info, err := os.Stat(source)
	if err != nil || info.Mode().Perm() != 0640 {
		t.Fatalf("clone alias changed original mode: %v", err)
	}
}

func TestSetupConvertInventoryCopyBudgetScalesWithSize(t *testing.T) {
	directory := t.TempDir()
	source, err := os.OpenFile(filepath.Join(directory, "l1.sqlite"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Truncate(1024 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "l3.sqlite"), []byte("ledger"), 0600); err != nil {
		t.Fatal(err)
	}
	observed := false
	stop := errors.New("copy deadline observed before reading sparse file")
	host := setupHost{conversionCopy: &plainConversionCopyOps{clone: func(ctx context.Context, _ *os.File, _ string) error {
		observed = true
		deadline, ok := ctx.Deadline()
		// A 1 GiB inventory receives about 91 seconds, rather than the floor of 30
		// or the query budget of 5. No sleep or large read is needed for this check.
		if !ok || time.Until(deadline) < 85*time.Second {
			t.Fatalf("copy budget did not scale with logical size: %v", deadline)
		}
		return stop
	}}}
	paths := setupPaths{home: directory, config: directory, data: directory, dataRel: "."}
	err = checkQuiescentPlainConversionWork(t.Context(), host, setupRequest{}, paths, &plainConversion{Version: 1, NodeID: "node", Phase: "stop"})
	if !observed || !errors.Is(err, stop) {
		t.Fatalf("scaled copy budget not observed: %v", err)
	}
}

func TestSetupConvertInventoryCloneSymlinkRefused(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	unrelated := filepath.Join(directory, "unrelated")
	if err := os.WriteFile(filepath.Join(directory, "source"), []byte("same bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unrelated, []byte("same bytes"), 0640); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ops := &plainConversionCopyOps{clone: func(context.Context, *os.File, string) error { return os.Symlink(unrelated, target) }}
	if err := copyConversionInventoryFile(t.Context(), root, "source", target, false, ops); err == nil || !strings.Contains(err.Error(), "copy verification") {
		t.Fatalf("clone symlink accepted: %v", err)
	}
	info, err := os.Stat(unrelated)
	if err != nil || info.Mode().Perm() != 0640 {
		t.Fatalf("clone symlink changed unrelated file mode: %v", err)
	}
}
