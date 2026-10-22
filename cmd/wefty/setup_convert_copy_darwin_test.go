//go:build darwin

package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSetupConvertInventoryLargeClone(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "large.sqlite")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}
	const size = 320 * 1024 * 1024
	if err := file.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, size / 2, size - 4096} {
		if _, err := file.WriteAt([]byte("conversion-clone-evidence"), offset); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	originalHash := conversionTestFileHash(t, path)
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	target := filepath.Join(directory, "copy.sqlite")
	cloneCalls := 0
	var cloneElapsed time.Duration
	ops := &plainConversionCopyOps{clone: func(ctx context.Context, source *os.File, to string) error {
		cloneCalls++
		start := time.Now()
		err := cloneConversionInventoryFile(ctx, source, to)
		cloneElapsed = time.Since(start)
		return err
	}, available: func(string) (uint64, error) {
		t.Fatal("native large-file clone fell back to a byte copy")
		return 0, nil
	}}
	started := time.Now()
	if err := copyConversionInventoryFile(context.Background(), root, "large.sqlite", target, false, ops); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if cloneCalls != 1 {
		t.Fatalf("native clone was not called exactly once: %d", cloneCalls)
	}
	copied, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	var sourceStat, targetStat unix.Stat_t
	if err := unix.Stat(path, &sourceStat); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(target, &targetStat); err != nil {
		t.Fatal(err)
	}
	if targetStat.Blocks > sourceStat.Blocks+2048 {
		t.Fatalf("large inventory used a full byte copy, not an APFS clone: source blocks=%d target blocks=%d", sourceStat.Blocks, targetStat.Blocks)
	}
	if copied.Size() != size || copied.Mode().Perm() != 0600 || os.SameFile(before, copied) || conversionTestFileHash(t, target) != originalHash {
		t.Fatal("clone size, privacy, inode or bytes differ")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) || after.Mode() != before.Mode() || conversionTestFileHash(t, path) != originalHash {
		t.Fatal("clone changed original")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("320 MiB clone and verification too slow: %s", elapsed)
	}
	t.Logf("320 MiB sparse inventory preparation with byte verification: %s; native clone: %s; source/target allocated bytes: %d/%d", elapsed, cloneElapsed, sourceStat.Blocks*512, targetStat.Blocks*512)

	// Corrupt a byte in the middle of another real native clone. This proves
	// preparation checks the payload, including bytes far beyond its header.
	ops.clone = func(ctx context.Context, source *os.File, to string) error {
		if err := cloneConversionInventoryFile(ctx, source, to); err != nil {
			return err
		}
		f, err := os.OpenFile(to, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, writeErr := f.WriteAt([]byte("corrupt clone"), size/2+123)
		return errors.Join(writeErr, f.Close())
	}
	if err := copyConversionInventoryFile(t.Context(), root, "large.sqlite", filepath.Join(directory, "corrupt.sqlite"), false, ops); err == nil || !strings.Contains(err.Error(), "copy verification") {
		t.Fatalf("middle-of-file native clone corruption was accepted: %v", err)
	}
	if conversionTestFileHash(t, path) != originalHash {
		t.Fatal("native clone corruption changed the original")
	}
}

func conversionTestFileHash(t *testing.T, path string) [32]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}
