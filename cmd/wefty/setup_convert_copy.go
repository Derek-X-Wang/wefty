package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// These operations belong only to conversion; other setup/upgrade paths are
// unchanged. The source descriptor was opened through the home-confined root.
type plainConversionCopyOps struct {
	clone     func(context.Context, *os.File, string) error
	available func(string) (uint64, error)
}

func plainConversionInventorySize(root *os.Root, paths setupPaths) (int64, error) {
	var size int64
	for _, db := range []string{"l1.sqlite", "l3.sqlite"} {
		for _, suffix := range []string{"", "-wal", "-journal"} {
			info, err := root.Lstat(filepath.Join(paths.dataRel, db+suffix))
			if suffix != "" && errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return 0, err
			}
			if !info.Mode().IsRegular() || !conversionSingleLink(info) {
				return 0, errors.New("plain inventory source must be a regular unlinked file")
			}
			if info.Size() > math.MaxInt64-size {
				return 0, errors.New("plain inventory size exceeds copy budget range")
			}
			size += info.Size()
		}
	}
	return size, nil
}

// Allow three sequential read passes at 50 MiB/s: fallback copy, source hash,
// copy hash. Cloning saves the copy pass; verification still reads the bytes.
func plainConversionCopyBudget(size int64) time.Duration {
	const throughput = 50 * 1024 * 1024
	// Division before multiplication avoids overflow for very large sparse files.
	seconds := size/throughput*3 + 30
	fractional := time.Duration(size%throughput) * 3 * time.Second / throughput
	if seconds >= math.MaxInt64/int64(time.Second)-1 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds)*time.Second + fractional
}

func copyConversionInventoryFile(ctx context.Context, root *os.Root, from, to string, optional bool, overrides ...*plainConversionCopyOps) error {
	info, err := root.Lstat(from)
	if optional && errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || !conversionSingleLink(info) {
		return errors.New("plain inventory source must be a regular unlinked file")
	}
	source, err := root.Open(from)
	if err != nil {
		return err
	}
	defer source.Close()
	opened, err := source.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, opened) || !opened.Mode().IsRegular() || !conversionSingleLink(opened) {
		return errors.New("plain inventory source changed while opening")
	}
	ops := plainConversionCopyOps{clone: cloneConversionInventoryFile, available: conversionInventoryAvailableBytes}
	if len(overrides) > 0 && overrides[0] != nil {
		if overrides[0].clone != nil {
			ops.clone = overrides[0].clone
		}
		if overrides[0].available != nil {
			ops.available = overrides[0].available
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ops.clone(ctx, source, to); err != nil {
		if !errors.Is(err, syscall.ENOTSUP) && !errors.Is(err, syscall.EXDEV) && !errors.Is(err, syscall.ENOSYS) {
			return fmt.Errorf("clone conversion inventory: %w", err)
		}
		available, err := ops.available(filepath.Dir(to))
		if err != nil {
			return fmt.Errorf("conversion inventory free space is unverified: %w", err)
		}
		required := uint64(opened.Size())
		margin := max(uint64(64*1024*1024), required/10)
		if required > math.MaxUint64-margin || available < required+margin {
			return fmt.Errorf("not enough free space for conversion inventory copy: need %d-byte copy plus %d-byte margin, available %d bytes; free space on the temporary-file volume and retry", required, margin, available)
		}
		target, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, copyErr := copyConversionInventoryStream(ctx, target, source)
		if err := errors.Join(copyErr, target.Close()); err != nil {
			return err
		}
	}
	linked, err := os.Lstat(to)
	if err != nil {
		return err
	}
	if !linked.Mode().IsRegular() || !conversionSingleLink(linked) || os.SameFile(opened, linked) {
		return errors.New("conversion inventory copy verification failed: copy must be a separate regular unlinked file")
	}
	target, err := os.Open(to)
	if err != nil {
		return err
	}
	defer target.Close()
	copied, err := target.Stat()
	if err != nil {
		return err
	}
	// Check identity before chmod: a faulty clone must never chmod the original.
	if !os.SameFile(linked, copied) || !copied.Mode().IsRegular() || !conversionSingleLink(copied) || os.SameFile(opened, copied) {
		return errors.New("conversion inventory copy verification failed: copy must be a separate regular unlinked file")
	}
	if err := target.Chmod(0600); err != nil {
		return err
	}
	if copied.Size() != opened.Size() {
		return errors.New("conversion inventory copy verification failed: size differs")
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	originalHash, copiedHash := sha256.New(), sha256.New()
	n, err := copyConversionInventoryStream(ctx, originalHash, source)
	if err != nil {
		return err
	}
	m, err := copyConversionInventoryStream(ctx, copiedHash, target)
	if err != nil {
		return err
	}
	after, err := source.Stat()
	if err != nil {
		return err
	}
	if n != opened.Size() || m != opened.Size() || string(originalHash.Sum(nil)) != string(copiedHash.Sum(nil)) || after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return errors.New("conversion inventory copy verification failed: bytes or original metadata differ")
	}
	return ctx.Err()
}

func copyConversionInventoryStream(ctx context.Context, to io.Writer, from io.Reader) (int64, error) {
	buffer := make([]byte, 128*1024)
	var copied int64
	for {
		if err := ctx.Err(); err != nil {
			return copied, err
		}
		n, readErr := from.Read(buffer)
		if n > 0 {
			written, err := to.Write(buffer[:n])
			copied += int64(written)
			if err != nil {
				return copied, err
			}
			if written != n {
				return copied, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return copied, nil
		}
		if readErr != nil {
			return copied, readErr
		}
	}
}
