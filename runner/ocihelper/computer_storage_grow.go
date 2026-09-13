package ocihelper

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// ComputerStorageGrowPreallocationError means the extended Computer disk image
// could not be fully allocated and was rolled back to its old size, so the
// same grow authority stays retryable. An ENOSPC cause keeps unwrapping so the
// existing insufficient-disk vocabulary still classifies it.
type ComputerStorageGrowPreallocationError struct{ Cause error }

func (err *ComputerStorageGrowPreallocationError) Error() string {
	return "Computer Storage grow could not fully allocate the extended disk image: " + err.Cause.Error()
}

func (err *ComputerStorageGrowPreallocationError) Unwrap() error { return err.Cause }

func verifyComputerDiskAllocation(path string, bytes int64) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != bytes {
		return fmt.Errorf("Computer disk allocation does not match its %d-byte budget", bytes)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Blocks*512 < bytes {
		return errors.New("Computer disk image is not fully allocated")
	}
	return nil
}

// growComputerDiskExtent extends the Computer disk image from oldBytes to
// newBytes by preallocating the new extent the same way create does — the
// caller's allocate is exactly the create-path full-allocation helper — and
// then verifying the fully-allocated invariant with the same check the attempt
// admission uses. An allocation or verification failure rolls the image back
// to oldBytes and returns a typed reason, so a grow never reports success for
// a sparse image and the published generation keeps its old size.
func growComputerDiskExtent(imagePath string, oldBytes, newBytes int64, allocate func(path string, bytes int64) error) error {
	info, err := os.Lstat(imagePath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || (info.Size() != oldBytes && info.Size() != newBytes) {
		return errors.New("Computer disk image size conflicts with grow authority")
	}
	if info.Size() == oldBytes {
		allocationErr := allocate(imagePath, newBytes)
		if allocationErr == nil {
			allocationErr = verifyComputerDiskAllocation(imagePath, newBytes)
		}
		if allocationErr != nil {
			_ = os.Truncate(imagePath, oldBytes)
			return &ComputerStorageGrowPreallocationError{Cause: allocationErr}
		}
	}
	return nil
}
