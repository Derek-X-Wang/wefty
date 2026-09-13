package ocihelper

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeFullAllocationForTest(path string, bytes int64) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > bytes {
		return errors.New("Computer disk image would shrink")
	}
	if _, err := file.WriteAt(make([]byte, bytes-info.Size()), info.Size()); err != nil {
		return err
	}
	return file.Sync()
}

// extendWithoutAllocation stands for a grow that only lengthens the file — the
// trace a defective grow must never be allowed to report as success.
func extendWithoutAllocation(path string, bytes int64) error {
	return os.Truncate(path, bytes)
}

func TestComputerGrowExtentRefusesSparseExtension(t *testing.T) {
	oldBytes := int64(8 << 20)
	newBytes := int64(12 << 20)
	imagePath := filepath.Join(t.TempDir(), "disk.ext4")
	if err := writeFullAllocationForTest(imagePath, oldBytes); err != nil {
		t.Fatal(err)
	}
	growErr := growComputerDiskExtent(imagePath, oldBytes, newBytes, extendWithoutAllocation)
	var preallocate *ComputerStorageGrowPreallocationError
	if !errors.As(growErr, &preallocate) {
		t.Fatalf("sparse grow was not refused: err=%v; admission check on the grown image = %v",
			growErr, verifyComputerDiskAllocation(imagePath, newBytes))
	}
	if _, statErr := os.Stat(imagePath); statErr != nil {
		t.Fatal(statErr)
	} else if info, _ := os.Stat(imagePath); info.Size() != oldBytes {
		t.Fatalf("refused grow left size %d, want old %d", info.Size(), oldBytes)
	}
}

func TestComputerGrowExtentGrownImagePassesAdmissionAllocation(t *testing.T) {
	oldBytes := int64(8 << 20)
	newBytes := int64(12 << 20)
	imagePath := filepath.Join(t.TempDir(), "disk.ext4")
	if err := writeFullAllocationForTest(imagePath, oldBytes); err != nil {
		t.Fatal(err)
	}
	if err := growComputerDiskExtent(imagePath, oldBytes, newBytes, writeFullAllocationForTest); err != nil {
		t.Fatal(err)
	}
	if err := verifyComputerDiskAllocation(imagePath, newBytes); err != nil {
		t.Fatalf("grown image fails the admission allocation check: %v", err)
	}
}

func TestComputerGrowExtentPreallocationFailureKeepsOldSizeAndReason(t *testing.T) {
	oldBytes := int64(8 << 20)
	newBytes := int64(12 << 20)
	imagePath := filepath.Join(t.TempDir(), "disk.ext4")
	if err := writeFullAllocationForTest(imagePath, oldBytes); err != nil {
		t.Fatal(err)
	}
	space := errors.New("simulated ENOSPC while extending the image")
	if err := growComputerDiskExtent(imagePath, oldBytes, newBytes,
		func(string, int64) error { return space }); !errors.Is(err, space) {
		t.Fatalf("preallocation failure = %v, want the typed cause wrapped", err)
	}
	if info, statErr := os.Stat(imagePath); statErr != nil || info.Size() != oldBytes {
		t.Fatalf("failed grow left size %v err=%v, want old %d", info, statErr, oldBytes)
	}
}
