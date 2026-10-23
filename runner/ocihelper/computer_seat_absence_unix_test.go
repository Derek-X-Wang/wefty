//go:build linux || darwin

package ocihelper

import (
	"os"
	"path/filepath"
	"testing"
)

func TestComputerSeatFolderAbsenceRefusesPresentOrLinkedPlace(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "wefty/service/seats")
	place := filepath.Join(parent, "1")
	if err := os.MkdirAll(place, 0700); err != nil {
		t.Fatal(err)
	}
	if err := confirmSeatFolderAbsent(root, 1); err == nil {
		t.Fatal("present folder accepted")
	}
	if err := os.Remove(place); err != nil {
		t.Fatal(err)
	}
	if err := confirmSeatFolderAbsent(root, 1); err != nil {
		t.Fatalf("absent folder refused: %v", err)
	}
	if err := os.Symlink(t.TempDir(), place); err != nil {
		t.Fatal(err)
	}
	if err := confirmSeatFolderAbsent(root, 1); err == nil {
		t.Fatal("linked place accepted as absent")
	}
	if err := os.Remove(place); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(parent); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), parent); err != nil {
		t.Fatal(err)
	}
	if err := confirmSeatFolderAbsent(root, 1); err == nil {
		t.Fatal("linked parent accepted as absence")
	}
}
