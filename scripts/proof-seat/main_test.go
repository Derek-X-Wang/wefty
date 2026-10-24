package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProofRootRefusesRedirectedOrUnownedInstall(t *testing.T) {
	home := t.TempDir()
	if _, err := proofRoot(home); err == nil {
		t.Fatal("accepted a home without a proof installation")
	}
	root := filepath.Join(home, ".local", "share", "wefty-proof")
	if err := os.MkdirAll(filepath.Dir(root), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), root); err != nil {
		t.Fatal(err)
	}
	if _, err := proofRoot(home); err == nil {
		t.Fatal("accepted redirected proof state")
	}
}
