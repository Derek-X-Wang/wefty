package main

import (
	"fmt"
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

func TestProofRootAcceptsRecordedDialogChoice(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(home, ".local", "share", "wefty-proof")
	for _, suffix := range []string{"config", "data/lima"} {
		if err := os.MkdirAll(filepath.Join(root, suffix), 0700); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]string{
		"data/l1.sqlite":         "fixture",
		"config/oci-user.json":   fmt.Sprintf(`{"instance":"wefty-proof","lima_home":%q}`, filepath.Join(root, "data", "lima")),
		"data/proof-dialog.json": `{"approved":true,"choice":"approve","pid":123,"at":"2026-10-23T12:00:00Z"}`,
	}
	for suffix, payload := range files {
		if err := os.WriteFile(filepath.Join(root, suffix), []byte(payload), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := proofRoot(home); err != nil || got != root {
		t.Fatalf("proof root=%s err=%v", got, err)
	}
}
