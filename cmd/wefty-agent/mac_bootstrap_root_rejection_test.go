package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMacBootstrapRejectsRootBeforeAnyWrite guards #396's D5: running the
// private __wefty_mac_bootstrap mode as root must be refused before it
// writes anything, not after. Writing the durable intent file first and
// rejecting root second leaves a root-owned file that permanently blocks
// every later non-root operator run until a human removes it by hand.
func TestMacBootstrapRejectsRootBeforeAnyWrite(t *testing.T) {
	originalEUID := currentEUID
	currentEUID = func() int { return 0 }
	defer func() { currentEUID = originalEUID }()

	tempDir := t.TempDir()
	intentPath := filepath.Join(tempDir, "wefty-oci-intent.json")

	// Root must be rejected even with an otherwise-plausible, fully-formed
	// argument list -- the check must precede flag-driven validation and
	// every subsequent write, not merely a missing-flag short circuit.
	arguments := []string{
		"-operator-user=derekxwang",
		"-operator-home=" + tempDir,
		"-lima-home=" + tempDir,
		"-working-directory=" + tempDir,
		"-agent-path=" + filepath.Join(tempDir, "wefty-agent"),
		"-linux-helper=" + filepath.Join(tempDir, "wefty-agent-linux"),
		"-helper-checksum=deadbeef",
		"-guest-user=wefty",
		"-guest-uid=501",
		"-node-id=node-1",
		"-host-mount-root=" + tempDir,
		"-intent-file=" + intentPath,
	}
	err := runMacBootstrap(arguments)
	if err == nil {
		t.Fatal("root invocation returned no error")
	}
	if !strings.Contains(err.Error(), "root") {
		t.Fatalf("error = %q, want it to explain the root rejection", err.Error())
	}

	if _, statErr := os.Stat(intentPath); !os.IsNotExist(statErr) {
		t.Fatalf("refused root bootstrap left behind an intent file: stat err=%v", statErr)
	}
	entries, readErr := os.ReadDir(tempDir)
	if readErr != nil {
		t.Fatalf("read temp dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("refused root bootstrap wrote files: %v", entries)
	}
}

// TestMacBootstrapNonRootProceedsPastIdentityCheck confirms the new root
// rejection targets only euid 0: a non-root invocation must reach ordinary
// flag validation (and fail there, since this fixture supplies no real Lima
// or operator environment) rather than the root-rejection error.
func TestMacBootstrapNonRootProceedsPastIdentityCheck(t *testing.T) {
	originalEUID := currentEUID
	currentEUID = func() int { return 501 }
	defer func() { currentEUID = originalEUID }()

	err := runMacBootstrap(nil)
	if err == nil {
		t.Fatal("empty argument invocation returned no error")
	}
	if strings.Contains(err.Error(), "root") {
		t.Fatalf("non-root invocation was rejected as root: %v", err)
	}
}
