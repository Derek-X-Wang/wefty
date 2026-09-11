//go:build darwin

// This file exercises runMacBootstrap end to end, which bails out on any
// non-darwin GOOS before the root check it is testing ever runs (see
// rootBootstrapRejection in main.go). Constrained to darwin so it is not
// compiled -- let alone silently wrong -- on other platforms; the shared
// rejection logic itself has a platform-neutral test in
// mac_bootstrap_root_rejection_helper_test.go.

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

	// -intent-file is deliberately relative: runMacBootstrap's own flag
	// validation ("--intent-file must be absolute") would fatally reject it
	// too, but only after HelperSocketPath and the facts-path check, well
	// past where the root check must sit. That later, unrelated error keeps
	// this test honest: if the root check were removed, this exact argument
	// list would fail with the flag-validation error instead of the root
	// rejection, and the Contains(err, "root") assertion below would go red
	// -- not merely "no error" or some earlier, incidental failure. This
	// proves the root check runs first, rather than the test happening to
	// stop at the first error the fixture trips regardless of ordering.
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
		"-minimal-doctor-facts=" + filepath.Join(tempDir, "facts.json"),
		"-intent-file=relative/wefty-oci-intent.json",
	}
	err := runMacBootstrap(arguments)
	if err == nil {
		t.Fatal("root invocation returned no error")
	}
	if !strings.Contains(err.Error(), "root") {
		t.Fatalf("error = %q, want the root rejection, not a later flag-validation error", err.Error())
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
