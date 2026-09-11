package main

import (
	"strings"
	"testing"
)

// TestRootBootstrapRejectionRejectsOnlyEUIDZero is the platform-neutral
// counterpart to TestMacBootstrapRejectsRootBeforeAnyWrite (darwin-only,
// exercised end to end through runMacBootstrap). runMacBootstrap itself
// bails out on any non-darwin GOOS before rootBootstrapRejection ever runs,
// so this test exercises the shared rejection helper directly to guard
// #396's D5 on every platform CI runs on, not only macOS.
func TestRootBootstrapRejectionRejectsOnlyEUIDZero(t *testing.T) {
	if err := rootBootstrapRejection(0); err == nil || !strings.Contains(err.Error(), "root") {
		t.Fatalf("rootBootstrapRejection(0) = %v, want an error explaining the root rejection", err)
	}
	for _, euid := range []int{1, 501, 65534} {
		if err := rootBootstrapRejection(euid); err != nil {
			t.Fatalf("rootBootstrapRejection(%d) = %v, want nil for a non-root identity", euid, err)
		}
	}
}
