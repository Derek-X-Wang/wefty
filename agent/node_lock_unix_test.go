//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package agent

import (
	"errors"
	"testing"
)

func TestLegacyAgentNodeLockRefusalNamesNode(t *testing.T) {
	for _, generationID := range []string{"", "generation-1"} {
		t.Run("holder-generation="+generationID, func(t *testing.T) {
			directory := t.TempDir()
			lock, err := acquireAgentNodeLock(directory, "dogfood-local", generationID)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lock.Close() })

			duplicate, err := acquireAgentNodeLock(directory, "dogfood-local", "")
			if duplicate != nil {
				_ = duplicate.Close()
				t.Fatal("second legacy agent acquired the Node lock")
			}
			if !errors.Is(err, errSharedLeaseBusy) {
				t.Fatalf("got %v, want busy Node lock", err)
			}
			const want = `agent: stable node "dogfood-local" is already active: node-local ownership is busy`
			if err.Error() != want {
				t.Fatalf("got %q, want %q", err.Error(), want)
			}
		})
	}
}
