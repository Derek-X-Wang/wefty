//go:build service_acceptance && (darwin || linux)

package managedroot

import "testing"

// This tagged test intentionally runs the complete destructive-deletion matrix
// against synthetic roots. The library is merge-gated here before any agent
// code is allowed to point it at the owner's managed state root.
func TestServiceAcceptanceDestructiveDeletionGuardrails(t *testing.T) {
	runDeletionGuardrailMatrix(t)
}
