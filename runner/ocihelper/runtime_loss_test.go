package ocihelper

import "testing"

// TestPerResourceDeletionRefusalsAreNotRuntimeLoss pins the narrow set of
// engine refusals that are scoped to the one durable resource they name.
// Deleting a Backup copy is authorized on its own, deletes only that copy, and
// is accepted by the caller only against an independently verified
// positive-absence receipt -- so a refusal there says nothing about whether the
// exclusive helper session still holds namespace authority.
//
// Reading it as node-wide loss is what turned one immutable Backup copy into a
// node that dropped kind:oci and could place nothing, while the removal holding
// that copy had already been declared stalled and had released its Slot (#513).
func TestPerResourceDeletionRefusalsAreNotRuntimeLoss(t *testing.T) {
	scoped := []Method{MethodDeleteVolume, MethodDeleteBackup}
	for _, operation := range scoped {
		refusal := &RPCError{
			Code: CodeEngineFailure, Message: "OCI engine operation failed",
			EngineFailure: &EngineFailureFact{Operation: operation, Reason: EngineFailurePermissionDenied},
		}
		if rpcErrorProvesRuntimeLoss(refusal) {
			t.Fatalf("%s refusal would invalidate the exclusive helper session: %+v", operation, refusal)
		}
	}

	// The default stands: an engine failure that names no independently
	// verified per-resource operation is still runtime-loss evidence.
	wholeNamespace := &RPCError{
		Code: CodeEngineFailure, Message: "OCI engine operation failed",
		EngineFailure: &EngineFailureFact{Operation: MethodSweep, Reason: EngineFailureOperationFailed},
	}
	if !rpcErrorProvesRuntimeLoss(wholeNamespace) {
		t.Fatalf("a swept-namespace engine failure stopped proving runtime loss: %+v", wholeNamespace)
	}
}
