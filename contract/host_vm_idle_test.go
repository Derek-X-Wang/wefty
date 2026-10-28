package contract

import "testing"

func TestHostVMIdleIsAClosedOCIRestriction(t *testing.T) {
	reason := CapabilityReasonCode("host_vm_idle")
	if !reason.Valid() || !reason.ValidOCIRestriction() {
		t.Fatalf("host_vm_idle must be a closed OCI restriction")
	}
}
