package contract

import (
	"slices"
	"testing"
)

func TestOCIRestrictionReasonVocabularyIsExplicitlyPinned(t *testing.T) {
	if !slices.Equal(ociRestrictionCapabilityReasonCodes, capabilityReasonCodes) {
		t.Fatalf("OCI restriction reasons = %v, want current wire vocabulary %v", ociRestrictionCapabilityReasonCodes, capabilityReasonCodes)
	}
	for _, reason := range ociRestrictionCapabilityReasonCodes {
		if !reason.ValidOCIRestriction() {
			t.Fatalf("OCI restriction reason %q is not accepted", reason)
		}
	}
	if CapabilityReasonCode("future_non_oci_reason").ValidOCIRestriction() {
		t.Fatal("unknown future reason was accepted as OCI restriction authority")
	}
}
