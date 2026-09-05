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
	future := CapabilityReasonCode("future_non_oci_reason")
	original := capabilityReasonCodes
	capabilityReasonCodes = append(append([]CapabilityReasonCode(nil), capabilityReasonCodes...), future)
	t.Cleanup(func() { capabilityReasonCodes = original })
	if !future.Valid() {
		t.Fatal("mutation control did not extend the general wire vocabulary")
	}
	if future.ValidOCIRestriction() {
		t.Fatal("unknown future reason was accepted as OCI restriction authority")
	}
}
