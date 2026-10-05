package contract

import "testing"

func TestOCIExecutionHandoffUsesServerJobOwner(t *testing.T) {
	spec := JobSpec{Kind: JobKindOCI, Class: JobClassOneShot}
	if err := ValidateHandoffOwner(spec); err != nil {
		t.Fatalf("direct OCI submission refused: %v", err)
	}
	if got := ExecutionHandoffOwnerKey(spec, "job_direct"); got != "job_direct" {
		t.Fatalf("execution owner = %q, want server job ID", got)
	}
	if HandoffOwnerKey(spec) != "" || len(RunIdentityLabels(spec)) != 0 {
		t.Fatal("job ownership created run entitlement")
	}
	for _, labels := range []map[string]string{
		{LabelRunID: "run_current"},
		{LabelRunID: "run_current", LabelHandoffOwnerRunID: "run_source"},
	} {
		spec.Labels = labels
		if got := ExecutionHandoffOwnerKey(spec, "job_direct"); got != HandoffOwnerKey(spec) {
			t.Fatalf("run ownership changed: %q", got)
		}
	}
}
