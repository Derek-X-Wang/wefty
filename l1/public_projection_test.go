package l1

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// redactJob is the single gate every public job read and listing passes
// through. The run-params label is agent-only transport for the run mailbox,
// so it belongs on the claim path and nowhere a client can read.
func TestPublicJobProjectionOmitsAgentOnlyLabels(t *testing.T) {
	labels := map[string]string{
		"run_id":                "run_public",
		contract.LabelRunParams: `{"ref":"main"}`,
		"handoff_owner_run_id":  "run_public",
	}
	job := Job{Spec: contract.JobSpec{
		Labels: labels,
		Execution: contract.ExecutionSpec{
			Env:          map[string]string{contract.EnvRunID: "run_public"},
			SensitiveEnv: map[string]string{contract.EnvRunToken: "wrun_secret"},
		},
	}}

	redacted := redactJob(job)
	if _, present := redacted.Spec.Labels[contract.LabelRunParams]; present {
		t.Fatalf("public job projection labels = %v", redacted.Spec.Labels)
	}
	if redacted.Spec.Labels["run_id"] != "run_public" || redacted.Spec.Labels["handoff_owner_run_id"] != "run_public" {
		t.Fatalf("redaction removed ordinary labels: %v", redacted.Spec.Labels)
	}
	if redacted.Spec.Execution.SensitiveEnv != nil {
		t.Fatalf("public job projection still carries sensitive environment values")
	}
	// The stored job is shared with the claim path, which must keep the label.
	if labels[contract.LabelRunParams] != `{"ref":"main"}` {
		t.Fatalf("redaction mutated the stored labels: %v", labels)
	}
}
