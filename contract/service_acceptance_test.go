//go:build service_acceptance

package contract

import (
	"encoding/json"
	"testing"
)

func TestServiceAcceptanceJobSpecSchemaAndProcessResult(t *testing.T) {
	schema := compileSchemas(t)["job-spec"]
	for _, path := range []string{
		"testdata/schemas/job-spec/valid-service.json",
		"testdata/schemas/job-spec/valid-unknown-class.json",
	} {
		if err := schema.Validate(unmarshalJSONFile(t, path)); err != nil {
			t.Fatalf("%s rejected: %v", path, err)
		}
	}
	for _, path := range []string{
		"testdata/schemas/job-spec/invalid-missing-class.json",
		"testdata/schemas/job-spec/invalid-one-shot-missing-handoff.json",
		"testdata/schemas/job-spec/invalid-service-missing-restart.json",
	} {
		if err := schema.Validate(unmarshalJSONFile(t, path)); err == nil {
			t.Fatalf("%s accepted", path)
		}
	}

	encoded, err := json.Marshal(ProcessResult{
		Signal: "terminated", TerminationCause: TerminationCauseGuardian,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"signal":"terminated","termination_cause":"guardian"}`; got != want {
		t.Fatalf("ProcessResult JSON = %s, want %s", got, want)
	}
}
