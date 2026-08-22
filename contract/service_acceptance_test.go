//go:build service_acceptance

package contract

import (
	"encoding/json"
	"testing"
)

func TestServiceAcceptanceJobSpecSchemaAndProcessResult(t *testing.T) {
	schema := compileSchemas(t)["job-spec"]
	for _, path := range []string{
		"testdata/schemas/job-spec/valid-oci-one-shot.json",
		"testdata/schemas/job-spec/valid-oci-reserved-environment-names.json",
		"testdata/schemas/job-spec/valid-oci-service.json",
		"testdata/schemas/job-spec/valid-service.json",
		"testdata/schemas/job-spec/valid-unknown-class.json",
	} {
		if err := schema.Validate(unmarshalJSONFile(t, path)); err != nil {
			t.Fatalf("%s rejected: %v", path, err)
		}
		var spec JobSpec
		raw, err := contractFiles.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &spec); err != nil {
			t.Fatal(err)
		}
		if err := ValidateJobSpec(spec); err != nil {
			t.Fatalf("%s rejected by Go validation: %v", path, err)
		}
	}
	for _, path := range []string{
		"testdata/schemas/job-spec/invalid-missing-class.json",
		"testdata/schemas/job-spec/invalid-oci-image-reference-uppercase-repository.json",
		"testdata/schemas/job-spec/invalid-oci-missing-arm.json",
		"testdata/schemas/job-spec/invalid-oci-mount-handoff-target.json",
		"testdata/schemas/job-spec/invalid-oci-mount-missing-node-tag.json",
		"testdata/schemas/job-spec/invalid-oci-mount-service-target.json",
		"testdata/schemas/job-spec/invalid-oci-mount-two-node-tags.json",
		"testdata/schemas/job-spec/invalid-oci-mount-wrong-node-tag-prefix.json",
		"testdata/schemas/job-spec/invalid-oci-nonnormal-node-path.json",
		"testdata/schemas/job-spec/invalid-oci-root-node-path.json",
		"testdata/schemas/job-spec/invalid-oci-service-missing-digest.json",
		"testdata/schemas/job-spec/invalid-oci-unknown-class-missing-digest.json",
		"testdata/schemas/job-spec/invalid-oci-with-executable.json",
		"testdata/schemas/job-spec/invalid-one-shot-missing-handoff.json",
		"testdata/schemas/job-spec/invalid-process-environment-name.json",
		"testdata/schemas/job-spec/invalid-process-with-oci.json",
		"testdata/schemas/job-spec/invalid-service-missing-restart.json",
	} {
		if err := schema.Validate(unmarshalJSONFile(t, path)); err == nil {
			t.Fatalf("%s accepted", path)
		}
		var spec JobSpec
		raw, err := contractFiles.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &spec); err != nil {
			t.Fatal(err)
		}
		if err := ValidateJobSpec(spec); err == nil {
			t.Fatalf("%s accepted by Go validation", path)
		}
	}
}
