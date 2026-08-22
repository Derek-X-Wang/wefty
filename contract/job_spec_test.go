package contract

import (
	"errors"
	"testing"
)

func TestValidateJobSpecReturnsTypedRuntimeHandlerError(t *testing.T) {
	t.Parallel()

	spec := validProcessJobSpecForValidation()
	spec.RuntimeHandler = "io.containerd.runc.v2"
	err := ValidateJobSpec(spec)
	var coded interface{ Code() ErrorCode }
	if !errors.As(err, &coded) {
		t.Fatalf("ValidateJobSpec() error = %T %v, want typed contract error", err, err)
	}
	if coded.Code() != ErrorUnsupportedRuntimeHandler {
		t.Fatalf("ValidateJobSpec() code = %q, want %q", coded.Code(), ErrorUnsupportedRuntimeHandler)
	}
}

func TestValidateProcessEnvironmentNames(t *testing.T) {
	t.Parallel()

	spec := validProcessJobSpecForValidation()
	spec.Execution.Env = map[string]string{"INVALID-NAME": "value"}
	if err := ValidateJobSpec(spec); err == nil {
		t.Fatal("ValidateJobSpec() accepted an invalid process environment name")
	}
}

func TestOCIImageReferenceDistributionGrammar(t *testing.T) {
	t.Parallel()

	for _, reference := range []string{
		"alpine",
		"alpine:latest",
		"ghcr.io/example/tool:v1",
		"registry.example.com:5000/team/foo__bar:Tag_1",
		"team/foo--bar",
	} {
		if !ociImageReferenceRE.MatchString(reference) {
			t.Errorf("valid distribution reference %q was rejected", reference)
		}
	}
	for _, reference := range []string{":", "/", "Alpine:LATEST", "alpine@sha256:abc", "team//tool"} {
		if ociImageReferenceRE.MatchString(reference) {
			t.Errorf("invalid distribution reference %q was accepted", reference)
		}
	}
}

func validProcessJobSpecForValidation() JobSpec {
	return JobSpec{
		SchemaVersion: SchemaVersionV1,
		DispatchKey:   "process:validation",
		Kind:          JobKindProcess,
		Class:         JobClassOneShot,
		Execution: ExecutionSpec{
			Executable:       ExecutableSpec{Path: "/bin/true"},
			Argv:             []string{"true"},
			WorkingDirectory: "/tmp",
			HandoffDirectory: "/tmp/out",
		},
	}
}
