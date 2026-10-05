package contract

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestValidateJobSpecReturnsTypedRuntimeHandlerError(t *testing.T) {
	t.Parallel()

	spec := validProcessJobSpecForValidation()
	spec.RuntimeHandler = "io.containerd.runc.v2"
	err := ValidateJobSpec(&spec)
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
	if err := ValidateJobSpec(&spec); err == nil {
		t.Fatal("ValidateJobSpec() accepted an invalid process environment name")
	}
}

func TestValidateJobSpecNormalizesExecutionIdentifiers(t *testing.T) {
	spec := JobSpec{
		SchemaVersion:  SchemaVersionV1,
		DispatchKey:    "normalize-identifiers",
		Kind:           " OCI ",
		Class:          JobClassOneShot,
		RuntimeHandler: " IO.CONTAINERD.RUNSC.V1 ",
		Execution: ExecutionSpec{OCI: &OCIExecutionSpec{
			Image: OCIImageSpec{Reference: "alpine:latest"},
		}},
	}
	if err := ValidateJobSpec(&spec); err != nil {
		t.Fatal(err)
	}
	if spec.Kind != JobKindOCI || spec.RuntimeHandler != "io.containerd.runsc.v1" {
		t.Fatalf("normalized identifiers = kind %q handler %q", spec.Kind, spec.RuntimeHandler)
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

func TestRequiresPinnedPlacement(t *testing.T) {
	t.Parallel()

	plain := JobSpec{Kind: JobKindOCI, Execution: ExecutionSpec{OCI: &OCIExecutionSpec{}}}
	if RequiresPinnedPlacement(plain) {
		t.Fatal("plain OCI spec unexpectedly requires Pinned placement")
	}
	withMount := plain
	withMount.Execution.OCI = &OCIExecutionSpec{Mounts: []OCIMount{{NodePath: "/srv", ContainerPath: "/srv"}}}
	if !RequiresPinnedPlacement(withMount) {
		t.Fatal("OCI mount did not require Pinned placement")
	}
	computer := plain
	computer.Execution.OCI = &OCIExecutionSpec{Computer: &OCIComputerSpec{}}
	if !RequiresPinnedPlacement(computer) {
		t.Fatal("Computer trait did not require Pinned placement")
	}
	nonOCI := computer
	nonOCI.Kind = JobKindProcess
	if RequiresPinnedPlacement(nonOCI) {
		t.Fatal("non-OCI spec unexpectedly required Pinned placement")
	}
}

func TestPinnedRoutingErrorsDescribeTheTrigger(t *testing.T) {
	t.Parallel()

	mounted := ImageProgram{Mounts: []OCIMount{{NodePath: "/srv", ContainerPath: "/srv"}}}
	if err := ValidatePinnedRouting(mounted, nil); err == nil || !strings.Contains(err.Error(), "OCI jobs with mounts require exactly one stable-node routing tag") {
		t.Fatalf("mounts-only error = %v", err)
	}

	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	memoryBytes := int64(1)
	computer := JobSpec{
		SchemaVersion: SchemaVersionV1,
		DispatchKey:   "oci:computer-routing",
		Kind:          JobKindOCI,
		Class:         JobClassService,
		Restart:       RestartAlways,
		Execution: ExecutionSpec{OCI: &OCIExecutionSpec{
			Image:    OCIImageSpec{Reference: "computer:v1", Digest: &digest},
			Limits:   &OCILimits{MemoryBytes: &memoryBytes},
			Computer: &OCIComputerSpec{Display: OCIComputerDisplaySpec{Protocol: ComputerDisplayProtocolRFBWebSocketV1}, DiskBytes: 1},
		}},
	}
	if err := ValidateJobSpec(&computer); err == nil || !strings.Contains(err.Error(), "Pinned OCI jobs require exactly one stable-node routing tag") {
		t.Fatalf("Computer trait error = %v", err)
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

func TestServiceRestartPolicies(t *testing.T) {
	for _, restart := range []string{"", "always", "on-failure"} {
		t.Run("ordinary/"+restart, func(t *testing.T) {
			spec := JobSpec{SchemaVersion: 1, DispatchKey: "policy", Kind: "process", Class: "service", Restart: restart, Execution: ExecutionSpec{Executable: ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: "/tmp"}}
			if err := ValidateJobSpec(&spec); err != nil {
				t.Fatal(err)
			}
			if restart == "" && spec.Restart != RestartAlways {
				t.Fatalf("omitted restart = %q", spec.Restart)
			}
		})
	}
}

// A Computer is always-only and must say so: an omitted restart is not
// normalized to always for it, and on-failure is refused, so a Computer can
// never end in a policy stop.
func TestComputerRestartMustBeExplicitlyAlways(t *testing.T) {
	raw, err := contractFiles.ReadFile("testdata/schemas/job-spec/valid-oci-computer.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		restart string
		valid   bool
	}{
		{restart: RestartAlways, valid: true},
		{restart: "", valid: false},
		{restart: RestartOnFailure, valid: false},
	} {
		t.Run("restart="+test.restart, func(t *testing.T) {
			var spec JobSpec
			if err := json.Unmarshal(raw, &spec); err != nil {
				t.Fatal(err)
			}
			spec.Restart = test.restart
			err := ValidateJobSpec(&spec)
			if test.valid {
				if err != nil {
					t.Fatalf("explicit always Computer rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "Computer restart must be explicitly") {
				t.Fatalf("Computer restart %q = %v, want the always-only refusal", test.restart, err)
			}
			if spec.Restart != test.restart {
				t.Fatalf("refused Computer restart was rewritten to %q", spec.Restart)
			}
		})
	}
}
