package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestPrestartRetryDelayAppliesJitterWithinThirtySecondCap(t *testing.T) {
	if got := prestartRetryDelay(1, func(delay time.Duration) time.Duration { return delay * 8 / 10 }); got != 800*time.Millisecond {
		t.Fatalf("first pre-start retry with 80%% jitter = %s, want 800ms", got)
	}
	if got := prestartRetryDelay(2, func(delay time.Duration) time.Duration { return delay * 12 / 10 }); got != 2400*time.Millisecond {
		t.Fatalf("second pre-start retry with 120%% jitter = %s, want 2.4s", got)
	}
	if got := prestartRetryDelay(6, func(delay time.Duration) time.Duration { return delay * 12 / 10 }); got != 30*time.Second {
		t.Fatalf("capped pre-start retry = %s, want 30s", got)
	}
}

func TestSpawnFailureClassificationDefaultsTerminal(t *testing.T) {
	if !IsRestartableSpawnFailure(contract.SpawnFailureStartupReadinessTimeout) {
		t.Fatal("startup readiness timeout must be restartable")
	}
	if got := classifySpawnFailure(contract.SpawnFailurePublishedListener); got != failureInfrastructure {
		t.Fatalf("published listener classification = %d, want infrastructure", got)
	}
	for _, code := range []contract.SpawnFailureCode{
		contract.SpawnFailureProcessSpawn,
		contract.SpawnFailurePublishedPortOccupied,
		contract.SpawnFailureImageUnavailable,
		contract.SpawnFailureImageNotFound,
		contract.SpawnFailureImageManifestInvalid,
		contract.SpawnFailureImagePlatformUnsupported,
		contract.SpawnFailureInsufficientMemory,
		contract.SpawnFailureInsufficientDisk,
		contract.SpawnFailureCode("future_unknown_failure"),
	} {
		if got := classifySpawnFailure(code); got != failureTerminal {
			t.Fatalf("spawn failure %q classification = %d, want terminal", code, got)
		}
	}
}

func TestInsufficientResourceFactsFailClosed(t *testing.T) {
	valid := ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureInsufficientMemory, Message: "cap sum exceeded", RequestedBytes: 1 << 30}}
	if err := validateProcessResult(valid); err != nil {
		t.Fatalf("valid insufficient-memory facts rejected: %v", err)
	}
	mutations := []ProcessResult{
		{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureInsufficientMemory, Message: "cap sum exceeded"}},
		{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureInsufficientDisk, Message: "disk full", RequestedBytes: 1, ObservedAvailableBytes: -1}},
		{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureInsufficientMemory, Message: "cap sum exceeded", RequestedBytes: 1, NodeID: "spoofed"}},
		{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureProcessSpawn, Message: "bad", RequestedBytes: 1}},
	}
	for index, mutation := range mutations {
		if err := validateProcessResult(mutation); err == nil {
			t.Fatalf("resource fact mutation %d passed", index)
		}
	}
}

func TestTerminalRuntimeResourceFailureUsesDeclaredComputerCaps(t *testing.T) {
	memoryBytes := int64(1 << 30)
	job := Job{Spec: contract.JobSpec{Kind: contract.JobKindOCI, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
		Limits: &contract.OCILimits{MemoryBytes: &memoryBytes}, Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30},
	}}}}
	exitCode := 1
	for _, test := range []struct {
		name   string
		result ProcessResult
		code   contract.SpawnFailureCode
		bytes  int64
	}{
		{name: "oom", result: ProcessResult{ExitCode: &exitCode, OOM: true}, code: contract.SpawnFailureInsufficientMemory, bytes: 1 << 30},
		{name: "disk", result: ProcessResult{ExitCode: &exitCode, DiskExhausted: true}, code: contract.SpawnFailureInsufficientDisk, bytes: 8 << 30},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := terminalResourceFailure(job, test.result, "node-a")
			if failure == nil || failure.Code != test.code || failure.NodeID != "node-a" || failure.RequestedBytes != test.bytes || failure.ObservedAvailableBytes != 0 {
				t.Fatalf("runtime resource failure = %+v", failure)
			}
		})
	}
	if failure := terminalResourceFailure(Job{}, ProcessResult{ExitCode: &exitCode, OOM: true, DiskExhausted: true}, "node-a"); failure != nil {
		t.Fatalf("non-OCI mutation synthesized resource failure: %+v", failure)
	}
	ordinary := Job{Spec: contract.JobSpec{Kind: contract.JobKindOCI, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
		Limits: &contract.OCILimits{MemoryBytes: &memoryBytes},
	}}}}
	if failure := terminalResourceFailure(ordinary, ProcessResult{ExitCode: &exitCode, OOM: true, DiskExhausted: true}, "node-a"); failure != nil {
		t.Fatalf("ordinary OCI synthesized Computer resource failure: %+v", failure)
	}
}

func TestProcessResultRequiresStructuredSignalCause(t *testing.T) {
	if err := validateProcessResult(ProcessResult{Signal: "terminated"}); err == nil {
		t.Fatal("signal without termination_cause must be rejected")
	}
	if err := validateProcessResult(ProcessResult{
		Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian,
	}); err != nil {
		t.Fatalf("structured signal result rejected: %v", err)
	}
}
