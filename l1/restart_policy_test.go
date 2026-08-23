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
		contract.SpawnFailureCode("future_unknown_failure"),
	} {
		if got := classifySpawnFailure(code); got != failureTerminal {
			t.Fatalf("spawn failure %q classification = %d, want terminal", code, got)
		}
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
