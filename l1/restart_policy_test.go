package l1

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestSpawnFailureClassificationDefaultsTerminal(t *testing.T) {
	if !IsRestartableSpawnFailure(contract.SpawnFailureStartupReadinessTimeout) {
		t.Fatal("startup readiness timeout must be restartable")
	}
	if got := classifySpawnFailure(contract.SpawnFailurePublishedListener); got != spawnFailureInfrastructure {
		t.Fatalf("published listener classification = %d, want infrastructure", got)
	}
	for _, code := range []contract.SpawnFailureCode{
		contract.SpawnFailureProcessSpawn,
		contract.SpawnFailurePublishedPortOccupied,
		contract.SpawnFailureCode("future_unknown_failure"),
	} {
		if got := classifySpawnFailure(code); got != spawnFailureTerminal {
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
