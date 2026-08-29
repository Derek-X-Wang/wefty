package computerconformance

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestMissingEndpointObservationWindowStaysInsideReadinessBudget(t *testing.T) {
	if missingEndpointObservationWindow != 15*time.Second {
		t.Fatalf("missing endpoint observation window = %s, want 15s", missingEndpointObservationWindow)
	}
	if missingEndpointObservationWindow >= contract.ComputerStartupReadinessTimeout {
		t.Fatal("missing endpoint observation window exhausted the contract readiness budget")
	}
}

func TestMutationFailureStopsAfterOwningCell(t *testing.T) {
	runner := runtimeRunner{
		config:   RuntimeConfig{MutationProfile: "shm-too-small"},
		recorder: NewRecorder("broken", "docker", "linux/arm64", time.Unix(100, 0)),
	}
	if runner.mutationFailed() {
		t.Fatal("mutation stopped before a failed assertion")
	}
	runner.record("harness.shm-size", StatusFail, "/dev/shm ceiling is not 1 GiB")
	if !runner.mutationFailed() {
		t.Fatal("mutation continued after its first failed assertion")
	}
}

func TestConformantSubjectNeverUsesMutationShortCircuit(t *testing.T) {
	runner := runtimeRunner{
		recorder: NewRecorder("reference", "docker", "linux/arm64", time.Unix(100, 0)),
	}
	runner.record("input.control-accepted", StatusFail, "control pointer was not observed")
	if runner.mutationFailed() {
		t.Fatal("conformant subject used the broken-image short circuit")
	}
}

func TestInputObserverAdvanceRequiresKeyAndObserverProgress(t *testing.T) {
	observerLines := func(value uint64) *uint64 { return &value }
	before := inputObservation{KeyEvents: 7, ObserverLines: observerLines(19)}
	for name, after := range map[string]inputObservation{
		"neither":       {KeyEvents: 7, ObserverLines: observerLines(19)},
		"key only":      {KeyEvents: 8, ObserverLines: observerLines(19)},
		"observer only": {KeyEvents: 7, ObserverLines: observerLines(20)},
		"field removed": {KeyEvents: 8},
	} {
		t.Run(name, func(t *testing.T) {
			if inputObserverAdvanced(before, after) {
				t.Fatalf("inputObserverAdvanced(%+v, %+v) = true", before, after)
			}
		})
	}
	if after := (inputObservation{KeyEvents: 8, ObserverLines: observerLines(20)}); !inputObserverAdvanced(before, after) {
		t.Fatalf("inputObserverAdvanced(%+v, %+v) = false", before, after)
	}
	if !inputObserverAdvanced(inputObservation{KeyEvents: 7}, inputObservation{KeyEvents: 8}) {
		t.Fatal("inputObserverAdvanced rejected a key-only legacy oracle")
	}
}
