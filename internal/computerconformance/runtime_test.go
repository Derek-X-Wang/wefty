package computerconformance

import (
	"testing"
	"time"
)

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
