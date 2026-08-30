package computerconformance

import (
	"context"
	"os"
	"path/filepath"
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

func TestUnknownDriverMutationCannotPassWhenObserverAttachesLate(t *testing.T) {
	const (
		falseDocument   = `{"version":1,"human_driving":false}`
		unknownDocument = `{"version":2,"human_driving":true}`
	)

	t.Run("mutation window was overwritten", func(t *testing.T) {
		lateObservation := driverObservation{
			Version:        1,
			HumanDriving:   false,
			Generation:     3,
			Fingerprint:    driverFingerprint(falseDocument),
			Classification: "valid",
		}
		reads := 0
		written := ""
		runner := runtimeRunner{
			controlDir: t.TempDir(),
			config: RuntimeConfig{Sleep: func(context.Context, time.Duration) error {
				return nil
			}},
			readDriverObservationHook: func(context.Context) (driverObservation, error) {
				reads++
				if reads == 1 {
					return driverObservation{
						Version:        1,
						Generation:     1,
						Fingerprint:    driverFingerprint(falseDocument),
						Classification: "valid",
					}, nil
				}
				// The fixture's unknown-version event was emitted before the
				// checker's post-write observer attached. A later snapshot must
				// not be mistaken for observation of that exact mutation.
				return lateObservation, nil
			},
			writeDriverHook: func(document string) error {
				written = document
				return nil
			},
		}
		if err := runner.writeDriver(falseDocument); err != nil {
			t.Fatal(err)
		}
		if result := runner.mutateAndObserveDriver(context.Background(), unknownDocument, "unknown-version", false); result.Failure != driverMutationNotObserved {
			t.Fatal("unknown-driver-version-accepted mutation passed without exact rejection evidence")
		}
		if written != unknownDocument {
			t.Fatalf("written driver document = %q, want %q", written, unknownDocument)
		}
	})
}

func TestDriverMutationWaitsForObserverBeforePublishing(t *testing.T) {
	const (
		falseDocument = `{"version":1,"human_driving":false}`
		trueDocument  = `{"version":1,"human_driving":true}`
	)
	reads := 0
	runner := runtimeRunner{
		controlDir: t.TempDir(),
		config: RuntimeConfig{Sleep: func(context.Context, time.Duration) error {
			return nil
		}},
		readDriverObservationHook: func(context.Context) (driverObservation, error) {
			reads++
			switch reads {
			case 1:
				return driverObservation{Version: 1, Generation: 7, Fingerprint: "stale", Classification: "valid"}, nil
			case 2:
				return driverObservation{Version: 1, Generation: 8, Fingerprint: driverFingerprint(falseDocument), Classification: "valid"}, nil
			default:
				return driverObservation{Version: 1, HumanDriving: true, Generation: 9, Fingerprint: driverFingerprint(trueDocument), Classification: "valid"}, nil
			}
		},
		writeDriverHook: func(string) error {
			if reads < 2 {
				t.Fatal("driver mutation published before the observer attached to the current document")
			}
			return nil
		},
	}
	if err := runner.writeDriver(falseDocument); err != nil {
		t.Fatal(err)
	}
	if !runner.mutateAndObserveDriver(context.Background(), trueDocument, "valid", true).OK() {
		t.Fatal("exact post-mutation observation was not accepted")
	}
}

func TestNegativeDriverFailureDetailsAreDistinct(t *testing.T) {
	for name, fixture := range map[string]struct {
		failure driverMutationFailure
		want    string
	}{
		"accepted":           {driverMutationUnexpectedHumanDriving, "unknown-version generation was accepted"},
		"not observed":       {driverMutationNotObserved, "unknown-version generation was not observed"},
		"never attached":     {driverObserverNeverAttached, "driver observer never attached before unknown-version generation"},
		"generation reset":   {driverObserverGenerationReset, "driver observer generation reset before unknown-version generation was observed"},
		"no-op rewrite":      {driverMutationNoOp, "driver assertion attempted a no-op rewrite before unknown-version generation"},
		"publication failed": {driverMutationWriteFailed, "unknown-version generation could not be published"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := negativeDriverFailureDetail("unknown-version generation", driverMutationResult{Failure: fixture.failure}); got != fixture.want {
				t.Fatalf("detail = %q, want %q", got, fixture.want)
			}
		})
	}
}

func TestDriverMutationRejectsNoOpAndGenerationReset(t *testing.T) {
	const (
		falseDocument = `{"version":1,"human_driving":false}`
		trueDocument  = `{"version":1,"human_driving":true}`
	)
	runner := runtimeRunner{
		controlDir: t.TempDir(),
		config:     RuntimeConfig{Sleep: func(context.Context, time.Duration) error { return nil }},
	}
	if err := runner.writeDriver(falseDocument); err != nil {
		t.Fatal(err)
	}
	if result := runner.mutateAndObserveDriver(context.Background(), falseDocument, "valid", false); result.Failure != driverMutationNoOp {
		t.Fatalf("no-op failure = %q", result.Failure)
	}
	runner.readDriverObservationHook = func(context.Context) (driverObservation, error) {
		payload, err := os.ReadFile(filepath.Join(runner.controlDir, "driver.json"))
		if err != nil {
			return driverObservation{}, err
		}
		if string(payload) == falseDocument {
			return driverObservation{Version: 1, Generation: 8, Fingerprint: driverFingerprint(falseDocument), Classification: "valid"}, nil
		}
		return driverObservation{Version: 1, Generation: 1, Fingerprint: driverFingerprint(trueDocument), Classification: "valid"}, nil
	}
	if result := runner.mutateAndObserveDriver(context.Background(), trueDocument, "valid", true); result.Failure != driverObserverGenerationReset {
		t.Fatalf("generation reset failure = %q", result.Failure)
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
