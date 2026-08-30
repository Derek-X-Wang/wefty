package computerconformance

import (
	"context"
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

	for name, lateObservation := range map[string]driverObservation{
		"mutation remains current": {
			Version:        1,
			HumanDriving:   true,
			Generation:     2,
			Fingerprint:    driverFingerprint(unknownDocument),
			Classification: "unknown-version",
		},
		"mutation window was overwritten": {
			Version:        1,
			HumanDriving:   false,
			Generation:     3,
			Fingerprint:    driverFingerprint(falseDocument),
			Classification: "valid",
		},
	} {
		t.Run(name, func(t *testing.T) {
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
			if runner.mutateAndObserveDriver(context.Background(), unknownDocument, "unknown-version", false) {
				t.Fatal("unknown-driver-version-accepted mutation passed without exact rejection evidence")
			}
			if written != unknownDocument {
				t.Fatalf("written driver document = %q, want %q", written, unknownDocument)
			}
		})
	}
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
	if !runner.mutateAndObserveDriver(context.Background(), trueDocument, "valid", true) {
		t.Fatal("exact post-mutation observation was not accepted")
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
