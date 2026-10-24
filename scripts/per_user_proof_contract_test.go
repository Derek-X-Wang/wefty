package scripts

import (
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Keep the measurement safety fixtures in the ordinary secretless contract
// gate. The Python module is imported only; no Mac command or proof runs.
func TestPerUserProofMeasurementSafety(t *testing.T) {
	// This checked-in Python input must remain an actual marshalled snapshot:
	// CapabilityObservation has tags; ComputerSeatEvidence's outer fields do not.
	probe := contract.CapabilityObservation{Revision: 1, Capabilities: map[string]bool{"kind:oci": true}, ObservedAt: time.Unix(1, 0).UTC()}
	seat := func(id, attempt string, at time.Time, place int) l1.ComputerSeatEvidence {
		return l1.ComputerSeatEvidence{ComputerSeatObservation: l1.ComputerSeatObservation{SeatID: id, Place: place, State: l1.ComputerSeatReady}, AttemptID: attempt, ListRevision: 1, Generation: 1, Sequence: 1, ObservedAt: at}
	}
	actual, err := json.Marshal(map[string]any{
		"capability": agent.CapabilitySnapshot{CapabilityObservation: probe, LastProbe: &probe},
		"seats":      []l1.ComputerSeatEvidence{seat("seat-current", "attempt-current", time.Unix(2, 0).UTC(), 1), seat("seat-old", "attempt-old", time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), 2)},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("testdata/per-user-proof-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := json.Unmarshal(fixture, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(actual, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("Python fixture differs from Go snapshot: %s", actual)
	}
	command := exec.Command("python3", "test-proof-measurements.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("proof safety fixtures: %v\n%s", err, output)
	}
}
