package scripts

import (
	"os/exec"
	"testing"
)

// Keep the measurement safety fixtures in the ordinary secretless contract
// gate. The Python module is imported only; no Mac command or proof runs.
func TestPerUserProofMeasurementSafety(t *testing.T) {
	command := exec.Command("python3", "test-proof-measurements.py")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("proof safety fixtures: %v\n%s", err, output)
	}
}
