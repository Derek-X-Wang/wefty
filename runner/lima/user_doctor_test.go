package lima

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestMinimalDoctorFactsCanNamePerUserLaunchAgent(t *testing.T) {
	facts := BuildMinimalDoctorFacts(UnitStateLaunchedByUnit, SupervisorFacts{Instance: "wefty-proof", State: InstanceRunning, ObservedAt: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)}, nil, contract.CapabilityObservation{}, nil, time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
	facts.Unit.Label = "dev.wefty-proof.node-agent"
	path := filepath.Join(t.TempDir(), "facts.json")
	if err := WriteMinimalDoctorFacts(path, facts); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
