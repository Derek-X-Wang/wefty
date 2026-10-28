package ocicontrol

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestDoctorTreatsIdleHostVMAsHealthyAndDoesNotContactGuest(t *testing.T) {
	now := time.Unix(1000, 0)
	report := BuildDoctor(t.Context(), DoctorConfig{HostPlatform: PlatformFacts{OS: "darwin", Architecture: "arm64"}, AgentUser: "operator", Clock: &controlTestClock{now: now}, Intent: func(context.Context) (lima.OCIIntent, error) {
		return lima.OCIIntent{Version: lima.OCIIntentVersion, Revision: 1, Enabled: true, Position: lima.OCIPositionOnDemand, UpdatedAt: now}, nil
	}, LimaFacts: func() lima.SupervisorFacts {
		return lima.SupervisorFacts{State: lima.InstanceStopped, Enabled: true, Position: lima.OCIPositionOnDemand, ReasonCode: contract.CapabilityReasonCode("host_vm_idle"), ObservedAt: now}
	}, CapabilitySnapshot: func() CapabilitySnapshot {
		return CapabilitySnapshot{CapabilityObservation: contract.CapabilityObservation{Revision: 1, ObservedAt: now, Capabilities: map[string]bool{"kind:process": true}, MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonCode("host_vm_idle")}}
	}, Helper: func(context.Context) (HelperDoctorSnapshot, error) {
		t.Fatal("idle doctor contacted guest")
		return HelperDoctorSnapshot{}, nil
	}})
	if report.Lima.Outcome != DiagnosticOK {
		t.Fatalf("idle Lima is unhealthy: %+v", report.Lima)
	}
	for _, f := range report.Findings {
		if f.Check == "capability-observation" && f.Outcome != DiagnosticOK {
			t.Fatalf("idle capability unhealthy: %+v", f)
		}
	}
}

func TestDoctorShowsHostVMDemand(t *testing.T) {
	report := BuildDoctor(t.Context(), DoctorConfig{HostPlatform: PlatformFacts{OS: "darwin", Architecture: "arm64"}, LimaFacts: func() lima.SupervisorFacts {
		return lima.SupervisorFacts{State: lima.InstanceRunning, Enabled: true, Demand: []lima.DemandItem{{Kind: "backup", ID: "backup-1"}}}
	}})
	var output bytes.Buffer
	if err := WriteDoctorHuman(&output, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "backup:backup-1") {
		t.Fatalf("doctor did not explain demand: %s", output.String())
	}
}
